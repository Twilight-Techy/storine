//! Parallel execution of aggregate queries over a columnar snapshot.

use std::borrow::Cow;
use std::cell::OnceCell;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use storage_codec::simd;
use storage_codec::types::PropertyValue;
use storage_columnar::column::I128Body;
use storage_columnar::int::IntChunk;
use storage_columnar::{Body, Chunk, Kind, Val, ROW_GROUP_ROWS};
use storage_types::{Decimal, FastHashMap, Result, StorageError};

use crate::columnar::ColumnarSnapshot;
use crate::document::{Direction, Document};

use super::filter::{eval, RowGroupView};
use super::spill::{Spill, DEFAULT_SPILL_BUDGET, PARTITIONS};
use super::values::{cmp_props, pow10, put_u32, put_u64, DistinctSet, GroupValue, Hll, IntMap, IntSet, MinMax, SumAcc, HLL_DENSE_BYTES};
use super::{AggFunc, AggregateQuery, AggregateResult, Expr, GroupKey, DEFAULT_MEMORY_BUDGET, MAX_QUERY_ITEMS};

// ── per-thread buffers ──

thread_local! {
    /// Column buffers reused across row groups: no allocation (and no page faults on fresh
    /// mappings) per column per row group.
    static POOL: core::cell::RefCell<Vec<Vec<i64>>> = const { core::cell::RefCell::new(Vec::new()) };
}

/// A buffer of `n` values (contents unspecified: every caller overwrites it).
pub(super) fn take_i64(n: usize) -> Vec<i64> {
    let mut v = POOL.with(|p| p.borrow_mut().pop()).unwrap_or_default();
    if v.len() < n {
        v.resize(n, 0);
    } else {
        v.truncate(n);
    }
    v
}

pub(super) fn give_i64(v: Vec<i64>) {
    if v.capacity() > 0 {
        POOL.with(|p| {
            let mut p = p.borrow_mut();
            if p.len() < 64 {
                p.push(v);
            }
        });
    }
}

fn give_col(c: ColV) {
    if let Col::I64 { v, .. } = c.col {
        give_i64(v);
    }
}

/// Calls `f(i)` for every set bit `i` (64 bits per word, lowest first).
#[inline]
fn for_each_set(words: &[u64], mut f: impl FnMut(usize)) {
    for (w, &word) in words.iter().enumerate() {
        let mut m = word;
        while m != 0 {
            let j = m.trailing_zeros() as usize;
            f(w * 64 + j);
            m &= m - 1;
        }
    }
}

// ── deterministic tie-breaks ──

/// Of two values that compare equal (`1` and `1.0`), the one every run keeps.
fn det_pick(a: PropertyValue, b: PropertyValue) -> PropertyValue {
    if format!("{a:?}") <= format!("{b:?}") {
        a
    } else {
        b
    }
}

// ── accumulators ──

#[derive(Debug)]
enum Acc {
    Count(Vec<u64>),
    Sum(Vec<SumAcc>),
    Avg(Vec<SumAcc>, Vec<u64>),
    Min(Vec<MinMax>),
    Max(Vec<MinMax>),
    Distinct(Vec<DistinctSet>),
    Approx(Vec<Hll>),
}

impl Acc {
    fn new(f: AggFunc) -> Self {
        match f {
            AggFunc::Count => Self::Count(Vec::new()),
            AggFunc::Sum => Self::Sum(Vec::new()),
            AggFunc::Avg => Self::Avg(Vec::new(), Vec::new()),
            AggFunc::Min => Self::Min(Vec::new()),
            AggFunc::Max => Self::Max(Vec::new()),
            AggFunc::CountDistinct => Self::Distinct(Vec::new()),
            AggFunc::ApproxCountDistinct => Self::Approx(Vec::new()),
        }
    }

    fn push(&mut self) -> usize {
        match self {
            Self::Count(v) => {
                v.push(0);
                8
            }
            Self::Sum(v) => {
                v.push(SumAcc::default());
                64
            }
            Self::Avg(v, c) => {
                v.push(SumAcc::default());
                c.push(0);
                72
            }
            Self::Min(v) | Self::Max(v) => {
                v.push(MinMax::default());
                48
            }
            Self::Distinct(v) => {
                v.push(DistinctSet::default());
                48
            }
            Self::Approx(v) => {
                v.push(Hll::default());
                // A sparse sketch; turning dense charges `HLL_DENSE_BYTES` more.
                1024
            }
        }
    }
}

/// The representative key values of every group, `stride` (the key count) per group in one
/// vector: no allocation per group.
struct Reps {
    vals: Vec<PropertyValue>,
    stride: usize,
    n: usize,
}

impl Reps {
    fn new(stride: usize) -> Self {
        Self { vals: Vec::new(), stride, n: 0 }
    }

    fn len(&self) -> usize {
        self.n
    }

    fn is_empty(&self) -> bool {
        self.n == 0
    }

    fn get(&self, g: usize) -> &[PropertyValue] {
        &self.vals[g * self.stride..(g + 1) * self.stride]
    }

    fn get_mut(&mut self, g: usize) -> &mut [PropertyValue] {
        &mut self.vals[g * self.stride..(g + 1) * self.stride]
    }

    fn push(&mut self, rep: impl IntoIterator<Item = PropertyValue>) {
        let start = self.vals.len();
        self.vals.extend(rep.into_iter().take(self.stride));
        self.vals.resize(start + self.stride, PropertyValue::Null);
        self.n += 1;
    }

    /// Moves group `g`'s values out (leaving nulls).
    fn take(&mut self, g: usize) -> Vec<PropertyValue> {
        self.get_mut(g).iter_mut().map(|v| core::mem::replace(v, PropertyValue::Null)).collect()
    }

    fn take_one(&mut self, g: usize) -> PropertyValue {
        self.get_mut(g).first_mut().map_or(PropertyValue::Null, |v| core::mem::replace(v, PropertyValue::Null))
    }

    /// Replaces each representative that differs from `rep` by the deterministic choice.
    fn offer(&mut self, g: usize, rep: impl IntoIterator<Item = PropertyValue>) {
        for (c, r) in self.get_mut(g).iter_mut().zip(rep) {
            if *c != r {
                *c = det_pick(core::mem::replace(c, PropertyValue::Null), r);
            }
        }
    }
}

/// Group table and accumulators of one worker (or of the final result).
struct State<'b> {
    groups: FastHashMap<Vec<GroupValue>, u32>,
    reps: Reps,
    accs: Vec<Acc>,
    matched: u64,
    /// Row groups skipped by zone maps.
    pruned: u64,
    /// Bytes charged to the budget for groups (distinct sets are counted from their sizes).
    charged: usize,
    /// Group of each plain integer / timestamp key seen (`true`: timestamp), so a high-cardinality
    /// integer key costs one probe per row instead of building and hashing a key vector.
    ints: IntMap<(bool, i64), u32>,
    budget: &'b Budget,
}

thread_local! {
    /// `(budget address, bytes not yet added to it)` of this thread.
    static BUDGET_BATCH: core::cell::Cell<(usize, usize)> = const { core::cell::Cell::new((0, 0)) };
}

/// Memory accounting of one query. With a spill directory the limit is soft: workers spill
/// their partial groups between row groups once usage passes three quarters of it (one row
/// group can add at most its rows' worth of groups), and only a hard ceiling of twice the
/// limit fails. Without one, the limit fails at once.
struct Budget {
    used: AtomicUsize,
    limit: usize,
    spill_dir: Option<std::path::PathBuf>,
    spill_limit: u64,
    spill: std::sync::OnceLock<Result<Spill>>,
    /// What workers may add between two pressure checks (one row group each).
    slack: usize,
}

/// Upper bound of the bytes one row adds to a group table: a new group with every
/// accumulator, plus a distinct insert or a sketch's share of turning dense.
fn row_bytes(q: &AggregateQuery) -> usize {
    let per_agg: usize = q
        .aggregates
        .iter()
        .map(|a| match a.func {
            AggFunc::Count => 8,
            AggFunc::Sum => 64,
            AggFunc::Avg => 72,
            AggFunc::Min | AggFunc::Max => 48,
            AggFunc::CountDistinct => 48 + 64,
            AggFunc::ApproxCountDistinct => 1024 + 64,
        })
        .sum();
    128 + 64 * q.group_by.len() + per_agg
}

impl Budget {
    fn new(q: &AggregateQuery, spill_dir: Option<&std::path::Path>) -> Self {
        Self {
            used: AtomicUsize::new(0),
            limit: if q.memory_budget == 0 { DEFAULT_MEMORY_BUDGET } else { q.memory_budget },
            spill_dir: spill_dir.map(std::path::Path::to_path_buf),
            spill_limit: if q.spill_budget == 0 { DEFAULT_SPILL_BUDGET } else { q.spill_budget },
            spill: std::sync::OnceLock::new(),
            slack: 0,
        }
    }

    fn take(&self, n: usize) -> Result<()> {
        // Charges batch per thread (one shared atomic add per 64 KiB instead of one per group):
        // usage reads at most 64 KiB per worker low, and no worker contends on the counter.
        let n = BUDGET_BATCH.with(|b| {
            let (owner, pending) = b.get();
            let me = core::ptr::from_ref(self) as usize;
            let pending = if owner == me { pending + n } else { n };
            if pending < 1 << 16 {
                b.set((me, pending));
                0
            } else {
                b.set((me, 0));
                pending
            }
        });
        if n == 0 {
            return Ok(());
        }
        let now = self.used.fetch_add(n, Ordering::Relaxed) + n;
        let ceiling = if self.spill_dir.is_some() { self.limit.saturating_add(self.slack) } else { self.limit };
        if now > ceiling {
            return Err(StorageError::invalid_value(
                "the query needs more memory than its budget: narrow it with a filter, group by fewer values, or raise memory_budget",
            ));
        }
        Ok(())
    }

    fn release(&self, n: usize) {
        let _ = self.used.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |u| Some(u.saturating_sub(n)));
    }

    /// Usage calls for spilling (only when spilling is possible).
    fn pressed(&self) -> bool {
        self.spill_dir.is_some() && self.used.load(Ordering::Relaxed) > self.limit / 4 * 3
    }

    /// The query's spill runs, created on first use.
    fn spill(&self) -> Result<&Spill> {
        let dir = self.spill_dir.as_deref().ok_or_else(|| StorageError::invalid_value("internal: no spill directory"))?;
        self.spill.get_or_init(|| Spill::new(dir, self.spill_limit)).as_ref().map_err(Clone::clone)
    }

    fn spilled(&self) -> Option<&Spill> {
        match self.spill.get() {
            Some(Ok(s)) if s.used() => Some(s),
            _ => None,
        }
    }
}

impl<'b> State<'b> {
    fn new(q: &AggregateQuery, budget: &'b Budget) -> Result<Self> {
        let mut s = Self {
            groups: FastHashMap::default(),
            reps: Reps::new(q.group_by.len()),
            accs: q.aggregates.iter().map(|a| Acc::new(a.func)).collect(),
            matched: 0,
            pruned: 0,
            charged: 0,
            ints: IntMap::default(),
            budget,
        };
        if q.group_by.is_empty() {
            s.group(Vec::new(), Vec::new())?;
        }
        Ok(s)
    }

    /// A state without any group (not even the single group of an ungrouped query).
    fn empty(q: &AggregateQuery, budget: &'b Budget) -> Self {
        Self {
            groups: FastHashMap::default(),
            reps: Reps::new(q.group_by.len()),
            accs: q.aggregates.iter().map(|a| Acc::new(a.func)).collect(),
            matched: 0,
            pruned: 0,
            charged: 0,
            ints: IntMap::default(),
            budget,
        }
    }

    /// Splits the groups into `PARTITIONS` states by key hash (counters stay here).
    fn split(mut self, q: &AggregateQuery) -> Result<Vec<State<'b>>> {
        // The groups move: release what this state charged, the parts charge it again.
        self.budget.release(self.footprint());
        self.charged = 0;
        let mut out: Vec<State<'b>> = (0..PARTITIONS).map(|_| State::empty(q, self.budget)).collect();
        let generic: Vec<(Vec<GroupValue>, u32)> = core::mem::take(&mut self.groups).into_iter().collect();
        let ints: Vec<((bool, i64), u32)> = core::mem::take(&mut self.ints).into_iter().collect();
        let mut reps = core::mem::replace(&mut self.reps, Reps::new(0));
        let mut accs = core::mem::take(&mut self.accs);
        for (key, g) in generic {
            let part = &mut out[partition_of(&key)];
            let ng = part.group(key, reps.take(g as usize))? as usize;
            move_accs(&mut accs, g as usize, &mut part.accs, ng);
        }
        for (ik, g) in ints {
            let part = &mut out[partition_of(core::slice::from_ref(&int_gv(ik)))];
            let ng = part.int_group_rep(ik, reps.take_one(g as usize))? as usize;
            move_accs(&mut accs, g as usize, &mut part.accs, ng);
        }
        Ok(out)
    }

    /// The group of a single integer (or timestamp) key: the same group `group` gives it.
    fn int_group(&mut self, x: i64, ts: bool) -> Result<u32> {
        if let Some(&g) = self.ints.get(&(ts, x)) {
            // The representative is the plain value unless another kind (`1.0`) came first.
            let canonical = matches!((self.reps.get(g as usize), ts), ([PropertyValue::Int64(y)], false) | ([PropertyValue::TimestampNs(y)], true) if *y == x);
            if canonical {
                return Ok(g);
            }
        }
        let rep = if ts { PropertyValue::TimestampNs(x) } else { PropertyValue::Int64(x) };
        self.int_group_rep((ts, x), rep)
    }

    /// Bytes this state holds against the budget.
    fn footprint(&self) -> usize {
        let distinct: usize = self
            .accs
            .iter()
            .map(|a| match a {
                Acc::Distinct(d) => d.iter().map(|s| s.len() * 64).sum(),
                Acc::Approx(h) => h.iter().filter(|x| x.is_dense()).count() * HLL_DENSE_BYTES,
                _ => 0,
            })
            .sum();
        self.charged + distinct
    }

    /// Writes every group to its spill partition and starts over empty (counters kept).
    fn spill_out(&mut self, q: &AggregateQuery) -> Result<()> {
        let spill = self.budget.spill()?;
        let mut rec = Vec::new();
        for (key, g) in self.take_keys() {
            let key = &key;
            let g = g as usize;
            rec.clear();
            put_u32(&mut rec, key.len() as u32);
            for k in key {
                k.encode(&mut rec);
            }
            put_u32(&mut rec, self.reps.get(g).len() as u32);
            for v in self.reps.get(g) {
                storage_codec::value_wire::encode_value(v, &mut rec);
            }
            for a in &self.accs {
                match a {
                    Acc::Count(c) => put_u64(&mut rec, c[g]),
                    Acc::Sum(s) => s[g].encode(&mut rec),
                    Acc::Avg(s, c) => {
                        s[g].encode(&mut rec);
                        put_u64(&mut rec, c[g]);
                    }
                    Acc::Min(m) | Acc::Max(m) => m[g].encode(&mut rec),
                    Acc::Distinct(d) => {
                        put_u32(&mut rec, d[g].len() as u32);
                        for v in d[g].values() {
                            v.encode(&mut rec);
                        }
                    }
                    Acc::Approx(h) => h[g].encode(&mut rec),
                }
            }
            spill.append(partition_of(key), &rec)?;
        }
        self.budget.release(self.footprint());
        self.groups = FastHashMap::default();
        self.reps = Reps::new(q.group_by.len());
        self.accs = q.aggregates.iter().map(|a| Acc::new(a.func)).collect();
        self.ints = IntMap::default();
        self.charged = 0;
        if q.group_by.is_empty() {
            self.group(Vec::new(), Vec::new())?;
        }
        Ok(())
    }

    /// Merges one spilled group record.
    fn load(&mut self, rec: &[u8]) -> Result<()> {
        let mut r = storage_codec::value_wire::SveReader::new(rec);
        let nk = r.count(1)?;
        let key = (0..nk).map(|_| GroupValue::decode(&mut r)).collect::<Result<Vec<_>>>()?;
        let nr = r.count(1)?;
        let rep = (0..nr).map(|_| r.value()).collect::<Result<Vec<_>>>()?;
        let g = self.group(key, rep)? as usize;
        for a in &mut self.accs {
            match a {
                Acc::Count(c) => c[g] += r.u64()?,
                Acc::Sum(s) => s[g].merge(&SumAcc::decode(&mut r)?),
                Acc::Avg(s, c) => {
                    s[g].merge(&SumAcc::decode(&mut r)?);
                    c[g] += r.u64()?;
                }
                Acc::Min(m) => offer_mm(&mut m[g], MinMax::decode(&mut r)?.value, false),
                Acc::Max(m) => offer_mm(&mut m[g], MinMax::decode(&mut r)?.value, true),
                Acc::Distinct(d) => {
                    let n = r.count(1)?;
                    for _ in 0..n {
                        if d[g].insert(GroupValue::decode(&mut r)?) {
                            self.budget.take(64)?;
                        }
                    }
                }
                Acc::Approx(h) => {
                    if h[g].merge(&Hll::decode(&mut r)?) {
                        self.budget.take(HLL_DENSE_BYTES)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn group(&mut self, key: Vec<GroupValue>, rep: Vec<PropertyValue>) -> Result<u32> {
        // Single integer / timestamp keys live only in `ints` (whatever value kind produced
        // them: `1`, `1.0` and `1.00` are all `Exact(1, 0)`), so every path finds one group.
        if let [k] = key.as_slice() {
            if let Some(ik) = int_key(k) {
                return self.int_group_rep(ik, rep.into_iter().next().unwrap_or(PropertyValue::Null));
            }
        }
        if let Some(&g) = self.groups.get(&key) {
            self.reps.offer(g as usize, rep);
            return Ok(g);
        }
        let mut size = key.iter().map(GroupValue::size).sum::<usize>() + 64;
        let g = self.new_group(rep, &mut size)?;
        self.groups.insert(key, g);
        Ok(g)
    }

    fn new_group(&mut self, rep: impl IntoIterator<Item = PropertyValue>, size: &mut usize) -> Result<u32> {
        let g = self.reps.len() as u32;
        for a in &mut self.accs {
            *size += a.push();
        }
        self.budget.take(*size)?;
        self.charged += *size;
        self.reps.push(rep);
        Ok(g)
    }

    /// The group of single key `(timestamp?, value)`, keeping its representative deterministic.
    fn int_group_rep(&mut self, ik: (bool, i64), rep: PropertyValue) -> Result<u32> {
        if let Some(&g) = self.ints.get(&ik) {
            self.reps.offer(g as usize, [rep]);
            return Ok(g);
        }
        let mut size = 48;
        let g = self.new_group([rep], &mut size)?;
        self.ints.insert(ik, g);
        Ok(g)
    }

    /// Every group's key (generic keys, then integer keys) with its id.
    fn take_keys(&mut self) -> Vec<(Vec<GroupValue>, u32)> {
        let mut out: Vec<(Vec<GroupValue>, u32)> = core::mem::take(&mut self.groups).into_iter().collect();
        out.extend(core::mem::take(&mut self.ints).into_iter().map(|(k, g)| (vec![int_gv(k)], g)));
        out
    }

    fn merge(&mut self, mut other: State<'_>) -> Result<()> {
        self.matched += other.matched;
        self.pruned += other.pruned;
        // `other`'s memory moves here: what this state does not re-charge is released.
        self.budget.release(other.footprint());
        other.charged = 0;
        // Merge order does not matter: every accumulator merge and representative choice is
        // order-independent.
        let generic: Vec<(Vec<GroupValue>, u32)> = core::mem::take(&mut other.groups).into_iter().collect();
        let ints: Vec<((bool, i64), u32)> = core::mem::take(&mut other.ints).into_iter().collect();
        let mut other_reps = core::mem::replace(&mut other.reps, Reps::new(0));
        let mut map = vec![0u32; other_reps.len()];
        for (key, og) in generic {
            map[og as usize] = self.group(key, other_reps.take(og as usize))?;
        }
        for (ik, og) in ints {
            map[og as usize] = self.int_group_rep(ik, other_reps.take_one(og as usize))?;
        }
        let budget = self.budget;
        for (mine, theirs) in self.accs.iter_mut().zip(core::mem::take(&mut other.accs)) {
            match (mine, theirs) {
                (Acc::Count(a), Acc::Count(b)) => b.iter().enumerate().for_each(|(i, x)| a[map[i] as usize] += x),
                (Acc::Sum(a), Acc::Sum(b)) => b.iter().enumerate().for_each(|(i, x)| a[map[i] as usize].merge(x)),
                (Acc::Avg(a, ac), Acc::Avg(b, bc)) => {
                    for (i, x) in b.iter().enumerate() {
                        a[map[i] as usize].merge(x);
                        ac[map[i] as usize] += bc[i];
                    }
                }
                (Acc::Min(a), Acc::Min(b)) => b.into_iter().enumerate().for_each(|(i, x)| offer_mm(&mut a[map[i] as usize], x.value, false)),
                (Acc::Max(a), Acc::Max(b)) => b.into_iter().enumerate().for_each(|(i, x)| offer_mm(&mut a[map[i] as usize], x.value, true)),
                (Acc::Distinct(a), Acc::Distinct(b)) => {
                    for (i, set) in b.into_iter().enumerate() {
                        let added = a[map[i] as usize].extend(set);
                        budget.take(added * 64)?;
                    }
                }
                (Acc::Approx(a), Acc::Approx(b)) => {
                    for (i, x) in b.iter().enumerate() {
                        if a[map[i] as usize].merge(x) {
                            budget.take(HLL_DENSE_BYTES)?;
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
}

impl Drop for State<'_> {
    fn drop(&mut self) {
        self.budget.release(self.footprint());
    }
}

/// Moves group `g`'s accumulator entries to entry `ng` of `dst`.
fn move_accs(src: &mut [Acc], g: usize, dst: &mut [Acc], ng: usize) {
    for (a, b) in src.iter_mut().zip(dst.iter_mut()) {
        match (a, b) {
            (Acc::Count(a), Acc::Count(b)) => b[ng] = a[g],
            (Acc::Sum(a), Acc::Sum(b)) => b[ng] = core::mem::take(&mut a[g]),
            (Acc::Avg(a, ac), Acc::Avg(b, bc)) => {
                b[ng] = core::mem::take(&mut a[g]);
                bc[ng] = ac[g];
            }
            (Acc::Min(a), Acc::Min(b)) | (Acc::Max(a), Acc::Max(b)) => b[ng] = core::mem::take(&mut a[g]),
            (Acc::Distinct(a), Acc::Distinct(b)) => b[ng] = core::mem::take(&mut a[g]),
            (Acc::Approx(a), Acc::Approx(b)) => b[ng] = core::mem::take(&mut a[g]),
            _ => {}
        }
    }
}

/// The `ints` key of a single group key value, if it is an integer or a timestamp.
fn int_key(k: &GroupValue) -> Option<(bool, i64)> {
    match k {
        GroupValue::Exact(m, 0) => i64::try_from(*m).ok().map(|x| (false, x)),
        GroupValue::Timestamp(t) => Some((true, *t)),
        _ => None,
    }
}

fn int_gv((ts, x): (bool, i64)) -> GroupValue {
    if ts {
        GroupValue::Timestamp(x)
    } else {
        GroupValue::Exact(i128::from(x), 0)
    }
}

/// Spill partition of a group key.
fn partition_of(key: &[GroupValue]) -> usize {
    let h = key.iter().fold(0x9E37_79B9_7F4A_7C15u64, |a, v| (a.rotate_left(23) ^ v.hash()).wrapping_mul(0xFF51_AFD7_ED55_8CCD));
    ((h ^ (h >> 29)) % PARTITIONS as u64) as usize
}

/// `MinMax::offer` with a deterministic choice between equal values.
fn offer_mm(m: &mut MinMax, v: Option<PropertyValue>, max: bool) {
    let Some(v) = v else { return };
    if let Some(cur) = &m.value {
        if cmp_props(&v, cur) == core::cmp::Ordering::Equal {
            let cur = m.value.take().unwrap_or(PropertyValue::Null);
            m.value = Some(det_pick(cur, v));
            return;
        }
    }
    m.offer(v, max);
}

// ── column values for keys and aggregates ──

/// A column (or expression) over the rows of a row group.
enum Col {
    /// Exact numbers `v * 10^-scale` (`dec`: decimals; `kind` for the output type).
    I64 { v: Vec<i64>, scale: u8, dec: bool, kind: Kind },
    /// `kind`: `UInt` / `Int128` columns keep their type; `Int` (arithmetic) prints as `Int64`
    /// when it fits.
    I128 { v: Vec<i128>, scale: u8, dec: bool, kind: Kind },
    F64(Vec<f64>),
    /// Timestamps (nanoseconds): typed for min / max / distinct / grouping, not numbers in
    /// arithmetic (they take the row-by-row path there, as other non-numeric values do).
    Ts(Vec<i64>),
    /// Anything else, row by row.
    Vals(Vec<Option<Val>>),
}

struct ColV {
    col: Col,
    /// Present bits (rows with a value).
    valid: Vec<u64>,
}

fn all_bits(n: usize) -> Vec<u64> {
    let mut v = vec![u64::MAX; n.div_ceil(64)];
    if n % 64 != 0 {
        if let Some(l) = v.last_mut() {
            *l = (1u64 << (n % 64)) - 1;
        }
    }
    v
}

#[inline]
fn bit(words: &[u64], i: usize) -> bool {
    words.get(i / 64).is_some_and(|w| w >> (i % 64) & 1 == 1)
}

fn validity_of(chunk: &Chunk<'_>, n: usize) -> Vec<u64> {
    let mut v = all_bits(n);
    if matches!(chunk.body, Body::AllNull) {
        v.fill(0);
    } else if let Some(p) = &chunk.validity {
        for (w, word) in v.iter_mut().enumerate() {
            let mut m = 0u64;
            for j in 0..64 {
                let i = w * 64 + j;
                if i < n && p.get(i) == 1 {
                    m |= 1 << j;
                }
            }
            *word = m;
        }
    }
    v
}

fn vals_col(vals: Vec<Option<Val>>) -> ColV {
    let n = vals.len();
    let mut valid = vec![0u64; n.div_ceil(64)];
    for (i, v) in vals.iter().enumerate() {
        let null = match v {
            None => true,
            Some(Val::Nested(b)) => b.as_slice() == [storage_codec::value_wire::SVE_NULL],
            _ => false,
        };
        if !null {
            valid[i / 64] |= 1 << (i % 64);
        }
    }
    ColV { col: Col::Vals(vals), valid }
}

fn field_col(rg: &RowGroupView<'_>, path: &str, sel: &[u64]) -> ColV {
    let n = rg.n;
    if rg.has_children(path) {
        let vals = (0..n).map(|i| Val::from_property(&rg.value_at(path, i))).collect();
        return vals_col(vals);
    }
    let Some(chunk) = rg.chunks.get(path) else {
        return ColV { col: Col::Vals(vec![None; n]), valid: vec![0; n.div_ceil(64)] };
    };
    let mut valid = validity_of(chunk, n);
    // Late materialization: with few selected rows, decode only those (random access).
    let sparse = (simd::count_ones(sel) as usize) * 8 < n;
    let ints = |ic: &IntChunk<'_>| {
        let mut v = take_i64(n);
        if !(sparse && ic.gather(sel, &mut v)) {
            ic.decode_into(0, &mut v);
        }
        v
    };
    let col = match &chunk.body {
        Body::Int(Kind::Int, ic) => Col::I64 { v: ints(ic), scale: 0, dec: false, kind: Kind::Int },
        Body::Int(Kind::Timestamp, ic) => Col::Ts(ints(ic)),
        Body::Int(Kind::UInt, ic) => Col::I128 { v: ints(ic).into_iter().map(|x| i128::from(x as u64 ^ (1 << 63))).collect(), scale: 0, dec: false, kind: Kind::UInt },
        Body::Int128(I128Body::Small(ic)) => Col::I64 { v: ints(ic), scale: 0, dec: false, kind: Kind::Int128 },
        Body::Int128(b) => Col::I128 { v: Body::i128_values(b, n), scale: 0, dec: false, kind: Kind::Int128 },
        Body::Decimal(I128Body::Small(ic), IntChunk::Const { value, .. }) if (0..=38).contains(value) => {
            Col::I64 { v: ints(ic), scale: *value as u8, dec: true, kind: Kind::Decimal }
        }
        Body::Float(fb) => Col::F64(Body::float_values(fb, n)),
        _ => return vals_col(rg.vals(path).cloned().unwrap_or_else(|| vec![None; n])),
    };
    if sparse {
        // Rows outside the selection may hold stale buffer contents: never count them.
        simd::and_words(&mut valid, sel);
    }
    ColV { col, valid }
}

fn const_col(v: &PropertyValue, n: usize) -> ColV {
    let col = match v {
        PropertyValue::Int64(x) => Col::I64 { v: vec![*x; n], scale: 0, dec: false, kind: Kind::Int },
        PropertyValue::UInt64(x) => Col::I128 { v: vec![i128::from(*x); n], scale: 0, dec: false, kind: Kind::UInt },
        PropertyValue::Int128(x) => Col::I128 { v: vec![*x; n], scale: 0, dec: false, kind: Kind::Int128 },
        PropertyValue::Decimal(d) => match i64::try_from(d.mantissa()) {
            Ok(m) => Col::I64 { v: vec![m; n], scale: d.scale(), dec: true, kind: Kind::Decimal },
            Err(_) => Col::I128 { v: vec![d.mantissa(); n], scale: d.scale(), dec: true, kind: Kind::Decimal },
        },
        PropertyValue::Float64(f) => Col::F64(vec![*f; n]),
        PropertyValue::Null => return ColV { col: Col::Vals(vec![None; n]), valid: vec![0; n.div_ceil(64)] },
        other => return vals_col(vec![Val::from_property(other); n]),
    };
    ColV { col, valid: all_bits(n) }
}

#[derive(Clone, Copy)]
enum Op {
    Add,
    Sub,
    Mul,
}

fn to_i128(c: Col) -> Option<(Vec<i128>, u8, bool)> {
    match c {
        Col::I64 { v, scale, dec, .. } => Some((v.into_iter().map(i128::from).collect(), scale, dec)),
        Col::I128 { v, scale, dec, .. } => Some((v, scale, dec)),
        _ => None,
    }
}

fn exact_f64(m: i128, s: u8) -> f64 {
    if s == 0 {
        m as f64
    } else {
        format!("{m}e-{s}").parse().unwrap_or(f64::NAN)
    }
}

fn to_f64(c: &Col, n: usize) -> Vec<f64> {
    match c {
        Col::I64 { v, scale, .. } => v.iter().map(|&x| exact_f64(i128::from(x), *scale)).collect(),
        Col::I128 { v, scale, .. } => v.iter().map(|&x| exact_f64(x, *scale)).collect(),
        Col::F64(v) => v.clone(),
        Col::Ts(_) | Col::Vals(_) => vec![f64::NAN; n],
    }
}

fn overflow() -> StorageError {
    StorageError::invalid_value("numeric overflow: the value does not fit 38 digits")
}

/// Numeric value of a row-wise value (for arithmetic on mixed columns).
fn num_of(v: &Val) -> Option<Val> {
    matches!(v, Val::Int(_) | Val::UInt(_) | Val::Int128(_) | Val::Decimal(..) | Val::Float(_)).then(|| v.clone())
}

fn arith_vals(a: Option<&Val>, b: Option<&Val>, op: Op) -> Result<Option<Val>> {
    let (Some(a), Some(b)) = (a.and_then(num_of), b.and_then(num_of)) else { return Ok(None) };
    let exact = |v: &Val| -> Option<(i128, u8)> {
        match v {
            Val::Int(x) => Some((i128::from(*x), 0)),
            Val::UInt(x) => Some((i128::from(*x), 0)),
            Val::Int128(x) => Some((*x, 0)),
            Val::Decimal(m, s) => Some((*m, *s)),
            _ => None,
        }
    };
    let dec = matches!(a, Val::Decimal(..)) || matches!(b, Val::Decimal(..));
    match (exact(&a), exact(&b)) {
        (Some((x, sx)), Some((y, sy))) => {
            let (m, s) = exact_op(x, sx, y, sy, op)?;
            Ok(Some(if dec { Val::Decimal(m, s) } else { int_val(m) }))
        }
        _ => {
            let f = |v: &Val| match v {
                Val::Float(f) => *f,
                other => exact(other).map_or(f64::NAN, |(m, s)| exact_f64(m, s)),
            };
            let (x, y) = (f(&a), f(&b));
            Ok(Some(Val::Float(match op {
                Op::Add => x + y,
                Op::Sub => x - y,
                Op::Mul => x * y,
            })))
        }
    }
}

/// An integer result: `Int` when it fits 64 bits, `Int128` otherwise.
fn int_val(m: i128) -> Val {
    i64::try_from(m).map_or(Val::Int128(m), Val::Int)
}

fn exact_op(x: i128, sx: u8, y: i128, sy: u8, op: Op) -> Result<(i128, u8)> {
    match op {
        Op::Mul => {
            let s = sx.checked_add(sy).filter(|s| *s <= 38).ok_or_else(overflow)?;
            Ok((x.checked_mul(y).ok_or_else(overflow)?, s))
        }
        Op::Add | Op::Sub => {
            let s = sx.max(sy);
            let x = pow10(u32::from(s - sx)).and_then(|p| x.checked_mul(p)).ok_or_else(overflow)?;
            let y = pow10(u32::from(s - sy)).and_then(|p| y.checked_mul(p)).ok_or_else(overflow)?;
            let r = if matches!(op, Op::Add) { x.checked_add(y) } else { x.checked_sub(y) };
            Ok((r.ok_or_else(overflow)?, s))
        }
    }
}

fn binop(a: ColV, b: ColV, op: Op, n: usize) -> Result<ColV> {
    let (a, b) = (untyped_ts(a, n), untyped_ts(b, n));
    let mut valid = a.valid.clone();
    simd::and_words(&mut valid, &b.valid);
    let col = match (a.col, b.col) {
        // Row by row (mixed or non-numeric columns). Only `+` and `*` get here (subtraction goes
        // through `sub_op`, which keeps operand order), so operand order does not matter.
        (Col::Vals(x), y) | (y, Col::Vals(x)) => {
            let ys = col_vals(y, n);
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                out.push(if bit(&valid, i) { arith_vals(x[i].as_ref(), ys[i].as_ref(), op)? } else { None });
            }
            return Ok(ColV { col: Col::Vals(out), valid });
        }
        (Col::F64(x), y) => {
            let y = to_f64(&y, n);
            Col::F64(x.iter().zip(&y).map(|(a, b)| apply_f(*a, *b, op)).collect())
        }
        (x, Col::F64(y)) => {
            let x = to_f64(&x, n);
            Col::F64(x.iter().zip(&y).map(|(a, b)| apply_f(*a, *b, op)).collect())
        }
        (Col::I64 { v: x, scale: sx, dec: dx, .. }, Col::I64 { v: y, scale: sy, dec: dy, .. }) => {
            let dec = dx || dy;
            match i64_op(&x, sx, &y, sy, op, &valid) {
                Some((v, s)) => {
                    give_i64(x);
                    give_i64(y);
                    Col::I64 {
                        v,
                        scale: s,
                        dec,
                        kind: if dec { Kind::Decimal } else { Kind::Int },
                    }
                }
                None => wide_op(
                    Col::I64 {
                        v: x,
                        scale: sx,
                        dec: dx,
                        kind: Kind::Int,
                    },
                    Col::I64 {
                        v: y,
                        scale: sy,
                        dec: dy,
                        kind: Kind::Int,
                    },
                    op,
                    &valid,
                )?,
            }
        }
        (x, y) => wide_op(x, y, op, &valid)?,
    };
    Ok(ColV { col, valid })
}

/// Timestamps as row-by-row values (arithmetic treats them as non-numbers).
fn untyped_ts(c: ColV, n: usize) -> ColV {
    match c.col {
        Col::Ts(_) => ColV { col: Col::Vals(col_vals(c.col, n)), valid: c.valid },
        _ => c,
    }
}

fn col_vals(c: Col, n: usize) -> Vec<Option<Val>> {
    match c {
        Col::Vals(v) => v,
        Col::Ts(v) => v.into_iter().map(|x| Some(Val::Timestamp(x))).collect(),
        Col::I64 { v, scale, dec, kind } => v
            .into_iter()
            .map(|x| {
                Some(match (dec, kind) {
                    (true, _) => Val::Decimal(i128::from(x), scale),
                    (_, Kind::Int128) => Val::Int128(i128::from(x)),
                    _ => Val::Int(x),
                })
            })
            .collect(),
        Col::I128 { v, scale, dec, kind } => v
            .into_iter()
            .map(|x| {
                Some(match (dec, kind) {
                    (true, _) => Val::Decimal(x, scale),
                    (_, Kind::UInt) => u64::try_from(x).map_or(Val::Int128(x), Val::UInt),
                    (_, Kind::Int128) => Val::Int128(x),
                    _ => int_val(x),
                })
            })
            .collect(),
        Col::F64(v) => {
            let _ = n;
            v.into_iter().map(|f| Some(Val::Float(f))).collect()
        }
    }
}

#[inline]
fn apply_f(a: f64, b: f64, op: Op) -> f64 {
    match op {
        Op::Add => a + b,
        Op::Sub => a - b,
        Op::Mul => a * b,
    }
}

/// `i64` arithmetic; `None` on any overflow (the caller widens to `i128`).
fn max_abs(v: &[i64]) -> u128 {
    u128::from(simd::abs_bound_i64(v))
}

fn i64_op(x: &[i64], sx: u8, y: &[i64], sy: u8, op: Op, valid: &[u64]) -> Option<(Vec<i64>, u8)> {
    let n = x.len();
    let mut out = take_i64(n);
    // When the operands' magnitudes bound every result below 2^63, plain wrapping arithmetic
    // is exact for every row (including rows without a value): one vectorizable loop.
    let limit = i64::MAX as u128;
    match op {
        Op::Mul => {
            let s = sx.checked_add(sy).filter(|s| *s <= 18)?;
            if max_abs(x)
                .checked_mul(max_abs(y))
                .is_some_and(|m| m <= limit)
            {
                for ((o, a), b) in out.iter_mut().zip(x).zip(y) {
                    *o = a.wrapping_mul(*b);
                }
                return Some((out, s));
            }
            for i in 0..n {
                if bit(valid, i) {
                    out[i] = x[i].checked_mul(y[i])?;
                }
            }
            Some((out, s))
        }
        Op::Add | Op::Sub => {
            let s = sx.max(sy);
            let fx = 10i64.checked_pow(u32::from(s - sx))?;
            let fy = 10i64.checked_pow(u32::from(s - sy))?;
            let bound = max_abs(x)
                .checked_mul(fx as u128)
                .zip(max_abs(y).checked_mul(fy as u128))
                .and_then(|(a, b)| a.checked_add(b));
            if bound.is_some_and(|b| b <= limit) {
                let add = matches!(op, Op::Add);
                for ((o, a), b) in out.iter_mut().zip(x).zip(y) {
                    let (a, b) = (a.wrapping_mul(fx), b.wrapping_mul(fy));
                    *o = if add {
                        a.wrapping_add(b)
                    } else {
                        a.wrapping_sub(b)
                    };
                }
                return Some((out, s));
            }
            for i in 0..n {
                if bit(valid, i) {
                    let a = x[i].checked_mul(fx)?;
                    let b = y[i].checked_mul(fy)?;
                    out[i] = if matches!(op, Op::Add) { a.checked_add(b)? } else { a.checked_sub(b)? };
                }
            }
            Some((out, s))
        }
    }
}

fn wide_op(x: Col, y: Col, op: Op, valid: &[u64]) -> Result<Col> {
    let (Some((x, sx, dx)), Some((y, sy, dy))) = (to_i128(x), to_i128(y)) else {
        return Err(StorageError::invalid_value("arithmetic on non-numeric values"));
    };
    let mut out = vec![0i128; x.len()];
    let mut scale = 0u8;
    for i in 0..x.len() {
        if bit(valid, i) {
            let (m, s) = exact_op(x[i], sx, y[i], sy, op)?;
            out[i] = m;
            scale = s;
        }
    }
    if scale == 0 {
        // Every row had the same result scale (it depends on the operands' scales only).
        scale = match op {
            Op::Mul => sx + sy,
            _ => sx.max(sy),
        };
    }
    Ok(Col::I128 { v: out, scale, dec: dx || dy, kind: Kind::Int })
}

fn eval_expr(e: &Expr, rg: &RowGroupView<'_>, sel: &[u64]) -> Result<ColV> {
    let n = rg.n;
    Ok(match e {
        Expr::Field(p) => field_col(rg, p, sel),
        Expr::Const(v) => const_col(v, n),
        Expr::Add(a, b) => binop(eval_expr(a, rg, sel)?, eval_expr(b, rg, sel)?, Op::Add, n)?,
        Expr::Sub(a, b) => sub_op(eval_expr(a, rg, sel)?, eval_expr(b, rg, sel)?, n)?,
        Expr::Mul(a, b) => binop(eval_expr(a, rg, sel)?, eval_expr(b, rg, sel)?, Op::Mul, n)?,
        Expr::Neg(a) => sub_op(
            const_col(&PropertyValue::Int64(0), n),
            eval_expr(a, rg, sel)?,
            n,
        )?,
    })
}

/// Subtraction keeps operand order even on the row-by-row path.
fn sub_op(a: ColV, b: ColV, n: usize) -> Result<ColV> {
    if matches!(a.col, Col::Vals(_) | Col::Ts(_)) || matches!(b.col, Col::Vals(_) | Col::Ts(_)) {
        let mut valid = a.valid.clone();
        simd::and_words(&mut valid, &b.valid);
        let (xs, ys) = (col_vals(a.col, n), col_vals(b.col, n));
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            out.push(if bit(&valid, i) { arith_vals(xs[i].as_ref(), ys[i].as_ref(), Op::Sub)? } else { None });
        }
        return Ok(ColV { col: Col::Vals(out), valid });
    }
    binop(a, b, Op::Sub, n)
}

/// The value of row `i` as a document value.
fn prop_at(c: &Col, i: usize) -> PropertyValue {
    match c {
        Col::I64 { v, scale, dec, kind } => match (dec, kind) {
            (true, _) => Decimal::new(i128::from(v[i]), *scale).map_or(PropertyValue::Null, PropertyValue::Decimal),
            (_, Kind::Int128) => PropertyValue::Int128(i128::from(v[i])),
            _ => PropertyValue::Int64(v[i]),
        },
        Col::I128 { v, scale, dec, kind } => match (dec, kind) {
            (true, _) => Decimal::new(v[i], *scale).map_or(PropertyValue::Null, PropertyValue::Decimal),
            (_, Kind::UInt) => u64::try_from(v[i]).map_or(PropertyValue::Int128(v[i]), PropertyValue::UInt64),
            (_, Kind::Int128) => PropertyValue::Int128(v[i]),
            _ => int_val(v[i]).to_property(),
        },
        Col::F64(v) => PropertyValue::Float64(v[i]),
        Col::Ts(v) => PropertyValue::TimestampNs(v[i]),
        Col::Vals(v) => v[i].as_ref().map_or(PropertyValue::Null, Val::to_property),
    }
}

fn group_value_at(c: &Col, i: usize) -> GroupValue {
    match c {
        Col::I64 { v, scale, dec, .. } => GroupValue::of_val(Some(&if *dec { Val::Decimal(i128::from(v[i]), *scale) } else { Val::Int(v[i]) })),
        Col::I128 { v, scale, .. } => GroupValue::of_val(Some(&Val::Decimal(v[i], *scale))),
        Col::F64(v) => GroupValue::of_val(Some(&Val::Float(v[i]))),
        Col::Ts(v) => GroupValue::of_val(Some(&Val::Timestamp(v[i]))),
        Col::Vals(v) => GroupValue::of_val(v[i].as_ref()),
    }
}

// ── group keys ──

/// A group key over the rows of a row group: per-row codes into a small table (dictionary
/// chunks) or per-row values.
enum KeyCol {
    Codes(Vec<u32>, Vec<(GroupValue, PropertyValue)>),
    /// Plain integers or timestamps (`ts`), with their present rows.
    Ints { v: Vec<i64>, valid: Vec<u64>, ts: bool },
    Rows(Vec<(GroupValue, PropertyValue)>),
}

impl KeyCol {
    fn at(&self, i: usize) -> (GroupValue, PropertyValue) {
        match self {
            Self::Codes(c, e) => e[c[i] as usize].clone(),
            Self::Ints { v, valid, ts } => match (bit(valid, i), ts) {
                (false, _) => (GroupValue::Null, PropertyValue::Null),
                (true, true) => (GroupValue::Timestamp(v[i]), PropertyValue::TimestampNs(v[i])),
                (true, false) => (GroupValue::Exact(i128::from(v[i]), 0), PropertyValue::Int64(v[i])),
            },
            Self::Rows(r) => r[i].clone(),
        }
    }
}

fn bucket(v: &PropertyValue, width: i64, offset: i64) -> PropertyValue {
    let b = |x: i64| -> Option<i64> {
        let d = x.checked_sub(offset)?;
        d.div_euclid(width).checked_mul(width)?.checked_add(offset)
    };
    match v {
        PropertyValue::Int64(x) => b(*x).map_or(PropertyValue::Null, PropertyValue::Int64),
        PropertyValue::TimestampNs(x) => b(*x).map_or(PropertyValue::Null, PropertyValue::TimestampNs),
        PropertyValue::Float64(f) if f.is_finite() => {
            let w = width as f64;
            PropertyValue::Float64(((f - offset as f64) / w).floor() * w + offset as f64)
        }
        PropertyValue::Decimal(d) => {
            let w = pow10(u32::from(d.scale())).and_then(|p| i128::from(width).checked_mul(p));
            let o = pow10(u32::from(d.scale())).and_then(|p| i128::from(offset).checked_mul(p));
            match (w, o) {
                (Some(w), Some(o)) if w > 0 => {
                    let m = (d.mantissa() - o).div_euclid(w) * w + o;
                    Decimal::new(m, d.scale()).map_or(PropertyValue::Null, PropertyValue::Decimal)
                }
                _ => PropertyValue::Null,
            }
        }
        _ => PropertyValue::Null,
    }
}

fn gv_of_prop(v: &PropertyValue) -> GroupValue {
    GroupValue::of_val(Val::from_property(v).as_ref())
}

fn key_col(k: &GroupKey, rg: &RowGroupView<'_>, sel: &[u64]) -> KeyCol {
    let n = rg.n;
    let (path, bucket_by) = match k {
        GroupKey::Field(p) => (p.as_str(), None),
        GroupKey::Bucket { field, width, offset } => (field.as_str(), Some((*width, *offset))),
    };
    let entry = |v: PropertyValue| -> (GroupValue, PropertyValue) {
        let v = match bucket_by {
            Some((w, o)) => bucket(&v, w, o),
            None => v,
        };
        (gv_of_prop(&v), v)
    };
    // Dictionary chunks without nulls group by code: one key per dictionary entry.
    if bucket_by.is_none() && !rg.has_children(path) {
        if let Some(chunk) = rg.chunks.get(path) {
            if chunk.validity.is_none() {
                if let Body::Bytes(kind, storage_columnar::column::BytesBody::Dict { offsets, blob, codes }) = &chunk.body {
                    let entries: Vec<(GroupValue, PropertyValue)> = (0..offsets.len().saturating_sub(1))
                        .map(|e| {
                            let raw = blob.bytes().get(offsets[e] as usize..offsets[e + 1] as usize).unwrap_or_default();
                            let v = match kind {
                                Kind::Text => PropertyValue::Text(String::from_utf8_lossy(raw).into_owned()),
                                Kind::Bytes => PropertyValue::Bytes(raw.to_vec()),
                                _ => return entry(PropertyValue::Null),
                            };
                            entry(v)
                        })
                        .collect();
                    if matches!(kind, Kind::Text | Kind::Bytes) {
                        let last = entries.len().saturating_sub(1) as u64;
                        let mut c = Vec::with_capacity(n);
                        let mut buf = [0u64; 2048];
                        for start in (0..n).step_by(buf.len()) {
                            let k = (n - start).min(buf.len());
                            codes.unpack_into(start, &mut buf[..k]);
                            c.extend(buf[..k].iter().map(|&x| x.min(last) as u32));
                        }
                        return KeyCol::Codes(c, entries);
                    }
                }
            }
        }
    }
    if bucket_by.is_none() && !rg.has_children(path) {
        if let Some(chunk) = rg.chunks.get(path) {
            if let Body::Int(kind @ (Kind::Int | Kind::Timestamp), ic) = &chunk.body {
                let ts = *kind == Kind::Timestamp;
                // Integer dictionaries without nulls: one key per entry, as for text.
                if let (None, IntChunk::Dict { dict, codes }) = (&chunk.validity, ic) {
                    let entries: Vec<(GroupValue, PropertyValue)> = dict
                        .iter()
                        .map(|&x| if ts { (GroupValue::Timestamp(x), PropertyValue::TimestampNs(x)) } else { (GroupValue::Exact(i128::from(x), 0), PropertyValue::Int64(x)) })
                        .collect();
                    let last = entries.len().saturating_sub(1) as u64;
                    let mut c = Vec::with_capacity(n);
                    let mut buf = [0u64; 2048];
                    for start in (0..n).step_by(buf.len()) {
                        let k = (n - start).min(buf.len());
                        codes.unpack_into(start, &mut buf[..k]);
                        c.extend(buf[..k].iter().map(|&x| x.min(last) as u32));
                    }
                    return KeyCol::Codes(c, entries);
                }
                let col = field_col(rg, path, sel);
                if let Col::I64 { v, .. } | Col::Ts(v) = col.col {
                    return KeyCol::Ints { v, valid: col.valid, ts };
                }
            }
        }
    }
    let col = field_col(rg, path, sel);
    let rows = (0..n)
        .map(|i| {
            if bit(&col.valid, i) {
                let v = prop_at(&col.col, i);
                match bucket_by {
                    Some(_) => entry(v),
                    None => (group_value_at(&col.col, i), v),
                }
            } else {
                (GroupValue::Null, PropertyValue::Null)
            }
        })
        .collect();
    KeyCol::Rows(rows)
}

// ── per row group ──

/// Adds the columns `f` reads to `out`.
pub(crate) fn filter_paths_into(f: Option<&crate::document::Filter>, out: &mut Vec<String>) {
    use crate::document::Filter as F;
    let Some(f) = f else { return };
    match f {
        F::And(fs) | F::Or(fs) => fs.iter().for_each(|x| filter_paths_into(Some(x), out)),
        F::Not(x) => filter_paths_into(Some(x), out),
        F::KeyPrefix(_) => out.push(storage_columnar::segment::KEY_NAME.to_owned()),
        leaf => {
            if let Some(p) = leaf.path() {
                out.push(p.to_owned());
            }
        }
    }
}

fn needed_columns(q: &AggregateQuery) -> Vec<String> {
    fn expr_paths(e: &Expr, out: &mut Vec<String>) {
        match e {
            Expr::Field(p) => out.push(p.clone()),
            Expr::Const(_) => {}
            Expr::Add(a, b) | Expr::Sub(a, b) | Expr::Mul(a, b) => {
                expr_paths(a, out);
                expr_paths(b, out);
            }
            Expr::Neg(a) => expr_paths(a, out),
        }
    }
    let mut out = Vec::new();
    filter_paths_into(q.filter.as_ref(), &mut out);
    for k in &q.group_by {
        match k {
            GroupKey::Field(p) | GroupKey::Bucket { field: p, .. } => out.push(p.clone()),
        }
    }
    for a in &q.aggregates {
        if let Some(e) = &a.arg {
            expr_paths(e, &mut out);
        }
    }
    out.sort();
    out.dedup();
    out
}

/// Opens row group `g` of segment `si` and selects its live rows matching `filter`, then calls
/// `f` with the view and the selection. `None` when zone maps rule the group out (`pruned` is
/// incremented) or no live row remains.
pub(crate) fn with_row_group<R>(
    filter: Option<&crate::document::Filter>,
    snap: &ColumnarSnapshot<'_>,
    si: usize,
    g: usize,
    needed: &[String],
    pruned: &mut u64,
    f: impl FnOnce(&RowGroupView<'_>, Vec<u64>) -> Result<R>,
) -> Result<Option<R>> {
    let seg = &snap.segments[si].segment;
    let seal = snap.seal();
    let n = seg.row_groups()[g].rows as usize;
    // Zone maps rule out row groups before any chunk is read or decoded.
    if filter.is_some_and(|f| super::filter::prune(f, seg, g)) {
        *pruned += 1;
        return Ok(None);
    }
    let mut sel = snap.live_mask(si, g);
    if sel.iter().all(|w| *w == 0) {
        return Ok(None);
    }
    // Columns this query reads (map-valued paths need their nested columns too).
    let names: Vec<String> = seg.columns().iter().map(|c| c.name.clone()).collect();
    let mut wanted: Vec<usize> = Vec::new();
    for (i, name) in names.iter().enumerate() {
        let hit = needed.iter().any(|p| name == p || (name.len() > p.len() && name.starts_with(p.as_str()) && name.as_bytes()[p.len()] == b'.'));
        if hit {
            wanted.push(i);
        }
    }
    let mut bytes: Vec<(usize, Cow<'_, [u8]>)> = Vec::with_capacity(wanted.len());
    for &c in &wanted {
        if let Some(b) = seg.chunk_bytes(g, c, &seal)? {
            bytes.push((c, b));
        }
    }
    let mut chunks = FastHashMap::default();
    let mut decoded = FastHashMap::default();
    for (c, b) in &bytes {
        let name = names[*c].as_str();
        chunks.insert(name, Chunk::parse(b, n)?);
        decoded.insert(name, OnceCell::new());
    }
    let rg = RowGroupView { n, chunks, names: &names, decoded, keys: OnceCell::new() };
    if let Some(flt) = filter {
        let mut m = vec![0u64; sel.len()];
        eval(flt, &rg, &sel, &mut m);
        sel = m;
    }
    if sel.iter().all(|w| *w == 0) {
        return Ok(None);
    }
    f(&rg, sel).map(Some)
}

fn run_row_group(q: &AggregateQuery, snap: &ColumnarSnapshot<'_>, si: usize, g: usize, needed: &[String], st: &mut State<'_>) -> Result<()> {
    let mut pruned = 0;
    with_row_group(q.filter.as_ref(), snap, si, g, needed, &mut pruned, |rg, sel| aggregate_row_group(q, rg, sel, st))?;
    st.pruned += pruned;
    Ok(())
}

fn aggregate_row_group(q: &AggregateQuery, rg: &RowGroupView<'_>, sel: Vec<u64>, st: &mut State<'_>) -> Result<()> {
    let n = rg.n;
    let selected = simd::count_ones(&sel);
    if selected == 0 {
        return Ok(());
    }
    st.matched += selected;
    // Groups. Dictionary-coded keys with a small combined code space get a local slot per row
    // (`SINK` for rows outside the selection), so aggregates scatter into a few accumulators
    // without per-row hashing; other keys fall back to one group lookup per row.
    let keys: Vec<KeyCol> = q.group_by.iter().map(|k| key_col(k, rg, &sel)).collect();
    let mut local: Option<Local> = None;
    let mut gids: Option<Vec<u32>> = None;
    if !q.group_by.is_empty() {
        let all_codes = keys.iter().all(|k| matches!(k, KeyCol::Codes(..)));
        let space: usize = keys.iter().map(|k| if let KeyCol::Codes(_, e) = k { e.len().max(1) } else { 1 }).product();
        if all_codes && space <= 1 << 16 {
            local = local_slots(&keys, space, &sel, n, st)?;
        }
        if local.is_none() {
            let mut g = vec![0u32; n];
            match keys.as_slice() {
                // One dictionary key with too many entries for slots: one lookup per entry.
                [KeyCol::Codes(c, e)] => {
                    let mut table = vec![u32::MAX; e.len()];
                    for_each_set_try(&sel, |i| {
                        let code = c[i] as usize;
                        if table[code] == u32::MAX {
                            let (gv, pv) = e[code].clone();
                            table[code] = st.group(vec![gv], vec![pv])?;
                        }
                        g[i] = table[code];
                        Ok(())
                    })?;
                }
                [KeyCol::Ints { v, valid, ts }] => {
                    for_each_set_try(&sel, |i| {
                        g[i] = if bit(valid, i) { st.int_group(v[i], *ts)? } else { st.group(vec![GroupValue::Null], vec![PropertyValue::Null])? };
                        Ok(())
                    })?;
                }
                _ => {
                    for_each_set_try(&sel, |i| {
                        let mut key = Vec::with_capacity(keys.len());
                        let mut rep = Vec::with_capacity(keys.len());
                        for k in &keys {
                            let (gv, pv) = k.at(i);
                            key.push(gv);
                            rep.push(pv);
                        }
                        g[i] = st.group(key, rep)?;
                        Ok(())
                    })?;
                }
            }
            gids = Some(g);
        }
    }
    for (ai, a) in q.aggregates.iter().enumerate() {
        // COUNT(DISTINCT field) over a dictionary chunk: each (group, entry) pair once, without
        // decoding a value per row.
        if let (AggFunc::CountDistinct, Some(Expr::Field(p))) = (a.func, &a.arg) {
            if let KeyCol::Codes(codes, entries) = key_col(&GroupKey::Field(p.clone()), rg, &sel) {
                if gids.is_none() && !q.group_by.is_empty() {
                    let l = local.as_ref().ok_or_else(|| StorageError::invalid_value("internal: no groups"))?;
                    let mut g = vec![0u32; n];
                    for_each_set(&sel, |i| g[i] = l.gids[l.slot[i] as usize]);
                    gids = Some(g);
                }
                let mut pairs: IntSet<(u32, u32)> = IntSet::default();
                match &gids {
                    Some(g) => for_each_set(&sel, |i| {
                        pairs.insert((g[i], codes[i]));
                    }),
                    None => {
                        let mut seen = vec![false; entries.len()];
                        for_each_set(&sel, |i| seen[codes[i] as usize] = true);
                        pairs.extend(seen.iter().enumerate().filter(|(_, s)| **s).map(|(e, _)| (0, e as u32)));
                    }
                }
                if let Acc::Distinct(d) = &mut st.accs[ai] {
                    let mut added = 0usize;
                    for (g, e) in pairs {
                        let gv = &entries[e as usize].0;
                        if !matches!(gv, GroupValue::Null) && d[g as usize].insert(gv.clone()) {
                            added += 1;
                        }
                    }
                    st.budget.take(added * 64)?;
                }
                continue;
            }
        }
        let col = match &a.arg {
            Some(e) => Some(eval_expr(e, rg, &sel)?),
            None => None,
        };
        let mut rows = sel.clone();
        if let Some(c) = &col {
            simd::and_words(&mut rows, &c.valid);
        }
        let acc = &mut st.accs[ai];
        let done = match &local {
            Some(l) => update_local(acc, a.func, col.as_ref(), &rows, &sel, l, n),
            None => false,
        };
        if !done {
            if gids.is_none() && !q.group_by.is_empty() {
                // Row-by-row aggregate over slot-grouped rows: global ids from the slots.
                let l = local
                    .as_ref()
                    .ok_or_else(|| StorageError::invalid_value("internal: no groups"))?;
                let mut g = vec![0u32; n];
                for_each_set(&sel, |i| g[i] = l.gids[l.slot[i] as usize]);
                gids = Some(g);
            }
            update(
                acc,
                a.func,
                col.as_ref(),
                &rows,
                gids.as_deref(),
                n,
                st.budget,
            )?;
        }
        if let Some(c) = col {
            give_col(c);
        }
    }
    Ok(())
}

/// Most local slots per row group (more groups fall back to per-row lookups).
const MAX_SLOTS: usize = 255;
/// Slot of rows outside the selection.
const SINK: u8 = 255;

/// Row group-local grouping: the slot of every row and the global group id of every slot.
struct Local {
    slot: Vec<u8>,
    gids: Vec<u32>,
    /// Selected rows per slot.
    counts: Vec<u64>,
}

/// Calls `f(i)` for every set bit, stopping at the first error.
fn for_each_set_try(words: &[u64], mut f: impl FnMut(usize) -> Result<()>) -> Result<()> {
    for (w, &word) in words.iter().enumerate() {
        let mut m = word;
        while m != 0 {
            f(w * 64 + m.trailing_zeros() as usize)?;
            m &= m - 1;
        }
    }
    Ok(())
}

fn local_slots(
    keys: &[KeyCol],
    space: usize,
    sel: &[u64],
    n: usize,
    st: &mut State<'_>,
) -> Result<Option<Local>> {
    // Combined code of every row (mixed radix over the keys' dictionaries).
    let mut idx: Vec<u32> = Vec::new();
    for (i, k) in keys.iter().enumerate() {
        if let KeyCol::Codes(c, e) = k {
            if i == 0 {
                idx.extend_from_slice(&c[..n.min(c.len())]);
                idx.resize(n, 0);
                continue;
            }
            let radix = e.len().max(1) as u32;
            for (x, &code) in idx.iter_mut().zip(c) {
                *x = *x * radix + code;
            }
        }
    }
    let mut slot_of = vec![SINK; space];
    let mut gids: Vec<u32> = Vec::new();
    let mut slot = vec![SINK; n];
    let mut overflow = false;
    for_each_set_try(sel, |i| {
        if overflow {
            return Ok(());
        }
        let code = idx[i] as usize;
        if slot_of[code] == SINK {
            if gids.len() >= MAX_SLOTS {
                overflow = true;
                return Ok(());
            }
            // Decode the combined code back into each key's dictionary entry.
            let mut rest = code;
            let mut parts = vec![0usize; keys.len()];
            for (p, k) in parts.iter_mut().zip(keys).rev() {
                if let KeyCol::Codes(_, e) = k {
                    let radix = e.len().max(1);
                    *p = rest % radix;
                    rest /= radix;
                }
            }
            let mut key = Vec::with_capacity(keys.len());
            let mut rep = Vec::with_capacity(keys.len());
            for (k, p) in keys.iter().zip(&parts) {
                if let KeyCol::Codes(_, e) = k {
                    let (gv, pv) = &e[*p];
                    key.push(gv.clone());
                    rep.push(pv.clone());
                }
            }
            slot_of[code] = gids.len() as u8;
            gids.push(st.group(key, rep)?);
        }
        slot[i] = slot_of[code];
        Ok(())
    })?;
    if overflow {
        return Ok(None);
    }
    let counts = slot_counts(&slot, gids.len());
    Ok(Some(Local { slot, gids, counts }))
}

/// Rows per slot (the sink excluded), four interleaved counters breaking store-to-load chains.
fn slot_counts(slots: &[u8], groups: usize) -> Vec<u64> {
    let mut c = [[0u32; 256]; 4];
    for (i, &s) in slots.iter().enumerate() {
        c[i & 3][s as usize] += 1;
    }
    (0..groups).map(|k| c.iter().map(|a| u64::from(a[k])).sum()).collect()
}

/// Exact sum per slot. Narrow values (the bound proves no i64 overflow) accumulate in i64.
fn slot_sums(slots: &[u8], v: &[i64], groups: usize) -> Vec<i128> {
    let n = slots.len().min(v.len());
    let (slots, v) = (&slots[..n], &v[..n]);
    let narrow = u128::from(simd::abs_bound_i64(v)) * (n as u128) < 1u128 << 63;
    if narrow {
        let mut s = [[0i64; 256]; 4];
        for (i, (&k, &x)) in slots.iter().zip(v).enumerate() {
            s[i & 3][k as usize] += x;
        }
        (0..groups).map(|k| s.iter().map(|a| i128::from(a[k])).sum()).collect()
    } else {
        let mut s = [[0i128; 256]; 4];
        for (i, (&k, &x)) in slots.iter().zip(v).enumerate() {
            s[i & 3][k as usize] += i128::from(x);
        }
        (0..groups).map(|k| s.iter().map(|a| a[k]).sum()).collect()
    }
}

/// Count / sum / avg over slot-grouped rows: one branch-free scatter pass into a few
/// accumulators (four interleaved copies to break store-to-load dependencies). `false` when
/// the aggregate needs the row-by-row path.
fn update_local(
    acc: &mut Acc,
    func: AggFunc,
    col: Option<&ColV>,
    rows: &[u64],
    sel: &[u64],
    l: &Local,
    n: usize,
) -> bool {
    if !matches!(func, AggFunc::Count | AggFunc::Sum | AggFunc::Avg)
        || col.is_some_and(|c| !matches!(c.col, Col::I64 { .. }))
    {
        return false;
    }
    // Rows without a value go to the sink too.
    let masked;
    let slots: &[u8] = if rows == sel {
        &l.slot
    } else {
        let mut s = vec![SINK; n];
        for_each_set(rows, |i| s[i] = l.slot[i]);
        masked = s;
        &masked
    };
    let counts = if rows == sel { l.counts.clone() } else { slot_counts(slots, l.gids.len()) };
    let sums: Vec<i128> = match col.map(|c| &c.col) {
        Some(Col::I64 { v, .. }) if func != AggFunc::Count => slot_sums(slots, v, l.gids.len()),
        _ => Vec::new(),
    };
    for (k, &g) in l.gids.iter().enumerate() {
        let c = counts[k];
        if c == 0 {
            continue;
        }
        let g = g as usize;
        match (&mut *acc, col.map(|c| &c.col)) {
            (Acc::Count(cs), _) => cs[g] += c,
            (Acc::Sum(ss), Some(Col::I64 { scale, dec, .. })) => {
                ss[g].add_exact(sums[k], *scale, *dec)
            }
            (Acc::Avg(ss, cs), Some(Col::I64 { scale, dec, .. })) => {
                ss[g].add_exact(sums[k], *scale, *dec);
                cs[g] += c;
            }
            _ => {}
        }
    }
    true
}

fn update(
    acc: &mut Acc,
    func: AggFunc,
    col: Option<&ColV>,
    rows: &[u64],
    gids: Option<&[u32]>,
    n: usize,
    budget: &Budget,
) -> Result<()> {
    let gid = |i: usize| gids.map_or(0, |g| g[i]) as usize;
    match (acc, col) {
        (Acc::Count(c), _) => match gids {
            None => c[0] += simd::count_ones(rows),
            Some(g) => for_each_set(rows, |i| c[g[i] as usize] += 1),
        },
        (Acc::Sum(s), Some(col)) => sum_into(s, None, &col.col, rows, gids, n),
        (Acc::Avg(s, c), Some(col)) => sum_into(s, Some(c), &col.col, rows, gids, n),
        (Acc::Min(m), Some(col)) | (Acc::Max(m), Some(col)) => {
            let max = matches!(func, AggFunc::Max);
            // Typed fast path: one candidate per row group without grouping.
            if let (None, Col::I64 { v, .. } | Col::Ts(v)) = (gids, &col.col) {
                if let Some((lo, hi)) = simd::min_max_i64_masked(v, rows) {
                    let target = if max { hi } else { lo };
                    if let Some(i) = (0..n).find(|&i| bit(rows, i) && v[i] == target) {
                        offer_mm(&mut m[0], Some(prop_at(&col.col, i)), max);
                    }
                }
                return Ok(());
            }
            for i in 0..n {
                if bit(rows, i) {
                    offer_mm(&mut m[gid(i)], Some(prop_at(&col.col, i)), max);
                }
            }
        }
        (Acc::Distinct(d), Some(col)) => {
            let mut added = 0usize;
            match &col.col {
                // Plain integers and timestamps go into the i64 sets. With groups, the row
                // group's (group, value) pairs are sorted and deduplicated first: repeated values
                // cost no probe, and each group's set is filled in one stretch.
                Col::I64 { v, scale: 0, dec: false, .. } | Col::Ts(v) if gids.is_some() => {
                    let ts = matches!(col.col, Col::Ts(_));
                    let mut pairs: Vec<(u32, i64)> = Vec::with_capacity(simd::count_ones(rows) as usize);
                    for_each_set(rows, |i| pairs.push((gid(i) as u32, v[i])));
                    pairs.sort_unstable();
                    pairs.dedup();
                    for (g, x) in pairs {
                        let set = &mut d[g as usize];
                        added += usize::from(if ts { set.insert_stamp(x) } else { set.insert_int(x) });
                    }
                }
                Col::I64 { v, scale: 0, dec: false, .. } => for_each_set(rows, |i| added += usize::from(d[gid(i)].insert_int(v[i]))),
                Col::Ts(v) => for_each_set(rows, |i| added += usize::from(d[gid(i)].insert_stamp(v[i]))),
                c => for_each_set(rows, |i| added += usize::from(d[gid(i)].insert(group_value_at(c, i)))),
            }
            budget.take(added * 64)?;
        }
        (Acc::Approx(h), Some(col)) => {
            for i in 0..n {
                if bit(rows, i) && h[gid(i)].add(&group_value_at(&col.col, i)) {
                    budget.take(HLL_DENSE_BYTES)?;
                }
            }
        }
        _ => return Err(StorageError::invalid_value("this aggregate needs an argument")),
    }
    Ok(())
}

fn sum_into(s: &mut [SumAcc], mut counts: Option<&mut Vec<u64>>, col: &Col, rows: &[u64], gids: Option<&[u32]>, n: usize) {
    match (col, gids) {
        (Col::I64 { v, scale, dec, .. }, None) => {
            s[0].add_exact(simd::sum_i64_masked(v, rows), *scale, *dec);
            if let Some(c) = counts.as_deref_mut() {
                c[0] += simd::count_ones(rows);
            }
        }
        (Col::I64 { v, scale, dec, .. }, Some(g)) => {
            // Per-group partial sums for this row group, folded once per group.
            let groups = s.len();
            if groups <= 1 << 16 {
                let mut acc = vec![0i128; groups];
                let mut cnt = vec![0u64; groups];
                for_each_set(rows, |i| {
                    let k = g[i] as usize;
                    acc[k] += i128::from(v[i]);
                    cnt[k] += 1;
                });
                for k in 0..groups {
                    if cnt[k] > 0 {
                        s[k].add_exact(acc[k], *scale, *dec);
                        if let Some(c) = counts.as_deref_mut() {
                            c[k] += cnt[k];
                        }
                    }
                }
            } else {
                for_each_set(rows, |i| {
                    let k = g[i] as usize;
                    s[k].add_exact(i128::from(v[i]), *scale, *dec);
                    if let Some(c) = counts.as_deref_mut() {
                        c[k] += 1;
                    }
                });
            }
        }
        _ => {
            for i in 0..n {
                if !bit(rows, i) {
                    continue;
                }
                let k = gids.map_or(0, |g| g[i] as usize);
                match col {
                    Col::I128 { v, scale, dec, .. } => s[k].add_exact(v[i], *scale, *dec),
                    Col::F64(v) => s[k].add_float(v[i]),
                    Col::Vals(v) => match &v[i] {
                        Some(x) if num_of(x).is_some() => s[k].add_val(x),
                        _ => continue,
                    },
                    // Not numbers: skipped, as non-numeric values are.
                    Col::Ts(_) => continue,
                    Col::I64 { .. } => {}
                }
                if let Some(c) = counts.as_deref_mut() {
                    c[k] += 1;
                }
            }
        }
    }
}

// ── delta documents ──

fn doc_value(fields: &BTreeMap<String, PropertyValue>, path: &str) -> PropertyValue {
    let mut cur = match path.split('.').next().and_then(|h| fields.get(h)) {
        Some(v) => v,
        None => return PropertyValue::Null,
    };
    for part in path.split('.').skip(1) {
        match cur {
            PropertyValue::Map(m) => match m.get(part) {
                Some(v) => cur = v,
                None => return PropertyValue::Null,
            },
            _ => return PropertyValue::Null,
        }
    }
    cur.clone()
}

fn eval_doc(e: &Expr, fields: &BTreeMap<String, PropertyValue>) -> Result<Option<Val>> {
    Ok(match e {
        Expr::Field(p) => {
            let v = doc_value(fields, p);
            if matches!(v, PropertyValue::Null) {
                None
            } else {
                Val::from_property(&v)
            }
        }
        Expr::Const(v) => Val::from_property(v).filter(|_| !matches!(v, PropertyValue::Null)),
        Expr::Add(a, b) => arith_vals(eval_doc(a, fields)?.as_ref(), eval_doc(b, fields)?.as_ref(), Op::Add)?,
        Expr::Sub(a, b) => arith_vals(eval_doc(a, fields)?.as_ref(), eval_doc(b, fields)?.as_ref(), Op::Sub)?,
        Expr::Mul(a, b) => arith_vals(eval_doc(a, fields)?.as_ref(), eval_doc(b, fields)?.as_ref(), Op::Mul)?,
        Expr::Neg(a) => arith_vals(Some(&Val::Int(0)), eval_doc(a, fields)?.as_ref(), Op::Sub)?,
    })
}

fn doc_matches(q: &AggregateQuery, d: &Document) -> bool {
    let Some(f) = &q.filter else { return true };
    let mut buf = storage_codec::AlignedScratchBuf::with_capacity(512);
    let map = {
        let mut b = storage_codec::archive::ArchiveBuilder::new(&mut buf, 0);
        storage_codec::types::ArchivedMap::archive(&mut b, &d.fields)
    };
    f.matches(&d.key, map.view(buf.as_slice()))
}

/// Adds documents (delta, or a collection without segments) to `st`.
fn add_documents(q: &AggregateQuery, docs: &[Document], st: &mut State<'_>) -> Result<()> {
    for (i, d) in docs.iter().enumerate() {
        // Between documents no group id is outstanding: spilling is safe.
        if i % 1024 == 1023 && st.budget.pressed() {
            st.spill_out(q)?;
        }
        if !doc_matches(q, d) {
            continue;
        }
        st.matched += 1;
        let mut key = Vec::with_capacity(q.group_by.len());
        let mut rep = Vec::with_capacity(q.group_by.len());
        for k in &q.group_by {
            let v = match k {
                GroupKey::Field(p) => doc_value(&d.fields, p),
                GroupKey::Bucket { field, width, offset } => bucket(&doc_value(&d.fields, field), *width, *offset),
            };
            key.push(gv_of_prop(&v));
            rep.push(v);
        }
        let g = st.group(key, rep)? as usize;
        for (ai, a) in q.aggregates.iter().enumerate() {
            let v = match &a.arg {
                Some(e) => eval_doc(e, &d.fields)?,
                None => Some(Val::Bool(true)),
            };
            let v = v.filter(|x| !matches!(x, Val::Nested(b) if b.as_slice() == [storage_codec::value_wire::SVE_NULL]));
            let Some(v) = v else { continue };
            let budget = st.budget;
            match &mut st.accs[ai] {
                Acc::Count(c) => c[g] += 1,
                Acc::Sum(s) => {
                    if num_of(&v).is_some() {
                        s[g].add_val(&v);
                    }
                }
                Acc::Avg(s, c) => {
                    if num_of(&v).is_some() {
                        s[g].add_val(&v);
                        c[g] += 1;
                    }
                }
                Acc::Min(m) => offer_mm(&mut m[g], Some(v.to_property()), false),
                Acc::Max(m) => offer_mm(&mut m[g], Some(v.to_property()), true),
                Acc::Distinct(s) => {
                    if s[g].insert(GroupValue::of_val(Some(&v))) {
                        budget.take(64)?;
                    }
                }
                Acc::Approx(h) => {
                    if h[g].add(&GroupValue::of_val(Some(&v))) {
                        budget.take(HLL_DENSE_BYTES)?;
                    }
                }
            }
        }
    }
    Ok(())
}

// ── driver ──

fn validate(q: &AggregateQuery) -> Result<()> {
    if q.group_by.len() > MAX_QUERY_ITEMS || q.aggregates.len() > MAX_QUERY_ITEMS {
        return Err(StorageError::invalid_value("at most 64 group keys and 64 aggregates"));
    }
    for k in &q.group_by {
        if let GroupKey::Bucket { width, .. } = k {
            if *width <= 0 {
                return Err(StorageError::invalid_value("a bucket width must be positive"));
            }
        }
    }
    for a in &q.aggregates {
        if a.arg.is_none() && a.func != AggFunc::Count {
            return Err(StorageError::invalid_value("only count may omit its argument"));
        }
    }
    let width = q.group_by.len() + q.aggregates.len();
    if q.order_by.iter().any(|(i, _)| *i >= width) {
        return Err(StorageError::invalid_value("order_by refers to a column that is not in the result"));
    }
    Ok(())
}

/// Runs `q` over a columnar snapshot (segments in parallel, then the delta).
pub fn run(q: &AggregateQuery, snap: &ColumnarSnapshot<'_>) -> Result<AggregateResult> {
    validate(q)?;
    let mut budget = Budget::new(q, snap.spill_dir.as_deref());
    let needed = needed_columns(q);
    let morsels: Vec<(usize, usize)> = snap
        .segments
        .iter()
        .enumerate()
        .flat_map(|(si, s)| (0..s.segment.row_groups().len()).map(move |g| (si, g)))
        .collect();
    let cores = std::env::var("STORINE_THREADS")
        .ok()
        .and_then(|t| t.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, |n| n.get()));
    let mut threads = cores.min(morsels.len()).clamp(1, 16);
    // Spilling happens between row groups, so each worker may add one row group's groups past
    // the soft limit: fewer workers when that would exceed the budget, and the ceiling allows
    // that much (memory stays below twice the budget, or one row group's worth).
    let group_bytes = ROW_GROUP_ROWS.saturating_mul(row_bytes(q));
    let grows = !q.group_by.is_empty() || q.aggregates.iter().any(|a| matches!(a.func, AggFunc::CountDistinct | AggFunc::ApproxCountDistinct));
    if budget.spill_dir.is_some() && grows {
        threads = threads.min((budget.limit / group_bytes.max(1)).max(1));
    }
    budget.slack = budget.limit.max(group_bytes.saturating_mul(threads));
    let profile = std::env::var_os("STORINE_PROFILE").is_some();
    let started = std::time::Instant::now();
    let next = AtomicUsize::new(0);
    let results: Mutex<Vec<(usize, State<'_>)>> = Mutex::new(Vec::new());
    let failure: Mutex<Option<StorageError>> = Mutex::new(None);
    std::thread::scope(|scope| {
        for t in 0..threads {
            let (next, results, failure, needed, budget, morsels) = (&next, &results, &failure, &needed, &budget, &morsels);
            scope.spawn(move || {
                let run = || -> Result<State<'_>> {
                    let mut st = State::new(q, budget)?;
                    loop {
                        let m = next.fetch_add(1, Ordering::Relaxed);
                        let Some(&(si, g)) = morsels.get(m) else { break };
                        if failure.lock().unwrap_or_else(|p| p.into_inner()).is_some() {
                            break;
                        }
                        run_row_group(q, snap, si, g, needed, &mut st)?;
                        if budget.pressed() {
                            st.spill_out(q)?;
                        }
                    }
                    Ok(st)
                };
                match run() {
                    Ok(st) => results.lock().unwrap_or_else(|p| p.into_inner()).push((t, st)),
                    Err(e) => {
                        failure.lock().unwrap_or_else(|p| p.into_inner()).get_or_insert(e);
                    }
                }
            });
        }
    });
    if let Some(e) = failure.into_inner().unwrap_or_else(|p| p.into_inner()) {
        return Err(e);
    }
    let scanned = started.elapsed();
    let mut parts = results.into_inner().unwrap_or_else(|p| p.into_inner());
    parts.sort_by_key(|(t, _)| *t);
    // Many groups and no spill: merge and finish hash partitions in parallel.
    let groups: usize = parts.iter().map(|(_, st)| st.reps.len()).sum();
    if !q.group_by.is_empty() && groups >= PARALLEL_MERGE_GROUPS && budget.spilled().is_none() {
        let mut delta = State::new(q, &budget)?;
        add_documents(q, &snap.delta, &mut delta)?;
        parts.push((usize::MAX, delta));
        let out = merge_partitioned(q, parts.into_iter().map(|(_, st)| st).collect(), threads);
        if profile {
            eprintln!("[analytics] {} morsels on {threads} threads: scan {scanned:?}, partitioned merge+finish {:?}", morsels.len(), started.elapsed() - scanned);
        }
        return out;
    }
    // The first worker's state becomes the total (merging it into an empty one would only copy).
    let mut parts = parts.into_iter();
    let mut total = match parts.next() {
        Some((_, st)) => st,
        None => State::new(q, &budget)?,
    };
    for (_, st) in parts {
        total.merge(st)?;
        if budget.pressed() {
            total.spill_out(q)?;
        }
    }
    let merged = started.elapsed();
    add_documents(q, &snap.delta, &mut total)?;
    let out = if budget.spilled().is_some() { finish_spilled(q, total, &budget) } else { finish(q, total) };
    if profile {
        eprintln!("[analytics] {} morsels on {threads} threads: scan {scanned:?}, merge {:?}, delta+finish {:?}", morsels.len(), merged - scanned, started.elapsed() - merged);
    }
    out
}

/// Runs `q` over documents only (a collection without column segments).
pub fn run_documents(q: &AggregateQuery, docs: &[Document]) -> Result<AggregateResult> {
    validate(q)?;
    let budget = Budget::new(q, None);
    let mut st = State::new(q, &budget)?;
    add_documents(q, docs, &mut st)?;
    finish(q, st)
}

static MERGE_NS: AtomicUsize = AtomicUsize::new(0);
static FINISH_NS: AtomicUsize = AtomicUsize::new(0);

/// Group count from which worker states merge in parallel hash partitions.
const PARALLEL_MERGE_GROUPS: usize = 16_384;

/// Splits every state into hash partitions, then merges and finishes the partitions in parallel
/// (top-K per partition) and orders the rows as `finish` orders them. Partitions merge their
/// parts in input order, so the result does not depend on scheduling.
fn merge_partitioned<'b>(q: &AggregateQuery, parts: Vec<State<'b>>, threads: usize) -> Result<AggregateResult> {
    let matched = parts.iter().map(|st| st.matched).sum();
    let pruned = parts.iter().map(|st| st.pruned).sum();
    let n = parts.len();
    let t0 = std::time::Instant::now();
    let slots: Vec<Mutex<Option<State<'b>>>> = parts.into_iter().map(|st| Mutex::new(Some(st))).collect();
    let split: Vec<Mutex<Option<Vec<State<'b>>>>> = (0..n).map(|_| Mutex::new(None)).collect();
    let failure: Mutex<Option<StorageError>> = Mutex::new(None);
    let fail = |e: StorageError| {
        failure.lock().unwrap_or_else(|p| p.into_inner()).get_or_insert(e);
    };
    let next = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        for _ in 0..threads.min(n).max(1) {
            scope.spawn(|| loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= n {
                    break;
                }
                let Some(st) = slots[i].lock().unwrap_or_else(|p| p.into_inner()).take() else { continue };
                match st.split(q) {
                    Ok(p) => *split[i].lock().unwrap_or_else(|p| p.into_inner()) = Some(p),
                    Err(e) => fail(e),
                }
            });
        }
    });
    if let Some(e) = failure.lock().unwrap_or_else(|p| p.into_inner()).take() {
        return Err(e);
    }
    let t_split = t0.elapsed();
    // by_partition[p] = the parts' states of partition p, in part order.
    let mut by_partition: Vec<Vec<State<'b>>> = (0..PARTITIONS).map(|_| Vec::with_capacity(n)).collect();
    for s in split {
        for (p, st) in s.into_inner().unwrap_or_else(|p| p.into_inner()).unwrap_or_default().into_iter().enumerate() {
            by_partition[p].push(st);
        }
    }
    let work: Vec<Mutex<Vec<State<'b>>>> = by_partition.into_iter().map(Mutex::new).collect();
    let rows: Vec<Mutex<Vec<Vec<PropertyValue>>>> = (0..PARTITIONS).map(|_| Mutex::new(Vec::new())).collect();
    let next = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        for _ in 0..threads.max(1) {
            scope.spawn(|| loop {
                let p = next.fetch_add(1, Ordering::Relaxed);
                if p >= PARTITIONS {
                    break;
                }
                let states = core::mem::take(&mut *work[p].lock().unwrap_or_else(|e| e.into_inner()));
                let mut it = states.into_iter();
                let Some(mut total) = it.next() else { continue };
                let done = (|| -> Result<Vec<Vec<PropertyValue>>> {
                    let t = std::time::Instant::now();
                    for st in it {
                        total.merge(st)?;
                    }
                    MERGE_NS.fetch_add(t.elapsed().as_nanos() as usize, Ordering::Relaxed);
                    let t = std::time::Instant::now();
                    let r = finish(q, total)?.rows;
                    FINISH_NS.fetch_add(t.elapsed().as_nanos() as usize, Ordering::Relaxed);
                    Ok(r)
                })();
                match done {
                    Ok(r) => *rows[p].lock().unwrap_or_else(|e| e.into_inner()) = r,
                    Err(e) => fail(e),
                }
            });
        }
    });
    if let Some(e) = failure.into_inner().unwrap_or_else(|p| p.into_inner()) {
        return Err(e);
    }
    if std::env::var_os("STORINE_PROFILE").is_some() {
        eprintln!(
            "[analytics] split {t_split:?}, merge+finish {:?} (thread time: merge {} ms, finish {} ms)",
            t0.elapsed() - t_split,
            MERGE_NS.swap(0, Ordering::Relaxed) / 1_000_000,
            FINISH_NS.swap(0, Ordering::Relaxed) / 1_000_000
        );
    }
    let mut out: Vec<Vec<PropertyValue>> = Vec::new();
    for r in rows {
        out.extend(r.into_inner().unwrap_or_else(|p| p.into_inner()));
    }
    sort_rows(q, &mut out);
    Ok(AggregateResult { rows: out, matched, pruned_row_groups: pruned, spilled_bytes: 0 })
}

/// Finishes a query whose groups partly live in spill runs: every partition is merged and
/// finished on its own (top-K per partition), then the rows are ordered as `finish` orders them.
fn finish_spilled(q: &AggregateQuery, mut total: State<'_>, budget: &Budget) -> Result<AggregateResult> {
    total.spill_out(q)?;
    let spill = budget.spill()?;
    let (matched, pruned) = (total.matched, total.pruned);
    drop(total);
    let mut rows = Vec::new();
    for p in 0..PARTITIONS {
        let mut part = State::empty(q, budget);
        spill.read(p, |rec| part.load(rec))?;
        if !part.reps.is_empty() {
            rows.extend(finish(q, part)?.rows);
        }
    }
    sort_rows(q, &mut rows);
    Ok(AggregateResult { rows, matched, pruned_row_groups: pruned, spilled_bytes: spill.bytes_written() })
}

fn agg_value(acc: &Acc, g: usize) -> Result<PropertyValue> {
    Ok(match acc {
        Acc::Count(c) => PropertyValue::Int64(c[g] as i64),
        Acc::Sum(s) => s[g].sum()?,
        Acc::Avg(s, c) => s[g].avg(c[g])?,
        Acc::Min(m) | Acc::Max(m) => m[g].value.clone().unwrap_or(PropertyValue::Null),
        Acc::Distinct(d) => PropertyValue::Int64(d[g].len() as i64),
        Acc::Approx(h) => PropertyValue::Int64(h[g].estimate() as i64),
    })
}

fn finish(q: &AggregateQuery, st: State<'_>) -> Result<AggregateResult> {
    let groups = st.reps.len();
    let keys = q.group_by.len();
    // With a limit below the group count only the ORDER BY columns are computed for every
    // group; the first `limit` groups are selected by index and only their rows are built.
    let chosen: Vec<usize> = match q.limit {
        Some(l) if l < groups => {
            let mut ord: Vec<Option<Vec<PropertyValue>>> = vec![None; keys + st.accs.len()];
            for &(c, _) in &q.order_by {
                if c >= keys && ord[c].is_none() {
                    let acc = st.accs.get(c - keys).ok_or_else(|| StorageError::invalid_value("order column out of range"))?;
                    ord[c] = Some((0..groups).map(|g| agg_value(acc, g)).collect::<Result<_>>()?);
                }
            }
            let value = |c: usize, g: usize| -> &PropertyValue {
                match &ord[c] {
                    Some(v) => &v[g],
                    None => &st.reps.get(g)[c],
                }
            };
            let cmp = |&a: &usize, &b: &usize| {
                for &(c, dir) in &q.order_by {
                    let o = cmp_props(value(c, a), value(c, b));
                    let o = if dir == Direction::Desc { o.reverse() } else { o };
                    if o != core::cmp::Ordering::Equal {
                        return o;
                    }
                }
                for c in 0..keys {
                    let o = cmp_props(&st.reps.get(a)[c], &st.reps.get(b)[c]);
                    if o != core::cmp::Ordering::Equal {
                        return o;
                    }
                }
                core::cmp::Ordering::Equal
            };
            let mut idx: Vec<usize> = (0..groups).collect();
            if l == 0 {
                idx.clear();
            } else {
                idx.select_nth_unstable_by(l - 1, cmp);
                idx.truncate(l);
            }
            idx
        }
        _ => (0..groups).collect(),
    };
    let mut rows: Vec<Vec<PropertyValue>> = Vec::with_capacity(chosen.len());
    for g in chosen {
        let mut row = Vec::with_capacity(keys + st.accs.len());
        row.extend_from_slice(st.reps.get(g));
        for acc in &st.accs {
            row.push(agg_value(acc, g)?);
        }
        rows.push(row);
    }
    sort_rows(q, &mut rows);
    Ok(AggregateResult { rows, matched: st.matched, pruned_row_groups: st.pruned, spilled_bytes: 0 })
}

/// Orders result rows by `order_by`, then by group keys, and applies the limit.
fn sort_rows(q: &AggregateQuery, rows: &mut Vec<Vec<PropertyValue>>) {
    let keys = q.group_by.len();
    let by_keys = |a: &Vec<PropertyValue>, b: &Vec<PropertyValue>| {
        for i in 0..keys {
            let o = cmp_props(&a[i], &b[i]);
            if o != core::cmp::Ordering::Equal {
                return o;
            }
        }
        core::cmp::Ordering::Equal
    };
    let cmp = |a: &Vec<PropertyValue>, b: &Vec<PropertyValue>| {
        for (i, dir) in &q.order_by {
            let o = cmp_props(&a[*i], &b[*i]);
            let o = if *dir == Direction::Desc { o.reverse() } else { o };
            if o != core::cmp::Ordering::Equal {
                return o;
            }
        }
        by_keys(a, b)
    };
    // Top-K: select the first `limit` rows, then sort only those. Group keys are unique, so
    // the order is total and the result is the same as a full sort.
    if let Some(l) = q.limit {
        if l == 0 {
            rows.clear();
            return;
        }
        if l < rows.len() {
            rows.select_nth_unstable_by(l - 1, cmp);
            rows.truncate(l);
        }
    }
    rows.sort_by(cmp);
}
