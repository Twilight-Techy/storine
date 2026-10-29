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
use storage_columnar::{Body, Chunk, Kind, Val};
use storage_types::{Decimal, FastHashMap, FastHashSet, Result, StorageError};

use crate::columnar::ColumnarSnapshot;
use crate::document::{Direction, Document};

use super::filter::{eval, RowGroupView};
use super::values::{cmp_props, pow10, GroupValue, Hll, MinMax, SumAcc};
use super::{AggFunc, AggregateQuery, AggregateResult, Expr, GroupKey, DEFAULT_MEMORY_BUDGET, MAX_QUERY_ITEMS};

// ── per-thread buffers ──

thread_local! {
    /// Column buffers reused across row groups: no allocation (and no page faults on fresh
    /// mappings) per column per row group.
    static POOL: core::cell::RefCell<Vec<Vec<i64>>> = const { core::cell::RefCell::new(Vec::new()) };
}

/// A buffer of `n` values (contents unspecified: every caller overwrites it).
fn take_i64(n: usize) -> Vec<i64> {
    let mut v = POOL.with(|p| p.borrow_mut().pop()).unwrap_or_default();
    if v.len() < n {
        v.resize(n, 0);
    } else {
        v.truncate(n);
    }
    v
}

fn give_i64(v: Vec<i64>) {
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
    Distinct(Vec<FastHashSet<GroupValue>>),
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
                v.push(FastHashSet::default());
                48
            }
            Self::Approx(v) => {
                v.push(Hll::default());
                1 << 14
            }
        }
    }
}

/// Group table and accumulators of one worker (or of the final result).
struct State<'b> {
    groups: FastHashMap<Vec<GroupValue>, u32>,
    reps: Vec<Vec<PropertyValue>>,
    accs: Vec<Acc>,
    matched: u64,
    budget: &'b Budget,
}

#[derive(Debug)]
struct Budget {
    used: AtomicUsize,
    limit: usize,
}

impl Budget {
    fn take(&self, n: usize) -> Result<()> {
        let now = self.used.fetch_add(n, Ordering::Relaxed) + n;
        if now > self.limit {
            return Err(StorageError::invalid_value(
                "the query needs more memory than its budget: narrow it with a filter, group by fewer values, or raise memory_budget",
            ));
        }
        Ok(())
    }
}

impl<'b> State<'b> {
    fn new(q: &AggregateQuery, budget: &'b Budget) -> Result<Self> {
        let mut s = Self {
            groups: FastHashMap::default(),
            reps: Vec::new(),
            accs: q.aggregates.iter().map(|a| Acc::new(a.func)).collect(),
            matched: 0,
            budget,
        };
        if q.group_by.is_empty() {
            s.group(Vec::new(), Vec::new())?;
        }
        Ok(s)
    }

    fn group(&mut self, key: Vec<GroupValue>, rep: Vec<PropertyValue>) -> Result<u32> {
        if let Some(&g) = self.groups.get(&key) {
            let cur = &mut self.reps[g as usize];
            for (c, r) in cur.iter_mut().zip(rep) {
                if *c != r {
                    *c = det_pick(core::mem::replace(c, PropertyValue::Null), r);
                }
            }
            return Ok(g);
        }
        let g = self.reps.len() as u32;
        let mut size = key.iter().map(GroupValue::size).sum::<usize>() + 64;
        for a in &mut self.accs {
            size += a.push();
        }
        self.budget.take(size)?;
        self.groups.insert(key, g);
        self.reps.push(rep);
        Ok(g)
    }

    fn merge(&mut self, other: State<'_>) -> Result<()> {
        self.matched += other.matched;
        let mut order: Vec<(Vec<GroupValue>, u32)> = other.groups.into_iter().collect();
        order.sort_by_key(|(_, g)| *g);
        let mut other_reps = other.reps;
        let mut map = vec![0u32; other_reps.len()];
        for (key, og) in order {
            let rep = core::mem::take(&mut other_reps[og as usize]);
            map[og as usize] = self.group(key, rep)?;
        }
        for (mine, theirs) in self.accs.iter_mut().zip(other.accs) {
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
                        a[map[i] as usize].extend(set);
                    }
                }
                (Acc::Approx(a), Acc::Approx(b)) => b.iter().enumerate().for_each(|(i, x)| a[map[i] as usize].merge(x)),
                _ => {}
            }
        }
        Ok(())
    }
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
        Col::Vals(_) => vec![f64::NAN; n],
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

fn col_vals(c: Col, n: usize) -> Vec<Option<Val>> {
    match c {
        Col::Vals(v) => v,
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
    u128::from(v.iter().fold(0u64, |m, &x| m.max(x.unsigned_abs())))
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
    if matches!(a.col, Col::Vals(_)) || matches!(b.col, Col::Vals(_)) {
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
        Col::Vals(v) => v[i].as_ref().map_or(PropertyValue::Null, Val::to_property),
    }
}

fn group_value_at(c: &Col, i: usize) -> GroupValue {
    match c {
        Col::I64 { v, scale, dec, .. } => GroupValue::of_val(Some(&if *dec { Val::Decimal(i128::from(v[i]), *scale) } else { Val::Int(v[i]) })),
        Col::I128 { v, scale, .. } => GroupValue::of_val(Some(&Val::Decimal(v[i], *scale))),
        Col::F64(v) => GroupValue::of_val(Some(&Val::Float(v[i]))),
        Col::Vals(v) => GroupValue::of_val(v[i].as_ref()),
    }
}

// ── group keys ──

/// A group key over the rows of a row group: per-row codes into a small table (dictionary
/// chunks) or per-row values.
enum KeyCol {
    Codes(Vec<u32>, Vec<(GroupValue, PropertyValue)>),
    Rows(Vec<(GroupValue, PropertyValue)>),
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
                        let mut c = vec![0u64; n];
                        codes.unpack_into(0, &mut c);
                        let last = entries.len().saturating_sub(1) as u64;
                        return KeyCol::Codes(c.into_iter().map(|x| x.min(last) as u32).collect(), entries);
                    }
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
    fn filter_paths(f: &crate::document::Filter, out: &mut Vec<String>) {
        use crate::document::Filter as F;
        match f {
            F::And(fs) | F::Or(fs) => fs.iter().for_each(|x| filter_paths(x, out)),
            F::Not(x) => filter_paths(x, out),
            F::KeyPrefix(_) => out.push(storage_columnar::segment::KEY_NAME.to_owned()),
            leaf => {
                if let Some(p) = leaf.path() {
                    out.push(p.to_owned());
                }
            }
        }
    }
    let mut out = Vec::new();
    if let Some(f) = &q.filter {
        filter_paths(f, &mut out);
    }
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

fn run_row_group(q: &AggregateQuery, snap: &ColumnarSnapshot<'_>, si: usize, g: usize, needed: &[String], st: &mut State<'_>) -> Result<()> {
    let seg = &snap.segments[si].segment;
    let seal = snap.seal();
    let n = seg.row_groups()[g].rows as usize;
    let mut sel = snap.live_mask(si, g);
    if sel.iter().all(|w| *w == 0) {
        return Ok(());
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
    if let Some(f) = &q.filter {
        let mut m = vec![0u64; sel.len()];
        eval(f, &rg, &sel, &mut m);
        sel = m;
    }
    let selected = simd::count_ones(&sel);
    if selected == 0 {
        return Ok(());
    }
    st.matched += selected;
    // Group id of every selected row.
    // Groups. Dictionary-coded keys with a small combined code space get a local slot per row
    // (`SINK` for rows outside the selection), so aggregates scatter into a few accumulators
    // without per-row hashing; other keys fall back to one group lookup per row.
    let keys: Vec<KeyCol> = q.group_by.iter().map(|k| key_col(k, &rg, &sel)).collect();
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
            for_each_set_try(&sel, |i| {
                let mut key = Vec::with_capacity(keys.len());
                let mut rep = Vec::with_capacity(keys.len());
                for k in &keys {
                    let (gv, pv) = match k {
                        KeyCol::Codes(c, e) => e[c[i] as usize].clone(),
                        KeyCol::Rows(r) => r[i].clone(),
                    };
                    key.push(gv);
                    rep.push(pv);
                }
                g[i] = st.group(key, rep)?;
                Ok(())
            })?;
            gids = Some(g);
        }
    }
    for (ai, a) in q.aggregates.iter().enumerate() {
        let col = match &a.arg {
            Some(e) => Some(eval_expr(e, &rg, &sel)?),
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
    let mut idx = vec![0u32; n];
    for k in keys {
        if let KeyCol::Codes(c, e) = k {
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
    Ok((!overflow).then_some(Local { slot, gids }))
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
    let mut counts = [[0u64; 256]; 4];
    let mut sums = [[0i128; 256]; 4];
    match col.map(|c| &c.col) {
        Some(Col::I64 { v, .. }) => {
            for (i, (&s, &x)) in slots.iter().zip(v.iter()).enumerate() {
                sums[i & 3][s as usize] += i128::from(x);
                counts[i & 3][s as usize] += 1;
            }
        }
        _ => {
            for (i, &s) in slots.iter().enumerate() {
                counts[i & 3][s as usize] += 1;
            }
        }
    }
    for (k, &g) in l.gids.iter().enumerate() {
        let c: u64 = counts.iter().map(|a| a[k]).sum();
        if c == 0 {
            continue;
        }
        let g = g as usize;
        match (&mut *acc, col.map(|c| &c.col)) {
            (Acc::Count(cs), _) => cs[g] += c,
            (Acc::Sum(ss), Some(Col::I64 { scale, dec, .. })) => {
                ss[g].add_exact(sums.iter().map(|a| a[k]).sum(), *scale, *dec)
            }
            (Acc::Avg(ss, cs), Some(Col::I64 { scale, dec, .. })) => {
                ss[g].add_exact(sums.iter().map(|a| a[k]).sum(), *scale, *dec);
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
            if let (None, Col::I64 { v, .. }) = (gids, &col.col) {
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
            for i in 0..n {
                if bit(rows, i) && d[gid(i)].insert(group_value_at(&col.col, i)) {
                    budget.take(64)?;
                }
            }
        }
        (Acc::Approx(h), Some(col)) => {
            for i in 0..n {
                if bit(rows, i) {
                    h[gid(i)].add(&group_value_at(&col.col, i));
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
    for d in docs {
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
                Acc::Approx(h) => h[g].add(&GroupValue::of_val(Some(&v))),
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
    let budget = Budget { used: AtomicUsize::new(0), limit: if q.memory_budget == 0 { DEFAULT_MEMORY_BUDGET } else { q.memory_budget } };
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
    let threads = cores.min(morsels.len()).clamp(1, 16);
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
    let mut total = State::new(q, &budget)?;
    for (_, st) in parts {
        total.merge(st)?;
    }
    let merged = started.elapsed();
    add_documents(q, &snap.delta, &mut total)?;
    let out = finish(q, total);
    if profile {
        eprintln!("[analytics] {} morsels on {threads} threads: scan {scanned:?}, merge {:?}, delta+finish {:?}", morsels.len(), merged - scanned, started.elapsed() - merged);
    }
    out
}

/// Runs `q` over documents only (a collection without column segments).
pub fn run_documents(q: &AggregateQuery, docs: &[Document]) -> Result<AggregateResult> {
    validate(q)?;
    let budget = Budget { used: AtomicUsize::new(0), limit: if q.memory_budget == 0 { DEFAULT_MEMORY_BUDGET } else { q.memory_budget } };
    let mut st = State::new(q, &budget)?;
    add_documents(q, docs, &mut st)?;
    finish(q, st)
}

fn finish(q: &AggregateQuery, st: State<'_>) -> Result<AggregateResult> {
    let groups = st.reps.len();
    let mut rows: Vec<Vec<PropertyValue>> = Vec::with_capacity(groups);
    for g in 0..groups {
        let mut row = st.reps[g].clone();
        for acc in &st.accs {
            row.push(match acc {
                Acc::Count(c) => PropertyValue::Int64(c[g] as i64),
                Acc::Sum(s) => s[g].sum()?,
                Acc::Avg(s, c) => s[g].avg(c[g])?,
                Acc::Min(m) | Acc::Max(m) => m[g].value.clone().unwrap_or(PropertyValue::Null),
                Acc::Distinct(d) => PropertyValue::Int64(d[g].len() as i64),
                Acc::Approx(h) => PropertyValue::Int64(h[g].estimate() as i64),
            });
        }
        rows.push(row);
    }
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
    rows.sort_by(|a, b| {
        for (i, dir) in &q.order_by {
            let o = cmp_props(&a[*i], &b[*i]);
            let o = if *dir == Direction::Desc { o.reverse() } else { o };
            if o != core::cmp::Ordering::Equal {
                return o;
            }
        }
        by_keys(a, b)
    });
    if let Some(l) = q.limit {
        rows.truncate(l);
    }
    Ok(AggregateResult { rows, matched: st.matched })
}
