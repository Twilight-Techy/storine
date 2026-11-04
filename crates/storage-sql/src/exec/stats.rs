//! Table statistics (`F-ENT-04`, `TASK-148`): what `ANALYZE` measures, for the optimizer.
//!
//! One pass over a table's rows gives the row count, each column's null fraction and distinct
//! count (HyperLogLog over every row), and from a reservoir sample of up to [`SAMPLE_ROWS`] rows
//! an equi-depth histogram and the most common values. Statistics are documents (`_sql_stats`), so
//! every gateway plans from the same numbers.
//!
//! They only ever change plans, not results. Without statistics a table keeps its written join
//! order.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use storine::{Doc, Value};

use crate::catalog::Table;
use crate::datum::{to_f64, Datum};
use crate::error::SqlResult;
use crate::exec::{Engine, Session};
use crate::store::{Op, Pre, Tag, Write};

pub const STATS: &str = "_sql_stats";
/// Rows sampled per table for histograms and common values.
pub const SAMPLE_ROWS: usize = 30_000;
const HISTOGRAM_BUCKETS: usize = 100;
const COMMON_VALUES: usize = 10;
/// How long a gateway plans from statistics it has read.
const STATS_TTL: Duration = Duration::from_secs(5);

/// HyperLogLog with 2^14 registers: about 0.8% error in 16 KiB per column.
#[derive(Debug, Clone)]
pub struct HyperLogLog {
    registers: Vec<u8>,
}

const HLL_BITS: u32 = 14;

impl Default for HyperLogLog {
    fn default() -> Self {
        HyperLogLog { registers: vec![0; 1 << HLL_BITS] }
    }
}

/// A 64-bit hash (FNV-1a, then a finalizer to spread short keys over the high bits).
fn hash64(bytes: &[u8]) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^ (h >> 33)
}

impl HyperLogLog {
    pub fn add(&mut self, bytes: &[u8]) {
        let h = hash64(bytes);
        let index = (h >> (64 - HLL_BITS)) as usize;
        let rest = (h << HLL_BITS) | (1u64 << (HLL_BITS - 1));
        let rank = rest.leading_zeros() as u8 + 1;
        if rank > self.registers[index] {
            self.registers[index] = rank;
        }
    }

    pub fn estimate(&self) -> f64 {
        let m = self.registers.len() as f64;
        let alpha = 0.7213 / (1.0 + 1.079 / m);
        let sum: f64 = self.registers.iter().map(|r| 2f64.powi(-i32::from(*r))).sum();
        let raw = alpha * m * m / sum;
        let zeros = self.registers.iter().filter(|r| **r == 0).count();
        if raw <= 2.5 * m && zeros > 0 {
            // Small range: linear counting.
            m * (m / zeros as f64).ln()
        } else {
            raw
        }
    }
}

/// What is known about one column.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ColumnStats {
    pub null_frac: f64,
    pub distinct: f64,
    /// Equi-depth bucket bounds of numeric, temporal and boolean values, ascending.
    pub numeric_bounds: Vec<f64>,
    /// Equi-depth bucket bounds of text values, ascending.
    pub text_bounds: Vec<String>,
    /// Most common values (as text) and their fraction of all rows.
    pub common: Vec<(String, f64)>,
}

/// What is known about one table.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TableStats {
    pub rows: f64,
    pub columns: HashMap<String, ColumnStats>,
}

fn datum_key(d: &Datum) -> String {
    d.as_text()
}

impl ColumnStats {
    /// Fraction of rows equal to `value`.
    pub fn eq_selectivity(&self, value: Option<&Datum>) -> f64 {
        if let Some(v) = value {
            let k = datum_key(v);
            if let Some((_, f)) = self.common.iter().find(|(c, _)| *c == k) {
                return *f;
            }
            // Spread what the common values leave over the other distinct values.
            let common: f64 = self.common.iter().map(|c| c.1).sum();
            let others = (self.distinct - self.common.len() as f64).max(1.0);
            return ((1.0 - self.null_frac - common).max(0.0) / others).clamp(0.0, 1.0);
        }
        ((1.0 - self.null_frac) / self.distinct.max(1.0)).clamp(0.0, 1.0)
    }

    /// Fraction of non-null rows below `v`, by the histogram.
    fn fraction_below(&self, v: &Datum) -> Option<f64> {
        if let (Some(x), false) = (to_f64(v).filter(|_| !matches!(v, Datum::Text(_))), self.numeric_bounds.is_empty()) {
            return Some(position(&self.numeric_bounds, |b| b.partial_cmp(&x).unwrap_or(std::cmp::Ordering::Less), |a, b| if b > a { (x - a) / (b - a) } else { 0.5 }));
        }
        if let (Datum::Text(s), false) = (v, self.text_bounds.is_empty()) {
            return Some(position(&self.text_bounds, |b| b.as_str().cmp(s.as_str()), |_, _| 0.5));
        }
        None
    }

    /// Fraction of rows in the open range `(lo, hi)`.
    pub fn range_selectivity(&self, lo: Option<&Datum>, hi: Option<&Datum>) -> Option<f64> {
        let low = match lo {
            Some(v) => self.fraction_below(v)?,
            None => 0.0,
        };
        let high = match hi {
            Some(v) => self.fraction_below(v)?,
            None => 1.0,
        };
        Some(((high - low).max(0.0) * (1.0 - self.null_frac)).clamp(0.0, 1.0))
    }
}

/// Where a value falls among ascending bounds, as a fraction of the buckets.
fn position<T>(bounds: &[T], cmp: impl Fn(&T) -> std::cmp::Ordering, within: impl Fn(&T, &T) -> f64) -> f64 {
    let n = bounds.len();
    if n < 2 {
        return 0.5;
    }
    let after = bounds.partition_point(|b| cmp(b) == std::cmp::Ordering::Less);
    if after == 0 {
        return 0.0;
    }
    if after >= n {
        return 1.0;
    }
    let inside = within(&bounds[after - 1], &bounds[after]).clamp(0.0, 1.0);
    ((after - 1) as f64 + inside) / (n - 1) as f64
}

/// Measures `table` and stores the result.
pub fn analyze_table(s: &Session, table: &Table) -> SqlResult<TableStats> {
    let n = table.columns.len();
    let mut hll = vec![HyperLogLog::default(); n];
    let mut nulls = vec![0u64; n];
    let mut sample: Vec<Vec<Datum>> = Vec::new();
    let mut rows = 0u64;
    // A small deterministic generator: statistics need no real randomness.
    let mut seed = 0x9E37_79B9_7F4A_7C15u64;
    let control = s.control.clone();
    s.engine.store.scan_while(&table.collection, None, |d| {
        control.check()?;
        let row = table.from_doc(&d.fields);
        rows += 1;
        for (i, v) in row.iter().enumerate() {
            if v.is_null() {
                nulls[i] += 1;
            } else {
                hll[i].add(datum_key(v).as_bytes());
            }
        }
        if sample.len() < SAMPLE_ROWS {
            sample.push(row);
        } else {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let at = seed % rows;
            if (at as usize) < SAMPLE_ROWS {
                sample[at as usize] = row;
            }
        }
        Ok(true)
    })?;
    let mut stats = TableStats { rows: rows as f64, columns: HashMap::new() };
    for (i, c) in table.columns.iter().enumerate() {
        let mut col = ColumnStats { null_frac: if rows == 0 { 0.0 } else { nulls[i] as f64 / rows as f64 }, distinct: hll[i].estimate().round().min((rows - nulls[i]) as f64), ..ColumnStats::default() };
        let values: Vec<&Datum> = sample.iter().map(|r| &r[i]).filter(|v| !v.is_null()).collect();
        let mut counts: HashMap<String, u64> = HashMap::new();
        for v in &values {
            *counts.entry(datum_key(v)).or_default() += 1;
        }
        let mut common: Vec<(String, u64)> = counts.into_iter().filter(|(_, n)| *n > 1).collect();
        common.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        let scale = (1.0 - col.null_frac) / values.len().max(1) as f64;
        col.common = common.into_iter().take(COMMON_VALUES).map(|(k, n)| (k, n as f64 * scale)).collect();
        // Long text helps no estimate and would bloat the document.
        col.common.retain(|(k, _)| k.len() <= 128);
        let mut numbers: Vec<f64> = values.iter().filter(|v| !matches!(v, Datum::Text(_) | Datum::Json(_))).filter_map(|v| to_f64(v)).filter(|x| x.is_finite()).collect();
        if !numbers.is_empty() {
            numbers.sort_by(f64::total_cmp);
            col.numeric_bounds = equi_depth(&numbers);
        }
        let mut texts: Vec<String> = values.iter().filter_map(|v| if let Datum::Text(t) = v { Some(t.chars().take(64).collect()) } else { None }).collect();
        if !texts.is_empty() {
            texts.sort();
            col.text_bounds = equi_depth(&texts);
        }
        stats.columns.insert(c.name.to_ascii_lowercase(), col);
    }
    let w = Write { collection: STATS.into(), key: table.name.to_ascii_lowercase(), op: Op::Put(stats.to_doc()), pre: Pre::None, tag: Tag::Internal };
    s.engine.store.commit(std::slice::from_ref(&w)).map_err(|e| e.to_sql(std::slice::from_ref(&w)))?;
    s.engine.forget_stats();
    Ok(stats)
}

fn equi_depth<T: Clone>(sorted: &[T]) -> Vec<T> {
    let buckets = HISTOGRAM_BUCKETS.min(sorted.len().saturating_sub(1)).max(1);
    (0..=buckets).map(|i| sorted[(i * (sorted.len() - 1)) / buckets].clone()).collect()
}

impl TableStats {
    fn to_doc(&self) -> Doc {
        let mut columns = Doc::new();
        for (name, c) in &self.columns {
            let mut m = Doc::new();
            m.insert("null_frac".into(), Value::Float(c.null_frac));
            m.insert("distinct".into(), Value::Float(c.distinct));
            m.insert("numeric_bounds".into(), Value::List(c.numeric_bounds.iter().map(|x| Value::Float(*x)).collect()));
            m.insert("text_bounds".into(), Value::List(c.text_bounds.iter().map(|x| Value::Text(x.clone())).collect()));
            m.insert("common_values".into(), Value::List(c.common.iter().map(|x| Value::Text(x.0.clone())).collect()));
            m.insert("common_fractions".into(), Value::List(c.common.iter().map(|x| Value::Float(x.1)).collect()));
            columns.insert(name.clone(), Value::Map(m));
        }
        let mut d = Doc::new();
        d.insert("rows".into(), Value::Float(self.rows));
        d.insert("columns".into(), Value::Map(columns));
        d
    }

    fn from_doc(d: &Doc) -> TableStats {
        let num = |m: &Doc, k: &str| m.get(k).and_then(Value::as_f64).unwrap_or(0.0);
        let mut out = TableStats { rows: num(d, "rows"), columns: HashMap::new() };
        if let Some(cs) = d.get("columns").and_then(Value::as_map) {
            for (name, v) in cs {
                let Some(m) = v.as_map() else { continue };
                let floats = |k: &str| -> Vec<f64> {
                    match m.get(k) {
                        Some(Value::List(l)) => l.iter().filter_map(Value::as_f64).collect(),
                        _ => Vec::new(),
                    }
                };
                let texts = |k: &str| -> Vec<String> {
                    match m.get(k) {
                        Some(Value::List(l)) => l.iter().filter_map(Value::as_str).map(str::to_owned).collect(),
                        _ => Vec::new(),
                    }
                };
                let common = texts("common_values").into_iter().zip(floats("common_fractions")).collect();
                out.columns.insert(name.clone(), ColumnStats { null_frac: num(m, "null_frac"), distinct: num(m, "distinct"), numeric_bounds: floats("numeric_bounds"), text_bounds: texts("text_bounds"), common });
            }
        }
        out
    }
}

impl Engine {
    /// Statistics of every analyzed table, as last read.
    pub fn stats(&self) -> SqlResult<Arc<HashMap<String, TableStats>>> {
        {
            let c = self.stats_cache.read().unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some((at, s)) = c.as_ref() {
                if at.elapsed() < STATS_TTL {
                    return Ok(Arc::clone(s));
                }
            }
        }
        let mut map = HashMap::new();
        for d in self.store.scan(STATS, None)? {
            map.insert(d.key.clone(), TableStats::from_doc(&d.fields));
        }
        let map = Arc::new(map);
        *self.stats_cache.write().unwrap_or_else(std::sync::PoisonError::into_inner) = Some((Instant::now(), Arc::clone(&map)));
        Ok(map)
    }

    pub(crate) fn forget_stats(&self) {
        *self.stats_cache.write().unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }
}

/// Runs `ANALYZE`: of the named tables, or of every table this role owns or may read.
pub fn analyze(s: &Session, tables: &[String]) -> SqlResult<()> {
    let list: Vec<Arc<Table>> = if tables.is_empty() {
        s.engine.refresh_catalog(true)?;
        let acl = s.engine.acl()?;
        s.engine.catalog.all().into_iter().filter(|t| acl.table_access(s.current_role(), t, "SELECT") == crate::exec::acl::Access::All).collect()
    } else {
        tables.iter().map(|n| s.engine.table(n)).collect::<SqlResult<_>>()?
    };
    for t in list {
        // It reads every row: the same right as reading them.
        s.require_table(&t, "SELECT", &[])?;
        if s.engine.acl()?.table_access(s.current_role(), &t, "SELECT") != crate::exec::acl::Access::All {
            return Err(crate::error::SqlError::new("42501", format!("permission denied for table {}", t.name)));
        }
        analyze_table(s, &t)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hyperloglog_is_within_a_few_percent() {
        for n in [10u64, 1_000, 100_000, 1_000_000] {
            let mut h = HyperLogLog::default();
            for i in 0..n {
                h.add(i.to_string().as_bytes());
                h.add(i.to_string().as_bytes());
            }
            let e = h.estimate();
            let err = (e - n as f64).abs() / n as f64;
            assert!(err < 0.03, "{n}: estimated {e}");
        }
        assert_eq!(HyperLogLog::default().estimate(), 0.0);
    }

    #[test]
    fn histograms_locate_values() {
        let c = ColumnStats { null_frac: 0.1, distinct: 100.0, numeric_bounds: (0..=100).map(f64::from).collect(), ..ColumnStats::default() };
        let below = c.range_selectivity(None, Some(&Datum::Int(25))).unwrap();
        assert!((below - 0.225).abs() < 0.01, "{below}");
        let mid = c.range_selectivity(Some(&Datum::Int(40)), Some(&Datum::Int(60))).unwrap();
        assert!((mid - 0.18).abs() < 0.01, "{mid}");
        assert_eq!(c.range_selectivity(Some(&Datum::Int(500)), None).unwrap(), 0.0);
        assert!((c.range_selectivity(Some(&Datum::Int(-5)), None).unwrap() - 0.9).abs() < 1e-9);
        let t = ColumnStats { text_bounds: vec!["a".into(), "m".into(), "z".into()], ..ColumnStats::default() };
        assert!(t.range_selectivity(None, Some(&Datum::Text("n".into()))).unwrap() > 0.5);
    }

    #[test]
    fn common_values_answer_equalities() {
        let c = ColumnStats { null_frac: 0.0, distinct: 11.0, common: vec![("hot".into(), 0.5)], ..ColumnStats::default() };
        assert_eq!(c.eq_selectivity(Some(&Datum::Text("hot".into()))), 0.5);
        assert!((c.eq_selectivity(Some(&Datum::Text("cold".into()))) - 0.05).abs() < 1e-9);
        assert!((c.eq_selectivity(None) - 1.0 / 11.0).abs() < 1e-9);
    }

    #[test]
    fn documents_round_trip() {
        let mut t = TableStats { rows: 12.0, columns: HashMap::new() };
        t.columns.insert("a".into(), ColumnStats { null_frac: 0.5, distinct: 3.0, numeric_bounds: vec![1.0, 2.0], text_bounds: vec!["x".into()], common: vec![("x".into(), 0.25)] });
        assert_eq!(TableStats::from_doc(&t.to_doc()), t);
    }
}
