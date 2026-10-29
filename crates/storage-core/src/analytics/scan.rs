//! Columnar scans (`TASK-129`, `OP_DOC_SCAN_COLUMNS`): chosen fields of the documents matching a
//! filter, in key order and in pages, read from column segments plus the delta. The filter is
//! evaluated with the vectorized kernels (zone maps prune first), only the page's rows are
//! materialized, and only the requested columns are decoded.

use std::collections::BinaryHeap;

use storage_codec::types::{PropertyValue, PropertyValueRef};
use storage_types::{Result, StorageError};

use crate::columnar::ColumnarSnapshot;
use crate::document::{lookup_path, Document, Filter};

use super::exec::with_row_group;
use super::MAX_QUERY_ITEMS;

/// Default rows per scan page.
pub const DEFAULT_SCAN_LIMIT: usize = 1000;
/// Most rows per scan page.
pub const MAX_SCAN_LIMIT: usize = 100_000;

/// A columnar scan: `fields` of every document matching `filter`, keys after `after_key`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ScanQuery {
    pub collection: String,
    pub filter: Option<Filter>,
    /// Dotted paths; a missing field reads as null.
    pub fields: Vec<String>,
    pub after_key: Option<String>,
    /// Rows per page (`0`: 1,000; at most 100,000).
    pub limit: usize,
}

/// One page of a scan, in key order.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ScanPage {
    /// `(key, values in field order)`.
    pub rows: Vec<(String, Vec<PropertyValue>)>,
    /// Documents matching the filter (all pages).
    pub total: u64,
    /// Pass as `after_key` for the next page, if more remain.
    pub next_after_key: Option<String>,
}

fn validate(q: &ScanQuery) -> Result<usize> {
    if q.fields.len() > MAX_QUERY_ITEMS {
        return Err(StorageError::invalid_value("a scan reads at most 64 fields"));
    }
    if q.fields.iter().any(|f| f.is_empty() || f.starts_with('$')) {
        return Err(StorageError::invalid_value("scan fields must be non-empty document paths"));
    }
    match q.limit {
        0 => Ok(DEFAULT_SCAN_LIMIT),
        l if l > MAX_SCAN_LIMIT => Err(StorageError::invalid_value("a scan page holds at most 100,000 rows")),
        l => Ok(l),
    }
}

/// Where a candidate row lives.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Src {
    Row(usize, usize, usize),
    Doc(usize),
}

/// Keeps the `limit + 1` smallest keys (one extra tells whether another page exists).
struct TopKeys {
    keep: usize,
    heap: BinaryHeap<(String, Src)>,
}

impl TopKeys {
    fn offer(&mut self, key: &str, src: Src) {
        if self.heap.len() < self.keep {
            self.heap.push((key.to_owned(), src));
        } else if self.heap.peek().is_some_and(|(top, _)| key < top.as_str()) {
            self.heap.pop();
            self.heap.push((key.to_owned(), src));
        }
    }
}

fn value_of(d: &Document, path: &str) -> PropertyValue {
    let mut buf = storage_codec::AlignedScratchBuf::with_capacity(512);
    let map = {
        let mut b = storage_codec::archive::ArchiveBuilder::new(&mut buf, 0);
        storage_codec::types::ArchivedMap::archive(&mut b, &d.fields)
    };
    match lookup_path(map.view(buf.as_slice()), path) {
        PropertyValueRef::Null => PropertyValue::Null,
        v => v.to_owned_value(),
    }
}

fn doc_matches(f: Option<&Filter>, d: &Document) -> bool {
    let Some(f) = f else { return true };
    let mut buf = storage_codec::AlignedScratchBuf::with_capacity(512);
    let map = {
        let mut b = storage_codec::archive::ArchiveBuilder::new(&mut buf, 0);
        storage_codec::types::ArchivedMap::archive(&mut b, &d.fields)
    };
    f.matches(&d.key, map.view(buf.as_slice()))
}

/// Runs one page of `q` over a columnar snapshot.
pub fn scan(q: &ScanQuery, snap: &ColumnarSnapshot<'_>) -> Result<ScanPage> {
    let limit = validate(q)?;
    let mut needed = q.fields.clone();
    needed.push(storage_columnar::segment::KEY_NAME.to_owned());
    super::exec::filter_paths_into(q.filter.as_ref(), &mut needed);
    needed.sort();
    needed.dedup();
    let after = q.after_key.as_deref();
    let mut top = TopKeys { keep: limit + 1, heap: BinaryHeap::new() };
    let mut total = 0u64;
    let mut pruned = 0;
    for (si, s) in snap.segments.iter().enumerate() {
        for g in 0..s.segment.row_groups().len() {
            with_row_group(q.filter.as_ref(), snap, si, g, &needed, &mut pruned, |rg, sel| {
                let keys = rg.row_keys();
                for (w, &word) in sel.iter().enumerate() {
                    let mut m = word;
                    while m != 0 {
                        let i = w * 64 + m.trailing_zeros() as usize;
                        m &= m - 1;
                        total += 1;
                        let key = keys.get(i).map_or("", String::as_str);
                        if after.is_none_or(|a| key > a) {
                            top.offer(key, Src::Row(si, g, i));
                        }
                    }
                }
                Ok(())
            })?;
        }
    }
    for (i, d) in snap.delta.iter().enumerate() {
        if doc_matches(q.filter.as_ref(), d) {
            total += 1;
            if after.is_none_or(|a| d.key.as_str() > a) {
                top.offer(&d.key, Src::Doc(i));
            }
        }
    }
    let mut picked = top.heap.into_sorted_vec();
    let more = picked.len() > limit;
    picked.truncate(limit);
    // Materialize the page: each row group's rows together, so its columns decode once.
    let mut values: Vec<Option<Vec<PropertyValue>>> = vec![None; picked.len()];
    let mut order: Vec<usize> = (0..picked.len()).collect();
    order.sort_by(|&a, &b| picked[a].1.cmp(&picked[b].1));
    let mut at = 0;
    while at < order.len() {
        match picked[order[at]].1.clone() {
            Src::Doc(i) => {
                let d = &snap.delta[i];
                values[order[at]] = Some(q.fields.iter().map(|p| value_of(d, p)).collect());
                at += 1;
            }
            Src::Row(si, g, _) => {
                let end = order[at..].iter().position(|&k| !matches!(picked[k].1, Src::Row(s2, g2, _) if s2 == si && g2 == g)).map_or(order.len(), |p| at + p);
                let rows: Vec<(usize, usize)> = order[at..end]
                    .iter()
                    .map(|&k| match picked[k].1 {
                        Src::Row(_, _, r) => (k, r),
                        Src::Doc(_) => (k, 0),
                    })
                    .collect();
                // No filter: the rows were selected already.
                let mut ignored = 0;
                with_row_group(None, snap, si, g, &needed, &mut ignored, |rg, _| {
                    for &(k, r) in &rows {
                        values[k] = Some(q.fields.iter().map(|p| rg.value_at(p, r)).collect());
                    }
                    Ok(())
                })?;
                at = end;
            }
        }
    }
    let next_after_key = if more { picked.last().map(|(k, _)| k.clone()) } else { None };
    let rows = picked.into_iter().zip(values).map(|((k, _), v)| (k, v.unwrap_or_default())).collect();
    Ok(ScanPage { rows, total, next_after_key })
}

/// Runs one page of `q` over documents (a collection without column segments).
pub fn scan_documents(q: &ScanQuery, docs: &[Document]) -> Result<ScanPage> {
    let limit = validate(q)?;
    let after = q.after_key.as_deref();
    let mut matching: Vec<&Document> = docs.iter().filter(|d| doc_matches(q.filter.as_ref(), d)).collect();
    let total = matching.len() as u64;
    matching.retain(|d| after.is_none_or(|a| d.key.as_str() > a));
    matching.sort_by(|a, b| a.key.cmp(&b.key));
    let more = matching.len() > limit;
    matching.truncate(limit);
    let next_after_key = if more { matching.last().map(|d| d.key.clone()) } else { None };
    let rows = matching.into_iter().map(|d| (d.key.clone(), q.fields.iter().map(|p| value_of(d, p)).collect())).collect();
    Ok(ScanPage { rows, total, next_after_key })
}
