//! Range descriptors, the meta range and range caches (`F-GEO-02`, `TASK-138`).
//!
//! A **range descriptor** names a range, its key span, its replicas, a generation and the last
//! known leaseholder. Descriptors live as ordinary records `_meta/<range id>` in the cluster's
//! own key space, so they are replicated like data. The range holding the `_meta/` prefix is
//! the **meta range**; it never splits inside that prefix.
//!
//! Descriptors are **routing hints**, never a source of truth:
//! - every replica checks its own span and refuses keys outside it (`WrongRange`), and only a
//!   leaseholder serves (`NotLeader`);
//! - so a stale descriptor costs a retry, never a wrong answer;
//! - each leaseholder republishes its range's descriptor when the generation or the lease
//!   changes, and the meta range keeps the newest generation per range.
//!
//! **Generation**: the log index of the range's latest span or configuration change (both are
//! entries of the same log, so it only grows). A split publishes both halves; a merge
//! publishes the merged range naming the range it absorbed, whose descriptor is dropped.

use std::collections::BTreeMap;

use storage_types::{InlineId, Result, StorageError};

use crate::dist::node::RangeId;
use crate::raft::KeySpan;
use crate::sharding::NodeId;

/// Collection of the descriptor records.
pub const META_COLLECTION: &str = "_meta";
/// Raw-id prefix of the descriptor records: the meta range holds all of `[META_START, META_END)`.
pub const META_START: &[u8] = b"_meta/";
pub const META_END: &[u8] = b"_meta0";

/// Whether a split at `at` would cut the meta prefix in two (refused).
pub fn splits_meta(at: &[u8]) -> bool {
    at > META_START && at < META_END
}

/// The record key of `range`'s descriptor (fits an inline id).
pub fn meta_key(range: RangeId) -> String {
    format!("{range:016x}")
}

/// Where a range is and who serves it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeDescriptor {
    pub range: RangeId,
    pub span: KeySpan,
    /// `(node, voter)`.
    pub members: Vec<(NodeId, bool)>,
    pub generation: u64,
    /// Leaseholder and its Raft term when published.
    pub lease: Option<(NodeId, u64)>,
}

const DESC_VERSION: u8 = 1;

impl RangeDescriptor {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![DESC_VERSION];
        out.extend_from_slice(&self.range.to_le_bytes());
        out.extend_from_slice(&self.generation.to_le_bytes());
        let (lease, term) = self.lease.unwrap_or((0, 0));
        out.extend_from_slice(&lease.to_le_bytes());
        out.extend_from_slice(&term.to_le_bytes());
        out.extend_from_slice(&(self.members.len() as u32).to_le_bytes());
        for &(id, voter) in &self.members {
            out.extend_from_slice(&id.to_le_bytes());
            out.push(u8::from(voter));
        }
        self.span.encode(&mut out);
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let bad = || StorageError::corrupted_header("range descriptor invalid");
        let mut r = storage_codec::value_wire::SveReader::new(bytes);
        if r.u8()? != DESC_VERSION {
            return Err(bad());
        }
        let range = r.u64()?;
        let generation = r.u64()?;
        let lo = r.u8()?;
        let hi = r.u8()?;
        let lease = u16::from_le_bytes([lo, hi]);
        let term = r.u64()?;
        let n = r.u32()? as usize;
        if n > crate::raft::MAX_MEMBERS {
            return Err(bad());
        }
        let mut members = Vec::with_capacity(n);
        for _ in 0..n {
            let lo = r.u8()?;
            let hi = r.u8()?;
            let voter = match r.u8()? {
                0 => false,
                1 => true,
                _ => return Err(bad()),
            };
            members.push((u16::from_le_bytes([lo, hi]), voter));
        }
        let span = KeySpan::decode(&mut r)?;
        if r.remaining() != 0 {
            return Err(bad());
        }
        Ok(Self { range, span, members, generation, lease: (lease != 0).then_some((lease, term)) })
    }

    /// Nodes to try for this range: the leaseholder first, then the voters.
    pub fn candidates(&self) -> Vec<NodeId> {
        let mut out: Vec<NodeId> = self.lease.map(|(n, _)| n).into_iter().collect();
        out.extend(self.members.iter().filter(|&&(id, voter)| voter && Some(id) != self.lease.map(|l| l.0)).map(|&(id, _)| id));
        out
    }

    fn newer_than(&self, other: &Self) -> bool {
        (self.generation, self.lease.map_or(0, |l| l.1)) > (other.generation, other.lease.map_or(0, |l| l.1))
    }
}

/// Sort key of a span end: `None` (the end of the key space) last.
type EndKey = (bool, Vec<u8>);

fn end_key(span: &KeySpan) -> EndKey {
    match &span.end {
        Some(e) => (false, e.clone()),
        None => (true, Vec::new()),
    }
}

fn overlaps(a: &KeySpan, b: &KeySpan) -> bool {
    let a_before_b = a.end.as_ref().is_some_and(|e| e.as_slice() <= b.start.as_slice());
    let b_before_a = b.end.as_ref().is_some_and(|e| e.as_slice() <= a.start.as_slice());
    !a_before_b && !b_before_a
}

/// Descriptors ordered by span end: the meta range's index and every client's cache.
#[derive(Clone, Debug, Default)]
pub struct RangeIndex {
    by_end: BTreeMap<(EndKey, RangeId), RangeDescriptor>,
    ends: BTreeMap<RangeId, EndKey>,
}

impl RangeIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.ends.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ends.is_empty()
    }

    pub fn get(&self, range: RangeId) -> Option<&RangeDescriptor> {
        let end = self.ends.get(&range)?;
        self.by_end.get(&(end.clone(), range))
    }

    /// The most specific descriptor whose span holds `key`.
    pub fn lookup(&self, key: &[u8]) -> Option<&RangeDescriptor> {
        let from = ((false, key.to_vec()), RangeId::MAX);
        let mut best: Option<&RangeDescriptor> = None;
        for ((end, _), d) in self.by_end.range((std::ops::Bound::Excluded(from), std::ops::Bound::Unbounded)) {
            if best.is_some_and(|b| end_key(&b.span) != *end) {
                break; // only same-end entries can be more specific than the first match
            }
            if d.span.contains(key) && best.is_none_or(|b| d.span.start > b.span.start) {
                best = Some(d);
            }
        }
        best
    }

    /// Descriptors from the one holding `key` on, in key order (at most `limit`).
    pub fn scan_from(&self, key: &[u8], limit: usize) -> Vec<RangeDescriptor> {
        let from = ((false, key.to_vec()), RangeId::MAX);
        self.by_end.range((std::ops::Bound::Excluded(from), std::ops::Bound::Unbounded)).map(|(_, d)| d.clone()).take(limit).collect()
    }

    pub fn all(&self) -> impl Iterator<Item = &RangeDescriptor> {
        self.by_end.values()
    }

    fn remove(&mut self, range: RangeId) -> Option<RangeDescriptor> {
        let end = self.ends.remove(&range)?;
        self.by_end.remove(&(end, range))
    }

    fn put(&mut self, d: RangeDescriptor) {
        self.remove(d.range);
        self.ends.insert(d.range, end_key(&d.span));
        self.by_end.insert((end_key(&d.span), d.range), d);
    }

    /// Meta-range rule: keeps the newest descriptor per range (by generation, then lease
    /// term); a merged range drops the descriptors of the ranges it `absorbed`. Returns
    /// whether `d` was taken.
    pub fn publish(&mut self, d: RangeDescriptor, absorbed: &[RangeId]) -> bool {
        if self.get(d.range).is_some_and(|old| !d.newer_than(old)) {
            return false;
        }
        for &a in absorbed {
            if self.get(a).is_some_and(|x| x.span.start >= d.span.start && end_key(&x.span) <= end_key(&d.span)) {
                self.remove(a);
            }
        }
        self.put(d);
        true
    }

    /// Client-cache rule: newer information replaces every overlapping entry (a stale entry
    /// costs one retry, then the cache learns again).
    pub fn learn(&mut self, d: RangeDescriptor) {
        if self.get(d.range).is_some_and(|old| old.newer_than(&d)) {
            return;
        }
        let stale: Vec<RangeId> = self.by_end.values().filter(|x| x.range != d.range && overlaps(&x.span, &d.span)).map(|x| x.range).collect();
        for r in stale {
            self.remove(r);
        }
        self.put(d);
    }

    /// Forgets `range` (its leaseholder said the key is not there).
    pub fn evict(&mut self, range: RangeId) {
        self.remove(range);
    }

    /// Records `node` as `range`'s leaseholder (a `NotLeader` hint).
    pub fn set_lease(&mut self, range: RangeId, node: NodeId) {
        if let Some(end) = self.ends.get(&range).cloned() {
            if let Some(d) = self.by_end.get_mut(&(end, range)) {
                let term = d.lease.map_or(0, |l| l.1);
                d.lease = Some((node, term));
            }
        }
    }
}

/// The id of `range`'s descriptor record.
pub fn meta_id(range: RangeId) -> Result<InlineId> {
    crate::document::document_id(META_COLLECTION, &meta_key(range))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(range: RangeId, start: &str, end: Option<&str>, generation: u64) -> RangeDescriptor {
        RangeDescriptor { range, span: KeySpan::new(start.as_bytes().to_vec(), end.map(|e| e.as_bytes().to_vec())), members: vec![(1, true), (2, true), (3, false)], generation, lease: Some((2, 7)) }
    }

    #[test]
    fn descriptor_round_trips() {
        for x in [d(1, "", None, 0), d(9, "a", Some("m"), 44), RangeDescriptor { lease: None, members: Vec::new(), ..d(3, "x", None, 1) }] {
            assert_eq!(RangeDescriptor::decode(&x.encode()).unwrap(), x);
        }
        let bytes = d(9, "a", Some("m"), 44).encode();
        for cut in 0..bytes.len() {
            assert!(RangeDescriptor::decode(&bytes[..cut]).is_err());
        }
        assert!(meta_id(u64::MAX).unwrap().as_bytes().starts_with(META_START));
    }

    #[test]
    fn lookup_publish_and_learn() {
        let mut m = RangeIndex::new();
        assert!(m.publish(d(1, "", None, 0), &[]));
        assert_eq!(m.lookup(b"q").unwrap().range, 1);
        // Split of r1 at "m" -> r2: right half published first, then the left.
        assert!(m.publish(d(2, "m", None, 5), &[]));
        assert_eq!(m.lookup(b"q").unwrap().range, 2, "the more specific descriptor wins");
        assert_eq!(m.lookup(b"a").unwrap().range, 1);
        assert!(m.publish(d(1, "", Some("m"), 5), &[]));
        assert!(!m.publish(d(1, "", None, 0), &[]), "older generations are ignored");
        assert_eq!(m.lookup(b"m").unwrap().range, 2);
        assert_eq!(m.lookup(b"l").unwrap().range, 1);
        assert_eq!(m.scan_from(b"", 10).len(), 2);
        // Merge r2 back into r1.
        assert!(m.publish(d(1, "", None, 9), &[2]));
        assert_eq!(m.len(), 1);
        assert_eq!(m.lookup(b"z").unwrap().range, 1);

        let mut c = RangeIndex::new();
        c.learn(d(1, "", None, 0));
        c.learn(d(2, "m", None, 5));
        assert!(c.get(1).is_none(), "overlapping stale entries are evicted");
        assert!(c.lookup(b"a").is_none());
        c.learn(d(1, "", Some("m"), 5));
        assert_eq!(c.lookup(b"a").unwrap().range, 1);
        c.set_lease(1, 3);
        assert_eq!(c.lookup(b"a").unwrap().candidates()[0], 3);
        c.evict(1);
        assert!(c.lookup(b"a").is_none());
        assert!(splits_meta(b"_meta/0001") && !splits_meta(b"_meta/") && !splits_meta(b"kv/"));
    }
}
