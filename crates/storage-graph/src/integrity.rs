//! Canonical lock ordering (`RULE-CONC-02`) and Referential Integrity policies (`TASK-021`, `TASK-022`, `F-REL-02`, `F-REL-03`, `F-REL-05`).

use storage_types::InlineId;

/// Referential integrity policy enforced when a referenced record is deleted (`F-REL-03`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum OnDelete {
    /// Reject deletion of the target record if any parent/peer still references it.
    Restrict,
    /// Recursively delete all dependent child records when the parent record is deleted.
    Cascade,
    /// Automatically remove the deleted record's `InlineId` from all referencing records.
    #[default]
    Unlink,
}

/// Relationship edge policy declaration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IntegrityPolicy {
    pub on_delete: OnDelete,
}

/// Canonical deadlock-free lock ordering (`RULE-CONC-02`).
///
/// Whenever two records `(coll_a, id_a)` and `(coll_b, id_b)` must be locked together
/// (e.g. during bidirectional `link` or `unlink`), returns them ordered strictly ascending
/// so two concurrent threads linking `(A, B)` and `(B, A)` always acquire locks in the
/// exact same order.
#[inline]
pub fn canonical_lock_order(
    a: (u16, InlineId),
    b: (u16, InlineId),
) -> ((u16, InlineId), (u16, InlineId)) {
    if a <= b {
        (a, b)
    } else {
        (b, a)
    }
}

/// Report item emitted by the Orphan / Dangling Reference Verifier (`F-REL-05`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DanglingEdge {
    pub source_collection: u16,
    pub source_id: InlineId,
    pub missing_target_id: InlineId,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_canonical_lock_order_symmetry() {
        let k1 = (1u16, InlineId::new("alpha").unwrap());
        let k2 = (1u16, InlineId::new("beta").unwrap());
        assert_eq!(canonical_lock_order(k1, k2), (k1, k2));
        assert_eq!(canonical_lock_order(k2, k1), (k1, k2));
    }
}
