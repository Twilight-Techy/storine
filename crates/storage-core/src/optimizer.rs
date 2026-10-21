//! Cost-Based Query Optimizer & `EXPLAIN` Execution Plans (`TASK-048`, `F-OPT-01`, `F-OPT-02`).
//!
//! Evaluates exact index cardinalities in $O(1)$ (for `BitmapTagIndex`) and $O(\log N)$
//! (for `NumericRangeIndex`) with **zero intermediate heap allocations**, ordering predicates
//! from lowest cardinality (most selective) to highest cardinality and selecting the lowest-cost
//! physical access path (`EmptyShortCircuit`, `BitmapTagFirst`, `BTreeRangeFirst`, or `FullScan`).

use std::ops::RangeInclusive;
use storage_codec::ZeroCopyRecord;
use storage_index::Indexable;
use storage_types::InlineId;

use crate::engine::StorageEngine;

/// Physical access path chosen by the Cost-Based Query Optimizer (`F-OPT-01`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccessPathKind {
    /// At least one predicate has 0 matching records; query short-circuits in $O(1)$ without scanning!
    EmptyShortCircuit,
    /// Starts from the most selective `BitmapTagIndex` posting list and filters remaining predicates.
    BitmapTagFirst,
    /// Starts from the most selective `NumericRangeIndex` B+Tree range scan and filters in-place.
    BTreeRangeFirst,
    /// No secondary predicates supplied.
    UnconstrainedEmpty,
}

/// Cardinality and selectivity estimate for a single query predicate (`F-OPT-02`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PredicateCostEstimate {
    pub predicate_kind: &'static str,
    pub field_id: u16,
    pub estimated_cardinality: usize,
}

/// Structured `EXPLAIN` execution plan returned by `QueryBuilder::explain()` (`TASK-048`, `F-OPT-02`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryExplainPlan {
    pub chosen_access_path: AccessPathKind,
    pub estimated_cost_units: u64,
    pub estimated_output_rows: usize,
    pub ordered_predicates: Vec<PredicateCostEstimate>,
}

/// Cost-Based Query Optimizer (`TASK-048`, `F-OPT-01`).
#[derive(Debug, Default)]
pub struct QueryOptimizer;

impl QueryOptimizer {
    /// Computes a `QueryExplainPlan` for the given tag and range predicates against `engine`.
    pub fn build_explain_plan<T: ZeroCopyRecord + Indexable>(
        engine: &StorageEngine<T>,
        tags: &[(u16, InlineId)],
        ranges: &[(u16, RangeInclusive<i64>)],
        limit: Option<usize>,
    ) -> QueryExplainPlan {
        if tags.is_empty() && ranges.is_empty() {
            return QueryExplainPlan {
                chosen_access_path: AccessPathKind::UnconstrainedEmpty,
                estimated_cost_units: 0,
                estimated_output_rows: 0,
                ordered_predicates: Vec::new(),
            };
        }

        let total_live = engine.primary_index().len().max(1);
        let mut preds = Vec::with_capacity(tags.len() + ranges.len());

        for &(field_id, tag) in tags {
            let card = engine.tag_index().cardinality(field_id, tag);
            preds.push(PredicateCostEstimate {
                predicate_kind: "BitmapTagEq",
                field_id,
                estimated_cardinality: card,
            });
        }

        for (field_id, range) in ranges {
            let card = engine.range_index().count_in_range(*field_id, range.clone());
            preds.push(PredicateCostEstimate {
                predicate_kind: "BTreeRange",
                field_id: *field_id,
                estimated_cardinality: card,
            });
        }

        // Order predicates from most selective (smallest cardinality) to least selective (`RULE-CPLX-01`)
        preds.sort_by_key(|p| p.estimated_cardinality);

        if preds[0].estimated_cardinality == 0 {
            return QueryExplainPlan {
                chosen_access_path: AccessPathKind::EmptyShortCircuit,
                estimated_cost_units: 1,
                estimated_output_rows: 0,
                ordered_predicates: preds,
            };
        }

        let chosen_access_path = if preds[0].predicate_kind == "BitmapTagEq" {
            AccessPathKind::BitmapTagFirst
        } else {
            AccessPathKind::BTreeRangeFirst
        };

        // Estimate combined output rows using independent selectivity product capped by `limit`
        let mut est_rows = preds[0].estimated_cardinality;
        let mut cost_units = est_rows as u64;
        for p in &preds[1..] {
            est_rows = ((est_rows as u128 * p.estimated_cardinality as u128)
                / (total_live as u128))
                .max(1) as usize;
            cost_units = cost_units.saturating_add((p.estimated_cardinality / 64).max(1) as u64);
        }
        if let Some(lim) = limit {
            est_rows = est_rows.min(lim);
        }

        QueryExplainPlan {
            chosen_access_path,
            estimated_cost_units: cost_units.max(1),
            estimated_output_rows: est_rows,
            ordered_predicates: preds,
        }
    }
}
