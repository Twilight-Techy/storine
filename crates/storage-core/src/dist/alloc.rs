//! Allocator: what a leaseholder does with its range next (`F-GEO-02`, `TASK-138`).
//!
//! Every leaseholder plans for the ranges it leads, from:
//! - its own view of each range (replicas, progress, size, write rate);
//! - store loads that nodes gossip in their batches (replica and lease counts, bytes, liveness).
//!
//! It makes at most one change per range per round:
//! 1. **Repair**: promote a caught-up learner; replace a dead replica; add replicas up to the
//!    replication factor; remove extra replicas (dead ones first, then the most loaded).
//! 2. **Rebalance replicas** by count, then bytes: when a member's store holds at least two
//!    more replicas than the least loaded live non-member, add a replica there (the extra
//!    replica on the loaded store is removed next round).
//! 3. **Rebalance leases**: hand a lease to a caught-up voter whose store leads fewer ranges
//!    than average when this store leads more than average plus one.
//! 4. **Split** a range above the size or write-rate threshold: at its median key (size) or
//!    the median of its recently written keys (load).
//! 5. **Merge** two small neighbours: co-locate their leases, align the right range's replicas
//!    with the left's, freeze the right range, then merge once every replica of it has
//!    applied everything.
//!
//! The planner is a pure function. The driver ([`crate::dist::node::NodeDriver`]) executes the
//! result, so the rules are tested without a cluster.

use std::collections::BTreeMap;

use crate::sharding::NodeId;

/// Allocator knobs.
#[derive(Clone, Debug)]
pub struct AllocatorConfig {
    /// Voters per range.
    pub replication: usize,
    /// Split a range holding more keys than this.
    pub split_keys: usize,
    /// Split a range written more often than this (writes per second).
    pub split_writes_per_sec: u64,
    /// Merge neighbours holding fewer keys than this together.
    pub merge_keys: usize,
    /// Planning interval (microseconds).
    pub interval_us: u64,
    /// A store not heard from for this long counts as dead (microseconds).
    pub dead_after_us: u64,
    /// Replica counts may differ by this much before replicas move.
    pub rebalance_slack: usize,
}

impl Default for AllocatorConfig {
    fn default() -> Self {
        Self { replication: 3, split_keys: 100_000, split_writes_per_sec: 2_000, merge_keys: 10_000, interval_us: 1_000_000, dead_after_us: 10_000_000, rebalance_slack: 2 }
    }
}

/// One store's load, as gossiped.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StoreLoad {
    pub replicas: u32,
    pub leases: u32,
    pub bytes: u64,
    /// Ranges led here in the middle of a replica change: rebalancing waits for them (a store
    /// mid-move looks more loaded than it will be).
    pub moving: u32,
}

/// What the planner knows about a store.
#[derive(Clone, Copy, Debug, Default)]
pub struct StoreView {
    pub load: StoreLoad,
    pub live: bool,
}

/// A replica of the planned range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MemberView {
    pub node: NodeId,
    pub voter: bool,
    pub caught_up: bool,
}

/// The right-hand neighbour of the planned range, as hosted on this node.
#[derive(Clone, Debug)]
pub struct NeighbourView {
    pub range: u64,
    pub keys: usize,
    /// This node leads it.
    pub led_here: bool,
    pub frozen: bool,
    pub members: Vec<MemberView>,
    /// [`crate::dist::node::NodeDriver::merge_ready`].
    pub merge_ready: bool,
}

/// The left-hand neighbour of the planned range, as hosted on this node.
#[derive(Clone, Copy, Debug)]
pub struct LeftView {
    pub keys: usize,
    /// Its leaseholder, as known here.
    pub leader: Option<NodeId>,
}

/// One range this node leads.
#[derive(Clone, Debug)]
pub struct RangeView {
    pub range: u64,
    pub me: NodeId,
    pub members: Vec<MemberView>,
    /// The configuration is committed and no removal is in flight.
    pub config_settled: bool,
    pub frozen: bool,
    pub keys: usize,
    pub writes_per_sec: u64,
    /// Split key candidates: the median key, the median recently written key.
    pub size_split_key: Option<Vec<u8>>,
    pub load_split_key: Option<Vec<u8>>,
    pub right: Option<NeighbourView>,
    pub left: Option<LeftView>,
    /// The store an earlier [`Action::Rebalance`] of this range relieves: removed once the
    /// replacement is a voter.
    pub planned_removal: Option<NodeId>,
    /// No replica rebalancing now: one already started this round, or it is another node's
    /// turn (nodes take turns, so two never move replicas onto one store at once).
    pub rebalanced_this_round: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    AddLearner(NodeId),
    /// Add a replica on `add` to relieve `relieve` (removed once `add` votes).
    Rebalance { add: NodeId, relieve: NodeId },
    Promote(NodeId),
    Remove(NodeId),
    TransferLease(NodeId),
    Split(Vec<u8>),
    /// Change the right-hand neighbour's replicas (it is led here).
    RightAddLearner(u64, NodeId),
    RightPromote(u64, NodeId),
    RightRemove(u64, NodeId),
    Freeze(u64),
    Merge(u64),
    /// Hand the lease of this (right-hand) range to the left neighbour's leaseholder.
    LeaseToLeft(NodeId),
}

fn score(s: &StoreView) -> (u32, u64) {
    (s.load.replicas, s.load.bytes)
}

/// The next change for `r`, if any.
pub fn plan(cfg: &AllocatorConfig, r: &RangeView, stores: &BTreeMap<NodeId, StoreView>) -> Option<Action> {
    let live = |n: NodeId| n == r.me || stores.get(&n).is_some_and(|s| s.live);
    let store = |n: NodeId| stores.get(&n).copied().unwrap_or_default();
    if r.frozen {
        // A merge in progress: bring the lease to the left neighbour's leaseholder.
        return match r.left {
            Some(LeftView { leader: Some(l), .. }) if l != r.me && r.members.iter().any(|m| m.node == l && m.voter && m.caught_up) => Some(Action::LeaseToLeft(l)),
            _ => None,
        };
    }
    if !r.config_settled {
        return None;
    }
    let cool = r.writes_per_sec < cfg.split_writes_per_sec / 2;
    // The small right half of a pair: its replicas are the left range's plan to align (two
    // plans would undo each other's moves). It only brings the left range's leaseholder in as
    // a voter and hands it the lease, so that one node leads both.
    // It may still be the left half of its own right-hand pair (chains merge from anywhere).
    if let Some(left) = r.left.filter(|l| r.keys + l.keys < cfg.merge_keys && cool) {
        let Some(l) = left.leader.filter(|&l| l != r.me) else { return merge_step(cfg, r) };
        return match r.members.iter().find(|m| m.node == l) {
            Some(m) if m.voter && m.caught_up => Some(Action::LeaseToLeft(l)),
            Some(m) if !m.voter && m.caught_up => Some(Action::Promote(l)),
            Some(_) => None,
            None => Some(Action::AddLearner(l)),
        };
    }
    let in_pair = r.right.as_ref().is_some_and(|x| r.keys + x.keys < cfg.merge_keys && cool);
    let merging = r.right.as_ref().is_some_and(|x| x.frozen);
    let voters: Vec<&MemberView> = r.members.iter().filter(|m| m.voter).collect();
    let learners: Vec<&MemberView> = r.members.iter().filter(|m| !m.voter).collect();
    // 1. Repair.
    if let Some(l) = learners.iter().find(|l| l.caught_up && live(l.node)) {
        return Some(Action::Promote(l.node));
    }
    if let Some(l) = learners.iter().find(|l| !live(l.node)) {
        return Some(Action::Remove(l.node));
    }
    if !learners.is_empty() {
        return None; // a learner is catching up
    }
    if merging {
        // The replicas stay as they are until the merge.
        return r.right.as_ref().filter(|x| x.led_here && x.merge_ready).map(|x| Action::Merge(x.range));
    }
    let candidates: Vec<NodeId> = {
        let mut c: Vec<NodeId> = stores.iter().filter(|(&n, s)| s.live && n != r.me && !r.members.iter().any(|m| m.node == n)).map(|(&n, _)| n).collect();
        c.sort_by_key(|&n| (score(&store(n)), n));
        c
    };
    let live_voters = voters.iter().filter(|v| live(v.node)).count();
    if live_voters < cfg.replication.min(voters.len() + candidates.len()) {
        if let Some(&n) = candidates.first() {
            return Some(Action::AddLearner(n));
        }
    }
    if voters.len() > cfg.replication {
        if let Some(p) = r.planned_removal.filter(|&p| p != r.me && voters.iter().any(|v| v.node == p)) {
            return Some(Action::Remove(p));
        }
        let removable = voters.iter().filter(|v| v.node != r.me);
        let victim = removable.clone().find(|v| !live(v.node)).or_else(|| removable.max_by_key(|v| (score(&store(v.node)), v.node)));
        if let Some(v) = victim {
            return Some(Action::Remove(v.node));
        }
    }
    // 2. Rebalance replicas: from a store above the mean to one below it (a move never
    // overshoots).
    let live_counts: Vec<u32> = stores.iter().filter(|(&n, s)| s.live || n == r.me).map(|(_, s)| s.load.replicas).collect();
    let mean_x = live_counts.iter().map(|&c| u64::from(c)).sum::<u64>() * 10 / live_counts.len().max(1) as u64;
    let above = |n: NodeId| u64::from(store(n).load.replicas) * 10 > mean_x;
    let below = |n: NodeId| u64::from(store(n).load.replicas) * 10 < mean_x;
    if let (Some(&best), Some(worst)) = (candidates.first(), voters.iter().filter(|v| v.node != r.me).max_by_key(|v| (score(&store(v.node)), v.node))) {
        if !r.rebalanced_this_round && !in_pair && above(worst.node) && below(best) && store(worst.node).load.replicas as usize >= store(best).load.replicas as usize + cfg.rebalance_slack {
            return Some(Action::Rebalance { add: best, relieve: worst.node });
        }
    }
    // This store is the loaded one: hand the lease over, so the new leaseholder can move this
    // replica away (a leaseholder never removes itself).
    if let Some(&best) = candidates.first() {
        let me = store(r.me).load.replicas as usize;
        if !r.rebalanced_this_round && !in_pair && above(r.me) && below(best) && me >= store(best).load.replicas as usize + cfg.rebalance_slack && voters.iter().all(|v| v.node == r.me || store(v.node).load.replicas as usize <= me) {
            if let Some(v) = voters.iter().filter(|v| v.node != r.me && v.caught_up && live(v.node)).min_by_key(|v| (score(&store(v.node)), v.node)) {
                return Some(Action::TransferLease(v.node));
            }
        }
    }
    // 3. Rebalance leases.
    let mine = store(r.me).load.leases;
    let total: u32 = stores.values().filter(|s| s.live).map(|s| s.load.leases).sum::<u32>() + if stores.contains_key(&r.me) { 0 } else { mine };
    let n_live = stores.iter().filter(|(&n, s)| s.live || n == r.me).count().max(1) as u32;
    let mean = total / n_live;
    if mine > mean + 1 && !in_pair {
        if let Some(v) = voters.iter().filter(|v| v.node != r.me && v.caught_up && live(v.node) && store(v.node).load.leases < mean).min_by_key(|v| (store(v.node).load.leases, v.node)) {
            return Some(Action::TransferLease(v.node));
        }
    }
    // 4. Split.
    if r.keys > cfg.split_keys {
        if let Some(k) = &r.size_split_key {
            return Some(Action::Split(k.clone()));
        }
    }
    if r.writes_per_sec > cfg.split_writes_per_sec {
        if let Some(k) = &r.load_split_key {
            return Some(Action::Split(k.clone()));
        }
    }
    // 5. Merge (the right half acted above).
    merge_step(cfg, r)
}

/// The left half of a small pair: align the right half's replicas with ours, freeze it, merge.
fn merge_step(cfg: &AllocatorConfig, r: &RangeView) -> Option<Action> {
    let right = r.right.as_ref()?;
    if !right.led_here || r.keys + right.keys >= cfg.merge_keys || r.writes_per_sec >= cfg.split_writes_per_sec / 2 {
        return None;
    }
    let voters: Vec<&MemberView> = r.members.iter().filter(|m| m.voter).collect();
    if voters.len() != r.members.len() {
        return None; // our own replicas are changing
    }
    if right.frozen {
        return right.merge_ready.then_some(Action::Merge(right.range));
    }
    let want: Vec<NodeId> = voters.iter().map(|v| v.node).collect();
    if let Some(l) = right.members.iter().find(|m| !m.voter) {
        return (l.caught_up && want.contains(&l.node)).then_some(Action::RightPromote(right.range, l.node)).or(Some(Action::RightRemove(right.range, l.node)));
    }
    if let Some(&n) = want.iter().find(|&&n| !right.members.iter().any(|m| m.node == n)) {
        return Some(Action::RightAddLearner(right.range, n));
    }
    if let Some(m) = right.members.iter().find(|m| !want.contains(&m.node) && m.node != r.me) {
        return Some(Action::RightRemove(right.range, m.node));
    }
    Some(Action::Freeze(right.range))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stores(loads: &[(NodeId, u32, u32, bool)]) -> BTreeMap<NodeId, StoreView> {
        loads.iter().map(|&(n, replicas, leases, live)| (n, StoreView { load: StoreLoad { replicas, leases, bytes: 0, moving: 0 }, live })).collect()
    }

    fn voters(ns: &[NodeId]) -> Vec<MemberView> {
        ns.iter().map(|&node| MemberView { node, voter: true, caught_up: true }).collect()
    }

    fn view(members: Vec<MemberView>) -> RangeView {
        RangeView { range: 1, me: 1, members, config_settled: true, frozen: false, keys: 10, writes_per_sec: 0, size_split_key: Some(b"m".to_vec()), load_split_key: None, right: None, left: None, planned_removal: None, rebalanced_this_round: false }
    }

    #[test]
    fn repairs_and_rebalances() {
        let cfg = AllocatorConfig::default();
        let s = stores(&[(1, 5, 1, true), (2, 5, 1, true), (3, 5, 1, true), (4, 1, 1, true), (5, 5, 1, false)]);
        // Under-replicated: add on the least loaded live store.
        assert_eq!(plan(&cfg, &view(voters(&[1, 2])), &s), Some(Action::AddLearner(4)));
        // A dead voter is replaced, then removed.
        assert_eq!(plan(&cfg, &view(voters(&[1, 2, 5])), &s), Some(Action::AddLearner(4)));
        assert_eq!(plan(&cfg, &view(voters(&[1, 2, 4, 5])), &s), Some(Action::Remove(5)));
        // A caught-up learner is promoted; one catching up blocks other changes.
        let mut m = voters(&[1, 2, 3]);
        m.push(MemberView { node: 4, voter: false, caught_up: true });
        assert_eq!(plan(&cfg, &view(m.clone()), &s), Some(Action::Promote(4)));
        m[3].caught_up = false;
        assert_eq!(plan(&cfg, &view(m), &s), None);
        // Count imbalance: store 4 holds far fewer replicas; the store relieved is removed next.
        assert_eq!(plan(&cfg, &view(voters(&[1, 2, 3])), &s), Some(Action::Rebalance { add: 4, relieve: 3 }));
        let after = RangeView { planned_removal: Some(3), ..view(voters(&[1, 2, 3, 4])) };
        assert_eq!(plan(&cfg, &after, &s), Some(Action::Remove(3)));
        assert_eq!(plan(&cfg, &RangeView { rebalanced_this_round: true, ..view(voters(&[1, 2, 3])) }, &s), None);
        // Over-replicated: the most loaded non-leader goes.
        let s2 = stores(&[(1, 5, 1, true), (2, 9, 1, true), (3, 5, 1, true), (4, 5, 1, true)]);
        assert_eq!(plan(&cfg, &view(voters(&[1, 2, 3, 4])), &s2), Some(Action::Remove(2)));
    }

    #[test]
    fn leases_splits_and_merges() {
        let cfg = AllocatorConfig { split_keys: 100, merge_keys: 50, ..AllocatorConfig::default() };
        let s = stores(&[(1, 3, 9, true), (2, 3, 0, true), (3, 3, 0, true)]);
        assert_eq!(plan(&cfg, &view(voters(&[1, 2, 3])), &s), Some(Action::TransferLease(2)));
        let s = stores(&[(1, 3, 1, true), (2, 3, 1, true), (3, 3, 1, true)]);
        let big = RangeView { keys: 500, ..view(voters(&[1, 2, 3])) };
        assert_eq!(plan(&cfg, &big, &s), Some(Action::Split(b"m".to_vec())));
        let hot = RangeView { writes_per_sec: 10_000, load_split_key: Some(b"q".to_vec()), ..view(voters(&[1, 2, 3])) };
        assert_eq!(plan(&cfg, &hot, &s), Some(Action::Split(b"q".to_vec())));
        // Merge: align the right range's replicas, freeze it, then merge.
        let right = NeighbourView { range: 7, keys: 5, led_here: true, frozen: false, members: voters(&[1, 2, 4]), merge_ready: false };
        let r = RangeView { right: Some(right.clone()), ..view(voters(&[1, 2, 3])) };
        assert_eq!(plan(&cfg, &r, &s), Some(Action::RightAddLearner(7, 3)));
        let r = RangeView { right: Some(NeighbourView { members: voters(&[1, 2, 3, 4]), ..right.clone() }), ..view(voters(&[1, 2, 3])) };
        assert_eq!(plan(&cfg, &r, &s), Some(Action::RightRemove(7, 4)));
        let r = RangeView { right: Some(NeighbourView { members: voters(&[1, 2, 3]), ..right.clone() }), ..view(voters(&[1, 2, 3])) };
        assert_eq!(plan(&cfg, &r, &s), Some(Action::Freeze(7)));
        let r = RangeView { right: Some(NeighbourView { members: voters(&[1, 2, 3]), frozen: true, merge_ready: true, ..right }), ..view(voters(&[1, 2, 3])) };
        assert_eq!(plan(&cfg, &r, &s), Some(Action::Merge(7)));
        // The right half of a small pair sends its lease to the left's leaseholder.
        let r = RangeView { left: Some(LeftView { keys: 3, leader: Some(2) }), ..view(voters(&[1, 2, 3])) };
        assert_eq!(plan(&cfg, &r, &s), Some(Action::LeaseToLeft(2)));
    }
}
