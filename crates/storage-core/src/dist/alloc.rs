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
//! **Zone configurations** (`TASK-141`, [`crate::dist::zone`]) shape every step through the
//! range's [`Placement`]:
//! - a range straddling two configurations splits at the boundary first, and neighbours under
//!   different configurations never merge;
//! - replicas go only to stores the constraints allow, spread across regions and zones first
//!   (diversity), then by load; a replica on a disallowed store, or a failure domain holding a
//!   quorum against the survival goal, is moved;
//! - rebalancing by load never lowers diversity;
//! - the lease goes to the most preferred region holding a live, caught-up voter, and lease
//!   balancing stays within equally preferred stores.
//!
//! The planner is a pure function. The driver ([`crate::dist::node::NodeDriver`]) executes the
//! result, so the rules are tested without a cluster.

use std::collections::BTreeMap;

use crate::dist::zone::Survival;
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
    /// Interned region and zone (`0`: unknown; zone ids are unique across regions).
    pub region: u32,
    pub zone: u32,
}

/// A range's zone configuration as the planner sees it (localities resolved by the driver).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Placement {
    /// Voters (`0`: [`AllocatorConfig::replication`]).
    pub replicas: usize,
    /// Stores the constraints allow (`None`: every store).
    pub allowed: Option<Vec<NodeId>>,
    /// Lease preference rank of stores (lower is better; unlisted stores rank `0`).
    pub lease_rank: Vec<(NodeId, u32)>,
    pub survival: Survival,
}

impl Placement {
    fn allows(&self, n: NodeId) -> bool {
        self.allowed.as_ref().is_none_or(|a| a.contains(&n))
    }

    fn rank(&self, n: NodeId) -> u32 {
        self.lease_rank.iter().find(|(x, _)| *x == n).map_or(0, |&(_, k)| k)
    }
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
    /// Under the same zone configuration entry as the planned range (else never merged).
    pub same_zone: bool,
    /// It is absorbing its own frozen right-hand neighbour: its replicas must not change until
    /// that merge is done (the merge needs both ranges' voters equal).
    pub merging: bool,
    pub members: Vec<MemberView>,
    /// [`crate::dist::node::NodeDriver::merge_ready`].
    pub merge_ready: bool,
}

/// The left-hand neighbour of the planned range, as hosted on this node.
#[derive(Clone, Copy, Debug)]
pub struct LeftView {
    pub keys: usize,
    /// Under the same zone configuration entry as the planned range.
    pub same_zone: bool,
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
    /// A zone configuration boundary inside the range (split there first).
    pub zone_split_key: Option<Vec<u8>>,
    pub placement: Placement,
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
    let pl = &r.placement;
    let replication = if pl.replicas == 0 { cfg.replication } else { pl.replicas };
    let cool = r.writes_per_sec < cfg.split_writes_per_sec / 2;
    let merging = r.right.as_ref().is_some_and(|x| x.frozen);
    // 0. A range straddling two zone configurations splits at the boundary.
    if !merging {
        if let Some(k) = &r.zone_split_key {
            return Some(Action::Split(k.clone()));
        }
    }
    // The small right half of a pair: its replicas are the left range's plan to align (two
    // plans would undo each other's moves). It only brings the left range's leaseholder in as
    // a voter and hands it the lease, so that one node leads both.
    // It may still be the left half of its own right-hand pair (chains merge from anywhere).
    // A merge it already froze its right neighbour for comes first: handing its lease away or
    // having its replicas aligned now would strand the frozen range.
    if let Some(left) = r.left.filter(|l| !merging && l.same_zone && r.keys + l.keys < cfg.merge_keys && cool) {
        let Some(l) = left.leader.filter(|&l| l != r.me) else { return merge_step(cfg, r) };
        return match r.members.iter().find(|m| m.node == l) {
            Some(m) if m.voter && m.caught_up => Some(Action::LeaseToLeft(l)),
            Some(m) if !m.voter && m.caught_up => Some(Action::Promote(l)),
            Some(_) => None,
            None => Some(Action::AddLearner(l)),
        };
    }
    if merging {
        // Our replicas must match the frozen range's, which can no longer change. They drift
        // only if a neighbour aligned them before it saw the freeze: move them back.
        let right = r.right.as_ref().filter(|x| x.led_here)?;
        let want: Vec<NodeId> = right.members.iter().map(|m| m.node).collect();
        if let Some(m) = r.members.iter().find(|m| !want.contains(&m.node) && m.node != r.me) {
            return Some(Action::Remove(m.node));
        }
        if let Some(l) = r.members.iter().find(|m| !m.voter) {
            return (l.caught_up && live(l.node)).then_some(Action::Promote(l.node));
        }
        if let Some(&n) = want.iter().find(|&&n| !r.members.iter().any(|m| m.node == n)) {
            return live(n).then_some(Action::AddLearner(n));
        }
        return right.merge_ready.then_some(Action::Merge(right.range));
    }
    let in_pair = r.right.as_ref().is_some_and(|x| x.same_zone && r.keys + x.keys < cfg.merge_keys && cool);
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
    // Failure domain of a store under the survival goal, and how many voters share it.
    let domain = |n: NodeId| if pl.survival == Survival::Region { store(n).region } else { store(n).zone };
    let crowd = |n: NodeId, without: Option<NodeId>| voters.iter().filter(|v| Some(v.node) != without && v.node != n && domain(v.node) == domain(n)).count();
    // Diversity of a voter set: distinct regions, then distinct zones (more is better).
    let diversity = |set: &[NodeId]| {
        let mut regions: Vec<u32> = set.iter().map(|&n| store(n).region).collect();
        let mut zones: Vec<u32> = set.iter().map(|&n| store(n).zone).collect();
        regions.sort_unstable();
        regions.dedup();
        zones.sort_unstable();
        zones.dedup();
        (regions.len(), zones.len())
    };
    let voter_ids: Vec<NodeId> = voters.iter().map(|v| v.node).collect();
    let candidates: Vec<NodeId> = {
        let mut c: Vec<NodeId> = stores.iter().filter(|(&n, s)| s.live && n != r.me && pl.allows(n) && !r.members.iter().any(|m| m.node == n)).map(|(&n, _)| n).collect();
        // Spread first (a region, then a zone, with fewer voters), then load.
        let spread = |n: NodeId| {
            let same_region = voter_ids.iter().filter(|&&v| store(v).region == store(n).region).count();
            let same_zone = voter_ids.iter().filter(|&&v| store(v).zone == store(n).zone).count();
            (same_region, same_zone)
        };
        c.sort_by_key(|&n| (spread(n), score(&store(n)), n));
        c
    };
    let live_voters = voters.iter().filter(|v| live(v.node)).count();
    if live_voters < replication.min(voters.len() + candidates.len()) {
        if let Some(&n) = candidates.first() {
            return Some(Action::AddLearner(n));
        }
    }
    if voters.len() > replication {
        if let Some(p) = r.planned_removal.filter(|&p| p != r.me && voters.iter().any(|v| v.node == p)) {
            return Some(Action::Remove(p));
        }
        // Disallowed first, then dead, then the most crowded failure domain, then load.
        let victim = voters.iter().filter(|v| v.node != r.me).max_by_key(|v| (!pl.allows(v.node), !live(v.node), crowd(v.node, None), score(&store(v.node)), v.node));
        if let Some(v) = victim {
            return Some(Action::Remove(v.node));
        }
    }
    let quorum = replication / 2 + 1;
    // 1b. Constraints and survival: move a replica off a disallowed store, or out of a failure
    // domain holding a quorum, when a better store exists.
    if !r.rebalanced_this_round {
        let bad = voters.iter().find(|v| !pl.allows(v.node)).map(|v| v.node).or_else(|| {
            let crowded = voters.iter().filter(|v| domain(v.node) != 0 && crowd(v.node, None) + 1 >= quorum && replication > 1).max_by_key(|v| (v.node != r.me, score(&store(v.node)), v.node));
            crowded.map(|v| v.node)
        });
        if let Some(bad) = bad {
            // A replacement that does not land in an equally crowded domain.
            let better = candidates.iter().copied().find(|&c| !pl.allows(bad) || crowd(c, Some(bad)) + 1 < quorum);
            if let Some(add) = better {
                if bad != r.me {
                    return Some(Action::Rebalance { add, relieve: bad });
                }
                // A leaseholder never removes itself: hand the lease to an acceptable voter.
                if let Some(v) = voters.iter().filter(|v| v.node != r.me && v.caught_up && live(v.node) && pl.allows(v.node)).min_by_key(|v| (pl.rank(v.node), store(v.node).load.leases, v.node)) {
                    return Some(Action::TransferLease(v.node));
                }
            }
        }
    }
    // 1c. Lease preferences: the lease belongs in the most preferred region with a live,
    // caught-up voter.
    let my_rank = pl.rank(r.me);
    let lease_ok = |v: &&&MemberView| v.node != r.me && v.caught_up && live(v.node) && pl.allows(v.node);
    if let Some(v) = voters.iter().filter(lease_ok).filter(|v| pl.rank(v.node) < my_rank).min_by_key(|v| (pl.rank(v.node), store(v.node).load.leases, v.node)) {
        return Some(Action::TransferLease(v.node));
    }
    // 2. Rebalance replicas: from a store above the mean to one below it (a move never
    // overshoots, and never lowers diversity).
    let live_counts: Vec<u32> = stores.iter().filter(|(&n, s)| s.live || n == r.me).map(|(_, s)| s.load.replicas).collect();
    let mean_x = live_counts.iter().map(|&c| u64::from(c)).sum::<u64>() * 10 / live_counts.len().max(1) as u64;
    let above = |n: NodeId| u64::from(store(n).load.replicas) * 10 > mean_x;
    let below = |n: NodeId| u64::from(store(n).load.replicas) * 10 < mean_x;
    let keeps_diversity = |add: NodeId, remove: NodeId| {
        let after: Vec<NodeId> = voter_ids.iter().copied().filter(|&v| v != remove).chain([add]).collect();
        diversity(&after) >= diversity(&voter_ids)
    };
    if let Some(worst) = voters.iter().filter(|v| v.node != r.me).max_by_key(|v| (score(&store(v.node)), v.node)) {
        let best = candidates.iter().copied().filter(|&c| keeps_diversity(c, worst.node)).min_by_key(|&c| (score(&store(c)), c));
        if let Some(best) = best {
            if !r.rebalanced_this_round && !in_pair && above(worst.node) && below(best) && store(worst.node).load.replicas as usize >= store(best).load.replicas as usize + cfg.rebalance_slack {
                return Some(Action::Rebalance { add: best, relieve: worst.node });
            }
        }
    }
    // This store is the loaded one: hand the lease over (to an equally preferred voter), so the
    // new leaseholder can move this replica away (a leaseholder never removes itself).
    if let Some(best) = candidates.iter().copied().filter(|&c| keeps_diversity(c, r.me)).min_by_key(|&c| (score(&store(c)), c)) {
        let me = store(r.me).load.replicas as usize;
        if !r.rebalanced_this_round && !in_pair && above(r.me) && below(best) && me >= store(best).load.replicas as usize + cfg.rebalance_slack && voters.iter().all(|v| v.node == r.me || store(v.node).load.replicas as usize <= me) {
            if let Some(v) = voters.iter().filter(lease_ok).filter(|v| pl.rank(v.node) == my_rank).min_by_key(|v| (score(&store(v.node)), v.node)) {
                return Some(Action::TransferLease(v.node));
            }
        }
    }
    // 3. Rebalance leases (among equally preferred voters).
    let mine = store(r.me).load.leases;
    let total: u32 = stores.values().filter(|s| s.live).map(|s| s.load.leases).sum::<u32>() + if stores.contains_key(&r.me) { 0 } else { mine };
    let n_live = stores.iter().filter(|(&n, s)| s.live || n == r.me).count().max(1) as u32;
    let mean = total / n_live;
    if mine > mean + 1 && !in_pair {
        if let Some(v) = voters.iter().filter(lease_ok).filter(|v| pl.rank(v.node) == my_rank && store(v.node).load.leases < mean).min_by_key(|v| (store(v.node).load.leases, v.node)) {
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
    if !right.led_here || right.merging || (!right.same_zone && !right.frozen) || r.keys + right.keys >= cfg.merge_keys || r.writes_per_sec >= cfg.split_writes_per_sec / 2 {
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
        loads.iter().map(|&(n, replicas, leases, live)| (n, StoreView { load: StoreLoad { replicas, leases, bytes: 0, moving: 0 }, live, region: 0, zone: 0 })).collect()
    }

    fn voters(ns: &[NodeId]) -> Vec<MemberView> {
        ns.iter().map(|&node| MemberView { node, voter: true, caught_up: true }).collect()
    }

    fn view(members: Vec<MemberView>) -> RangeView {
        RangeView { range: 1, me: 1, members, config_settled: true, frozen: false, keys: 10, writes_per_sec: 0, size_split_key: Some(b"m".to_vec()), load_split_key: None, zone_split_key: None, placement: Placement::default(), right: None, left: None, planned_removal: None, rebalanced_this_round: false }
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
        let right = NeighbourView { range: 7, keys: 5, led_here: true, frozen: false, same_zone: true, merging: false, members: voters(&[1, 2, 4]), merge_ready: false };
        let r = RangeView { right: Some(right.clone()), ..view(voters(&[1, 2, 3])) };
        assert_eq!(plan(&cfg, &r, &s), Some(Action::RightAddLearner(7, 3)));
        let r = RangeView { right: Some(NeighbourView { members: voters(&[1, 2, 3, 4]), ..right.clone() }), ..view(voters(&[1, 2, 3])) };
        assert_eq!(plan(&cfg, &r, &s), Some(Action::RightRemove(7, 4)));
        let r = RangeView { right: Some(NeighbourView { members: voters(&[1, 2, 3]), ..right.clone() }), ..view(voters(&[1, 2, 3])) };
        assert_eq!(plan(&cfg, &r, &s), Some(Action::Freeze(7)));
        let r = RangeView { right: Some(NeighbourView { members: voters(&[1, 2, 3]), frozen: true, merge_ready: true, ..right.clone() }), ..view(voters(&[1, 2, 3])) };
        assert_eq!(plan(&cfg, &r, &s), Some(Action::Merge(7)));
        // The right half of a small pair sends its lease to the left's leaseholder.
        let r = RangeView { left: Some(LeftView { keys: 3, same_zone: true, leader: Some(2) }), ..view(voters(&[1, 2, 3])) };
        assert_eq!(plan(&cfg, &r, &s), Some(Action::LeaseToLeft(2)));
        // Merge chains: a range absorbing its frozen right neighbour finishes that merge first,
        // even when it is itself the small right half of a pair ...
        let frozen = NeighbourView { members: voters(&[1, 2, 3]), frozen: true, merge_ready: true, ..right.clone() };
        let middle = RangeView { left: Some(LeftView { keys: 3, same_zone: true, leader: Some(2) }), right: Some(frozen.clone()), ..view(voters(&[1, 2, 3])) };
        assert_eq!(plan(&cfg, &middle, &s), Some(Action::Merge(7)));
        let middle = RangeView { right: Some(NeighbourView { merge_ready: false, ..frozen.clone() }), ..middle };
        assert_eq!(plan(&cfg, &middle, &s), None);
        // Replicas that drifted from the frozen range's (aligned by a neighbour that had not
        // seen the freeze yet) move back, so the merge can still happen.
        let drifted = RangeView { right: Some(NeighbourView { merge_ready: false, ..frozen.clone() }), ..view(voters(&[1, 2, 3, 4])) };
        assert_eq!(plan(&cfg, &drifted, &s), Some(Action::Remove(4)));
        let short = RangeView { right: Some(NeighbourView { merge_ready: false, ..frozen.clone() }), ..view(voters(&[1, 2])) };
        assert_eq!(plan(&cfg, &short, &s), Some(Action::AddLearner(3)));
        // ... and its left neighbour leaves its replicas alone meanwhile.
        let r = RangeView { right: Some(NeighbourView { merging: true, ..right }), ..view(voters(&[1, 2, 3])) };
        assert_eq!(plan(&cfg, &r, &s), None);
    }

    /// Stores in (region, zone) localities: `(node, replicas, leases, region, zone)`.
    fn placed(spec: &[(NodeId, u32, u32, u32, u32)]) -> BTreeMap<NodeId, StoreView> {
        spec.iter().map(|&(n, replicas, leases, region, zone)| (n, StoreView { load: StoreLoad { replicas, leases, bytes: 0, moving: 0 }, live: true, region, zone })).collect()
    }

    #[test]
    fn zone_configs_shape_placement() {
        let cfg = AllocatorConfig::default();
        // Regions 1..3, zones 11, 12, 21, 22, 31.
        let s = placed(&[(1, 5, 1, 1, 11), (2, 1, 1, 1, 12), (3, 5, 1, 2, 21), (4, 5, 1, 2, 22), (5, 5, 1, 3, 31)]);
        // A boundary inside the range splits it first.
        let r = RangeView { zone_split_key: Some(b"k".to_vec()), ..view(voters(&[1, 3, 5])) };
        assert_eq!(plan(&cfg, &r, &s), Some(Action::Split(b"k".to_vec())));
        // Adding prefers a new region over a lightly loaded store in a region already used.
        assert_eq!(plan(&cfg, &view(voters(&[1, 5])), &s), Some(Action::AddLearner(3)));
        // Constraints: only stores 1, 2 and 5 are allowed; the replica on 3 moves to 2.
        let only = Placement { allowed: Some(vec![1, 2, 5]), ..Placement::default() };
        let r = RangeView { placement: only.clone(), ..view(voters(&[1, 3, 5])) };
        assert_eq!(plan(&cfg, &r, &s), Some(Action::Rebalance { add: 2, relieve: 3 }));
        // Over-replicated: the disallowed replica goes first.
        let r = RangeView { placement: only, ..view(voters(&[1, 2, 3, 5])) };
        assert_eq!(plan(&cfg, &r, &s), Some(Action::Remove(3)));
        // Region survival with 5 voters: region 2 holding 3 of them is a quorum; one moves.
        let s5 = placed(&[(1, 5, 1, 1, 11), (2, 5, 1, 1, 12), (3, 5, 1, 2, 21), (4, 5, 1, 2, 22), (5, 5, 1, 3, 31), (6, 5, 1, 2, 23), (7, 5, 1, 3, 32)]);
        let region = Placement { replicas: 5, survival: Survival::Region, ..Placement::default() };
        let r = RangeView { placement: region.clone(), ..view(voters(&[1, 3, 4, 5, 6])) };
        assert_eq!(plan(&cfg, &r, &s5), Some(Action::Rebalance { add: 2, relieve: 6 }));
        // Balanced 2-2-1: nothing to do.
        let r = RangeView { placement: region, ..view(voters(&[1, 2, 3, 4, 5])) };
        assert_eq!(plan(&cfg, &r, &s5), None);
        // Lease preferences: region 2 first; the lease leaves store 1 (region 1) for store 3.
        let prefs = Placement { lease_rank: vec![(1, 1), (2, 1), (3, 0), (4, 0), (5, 2)], ..Placement::default() };
        let r = RangeView { placement: prefs.clone(), ..view(voters(&[1, 3, 5])) };
        assert_eq!(plan(&cfg, &r, &s), Some(Action::TransferLease(3)));
        // On the preferred store, lease balancing stays among equally preferred voters.
        let busy = placed(&[(1, 3, 0, 1, 11), (3, 3, 9, 2, 21), (5, 3, 0, 3, 31)]);
        let r = RangeView { me: 3, placement: prefs, ..view(voters(&[1, 3, 5])) };
        assert_eq!(plan(&cfg, &r, &busy), None);
        // Neighbours under different configurations never merge.
        let small = AllocatorConfig { merge_keys: 50, ..AllocatorConfig::default() };
        let right = NeighbourView { range: 7, keys: 5, led_here: true, frozen: false, same_zone: false, merging: false, members: voters(&[1, 3, 5]), merge_ready: false };
        let r = RangeView { right: Some(right), ..view(voters(&[1, 3, 5])) };
        let even = placed(&[(1, 5, 1, 1, 11), (3, 5, 1, 2, 21), (5, 5, 1, 3, 31)]);
        assert_eq!(plan(&small, &r, &even), None);
    }
}
