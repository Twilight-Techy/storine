//! Linearizability checking of client histories (`F-SIM-03`, `RULE-TEST-03`).
//!
//! The checker is Wing & Gong's search with Lowe's memoisation (as in Knossos and Porcupine).
//! - It looks for a total order of the operations that respects real time (an operation that
//!   returned before another was invoked comes first) and that a sequential model accepts.
//! - (linearized set, model state) pairs already explored are cached, so no search repeats.
//! - A history is linearizable iff each key's sub-history is (P-compositionality), so keys are
//!   checked one at a time, which keeps the search small.
//! - Operations whose outcome is unknown (a timeout, `CommitUnknown`, a crashed server) get a
//!   return time of infinity. They may take effect at any point after their invocation, or
//!   never: the search can always place them last, where they are invisible.

use std::collections::{BTreeMap, HashSet};
use std::hash::Hash;

/// A sequential specification.
pub trait Model {
    type State: Clone + Eq + Hash + core::fmt::Debug;
    type Op: Clone + core::fmt::Debug;
    fn init(&self) -> Self::State;
    /// The state after `op`, or `None` if `op` (with its observed output) is impossible here.
    fn step(&self, state: &Self::State, op: &Self::Op) -> Option<Self::State>;
}

/// One completed or pending operation.
#[derive(Clone, Debug)]
pub struct Event<Op> {
    /// Invocation time.
    pub call: u64,
    /// Return time; `None` when the outcome is unknown.
    pub ret: Option<u64>,
    pub op: Op,
}

/// Why a history is not linearizable, or why the search gave up.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CheckError {
    /// No valid order exists. `partial` is the longest prefix of operations (indexes into the
    /// checked history) that could be linearized.
    NotLinearizable { partial: Vec<usize> },
    /// The search exceeded its step budget (the history is too concurrent to decide).
    Inconclusive,
}

/// Checks one history against `model` with at most `budget` search steps.
pub fn check<M: Model>(model: &M, history: &[Event<M::Op>], budget: u64) -> Result<(), CheckError> {
    let n = history.len();
    if n == 0 {
        return Ok(());
    }
    // Entries: 2 per operation (call, return), sorted by time; calls before returns on ties,
    // so operations touching at one instant count as concurrent.
    #[derive(Clone, Copy)]
    struct Entry {
        op: usize,
        is_call: bool,
        time: u64,
    }
    let mut entries: Vec<Entry> = Vec::with_capacity(2 * n);
    for (i, e) in history.iter().enumerate() {
        entries.push(Entry { op: i, is_call: true, time: e.call });
        entries.push(Entry { op: i, is_call: false, time: e.ret.unwrap_or(u64::MAX) });
    }
    entries.sort_by_key(|e| (e.time, !e.is_call, e.op));
    // Doubly linked list over `entries` with a head sentinel at index `len`.
    let len = entries.len();
    let head = len;
    let mut next: Vec<usize> = (1..=len).collect();
    next.push(0);
    let mut prev: Vec<usize> = (0..=len).map(|i| if i == 0 { head } else { i - 1 }).collect();
    prev[len] = 0;
    next[head] = 0;
    let end = usize::MAX;
    next[len - 1] = end;
    let mut ret_of = vec![0usize; n];
    for (i, e) in entries.iter().enumerate() {
        if !e.is_call {
            ret_of[e.op] = i;
        }
    }
    let unlink = |next: &mut Vec<usize>, prev: &mut Vec<usize>, i: usize| {
        let (p, q) = (prev[i], next[i]);
        next[p] = q;
        if q != end {
            prev[q] = p;
        }
    };
    let relink = |next: &mut Vec<usize>, prev: &mut Vec<usize>, i: usize| {
        let (p, q) = (prev[i], next[i]);
        next[p] = i;
        if q != end {
            prev[q] = i;
        }
    };

    let words = n.div_ceil(64);
    let mut linearized = vec![0u64; words];
    let mut cache: HashSet<(Vec<u64>, M::State)> = HashSet::new();
    let mut state = model.init();
    let mut stack: Vec<(usize, M::State)> = Vec::new();
    let mut best: Vec<usize> = Vec::new();
    let mut steps = 0u64;
    // Operations with a known outcome not yet placed. At zero, only unknown ones remain, and
    // those may never have taken effect.
    let mut determinate_left = history.iter().filter(|e| e.ret.is_some()).count();
    let mut cur = next[head];
    loop {
        if determinate_left == 0 {
            return Ok(());
        }
        steps += 1;
        if steps > budget {
            return Err(CheckError::Inconclusive);
        }
        let e = entries[cur];
        if e.is_call {
            let op = e.op;
            if let Some(new_state) = model.step(&state, &history[op].op) {
                let mut lin = linearized.clone();
                lin[op / 64] |= 1 << (op % 64);
                if cache.insert((lin.clone(), new_state.clone())) {
                    stack.push((cur, state));
                    state = new_state;
                    linearized = lin;
                    unlink(&mut next, &mut prev, cur);
                    unlink(&mut next, &mut prev, ret_of[op]);
                    determinate_left -= usize::from(history[op].ret.is_some());
                    if stack.len() > best.len() {
                        best = stack.iter().map(|(c, _)| entries[*c].op).collect();
                    }
                    cur = next[head];
                    continue;
                }
            }
            cur = next[cur];
        } else {
            // A return whose call is still unlinearized: this branch is dead.
            let Some((c, s)) = stack.pop() else {
                return Err(CheckError::NotLinearizable { partial: best });
            };
            let op = entries[c].op;
            linearized[op / 64] &= !(1 << (op % 64));
            determinate_left += usize::from(history[op].ret.is_some());
            state = s;
            relink(&mut next, &mut prev, ret_of[op]);
            relink(&mut next, &mut prev, c);
            cur = next[c];
        }
    }
}

/// A read/write register per key, the model for key-value histories.
#[derive(Debug, Clone, Copy, Default)]
pub struct Register;

/// A register operation with its observed output.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RegisterOp {
    Write(i64),
    /// A read and the value it returned (`None`: absent).
    Read(Option<i64>),
}

impl Model for Register {
    type State = Option<i64>;
    type Op = RegisterOp;
    fn init(&self) -> Option<i64> {
        None
    }
    fn step(&self, state: &Option<i64>, op: &RegisterOp) -> Option<Option<i64>> {
        match op {
            RegisterOp::Write(v) => Some(Some(*v)),
            RegisterOp::Read(r) => (r == state).then_some(*state),
        }
    }
}

/// Checks a key-value history one key at a time (P-compositionality). Returns the first key
/// whose history is not linearizable, with the error.
pub fn check_keyed<K: Ord + Clone + core::fmt::Debug>(history: &[(K, Event<RegisterOp>)], budget: u64) -> Result<(), (K, CheckError)> {
    let mut by_key: BTreeMap<K, Vec<Event<RegisterOp>>> = BTreeMap::new();
    for (k, e) in history {
        // A read whose result is unknown constrains nothing.
        if matches!(e.op, RegisterOp::Read(_)) && e.ret.is_none() {
            continue;
        }
        by_key.entry(k.clone()).or_default().push(e.clone());
    }
    for (k, events) in by_key {
        check(&Register, &events, budget).map_err(|e| (k, e))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(call: u64, ret: Option<u64>, op: RegisterOp) -> Event<RegisterOp> {
        Event { call, ret, op }
    }
    use RegisterOp::{Read, Write};

    #[test]
    fn sequential_histories() {
        let h = [ev(0, Some(1), Write(1)), ev(2, Some(3), Read(Some(1))), ev(4, Some(5), Write(2)), ev(6, Some(7), Read(Some(2)))];
        assert_eq!(check(&Register, &h, 1_000), Ok(()));
        let stale = [ev(0, Some(1), Write(1)), ev(2, Some(3), Write(2)), ev(4, Some(5), Read(Some(1)))];
        assert!(matches!(check(&Register, &stale, 1_000), Err(CheckError::NotLinearizable { .. })));
    }

    #[test]
    fn concurrency_allows_either_order() {
        // Write(2) overlaps the read: the read may see 1 or 2, but not something else.
        let base = |r| [ev(0, Some(1), Write(1)), ev(2, Some(10), Write(2)), ev(3, Some(4), Read(r))];
        assert_eq!(check(&Register, &base(Some(1)), 1_000), Ok(()));
        assert_eq!(check(&Register, &base(Some(2)), 1_000), Ok(()));
        assert!(check(&Register, &base(Some(3)), 1_000).is_err());
        assert!(check(&Register, &base(None), 1_000).is_err());
    }

    #[test]
    fn reads_cannot_go_back_in_time() {
        // Two sequential reads during one long write: 2 then 1 is impossible.
        let h = [ev(0, Some(1), Write(1)), ev(2, Some(20), Write(2)), ev(3, Some(4), Read(Some(2))), ev(5, Some(6), Read(Some(1)))];
        assert!(check(&Register, &h, 1_000).is_err());
    }

    #[test]
    fn unknown_outcomes_may_or_may_not_apply() {
        let applied = [ev(0, None, Write(7)), ev(5, Some(6), Read(Some(7)))];
        assert_eq!(check(&Register, &applied, 1_000), Ok(()));
        let not_applied = [ev(0, None, Write(7)), ev(5, Some(6), Read(None))];
        assert_eq!(check(&Register, &not_applied, 1_000), Ok(()));
        // ...but it cannot apply before it was invoked.
        let early = [ev(0, Some(1), Read(Some(7))), ev(5, None, Write(7))];
        assert!(check(&Register, &early, 1_000).is_err());
        // Once observed, it stays applied for later reads.
        let flicker = [ev(0, None, Write(7)), ev(5, Some(6), Read(Some(7))), ev(7, Some(8), Read(None))];
        assert!(check(&Register, &flicker, 1_000).is_err());
    }

    #[test]
    fn keys_are_checked_independently() {
        let h = vec![
            ("a", ev(0, Some(1), Write(1))),
            ("b", ev(0, Some(1), Write(5))),
            ("a", ev(2, Some(3), Read(Some(1)))),
            ("b", ev(2, Some(3), Read(Some(4)))),
        ];
        let err = check_keyed(&h, 1_000).unwrap_err();
        assert_eq!(err.0, "b");
    }

    #[test]
    fn larger_random_linearizable_histories_pass() {
        // A sequential register executed with overlapping intervals around each real step.
        let mut rng = storage_types::testing::SweepRng::new(9);
        for _ in 0..50 {
            let mut h = Vec::new();
            let mut value = None;
            let mut t = 100u64;
            for i in 0..60 {
                t += 10;
                let lin = t;
                let call = lin - rng.below(25) as u64;
                let ret = lin + rng.below(25) as u64;
                let op = if rng.below(2) == 0 {
                    value = Some(i);
                    Write(i)
                } else {
                    Read(value)
                };
                h.push(ev(call, if rng.below(20) == 0 && matches!(op, Write(_)) { None } else { Some(ret) }, op));
            }
            assert_eq!(check(&Register, &h, 1_000_000), Ok(()));
        }
    }
}
