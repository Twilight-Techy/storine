//! Hybrid logical clocks with a bounded clock offset (`F-GEO-03`, `TASK-139`, `RULE-DIST-01`).
//!
//! A [`Timestamp`] is `(wall nanoseconds, logical counter)`, ordered lexicographically. An
//! [`Hlc`] never goes backwards and is always at least the physical clock. Every message
//! carries the sender's timestamp, and [`Hlc::update`] moves the receiver forward, so causally
//! later events always get larger timestamps, whatever the physical clocks say.
//!
//! Clocks are hints, bounded by `max_offset`:
//! - A remote timestamp more than `max_offset` ahead of the local physical clock means the
//!   offset bound is broken. The update is refused, and with self-fencing the node stops
//!   serving until its clock is back within bounds.
//! - A read at `ts` carries the uncertainty limit `ts + max_offset`. A value written in
//!   `(ts, limit]` may have been written before the read in real time, so the read restarts
//!   above it ([`Uncertainty`]). No read misses a causally earlier write.
//! - [`Hlc::commit_wait`] (optional) waits until `ts` is certainly in the past on every clock
//!   (`physical - max_offset > ts`), giving linearizability across transactions that
//!   share no data (Spanner-style), at the cost of `max_offset` latency.

use core::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// A hybrid logical timestamp. `Timestamp::ZERO` is before everything.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp {
    pub wall: u64,
    pub logical: u32,
}

impl Timestamp {
    pub const ZERO: Timestamp = Timestamp { wall: 0, logical: 0 };
    pub const MAX: Timestamp = Timestamp { wall: u64::MAX, logical: u32::MAX };

    pub const fn new(wall: u64, logical: u32) -> Self {
        Self { wall, logical }
    }

    /// The next timestamp after this one.
    pub fn next(self) -> Self {
        match self.logical.checked_add(1) {
            Some(l) => Self { wall: self.wall, logical: l },
            None => Self { wall: self.wall.saturating_add(1), logical: 0 },
        }
    }

    /// The immediately preceding timestamp (saturating at zero).
    pub fn prev(self) -> Self {
        if self.logical > 0 {
            Self { wall: self.wall, logical: self.logical - 1 }
        } else if self.wall > 0 {
            Self { wall: self.wall - 1, logical: u32::MAX }
        } else {
            Self::ZERO
        }
    }

    pub fn add_nanos(self, ns: u64) -> Self {
        Self { wall: self.wall.saturating_add(ns), logical: self.logical }
    }

    /// 12-byte big-endian encoding (sorts like the timestamp).
    pub fn to_bytes(self) -> [u8; 12] {
        let mut b = [0u8; 12];
        b[..8].copy_from_slice(&self.wall.to_be_bytes());
        b[8..].copy_from_slice(&self.logical.to_be_bytes());
        b
    }

    pub fn from_bytes(b: &[u8]) -> Option<Self> {
        if b.len() != 12 {
            return None;
        }
        Some(Self { wall: u64::from_be_bytes(b[..8].try_into().ok()?), logical: u32::from_be_bytes(b[8..].try_into().ok()?) })
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{:09},{}", self.wall / 1_000_000_000, self.wall % 1_000_000_000, self.logical)
    }
}

/// A physical clock in nanoseconds (wall time). Real nodes use [`SystemClock`]; the simulator
/// and tests use [`ManualClock`].
pub trait Clock: Send + Sync + fmt::Debug {
    fn now_ns(&self) -> u64;
}

/// The system's wall clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ns(&self) -> u64 {
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
    }
}

/// A clock set by hand (simulation, tests).
#[derive(Debug, Default, Clone)]
pub struct ManualClock(Arc<AtomicU64>);

impl ManualClock {
    pub fn new(ns: u64) -> Self {
        Self(Arc::new(AtomicU64::new(ns)))
    }
    pub fn set(&self, ns: u64) {
        self.0.store(ns, Ordering::Relaxed);
    }
    pub fn advance(&self, ns: u64) {
        self.0.fetch_add(ns, Ordering::Relaxed);
    }
}

impl Clock for ManualClock {
    fn now_ns(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Why a timestamp was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClockError {
    /// A remote timestamp is further ahead of our clock than the configured maximum offset.
    OffsetExceeded { remote: Timestamp, local_physical: u64, max_offset_ns: u64 },
    /// This node fenced itself after detecting an offset violation.
    Fenced,
}

impl fmt::Display for ClockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OffsetExceeded { remote, local_physical, max_offset_ns } => write!(
                f,
                "clock offset exceeds the configured maximum: remote timestamp {remote} is more than {}ms ahead of this node's clock ({local_physical}); check NTP",
                max_offset_ns / 1_000_000
            ),
            Self::Fenced => write!(f, "this node stopped serving: its clock was found outside the maximum offset"),
        }
    }
}

/// A hybrid logical clock.
#[derive(Debug)]
pub struct Hlc {
    clock: Arc<dyn Clock>,
    max_offset_ns: u64,
    state: Mutex<Timestamp>,
    fence_on_violation: bool,
    fenced: AtomicBool,
}

impl Hlc {
    pub fn new(clock: Arc<dyn Clock>, max_offset: Duration) -> Self {
        Self {
            clock,
            max_offset_ns: u64::try_from(max_offset.as_nanos()).unwrap_or(u64::MAX),
            state: Mutex::new(Timestamp::ZERO),
            fence_on_violation: true,
            fenced: AtomicBool::new(false),
        }
    }

    /// Whether an offset violation fences this node (default `true`).
    #[must_use]
    pub fn with_self_fencing(mut self, on: bool) -> Self {
        self.fence_on_violation = on;
        self
    }

    pub fn max_offset_ns(&self) -> u64 {
        self.max_offset_ns
    }

    pub fn physical_now(&self) -> u64 {
        self.clock.now_ns()
    }

    pub fn is_fenced(&self) -> bool {
        self.fenced.load(Ordering::Acquire)
    }

    /// Lifts the fence (an operator decision once the clock is fixed).
    pub fn unfence(&self) {
        self.fenced.store(false, Ordering::Release);
    }

    /// A timestamp for a local event: greater than every timestamp issued or observed so far.
    pub fn now(&self) -> Timestamp {
        let pt = self.clock.now_ns();
        let mut st = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let next = if pt > st.wall { Timestamp::new(pt, 0) } else { st.next() };
        *st = next;
        next
    }

    /// The latest timestamp issued or observed, without advancing.
    pub fn peek(&self) -> Timestamp {
        *self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Observes a timestamp from another node: the clock moves past it (causality), unless
    /// it is further ahead than `max_offset`, which is refused (and fences this node if
    /// configured).
    pub fn update(&self, remote: Timestamp) -> Result<Timestamp, ClockError> {
        if self.is_fenced() {
            return Err(ClockError::Fenced);
        }
        let pt = self.clock.now_ns();
        if remote.wall > pt.saturating_add(self.max_offset_ns) {
            if self.fence_on_violation {
                self.fenced.store(true, Ordering::Release);
            }
            return Err(ClockError::OffsetExceeded { remote, local_physical: pt, max_offset_ns: self.max_offset_ns });
        }
        let mut st = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let wall = pt.max(st.wall).max(remote.wall);
        let logical = if wall == st.wall && wall == remote.wall {
            st.logical.max(remote.logical).saturating_add(1)
        } else if wall == st.wall {
            st.logical.saturating_add(1)
        } else if wall == remote.wall {
            remote.logical.saturating_add(1)
        } else {
            0
        };
        *st = Timestamp::new(wall, logical);
        Ok(*st)
    }

    /// The uncertainty window of a read starting now at `ts`.
    pub fn uncertainty(&self, ts: Timestamp) -> Uncertainty {
        Uncertainty { read_ts: ts, limit: ts.add_nanos(self.max_offset_ns) }
    }

    /// Blocks until `ts` is in the past on every node's clock (`physical - max_offset > ts`).
    /// Returns how long it waited.
    pub fn commit_wait(&self, ts: Timestamp) -> Duration {
        let target = ts.wall.saturating_add(self.max_offset_ns);
        let start = self.clock.now_ns();
        let mut now = start;
        while now <= target {
            std::thread::sleep(Duration::from_nanos((target - now).clamp(1_000, 1_000_000)));
            now = self.clock.now_ns();
        }
        Duration::from_nanos(now.saturating_sub(start))
    }
}

/// The uncertainty window `(read_ts, limit]` of a read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Uncertainty {
    pub read_ts: Timestamp,
    pub limit: Timestamp,
}

impl Uncertainty {
    /// A value written at `ts` is uncertain for this read: it may precede the read in real
    /// time, so the read must restart at or above `ts`.
    pub fn is_uncertain(&self, ts: Timestamp) -> bool {
        ts > self.read_ts && ts <= self.limit
    }

    /// The window after restarting the read at `ts` (the limit stays: it bounds the clock
    /// offset at the read's start, not at the restart).
    pub fn restart_at(&self, ts: Timestamp) -> Uncertainty {
        Uncertainty { read_ts: ts.max(self.read_ts), limit: self.limit }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hlc(clock: &ManualClock, max_ms: u64) -> Hlc {
        Hlc::new(Arc::new(clock.clone()), Duration::from_millis(max_ms))
    }

    #[test]
    fn monotonic_even_when_the_physical_clock_goes_back() {
        let c = ManualClock::new(1_000_000);
        let h = hlc(&c, 500);
        let a = h.now();
        c.set(500_000); // clock jumps back
        let b = h.now();
        let d = h.now();
        assert!(a < b && b < d, "{a} {b} {d}");
        c.set(2_000_000);
        let e = h.now();
        assert_eq!(e, Timestamp::new(2_000_000, 0));
    }

    #[test]
    fn updates_preserve_causality() {
        let (ca, cb) = (ManualClock::new(10_000_000), ManualClock::new(9_000_000));
        let (a, b) = (hlc(&ca, 500), hlc(&cb, 500));
        // `a` sends at its time; `b`'s clock is 1 ms behind but its next event is still later.
        let sent = a.now();
        let recv = b.update(sent).unwrap();
        assert!(recv > sent);
        assert!(b.now() > sent);
        // Equal walls: logical counters break the tie.
        let x = Timestamp::new(20_000_000, 7);
        ca.set(20_000_000);
        let got = a.update(x).unwrap();
        assert!(got > x && got.wall == x.wall);
    }

    #[test]
    fn offsets_beyond_the_bound_are_refused_and_fence() {
        let c = ManualClock::new(1_000_000_000);
        let h = hlc(&c, 250);
        // 200 ms ahead: fine.
        assert!(h.update(Timestamp::new(1_200_000_000, 0)).is_ok());
        // 300 ms ahead: refused, node fenced.
        let err = h.update(Timestamp::new(1_300_000_001, 0)).unwrap_err();
        assert!(matches!(err, ClockError::OffsetExceeded { .. }));
        assert!(h.is_fenced());
        assert_eq!(h.update(Timestamp::new(1_000_000_000, 0)), Err(ClockError::Fenced));
        h.unfence();
        assert!(h.update(Timestamp::new(1_000_000_000, 0)).is_ok());
        // Without self-fencing only the update is refused.
        let h = hlc(&c, 250).with_self_fencing(false);
        assert!(h.update(Timestamp::new(2_000_000_000, 0)).is_err());
        assert!(!h.is_fenced());
    }

    #[test]
    fn uncertainty_windows() {
        let c = ManualClock::new(1_000_000_000);
        let h = hlc(&c, 100);
        let ts = h.now();
        let u = h.uncertainty(ts);
        assert!(!u.is_uncertain(ts));
        assert!(u.is_uncertain(ts.add_nanos(50_000_000)));
        assert!(u.is_uncertain(ts.add_nanos(100_000_000)));
        assert!(!u.is_uncertain(ts.add_nanos(100_000_001)));
        let r = u.restart_at(ts.add_nanos(50_000_000));
        assert!(!r.is_uncertain(ts.add_nanos(50_000_000)));
        assert!(r.is_uncertain(ts.add_nanos(60_000_000)));
        assert_eq!(r.limit, u.limit);
    }

    #[test]
    fn commit_wait_waits_out_the_offset() {
        let h = Hlc::new(Arc::new(SystemClock), Duration::from_millis(5));
        let ts = h.now();
        let waited = h.commit_wait(ts);
        assert!(waited >= Duration::from_millis(5), "{waited:?}");
        assert!(h.physical_now() > ts.wall + 5_000_000);
    }

    #[test]
    fn timestamp_bytes_sort_like_timestamps() {
        let ts = [Timestamp::new(1, 5), Timestamp::new(1, 6), Timestamp::new(2, 0), Timestamp::new(u64::MAX, 0)];
        for w in ts.windows(2) {
            assert!(w[0].to_bytes() < w[1].to_bytes());
            assert_eq!(Timestamp::from_bytes(&w[0].to_bytes()), Some(w[0]));
        }
        assert_eq!(Timestamp::new(3, u32::MAX).next(), Timestamp::new(4, 0));
        assert_eq!(Timestamp::new(4, 0).prev(), Timestamp::new(3, u32::MAX));
    }
}
