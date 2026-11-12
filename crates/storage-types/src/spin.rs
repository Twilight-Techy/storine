//! Locking hot, short critical sections without sleeping.
//!
//! `std::sync::Mutex` spins only briefly before it parks a waiter in the kernel. When the lock
//! is held for about a hundred nanoseconds but taken a million times a second (the WAL append,
//! the flush queue), waiters still park now and then. Waking a parked thread costs a system
//! call, and when every core is busy the woken thread waits for a whole scheduler slice,
//! hundreds of microseconds. That is where the 99.9th-percentile write latency went.
//! [`lock_spinning`] keeps trying for a few microseconds first, which is far longer than these
//! locks are held, then parks as usual.

use std::sync::{Mutex, MutexGuard, RwLock, RwLockWriteGuard, TryLockError};
use std::time::{Duration, Instant};

/// How long [`lock_spinning`] spins before it blocks: long against the critical sections it
/// is for, short against a scheduler slice.
pub const SPIN_BUDGET: Duration = Duration::from_micros(4);

/// Locks `m`, spinning up to [`SPIN_BUDGET`] before blocking. Poisoning is ignored, as
/// everywhere in the engine: the data behind these locks is valid at every unlock.
///
/// For locks held briefly and never across I/O; a lock held long only wastes the spin.
#[inline]
pub fn lock_spinning<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    match m.try_lock() {
        Ok(g) => return g,
        Err(TryLockError::Poisoned(p)) => return p.into_inner(),
        Err(TryLockError::WouldBlock) => {}
    }
    lock_spinning_slow(m)
}

/// [`lock_spinning`] for the write side of an `RwLock`.
#[inline]
pub fn write_spinning<T>(l: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    match l.try_write() {
        Ok(g) => return g,
        Err(TryLockError::Poisoned(p)) => return p.into_inner(),
        Err(TryLockError::WouldBlock) => {}
    }
    write_spinning_slow(l)
}

#[cold]
fn write_spinning_slow<T>(l: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    let start = Instant::now();
    let mut pause = 1u32;
    loop {
        for _ in 0..pause {
            std::hint::spin_loop();
        }
        pause = (pause * 2).min(64);
        match l.try_write() {
            Ok(g) => return g,
            Err(TryLockError::Poisoned(p)) => return p.into_inner(),
            Err(TryLockError::WouldBlock) => {}
        }
        if start.elapsed() >= SPIN_BUDGET {
            return l.write().unwrap_or_else(|p| p.into_inner());
        }
    }
}

#[cold]
fn lock_spinning_slow<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    let start = Instant::now();
    let mut pause = 1u32;
    loop {
        for _ in 0..pause {
            std::hint::spin_loop();
        }
        pause = (pause * 2).min(64);
        match m.try_lock() {
            Ok(g) => return g,
            Err(TryLockError::Poisoned(p)) => return p.into_inner(),
            Err(TryLockError::WouldBlock) => {}
        }
        if start.elapsed() >= SPIN_BUDGET {
            return m.lock().unwrap_or_else(|p| p.into_inner());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn excludes_and_counts_every_increment() {
        let m = Arc::new(Mutex::new(0u64));
        let hs: Vec<_> = (0..4)
            .map(|_| {
                let m = Arc::clone(&m);
                std::thread::spawn(move || {
                    for _ in 0..100_000 {
                        *lock_spinning(&m) += 1;
                    }
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap();
        }
        assert_eq!(*lock_spinning(&m), 400_000);
    }

    #[test]
    fn blocks_past_the_budget_and_survives_poison() {
        let m = Arc::new(Mutex::new(1u32));
        let held = lock_spinning(&m);
        let m2 = Arc::clone(&m);
        let h = std::thread::spawn(move || *lock_spinning(&m2));
        std::thread::sleep(Duration::from_millis(20));
        drop(held);
        assert_eq!(h.join().unwrap(), 1);
        let m3 = Arc::clone(&m);
        let _ = std::thread::spawn(move || {
            let _g = m3.lock().unwrap();
            panic!("poison");
        })
        .join();
        assert!(m.is_poisoned());
        *lock_spinning(&m) = 2;
        assert_eq!(*lock_spinning(&m), 2);
    }
}
