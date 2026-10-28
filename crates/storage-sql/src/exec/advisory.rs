//! Application-defined locks: PostgreSQL advisory locks (`pg_advisory_lock` and family), MySQL
//! named locks (`GET_LOCK`, `RELEASE_LOCK`, ...) and SQL Server application locks
//! (`sp_getapplock` / `sp_releaseapplock`). Migration tools and job schedulers use them to make
//! sure only one process runs a task.
//!
//! A lock is a document in the database's `_sql_advisory` collection holding, per owner (gateway
//! instance + session), how many times it holds the lock exclusively and shared, and when its
//! lease expires. Every change is a version-conditioned write, so gateways sharing a store see
//! the same locks. A background thread renews the leases of live sessions; the locks of a
//! gateway that stopped without releasing them lapse after [`LEASE`].

use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use storine::{Doc, Value};

use crate::error::{SqlError, SqlResult};
use crate::exec::Session;
use crate::store::{CommitError, Op, Pre, Store, Tag, Write};

/// Collection of lock documents (one namespace per database, like every `_sql_` collection).
pub const ADVISORY: &str = "_sql_advisory";
/// How long a lock outlives its gateway without renewal.
const LEASE: Duration = Duration::from_secs(60);
/// How often live leases are renewed.
const RENEW_EVERY: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Mode {
    Exclusive,
    Shared,
}

/// When a lock is released by itself: at the end of the session or of the transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Scope {
    Session,
    Xact,
}

/// The locks one session holds (its own bookkeeping; the store holds the shared truth).
#[derive(Debug, Default)]
pub struct Held {
    counts: HashMap<(String, Mode, Scope), u32>,
}

impl Held {
    fn add(&mut self, key: &str, mode: Mode, scope: Scope) {
        *self
            .counts
            .entry((key.to_owned(), mode, scope))
            .or_default() += 1;
    }

    /// Takes one hold of `key` in `mode` (session scope first, as PostgreSQL's unlock does).
    fn take(&mut self, key: &str, mode: Mode, scope: Option<Scope>) -> bool {
        for sc in [Scope::Session, Scope::Xact] {
            if scope.is_some_and(|s| s != sc) {
                continue;
            }
            let k = (key.to_owned(), mode, sc);
            if let Some(n) = self.counts.get_mut(&k) {
                *n -= 1;
                if *n == 0 {
                    self.counts.remove(&k);
                }
                return true;
            }
        }
        false
    }

    fn holds(&self, key: &str) -> bool {
        self.counts.keys().any(|(k, _, _)| k == key)
    }

    /// Removes every hold of `scope` (or all) whose key starts with `prefix`; `(key, mode,
    /// count)` of what was removed.
    fn drain(&mut self, scope: Option<Scope>, prefix: &str) -> Vec<(String, Mode, u32)> {
        let keys: Vec<_> = self
            .counts
            .keys()
            .filter(|(k, _, s)| scope.is_none_or(|sc| sc == *s) && k.starts_with(prefix))
            .cloned()
            .collect();
        keys.into_iter()
            .filter_map(|k| self.counts.remove(&k).map(|n| (k.0, k.1, n)))
            .collect()
    }
}

/// The owner id stored in lock documents: gateway instance and session.
pub fn owner(s: &Session) -> String {
    format!("{}#{}", crate::instance_id(), s.id)
}

fn now_ns() -> i64 {
    crate::exec::now_ns()
}

fn count(m: &Doc, field: &str) -> i64 {
    match m.get(field) {
        Some(Value::Int(n)) => *n,
        _ => 0,
    }
}

/// Holders of a lock document: owner -> (exclusive, shared, lease expiry).
fn holders(doc: Option<&Doc>) -> BTreeMap<String, (i64, i64, i64)> {
    let mut out = BTreeMap::new();
    if let Some(Value::Map(h)) = doc.and_then(|d| d.get("h")) {
        for (o, v) in h {
            if let Value::Map(m) = v {
                let exp = match m.get("exp") {
                    Some(Value::Timestamp(t)) => *t,
                    _ => 0,
                };
                out.insert(o.clone(), (count(m, "x"), count(m, "s"), exp));
            }
        }
    }
    out
}

fn holders_doc(h: &BTreeMap<String, (i64, i64, i64)>) -> Doc {
    let mut map = Doc::new();
    for (o, (x, sh, exp)) in h {
        let mut m = Doc::new();
        m.insert("x".into(), Value::Int(*x));
        m.insert("s".into(), Value::Int(*sh));
        m.insert("exp".into(), Value::Timestamp(*exp));
        map.insert(o.clone(), Value::Map(m));
    }
    let mut d = Doc::new();
    d.insert("h".into(), Value::Map(map));
    d
}

/// Reads a lock, lets `change` edit its holders (expired ones other than `me` already pruned),
/// and writes it back conditioned on the version read, retrying on a race. `change` returns
/// `None` to leave the lock as it is.
fn update<T>(
    store: &Store,
    key: &str,
    me: &str,
    mut change: impl FnMut(&mut BTreeMap<String, (i64, i64, i64)>) -> Option<T>,
) -> SqlResult<Option<T>> {
    loop {
        let doc = store.get(ADVISORY, key)?;
        let mut h = holders(doc.as_ref().map(|d| &d.fields));
        let now = now_ns();
        let before = h.len();
        h.retain(|o, (_, _, exp)| o == me || *exp >= now);
        let pruned = h.len() != before;
        let result = change(&mut h);
        if result.is_none() && !pruned {
            return Ok(None);
        }
        let pre = doc
            .as_ref()
            .map_or(Pre::Absent, |d| Pre::Version(d.version));
        let op = if h.is_empty() {
            Op::Delete
        } else {
            Op::Put(holders_doc(&h))
        };
        if doc.is_none() && h.is_empty() {
            return Ok(result);
        }
        let w = Write {
            collection: ADVISORY.into(),
            key: key.to_owned(),
            op,
            pre,
            tag: Tag::Lock,
        };
        match store.commit(std::slice::from_ref(&w)) {
            Ok(_) => return Ok(result),
            Err(CommitError::Condition { .. }) | Err(CommitError::Missing { .. }) => continue,
            Err(e) => return Err(e.to_sql(std::slice::from_ref(&w))),
        }
    }
}

fn lease_end() -> i64 {
    now_ns() + LEASE.as_nanos() as i64
}

/// One attempt: grants the lock (`true`) when no other owner conflicts.
fn try_take(store: &Store, key: &str, me: &str, mode: Mode) -> SqlResult<bool> {
    let got = update(store, key, me, |h| {
        let conflict = h
            .iter()
            .any(|(o, (x, sh, _))| o != me && (*x > 0 || (mode == Mode::Exclusive && *sh > 0)));
        if conflict {
            return None;
        }
        let e = h.entry(me.to_owned()).or_insert((0, 0, 0));
        match mode {
            Mode::Exclusive => e.0 += 1,
            Mode::Shared => e.1 += 1,
        }
        e.2 = lease_end();
        Some(())
    })?;
    Ok(got.is_some())
}

/// Gives back one hold of `mode`.
fn give_back(store: &Store, key: &str, me: &str, mode: Mode, n: u32) -> SqlResult<()> {
    update(store, key, me, |h| {
        let e = h.get_mut(me)?;
        let c = match mode {
            Mode::Exclusive => &mut e.0,
            Mode::Shared => &mut e.1,
        };
        *c = (*c - i64::from(n)).max(0);
        if e.0 == 0 && e.1 == 0 {
            h.remove(me);
        }
        Some(())
    })?;
    Ok(())
}

// ── lease renewal ──

type Leases = Mutex<HashMap<(String, String), (Store, String)>>;

fn leases() -> &'static Leases {
    static LEASES: OnceLock<Leases> = OnceLock::new();
    LEASES.get_or_init(|| {
        std::thread::Builder::new()
            .name("advisory-leases".into())
            .spawn(|| loop {
                std::thread::sleep(RENEW_EVERY);
                let live: Vec<(Store, String, String)> = match leases().lock() {
                    Ok(l) => l
                        .iter()
                        .map(|((o, _), (st, k))| (st.clone(), k.clone(), o.clone()))
                        .collect(),
                    Err(_) => continue,
                };
                for (store, key, me) in live {
                    let _ = update(&store, &key, &me, |h| {
                        let e = h.get_mut(&me)?;
                        e.2 = lease_end();
                        Some(())
                    });
                }
            })
            .ok();
        Mutex::new(HashMap::new())
    })
}

fn lease_key(s: &Session, key: &str) -> (String, String) {
    // The database is part of the key: the same lock name in two databases is two locks.
    (owner(s), format!("{}\u{0}{key}", s.database))
}

fn track(s: &Session, key: &str) {
    if let Ok(mut l) = leases().lock() {
        l.insert(lease_key(s, key), (s.engine.store.clone(), key.to_owned()));
    }
}

fn untrack_if_free(s: &Session, held: &Held, key: &str) {
    if !held.holds(key) {
        if let Ok(mut l) = leases().lock() {
            l.remove(&lease_key(s, key));
        }
    }
}

// ── operations ──

/// Takes `key` in `mode` for `scope`, waiting up to `timeout` (`None`: forever,
/// `Some(ZERO)`: one attempt). `Ok(false)` when it timed out.
pub fn acquire(
    s: &Session,
    key: &str,
    mode: Mode,
    scope: Scope,
    timeout: Option<Duration>,
) -> SqlResult<bool> {
    let me = owner(s);
    let deadline = timeout.map(|t| Instant::now() + t);
    let mut delay = Duration::from_millis(1);
    loop {
        if try_take(&s.engine.store, key, &me, mode)? {
            let mut held = s
                .advisory
                .lock()
                .map_err(|_| SqlError::internal("lock table poisoned"))?;
            held.add(key, mode, scope);
            track(s, key);
            return Ok(true);
        }
        if deadline.is_some_and(|d| Instant::now() >= d) {
            return Ok(false);
        }
        std::thread::sleep(delay);
        delay = (delay * 2).min(Duration::from_millis(50));
    }
}

/// Releases one hold of `key` in `mode` (of `scope`, or either). `false` if the session did
/// not hold it.
pub fn release(s: &Session, key: &str, mode: Mode, scope: Option<Scope>) -> SqlResult<bool> {
    let mut held = s
        .advisory
        .lock()
        .map_err(|_| SqlError::internal("lock table poisoned"))?;
    if !held.take(key, mode, scope) {
        return Ok(false);
    }
    give_back(&s.engine.store, key, &owner(s), mode, 1)?;
    untrack_if_free(s, &held, key);
    Ok(true)
}

/// Releases every lock of `scope` (or all) whose key starts with `prefix` (`""`: any); how many
/// holds were released.
pub fn release_all(s: &Session, scope: Option<Scope>, prefix: &str) -> u64 {
    let Ok(mut held) = s.advisory.lock() else {
        return 0;
    };
    let drained = held.drain(scope, prefix);
    let me = owner(s);
    let mut n = 0;
    for (key, mode, c) in drained {
        n += u64::from(c);
        let _ = give_back(&s.engine.store, &key, &me, mode, c);
        untrack_if_free(s, &held, &key);
    }
    n
}

/// Whether `key` is held by anyone (live lease); the holding session's id if one owner holds it.
pub fn holder(s: &Session, key: &str) -> SqlResult<Option<Option<u64>>> {
    let doc = s.engine.store.get(ADVISORY, key)?;
    let now = now_ns();
    let live: Vec<String> = holders(doc.as_ref().map(|d| &d.fields))
        .into_iter()
        .filter(|(_, (x, sh, exp))| (*x > 0 || *sh > 0) && *exp >= now)
        .map(|(o, _)| o)
        .collect();
    Ok(match live.as_slice() {
        [] => None,
        [one] => Some(one.rsplit('#').next().and_then(|id| id.parse().ok())),
        _ => Some(None),
    })
}

/// How long a blocking PostgreSQL lock waits: the session's `lock_timeout` (`0`: forever).
pub fn pg_lock_timeout(s: &Session) -> Option<Duration> {
    let v = s.vars.get("lock_timeout")?.trim().to_ascii_lowercase();
    let (num, unit) = v.split_at(
        v.find(|c: char| !c.is_ascii_digit() && c != '.')
            .unwrap_or(v.len()),
    );
    let n: f64 = num.parse().ok()?;
    let ms = match unit.trim() {
        "" | "ms" => n,
        "s" => n * 1e3,
        "min" => n * 60e3,
        "h" => n * 3600e3,
        "us" => n / 1e3,
        _ => return None,
    };
    (ms > 0.0).then(|| Duration::from_secs_f64(ms / 1e3))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn held_counts_by_scope() {
        let mut h = Held::default();
        h.add("k", Mode::Exclusive, Scope::Session);
        h.add("k", Mode::Exclusive, Scope::Xact);
        h.add("k", Mode::Shared, Scope::Xact);
        assert!(h.take("k", Mode::Exclusive, None));
        assert!(h.holds("k"));
        let xact = h.drain(Some(Scope::Xact), "");
        assert_eq!(xact.len(), 2);
        assert!(!h.holds("k"));
        assert!(!h.take("k", Mode::Exclusive, None));
    }
}
