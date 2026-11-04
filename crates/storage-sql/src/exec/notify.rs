//! `LISTEN` / `NOTIFY` (`F-ENT-05`, `TASK-149`).
//!
//! A notification reaches the sessions listening on its channel when its transaction commits; a
//! rollback drops it. Other gateways learn through documents in `_sql_notify`, polled while a local
//! session listens: delivery across gateways takes up to [`POLL`] and is best effort (a gateway that
//! was down misses what it would have read, as in PostgreSQL a disconnected client does).

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use storine::{Doc, Value};

use crate::error::SqlResult;
use crate::exec::control::Registry;
use crate::exec::{now_ns, Engine, Outcome, Session};
use crate::store::{Op, Pre, Tag, Write};

const BUS: &str = "_sql_notify";
const POLL: Duration = Duration::from_millis(100);
/// How long bus documents stay for other gateways to read.
const KEEP_NS: i64 = 60_000_000_000;
/// Most notifications waiting for one session: the oldest are dropped beyond this.
const MAX_QUEUED: usize = 10_000;

/// One notification, as the wire protocol sends it.
#[derive(Debug, Clone, PartialEq)]
pub struct Notification {
    pub pid: i32,
    pub channel: String,
    pub payload: String,
}

/// What waits for one listening session.
#[derive(Debug, Default)]
pub struct Inbox {
    pub queue: Mutex<VecDeque<Notification>>,
    pub ready: Condvar,
}

#[derive(Debug, Default)]
pub struct Listeners {
    channels: Mutex<HashMap<String, HashSet<u64>>>,
    inboxes: Mutex<HashMap<u64, Arc<Inbox>>>,
    poller: Mutex<bool>,
    seq: AtomicU64,
}

impl Registry {
    pub(crate) fn inbox(&self, session: u64) -> Arc<Inbox> {
        Arc::clone(self.listeners.inboxes.lock().unwrap_or_else(std::sync::PoisonError::into_inner).entry(session).or_default())
    }

    pub(crate) fn unlisten_all(&self, session: u64) {
        for ids in self.listeners.channels.lock().unwrap_or_else(std::sync::PoisonError::into_inner).values_mut() {
            ids.remove(&session);
        }
        self.listeners.inboxes.lock().unwrap_or_else(std::sync::PoisonError::into_inner).remove(&session);
    }

    fn deliver_local(&self, database: &str, channel: &str, payload: &str, pid: i32) {
        let key = channel_key(database, channel);
        let ids: Vec<u64> = self.listeners.channels.lock().unwrap_or_else(std::sync::PoisonError::into_inner).get(&key).map(|s| s.iter().copied().collect()).unwrap_or_default();
        for id in ids {
            let inbox = self.inbox(id);
            let mut q = inbox.queue.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            if q.len() >= MAX_QUEUED {
                q.pop_front();
            }
            q.push_back(Notification { pid, channel: channel.to_owned(), payload: payload.to_owned() });
            inbox.ready.notify_all();
        }
    }

    /// Sends right away: `pg_notify` from inside a query (no session to wait for a commit with).
    pub(crate) fn deliver_now(&self, engine: &Engine, pid: u64, channel: &str, payload: &str) -> SqlResult<()> {
        self.deliver_local(&engine.config.database, channel, payload, pid as i32);
        publish(engine, pid, channel, payload)
    }
}

fn channel_key(database: &str, channel: &str) -> String {
    format!("{database}\u{1}{channel}")
}

/// For other gateways.
fn publish(engine: &Engine, pid: u64, channel: &str, payload: &str) -> SqlResult<()> {
    let seq = engine.registry.listeners.seq.fetch_add(1, Ordering::Relaxed);
    let mut d = Doc::new();
    d.insert("gateway".into(), Value::Text(crate::instance_id().to_owned()));
    d.insert("database".into(), Value::Text(engine.config.database.clone()));
    d.insert("channel".into(), Value::Text(channel.to_owned()));
    d.insert("payload".into(), Value::Text(payload.to_owned()));
    d.insert("pid".into(), Value::Int(pid as i64));
    d.insert("at".into(), Value::Timestamp(now_ns()));
    let w = Write { collection: BUS.into(), key: format!("{}-{}-{seq}", now_ns(), crate::instance_id()), op: Op::Put(d), pre: Pre::None, tag: Tag::Internal };
    engine.root_store().commit(std::slice::from_ref(&w)).map_err(|e| e.to_sql(std::slice::from_ref(&w)))?;
    Ok(())
}

/// Reads what other gateways published, while the engine lives and anyone listens here.
fn poll_bus(engine: std::sync::Weak<Engine>) {
    let mut seen: HashSet<String> = HashSet::new();
    let started = now_ns();
    while let Some(e) = engine.upgrade() {
        let store = e.root_store();
        if let Ok(docs) = store.scan(BUS, None) {
            let now = now_ns();
            let mut live = HashSet::new();
            for d in docs {
                let text = |k: &str| d.fields.get(k).and_then(Value::as_str).unwrap_or_default().to_owned();
                let at = match d.fields.get("at") {
                    Some(Value::Timestamp(t)) => *t,
                    _ => 0,
                };
                if at.saturating_add(KEEP_NS) < now {
                    let w = Write { collection: BUS.into(), key: d.key.clone(), op: Op::Delete, pre: Pre::None, tag: Tag::Internal };
                    let _ = store.commit(std::slice::from_ref(&w));
                    continue;
                }
                live.insert(d.key.clone());
                if at < started || seen.contains(&d.key) || text("gateway") == crate::instance_id() {
                    continue;
                }
                seen.insert(d.key.clone());
                let pid = d.fields.get("pid").and_then(Value::as_i64).unwrap_or(0) as i32;
                e.registry.deliver_local(&text("database"), &text("channel"), &text("payload"), pid);
            }
            seen.retain(|k| live.contains(k));
        }
        drop(e);
        std::thread::sleep(POLL);
    }
}

impl Session {
    pub(crate) fn listen(&mut self, channel: &str) -> SqlResult<Outcome> {
        let r = &self.engine.registry;
        r.listeners.channels.lock().unwrap_or_else(std::sync::PoisonError::into_inner).entry(channel_key(&self.engine.config.database, channel)).or_default().insert(self.id);
        let _ = r.inbox(self.id);
        self.listening.insert(channel.to_owned());
        let mut running = r.listeners.poller.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if !*running {
            *running = true;
            let weak = Arc::downgrade(&self.engine);
            let _ = std::thread::Builder::new().name("sql-notify".into()).spawn(move || poll_bus(weak));
        }
        Ok(Outcome::command("LISTEN"))
    }

    pub(crate) fn unlisten(&mut self, channel: Option<&str>) -> SqlResult<Outcome> {
        let r = &self.engine.registry;
        let mut channels = r.listeners.channels.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let names: Vec<String> = match channel {
            Some(c) => vec![c.to_owned()],
            None => self.listening.iter().cloned().collect(),
        };
        for c in names {
            if let Some(ids) = channels.get_mut(&channel_key(&self.engine.config.database, &c)) {
                ids.remove(&self.id);
            }
            self.listening.remove(&c);
        }
        Ok(Outcome::command("UNLISTEN"))
    }

    /// `NOTIFY`: identical notifications of one transaction collapse into one, as in PostgreSQL.
    pub(crate) fn notify(&mut self, channel: &str, payload: &str) -> SqlResult<Outcome> {
        let n = (channel.to_owned(), payload.to_owned());
        if !self.pending_notifies.contains(&n) {
            self.pending_notifies.push(n);
        }
        Ok(Outcome::command("NOTIFY"))
    }

    /// The transaction ended: send what it notified, or (`committed` false) forget it.
    pub(crate) fn flush_notifies(&mut self, committed: bool) {
        let pending = std::mem::take(&mut self.pending_notifies);
        if !committed {
            return;
        }
        for (channel, payload) in pending {
            let _ = self.engine.registry.deliver_now(&self.engine, self.id, &channel, &payload);
        }
    }

    /// What waits for this session, right now (the wire protocol sends it).
    pub fn take_notifications(&self) -> Vec<Notification> {
        if self.listening.is_empty() {
            return Vec::new();
        }
        let inbox = self.engine.registry.inbox(self.id);
        let mut q = inbox.queue.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        q.drain(..).collect()
    }
}
