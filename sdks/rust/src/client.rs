//! Connection pool, retries, redirects and the collection API.

use std::collections::hash_map::RandomState;
use std::collections::HashSet;
use std::hash::{BuildHasher, Hasher};
use std::io::{self, Read, Write as _};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use crate::error::{invalid, Error, Result};
use crate::value::{Doc, Reader};
use crate::wire::{
    self, ByteRange, ClusterInfo, CollectionInfo, Document, FieldOp, Filter, Nearest, Query, VectorIndex, Write,
    MAX_BYTE_RANGE,
};

const MAX_BODY: u32 = 256 * 1024 * 1024;

/// Options of [`connect_with`]. `Options::default()`: no token, 5 s timeout, leader reads, 16 pooled connections.
#[derive(Debug, Clone)]
pub struct Options {
    /// Bearer token (also accepted in the URL: `storine://token@host:port`).
    pub token: Option<String>,
    /// Timeout of one request. Retries continue for up to 3x this.
    pub timeout: Duration,
    /// Allow possibly stale reads from cluster followers.
    pub follower_reads: bool,
    /// Idle connections kept to the current server.
    pub pool_size: usize,
}

impl Default for Options {
    fn default() -> Self {
        Options { token: None, timeout: Duration::from_secs(5), follower_reads: false, pool_size: 16 }
    }
}

/// [`connect_with`] with default options.
pub fn connect(url: &str) -> Result<Client> {
    connect_with(url, Options::default())
}

/// A client for `storine://[token@]host:port[,host:port...]` (or just `host:port`). Query parameters
/// `timeout_ms=5000` and `follower_reads=1` are accepted too. Connections open lazily.
pub fn connect_with(url: &str, mut options: Options) -> Result<Client> {
    let rest = match url.split_once("://") {
        Some(("storine", rest)) => rest,
        Some(_) => return Err(invalid("connection strings start with storine://")),
        None => url,
    };
    let (rest, query) = rest.split_once('?').unwrap_or((rest, ""));
    let hosts = match rest.rsplit_once('@') {
        Some((token, hosts)) => {
            if options.token.is_none() {
                options.token = Some(percent_decode(token)?);
            }
            hosts
        }
        None => rest,
    };
    for kv in query.split('&').filter(|s| !s.is_empty()) {
        let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
        match k {
            "timeout_ms" => {
                let ms: u64 = v
                    .parse()
                    .ok()
                    .filter(|ms| *ms > 0)
                    .ok_or_else(|| invalid("timeout_ms must be a positive integer"))?;
                options.timeout = Duration::from_millis(ms);
            }
            "follower_reads" => options.follower_reads = matches!(v, "1" | "true" | "yes"),
            _ => {}
        }
    }
    if options.timeout.is_zero() {
        return Err(invalid("timeout must be positive"));
    }
    let seeds: Vec<String> =
        hosts.trim_end_matches('/').split(',').map(str::trim).filter(|h| !h.is_empty()).map(normalize).collect();
    if seeds.is_empty() {
        return Err(invalid("no server address given"));
    }
    let target = seeds[0].clone();
    Ok(Client { seeds, options: options.clone(), state: Mutex::new(State { target, pool: Vec::new() }) })
}

fn percent_decode(s: &str) -> Result<String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            let hex = std::str::from_utf8(&b[i + 1..i + 3]).ok().and_then(|h| u8::from_str_radix(h, 16).ok());
            out.push(hex.ok_or_else(|| invalid("bad token in connection string"))?);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|_| invalid("bad token in connection string"))
}

fn normalize(address: &str) -> String {
    if address.starts_with('[') {
        return if address.contains("]:") { address.to_owned() } else { format!("{address}:7878") };
    }
    match address.matches(':').count() {
        0 => format!("{address}:7878"),
        1 => address.to_owned(),
        _ => format!("[{address}]:7878"),
    }
}

/// 16 unpredictable bytes from the OS-seeded SipHash keys, plus a counter and the clock so ids never repeat.
fn request_id() -> [u8; 16] {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
    let mut id = [0u8; 16];
    for (i, half) in id.chunks_mut(8).enumerate() {
        let mut h = RandomState::new().build_hasher();
        h.write_u64(n);
        h.write_u128(now);
        h.write_usize(i);
        half.copy_from_slice(&h.finish().to_le_bytes());
    }
    id
}

fn random_index(len: usize) -> usize {
    let mut h = RandomState::new().build_hasher();
    h.write_u128(SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0));
    (h.finish() % len as u64) as usize
}

struct Conn {
    address: String,
    stream: TcpStream,
}

impl Conn {
    fn open(address: &str, timeout: Duration) -> io::Result<Conn> {
        let mut last = io::Error::new(io::ErrorKind::NotFound, format!("cannot resolve {address}"));
        for addr in address.to_socket_addrs()? {
            match TcpStream::connect_timeout(&addr, timeout) {
                Ok(stream) => {
                    stream.set_nodelay(true)?;
                    stream.set_read_timeout(Some(timeout))?;
                    stream.set_write_timeout(Some(timeout))?;
                    return Ok(Conn { address: address.to_owned(), stream });
                }
                Err(e) => last = e,
            }
        }
        Err(last)
    }

    fn call(&mut self, op: u8, body: &[u8]) -> io::Result<(u8, u64, Vec<u8>)> {
        let mut frame = Vec::with_capacity(29 + body.len());
        frame.push(op);
        frame.extend_from_slice(&[0u8; 24]);
        frame.extend_from_slice(&(body.len() as u32).to_le_bytes());
        frame.extend_from_slice(body);
        self.stream.write_all(&frame)?;
        let mut hdr = [0u8; 13];
        self.stream.read_exact(&mut hdr)?;
        let n = u32::from_le_bytes([hdr[9], hdr[10], hdr[11], hdr[12]]);
        if n > MAX_BODY {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "response too large"));
        }
        let mut rbody = vec![0u8; n as usize];
        self.stream.read_exact(&mut rbody)?;
        let mut version = [0u8; 8];
        version.copy_from_slice(&hdr[1..9]);
        Ok((hdr[0], u64::from_le_bytes(version), rbody))
    }
}

struct State {
    target: String,
    pool: Vec<Conn>,
}

enum Acquire {
    Dial(String, io::Error),
    Fatal(Error),
}

/// A connection pool to a Storine server or cluster. `Send + Sync`; share one per application (e.g. in an `Arc`).
///
/// Writes carry a request id that the client reuses on its own retries, so timeouts, reconnects and leader changes
/// never apply a write twice.
pub struct Client {
    seeds: Vec<String>,
    options: Options,
    state: Mutex<State>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("seeds", &self.seeds)
            .field("target", &self.lock().target)
            .finish_non_exhaustive()
    }
}

pub(crate) struct Reply {
    pub status: u8,
    pub version: u64,
    pub body: Vec<u8>,
}

fn text(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn or_default(s: String, default: &str) -> String {
    if s.is_empty() {
        default.to_owned()
    } else {
        s
    }
}

impl Client {
    /// A handle on the collection `name` (created on first write).
    pub fn collection(&self, name: &str) -> Collection<'_> {
        Collection { client: self, name: name.to_owned() }
    }

    /// Collects writes in `body`, then commits them atomically (all or none). Returns the new version of each write
    /// in order (`None` for deletes and checks). If `body` returns an error, nothing is written.
    ///
    /// ```no_run
    /// # use storine::{add, dec};
    /// # let db = storine::connect("127.0.0.1:7878").unwrap();
    /// db.transaction(|tx| {
    ///     tx.update("accounts", "alice", [add("balance", dec("-10")).min(0)]);
    ///     tx.update("accounts", "bob", [add("balance", dec("10"))]);
    ///     Ok(())
    /// })?;
    /// # Ok::<(), storine::Error>(())
    /// ```
    pub fn transaction<F>(&self, body: F) -> Result<Vec<Option<u64>>>
    where
        F: FnOnce(&mut Transaction) -> Result<()>,
    {
        let mut tx = Transaction { writes: Vec::new() };
        body(&mut tx)?;
        if tx.writes.is_empty() {
            return Ok(Vec::new());
        }
        self.commit(&tx.writes)
    }

    /// Names of every declared collection.
    pub fn collections(&self) -> Result<Vec<String>> {
        let reply = self.call(wire::OP_DOC_INFO, &[3])?;
        let mut r = Reader::new(&reply.body);
        let n = r.count(17)?;
        (0..n).map(|_| wire::decode_info(&mut r).map(|i| i.name)).collect()
    }

    /// Node id, leader, term and commit indexes of the server currently used.
    pub fn cluster_info(&self) -> Result<ClusterInfo> {
        wire::decode_cluster_info(&self.call(wire::OP_CLUSTER_INFO, &[])?.body)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn acquire(&self) -> std::result::Result<Conn, Acquire> {
        let target = {
            let mut st = self.lock();
            while let Some(c) = st.pool.pop() {
                if c.address == st.target {
                    return Ok(c);
                }
            }
            st.target.clone()
        };
        let mut c = Conn::open(&target, self.options.timeout).map_err(|e| Acquire::Dial(target.clone(), e))?;
        if let Some(token) = &self.options.token {
            let (status, _, body) =
                c.call(wire::OP_AUTH, token.as_bytes()).map_err(|e| Acquire::Dial(target.clone(), e))?;
            if status != wire::ST_OK {
                return Err(Acquire::Fatal(Error::Unauthorized(format!("authentication failed: {}", text(&body)))));
            }
        }
        if self.options.follower_reads {
            c.call(wire::OP_SET_OPTIONS, &[1]).map_err(|e| Acquire::Dial(target.clone(), e))?;
        }
        Ok(c)
    }

    fn release(&self, c: Conn) {
        let mut st = self.lock();
        if c.address == st.target && st.pool.len() < self.options.pool_size {
            st.pool.push(c);
        }
    }

    /// Switches to `address`, or (when `None`) to a random other seed not known to have failed.
    fn redirect(&self, address: Option<&str>, failed: &HashSet<String>) {
        let mut st = self.lock();
        if let Some(a) = address {
            st.target = normalize(a);
            return;
        }
        let mut others: Vec<&String> = self.seeds.iter().filter(|s| **s != st.target && !failed.contains(*s)).collect();
        if others.is_empty() {
            others = self.seeds.iter().filter(|s| **s != st.target).collect();
        }
        if others.is_empty() {
            others = self.seeds.iter().collect();
        }
        st.target = others[random_index(others.len())].clone();
    }

    /// Sends a request, following leader redirects and retrying transient failures for up to three times the
    /// timeout (enough to ride out a leader election). Every request is idempotent: reads by nature, writes by
    /// request id.
    pub(crate) fn request(&self, op: u8, body: &[u8]) -> Result<Reply> {
        let deadline = Instant::now() + (self.options.timeout * 3).max(Duration::from_secs(1));
        let mut delay = Duration::from_millis(20);
        let mut backoff = || {
            thread::sleep(delay);
            delay = (delay * 2).min(Duration::from_millis(500));
        };
        let mut last: Option<Error> = None;
        let mut failed = HashSet::new();
        let mut attempt = 0;
        while attempt < 1000 && (attempt < 8 || Instant::now() < deadline) {
            attempt += 1;
            let mut c = match self.acquire() {
                Ok(c) => c,
                Err(Acquire::Fatal(e)) => return Err(e),
                Err(Acquire::Dial(address, e)) => {
                    last = Some(Error::Unavailable(format!("could not reach {address}: {e}")));
                    failed.insert(address);
                    self.redirect(None, &failed);
                    backoff();
                    continue;
                }
            };
            let (status, version, rbody) = match c.call(op, body) {
                Ok(r) => r,
                Err(e) => {
                    last = Some(Error::Unavailable(format!("connection to {} lost: {e}", c.address)));
                    failed.insert(c.address);
                    self.redirect(None, &failed);
                    backoff();
                    continue;
                }
            };
            match status {
                wire::ST_NOT_LEADER => {
                    let hint = Some(text(&rbody)).filter(|h| !h.is_empty() && !failed.contains(&normalize(h)));
                    self.redirect(hint.as_deref(), &failed); // a hint naming a failed node is stale
                    last = Some(Error::Unavailable("no cluster leader is available yet".into()));
                    if hint.is_none() {
                        backoff();
                    }
                    continue;
                }
                wire::ST_COMMIT_UNKNOWN => {
                    last = Some(Error::Unavailable("write outcome unknown and retries exhausted".into()));
                    backoff();
                    continue;
                }
                wire::ST_OVERLOADED => {
                    last = Some(Error::Overloaded(or_default(text(&rbody), "server overloaded")));
                    backoff();
                    continue;
                }
                wire::ST_UNAUTHORIZED | wire::ST_RATE_LIMITED => {} // the server closed the connection
                _ => self.release(c),
            }
            return Ok(Reply { status, version, body: rbody });
        }
        Err(last.unwrap_or_else(|| Error::Unavailable("could not reach any server".into())))
    }

    pub(crate) fn error(status: u8, body: &[u8]) -> Error {
        let t = text(body);
        match status {
            wire::ST_CONFLICT => {
                if body.len() < 13 {
                    return Error::Conflict { message: t, write_index: None, current_version: None };
                }
                let index = u32::from_le_bytes([body[0], body[1], body[2], body[3]]);
                let mut v = [0u8; 8];
                v.copy_from_slice(&body[5..13]);
                Error::Conflict {
                    message: text(&body[13..]),
                    write_index: (index != u32::MAX).then_some(index),
                    current_version: (body[4] != 0).then_some(u64::from_le_bytes(v)),
                }
            }
            wire::ST_NOT_FOUND => Error::NotFound { message: or_default(t, "document not found"), write_index: None },
            wire::ST_INVALID => Error::Invalid(t),
            wire::ST_UNAUTHORIZED => Error::Unauthorized(or_default(t, "not authorized")),
            wire::ST_RATE_LIMITED => Error::RateLimited(or_default(t, "rate limited")),
            wire::ST_OVERLOADED => Error::Overloaded(or_default(t, "server overloaded")),
            wire::ST_COMMIT_UNKNOWN => Error::Unavailable("write outcome unknown; retry the same call".into()),
            _ => Error::Server(or_default(t, &format!("server error (status {status})"))),
        }
    }

    /// [`Client::request`] that fails on any status but OK.
    pub(crate) fn call(&self, op: u8, body: &[u8]) -> Result<Reply> {
        let r = self.request(op, body)?;
        if r.status != wire::ST_OK {
            return Err(Self::error(r.status, &r.body));
        }
        Ok(r)
    }

    /// Applies writes atomically, exactly once even across retries; returns the new versions.
    pub(crate) fn commit(&self, writes: &[Write]) -> Result<Vec<Option<u64>>> {
        let body = wire::encode_writes(writes, Some(&request_id()))?;
        let r = self.request(wire::OP_DOC_WRITE, &body)?;
        if r.status == wire::ST_NOT_FOUND {
            // Body: u32 index of the write that patched a missing document, then the message.
            let (write_index, message) = if r.body.len() >= 4 {
                let i = u32::from_le_bytes([r.body[0], r.body[1], r.body[2], r.body[3]]);
                (Some(i), or_default(text(&r.body[4..]), "document not found"))
            } else {
                (None, "document not found".to_owned())
            };
            return Err(Error::NotFound { message, write_index });
        }
        if r.status != wire::ST_OK {
            return Err(Self::error(r.status, &r.body));
        }
        wire::decode_write_response(&r.body)
    }
}

fn ttl_ms(ttl: Duration) -> Result<u64> {
    if ttl.is_zero() {
        return Err(invalid("ttl must be positive"));
    }
    Ok(u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX).max(1))
}

/// A named set of documents. Every method is one atomic, exactly-once operation.
#[derive(Debug)]
pub struct Collection<'a> {
    pub(crate) client: &'a Client,
    pub(crate) name: String,
}

impl Collection<'_> {
    pub fn name(&self) -> &str {
        &self.name
    }

    fn one(&self, w: Write) -> Result<u64> {
        Ok(self.client.commit(&[w])?[0].unwrap_or(0))
    }

    /// The document `key`, or `None` if it does not exist.
    pub fn get(&self, key: &str) -> Result<Option<Document>> {
        let r = self.client.request(wire::OP_DOC_GET, &wire::encode_get(&self.name, key)?)?;
        match r.status {
            wire::ST_NOT_FOUND => Ok(None),
            wire::ST_OK => {
                Ok(Some(Document { key: key.to_owned(), version: r.version, fields: Reader::new(&r.body).fields(1)? }))
            }
            s => Err(Client::error(s, &r.body)),
        }
    }

    /// The document `key` with only the fields at `paths` (such as `"name"` or `"meta.size"`; nested
    /// paths come back nested, absent ones are left out), or `None` if it does not exist. Reads
    /// metadata without transferring a large value stored beside it. At most 256 paths.
    pub fn get_fields(&self, key: &str, paths: &[&str]) -> Result<Option<Document>> {
        let r = self.client.request(wire::OP_DOC_GET, &wire::encode_get_fields(&self.name, key, paths)?)?;
        match r.status {
            wire::ST_NOT_FOUND => Ok(None),
            wire::ST_OK => {
                Ok(Some(Document { key: key.to_owned(), version: r.version, fields: Reader::new(&r.body).fields(1)? }))
            }
            s => Err(Client::error(s, &r.body)),
        }
    }

    /// Bytes `offset .. offset + length` of the bytes (or text) field `field`, without reading the
    /// rest of the document; `None` if the document does not exist. `length` is at most
    /// [`MAX_BYTE_RANGE`] (16 MiB); past the end of the value, `data` is empty. `Error::Invalid` if
    /// the field is missing or holds another type.
    pub fn read_bytes(&self, key: &str, field: &str, offset: u64, length: u32) -> Result<Option<ByteRange>> {
        let body = wire::encode_get_range(&self.name, key, field, offset, length)?;
        let r = self.client.request(wire::OP_DOC_GET, &body)?;
        match r.status {
            wire::ST_NOT_FOUND => Ok(None),
            wire::ST_OK => {
                let (total, data) = wire::decode_byte_range(&r.body)?;
                Ok(Some(ByteRange { version: r.version, total, data }))
            }
            s => Err(Client::error(s, &r.body)),
        }
    }

    /// A large bytes (or text) field read lazily, chunk by chunk (each at most `chunk_size` bytes,
    /// 1 ..= 16 MiB). Yields `Error::NotFound` if the document does not exist and `Error::Conflict`
    /// if it changes while being read (the chunks would otherwise mix two versions).
    pub fn stream_bytes(&self, key: &str, field: &str, chunk_size: u32) -> ByteStream<'_> {
        ByteStream {
            collection: self,
            key: key.to_owned(),
            field: field.to_owned(),
            chunk_size,
            offset: 0,
            version: None,
            done: false,
        }
    }

    /// Creates or replaces the document; returns its new version.
    pub fn put(&self, key: &str, fields: Doc) -> Result<u64> {
        self.one(Write::put(&self.name, key, fields, 0, 0, 0))
    }

    /// [`Collection::put`] for a document that expires after `ttl`.
    pub fn put_with_ttl(&self, key: &str, fields: Doc, ttl: Duration) -> Result<u64> {
        self.one(Write::put(&self.name, key, fields, ttl_ms(ttl)?, 0, 0))
    }

    /// Creates the document; `Error::Conflict` if it already exists.
    pub fn insert(&self, key: &str, fields: Doc) -> Result<u64> {
        self.one(Write::put(&self.name, key, fields, 0, 1, 0))
    }

    /// Replaces an existing document; `Error::Conflict` if it does not exist.
    pub fn replace(&self, key: &str, fields: Doc) -> Result<u64> {
        self.one(Write::put(&self.name, key, fields, 0, 2, 0))
    }

    /// Replaces the document only if it is still at `version`; `Error::Conflict` otherwise.
    pub fn replace_if_version(&self, key: &str, version: u64, fields: Doc) -> Result<u64> {
        self.one(Write::put(&self.name, key, fields, 0, 3, version))
    }

    /// Applies field changes atomically on the server; returns the new version:
    /// `accounts.update("alice", [add("balance", dec("-25.00")).min(0)])`.
    /// `Error::NotFound` if the document does not exist; `Error::Invalid` if a bound would be violated.
    pub fn update(&self, key: &str, changes: impl IntoIterator<Item = FieldOp>) -> Result<u64> {
        self.one(Write::patch(&self.name, key, 0, 0, changes.into_iter().collect()))
    }

    /// [`Collection::update`] applied only if the document is still at `version`; `Error::Conflict` otherwise.
    pub fn update_if_version(
        &self,
        key: &str,
        version: u64,
        changes: impl IntoIterator<Item = FieldOp>,
    ) -> Result<u64> {
        self.one(Write::patch(&self.name, key, 3, version, changes.into_iter().collect()))
    }

    /// Deletes the document; returns `false` if it did not exist.
    pub fn delete(&self, key: &str) -> Result<bool> {
        match self.one(Write::new(&self.name, key, 3, 2, 0)) {
            Ok(_) => Ok(true),
            Err(Error::Conflict { current_version: None, .. }) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Deletes the document only if it is still at `version`; `Error::Conflict` otherwise.
    pub fn delete_if_version(&self, key: &str, version: u64) -> Result<()> {
        self.one(Write::new(&self.name, key, 3, 3, version)).map(|_| ())
    }

    fn query(&self, q: &Query, after: Option<&str>) -> Result<wire::Page> {
        wire::decode_page(&self.client.call(wire::OP_DOC_QUERY, &wire::encode_query(&self.name, q, after, None)?)?.body)
    }

    /// Documents matching `query` (a [`Query`] or a [`Filter`]).
    pub fn find(&self, query: impl Into<Query>) -> Result<Vec<Document>> {
        Ok(self.query(&query.into(), None)?.docs)
    }

    /// The first document matching `query`, or `None`.
    pub fn first(&self, query: impl Into<Query>) -> Result<Option<Document>> {
        Ok(self.find(query.into().limit(1))?.into_iter().next())
    }

    /// Number of documents matching `filter` (`None`: all).
    pub fn count(&self, filter: impl Into<Option<Filter>>) -> Result<u64> {
        let q = Query { filter: filter.into(), limit: 1, ..Query::default() };
        Ok(u64::from(self.query(&q, None)?.total))
    }

    /// Every document matching `filter` (`None`: all) in key order, fetched lazily page by page.
    pub fn scan(&self, filter: impl Into<Option<Filter>>) -> Scan<'_> {
        Scan {
            collection: self,
            query: Query { filter: filter.into(), limit: 1000, ..Query::default() },
            page: Vec::new().into_iter(),
            after: None,
            done: false,
        }
    }

    /// The `k` documents whose `field` (a list of numbers) is most similar to `vector` (cosine), best
    /// first, among those matching `filter` (`None`: all). Each document's similarity is in
    /// [`Document::score`] (also the `$score` field). Uses the field's vector index when it has one
    /// (approximate candidates, exact scores).
    pub fn nearest(
        &self,
        field: &str,
        vector: &[f32],
        k: u32,
        filter: impl Into<Option<Filter>>,
    ) -> Result<Vec<Document>> {
        self.nearest_query(Nearest { field, vector, k, exact: false }, filter.into())
    }

    /// [`Collection::nearest`] scoring every candidate instead of using the vector index.
    pub fn nearest_exact(
        &self,
        field: &str,
        vector: &[f32],
        k: u32,
        filter: impl Into<Option<Filter>>,
    ) -> Result<Vec<Document>> {
        self.nearest_query(Nearest { field, vector, k, exact: true }, filter.into())
    }

    fn nearest_query(&self, n: Nearest<'_>, filter: Option<Filter>) -> Result<Vec<Document>> {
        let q = Query { filter, limit: n.k, ..Query::default() };
        let body = wire::encode_query(&self.name, &q, None, Some(n))?;
        Ok(wire::decode_page(&self.client.call(wire::OP_DOC_QUERY, &body)?.body)?.docs)
    }

    /// Indexes a top-level field holding `dims` numbers for `nearest` (at most 4 per collection);
    /// existing documents included. Writes of other values to that field are then refused.
    pub fn create_vector_index(&self, field: &str, dims: u32) -> Result<()> {
        let mut body = wire::encode_admin(7, &self.name, Some(field))?;
        body.extend_from_slice(&dims.to_le_bytes());
        self.client.call(wire::OP_DOC_ADMIN, &body).map(|_| ())
    }

    /// Removes the vector index on `field`.
    pub fn drop_vector_index(&self, field: &str) -> Result<()> {
        self.client.call(wire::OP_DOC_ADMIN, &wire::encode_admin(8, &self.name, Some(field))?).map(|_| ())
    }

    /// The collection's vector indexes.
    pub fn vector_indexes(&self) -> Result<Vec<VectorIndex>> {
        let r = self.client.call(wire::OP_DOC_INFO, &wire::encode_admin(9, &self.name, None)?)?;
        wire::decode_vector_indexes(&r.body)
    }

    /// Indexes a top-level field (at most 16 per collection); existing documents included.
    pub fn create_index(&self, field: &str) -> Result<()> {
        self.client.call(wire::OP_DOC_ADMIN, &wire::encode_admin(1, &self.name, Some(field))?).map(|_| ())
    }

    /// Removes the index on `field`.
    pub fn drop_index(&self, field: &str) -> Result<()> {
        self.client.call(wire::OP_DOC_ADMIN, &wire::encode_admin(2, &self.name, Some(field))?).map(|_| ())
    }

    /// Deletes every document and the collection's metadata; returns the number deleted.
    pub fn drop(&self) -> Result<u64> {
        Ok(self.client.call(wire::OP_DOC_ADMIN, &wire::encode_admin(4, &self.name, None)?)?.version)
    }

    /// Indexes and metadata of the collection.
    pub fn info(&self) -> Result<CollectionInfo> {
        let r = self.client.call(wire::OP_DOC_INFO, &wire::encode_admin(6, &self.name, None)?)?;
        wire::decode_info(&mut Reader::new(&r.body))
    }
}

/// The iterator returned by [`Collection::scan`].
#[derive(Debug)]
pub struct Scan<'a> {
    collection: &'a Collection<'a>,
    query: Query,
    page: std::vec::IntoIter<Document>,
    after: Option<String>,
    done: bool,
}

impl Iterator for Scan<'_> {
    type Item = Result<Document>;

    fn next(&mut self) -> Option<Result<Document>> {
        loop {
            if let Some(d) = self.page.next() {
                return Some(Ok(d));
            }
            if self.done {
                return None;
            }
            match self.collection.query(&self.query, self.after.as_deref()) {
                Ok(p) => {
                    self.done = p.next_key.is_none();
                    self.after = p.next_key;
                    self.page = p.docs.into_iter();
                }
                Err(e) => {
                    self.done = true;
                    return Some(Err(e));
                }
            }
        }
    }
}

/// The iterator returned by [`Collection::stream_bytes`]: one `Vec<u8>` per chunk, in order.
#[derive(Debug)]
pub struct ByteStream<'a> {
    collection: &'a Collection<'a>,
    key: String,
    field: String,
    chunk_size: u32,
    offset: u64,
    version: Option<u64>,
    done: bool,
}

impl ByteStream<'_> {
    fn next_chunk(&mut self) -> Result<Option<Vec<u8>>> {
        if self.chunk_size == 0 || self.chunk_size > MAX_BYTE_RANGE {
            return Err(invalid(format!("chunk_size must be between 1 and {MAX_BYTE_RANGE}")));
        }
        let Some(r) = self.collection.read_bytes(&self.key, &self.field, self.offset, self.chunk_size)? else {
            return Err(Error::NotFound {
                message: format!("document {:?} does not exist", self.key),
                write_index: None,
            });
        };
        if self.version.is_some_and(|v| v != r.version) {
            return Err(Error::Conflict {
                message: "the document changed while it was being read".into(),
                write_index: None,
                current_version: Some(r.version),
            });
        }
        self.version = Some(r.version);
        if r.data.is_empty() {
            return Ok(None);
        }
        self.offset += r.data.len() as u64;
        self.done = self.offset >= r.total;
        Ok(Some(r.data))
    }
}

impl Iterator for ByteStream<'_> {
    type Item = Result<Vec<u8>>;

    fn next(&mut self) -> Option<Result<Vec<u8>>> {
        if self.done {
            return None;
        }
        match self.next_chunk() {
            Ok(Some(chunk)) => Some(Ok(chunk)),
            Ok(None) => {
                self.done = true;
                None
            }
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }
}

/// Writes collected by [`Client::transaction`]. Methods only buffer; nothing is sent until the body returns.
#[derive(Debug)]
pub struct Transaction {
    writes: Vec<Write>,
}

impl Transaction {
    /// Creates or replaces a document.
    pub fn put(&mut self, collection: &str, key: &str, fields: Doc) -> &mut Self {
        self.writes.push(Write::put(collection, key, fields, 0, 0, 0));
        self
    }

    /// Creates or replaces a document that expires after `ttl` (a zero ttl fails the commit).
    pub fn put_with_ttl(&mut self, collection: &str, key: &str, fields: Doc, ttl: Duration) -> &mut Self {
        let ms = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX).max(1);
        self.writes.push(Write::put(collection, key, fields, ms, 0, 0));
        self
    }

    /// Creates a document; the transaction fails with `Error::Conflict` if it exists.
    pub fn insert(&mut self, collection: &str, key: &str, fields: Doc) -> &mut Self {
        self.writes.push(Write::put(collection, key, fields, 0, 1, 0));
        self
    }

    /// Replaces an existing document.
    pub fn replace(&mut self, collection: &str, key: &str, fields: Doc) -> &mut Self {
        self.writes.push(Write::put(collection, key, fields, 0, 2, 0));
        self
    }

    /// Replaces a document still at `version`.
    pub fn replace_if_version(&mut self, collection: &str, key: &str, version: u64, fields: Doc) -> &mut Self {
        self.writes.push(Write::put(collection, key, fields, 0, 3, version));
        self
    }

    /// Applies field changes to an existing document.
    pub fn update(&mut self, collection: &str, key: &str, changes: impl IntoIterator<Item = FieldOp>) -> &mut Self {
        self.writes.push(Write::patch(collection, key, 0, 0, changes.into_iter().collect()));
        self
    }

    /// Applies field changes to a document still at `version`.
    pub fn update_if_version(
        &mut self,
        collection: &str,
        key: &str,
        version: u64,
        changes: impl IntoIterator<Item = FieldOp>,
    ) -> &mut Self {
        self.writes.push(Write::patch(collection, key, 3, version, changes.into_iter().collect()));
        self
    }

    /// Deletes a document if it exists.
    pub fn delete(&mut self, collection: &str, key: &str) -> &mut Self {
        self.writes.push(Write::new(collection, key, 3, 0, 0));
        self
    }

    /// Deletes a document still at `version`.
    pub fn delete_if_version(&mut self, collection: &str, key: &str, version: u64) -> &mut Self {
        self.writes.push(Write::new(collection, key, 3, 3, version));
        self
    }

    /// Commits only if the document is at `version` (it is not written).
    pub fn require_version(&mut self, collection: &str, key: &str, version: u64) -> &mut Self {
        self.writes.push(Write::new(collection, key, 5, 3, version));
        self
    }

    /// Commits only if the document exists (`true`) or does not (`false`).
    pub fn require_exists(&mut self, collection: &str, key: &str, exists: bool) -> &mut Self {
        self.writes.push(Write::new(collection, key, 5, if exists { 2 } else { 1 }, 0));
        self
    }
}
