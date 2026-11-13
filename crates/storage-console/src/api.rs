//! The console's JSON API over the Storine client, and its access checks.
//!
//! | Method and path | Does |
//! | :--- | :--- |
//! | `GET /api/session` | whether the console is connected, and to what |
//! | `POST /api/connect` `{url, token?, tls_ca?, tls_server_name?}` | connects (and checks the server answers) |
//! | `POST /api/disconnect` | forgets the connection |
//! | `GET /api/overview` | cluster view, health report, collections with counts |
//! | `GET /api/collections/{c}` | indexes, vector indexes, metadata, document count |
//! | `DELETE /api/collections/{c}` | drops the collection |
//! | `POST /api/collections/{c}/query` `{filter?, sort?, limit?, offset?, after?}` | one page of documents |
//! | `POST /api/collections/{c}/aggregate` `{filter?, group_by?, aggs, order_by?, limit?}` | grouped aggregates |
//! | `GET /api/collections/{c}/docs/{key}` | one document |
//! | `PUT /api/collections/{c}/docs/{key}?mode=create\|replace\|put&version=N` | writes the document in the body |
//! | `DELETE /api/collections/{c}/docs/{key}?version=N` | deletes it |
//! | `POST /api/collections/{c}/indexes` `{field, dims?}` | creates an index (a vector index with `dims`) |
//! | `DELETE /api/collections/{c}/indexes/{field}?vector=1` | drops it |
//!
//! Every API request must carry the console's key in `X-Console-Key`, and every request's
//! `Host` must name the console itself. The key keeps out other local users and other web
//! pages; the header also forces a browser to ask before sending it cross-origin (a CORS
//! preflight the console never grants), and the `Host` check defeats DNS rebinding.

use std::sync::{Arc, RwLock};

use storine::{AggregateQuery, Client, Error, Filter, Options, Query, Value};

use crate::http::{Request, Response};
use crate::json::{self, Json};

/// What a browser may ask of this console.
#[derive(Debug)]
pub struct Console {
    key: String,
    /// `Host` header values this console answers to.
    hosts: Vec<String>,
    conn: RwLock<Option<Connection>>,
}

#[derive(Debug, Clone)]
struct Connection {
    client: Arc<Client>,
    /// The URL without its token.
    url: String,
}

/// An error as the browser sees it: an HTTP status and a message.
#[derive(Debug)]
pub struct ApiError {
    status: u16,
    message: String,
}

impl ApiError {
    fn new(status: u16, message: impl Into<String>) -> ApiError {
        ApiError { status, message: message.into() }
    }
}

impl From<Error> for ApiError {
    fn from(e: Error) -> ApiError {
        let status = match &e {
            Error::NotFound { .. } => 404,
            Error::Conflict { .. } => 409,
            Error::Invalid(_) => 422,
            Error::Unauthorized(_) => 403,
            Error::Overloaded(_) | Error::RateLimited(_) => 429,
            Error::Unavailable(_) => 503,
            Error::Server(_) => 502,
        };
        ApiError::new(status, e.message().to_owned())
    }
}

type ApiResult = Result<Json, ApiError>;

fn bad(message: impl Into<String>) -> ApiError {
    ApiError::new(400, message)
}

/// `storine://token@host` without the token, for display.
pub fn redact(url: &str) -> String {
    match url.split_once("://") {
        Some((scheme, rest)) => match rest.split_once('@') {
            Some((_, hosts)) => format!("{scheme}://{hosts}"),
            None => url.to_owned(),
        },
        None => url.to_owned(),
    }
}

/// Constant-time comparison, so the key cannot be guessed byte by byte from response times.
fn same(a: &str, b: &str) -> bool {
    a.len() == b.len() && a.bytes().zip(b.bytes()).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

impl Console {
    pub fn new(key: String, hosts: Vec<String>) -> Console {
        Console { key, hosts, conn: RwLock::new(None) }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    /// Connects to `url` and checks that the server answers.
    pub fn connect(&self, url: &str, options: Options) -> Result<String, ApiError> {
        let client = storine::connect_with(url, options)?;
        client.cluster_info()?;
        let shown = redact(url);
        *self.conn.write().unwrap_or_else(|p| p.into_inner()) = Some(Connection { client: Arc::new(client), url: shown.clone() });
        Ok(shown)
    }

    fn client(&self) -> Result<Arc<Client>, ApiError> {
        self.conn
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .map(|c| Arc::clone(&c.client))
            .ok_or_else(|| ApiError::new(409, "not connected: connect to a server first"))
    }

    /// Whether `req` may be served at all (`Host` names this console).
    pub fn host_allowed(&self, req: &Request) -> bool {
        req.header("Host").is_some_and(|h| self.hosts.iter().any(|a| a.eq_ignore_ascii_case(h)))
    }

    /// Answers an `/api/...` request.
    pub fn handle(&self, req: &Request) -> Response {
        if !req.header("X-Console-Key").is_some_and(|k| same(k, &self.key)) {
            return error_response(&ApiError::new(401, "missing or wrong console key: open the link the console printed"));
        }
        match self.route(req) {
            Ok(j) => Response::json(200, json::write(&j, false)),
            Err(e) => error_response(&e),
        }
    }

    fn route(&self, req: &Request) -> ApiResult {
        let segs = req.segments().map_err(bad)?;
        let segs: Vec<&str> = segs.iter().map(String::as_str).collect();
        let body = || -> Result<Json, ApiError> {
            let text = std::str::from_utf8(&req.body).map_err(|_| bad("the body is not UTF-8"))?;
            if text.trim().is_empty() {
                return Ok(Json::Obj(Vec::new()));
            }
            json::parse(text).map_err(bad)
        };
        match (req.method.as_str(), &segs[1..]) {
            ("GET", ["session"]) => Ok(self.session()),
            ("POST", ["connect"]) => self.connect_from(&body()?),
            ("POST", ["disconnect"]) => {
                *self.conn.write().unwrap_or_else(|p| p.into_inner()) = None;
                Ok(self.session())
            }
            ("GET", ["overview"]) => self.overview(),
            ("GET", ["collections", c]) => self.collection(c),
            ("DELETE", ["collections", c]) => {
                let n = self.client()?.collection(c).drop()?;
                Ok(Json::obj([("deleted", Json::num(n))]))
            }
            ("POST", ["collections", c, "query"]) => self.query(c, &body()?),
            ("POST", ["collections", c, "aggregate"]) => self.aggregate(c, &body()?),
            ("GET", ["collections", c, "docs", key]) => {
                let doc = self.client()?.collection(c).get(key)?.ok_or_else(|| ApiError::new(404, format!("no document \"{key}\" in {c}")))?;
                Ok(doc_json_with(&doc, true))
            }
            ("PUT", ["collections", c, "docs", key]) => self.put(c, key, req),
            ("DELETE", ["collections", c, "docs", key]) => {
                let coll = self.client()?;
                let coll = coll.collection(c);
                match req.param("version") {
                    Some(v) => coll.delete_if_version(key, v.parse().map_err(|_| bad("version is a number"))?)?,
                    None => {
                        if !coll.delete(key)? {
                            return Err(ApiError::new(404, format!("no document \"{key}\" in {c}")));
                        }
                    }
                }
                Ok(Json::obj([("deleted", Json::Bool(true))]))
            }
            ("POST", ["collections", c, "indexes"]) => {
                let b = body()?;
                let field = b.get("field").and_then(Json::as_str).filter(|f| !f.is_empty()).ok_or_else(|| bad("field is required"))?;
                let client = self.client()?;
                match b.get("dims").and_then(Json::as_u64) {
                    Some(d) => client.collection(c).create_vector_index(field, u32::try_from(d).map_err(|_| bad("dims is too large"))?)?,
                    None => client.collection(c).create_index(field)?,
                }
                self.collection(c)
            }
            ("DELETE", ["collections", c, "indexes", field]) => {
                let client = self.client()?;
                if req.param("vector") == Some("1") {
                    client.collection(c).drop_vector_index(field)?;
                } else {
                    client.collection(c).drop_index(field)?;
                }
                self.collection(c)
            }
            (_, ["session" | "connect" | "disconnect" | "overview"]) | (_, ["collections", ..]) => Err(ApiError::new(405, "method not allowed")),
            _ => Err(ApiError::new(404, "no such API")),
        }
    }

    fn session(&self) -> Json {
        match &*self.conn.read().unwrap_or_else(|p| p.into_inner()) {
            Some(c) => Json::obj([("connected", Json::Bool(true)), ("url", Json::str(c.url.clone()))]),
            None => Json::obj([("connected", Json::Bool(false))]),
        }
    }

    fn connect_from(&self, b: &Json) -> ApiResult {
        let url = b.get("url").and_then(Json::as_str).map(str::trim).filter(|u| !u.is_empty()).ok_or_else(|| bad("url is required"))?;
        let text = |k: &str| b.get(k).and_then(Json::as_str).map(str::trim).filter(|s| !s.is_empty()).map(str::to_owned);
        let mut options = Options { token: text("token"), timeout: std::time::Duration::from_secs(5), ..Options::default() };
        if let Some(ca) = text("tls_ca") {
            options.tls = true;
            options.tls_ca = Some(ca);
        }
        options.tls_server_name = text("tls_server_name");
        let url = if url.contains("://") { url.to_owned() } else { format!("storine://{url}") };
        self.connect(&url, options)?;
        Ok(self.session())
    }

    fn overview(&self) -> ApiResult {
        let client = self.client()?;
        let info = client.cluster_info()?;
        let health = client.health().ok().and_then(|h| json::parse(&h).ok()).unwrap_or(Json::Null);
        let mut collections = Vec::new();
        for name in client.collections()? {
            let coll = client.collection(&name);
            let i = coll.info()?;
            collections.push(Json::obj([
                ("name", Json::str(name.clone())),
                ("count", coll.count(None).map_or(Json::Null, Json::num)),
                ("indexes", Json::Arr(i.indexes.into_iter().map(Json::Str).collect())),
            ]));
        }
        let members = info
            .members
            .iter()
            .map(|(id, addr)| Json::obj([("id", Json::num(id)), ("addr", Json::str(addr.clone())), ("leader", Json::Bool(info.leader_id == Some(*id)))]))
            .collect();
        let url = self.conn.read().unwrap_or_else(|p| p.into_inner()).as_ref().map(|c| c.url.clone()).unwrap_or_default();
        Ok(Json::obj([
            ("url", Json::str(url)),
            (
                "cluster",
                Json::obj([
                    ("node_id", Json::num(info.node_id)),
                    ("is_leader", Json::Bool(info.is_leader)),
                    ("leader_id", info.leader_id.map_or(Json::Null, Json::num)),
                    ("leader_addr", info.leader_addr.map_or(Json::Null, Json::Str)),
                    ("term", Json::num(info.term)),
                    ("commit_index", Json::num(info.commit_index)),
                    ("last_applied", Json::num(info.last_applied)),
                    ("members", Json::Arr(members)),
                ]),
            ),
            ("health", health),
            ("collections", Json::Arr(collections)),
        ]))
    }

    fn collection(&self, name: &str) -> ApiResult {
        let client = self.client()?;
        let coll = client.collection(name);
        let i = coll.info()?;
        let vectors = coll
            .vector_indexes()?
            .into_iter()
            .map(|v| Json::obj([("field", Json::Str(v.field)), ("dims", Json::num(v.dims)), ("ready", Json::Bool(v.ready))]))
            .collect();
        Ok(Json::obj([
            ("name", Json::Str(i.name)),
            ("count", Json::num(coll.count(None)?)),
            ("indexes", Json::Arr(i.indexes.into_iter().map(Json::Str).collect())),
            ("building", Json::Arr(i.building.into_iter().map(Json::Str).collect())),
            ("vector_indexes", Json::Arr(vectors)),
            ("metadata", json::from_doc(&i.metadata)),
        ]))
    }

    fn query(&self, c: &str, b: &Json) -> ApiResult {
        let mut q = Query::new();
        if let Some(f) = b.get("filter").filter(|f| **f != Json::Null) {
            q = q.filter(filter(f, 0)?);
        }
        let mut sorted = false;
        for s in b.get("sort").and_then(Json::as_array).unwrap_or_default() {
            let s = s.as_str().filter(|s| !s.is_empty() && *s != "-").ok_or_else(|| bad("sort is a list of field names, \"-field\" for descending"))?;
            q = q.order_by(s);
            sorted = true;
        }
        let limit = b.get("limit").and_then(Json::as_u64).unwrap_or(50).clamp(1, 1000);
        q = q.limit(limit as u32);
        if let Some(o) = b.get("offset").and_then(Json::as_u64).filter(|o| *o > 0) {
            q = q.offset(u32::try_from(o).map_err(|_| bad("offset is too large"))?);
        }
        let after = b.get("after").and_then(Json::as_str);
        if sorted && after.is_some() {
            return Err(bad("a sorted query pages by offset, not by key"));
        }
        let page = self.client()?.collection(c).page(q, after)?;
        Ok(Json::obj([
            ("total", Json::num(page.total)),
            ("next", page.next_key.map_or(Json::Null, Json::Str)),
            ("docs", Json::Arr(page.docs.iter().map(doc_json).collect())),
        ]))
    }

    fn aggregate(&self, c: &str, b: &Json) -> ApiResult {
        let mut q = AggregateQuery::new();
        if let Some(f) = b.get("filter").filter(|f| **f != Json::Null) {
            q = q.filter(filter(f, 0)?);
        }
        for g in b.get("group_by").and_then(Json::as_array).unwrap_or_default() {
            q = q.group_by(g.as_str().filter(|s| !s.is_empty()).ok_or_else(|| bad("group_by is a list of field names"))?);
        }
        let aggs = b.get("aggs").and_then(Json::as_array).unwrap_or_default();
        if aggs.is_empty() {
            return Err(bad("add at least one aggregate"));
        }
        for a in aggs {
            let func = a.get("fn").and_then(Json::as_str).unwrap_or("");
            let field = a.get("field").and_then(Json::as_str).filter(|f| !f.is_empty());
            let need = || field.ok_or_else(|| bad(format!("{func} needs a field")));
            let agg = match func {
                "count" => field.map_or_else(storine::count, storine::count_of),
                "sum" => storine::sum(need()?),
                "avg" => storine::avg(need()?),
                "min" => storine::min(need()?),
                "max" => storine::max(need()?),
                "count_distinct" => storine::count_distinct(need()?),
                "approx_count_distinct" => storine::approx_count_distinct(need()?),
                _ => return Err(bad(format!("unknown aggregate \"{func}\""))),
            };
            let name = a
                .get("as")
                .and_then(Json::as_str)
                .filter(|s| !s.is_empty())
                .map_or_else(|| field.map_or_else(|| func.to_owned(), |f| format!("{func}({f})")), str::to_owned);
            q = q.agg(&name, agg);
        }
        for o in b.get("order_by").and_then(Json::as_array).unwrap_or_default() {
            q = q.order_by(o.as_str().ok_or_else(|| bad("order_by is a list of column names"))?);
        }
        if let Some(l) = b.get("limit").and_then(Json::as_u64) {
            q = q.limit(u32::try_from(l).map_err(|_| bad("limit is too large"))?);
        }
        let r = self.client()?.collection(c).aggregate(q)?;
        Ok(Json::obj([
            ("columns", Json::Arr(r.columns.into_iter().map(Json::Str).collect())),
            ("rows", Json::Arr(r.rows.iter().map(|row| Json::Arr(row.iter().map(json::from_value).collect())).collect())),
            ("matched", Json::num(r.matched)),
            ("pruned_row_groups", Json::num(r.pruned_row_groups)),
            ("spilled_bytes", Json::num(r.spilled_bytes)),
        ]))
    }

    fn put(&self, c: &str, key: &str, req: &Request) -> ApiResult {
        let text = std::str::from_utf8(&req.body).map_err(|_| bad("the body is not UTF-8"))?;
        let fields = json::to_doc(&json::parse(text).map_err(bad)?).map_err(|e| ApiError::new(422, e))?;
        let client = self.client()?;
        let coll = client.collection(c);
        let version = match (req.param("mode").unwrap_or("put"), req.param("version")) {
            ("create", _) => coll.insert(key, fields)?,
            ("replace", Some(v)) => coll.replace_if_version(key, v.parse().map_err(|_| bad("version is a number"))?, fields)?,
            ("replace", None) => coll.replace(key, fields)?,
            ("put", _) => coll.put(key, fields)?,
            (m, _) => return Err(bad(format!("unknown mode \"{m}\": create, replace or put"))),
        };
        let doc = coll.get(key)?;
        Ok(doc.map_or_else(|| Json::obj([("key", Json::str(key)), ("version", Json::num(version)), ("text", Json::str("{}"))]), |d| doc_json_with(&d, true)))
    }
}

fn error_response(e: &ApiError) -> Response {
    Response::json(e.status, json::write(&Json::obj([("error", Json::str(e.message.clone()))]), false))
}

/// A document for the browser: its fields as JSON, and (`with_text`) the same as indented text
/// for the editor, which the browser sends back untouched: `JSON.parse` would turn `1.0` into
/// `1`, and a float into an integer.
fn doc_json(d: &storine::Document) -> Json {
    doc_json_with(d, false)
}

fn doc_json_with(d: &storine::Document, with_text: bool) -> Json {
    let fields = json::from_doc(&d.fields);
    let text = with_text.then(|| json::write(&fields, true));
    let mut members = vec![("key".to_owned(), Json::str(d.key.clone())), ("version".to_owned(), Json::num(d.version)), ("doc".to_owned(), fields)];
    if let Some(t) = text {
        members.push(("text".to_owned(), Json::Str(t)));
    }
    Json::Obj(members)
}

/// A filter from its JSON form: `{"op": "eq", "path": "a.b", "value": 1}`; `between` takes
/// `low` and `high`, `in` takes `values`, `prefix` and `key_prefix` take `text`, `exists`
/// and `missing` take only `path`, `and` / `or` take `filters` and `not` takes `filter`.
pub fn filter(j: &Json, depth: usize) -> Result<Filter, ApiError> {
    if depth > 32 {
        return Err(bad("filter nested too deeply"));
    }
    let op = j.get("op").and_then(Json::as_str).ok_or_else(|| bad("each filter needs an \"op\""))?;
    let path = || j.get("path").and_then(Json::as_str).filter(|p| !p.is_empty()).ok_or_else(|| bad(format!("{op} needs a field path")));
    let value = |k: &str| -> Result<Value, ApiError> {
        json::to_value(j.get(k).ok_or_else(|| bad(format!("{op} needs \"{k}\"")))?).map_err(|e| ApiError::new(422, e))
    };
    let text = || j.get("text").and_then(Json::as_str).ok_or_else(|| bad(format!("{op} needs \"text\"")));
    let subs = || -> Result<Vec<Filter>, ApiError> {
        let list = j.get("filters").and_then(Json::as_array).filter(|l| !l.is_empty()).ok_or_else(|| bad(format!("{op} needs \"filters\"")))?;
        list.iter().map(|f| filter(f, depth + 1)).collect()
    };
    Ok(match op {
        "eq" => storine::eq(path()?, value("value")?),
        "ne" => storine::ne(path()?, value("value")?),
        "lt" => storine::lt(path()?, value("value")?),
        "le" => storine::le(path()?, value("value")?),
        "gt" => storine::gt(path()?, value("value")?),
        "ge" => storine::ge(path()?, value("value")?),
        "between" => storine::between(path()?, value("low")?, value("high")?),
        "in" => {
            let vals = j.get("values").and_then(Json::as_array).ok_or_else(|| bad("in needs \"values\""))?;
            storine::is_in(path()?, vals.iter().map(json::to_value).collect::<Result<Vec<_>, _>>().map_err(|e| ApiError::new(422, e))?)
        }
        "prefix" => storine::prefix(path()?, text()?),
        "contains" => storine::contains(path()?, value("value")?),
        "exists" => storine::exists(path()?),
        "missing" => storine::missing(path()?),
        "key_prefix" => storine::key_prefix(text()?),
        "and" => storine::and(subs()?),
        "or" => storine::or(subs()?),
        "not" => !filter(j.get("filter").ok_or_else(|| bad("not needs \"filter\""))?, depth + 1)?,
        _ => return Err(bad(format!("unknown filter op \"{op}\""))),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_are_not_shown() {
        assert_eq!(redact("storine://secret@h1:7878,h2:7878"), "storine://h1:7878,h2:7878");
        assert_eq!(redact("storines://h:1?tls_ca=/a"), "storines://h:1?tls_ca=/a");
    }

    #[test]
    fn filters_build_from_json_and_bad_ones_say_why() {
        let ok = r#"{"op":"and","filters":[{"op":"gt","path":"n","value":1},{"op":"not","filter":{"op":"key_prefix","text":"a"}},
            {"op":"in","path":"s","values":["x",{"$decimal":"1.5"}]},{"op":"between","path":"t","low":{"$time":"2026-01-01"},"high":{"$time":"2027-01-01"}}]}"#;
        assert!(filter(&json::parse(ok).unwrap(), 0).is_ok());
        for (bad_json, says) in [
            (r#"{"op":"eq","value":1}"#, "field path"),
            (r#"{"op":"eq","path":"a"}"#, "\"value\""),
            (r#"{"op":"zz","path":"a"}"#, "unknown"),
            (r#"{"op":"and","filters":[]}"#, "filters"),
            (r#"{"op":"eq","path":"a","value":{"$time":"soon"}}"#, "$time"),
        ] {
            let e = filter(&json::parse(bad_json).unwrap(), 0).unwrap_err();
            assert!(e.message.contains(says), "{bad_json}: {}", e.message);
        }
    }

    #[test]
    fn the_key_is_compared_whole() {
        assert!(same("abc", "abc"));
        assert!(!same("abc", "abd"));
        assert!(!same("abc", "ab"));
    }
}
