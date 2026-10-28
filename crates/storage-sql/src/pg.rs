//! PostgreSQL wire protocol (v3): startup, SCRAM-SHA-256 or trust authentication, the simple
//! and extended query protocols, text and binary value formats.

use std::collections::HashMap;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use storage_types::Decimal;

use crate::ast::{DataType, Expr, InsertSource, SetExpr, Statement};
use crate::datum::{self, Datum};
use crate::error::{SqlError, SqlResult};
use crate::exec::{system, Engine, OutCol, Outcome, Session};
use crate::hash;
use crate::lexer::Dialect;
use crate::parser::parse;

/// Who may connect: an empty map trusts everyone; otherwise SCRAM-SHA-256 against these passwords.
#[derive(Debug, Clone, Default)]
pub struct Auth {
    pub users: HashMap<String, String>,
}

/// Accepts PostgreSQL clients on `listener` forever (one thread per connection).
pub fn serve(engine: Arc<Engine>, listener: TcpListener, auth: Arc<Auth>) {
    for conn in listener.incoming() {
        let Ok(stream) = conn else { continue };
        let engine = Arc::clone(&engine);
        let auth = Arc::clone(&auth);
        thread::spawn(move || {
            let _ = stream.set_nodelay(true);
            if let Err(e) = Conn::run(engine, stream, &auth) {
                if e.kind() != io::ErrorKind::UnexpectedEof
                    && e.kind() != io::ErrorKind::ConnectionReset
                {
                    eprintln!("storine-sql: postgres connection ended: {e}");
                }
            }
        });
    }
}

const MAX_MESSAGE: usize = 1 << 30;

struct Prepared {
    stmt: Statement,
    param_types: Vec<u32>,
}

struct Portal {
    stmt: Statement,
    params: Vec<Datum>,
    result_formats: Vec<i16>,
    /// Result computed at Describe time (or on the first Execute), with the rows already sent.
    result: Option<(Outcome, usize)>,
}

struct Conn {
    r: BufReader<TcpStream>,
    w: BufWriter<TcpStream>,
    session: Session,
    prepared: HashMap<String, Prepared>,
    portals: HashMap<String, Portal>,
    /// After an error in the extended protocol, messages are skipped until Sync.
    skipping: bool,
}

fn read_i32(r: &mut (impl Read + ?Sized)) -> io::Result<i32> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b)?;
    Ok(i32::from_be_bytes(b))
}

fn bad_input(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.to_owned())
}

/// A message body reader.
struct Body<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> Body<'a> {
    fn cstr(&mut self) -> io::Result<String> {
        let end = self.b[self.pos..]
            .iter()
            .position(|c| *c == 0)
            .ok_or_else(|| bad_input("missing NUL"))?;
        let s = String::from_utf8(self.b[self.pos..self.pos + end].to_vec())
            .map_err(|_| bad_input("bad UTF-8"))?;
        self.pos += end + 1;
        Ok(s)
    }

    fn take(&mut self, n: usize) -> io::Result<&'a [u8]> {
        if self.pos + n > self.b.len() {
            return Err(bad_input("truncated message"));
        }
        let s = &self.b[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    fn i16(&mut self) -> io::Result<i16> {
        let s = self.take(2)?;
        Ok(i16::from_be_bytes([s[0], s[1]]))
    }

    fn i32(&mut self) -> io::Result<i32> {
        let s = self.take(4)?;
        Ok(i32::from_be_bytes([s[0], s[1], s[2], s[3]]))
    }

    fn u8(&mut self) -> io::Result<u8> {
        Ok(self.take(1)?[0])
    }
}

/// An outgoing message.
struct Msg {
    tag: u8,
    b: Vec<u8>,
}

impl Msg {
    fn new(tag: u8) -> Msg {
        Msg { tag, b: Vec::new() }
    }

    fn cstr(mut self, s: &str) -> Msg {
        self.b.extend_from_slice(s.as_bytes());
        self.b.push(0);
        self
    }

    fn i16(mut self, v: i16) -> Msg {
        self.b.extend_from_slice(&v.to_be_bytes());
        self
    }

    fn i32(mut self, v: i32) -> Msg {
        self.b.extend_from_slice(&v.to_be_bytes());
        self
    }

    fn u8(mut self, v: u8) -> Msg {
        self.b.push(v);
        self
    }

    fn bytes(mut self, v: &[u8]) -> Msg {
        self.b.extend_from_slice(v);
        self
    }
}

impl Conn {
    fn run(engine: Arc<Engine>, stream: TcpStream, auth: &Auth) -> io::Result<()> {
        let mut r = BufReader::new(stream.try_clone()?);
        let mut w = BufWriter::new(stream);
        // Startup (answer 'N' to SSL / GSS encryption requests: the client continues in clear).
        let params = loop {
            let len = read_i32(&mut r)? as usize;
            if !(8..=10_000).contains(&len) {
                return Err(bad_input("bad startup packet"));
            }
            let mut body = vec![0u8; len - 4];
            r.read_exact(&mut body)?;
            let code = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);
            match code {
                80_877_103 | 80_877_104 => {
                    w.write_all(b"N")?;
                    w.flush()?;
                }
                80_877_102 => return Ok(()), // cancel request: not supported, just close
                196_608 => {
                    let mut p = HashMap::new();
                    let mut b = Body {
                        b: &body[4..],
                        pos: 0,
                    };
                    loop {
                        let k = b.cstr()?;
                        if k.is_empty() {
                            break;
                        }
                        let v = b.cstr()?;
                        p.insert(k, v);
                    }
                    break p;
                }
                _ => {
                    send_raw(
                        &mut w,
                        Msg::new(b'E')
                            .u8(b'S')
                            .cstr("FATAL")
                            .u8(b'C')
                            .cstr("0A000")
                            .u8(b'M')
                            .cstr("unsupported protocol version")
                            .u8(0),
                    )?;
                    w.flush()?;
                    return Ok(());
                }
            }
        };
        let user = params
            .get("user")
            .cloned()
            .unwrap_or_else(|| "storine".into());
        let database = params.get("database").cloned();
        if !auth.users.is_empty() {
            match scram(&mut r, &mut w, auth.users.get(&user).map(String::as_str)) {
                Ok(true) => {}
                Ok(false) => {
                    let e = SqlError::auth(format!(
                        "password authentication failed for user \"{user}\""
                    ));
                    send_raw(&mut w, error_msg(&e, "FATAL"))?;
                    w.flush()?;
                    return Ok(());
                }
                Err(e) => return Err(e),
            }
        }
        send_raw(&mut w, Msg::new(b'R').i32(0))?;
        let mut session = engine.new_session(Dialect::Postgres, &user, database.as_deref());
        if let Some(app) = params.get("application_name") {
            session.vars.insert("application_name".into(), app.clone());
        }
        for (k, v) in [
            ("server_version", crate::PG_VERSION.to_owned()),
            ("server_encoding", "UTF8".into()),
            ("client_encoding", "UTF8".into()),
            ("DateStyle", "ISO, MDY".into()),
            ("TimeZone", "UTC".into()),
            ("integer_datetimes", "on".into()),
            ("standard_conforming_strings", "on".into()),
            ("IntervalStyle", "postgres".into()),
            ("is_superuser", "on".into()),
            ("session_authorization", user.clone()),
            (
                "application_name",
                params.get("application_name").cloned().unwrap_or_default(),
            ),
        ] {
            send_raw(&mut w, Msg::new(b'S').cstr(k).cstr(&v))?;
        }
        send_raw(
            &mut w,
            Msg::new(b'K').i32(session.id as i32).i32(0x5354_4f52),
        )?;
        let mut c = Conn {
            r,
            w,
            session,
            prepared: HashMap::new(),
            portals: HashMap::new(),
            skipping: false,
        };
        c.ready()?;
        c.loop_messages()
    }

    fn send(&mut self, m: Msg) -> io::Result<()> {
        send_raw(&mut self.w, m)
    }

    fn ready(&mut self) -> io::Result<()> {
        let status = if self.session.transaction_failed() {
            b'E'
        } else if self.session.in_transaction() {
            b'T'
        } else {
            b'I'
        };
        self.send(Msg::new(b'Z').u8(status))?;
        self.w.flush()
    }

    fn error(&mut self, e: &SqlError) -> io::Result<()> {
        if crate::log_enabled() {
            eprintln!("[pg {}] ERROR {}", self.session.id, e);
        }
        self.send(error_msg(e, "ERROR"))
    }

    fn loop_messages(&mut self) -> io::Result<()> {
        loop {
            let mut tag = [0u8; 1];
            self.r.read_exact(&mut tag)?;
            let len = read_i32(&mut self.r)? as usize;
            if !(4..=MAX_MESSAGE).contains(&len) {
                return Err(bad_input("bad message length"));
            }
            let mut body = vec![0u8; len - 4];
            self.r.read_exact(&mut body)?;
            if self.skipping && !matches!(tag[0], b'S' | b'X') {
                continue;
            }
            let r = match tag[0] {
                b'Q' => self.simple_query(&body),
                b'P' => self.parse_msg(&body),
                b'B' => self.bind(&body),
                b'D' => self.describe(&body),
                b'E' => self.execute(&body),
                b'C' => self.close(&body),
                b'S' => {
                    self.skipping = false;
                    self.ready()?;
                    Ok(())
                }
                b'H' => {
                    self.w.flush()?;
                    Ok(())
                }
                b'X' => return Ok(()),
                b'p' => Ok(()), // stray password message
                b'd' | b'c' | b'f' => Err(SqlError::unsupported("COPY")),
                b'F' => Err(SqlError::unsupported("the function-call protocol")),
                other => Err(SqlError::new(
                    "08P01",
                    format!("unknown message type '{}'", other as char),
                )),
            };
            match r {
                Ok(()) => {}
                Err(e) => {
                    self.error(&e)?;
                    if tag[0] == b'Q' {
                        self.ready()?;
                    } else {
                        self.skipping = true;
                        self.w.flush()?;
                    }
                }
            }
        }
    }

    fn simple_query(&mut self, body: &[u8]) -> SqlResult<()> {
        let sql = std::str::from_utf8(body.strip_suffix(&[0]).unwrap_or(body))
            .map_err(|_| SqlError::new("22021", "invalid UTF-8"))?;
        if crate::log_enabled() {
            eprintln!("[pg {}] {sql}", self.session.id);
        }
        let stmts = match parse(sql, Dialect::Postgres) {
            Ok((s, _)) => s,
            Err(e) => {
                // Errors in a simple query end it; ReadyForQuery is sent by the caller.
                return Err(e);
            }
        };
        if stmts.is_empty() {
            self.send(Msg::new(b'I')).map_err(io_err)?;
            self.ready().map_err(io_err)?;
            return Ok(());
        }
        for st in &stmts {
            let outcome = match self.session.execute(st, &[]) {
                Ok(o) => o,
                Err(e) => {
                    self.error(&e).map_err(io_err)?;
                    self.ready().map_err(io_err)?;
                    return Ok(());
                }
            };
            self.send_outcome(&outcome, &[], true, 0).map_err(io_err)?;
        }
        self.ready().map_err(io_err)?;
        Ok(())
    }

    fn parse_msg(&mut self, body: &[u8]) -> SqlResult<()> {
        let mut b = Body { b: body, pos: 0 };
        let name = b.cstr().map_err(io_sql)?;
        let sql = b.cstr().map_err(io_sql)?;
        let n = b.i16().map_err(io_sql)?.max(0) as usize;
        let mut types = Vec::with_capacity(n);
        for _ in 0..n {
            types.push(b.i32().map_err(io_sql)? as u32);
        }
        if crate::log_enabled() {
            eprintln!("[pg {} parse] {sql}", self.session.id);
        }
        let (stmts, count) = parse(&sql, Dialect::Postgres)?;
        let stmt = match stmts.len() {
            0 => Statement::Empty,
            1 => stmts.into_iter().next().unwrap_or(Statement::Empty),
            _ => {
                return Err(SqlError::syntax(
                    "cannot insert multiple commands into a prepared statement".into(),
                ))
            }
        };
        let mut inferred = infer_param_types(&self.session.engine, &stmt, count.max(types.len()));
        for (i, t) in types.iter().enumerate() {
            if *t != 0 {
                inferred[i] = *t;
            }
        }
        self.prepared.insert(
            name,
            Prepared {
                stmt,
                param_types: inferred,
            },
        );
        self.send(Msg::new(b'1')).map_err(io_err)
    }

    fn bind(&mut self, body: &[u8]) -> SqlResult<()> {
        let mut b = Body { b: body, pos: 0 };
        let portal = b.cstr().map_err(io_sql)?;
        let name = b.cstr().map_err(io_sql)?;
        let nf = b.i16().map_err(io_sql)?.max(0) as usize;
        let mut formats = Vec::with_capacity(nf);
        for _ in 0..nf {
            formats.push(b.i16().map_err(io_sql)?);
        }
        let np = b.i16().map_err(io_sql)?.max(0) as usize;
        let prep = self.prepared.get(&name).ok_or_else(|| {
            SqlError::new(
                "26000",
                format!("prepared statement \"{name}\" does not exist"),
            )
        })?;
        let mut params = Vec::with_capacity(np);
        for i in 0..np {
            let len = b.i32().map_err(io_sql)?;
            if len < 0 {
                params.push(Datum::Null);
                continue;
            }
            let raw = b.take(len as usize).map_err(io_sql)?;
            let fmt = match formats.len() {
                0 => 0,
                1 => formats[0],
                _ => formats.get(i).copied().unwrap_or(0),
            };
            let oid = prep.param_types.get(i).copied().unwrap_or(0);
            params.push(if fmt == 1 {
                decode_binary(oid, raw)?
            } else {
                decode_text(oid, raw)?
            });
        }
        let nr = b.i16().map_err(io_sql)?.max(0) as usize;
        let mut result_formats = Vec::with_capacity(nr);
        for _ in 0..nr {
            result_formats.push(b.i16().map_err(io_sql)?);
        }
        let stmt = prep.stmt.clone();
        self.portals.insert(
            portal,
            Portal {
                stmt,
                params,
                result_formats,
                result: None,
            },
        );
        self.send(Msg::new(b'2')).map_err(io_err)
    }

    fn describe(&mut self, body: &[u8]) -> SqlResult<()> {
        let mut b = Body { b: body, pos: 0 };
        let kind = b.u8().map_err(io_sql)?;
        let name = b.cstr().map_err(io_sql)?;
        if kind == b'S' {
            let prep = self.prepared.get(&name).ok_or_else(|| {
                SqlError::new(
                    "26000",
                    format!("prepared statement \"{name}\" does not exist"),
                )
            })?;
            let mut m = Msg::new(b't').i16(prep.param_types.len() as i16);
            for t in &prep.param_types {
                m = m.i32(*t as i32);
            }
            let stmt = prep.stmt.clone();
            let nparams = prep.param_types.len();
            self.send(m).map_err(io_err)?;
            match describe_columns(&mut self.session, &stmt, nparams)? {
                Some(cols) => self.send(row_description(&cols, &[])).map_err(io_err)?,
                None => self.send(Msg::new(b'n')).map_err(io_err)?,
            }
            return Ok(());
        }
        let portal = self
            .portals
            .get_mut(&name)
            .ok_or_else(|| SqlError::new("34000", format!("portal \"{name}\" does not exist")))?;
        // Describing a portal runs it (its parameters are bound); Execute then streams the rows.
        if portal.result.is_none() {
            let (stmt, params) = (portal.stmt.clone(), portal.params.clone());
            let o = self.session.execute(&stmt, &params)?;
            if let Some(p) = self.portals.get_mut(&name) {
                p.result = Some((o, 0));
            }
        }
        let portal = self
            .portals
            .get(&name)
            .ok_or_else(|| SqlError::internal("portal vanished"))?;
        let msg = match &portal.result {
            Some((Outcome::Rows(rs), _)) => row_description(&rs.columns, &portal.result_formats),
            _ => Msg::new(b'n'),
        };
        self.send(msg).map_err(io_err)
    }

    fn execute(&mut self, body: &[u8]) -> SqlResult<()> {
        let mut b = Body { b: body, pos: 0 };
        let name = b.cstr().map_err(io_sql)?;
        let max_rows = b.i32().map_err(io_sql)?.max(0) as usize;
        let portal = self
            .portals
            .get_mut(&name)
            .ok_or_else(|| SqlError::new("34000", format!("portal \"{name}\" does not exist")))?;
        if portal.result.is_none() {
            let (stmt, params) = (portal.stmt.clone(), portal.params.clone());
            let o = self.session.execute(&stmt, &params)?;
            if let Some(p) = self.portals.get_mut(&name) {
                p.result = Some((o, 0));
            }
        }
        let Some(portal) = self.portals.get_mut(&name) else {
            return Err(SqlError::internal("portal vanished"));
        };
        let formats = portal.result_formats.clone();
        let Some((outcome, sent)) = portal.result.take() else {
            return Ok(());
        };
        if let Outcome::Rows(rs) = &outcome {
            let end = if max_rows == 0 {
                rs.rows.len()
            } else {
                (sent + max_rows).min(rs.rows.len())
            };
            for row in &rs.rows[sent..end] {
                let m = data_row(&rs.columns, row, &formats);
                send_raw(&mut self.w, m).map_err(io_err)?;
            }
            if end < rs.rows.len() {
                self.send(Msg::new(b's')).map_err(io_err)?;
                if let Some(p) = self.portals.get_mut(&name) {
                    p.result = Some((outcome, end));
                }
                return Ok(());
            }
            self.send(Msg::new(b'C').cstr(&format!("SELECT {}", rs.rows.len())))
                .map_err(io_err)?;
        } else {
            self.send_outcome(&outcome, &formats, false, 0)
                .map_err(io_err)?;
        }
        // The unnamed portal is single-use; named ones may be executed again.
        if !name.is_empty() {
            if let Some(p) = self.portals.get_mut(&name) {
                p.result = None;
            }
        }
        Ok(())
    }

    fn close(&mut self, body: &[u8]) -> SqlResult<()> {
        let mut b = Body { b: body, pos: 0 };
        let kind = b.u8().map_err(io_sql)?;
        let name = b.cstr().map_err(io_sql)?;
        if kind == b'S' {
            self.prepared.remove(&name);
        } else {
            self.portals.remove(&name);
        }
        self.send(Msg::new(b'3')).map_err(io_err)
    }

    /// Sends a statement's result (`describe`: with its RowDescription first).
    fn send_outcome(
        &mut self,
        o: &Outcome,
        formats: &[i16],
        describe: bool,
        _skip: usize,
    ) -> io::Result<()> {
        match o {
            Outcome::Rows(rs) => {
                if describe {
                    self.send(row_description(&rs.columns, formats))?;
                }
                for row in &rs.rows {
                    let m = data_row(&rs.columns, row, formats);
                    send_raw(&mut self.w, m)?;
                }
                self.send(Msg::new(b'C').cstr(&format!("SELECT {}", rs.rows.len())))
            }
            Outcome::Command { tag, .. } if tag.is_empty() => self.send(Msg::new(b'I')),
            Outcome::Command { tag, .. } => self.send(Msg::new(b'C').cstr(tag)),
        }
    }
}

fn io_err(e: io::Error) -> SqlError {
    SqlError::new("08006", e.to_string())
}

fn io_sql(e: io::Error) -> SqlError {
    SqlError::new("08P01", e.to_string())
}

fn send_raw(w: &mut impl Write, m: Msg) -> io::Result<()> {
    let mut hdr = [0u8; 5];
    hdr[0] = m.tag;
    hdr[1..].copy_from_slice(&((m.b.len() + 4) as i32).to_be_bytes());
    w.write_all(&hdr)?;
    w.write_all(&m.b)
}

fn error_msg(e: &SqlError, severity: &str) -> Msg {
    Msg::new(b'E')
        .u8(b'S')
        .cstr(severity)
        .u8(b'V')
        .cstr(severity)
        .u8(b'C')
        .cstr(e.code)
        .u8(b'M')
        .cstr(&e.message)
        .u8(0)
}

/// SCRAM-SHA-256 (RFC 5802 / 7677) exchange; `Ok(false)`: wrong password or unknown user.
fn scram(r: &mut impl Read, w: &mut impl Write, password: Option<&str>) -> io::Result<bool> {
    send_raw(w, Msg::new(b'R').i32(10).cstr("SCRAM-SHA-256").u8(0))?;
    w.flush()?;
    let read_msg = |r: &mut dyn Read| -> io::Result<Vec<u8>> {
        let mut tag = [0u8; 1];
        r.read_exact(&mut tag)?;
        let len = read_i32(&mut *r)? as usize;
        if tag[0] != b'p' || !(4..=10_000).contains(&len) {
            return Err(bad_input("expected a SASL response"));
        }
        let mut body = vec![0u8; len - 4];
        r.read_exact(&mut body)?;
        Ok(body)
    };
    let first = read_msg(r)?;
    let mut b = Body { b: &first, pos: 0 };
    let mech = b.cstr()?;
    if mech != "SCRAM-SHA-256" {
        return Ok(false);
    }
    let n = b.i32()?.max(0) as usize;
    let client_first = String::from_utf8(b.take(n)?.to_vec()).map_err(|_| bad_input("bad SASL"))?;
    // gs2 header "n,," or "y,,"; the bare part follows.
    let mut parts = client_first.splitn(3, ',');
    let gs2_flag = parts.next().unwrap_or("");
    let _authzid = parts.next();
    let bare = parts.next().unwrap_or("").to_owned();
    let cnonce = bare
        .split(',')
        .find_map(|kv| kv.strip_prefix("r="))
        .unwrap_or("")
        .to_owned();
    let salt = hash::random_bytes(16);
    let snonce = hash::base64_encode(&hash::random_bytes(18));
    let nonce = format!("{cnonce}{snonce}");
    let iterations = 4096;
    let server_first = format!("r={nonce},s={},i={iterations}", hash::base64_encode(&salt));
    send_raw(w, Msg::new(b'R').i32(11).bytes(server_first.as_bytes()))?;
    w.flush()?;
    let fin = read_msg(r)?;
    let client_final = String::from_utf8(fin).map_err(|_| bad_input("bad SASL"))?;
    let (without_proof, proof_b64) = match client_final.rsplit_once(",p=") {
        Some(x) => x,
        None => return Ok(false),
    };
    let channel = client_final
        .split(',')
        .find_map(|kv| kv.strip_prefix("c="))
        .unwrap_or("");
    let expected_channel = hash::base64_encode(format!("{gs2_flag},,").as_bytes());
    let got_nonce = client_final
        .split(',')
        .find_map(|kv| kv.strip_prefix("r="))
        .unwrap_or("");
    let Some(password) = password else {
        return Ok(false);
    };
    if got_nonce != nonce || channel != expected_channel {
        return Ok(false);
    }
    let salted = hash::pbkdf2_sha256(password.as_bytes(), &salt, iterations);
    let client_key = hash::hmac_sha256(&salted, b"Client Key");
    let stored_key = hash::sha256(&client_key);
    let auth_message = format!("{bare},{server_first},{without_proof}");
    let client_sig = hash::hmac_sha256(&stored_key, auth_message.as_bytes());
    let Some(proof) = hash::base64_decode(proof_b64) else {
        return Ok(false);
    };
    if proof.len() != 32 {
        return Ok(false);
    }
    let recovered: Vec<u8> = proof
        .iter()
        .zip(client_sig.iter())
        .map(|(a, b)| a ^ b)
        .collect();
    if !hash::ct_eq(&hash::sha256(&recovered), &stored_key) {
        return Ok(false);
    }
    let server_key = hash::hmac_sha256(&salted, b"Server Key");
    let server_sig = hash::hmac_sha256(&server_key, auth_message.as_bytes());
    send_raw(
        w,
        Msg::new(b'R')
            .i32(12)
            .bytes(format!("v={}", hash::base64_encode(&server_sig)).as_bytes()),
    )?;
    Ok(true)
}

fn type_len(ty: &DataType) -> i16 {
    match ty {
        DataType::Bool => 1,
        DataType::SmallInt => 2,
        DataType::Int | DataType::Real | DataType::Date => 4,
        DataType::BigInt
        | DataType::Double
        | DataType::Timestamp
        | DataType::TimestampTz
        | DataType::Time => 8,
        DataType::Uuid | DataType::Interval => 16,
        _ => -1,
    }
}

fn format_for(formats: &[i16], i: usize) -> i16 {
    match formats.len() {
        0 => 0,
        1 => formats[0],
        _ => formats.get(i).copied().unwrap_or(0),
    }
}

fn row_description(cols: &[OutCol], formats: &[i16]) -> Msg {
    let mut m = Msg::new(b'T').i16(cols.len() as i16);
    for (i, c) in cols.iter().enumerate() {
        let table = c
            .table
            .as_deref()
            .map_or(0, |t| system::table_oid(t) as i32);
        m = m
            .cstr(&c.name)
            .i32(table)
            .i16(0)
            .i32(system::type_oid(&c.ty) as i32)
            .i16(type_len(&c.ty))
            .i32(system::type_mod(&c.ty))
            .i16(format_for(formats, i));
    }
    m
}

fn data_row(cols: &[OutCol], row: &[Datum], formats: &[i16]) -> Msg {
    let mut m = Msg::new(b'D').i16(row.len() as i16);
    for (i, d) in row.iter().enumerate() {
        if d.is_null() {
            m = m.i32(-1);
            continue;
        }
        let ty = cols.get(i).map_or(DataType::Text, |c| c.ty.clone());
        let bytes = if format_for(formats, i) == 1 {
            encode_binary(d, &ty)
        } else {
            encode_text(d, &ty).into_bytes()
        };
        m = m.i32(bytes.len() as i32).bytes(&bytes);
    }
    m
}

/// Text format of a value in a column of type `ty`.
pub fn encode_text(d: &Datum, ty: &DataType) -> String {
    match (d, ty) {
        (Datum::Timestamp(ns), DataType::TimestampTz) => datum::format_timestamp(*ns, true),
        (Datum::Bool(b), _) => if *b { "t" } else { "f" }.into(),
        (Datum::Int(i), DataType::Bool) => if *i != 0 { "t" } else { "f" }.into(),
        (Datum::Array(items), DataType::Array(inner)) if **inner == DataType::TimestampTz => {
            let parts: Vec<String> = items
                .iter()
                .map(|x| {
                    if x.is_null() {
                        "NULL".into()
                    } else {
                        format!("\"{}\"", encode_text(x, inner))
                    }
                })
                .collect();
            format!("{{{}}}", parts.join(","))
        }
        (other, _) => other.as_text(),
    }
}

// Binary formats: PostgreSQL epoch is 2000-01-01.
const PG_EPOCH_US: i64 = 946_684_800_000_000;
const PG_EPOCH_DAYS: i32 = 10_957;

fn encode_binary(d: &Datum, ty: &DataType) -> Vec<u8> {
    let d = datum::cast(d, ty).unwrap_or_else(|_| d.clone());
    match (ty, &d) {
        (DataType::Bool, _) => vec![u8::from(d.truthy().ok().flatten().unwrap_or(false))],
        (DataType::SmallInt, _) => (d.as_i64().unwrap_or(0) as i16).to_be_bytes().to_vec(),
        (DataType::Int, _) => (d.as_i64().unwrap_or(0) as i32).to_be_bytes().to_vec(),
        (DataType::BigInt, _) => d.as_i64().unwrap_or(0).to_be_bytes().to_vec(),
        (DataType::Real, _) => (datum::to_f64(&d).unwrap_or(0.0) as f32)
            .to_be_bytes()
            .to_vec(),
        (DataType::Double, _) => datum::to_f64(&d).unwrap_or(0.0).to_be_bytes().to_vec(),
        (DataType::Numeric(_), Datum::Numeric(x)) => numeric_binary(*x),
        (DataType::Numeric(_), _) => numeric_binary(datum::to_decimal(&d).unwrap_or_default()),
        (DataType::Bytes, Datum::Bytes(b)) => b.clone(),
        (DataType::Uuid, Datum::Uuid(u)) => u.to_vec(),
        (DataType::Date, Datum::Date(days)) => (days - PG_EPOCH_DAYS).to_be_bytes().to_vec(),
        (DataType::Timestamp | DataType::TimestampTz, Datum::Timestamp(ns)) => {
            (ns.div_euclid(1000) - PG_EPOCH_US).to_be_bytes().to_vec()
        }
        (DataType::Time, Datum::Time(ns)) => (ns / 1000).to_be_bytes().to_vec(),
        (DataType::Interval, Datum::Interval { months, days, ns }) => {
            let mut v = (ns / 1000).to_be_bytes().to_vec();
            v.extend_from_slice(&(*days as i32).to_be_bytes());
            v.extend_from_slice(&(*months as i32).to_be_bytes());
            v
        }
        (DataType::Jsonb, _) => {
            let mut v = vec![1u8];
            v.extend_from_slice(encode_text(&d, ty).as_bytes());
            v
        }
        (DataType::Array(inner), Datum::Array(items)) => {
            let mut v = Vec::new();
            v.extend_from_slice(&1i32.to_be_bytes());
            v.extend_from_slice(&i32::from(items.iter().any(Datum::is_null)).to_be_bytes());
            v.extend_from_slice(&(system::type_oid(inner) as i32).to_be_bytes());
            v.extend_from_slice(&(items.len() as i32).to_be_bytes());
            v.extend_from_slice(&1i32.to_be_bytes());
            for x in items {
                if x.is_null() {
                    v.extend_from_slice(&(-1i32).to_be_bytes());
                } else {
                    let e = encode_binary(x, inner);
                    v.extend_from_slice(&(e.len() as i32).to_be_bytes());
                    v.extend_from_slice(&e);
                }
            }
            v
        }
        _ => encode_text(&d, ty).into_bytes(),
    }
}

/// PostgreSQL binary NUMERIC: base-10000 digits with weight, sign and display scale.
fn numeric_binary(x: Decimal) -> Vec<u8> {
    let scale = x.scale() as usize;
    let neg = x.is_negative();
    let digits = x.mantissa().unsigned_abs().to_string();
    let (int_part, frac_part) = if digits.len() > scale {
        (
            digits[..digits.len() - scale].to_owned(),
            digits[digits.len() - scale..].to_owned(),
        )
    } else {
        (
            "0".to_owned(),
            format!("{}{digits}", "0".repeat(scale - digits.len())),
        )
    };
    // Group the integer part from the right, the fraction from the left, in 4-digit groups.
    let ipad = (4 - int_part.len() % 4) % 4;
    let ip = format!("{}{int_part}", "0".repeat(ipad));
    let fpad = (4 - frac_part.len() % 4) % 4;
    let fp = format!("{frac_part}{}", "0".repeat(fpad));
    let mut groups: Vec<i16> = Vec::new();
    for c in ip.as_bytes().chunks(4).chain(fp.as_bytes().chunks(4)) {
        groups.push(
            std::str::from_utf8(c)
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0),
        );
    }
    let mut weight = (ip.len() / 4) as i16 - 1;
    // Strip leading and trailing zero groups.
    while groups.first() == Some(&0) && !groups.is_empty() {
        groups.remove(0);
        weight -= 1;
    }
    while groups.last() == Some(&0) {
        groups.pop();
    }
    if groups.is_empty() {
        weight = 0;
    }
    let mut v = Vec::with_capacity(8 + 2 * groups.len());
    v.extend_from_slice(&(groups.len() as i16).to_be_bytes());
    v.extend_from_slice(&weight.to_be_bytes());
    v.extend_from_slice(&(if neg { 0x4000u16 } else { 0 }).to_be_bytes());
    v.extend_from_slice(&(scale as i16).to_be_bytes());
    for g in groups {
        v.extend_from_slice(&g.to_be_bytes());
    }
    v
}

fn numeric_from_binary(raw: &[u8]) -> SqlResult<Datum> {
    let bad = || SqlError::new("22P03", "invalid binary numeric");
    if raw.len() < 8 {
        return Err(bad());
    }
    let n = i16::from_be_bytes([raw[0], raw[1]]) as usize;
    let weight = i32::from(i16::from_be_bytes([raw[2], raw[3]]));
    let sign = u16::from_be_bytes([raw[4], raw[5]]);
    let dscale = i16::from_be_bytes([raw[6], raw[7]]).max(0) as usize;
    if sign == 0xC000 {
        return Ok(Datum::Float(f64::NAN));
    }
    if raw.len() < 8 + 2 * n {
        return Err(bad());
    }
    let mut s = String::new();
    for k in 0..n {
        let g = i16::from_be_bytes([raw[8 + 2 * k], raw[9 + 2 * k]]);
        s.push_str(&format!("{g:04}"));
    }
    // Digits represent groups at weights weight, weight-1, ...
    let int_groups = weight + 1;
    let text = if int_groups <= 0 {
        format!("0.{}{s}", "0000".repeat((-int_groups) as usize))
    } else if (int_groups as usize) >= n {
        format!("{s}{}", "0000".repeat(int_groups as usize - n))
    } else {
        format!(
            "{}.{}",
            &s[..4 * int_groups as usize],
            &s[4 * int_groups as usize..]
        )
    };
    let mut d = datum::parse_dec(&text).map_err(|_| bad())?;
    d = d
        .rescale(dscale.min(38) as u8, storage_types::RoundingMode::HalfUp)
        .map_err(|_| bad())?;
    if sign == 0x4000 {
        d = d.checked_neg().map_err(|_| bad())?;
    }
    Ok(Datum::Numeric(d))
}

fn decode_text(oid: u32, raw: &[u8]) -> SqlResult<Datum> {
    let s = String::from_utf8(raw.to_vec())
        .map_err(|_| SqlError::new("22021", "invalid UTF-8 in parameter"))?;
    let ty = match oid {
        16 => DataType::Bool,
        20 | 21 | 23 => DataType::BigInt,
        700 | 701 => DataType::Double,
        1700 => DataType::Numeric(None),
        1082 => DataType::Date,
        1114 | 1184 => DataType::Timestamp,
        1083 => DataType::Time,
        2950 => DataType::Uuid,
        17 => DataType::Bytes,
        114 | 3802 => return Ok(Datum::Json(s)),
        _ => return Ok(Datum::Text(s)),
    };
    datum::cast(&Datum::Text(s), &ty)
}

fn decode_binary(oid: u32, raw: &[u8]) -> SqlResult<Datum> {
    let bad = || SqlError::new("22P03", format!("invalid binary parameter for type {oid}"));
    let be8 = |r: &[u8]| -> SqlResult<[u8; 8]> { r.try_into().map_err(|_| bad()) };
    Ok(match oid {
        16 => Datum::Bool(raw.first().copied().unwrap_or(0) != 0),
        21 => Datum::Int(i64::from(i16::from_be_bytes(
            raw.try_into().map_err(|_| bad())?,
        ))),
        23 => Datum::Int(i64::from(i32::from_be_bytes(
            raw.try_into().map_err(|_| bad())?,
        ))),
        20 => Datum::Int(i64::from_be_bytes(be8(raw)?)),
        700 => Datum::Float(f64::from(f32::from_be_bytes(
            raw.try_into().map_err(|_| bad())?,
        ))),
        701 => Datum::Float(f64::from_be_bytes(be8(raw)?)),
        1700 => numeric_from_binary(raw)?,
        17 => Datum::Bytes(raw.to_vec()),
        2950 => Datum::Uuid(raw.try_into().map_err(|_| bad())?),
        1082 => Datum::Date(i32::from_be_bytes(raw.try_into().map_err(|_| bad())?) + PG_EPOCH_DAYS),
        1114 | 1184 => Datum::Timestamp((i64::from_be_bytes(be8(raw)?) + PG_EPOCH_US) * 1000),
        1083 => Datum::Time(i64::from_be_bytes(be8(raw)?) * 1000),
        1000 | 1001 | 1003 | 1005 | 1007 | 1009 | 1014 | 1015 | 1016 | 1021 | 1022 | 1115
        | 1182 | 1185 | 1231 | 2951 | 199 | 3807 => {
            // ndim, has-null flag, element type, (length, lower bound) per dimension, elements.
            let mut b = Body { b: raw, pos: 0 };
            let ndim = b.i32().map_err(|_| bad())?;
            let _flags = b.i32().map_err(|_| bad())?;
            let elem = b.i32().map_err(|_| bad())? as u32;
            if ndim == 0 {
                return Ok(Datum::Array(Vec::new()));
            }
            if ndim != 1 {
                return Err(SqlError::unsupported("multi-dimensional arrays"));
            }
            let n = b.i32().map_err(|_| bad())?.max(0) as usize;
            let _lbound = b.i32().map_err(|_| bad())?;
            let mut items = Vec::with_capacity(n.min(65_536));
            for _ in 0..n {
                let len = b.i32().map_err(|_| bad())?;
                if len < 0 {
                    items.push(Datum::Null);
                } else {
                    items.push(decode_binary(
                        elem,
                        b.take(len as usize).map_err(|_| bad())?,
                    )?);
                }
            }
            Datum::Array(items)
        }
        3802 => Datum::Json(String::from_utf8_lossy(raw.get(1..).unwrap_or(&[])).into_owned()),
        114 => Datum::Json(String::from_utf8_lossy(raw).into_owned()),
        _ => Datum::Text(String::from_utf8(raw.to_vec()).map_err(|_| bad())?),
    })
}

/// Parameter types from context (`col = $1`, `INSERT ... VALUES ($1)`, `SET col = $1`, LIMIT).
fn infer_param_types(engine: &Engine, stmt: &Statement, n: usize) -> Vec<u32> {
    let mut types: Vec<Option<DataType>> = vec![None; n];
    let mut set = |i: usize, ty: &DataType| {
        if let Some(slot) = types.get_mut(i) {
            if slot.is_none() {
                *slot = Some(ty.clone());
            }
        }
    };
    let col_type =
        |tables: &[std::sync::Arc<crate::catalog::Table>], name: &str| -> Option<DataType> {
            tables
                .iter()
                .find_map(|t| t.column_index(name).map(|i| t.columns[i].ty.clone()))
        };
    let mut tables = Vec::new();
    let visit = |e: &Expr,
                 tables: &[std::sync::Arc<crate::catalog::Table>],
                 set: &mut dyn FnMut(usize, &DataType)| {
        crate::exec::eval_walk(e, &mut |x| match x {
            Expr::Binary { left, right, .. } => {
                if let (Expr::Column(p), Expr::Param(i)) | (Expr::Param(i), Expr::Column(p)) =
                    (&**left, &**right)
                {
                    if let Some(t) = col_type(tables, &p[p.len() - 1]) {
                        set(*i, &t);
                    }
                }
            }
            Expr::InList { expr, list, .. } => {
                if let Expr::Column(p) = &**expr {
                    if let Some(t) = col_type(tables, &p[p.len() - 1]) {
                        for l in list {
                            if let Expr::Param(i) = l {
                                set(*i, &t);
                            }
                        }
                    }
                }
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                if let Expr::Column(p) = &**expr {
                    if let Some(t) = col_type(tables, &p[p.len() - 1]) {
                        for b in [low, high] {
                            if let Expr::Param(i) = &**b {
                                set(*i, &t);
                            }
                        }
                    }
                }
            }
            Expr::Like { pattern, .. } => {
                if let Expr::Param(i) = &**pattern {
                    set(*i, &DataType::Text);
                }
            }
            _ => {}
        });
    };
    let add_table = |name: &str, tables: &mut Vec<std::sync::Arc<crate::catalog::Table>>| {
        if let Ok(Some(t)) = engine.find_table(name) {
            tables.push(t);
        }
    };
    match stmt {
        Statement::Insert(ins) => {
            add_table(&ins.table, &mut tables);
            if let Some(t) = tables.first().cloned() {
                let target: Vec<usize> = if ins.columns.is_empty() {
                    (0..t.columns.len()).collect()
                } else {
                    ins.columns
                        .iter()
                        .filter_map(|c| t.column_index(c))
                        .collect()
                };
                if let InsertSource::Values(rows) = &ins.source {
                    for r in rows {
                        for (k, e) in r.iter().enumerate() {
                            if let (Expr::Param(i), Some(ci)) = (e, target.get(k)) {
                                set(*i, &t.columns[*ci].ty);
                            }
                        }
                    }
                }
                if let Some(crate::ast::OnConflict::DoUpdate { sets, .. }) = &ins.on_conflict {
                    for (c, e) in sets {
                        if let (Expr::Param(i), Some(ci)) = (e, t.column_index(c)) {
                            set(*i, &t.columns[ci].ty);
                        }
                    }
                }
            }
        }
        Statement::Update(u) => {
            add_table(&u.table, &mut tables);
            if let Some(t) = tables.first().cloned() {
                for (c, e) in &u.sets {
                    if let (Expr::Param(i), Some(ci)) = (e, t.column_index(c)) {
                        set(*i, &t.columns[ci].ty);
                    }
                }
            }
            for (_, e) in &u.sets {
                visit(e, &tables, &mut set);
            }
            if let Some(w) = &u.where_ {
                visit(w, &tables, &mut set);
            }
        }
        Statement::Delete(d) => {
            add_table(&d.table, &mut tables);
            if let Some(w) = &d.where_ {
                visit(w, &tables, &mut set);
            }
        }
        Statement::Query(q) => {
            if let SetExpr::Select(s) = &q.body {
                for f in &s.from {
                    collect_tables(f, &mut |n| add_table(n, &mut tables));
                }
                if let Some(w) = &s.where_ {
                    visit(w, &tables, &mut set);
                }
                if let Some(h) = &s.having {
                    visit(h, &tables, &mut set);
                }
                for it in &s.items {
                    if let crate::ast::SelectItem::Expr { expr, .. } = it {
                        visit(expr, &tables, &mut set);
                    }
                }
            }
            for e in [&q.limit, &q.offset].into_iter().flatten() {
                if let Expr::Param(i) = e {
                    set(*i, &DataType::BigInt);
                }
            }
        }
        _ => {}
    }
    types
        .into_iter()
        .map(|t| t.map_or(25, |t| system::type_oid(&t)))
        .collect()
}

fn collect_tables(t: &crate::ast::TableRef, f: &mut dyn FnMut(&str)) {
    match t {
        crate::ast::TableRef::Named { name, .. } => f(name),
        crate::ast::TableRef::Join { left, right, .. } => {
            collect_tables(left, f);
            collect_tables(right, f);
        }
        _ => {}
    }
}

/// Result columns of a statement without running its writes.
fn describe_columns(
    s: &mut Session,
    stmt: &Statement,
    nparams: usize,
) -> SqlResult<Option<Vec<OutCol>>> {
    let nulls = vec![Datum::Null; nparams];
    match stmt {
        Statement::Query(q) => {
            let mut q = (**q).clone();
            q.lock = None;
            match s.execute(&Statement::Query(Box::new(q)), &nulls)? {
                Outcome::Rows(rs) => Ok(Some(rs.columns)),
                _ => Ok(None),
            }
        }
        Statement::Show(_) => match s.execute(stmt, &nulls)? {
            Outcome::Rows(rs) => Ok(Some(rs.columns)),
            _ => Ok(None),
        },
        Statement::Insert(i) if !i.returning.is_empty() => {
            crate::exec::describe_returning(s, &i.table, &i.returning).map(Some)
        }
        Statement::Update(u) if !u.returning.is_empty() => {
            crate::exec::describe_returning(s, &u.table, &u.returning).map(Some)
        }
        Statement::Delete(d) if !d.returning.is_empty() => {
            crate::exec::describe_returning(s, &d.table, &d.returning).map(Some)
        }
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_binary_round_trips() {
        for s in [
            "0",
            "1",
            "-1",
            "12345.678",
            "0.0001",
            "100.00",
            "-0.5",
            "99999999999999999999.99",
            "10000",
            "0.00",
        ] {
            let d: Decimal = s.parse().unwrap();
            let back = numeric_from_binary(&numeric_binary(d)).unwrap();
            assert_eq!(back.as_text(), d.to_string(), "{s}");
        }
    }
}
