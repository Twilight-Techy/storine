//! MySQL client/server protocol: handshake v10, `mysql_native_password` (clients asking for
//! `caching_sha2_password` are switched to it), text queries with multiple statements, and
//! binary prepared statements.

use std::collections::HashMap;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use crate::ast::{DataType, Statement};
use crate::datum::{self, Datum};
use crate::error::{SqlError, SqlResult};
use crate::exec::{Engine, OutCol, Outcome, Session};
use crate::hash;
use crate::lexer::Dialect;
use crate::parser::{parse, parse_one};

/// Who may connect: an empty map trusts everyone; otherwise these passwords.
#[derive(Debug, Clone, Default)]
pub struct Auth {
    pub users: HashMap<String, String>,
}

/// Accepts MySQL clients on `listener` forever (one thread per connection).
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
                    eprintln!("storine-sql: mysql connection ended: {e}");
                }
            }
        });
    }
}

// Capability flags.
const CLIENT_LONG_PASSWORD: u32 = 1;
const CLIENT_FOUND_ROWS: u32 = 2;
const CLIENT_LONG_FLAG: u32 = 4;
const CLIENT_CONNECT_WITH_DB: u32 = 8;
const CLIENT_PROTOCOL_41: u32 = 0x200;
const CLIENT_TRANSACTIONS: u32 = 0x2000;
const CLIENT_SECURE_CONNECTION: u32 = 0x8000;
const CLIENT_MULTI_STATEMENTS: u32 = 0x1_0000;
const CLIENT_MULTI_RESULTS: u32 = 0x2_0000;
const CLIENT_PS_MULTI_RESULTS: u32 = 0x4_0000;
const CLIENT_PLUGIN_AUTH: u32 = 0x8_0000;
const CLIENT_CONNECT_ATTRS: u32 = 0x10_0000;
const CLIENT_PLUGIN_AUTH_LENENC: u32 = 0x20_0000;
const CLIENT_DEPRECATE_EOF: u32 = 0x100_0000;

const SERVER_CAPS: u32 = CLIENT_LONG_PASSWORD
    | CLIENT_FOUND_ROWS
    | CLIENT_LONG_FLAG
    | CLIENT_CONNECT_WITH_DB
    | CLIENT_PROTOCOL_41
    | CLIENT_TRANSACTIONS
    | CLIENT_SECURE_CONNECTION
    | CLIENT_MULTI_STATEMENTS
    | CLIENT_MULTI_RESULTS
    | CLIENT_PS_MULTI_RESULTS
    | CLIENT_PLUGIN_AUTH
    | CLIENT_CONNECT_ATTRS
    | CLIENT_PLUGIN_AUTH_LENENC
    | CLIENT_DEPRECATE_EOF;

const STATUS_IN_TRANS: u16 = 1;
const STATUS_AUTOCOMMIT: u16 = 2;
const STATUS_MORE_RESULTS: u16 = 8;

// Column types.
const T_DECIMAL: u8 = 0xf6;
const T_TINY: u8 = 1;
const T_SHORT: u8 = 2;
const T_LONG: u8 = 3;
const T_FLOAT: u8 = 4;
const T_DOUBLE: u8 = 5;
const T_NULL: u8 = 6;
const T_TIMESTAMP: u8 = 7;
const T_LONGLONG: u8 = 8;
const T_INT24: u8 = 9;
const T_DATE: u8 = 0x0a;
const T_TIME: u8 = 0x0b;
const T_DATETIME: u8 = 0x0c;
const T_YEAR: u8 = 0x0d;
const T_VARCHAR: u8 = 0x0f;
const T_BIT: u8 = 0x10;
const T_JSON: u8 = 0xf5;
const T_NEWDECIMAL: u8 = 0xf6;
const T_BLOB: u8 = 0xfc;
const T_VAR_STRING: u8 = 0xfd;
const T_STRING: u8 = 0xfe;

const NATIVE: &str = "mysql_native_password";

struct Prepared {
    stmt: Statement,
    nparams: usize,
    columns: Vec<OutCol>,
    /// Parameter types sent with the last execution (reused when a client omits them).
    types: Vec<(u8, bool)>,
    long_data: HashMap<usize, Vec<u8>>,
}

struct Conn {
    r: BufReader<TcpStream>,
    w: BufWriter<TcpStream>,
    seq: u8,
    caps: u32,
    session: Session,
    stmts: HashMap<u32, Prepared>,
    next_stmt: u32,
}

fn lenenc(out: &mut Vec<u8>, n: u64) {
    if n < 251 {
        out.push(n as u8);
    } else if n < 1 << 16 {
        out.push(0xfc);
        out.extend_from_slice(&(n as u16).to_le_bytes());
    } else if n < 1 << 24 {
        out.push(0xfd);
        out.extend_from_slice(&(n as u32).to_le_bytes()[..3]);
    } else {
        out.push(0xfe);
        out.extend_from_slice(&n.to_le_bytes());
    }
}

fn lenenc_str(out: &mut Vec<u8>, s: &[u8]) {
    lenenc(out, s.len() as u64);
    out.extend_from_slice(s);
}

struct Rd<'a> {
    b: &'a [u8],
    pos: usize,
}

fn bad(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.to_owned())
}

impl<'a> Rd<'a> {
    fn left(&self) -> usize {
        self.b.len() - self.pos
    }

    fn take(&mut self, n: usize) -> io::Result<&'a [u8]> {
        if n > self.left() {
            return Err(bad("truncated packet"));
        }
        let s = &self.b[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    fn u8(&mut self) -> io::Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> io::Result<u16> {
        let s = self.take(2)?;
        Ok(u16::from_le_bytes([s[0], s[1]]))
    }

    fn u32(&mut self) -> io::Result<u32> {
        let s = self.take(4)?;
        Ok(u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
    }

    fn u64(&mut self) -> io::Result<u64> {
        let s = self.take(8)?;
        Ok(u64::from_le_bytes(s.try_into().map_err(|_| bad("short"))?))
    }

    fn cstr(&mut self) -> io::Result<&'a [u8]> {
        let end = self.b[self.pos..]
            .iter()
            .position(|c| *c == 0)
            .unwrap_or(self.left());
        let s = &self.b[self.pos..self.pos + end];
        self.pos = (self.pos + end + 1).min(self.b.len());
        Ok(s)
    }

    fn lenenc(&mut self) -> io::Result<u64> {
        Ok(match self.u8()? {
            0xfc => u64::from(self.u16()?),
            0xfd => {
                let s = self.take(3)?;
                u64::from(s[0]) | (u64::from(s[1]) << 8) | (u64::from(s[2]) << 16)
            }
            0xfe => self.u64()?,
            n => u64::from(n),
        })
    }

    fn lenenc_bytes(&mut self) -> io::Result<&'a [u8]> {
        let n = self.lenenc()? as usize;
        self.take(n)
    }
}

impl Conn {
    fn read_packet(&mut self) -> io::Result<Vec<u8>> {
        let mut out = Vec::new();
        loop {
            let mut hdr = [0u8; 4];
            self.r.read_exact(&mut hdr)?;
            let len =
                usize::from(hdr[0]) | (usize::from(hdr[1]) << 8) | (usize::from(hdr[2]) << 16);
            self.seq = hdr[3].wrapping_add(1);
            let start = out.len();
            if start + len > (1 << 30) {
                return Err(bad("packet too large"));
            }
            out.resize(start + len, 0);
            self.r.read_exact(&mut out[start..])?;
            if len < 0xff_ffff {
                return Ok(out);
            }
        }
    }

    fn write_packet(&mut self, payload: &[u8]) -> io::Result<()> {
        let mut rest = payload;
        loop {
            let n = rest.len().min(0xff_ffff);
            let hdr = [n as u8, (n >> 8) as u8, (n >> 16) as u8, self.seq];
            self.seq = self.seq.wrapping_add(1);
            self.w.write_all(&hdr)?;
            self.w.write_all(&rest[..n])?;
            rest = &rest[n..];
            if n < 0xff_ffff {
                return Ok(());
            }
        }
    }

    /// Drivers track autocommit from these flags (PyMySQL skips `SET autocommit` when they agree).
    fn status(&self) -> u16 {
        let mut st = 0;
        if self.session.in_transaction() {
            st |= STATUS_IN_TRANS;
        }
        if self.session.autocommit() {
            st |= STATUS_AUTOCOMMIT;
        }
        st
    }

    fn ok(&mut self, affected: u64, last_id: u64, more: bool, info: &str) -> io::Result<()> {
        let mut p = vec![0u8];
        lenenc(&mut p, affected);
        lenenc(&mut p, last_id);
        let st = self.status() | if more { STATUS_MORE_RESULTS } else { 0 };
        p.extend_from_slice(&st.to_le_bytes());
        p.extend_from_slice(&0u16.to_le_bytes());
        // Servers write the info as a length-encoded string (Connector/J reads it so).
        if !info.is_empty() {
            lenenc_str(&mut p, info.as_bytes());
        }
        self.write_packet(&p)
    }

    fn eof(&mut self, more: bool) -> io::Result<()> {
        let st = self.status() | if more { STATUS_MORE_RESULTS } else { 0 };
        if self.caps & CLIENT_DEPRECATE_EOF != 0 {
            let mut p = vec![0xfe, 0, 0];
            p.extend_from_slice(&st.to_le_bytes());
            p.extend_from_slice(&0u16.to_le_bytes());
            self.write_packet(&p)
        } else {
            let mut p = vec![0xfe, 0, 0];
            p.extend_from_slice(&st.to_le_bytes());
            self.write_packet(&p)
        }
    }

    /// The EOF that separates column definitions from rows (absent with DEPRECATE_EOF).
    fn mid_eof(&mut self) -> io::Result<()> {
        if self.caps & CLIENT_DEPRECATE_EOF == 0 {
            let mut p = vec![0xfe, 0, 0];
            p.extend_from_slice(&self.status().to_le_bytes());
            self.write_packet(&p)?;
        }
        Ok(())
    }

    fn err(&mut self, e: &SqlError) -> io::Result<()> {
        if crate::log_enabled() {
            eprintln!("[mysql {}] ERROR {}", self.session.id, e);
        }
        let mut p = vec![0xff];
        p.extend_from_slice(&e.mysql_code().to_le_bytes());
        p.push(b'#');
        let state = if e.code.len() == 5 { e.code } else { "HY000" };
        p.extend_from_slice(state.as_bytes());
        p.extend_from_slice(e.message.as_bytes());
        self.write_packet(&p)
    }

    fn run(engine: Arc<Engine>, stream: TcpStream, auth: &Auth) -> io::Result<()> {
        let r = BufReader::new(stream.try_clone()?);
        let w = BufWriter::new(stream);
        let session = engine.new_session(Dialect::MySql, "", None);
        let mut c = Conn {
            r,
            w,
            seq: 0,
            caps: 0,
            session,
            stmts: HashMap::new(),
            next_stmt: 1,
        };
        // Handshake.
        let nonce: Vec<u8> = hash::random_bytes(20)
            .into_iter()
            .map(|b| (b % 94) + 33)
            .collect();
        let mut p = vec![10u8];
        p.extend_from_slice(crate::MYSQL_VERSION.as_bytes());
        p.push(0);
        p.extend_from_slice(&(c.session.id as u32).to_le_bytes());
        p.extend_from_slice(&nonce[..8]);
        p.push(0);
        p.extend_from_slice(&(SERVER_CAPS as u16).to_le_bytes());
        p.push(255); // utf8mb4_0900_ai_ci
        p.extend_from_slice(&STATUS_AUTOCOMMIT.to_le_bytes());
        p.extend_from_slice(&((SERVER_CAPS >> 16) as u16).to_le_bytes());
        p.push(21);
        p.extend_from_slice(&[0u8; 10]);
        p.extend_from_slice(&nonce[8..]);
        p.push(0);
        p.extend_from_slice(NATIVE.as_bytes());
        p.push(0);
        c.seq = 0;
        c.write_packet(&p)?;
        c.w.flush()?;
        let resp = c.read_packet()?;
        let mut rd = Rd { b: &resp, pos: 0 };
        let caps = rd.u32()?;
        if caps & CLIENT_PROTOCOL_41 == 0 {
            c.err(&SqlError::new(
                "08004",
                "this server needs a MySQL 4.1+ client",
            ))?;
            c.w.flush()?;
            return Ok(());
        }
        if caps & 0x800 != 0 && rd.left() <= 28 {
            // The client wants TLS (SSL request packet), which this server does not offer.
            c.err(&SqlError::new(
                "08004",
                "TLS is not supported by storine-sql; connect with ssl disabled",
            ))?;
            c.w.flush()?;
            return Ok(());
        }
        c.caps = caps & SERVER_CAPS;
        let _max_packet = rd.u32()?;
        let _charset = rd.u8()?;
        rd.take(23)?;
        let user = String::from_utf8_lossy(rd.cstr()?).into_owned();
        let token: Vec<u8> = if caps & CLIENT_PLUGIN_AUTH_LENENC != 0 {
            rd.lenenc_bytes()?.to_vec()
        } else if caps & CLIENT_SECURE_CONNECTION != 0 {
            let n = rd.u8()? as usize;
            rd.take(n)?.to_vec()
        } else {
            rd.cstr()?.to_vec()
        };
        let database = if caps & CLIENT_CONNECT_WITH_DB != 0 && rd.left() > 0 {
            Some(String::from_utf8_lossy(rd.cstr()?).into_owned())
        } else {
            None
        };
        let plugin = if caps & CLIENT_PLUGIN_AUTH != 0 && rd.left() > 0 {
            String::from_utf8_lossy(rd.cstr()?).into_owned()
        } else {
            NATIVE.to_owned()
        };
        let mut token = token;
        if !auth.users.is_empty() && plugin != NATIVE {
            // Switch to mysql_native_password with the same nonce.
            let mut sw = vec![0xfe];
            sw.extend_from_slice(NATIVE.as_bytes());
            sw.push(0);
            sw.extend_from_slice(&nonce);
            sw.push(0);
            c.write_packet(&sw)?;
            c.w.flush()?;
            token = c.read_packet()?;
        }
        if !auth.users.is_empty() {
            let good = match auth.users.get(&user) {
                Some(pw) => native_check(pw, &nonce, &token),
                None => false,
            };
            if !good {
                c.err(&SqlError::auth(format!("Access denied for user '{user}'")))?;
                c.w.flush()?;
                return Ok(());
            }
        }
        c.session.user = if user.is_empty() { "root".into() } else { user };
        if let Some(db) = database.filter(|d| !d.is_empty()) {
            if let Err(e) = c.session.use_database(&db) {
                c.err(&e)?;
                return c.w.flush();
            }
        }
        c.ok(0, 0, false, "")?;
        c.w.flush()?;
        c.commands()
    }

    fn commands(&mut self) -> io::Result<()> {
        loop {
            let pkt = match self.read_packet() {
                Ok(p) => p,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(e) => return Err(e),
            };
            let Some((&cmd, body)) = pkt.split_first() else {
                continue;
            };
            let r = match cmd {
                0x01 => return Ok(()),
                0x02 => match self.session.use_database(&String::from_utf8_lossy(body)) {
                    Ok(()) => self.ok(0, 0, false, ""),
                    Err(e) => self.err(&e),
                },
                0x03 => self.query(body),
                0x04 => self.eof(false),
                0x09 => self.write_packet(b"Uptime: 1  Threads: 1  Questions: 0"),
                0x0e => self.ok(0, 0, false, ""),
                0x16 => self.prepare(body),
                0x17 => self.execute(body),
                0x18 => {
                    // COM_STMT_SEND_LONG_DATA: no reply.
                    let mut rd = Rd { b: body, pos: 0 };
                    let id = rd.u32()?;
                    let p = rd.u16()? as usize;
                    if let Some(st) = self.stmts.get_mut(&id) {
                        st.long_data
                            .entry(p)
                            .or_default()
                            .extend_from_slice(&body[6..]);
                    }
                    continue;
                }
                0x19 => {
                    let mut rd = Rd { b: body, pos: 0 };
                    let id = rd.u32()?;
                    self.stmts.remove(&id);
                    continue; // no reply
                }
                0x1a => {
                    let mut rd = Rd { b: body, pos: 0 };
                    let id = rd.u32()?;
                    if let Some(st) = self.stmts.get_mut(&id) {
                        st.long_data.clear();
                    }
                    self.ok(0, 0, false, "")
                }
                0x1b => self.eof(false),
                0x1f => {
                    self.session.rollback();
                    self.stmts.clear();
                    self.ok(0, 0, false, "")
                }
                0x11 => self.err(&SqlError::unsupported("COM_CHANGE_USER")),
                other => self.err(&SqlError::new(
                    "08S01",
                    format!("unsupported command 0x{other:02x}"),
                )),
            };
            r?;
            self.w.flush()?;
        }
    }

    fn query(&mut self, body: &[u8]) -> io::Result<()> {
        let sql = String::from_utf8_lossy(body).into_owned();
        if crate::log_enabled() {
            eprintln!("[mysql {}] {sql}", self.session.id);
        }
        let stmts = match parse(&sql, Dialect::MySql) {
            Ok((s, _)) => s,
            Err(e) => return self.err(&e),
        };
        if stmts.is_empty() {
            return self.ok(0, 0, false, "");
        }
        let n = stmts.len();
        for (k, st) in stmts.iter().enumerate() {
            let more = k + 1 < n;
            match self.session.execute(st, &[]) {
                Ok(o) => self.send_text(&o, more)?,
                Err(e) => return self.err(&e),
            }
        }
        Ok(())
    }

    fn affected(&self, o: &Outcome) -> (u64, u64, String) {
        match o {
            Outcome::Command {
                tag,
                affected,
                last_insert_id,
            } => {
                let matched = tag
                    .strip_prefix("UPDATE ")
                    .and_then(|n| n.parse::<u64>().ok());
                let a = match matched {
                    Some(m) if self.caps & CLIENT_FOUND_ROWS != 0 => m,
                    _ => *affected,
                };
                let info = match matched {
                    Some(m) => format!("Rows matched: {m}  Changed: {affected}  Warnings: 0"),
                    None => String::new(),
                };
                (a, last_insert_id.map_or(0, |i| i as u64), info)
            }
            Outcome::Rows(_) => (0, 0, String::new()),
        }
    }

    fn send_text(&mut self, o: &Outcome, more: bool) -> io::Result<()> {
        match o {
            Outcome::Rows(rs) => {
                let mut p = Vec::new();
                lenenc(&mut p, rs.columns.len() as u64);
                self.write_packet(&p)?;
                for c in &rs.columns {
                    let def = column_def(c, &self.session.database);
                    self.write_packet(&def)?;
                }
                self.mid_eof()?;
                for row in &rs.rows {
                    let mut p = Vec::new();
                    for (d, c) in row.iter().zip(&rs.columns) {
                        if d.is_null() {
                            p.push(0xfb);
                        } else {
                            lenenc_str(&mut p, &text_value(d, &c.ty));
                        }
                    }
                    self.write_packet(&p)?;
                }
                self.eof(more)
            }
            other => {
                let (a, id, info) = self.affected(other);
                self.ok(a, id, more, &info)
            }
        }
    }

    fn prepare(&mut self, body: &[u8]) -> io::Result<()> {
        let sql = String::from_utf8_lossy(body).into_owned();
        if crate::log_enabled() {
            eprintln!("[mysql {} prepare] {sql}", self.session.id);
        }
        let (stmt, nparams) = match parse_one(&sql, Dialect::MySql) {
            Ok(x) => x,
            Err(e) => return self.err(&e),
        };
        let columns = match describe(&mut self.session, &stmt, nparams) {
            Ok(c) => c,
            Err(e) => return self.err(&e),
        };
        let id = self.next_stmt;
        self.next_stmt = self.next_stmt.wrapping_add(1).max(1);
        let mut p = vec![0u8];
        p.extend_from_slice(&id.to_le_bytes());
        p.extend_from_slice(&(columns.len() as u16).to_le_bytes());
        p.extend_from_slice(&(nparams as u16).to_le_bytes());
        p.push(0);
        p.extend_from_slice(&0u16.to_le_bytes());
        self.write_packet(&p)?;
        if nparams > 0 {
            for i in 0..nparams {
                let c = OutCol {
                    name: "?".into(),
                    ty: DataType::Text,
                    table: None,
                };
                let _ = i;
                self.write_packet(&column_def(&c, ""))?;
            }
            self.mid_eof()?;
        }
        if !columns.is_empty() {
            for c in &columns {
                self.write_packet(&column_def(c, &self.session.database.clone()))?;
            }
            self.mid_eof()?;
        }
        self.stmts.insert(
            id,
            Prepared {
                stmt,
                nparams,
                columns,
                types: Vec::new(),
                long_data: HashMap::new(),
            },
        );
        Ok(())
    }

    fn execute(&mut self, body: &[u8]) -> io::Result<()> {
        let mut rd = Rd { b: body, pos: 0 };
        let id = rd.u32()?;
        let _flags = rd.u8()?;
        let _iterations = rd.u32()?;
        let Some(st) = self.stmts.get_mut(&id) else {
            return self.err(&SqlError::new(
                "HY000",
                format!("unknown prepared statement handler ({id})"),
            ));
        };
        let n = st.nparams;
        let mut params = Vec::with_capacity(n);
        if n > 0 {
            let bitmap = rd.take(n.div_ceil(8))?.to_vec();
            let bound = rd.u8()?;
            if bound == 1 {
                st.types.clear();
                for _ in 0..n {
                    let t = rd.u8()?;
                    let flags = rd.u8()?;
                    st.types.push((t, flags & 0x80 != 0));
                }
            }
            for i in 0..n {
                if bitmap[i / 8] & (1 << (i % 8)) != 0 {
                    params.push(Datum::Null);
                    continue;
                }
                if let Some(data) = st.long_data.remove(&i) {
                    params
                        .push(Datum::Text(String::from_utf8(data.clone()).unwrap_or_else(
                            |_| String::from_utf8_lossy(&data).into_owned(),
                        )));
                    continue;
                }
                let (t, unsigned) = st.types.get(i).copied().unwrap_or((T_VAR_STRING, false));
                params.push(read_binary_param(&mut rd, t, unsigned)?);
            }
        }
        let stmt = st.stmt.clone();
        let described = st.columns.clone();
        match self.session.execute(&stmt, &params) {
            Ok(Outcome::Rows(rs)) => {
                // Column types come from the actual result (they may refine the prepared ones).
                let _ = described;
                let cols = rs.columns.clone();
                let mut p = Vec::new();
                lenenc(&mut p, cols.len() as u64);
                self.write_packet(&p)?;
                for c in &cols {
                    self.write_packet(&column_def(c, &self.session.database.clone()))?;
                }
                self.mid_eof()?;
                for row in &rs.rows {
                    let p = binary_row(row, &cols);
                    self.write_packet(&p)?;
                }
                self.eof(false)
            }
            Ok(other) => {
                let (a, id, info) = self.affected(&other);
                self.ok(a, id, false, &info)
            }
            Err(e) => self.err(&e),
        }
    }
}

/// `SHA1(pw) XOR SHA1(nonce + SHA1(SHA1(pw)))` (an empty password sends an empty token).
fn native_check(password: &str, nonce: &[u8], token: &[u8]) -> bool {
    if password.is_empty() {
        return token.is_empty();
    }
    if token.len() != 20 {
        return false;
    }
    let h1 = hash::sha1(password.as_bytes());
    let h2 = hash::sha1(&h1);
    let mut buf = nonce.to_vec();
    buf.extend_from_slice(&h2);
    let h3 = hash::sha1(&buf);
    let expect: Vec<u8> = h1.iter().zip(h3.iter()).map(|(a, b)| a ^ b).collect();
    hash::ct_eq(&expect, token)
}

/// MySQL wire type, flags, length, decimals and charset of a column type.
fn wire_type(ty: &DataType) -> (u8, u16, u32, u8, u16) {
    const BIN: u16 = 63;
    const UTF8: u16 = 255;
    match ty {
        DataType::Bool => (T_TINY, 0, 1, 0, BIN),
        DataType::SmallInt => (T_SHORT, 0, 6, 0, BIN),
        DataType::Int => (T_LONG, 0, 11, 0, BIN),
        DataType::BigInt => (T_LONGLONG, 0, 20, 0, BIN),
        DataType::Numeric(spec) => {
            let (p, s) = spec.unwrap_or((65, 30));
            (T_NEWDECIMAL, 0, u32::from(p) + 2, s.min(30), BIN)
        }
        DataType::Real => (T_FLOAT, 0, 12, 31, BIN),
        DataType::Double => (T_DOUBLE, 0, 22, 31, BIN),
        DataType::Bytes => (T_BLOB, 128 | 16, 65_535, 0, BIN),
        DataType::Timestamp | DataType::TimestampTz => (T_DATETIME, 128, 26, 6, BIN),
        DataType::Date => (T_DATE, 128, 10, 0, BIN),
        DataType::Time => (T_TIME, 128, 17, 6, BIN),
        DataType::Json | DataType::Jsonb | DataType::Array(_) => {
            (T_JSON, 16 | 128, 4_294_967_295, 0, BIN)
        }
        DataType::Varchar(n) | DataType::Char(n) => {
            (T_VAR_STRING, 0, n.unwrap_or(255).saturating_mul(4), 0, UTF8)
        }
        _ => (T_BLOB, 16, 4_294_967_295, 0, UTF8),
    }
}

fn column_def(c: &OutCol, db: &str) -> Vec<u8> {
    let (t, flags, len, decimals, charset) = wire_type(&c.ty);
    let mut p = Vec::new();
    lenenc_str(&mut p, b"def");
    lenenc_str(
        &mut p,
        if c.table.is_some() {
            db.as_bytes()
        } else {
            b""
        },
    );
    let table = c.table.as_deref().unwrap_or("");
    lenenc_str(&mut p, table.as_bytes());
    lenenc_str(&mut p, table.as_bytes());
    lenenc_str(&mut p, c.name.as_bytes());
    lenenc_str(&mut p, c.name.as_bytes());
    p.push(0x0c);
    p.extend_from_slice(&charset.to_le_bytes());
    p.extend_from_slice(&len.to_le_bytes());
    p.push(t);
    p.extend_from_slice(&flags.to_le_bytes());
    p.push(decimals);
    p.extend_from_slice(&[0, 0]);
    p
}

/// Text-protocol value: MySQL formats (`1`/`0` booleans, `YYYY-MM-DD HH:MM:SS.ffffff`, raw bytes).
fn text_value(d: &Datum, ty: &DataType) -> Vec<u8> {
    match (d, ty) {
        (Datum::Timestamp(ns), _) => mysql_datetime(*ns).into_bytes(),
        (Datum::Array(_), _) => datum::json_of(d).into_bytes(),
        (Datum::Bool(b), _) => {
            if *b {
                b"1".to_vec()
            } else {
                b"0".to_vec()
            }
        }
        (other, _) => other.to_wire_bytes(true),
    }
}

fn mysql_datetime(ns: i64) -> String {
    let s = datum::format_timestamp(ns - ns.rem_euclid(1000), false);
    // MySQL prints a fixed six-digit fraction only when there is one.
    match s.split_once('.') {
        Some((a, f)) => format!("{a}.{f:0<6}"),
        None => s,
    }
}

fn binary_row(row: &[Datum], cols: &[OutCol]) -> Vec<u8> {
    let mut p = vec![0u8];
    let bitmap_len = (cols.len() + 7 + 2) / 8;
    let start = p.len();
    p.resize(start + bitmap_len, 0);
    for (i, d) in row.iter().enumerate() {
        if d.is_null() {
            let bit = i + 2;
            p[start + bit / 8] |= 1 << (bit % 8);
        }
    }
    for (d, c) in row.iter().zip(cols) {
        if d.is_null() {
            continue;
        }
        let (t, ..) = wire_type(&c.ty);
        match t {
            T_TINY => p.push(
                d.as_i64()
                    .unwrap_or(i64::from(d.truthy().ok().flatten().unwrap_or(false)))
                    as u8,
            ),
            T_SHORT => p.extend_from_slice(&(d.as_i64().unwrap_or(0) as i16).to_le_bytes()),
            T_LONG => p.extend_from_slice(&(d.as_i64().unwrap_or(0) as i32).to_le_bytes()),
            T_LONGLONG => p.extend_from_slice(&d.as_i64().unwrap_or(0).to_le_bytes()),
            T_FLOAT => p.extend_from_slice(&(datum::to_f64(d).unwrap_or(0.0) as f32).to_le_bytes()),
            T_DOUBLE => p.extend_from_slice(&datum::to_f64(d).unwrap_or(0.0).to_le_bytes()),
            T_DATETIME | T_DATE => {
                let ns = match datum::cast(d, &DataType::Timestamp) {
                    Ok(Datum::Timestamp(ns)) => ns,
                    _ => 0,
                };
                let day = ns.div_euclid(86_400_000_000_000);
                let rem = ns.rem_euclid(86_400_000_000_000);
                let (y, m, dd) = datum::civil_from_days(day as i32);
                let (h, mi, s) = (
                    rem / 3_600_000_000_000,
                    (rem / 60_000_000_000) % 60,
                    (rem / 1_000_000_000) % 60,
                );
                let us = (rem % 1_000_000_000) / 1000;
                if t == T_DATE {
                    p.push(4);
                    p.extend_from_slice(&(y as u16).to_le_bytes());
                    p.extend_from_slice(&[m as u8, dd as u8]);
                } else {
                    p.push(11);
                    p.extend_from_slice(&(y as u16).to_le_bytes());
                    p.extend_from_slice(&[m as u8, dd as u8, h as u8, mi as u8, s as u8]);
                    p.extend_from_slice(&(us as u32).to_le_bytes());
                }
            }
            T_TIME => {
                let ns = match d {
                    Datum::Time(ns) => *ns,
                    _ => 0,
                };
                let secs = ns / 1_000_000_000;
                p.push(12);
                p.push(0);
                p.extend_from_slice(&((secs / 86_400) as u32).to_le_bytes());
                p.extend_from_slice(&[
                    ((secs / 3600) % 24) as u8,
                    ((secs / 60) % 60) as u8,
                    (secs % 60) as u8,
                ]);
                p.extend_from_slice(&(((ns % 1_000_000_000) / 1000) as u32).to_le_bytes());
            }
            _ => lenenc_str(&mut p, &text_value(d, &c.ty)),
        }
    }
    p
}

fn read_binary_param(rd: &mut Rd<'_>, t: u8, unsigned: bool) -> io::Result<Datum> {
    Ok(match t {
        T_NULL => Datum::Null,
        T_TINY => {
            let v = rd.u8()?;
            Datum::Int(if unsigned {
                i64::from(v)
            } else {
                i64::from(v as i8)
            })
        }
        T_SHORT | T_YEAR => {
            let v = rd.u16()?;
            Datum::Int(if unsigned {
                i64::from(v)
            } else {
                i64::from(v as i16)
            })
        }
        T_LONG | T_INT24 => {
            let v = rd.u32()?;
            Datum::Int(if unsigned {
                i64::from(v)
            } else {
                i64::from(v as i32)
            })
        }
        T_LONGLONG => {
            let v = rd.u64()?;
            if unsigned && v > i64::MAX as u64 {
                Datum::Numeric(storage_types::Decimal::from_i128(i128::from(v)))
            } else {
                Datum::Int(v as i64)
            }
        }
        T_FLOAT => {
            let b = rd.take(4)?;
            Datum::Float(f64::from(f32::from_le_bytes([b[0], b[1], b[2], b[3]])))
        }
        T_DOUBLE => Datum::Float(f64::from_bits(rd.u64()?)),
        T_DATE | T_DATETIME | T_TIMESTAMP => {
            let n = rd.u8()? as usize;
            let b = rd.take(n)?;
            let g = |i: usize| b.get(i).copied().unwrap_or(0);
            if n == 0 {
                return Ok(Datum::Timestamp(0));
            }
            let y = i32::from(u16::from_le_bytes([g(0), g(1)]));
            let days = datum::days_from_civil(y, u32::from(g(2)).max(1), u32::from(g(3)).max(1));
            let mut ns = i64::from(days) * 86_400_000_000_000;
            if n >= 7 {
                ns += (i64::from(g(4)) * 3600 + i64::from(g(5)) * 60 + i64::from(g(6)))
                    * 1_000_000_000;
            }
            if n >= 11 {
                ns += i64::from(u32::from_le_bytes([g(7), g(8), g(9), g(10)])) * 1000;
            }
            if t == T_DATE {
                Datum::Date(days)
            } else {
                Datum::Timestamp(ns)
            }
        }
        T_TIME => {
            let n = rd.u8()? as usize;
            let b = rd.take(n)?;
            let g = |i: usize| b.get(i).copied().unwrap_or(0);
            if n == 0 {
                return Ok(Datum::Time(0));
            }
            let days = i64::from(u32::from_le_bytes([g(1), g(2), g(3), g(4)]));
            let mut ns =
                ((days * 24 + i64::from(g(5))) * 3600 + i64::from(g(6)) * 60 + i64::from(g(7)))
                    * 1_000_000_000;
            if n >= 12 {
                ns += i64::from(u32::from_le_bytes([g(8), g(9), g(10), g(11)])) * 1000;
            }
            Datum::Time(if g(0) == 1 { -ns } else { ns })
        }
        T_DECIMAL | T_JSON | T_VARCHAR | T_BIT | T_BLOB | T_VAR_STRING | T_STRING | 0xf7 | 0xf8
        | 0xf9 | 0xfa | 0xfb => {
            let bytes = rd.lenenc_bytes()?;
            match String::from_utf8(bytes.to_vec()) {
                Ok(s) if t == T_DECIMAL => datum::parse_dec(&s)
                    .map(Datum::Numeric)
                    .unwrap_or(Datum::Text(s)),
                Ok(s) => Datum::Text(s),
                Err(_) => Datum::Bytes(bytes.to_vec()),
            }
        }
        other => return Err(bad(&format!("unsupported parameter type {other}"))),
    })
}

/// Result columns of a prepared statement (without running writes).
fn describe(s: &mut Session, stmt: &Statement, nparams: usize) -> SqlResult<Vec<OutCol>> {
    let nulls = vec![Datum::Null; nparams];
    match stmt {
        Statement::Query(q) => {
            let mut q = (**q).clone();
            q.lock = None;
            match s.execute(&Statement::Query(Box::new(q)), &nulls)? {
                Outcome::Rows(rs) => Ok(rs.columns),
                _ => Ok(Vec::new()),
            }
        }
        Statement::Show(_) => match s.execute(stmt, &nulls)? {
            Outcome::Rows(rs) => Ok(rs.columns),
            _ => Ok(Vec::new()),
        },
        _ => Ok(Vec::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_password_scramble() {
        let nonce = b"abcdefghijklmnopqrst";
        let h1 = hash::sha1(b"secret");
        let h2 = hash::sha1(&h1);
        let mut buf = nonce.to_vec();
        buf.extend_from_slice(&h2);
        let token: Vec<u8> = h1
            .iter()
            .zip(hash::sha1(&buf).iter())
            .map(|(a, b)| a ^ b)
            .collect();
        assert!(native_check("secret", nonce, &token));
        assert!(!native_check("wrong", nonce, &token));
        assert!(native_check("", nonce, &[]));
    }

    #[test]
    fn lenenc_encoding() {
        for n in [0u64, 250, 251, 65_535, 65_536, 16_777_216, u64::MAX] {
            let mut v = Vec::new();
            lenenc(&mut v, n);
            assert_eq!(Rd { b: &v, pos: 0 }.lenenc().unwrap(), n);
        }
    }
}
