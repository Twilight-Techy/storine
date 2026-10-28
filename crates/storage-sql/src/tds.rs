//! Microsoft SQL Server's wire protocol (TDS 7.1 - 7.4) over the same engine as the
//! PostgreSQL and MySQL endpoints.
//!
//! Messages are TDS packets (8-byte header, split at the negotiated packet size). A connection
//! goes PRELOGIN (version and encryption negotiation) -> LOGIN7 -> requests: SQL batches, RPC
//! calls (`sp_executesql`, `sp_prepare` / `sp_execute` / `sp_prepexec` / `sp_unprepare`) and
//! transaction-manager requests. Results are token streams: COLMETADATA / ROW / DONE, ERROR,
//! ENVCHANGE (database, packet size, transaction begin / commit / rollback), RETURNVALUE.

use std::collections::HashMap;
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use storage_types::Decimal;

use crate::ast::{DataType, Statement};
use crate::datum::{self, Datum};
use crate::error::SqlError;
use crate::exec::{Engine, OutCol, Outcome, Session};
use crate::lexer::Dialect;
use crate::parser::parse;

/// Connection settings shared by every TDS connection.
#[derive(Debug)]
pub struct Auth {
    pub users: HashMap<String, String>,
    pub tls: Option<Arc<crate::tls::TlsConfig>>,
    pub require_tls: bool,
}

/// Accepts SQL Server clients on `listener` forever (one thread per connection).
pub fn serve(engine: Arc<Engine>, listener: TcpListener, auth: Arc<Auth>) {
    for conn in listener.incoming() {
        let Ok(stream) = conn else { continue };
        let engine = Arc::clone(&engine);
        let auth = Arc::clone(&auth);
        thread::spawn(move || {
            let _ = stream.set_nodelay(true);
            if let Err(e) = Conn::run(engine, stream, &auth) {
                if !matches!(
                    e.kind(),
                    io::ErrorKind::UnexpectedEof | io::ErrorKind::ConnectionReset
                ) {
                    eprintln!("storine-sql: tds connection ended: {e}");
                }
            }
        });
    }
}

// Packet types.
const PT_SQL_BATCH: u8 = 0x01;
const PT_RPC: u8 = 0x03;
const PT_REPLY: u8 = 0x04;
const PT_ATTENTION: u8 = 0x06;
const PT_TRANSACTION: u8 = 0x0e;
const PT_LOGIN7: u8 = 0x10;
const PT_PRELOGIN: u8 = 0x12;

// Tokens.
const TK_RETURNSTATUS: u8 = 0x79;
const TK_COLMETADATA: u8 = 0x81;
const TK_ERROR: u8 = 0xaa;
const TK_RETURNVALUE: u8 = 0xac;
const TK_LOGINACK: u8 = 0xad;
const TK_ROW: u8 = 0xd1;
const TK_ENVCHANGE: u8 = 0xe3;
const TK_DONE: u8 = 0xfd;
const TK_DONEPROC: u8 = 0xfe;
const TK_DONEINPROC: u8 = 0xff;

const DONE_MORE: u16 = 0x01;
const DONE_ERROR: u16 = 0x02;
const DONE_COUNT: u16 = 0x10;
const DONE_ATTN: u16 = 0x20;

// TDS versions.
const TDS72: u32 = 0x7209_0002;
const TDS73: u32 = 0x730a_0003;
const TDS74: u32 = 0x7400_0004;

/// Latin1_General_CI_AS (LCID 1033), the collation reported for every text value.
const COLLATION: [u8; 5] = [0x09, 0x04, 0xd0, 0x00, 0x34];

/// Largest TDS message accepted from a client.
const MAX_MESSAGE: usize = 256 * 1024 * 1024;

/// Days between 0001-01-01 and 1970-01-01.
const DAYS_0001_TO_1970: i64 = 719_162;
/// Days between 1900-01-01 and 1970-01-01.
const DAYS_1900_TO_1970: i64 = 25_567;

fn utf16(s: &str) -> Vec<u8> {
    s.encode_utf16().flat_map(u16::to_le_bytes).collect()
}

fn from_utf16(b: &[u8]) -> String {
    let units: Vec<u16> = b
        .chunks_exact(2)
        .map(|c| u16::from_le_bytes([c[0], c[1]]))
        .collect();
    String::from_utf16_lossy(&units)
}

/// A token stream being built.
#[derive(Default)]
pub(crate) struct Tokens {
    pub(crate) b: Vec<u8>,
    /// Payload bytes per packet of the response this buffer becomes (0: unknown).
    room: usize,
}

impl Tokens {
    fn u8(&mut self, v: u8) -> &mut Self {
        self.b.push(v);
        self
    }
    fn u16(&mut self, v: u16) -> &mut Self {
        self.b.extend_from_slice(&v.to_le_bytes());
        self
    }
    fn u32(&mut self, v: u32) -> &mut Self {
        self.b.extend_from_slice(&v.to_le_bytes());
        self
    }
    fn u64(&mut self, v: u64) -> &mut Self {
        self.b.extend_from_slice(&v.to_le_bytes());
        self
    }
    fn bytes(&mut self, v: &[u8]) -> &mut Self {
        self.b.extend_from_slice(v);
        self
    }
    /// B_VARCHAR: 1-byte character count, UTF-16LE.
    fn b_varchar(&mut self, s: &str) -> &mut Self {
        let u = utf16(s);
        let chars = (u.len() / 2).min(255);
        self.u8(chars as u8).bytes(&u[..chars * 2])
    }
    /// US_VARCHAR: 2-byte character count, UTF-16LE.
    fn us_varchar(&mut self, s: &str) -> &mut Self {
        let u = utf16(s);
        let chars = (u.len() / 2).min(65_535);
        self.u16(chars as u16).bytes(&u[..chars * 2])
    }
    /// Appends a token whose body is prefixed by its 2-byte length.
    fn with_len(&mut self, token: u8, body: &Tokens) -> &mut Self {
        self.u8(token).u16(body.b.len() as u16).bytes(&body.b)
    }
}

/// The SQL Server error number for a SQLSTATE.
fn error_number(code: &str) -> i32 {
    match code {
        "23505" => 2627,           // Violation of PRIMARY KEY / UNIQUE KEY constraint
        "23503" | "23514" => 547,  // conflicted with a FOREIGN KEY / CHECK constraint
        "23502" => 515,            // Cannot insert the value NULL
        "42P01" => 208,            // Invalid object name
        "42703" => 207,            // Invalid column name
        "42601" => 102,            // Incorrect syntax
        "42P07" => 2714,           // There is already an object named
        "40001" | "40P01" => 1205, // deadlock victim: clients retry
        "55P03" => 1222,           // Lock request time out period exceeded
        "22012" => 8134,           // Divide by zero error encountered
        "22003" => 8115,           // Arithmetic overflow
        "28P01" | "28000" => 18456,
        "3D000" => 4060,
        "25P02" => 3998,
        _ => 50000,
    }
}

/// SQL Server's wording for the constraint errors ORMs recognise by message (Sequelize,
/// EF Core, ActiveRecord); other messages pass through.
fn tsql_message(e: &SqlError, db: &str) -> String {
    let quoted: Vec<&str> = e.message.split('"').skip(1).step_by(2).collect();
    let q = |i: usize| quoted.get(i).copied().unwrap_or("");
    match e.code {
        "23505" if e.message.starts_with("duplicate key") => {
            let kind = if q(0).starts_with("PK_") || q(0).ends_with("_pkey") {
                "PRIMARY"
            } else {
                "UNIQUE"
            };
            format!(
                "Violation of {kind} KEY constraint '{}'. Cannot insert duplicate key in object 'dbo.{}'.",
                q(0),
                q(1)
            )
        }
        "23503" if e.message.starts_with("insert or update") => format!(
            "The INSERT statement conflicted with the FOREIGN KEY constraint \"{}\". The conflict occurred in database \"{db}\", table \"dbo.{}\".",
            q(1),
            if quoted.len() > 2 { q(2) } else { q(0) }
        ),
        "23503" if e.message.starts_with("update or delete") => format!(
            "The DELETE statement conflicted with the REFERENCE constraint \"{}\". The conflict occurred in database \"{db}\", table \"dbo.{}\".",
            q(1),
            q(2)
        ),
        "23502" if e.message.starts_with("null value") => format!(
            "Cannot insert the value NULL into column '{}', table '{db}.dbo.{}'; column does not allow nulls. INSERT fails.",
            q(0),
            q(1)
        ),
        "23514" if e.message.starts_with("new row") => format!(
            "The INSERT statement conflicted with the CHECK constraint \"{}\". The conflict occurred in database \"{db}\", table \"dbo.{}\".",
            q(1),
            q(0)
        ),
        _ => e.message.clone(),
    }
}

/// How a result column is sent (chosen from the column's type and, for widths and scales,
/// from the values actually being sent).
#[derive(Debug, Clone)]
enum Wire {
    Bit,
    Int(u8),
    Float,
    Decimal {
        precision: u8,
        scale: u8,
    },
    /// NVARCHAR(n) (`max_bytes` <= 8000) or NVARCHAR(MAX) (`None`).
    NVarChar(Option<u16>),
    VarBinary(Option<u16>),
    Guid,
    Date,
    Time,
    DateTime2,
    DateTimeOffset,
    /// Legacy DATETIME (TDS 7.1 / 7.2 clients).
    DateTime,
}

fn wire_type(ty: &DataType, values: &[&Datum], version: u32) -> Wire {
    let modern = version >= TDS73;
    let text_bytes = || {
        values
            .iter()
            .map(|d| {
                if d.is_null() {
                    0
                } else {
                    d.as_text().encode_utf16().count() * 2
                }
            })
            .max()
            .unwrap_or(0)
    };
    let nvarchar = |declared: Option<u32>| {
        let need = text_bytes();
        let want = declared.map_or(0, |n| (n as usize) * 2).max(need).max(2);
        if want <= 8000 {
            Wire::NVarChar(Some(want as u16))
        } else if version >= TDS72 {
            Wire::NVarChar(None)
        } else {
            Wire::NVarChar(Some(8000))
        }
    };
    match ty {
        DataType::Bool => Wire::Bit,
        DataType::SmallInt => Wire::Int(2),
        DataType::Int => Wire::Int(4),
        DataType::BigInt => Wire::Int(8),
        DataType::Real | DataType::Double => Wire::Float,
        DataType::Numeric(Some((p, s))) => Wire::Decimal {
            precision: (*p).clamp(1, 38),
            scale: *s,
        },
        DataType::Numeric(None) => {
            let scale = values
                .iter()
                .filter_map(|d| match d {
                    Datum::Numeric(x) => Some(x.scale()),
                    _ => None,
                })
                .max()
                .unwrap_or(0)
                .min(38);
            Wire::Decimal {
                precision: 38,
                scale,
            }
        }
        DataType::Varchar(n) | DataType::Char(n) => nvarchar(*n),
        DataType::Bytes => {
            let need = values
                .iter()
                .map(|d| if let Datum::Bytes(b) = d { b.len() } else { 0 })
                .max()
                .unwrap_or(0);
            if need <= 8000 {
                Wire::VarBinary(Some(need.max(1) as u16))
            } else {
                Wire::VarBinary(None)
            }
        }
        DataType::Uuid => Wire::Guid,
        DataType::Date if modern => Wire::Date,
        DataType::Time if modern => Wire::Time,
        DataType::Timestamp if modern => Wire::DateTime2,
        DataType::TimestampTz if modern => Wire::DateTimeOffset,
        DataType::Date | DataType::Timestamp | DataType::TimestampTz => Wire::DateTime,
        _ => nvarchar(None),
    }
}

impl Wire {
    fn type_info(&self, t: &mut Tokens) {
        match self {
            Wire::Bit => t.u8(0x68).u8(1),
            Wire::Int(n) => t.u8(0x26).u8(*n),
            Wire::Float => t.u8(0x6d).u8(8),
            Wire::Decimal { precision, scale } => t
                .u8(0x6a)
                .u8(decimal_len(*precision))
                .u8(*precision)
                .u8(*scale),
            Wire::NVarChar(n) => t.u8(0xe7).u16(n.unwrap_or(0xffff)).bytes(&COLLATION),
            Wire::VarBinary(n) => t.u8(0xa5).u16(n.unwrap_or(0xffff)),
            Wire::Guid => t.u8(0x24).u8(16),
            Wire::Date => t.u8(0x28),
            Wire::Time => t.u8(0x29).u8(7),
            Wire::DateTime2 => t.u8(0x2a).u8(7),
            Wire::DateTimeOffset => t.u8(0x2b).u8(7),
            Wire::DateTime => t.u8(0x6f).u8(8),
        };
    }

    fn value(&self, d: &Datum, t: &mut Tokens) -> Result<(), SqlError> {
        let plp = matches!(self, Wire::NVarChar(None) | Wire::VarBinary(None));
        if d.is_null() {
            match self {
                Wire::NVarChar(Some(_)) | Wire::VarBinary(Some(_)) => t.u16(0xffff),
                _ if plp => t.u64(u64::MAX),
                _ => t.u8(0),
            };
            return Ok(());
        }
        match self {
            Wire::Bit => {
                t.u8(1)
                    .u8(u8::from(d.truthy().ok().flatten().unwrap_or(false)));
            }
            Wire::Int(n) => {
                let v = d.as_i64()?;
                t.u8(*n).bytes(&v.to_le_bytes()[..usize::from(*n)]);
            }
            Wire::Float => {
                let f = datum::to_f64(d).ok_or_else(|| SqlError::new("22003", "not a number"))?;
                t.u8(8).bytes(&f.to_le_bytes());
            }
            Wire::Decimal { precision, scale } => {
                let x = datum::to_decimal(d)
                    .ok_or_else(|| SqlError::new("22003", "not a number"))?
                    .rescale(*scale, storage_types::RoundingMode::HalfEven)
                    .map_err(|e| SqlError::new("22003", e.to_string()))?;
                let m = x.mantissa();
                let len = decimal_len(*precision);
                let mag = m.unsigned_abs();
                if usize::from(len - 1) < 16 && mag >> (8 * u32::from(len - 1)) != 0 {
                    return Err(SqlError::new(
                        "22003",
                        format!("{x} does not fit in DECIMAL({precision},{scale})"),
                    ));
                }
                t.u8(len)
                    .u8(u8::from(m >= 0))
                    .bytes(&mag.to_le_bytes()[..usize::from(len - 1)]);
            }
            Wire::NVarChar(Some(_)) => {
                let u = utf16(&text_of(d));
                t.u16(u.len() as u16).bytes(&u);
            }
            Wire::VarBinary(Some(_)) => {
                let b = bytes_of(d);
                t.u16(b.len() as u16).bytes(&b);
            }
            Wire::NVarChar(None) | Wire::VarBinary(None) => {
                let data = if matches!(self, Wire::NVarChar(_)) {
                    utf16(&text_of(d))
                } else {
                    bytes_of(d)
                };
                t.u64(data.len() as u64);
                let text = matches!(self, Wire::NVarChar(_));
                let mut rest = &data[..];
                while !rest.is_empty() {
                    let mut n = rest.len().min(8000);
                    let start = t.b.len() + 4;
                    if text && t.room > 0 && start % 2 == 1 {
                        // Characters would straddle packet boundaries, which SqlClient's PLP
                        // reader mishandles: end the chunk one byte before the packet does, so
                        // only the next chunk header straddles.
                        let mut fit = t.room - start % t.room - 1;
                        if fit == 0 {
                            fit = t.room;
                        }
                        n = n.min(fit);
                    }
                    t.u32(n as u32).bytes(&rest[..n]);
                    rest = &rest[n..];
                }
                t.u32(0);
            }
            Wire::Guid => {
                let Datum::Uuid(u) = datum::cast(d, &DataType::Uuid)? else {
                    return Err(SqlError::new("22P02", "not a uniqueidentifier"));
                };
                t.u8(16).bytes(&guid_bytes(&u));
            }
            Wire::Date => {
                let days = date_days(d)?;
                t.u8(3)
                    .bytes(&((days + DAYS_0001_TO_1970) as u32).to_le_bytes()[..3]);
            }
            Wire::Time => {
                let ns = match datum::cast(d, &DataType::Time)? {
                    Datum::Time(ns) => ns,
                    _ => 0,
                };
                t.u8(5).bytes(&((ns / 100) as u64).to_le_bytes()[..5]);
            }
            Wire::DateTime2 | Wire::DateTimeOffset => {
                let ns = timestamp_ns(d)?;
                let days = ns.div_euclid(86_400_000_000_000);
                let tod = ns.rem_euclid(86_400_000_000_000);
                let offset = matches!(self, Wire::DateTimeOffset);
                t.u8(if offset { 10 } else { 8 });
                t.bytes(&((tod / 100) as u64).to_le_bytes()[..5]);
                t.bytes(&((days + DAYS_0001_TO_1970) as u32).to_le_bytes()[..3]);
                if offset {
                    t.u16(0);
                }
            }
            Wire::DateTime => {
                let ns = timestamp_ns(d)?;
                let days = ns.div_euclid(86_400_000_000_000);
                let tod = ns.rem_euclid(86_400_000_000_000);
                let ticks = ((i128::from(tod) * 300 + 500_000_000) / 1_000_000_000) as u32; // 1/300 s
                t.u8(8)
                    .bytes(&((days + DAYS_1900_TO_1970) as i32).to_le_bytes())
                    .u32(ticks);
            }
        }
        Ok(())
    }
}

/// Bytes of a DECIMAL value of `precision` digits (sign byte included).
fn decimal_len(precision: u8) -> u8 {
    match precision {
        0..=9 => 5,
        10..=19 => 9,
        20..=28 => 13,
        _ => 17,
    }
}

fn text_of(d: &Datum) -> String {
    match d {
        Datum::Bool(b) => if *b { "1" } else { "0" }.into(),
        other => other.as_text(),
    }
}

fn bytes_of(d: &Datum) -> Vec<u8> {
    match d {
        Datum::Bytes(b) => b.clone(),
        other => other.as_text().into_bytes(),
    }
}

fn date_days(d: &Datum) -> Result<i64, SqlError> {
    Ok(match datum::cast(d, &DataType::Date)? {
        Datum::Date(days) => i64::from(days),
        _ => 0,
    })
}

fn timestamp_ns(d: &Datum) -> Result<i64, SqlError> {
    Ok(match d {
        Datum::Date(days) => i64::from(*days) * 86_400_000_000_000,
        other => match datum::cast(other, &DataType::Timestamp)? {
            Datum::Timestamp(ns) => ns,
            _ => 0,
        },
    })
}

/// SQL Server's GUID byte order: the first three groups little-endian.
fn guid_bytes(u: &[u8; 16]) -> [u8; 16] {
    let mut g = *u;
    g[0..4].reverse();
    g[4..6].reverse();
    g[6..8].reverse();
    g
}

// ── connection ──

// PRELOGIN encryption values.
const ENCRYPT_OFF: u8 = 0; // login only
const ENCRYPT_ON: u8 = 1;
const ENCRYPT_NOT_SUP: u8 = 2;
const ENCRYPT_REQ: u8 = 3;

/// The socket under TLS. During a TDS 7.x handshake (`framed`), TLS records travel inside
/// PRELOGIN packets; afterwards, and in TDS 8 strict mode, they go straight on the socket.
struct SocketReader {
    inner: BufReader<TcpStream>,
    framed: Arc<AtomicBool>,
    /// Payload bytes left in the current PRELOGIN packet.
    left: usize,
}

impl Read for SocketReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.left == 0
            && self.framed.load(Ordering::Relaxed)
            && matches!(self.inner.fill_buf()?.first(), Some(0x14..=0x17))
        {
            // A bare TLS record where a PRELOGIN packet was expected: the client has
            // finished its handshake (see `tls::accept_tds7`).
            self.framed.store(false, Ordering::Relaxed);
        }
        if !self.framed.load(Ordering::Relaxed) {
            return self.inner.read(buf);
        }
        while self.left == 0 {
            let mut h = [0u8; 8];
            self.inner.read_exact(&mut h)?;
            self.left = usize::from(u16::from_be_bytes([h[2], h[3]])).saturating_sub(8);
        }
        let n = buf.len().min(self.left);
        let n = self.inner.read(&mut buf[..n])?;
        self.left -= n;
        Ok(n)
    }
}

struct SocketWriter {
    inner: TcpStream,
    framed: Arc<AtomicBool>,
    /// The handshake flight being collected into PRELOGIN packets.
    flight: Vec<u8>,
}

impl Write for SocketWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.framed.load(Ordering::Relaxed) {
            self.flight.extend_from_slice(buf);
            return Ok(buf.len());
        }
        self.inner.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        if !self.flight.is_empty() {
            let chunks: Vec<&[u8]> = self.flight.chunks(4096 - 8).collect();
            let last = chunks.len() - 1;
            let mut out = Vec::with_capacity(self.flight.len() + 8 * chunks.len());
            for (i, c) in chunks.iter().enumerate() {
                let len = (c.len() + 8) as u16;
                out.extend_from_slice(&[PT_PRELOGIN, u8::from(i == last)]);
                out.extend_from_slice(&len.to_be_bytes());
                out.extend_from_slice(&[0, 0, (i + 1) as u8, 0]);
                out.extend_from_slice(c);
            }
            self.flight.clear();
            self.inner.write_all(&out)?;
        }
        self.inner.flush()
    }
}

enum Input {
    Plain(BufReader<TcpStream>),
    Tls(crate::tls::TlsReader<SocketReader>),
    Switching,
}

impl Read for Input {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Input::Plain(r) => r.read(buf),
            Input::Tls(r) => r.read(buf),
            Input::Switching => Err(io::Error::other("connection is switching to TLS")),
        }
    }
}

enum Output {
    Plain(BufWriter<TcpStream>),
    Tls(BufWriter<crate::tls::TlsWriter<SocketWriter>>),
    Switching,
}

impl Write for Output {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Output::Plain(w) => w.write(buf),
            Output::Tls(w) => w.write(buf),
            Output::Switching => Err(io::Error::other("connection is switching to TLS")),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        match self {
            Output::Plain(w) => w.flush(),
            Output::Tls(w) => w.flush(),
            Output::Switching => Ok(()),
        }
    }
}

struct Conn {
    r: Input,
    w: Output,
    session: Session,
    version: u32,
    packet_size: usize,
    packet_id: u8,
    /// Transaction descriptor of the open transaction (0: none), reported in ENVCHANGE tokens.
    txn: u64,
    next_txn: u64,
    prepared: HashMap<i32, Prepared>,
    next_handle: i32,
}

struct Prepared {
    stmts: Vec<Statement>,
    /// Parameter names in declaration order (`@p1`, `@name`, ...).
    names: Vec<String>,
}

impl Conn {
    fn run(engine: Arc<Engine>, stream: TcpStream, auth: &Auth) -> io::Result<()> {
        let session = engine.new_session(Dialect::MsSql, "", None);
        let mut c = Conn {
            r: Input::Plain(BufReader::new(stream.try_clone()?)),
            w: Output::Plain(BufWriter::new(stream)),
            session,
            version: TDS74,
            packet_size: 4096,
            packet_id: 1,
            txn: 0,
            next_txn: 1,
            prepared: HashMap::new(),
            next_handle: 1,
        };
        // TDS 8 (strict encryption): TLS first, with PRELOGIN inside it.
        let mut strict = false;
        if let (Some(cfg), Input::Plain(r)) = (&auth.tls, &mut c.r) {
            if r.fill_buf()?.first() == Some(&0x16) {
                c.start_tls(cfg, false)?;
                strict = true;
            }
        }
        // PRELOGIN.
        let (kind, pre) = c.read_message()?;
        if kind != PT_PRELOGIN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "expected PRELOGIN",
            ));
        }
        let client_encryption = prelogin_option(&pre, 0x01)
            .and_then(|v| v.first().copied())
            .unwrap_or(2);
        let encryption = match (&auth.tls, client_encryption) {
            _ if strict => ENCRYPT_NOT_SUP, // already encrypted
            (None, _) => ENCRYPT_NOT_SUP,
            (Some(_), ENCRYPT_NOT_SUP) if auth.require_tls => ENCRYPT_REQ,
            (Some(_), ENCRYPT_NOT_SUP) => ENCRYPT_NOT_SUP,
            (Some(_), ENCRYPT_OFF) if !auth.require_tls => ENCRYPT_OFF,
            (Some(_), _) => ENCRYPT_ON,
        };
        if crate::log_enabled() {
            eprintln!(
                "[tds {}] PRELOGIN encryption: client {client_encryption}, server {encryption}{}",
                c.session.id,
                if strict { " (TDS 8 strict)" } else { "" }
            );
        }
        c.send(PT_REPLY, &prelogin_reply(encryption))?;
        if encryption == ENCRYPT_REQ {
            return Ok(()); // the client cannot encrypt; SQL Server closes the connection too
        }
        if let (Some(cfg), ENCRYPT_ON | ENCRYPT_OFF) = (&auth.tls, encryption) {
            c.start_tls(cfg, true)?;
        }
        // LOGIN7.
        let (kind, login) = c.read_message()?;
        if encryption == ENCRYPT_OFF {
            c.stop_tls()?; // login-only encryption: the rest of the connection is plaintext
        }
        if kind != PT_LOGIN7 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "expected LOGIN7",
            ));
        }
        let l = match parse_login7(&login) {
            Ok(l) => l,
            Err(e) => {
                let mut t = Tokens::default();
                c.error_token(&mut t, &e, 1);
                c.done(&mut t, TK_DONE, DONE_ERROR, 0, 0);
                return c.send(PT_REPLY, &t.b);
            }
        };
        // TDS 8 (0x08000000) is TDS 7.4 inside TLS.
        c.version = if l.version == 0x0800_0000 {
            TDS74
        } else {
            l.version.min(TDS74)
        };
        c.packet_size = l.packet_size.clamp(512, 32_767);
        if !auth.users.is_empty() {
            let ok = auth
                .users
                .get(&l.user)
                .is_some_and(|pw| crate::hash::ct_eq(pw.as_bytes(), l.password.as_bytes()));
            if !ok {
                let mut t = Tokens::default();
                let e = SqlError::new("28000", format!("Login failed for user '{}'.", l.user));
                c.error_token(&mut t, &e, 1);
                c.done(&mut t, TK_DONE, DONE_ERROR, 0, 0);
                return c.send(PT_REPLY, &t.b);
            }
        }
        c.session.user = if l.user.is_empty() {
            "sa".into()
        } else {
            l.user.clone()
        };
        let mut t = Tokens::default();
        if !l.database.is_empty() {
            if let Err(e) = c.session.use_database(&l.database) {
                c.error_token(&mut t, &e, 1);
                c.done(&mut t, TK_DONE, DONE_ERROR, 0, 0);
                return c.send(PT_REPLY, &t.b);
            }
        }
        let db = c.session.database.clone();
        c.env_change(&mut t, 1, &db, "master");
        c.env_change(&mut t, 2, "us_english", "");
        let mut coll = Tokens::default();
        coll.u8(7).u8(5).bytes(&COLLATION).u8(0);
        t.with_len(TK_ENVCHANGE, &coll);
        let mut ack = Tokens::default();
        ack.u8(1)
            .bytes(&c.version.to_be_bytes())
            .b_varchar("Microsoft SQL Server")
            .bytes(&[16, 0, 0x03, 0xe8]);
        t.with_len(TK_LOGINACK, &ack);
        let ps = c.packet_size.to_string();
        c.env_change(&mut t, 4, &ps, "4096");
        c.done(&mut t, TK_DONE, 0, 0, 0);
        c.send(PT_REPLY, &t.b)?;
        c.requests()
    }

    /// Replaces the plaintext streams with a TLS session over the same socket; `framed` for the
    /// TDS 7.x handshake carried in PRELOGIN packets.
    fn start_tls(&mut self, cfg: &crate::tls::TlsConfig, framed: bool) -> io::Result<()> {
        self.w.flush()?;
        let (Input::Plain(r), Output::Plain(w)) = (
            std::mem::replace(&mut self.r, Input::Switching),
            std::mem::replace(&mut self.w, Output::Switching),
        ) else {
            return Err(io::Error::other("TLS requested twice"));
        };
        let tcp = w.into_inner().map_err(|e| e.into_error())?;
        let flag = Arc::new(AtomicBool::new(framed));
        let reader = SocketReader {
            inner: r,
            framed: Arc::clone(&flag),
            left: 0,
        };
        let writer = SocketWriter {
            inner: tcp,
            framed: Arc::clone(&flag),
            flight: Vec::new(),
        };
        let (tr, tw) = if framed {
            crate::tls::accept_tds7(reader, writer, cfg)?
        } else {
            crate::tls::accept(reader, writer, cfg, "tds/8.0")?
        };
        flag.store(false, Ordering::Relaxed);
        if crate::log_enabled() {
            eprintln!("[tds {}] TLS 1.3 established", self.session.id);
        }
        self.r = Input::Tls(tr);
        self.w = Output::Tls(BufWriter::with_capacity(16 * 1024, tw));
        Ok(())
    }

    /// Back to plaintext after the login (encryption negotiated as login only).
    fn stop_tls(&mut self) -> io::Result<()> {
        self.w.flush()?;
        let (Input::Tls(r), Output::Tls(w)) = (
            std::mem::replace(&mut self.r, Input::Switching),
            std::mem::replace(&mut self.w, Output::Switching),
        ) else {
            return Err(io::Error::other("TLS is not active"));
        };
        let w = w.into_inner().map_err(|e| e.into_error())?.into_inner();
        self.r = Input::Plain(r.into_inner().inner);
        self.w = Output::Plain(BufWriter::new(w.inner));
        Ok(())
    }

    fn requests(&mut self) -> io::Result<()> {
        loop {
            let (kind, msg) = match self.read_message() {
                Ok(m) => m,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(e) => return Err(e),
            };
            let mut t = Tokens {
                b: Vec::new(),
                room: self.packet_size - 8,
            };
            match kind {
                PT_SQL_BATCH => self.sql_batch(&msg, &mut t),
                PT_RPC => self.rpc(&msg, &mut t),
                PT_TRANSACTION => self.transaction_request(&msg, &mut t),
                PT_ATTENTION => {
                    // Requests run to completion before the next one is read: just acknowledge.
                    self.done(&mut t, TK_DONE, DONE_ATTN, 0, 0);
                }
                other => {
                    let e = SqlError::new(
                        "08P01",
                        format!("unsupported TDS packet type 0x{other:02x}"),
                    );
                    self.error_token(&mut t, &e, 1);
                    self.done(&mut t, TK_DONE, DONE_ERROR, 0, 0);
                }
            }
            self.send(PT_REPLY, &t.b)?;
        }
    }

    /// Skips the ALL_HEADERS block (TDS 7.2+) at the start of a request.
    fn skip_headers<'a>(&self, msg: &'a [u8]) -> &'a [u8] {
        if self.version < TDS72 || msg.len() < 4 {
            return msg;
        }
        let total = u32::from_le_bytes([msg[0], msg[1], msg[2], msg[3]]) as usize;
        if (4..=msg.len()).contains(&total) {
            &msg[total..]
        } else {
            msg
        }
    }

    fn sql_batch(&mut self, msg: &[u8], t: &mut Tokens) {
        let sql = from_utf16(self.skip_headers(msg));
        if crate::log_enabled() {
            eprintln!("[tds {}] {sql}", self.session.id);
        }
        let stmts = match parse(&sql, Dialect::MsSql) {
            Ok((s, _)) => s,
            Err(e) => {
                self.error_token(t, &e, 1);
                self.done(t, TK_DONE, DONE_ERROR, 0, 0);
                return;
            }
        };
        if stmts.is_empty() {
            self.done(t, TK_DONE, 0, 0, 0);
            return;
        }
        let last = stmts.len() - 1;
        for (i, st) in stmts.iter().enumerate() {
            let more = if i < last { DONE_MORE } else { 0 };
            if !self.statement(st, &[], t, TK_DONE, more) {
                return;
            }
        }
    }

    /// Runs one statement and appends its tokens; false after an error (the batch stops).
    fn statement(
        &mut self,
        st: &Statement,
        params: &[Datum],
        t: &mut Tokens,
        done: u8,
        more: u16,
    ) -> bool {
        let before = self.session.in_transaction();
        let r = self.session.execute(st, params);
        self.track_transaction(before, st, t);
        match r {
            Ok(Outcome::Rows(rs)) => {
                if let Err(e) = self.result_set(&rs.columns, &rs.rows, t) {
                    self.error_token(t, &e, 1);
                    self.done(t, done, DONE_ERROR | more, 0xc1, 0);
                    return false;
                }
                self.done(t, done, DONE_COUNT | more, 0xc1, rs.rows.len() as u64);
                true
            }
            Ok(Outcome::Command { tag, affected, .. }) => {
                let (cmd, counted) = match tag.split_whitespace().next().unwrap_or("") {
                    "INSERT" => (0xc3, true),
                    "UPDATE" => (0xc5, true),
                    "DELETE" => (0xc4, true),
                    "SELECT" => (0xc1, true),
                    _ => (0, false),
                };
                let nocount = self
                    .session
                    .vars
                    .get("nocount")
                    .is_some_and(|v| v == "on" || v == "1");
                let flags = if counted && !nocount { DONE_COUNT } else { 0 };
                self.done(t, done, flags | more, cmd, affected);
                true
            }
            Err(e) => {
                self.error_token(t, &e, 1);
                self.done(t, done, DONE_ERROR | more, 0, 0);
                false
            }
        }
    }

    /// ENVCHANGE tokens when a statement opened or ended a transaction.
    fn track_transaction(&mut self, before: bool, st: &Statement, t: &mut Tokens) {
        let after = self.session.in_transaction();
        if !before && after {
            self.txn = self.next_txn;
            self.next_txn += 1;
            let mut b = Tokens::default();
            b.u8(8).u8(8).u64(self.txn).u8(0);
            t.with_len(TK_ENVCHANGE, &b);
        } else if before && !after {
            let kind = if rolls_back(st) { 10 } else { 9 };
            let mut b = Tokens::default();
            b.u8(kind).u8(0).u8(8).u64(self.txn);
            t.with_len(TK_ENVCHANGE, &b);
            self.txn = 0;
        }
    }

    fn result_set(
        &mut self,
        cols: &[OutCol],
        rows: &[Vec<Datum>],
        t: &mut Tokens,
    ) -> Result<(), SqlError> {
        let wires: Vec<Wire> = cols
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let values: Vec<&Datum> = rows.iter().map(|r| &r[i]).collect();
                wire_type(&c.ty, &values, self.version)
            })
            .collect();
        t.u8(TK_COLMETADATA).u16(cols.len() as u16);
        for (c, w) in cols.iter().zip(&wires) {
            // UserType (4 bytes from TDS 7.2, 2 before), flags: nullable | updateable-unknown.
            if self.version >= TDS72 {
                t.u32(0);
            } else {
                t.u16(0);
            }
            t.u16(0x0009);
            w.type_info(t);
            t.b_varchar(&c.name);
        }
        for row in rows {
            t.u8(TK_ROW);
            for (d, w) in row.iter().zip(&wires) {
                w.value(d, t)?;
            }
        }
        Ok(())
    }

    fn rpc(&mut self, msg: &[u8], t: &mut Tokens) {
        let body = self.skip_headers(msg).to_vec();
        if let Err(e) = self.rpc_calls(&body, t) {
            self.error_token(t, &e, 1);
            self.done(t, TK_DONEPROC, DONE_ERROR, 0, 0);
        }
    }

    fn rpc_calls(&mut self, body: &[u8], t: &mut Tokens) -> Result<(), SqlError> {
        let mut rd = Rd { b: body, pos: 0 };
        loop {
            let call = read_rpc(&mut rd, self.version)?;
            self.rpc_call(call, t)?;
            // Further calls follow a batch separator (0x80, or 0xFF from TDS 7.2).
            match rd.peek() {
                Some(0x80 | 0xff) => rd.pos += 1,
                _ => return Ok(()),
            }
            if rd.left() == 0 {
                return Ok(());
            }
        }
    }

    fn rpc_call(&mut self, call: RpcCall, t: &mut Tokens) -> Result<(), SqlError> {
        if crate::log_enabled() {
            eprintln!(
                "[tds {}] rpc {} ({} params)",
                self.session.id,
                call.proc,
                call.params.len()
            );
        }
        let text_param = |call: &RpcCall, i: usize| -> String {
            call.params
                .get(i)
                .map(|p| p.value.as_text())
                .unwrap_or_default()
        };
        match call.proc.as_str() {
            "sp_executesql" => {
                let sql = text_param(&call, 0);
                let declared = declared_names(&text_param(&call, 1));
                // Unnamed values bind to the declared names in order.
                let bound: Vec<(String, Datum)> = call
                    .params
                    .iter()
                    .skip(2)
                    .enumerate()
                    .map(|(i, p)| {
                        (
                            if p.name.is_empty() {
                                declared.get(i).cloned().unwrap_or_default()
                            } else {
                                p.name.clone()
                            },
                            p.value.clone(),
                        )
                    })
                    .collect();
                self.run_with_params(&sql, &bound, t);
            }
            "sp_prepare" | "sp_prepexec" => {
                let declared = declared_names(&text_param(&call, 1));
                let sql = text_param(&call, 2);
                let stmts = parse(&sql, Dialect::MsSql)?.0;
                let handle = self.next_handle;
                self.next_handle += 1;
                self.prepared.insert(
                    handle,
                    Prepared {
                        stmts: stmts.clone(),
                        names: declared.clone(),
                    },
                );
                if call.proc == "sp_prepexec" {
                    let bound: Vec<(String, Datum)> = declared
                        .iter()
                        .cloned()
                        .zip(call.params.iter().skip(3).map(|p| p.value.clone()))
                        .collect();
                    self.run_prepared(&stmts, &bound, t);
                }
                self.return_status(t, 0);
                self.return_value(
                    t,
                    0,
                    &call
                        .params
                        .first()
                        .map(|p| p.name.clone())
                        .unwrap_or_default(),
                    handle,
                );
                self.done(t, TK_DONEPROC, 0, 0, 0);
            }
            "sp_execute" => {
                let handle = call
                    .params
                    .first()
                    .map(|p| p.value.as_i64())
                    .transpose()?
                    .unwrap_or(0) as i32;
                let Some(p) = self.prepared.get(&handle) else {
                    return Err(SqlError::new(
                        "26000",
                        format!("Could not find prepared statement with handle {handle}."),
                    ));
                };
                let stmts = p.stmts.clone();
                let bound: Vec<(String, Datum)> = p
                    .names
                    .iter()
                    .cloned()
                    .zip(call.params.iter().skip(1).map(|p| p.value.clone()))
                    .collect();
                self.run_prepared(&stmts, &bound, t);
                self.return_status(t, 0);
                self.done(t, TK_DONEPROC, 0, 0, 0);
            }
            "sp_unprepare" => {
                let handle = call
                    .params
                    .first()
                    .map(|p| p.value.as_i64())
                    .transpose()?
                    .unwrap_or(0) as i32;
                self.prepared.remove(&handle);
                self.return_status(t, 0);
                self.done(t, TK_DONEPROC, 0, 0, 0);
            }
            other => return Err(SqlError::unsupported(format!("stored procedure {other}"))),
        }
        Ok(())
    }

    /// `sp_executesql`: every statement of `sql` with `params` bound as variables.
    fn run_with_params(&mut self, sql: &str, params: &[(String, Datum)], t: &mut Tokens) {
        if crate::log_enabled() {
            eprintln!("[tds {}] {sql}", self.session.id);
        }
        match parse(sql, Dialect::MsSql) {
            Ok((stmts, _)) => {
                let saved = self.bind(params);
                for st in &stmts {
                    if !self.statement(st, &[], t, TK_DONEINPROC, DONE_MORE) {
                        break;
                    }
                }
                self.unbind(saved);
            }
            Err(e) => self.error_token(t, &e, 1),
        }
        self.return_status(t, 0);
        self.done(t, TK_DONEPROC, 0, 0, 0);
    }

    fn run_prepared(&mut self, stmts: &[Statement], params: &[(String, Datum)], t: &mut Tokens) {
        let saved = self.bind(params);
        for st in stmts {
            if !self.statement(st, &[], t, TK_DONEINPROC, DONE_MORE) {
                break;
            }
        }
        self.unbind(saved);
    }

    /// Binds parameters as session variables; returns what they shadowed.
    fn bind(&mut self, params: &[(String, Datum)]) -> Vec<(String, Option<Datum>)> {
        params
            .iter()
            .map(|(n, v)| {
                let key = n.trim_start_matches('@').to_ascii_lowercase();
                let old = self.session.user_vars.insert(key.clone(), v.clone());
                (key, old)
            })
            .collect()
    }

    fn unbind(&mut self, saved: Vec<(String, Option<Datum>)>) {
        for (k, old) in saved.into_iter().rev() {
            match old {
                Some(v) => self.session.user_vars.insert(k, v),
                None => self.session.user_vars.remove(&k),
            };
        }
    }

    fn return_status(&self, t: &mut Tokens, status: i32) {
        t.u8(TK_RETURNSTATUS).u32(status as u32);
    }

    /// RETURNVALUE for an INT output parameter (prepared statement handles).
    fn return_value(&self, t: &mut Tokens, ordinal: u16, name: &str, value: i32) {
        t.u8(TK_RETURNVALUE).u16(ordinal).b_varchar(name).u8(0x01);
        if self.version >= TDS72 {
            t.u32(0);
        } else {
            t.u16(0);
        }
        t.u16(0x0009).u8(0x26).u8(4).u8(4).u32(value as u32);
    }

    /// Transaction-manager request: begin / commit / rollback / save.
    fn transaction_request(&mut self, msg: &[u8], t: &mut Tokens) {
        let body = self.skip_headers(msg).to_vec();
        let kind = body
            .get(..2)
            .map_or(0, |b| u16::from_le_bytes([b[0], b[1]]));
        // BEGIN (isolation u8, name B_VARCHAR), COMMIT / ROLLBACK (name B_VARCHAR, flags with
        // fBeginXact), SAVE (name B_VARCHAR).
        let name_at = |off: usize| -> String {
            let n = body.get(off).copied().map_or(0, usize::from);
            body.get(off + 1..off + 1 + 2 * n)
                .map(from_utf16)
                .unwrap_or_default()
        };
        let stmt = match kind {
            5 => Ok(Statement::Begin {
                isolation: tm_isolation(body.get(2).copied().unwrap_or(0)),
            }),
            7 => Ok(Statement::Commit),
            8 => {
                let name = name_at(2);
                Ok(if name.is_empty() {
                    Statement::Rollback
                } else {
                    Statement::RollbackToSavepoint(name)
                })
            }
            9 => Ok(Statement::Savepoint(name_at(2))),
            other => Err(SqlError::unsupported(format!(
                "transaction manager request {other}"
            ))),
        };
        let begin_again = matches!(kind, 7 | 8)
            && body
                .get(3 + 2 * body.get(2).copied().map_or(0, usize::from))
                .is_some_and(|f| f & 1 == 1);
        match stmt {
            Ok(st) => {
                let before = self.session.in_transaction();
                let r = self.session.execute(&st, &[]);
                self.track_transaction(before, &st, t);
                if let Err(e) = r {
                    self.error_token(t, &e, 1);
                    self.done(t, TK_DONE, DONE_ERROR, 0, 0);
                    return;
                }
                if begin_again {
                    let st = Statement::Begin { isolation: None };
                    let _ = self.session.execute(&st, &[]);
                    self.track_transaction(false, &st, t);
                }
                self.done(t, TK_DONE, 0, 0, 0);
            }
            Err(e) => {
                self.error_token(t, &e, 1);
                self.done(t, TK_DONE, DONE_ERROR, 0, 0);
            }
        }
    }

    // ── tokens ──

    fn done(&self, t: &mut Tokens, token: u8, status: u16, cmd: u16, rows: u64) {
        t.u8(token).u16(status).u16(cmd);
        if self.version >= TDS72 {
            t.u64(rows);
        } else {
            t.u32(rows.min(u64::from(u32::MAX)) as u32);
        }
    }

    fn env_change(&self, t: &mut Tokens, kind: u8, new: &str, old: &str) {
        let mut b = Tokens::default();
        b.u8(kind).b_varchar(new).b_varchar(old);
        t.with_len(TK_ENVCHANGE, &b);
    }

    fn error_token(&self, t: &mut Tokens, e: &SqlError, line: i32) {
        if crate::log_enabled() {
            eprintln!("[tds {}] ERROR {e}", self.session.id);
        }
        let mut b = Tokens::default();
        b.u32(error_number(e.code) as u32)
            .u8(1)
            .u8(16)
            .us_varchar(&tsql_message(e, &self.session.database))
            .b_varchar("storine")
            .b_varchar("");
        if self.version >= TDS72 {
            b.u32(line as u32);
        } else {
            b.u16(line as u16);
        }
        t.with_len(TK_ERROR, &b);
    }

    // ── packets ──

    fn read_message(&mut self) -> io::Result<(u8, Vec<u8>)> {
        let mut out = Vec::new();
        loop {
            let mut h = [0u8; 8];
            self.r.read_exact(&mut h)?;
            let len = usize::from(u16::from_be_bytes([h[2], h[3]]));
            if len < 8 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "bad TDS packet length",
                ));
            }
            let start = out.len();
            out.resize(start + len - 8, 0);
            self.r.read_exact(&mut out[start..])?;
            if out.len() > MAX_MESSAGE {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "TDS message too large",
                ));
            }
            if h[1] & 0x01 != 0 {
                return Ok((h[0], out));
            }
        }
    }

    fn send(&mut self, kind: u8, payload: &[u8]) -> io::Result<()> {
        let room = self.packet_size - 8;
        let chunks: Vec<&[u8]> = if payload.is_empty() {
            vec![&[][..]]
        } else {
            payload.chunks(room).collect()
        };
        let last = chunks.len() - 1;
        for (i, c) in chunks.iter().enumerate() {
            let len = (c.len() + 8) as u16;
            let status = u8::from(i == last);
            let spid = (self.session.id as u16).to_be_bytes();
            self.w.write_all(&[
                kind,
                status,
                (len >> 8) as u8,
                len as u8,
                spid[0],
                spid[1],
                self.packet_id,
                0,
            ])?;
            self.w.write_all(c)?;
            self.packet_id = self.packet_id.wrapping_add(1);
        }
        self.w.flush()
    }
}

// ── PRELOGIN / LOGIN7 ──

/// The value of PRELOGIN option `token`.
fn prelogin_option(msg: &[u8], token: u8) -> Option<&[u8]> {
    let mut i = 0;
    while i + 5 <= msg.len() && msg[i] != 0xff {
        let off = usize::from(u16::from_be_bytes([msg[i + 1], msg[i + 2]]));
        let len = usize::from(u16::from_be_bytes([msg[i + 3], msg[i + 4]]));
        if msg[i] == token {
            return msg.get(off..off + len);
        }
        i += 5;
    }
    None
}

fn prelogin_reply(encryption: u8) -> Vec<u8> {
    // VERSION (16.0.1000), ENCRYPTION, INSTOPT, THREADID (empty), MARS (off).
    let options: [(u8, Vec<u8>); 5] = [
        (0x00, vec![16, 0, 0x03, 0xe8, 0, 0]),
        (0x01, vec![encryption]),
        (0x02, vec![0]),
        (0x03, vec![]),
        (0x04, vec![0]),
    ];
    let header_len = options.len() * 5 + 1;
    let mut head = Vec::new();
    let mut data = Vec::new();
    for (token, value) in &options {
        let off = (header_len + data.len()) as u16;
        head.push(*token);
        head.extend_from_slice(&off.to_be_bytes());
        head.extend_from_slice(&(value.len() as u16).to_be_bytes());
        data.extend_from_slice(value);
    }
    head.push(0xff);
    head.extend_from_slice(&data);
    head
}

struct Login {
    version: u32,
    packet_size: usize,
    user: String,
    password: String,
    database: String,
}

fn parse_login7(b: &[u8]) -> Result<Login, SqlError> {
    let bad = || SqlError::new("08P01", "malformed LOGIN7 message");
    if b.len() < 94 {
        return Err(bad());
    }
    let u32_at = |i: usize| u32::from_le_bytes([b[i], b[i + 1], b[i + 2], b[i + 3]]);
    let u16_at = |i: usize| usize::from(u16::from_le_bytes([b[i], b[i + 1]]));
    let field = |at: usize| -> Result<&[u8], SqlError> {
        let (off, chars) = (u16_at(at), u16_at(at + 2));
        b.get(off..off + chars * 2).ok_or_else(bad)
    };
    let version = u32::from_be_bytes(u32_at(4).to_le_bytes());
    let user = from_utf16(field(40)?);
    let password: Vec<u8> = field(44)?
        .iter()
        .map(|x| (x ^ 0xa5).rotate_left(4))
        .collect();
    Ok(Login {
        version: if version >= 0x7000_0000 {
            version
        } else {
            u32_at(4)
        },
        packet_size: u32_at(8) as usize,
        user,
        password: from_utf16(&password),
        database: from_utf16(field(68)?),
    })
}

/// The isolation level of a transaction-manager BEGIN.
fn tm_isolation(level: u8) -> Option<String> {
    match level {
        0x01 => Some("read uncommitted".into()),
        0x02 => Some("read committed".into()),
        0x04 => Some("repeatable read".into()),
        0x08 => Some("serializable".into()),
        0x10 => Some("snapshot".into()),
        _ => None,
    }
}

/// Parameter names of an `sp_executesql` / `sp_prepare` declaration (`@P1 int, @P2 nvarchar(50)`).
pub(crate) fn declared_names(decl: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0;
    let mut current = String::new();
    for c in decl.chars().chain(std::iter::once(',')) {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                if let Some(name) = current
                    .split_whitespace()
                    .next()
                    .filter(|n| n.starts_with('@'))
                {
                    out.push(name.to_owned());
                }
                current.clear();
                continue;
            }
            _ => {}
        }
        current.push(c);
    }
    out
}

// ── RPC requests ──

struct RpcParam {
    name: String,
    value: Datum,
}

struct RpcCall {
    proc: String,
    params: Vec<RpcParam>,
}

/// A cursor over a request body.
struct Rd<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> Rd<'a> {
    fn bad() -> SqlError {
        SqlError::new("08P01", "malformed RPC request")
    }
    fn left(&self) -> usize {
        self.b.len() - self.pos
    }
    fn peek(&self) -> Option<u8> {
        self.b.get(self.pos).copied()
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], SqlError> {
        let s = self.b.get(self.pos..self.pos + n).ok_or_else(Rd::bad)?;
        self.pos += n;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8, SqlError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, SqlError> {
        let s = self.take(2)?;
        Ok(u16::from_le_bytes([s[0], s[1]]))
    }
    fn u32(&mut self) -> Result<u32, SqlError> {
        let s = self.take(4)?;
        Ok(u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
    }
    fn u64(&mut self) -> Result<u64, SqlError> {
        let s = self.take(8)?;
        Ok(u64::from_le_bytes(s.try_into().map_err(|_| Rd::bad())?))
    }
    fn b_varchar(&mut self) -> Result<String, SqlError> {
        let n = usize::from(self.u8()?);
        Ok(from_utf16(self.take(n * 2)?))
    }
    fn us_varchar(&mut self) -> Result<String, SqlError> {
        let n = usize::from(self.u16()?);
        Ok(from_utf16(self.take(n * 2)?))
    }
    /// A PLP value (`None`: NULL).
    fn plp(&mut self) -> Result<Option<Vec<u8>>, SqlError> {
        let total = self.u64()?;
        if total == u64::MAX {
            return Ok(None);
        }
        let mut out = Vec::new();
        loop {
            let n = self.u32()? as usize;
            if n == 0 {
                return Ok(Some(out));
            }
            out.extend_from_slice(self.take(n)?);
            if out.len() > MAX_MESSAGE {
                return Err(Rd::bad());
            }
        }
    }
}

const PROC_IDS: [(u16, &str); 6] = [
    (10, "sp_executesql"),
    (11, "sp_prepare"),
    (12, "sp_execute"),
    (13, "sp_prepexec"),
    (14, "sp_prepexecrpc"),
    (15, "sp_unprepare"),
];

fn read_rpc(rd: &mut Rd<'_>, version: u32) -> Result<RpcCall, SqlError> {
    let len = rd.u16()?;
    let proc = if len == 0xffff {
        let id = rd.u16()?;
        PROC_IDS
            .iter()
            .find(|(i, _)| *i == id)
            .map(|(_, n)| (*n).to_owned())
            .ok_or_else(|| SqlError::unsupported(format!("system procedure #{id}")))?
    } else {
        let name = from_utf16(rd.take(usize::from(len) * 2)?);
        // [sys].[sp_executesql], dbo.sp_executesql, ...
        name.rsplit('.')
            .next()
            .unwrap_or(&name)
            .trim_matches(|c| c == '[' || c == ']')
            .to_ascii_lowercase()
    };
    rd.u16()?; // option flags
    let mut params = Vec::new();
    while rd.left() > 0 && !matches!(rd.peek(), Some(0x80 | 0xff)) {
        let name = rd.b_varchar()?;
        rd.u8()?; // status flags (output, default)
        let value = read_typed_value(rd, version)?;
        params.push(RpcParam { name, value });
    }
    Ok(RpcCall { proc, params })
}

/// TYPE_INFO followed by its value.
fn read_typed_value(rd: &mut Rd<'_>, version: u32) -> Result<Datum, SqlError> {
    let _ = version;
    let ty = rd.u8()?;
    Ok(match ty {
        0x1f => Datum::Null,
        0x30 => Datum::Int(i64::from(rd.u8()?)),
        0x32 => Datum::Bool(rd.u8()? != 0),
        0x34 => Datum::Int(i64::from(rd.u16()? as i16)),
        0x38 => Datum::Int(i64::from(rd.u32()? as i32)),
        0x7f => Datum::Int(rd.u64()? as i64),
        0x3b => Datum::Float(f64::from(f32::from_bits(rd.u32()?))),
        0x3e => Datum::Float(f64::from_bits(rd.u64()?)),
        0x3c => money(rd.take(8)?)?,
        0x7a => money(rd.take(4)?)?,
        0x3d => datetime(rd.take(8)?)?,
        0x3a => datetime(rd.take(4)?)?,
        // Byte-length types: maxlen (+ precision, scale), then a 1-byte value length.
        0x24 | 0x26 | 0x68 | 0x6d | 0x6e | 0x6f | 0x6a | 0x6c => {
            rd.u8()?;
            let scale = if matches!(ty, 0x6a | 0x6c) {
                rd.u8()?;
                rd.u8()?
            } else {
                0
            };
            let n = usize::from(rd.u8()?);
            if n == 0 {
                return Ok(Datum::Null);
            }
            let v = rd.take(n)?;
            match ty {
                0x24 => {
                    let g: [u8; 16] = v.try_into().map_err(|_| Rd::bad())?;
                    Datum::Uuid(guid_bytes(&g))
                }
                0x26 => Datum::Int(match n {
                    1 => i64::from(v[0]),
                    2 => i64::from(i16::from_le_bytes([v[0], v[1]])),
                    4 => i64::from(i32::from_le_bytes([v[0], v[1], v[2], v[3]])),
                    8 => i64::from_le_bytes(v.try_into().map_err(|_| Rd::bad())?),
                    _ => return Err(Rd::bad()),
                }),
                0x68 => Datum::Bool(v[0] != 0),
                0x6d => match n {
                    4 => Datum::Float(f64::from(f32::from_le_bytes([v[0], v[1], v[2], v[3]]))),
                    8 => Datum::Float(f64::from_le_bytes(v.try_into().map_err(|_| Rd::bad())?)),
                    _ => return Err(Rd::bad()),
                },
                0x6e => money(v)?,
                0x6f => datetime(v)?,
                _ => {
                    let body = &v[1..];
                    if body.len() > 16 {
                        return Err(Rd::bad());
                    }
                    let mut mag = [0u8; 16];
                    mag[..body.len()].copy_from_slice(body);
                    let m = i128::try_from(u128::from_le_bytes(mag)).map_err(|_| Rd::bad())?;
                    let m = if v[0] == 0 { -m } else { m };
                    Datum::Numeric(
                        Decimal::new(m, scale)
                            .map_err(|e| SqlError::new("22003", e.to_string()))?,
                    )
                }
            }
        }
        0x28 => {
            let n = usize::from(rd.u8()?);
            if n == 0 {
                return Ok(Datum::Null);
            }
            let v = rd.take(n)?;
            Datum::Date((le_uint(v) as i64 - DAYS_0001_TO_1970) as i32)
        }
        0x29..=0x2b => {
            let scale = u32::from(rd.u8()?.min(7));
            let n = usize::from(rd.u8()?);
            if n == 0 {
                return Ok(Datum::Null);
            }
            let v = rd.take(n)?;
            let time_len = match scale {
                0..=2 => 3,
                3..=4 => 4,
                _ => 5,
            };
            let ns = le_uint(v.get(..time_len).ok_or_else(Rd::bad)?) as i64 * 10i64.pow(9 - scale);
            if ty == 0x29 {
                Datum::Time(ns)
            } else {
                // DATETIMEOFFSET stores UTC; the offset only affects display.
                let days = le_uint(v.get(time_len..time_len + 3).ok_or_else(Rd::bad)?) as i64
                    - DAYS_0001_TO_1970;
                Datum::Timestamp(days * 86_400_000_000_000 + ns)
            }
        }
        // Short-length types: maxlen (u16; 0xFFFF = MAX, sent as PLP), collation for text.
        0xa5 | 0xa7 | 0xad | 0xaf | 0xe7 | 0xef => {
            let max = rd.u16()?;
            if matches!(ty, 0xa7 | 0xaf | 0xe7 | 0xef) {
                rd.take(5)?;
            }
            let raw = if max == 0xffff {
                rd.plp()?
            } else {
                let n = rd.u16()?;
                if n == 0xffff {
                    None
                } else {
                    Some(rd.take(usize::from(n))?.to_vec())
                }
            };
            match raw {
                None => Datum::Null,
                Some(b) => match ty {
                    0xe7 | 0xef => Datum::Text(from_utf16(&b)),
                    0xa7 | 0xaf => Datum::Text(String::from_utf8_lossy(&b).into_owned()),
                    _ => Datum::Bytes(b),
                },
            }
        }
        // Legacy long types: TEXT, NTEXT, IMAGE.
        0x23 | 0x63 | 0x22 => {
            rd.u32()?;
            if ty != 0x22 {
                rd.take(5)?;
            }
            let n = rd.u32()?;
            if n == u32::MAX {
                return Ok(Datum::Null);
            }
            let b = rd.take(n as usize)?.to_vec();
            match ty {
                0x63 => Datum::Text(from_utf16(&b)),
                0x23 => Datum::Text(String::from_utf8_lossy(&b).into_owned()),
                _ => Datum::Bytes(b),
            }
        }
        // XML: schema info, then PLP.
        0xf1 => {
            if rd.u8()? != 0 {
                rd.b_varchar()?;
                rd.b_varchar()?;
                rd.us_varchar()?;
            }
            rd.plp()?
                .map_or(Datum::Null, |b| Datum::Text(from_utf16(&b)))
        }
        other => {
            return Err(SqlError::unsupported(format!(
                "parameter type 0x{other:02x}"
            )))
        }
    })
}

fn le_uint(b: &[u8]) -> u64 {
    b.iter()
        .rev()
        .fold(0u64, |acc, x| (acc << 8) | u64::from(*x))
}

/// MONEY (8 bytes: high then low 32 bits) / SMALLMONEY (4 bytes), in ten-thousandths.
fn money(v: &[u8]) -> Result<Datum, SqlError> {
    let units = match v.len() {
        4 => i64::from(i32::from_le_bytes([v[0], v[1], v[2], v[3]])),
        8 => {
            let hi = i32::from_le_bytes([v[0], v[1], v[2], v[3]]);
            let lo = u32::from_le_bytes([v[4], v[5], v[6], v[7]]);
            (i64::from(hi) << 32) | i64::from(lo)
        }
        _ => return Err(Rd::bad()),
    };
    Ok(Datum::Numeric(
        Decimal::new(i128::from(units), 4).map_err(|e| SqlError::new("22003", e.to_string()))?,
    ))
}

/// DATETIME (days since 1900 + 1/300 s ticks) / SMALLDATETIME (days + minutes).
fn datetime(v: &[u8]) -> Result<Datum, SqlError> {
    let (days, ns) = match v.len() {
        8 => {
            let days = i64::from(i32::from_le_bytes([v[0], v[1], v[2], v[3]]));
            let ticks = i64::from(u32::from_le_bytes([v[4], v[5], v[6], v[7]]));
            (days, ticks * 1_000_000_000 / 300)
        }
        4 => {
            let days = i64::from(u16::from_le_bytes([v[0], v[1]]));
            let minutes = i64::from(u16::from_le_bytes([v[2], v[3]]));
            (days, minutes * 60_000_000_000)
        }
        _ => return Err(Rd::bad()),
    };
    Ok(Datum::Timestamp(
        (days - DAYS_1900_TO_1970) * 86_400_000_000_000 + ns,
    ))
}

/// Whether a statement ending a transaction rolled it back (ENVCHANGE 10) rather than committed it.
fn rolls_back(st: &Statement) -> bool {
    match st {
        Statement::Rollback | Statement::RollbackToSavepoint(_) => true,
        Statement::If {
            then, otherwise, ..
        } => rolls_back(then) || otherwise.as_deref().is_some_and(rolls_back),
        _ => false,
    }
}
