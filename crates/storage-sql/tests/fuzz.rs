//! Structure-aware fuzzing of the SQL gateway (`TASK-135`, `RULE-ERR-01`): the SQL parser in
//! every dialect, the value parsers, the COPY reader, and the PostgreSQL, MySQL and SQL Server
//! endpoints over real sockets. Hostile input must produce errors: never a panic, a stack
//! overflow, a hang, or an allocation sized by a length field instead of by the bytes sent.
//! The private decoders have their own sweeps (`fuzz_tests` in `pg.rs`, `mysql.rs`, `tds.rs`,
//! `tls.rs`, `datum.rs`).
//!
//! `STORINE_FUZZ_ITERS` scales every iteration count (e.g. `0.1` or `100`); `STORINE_FUZZ_SEED`
//! varies the generators.

use std::alloc::{GlobalAlloc, Layout, System};
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use storage_sql::ast::{CopyStmt, Expr};
use storage_sql::datum::{self, Datum};
use storage_sql::exec::{Engine, EngineConfig};
use storage_sql::lexer::tokenize;
use storage_sql::parser::{parse, parse_data_type, parse_expr};
use storage_sql::{embedded, vector, Dialect};
use storage_types::testing::SweepRng;

// ── harness ──

/// The system allocator, recording the largest single request (`LARGEST`).
struct Tracking;

static LARGEST: AtomicUsize = AtomicUsize::new(0);

// SAFETY: every call is forwarded unchanged to `System`; the bookkeeping is one atomic.
unsafe impl GlobalAlloc for Tracking {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        LARGEST.fetch_max(layout.size(), Ordering::Relaxed);
        // SAFETY: the caller upholds `GlobalAlloc::alloc`'s contract.
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        LARGEST.fetch_max(layout.size(), Ordering::Relaxed);
        // SAFETY: the caller upholds `GlobalAlloc::alloc_zeroed`'s contract.
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` came from `System` with this layout.
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        LARGEST.fetch_max(new_size, Ordering::Relaxed);
        // SAFETY: the caller upholds `GlobalAlloc::realloc`'s contract.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Tracking = Tracking;

/// No single allocation in this binary may exceed this: every input here is at most a few
/// MiB, so anything larger was sized by a hostile length field.
const ALLOC_LIMIT: usize = 64 << 20;

fn check_allocations() {
    let largest = LARGEST.load(Ordering::Relaxed);
    assert!(
        largest <= ALLOC_LIMIT,
        "an allocation of {largest} bytes: a length field was trusted"
    );
}

/// `base` scaled by `STORINE_FUZZ_ITERS`.
fn iters(base: usize) -> usize {
    let scale = std::env::var("STORINE_FUZZ_ITERS")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|s| s.is_finite() && *s > 0.0)
        .unwrap_or(1.0);
    ((base as f64 * scale) as usize).max(1)
}

fn rng(tag: u64) -> SweepRng {
    let seed = std::env::var("STORINE_FUZZ_SEED")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    SweepRng::new(tag ^ seed.wrapping_mul(0x9E37_79B9_7F4A_7C15))
}

/// Panics anywhere in the process (server connection threads included), recorded by a hook.
static PANICS: Mutex<Vec<String>> = Mutex::new(Vec::new());

fn install_panic_hook() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        let default = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            // Test threads are named after their test and fail on their own; server
            // connection threads (and sweep clients) are not.
            if std::thread::current().name().is_none() {
                PANICS
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .push(info.to_string());
            }
            default(info);
        }));
    });
}

fn assert_no_panics() {
    // Copied out first: the assertion panics, and the hook takes the lock again.
    let p = PANICS.lock().unwrap_or_else(|p| p.into_inner()).clone();
    assert!(p.is_empty(), "{} panic(s), first: {}", p.len(), p[0]);
}

const DIALECTS: [Dialect; 4] = [
    Dialect::Postgres,
    Dialect::MySql,
    Dialect::MsSql,
    Dialect::Sqlite,
];

/// Runs `f`, failing when one call takes longer than `limit` (a hang in the making).
fn timed<T>(what: &str, limit: Duration, f: impl FnOnce() -> T) -> T {
    let start = Instant::now();
    let out = f();
    let took = start.elapsed();
    let shown: String = what.chars().take(200).collect();
    assert!(took < limit, "{took:?} for {shown:?}");
    out
}

// ── SQL corpus ──

/// String literals of a Rust source file (plain and raw), escapes resolved.
fn rust_strings(src: &str) -> Vec<String> {
    let b: Vec<char> = src.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            '/' if b.get(i + 1) == Some(&'/') => {
                while i < b.len() && b[i] != '\n' {
                    i += 1;
                }
            }
            '\'' => {
                // A char literal ('x', '\n', '\'') or a lifetime.
                if b.get(i + 1) == Some(&'\\') {
                    i += 3;
                    while i < b.len() && b[i] != '\'' {
                        i += 1;
                    }
                    i += 1;
                } else if b.get(i + 2) == Some(&'\'') {
                    i += 3;
                } else {
                    i += 1;
                }
            }
            'r' if matches!(b.get(i + 1), Some('"' | '#'))
                && !(i > 0 && (b[i - 1].is_alphanumeric() || b[i - 1] == '_')) =>
            {
                let mut j = i + 1;
                let mut hashes = 0;
                while b.get(j) == Some(&'#') {
                    hashes += 1;
                    j += 1;
                }
                if b.get(j) != Some(&'"') {
                    i += 1;
                    continue;
                }
                j += 1;
                let start = j;
                while j < b.len() {
                    if b[j] == '"' && (1..=hashes).all(|k| b.get(j + k) == Some(&'#')) {
                        break;
                    }
                    j += 1;
                }
                out.push(b[start..j.min(b.len())].iter().collect());
                i = j + 1 + hashes;
            }
            '"' => {
                let mut s = String::new();
                i += 1;
                while i < b.len() && b[i] != '"' {
                    if b[i] == '\\' {
                        i += 1;
                        match b.get(i) {
                            Some('n') => s.push('\n'),
                            Some('t') => s.push('\t'),
                            Some('r') => s.push('\r'),
                            Some('0') => s.push('\0'),
                            Some('\n') => {
                                while b.get(i + 1).is_some_and(|c| c.is_whitespace()) {
                                    i += 1;
                                }
                            }
                            Some('u') => {
                                let close = (i..b.len()).find(|&k| b[k] == '}').unwrap_or(i);
                                let hex: String = b[(i + 2).min(close)..close].iter().collect();
                                if let Some(c) =
                                    u32::from_str_radix(&hex, 16).ok().and_then(char::from_u32)
                                {
                                    s.push(c);
                                }
                                i = close;
                            }
                            Some(c) => s.push(*c),
                            None => {}
                        }
                    } else {
                        s.push(b[i]);
                    }
                    i += 1;
                }
                out.push(s);
                i += 1;
            }
            _ => i += 1,
        }
    }
    out
}

const SQL_STARTS: &[&str] = &[
    "select",
    "insert",
    "update",
    "delete",
    "create",
    "alter",
    "drop",
    "with",
    "begin",
    "commit",
    "rollback",
    "set",
    "show",
    "explain",
    "values",
    "truncate",
    "copy",
    "merge",
    "exec",
    "execute",
    "declare",
    "if",
    "savepoint",
    "release",
    "start",
    "use",
    "describe",
    "replace",
    "analyze",
    "grant",
    "discard",
    "prepare",
    "deallocate",
    "lock",
    "listen",
    "table",
];

/// Hand-written statements for constructs the test files use little.
const EXTRA_SQL: &[&str] = &[
    "SELECT a, count(*) FILTER (WHERE b > 1) OVER (PARTITION BY c ORDER BY d DESC NULLS LAST) FROM t GROUP BY a HAVING sum(b) > 2 ORDER BY 1 LIMIT 10 OFFSET 5",
    "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT * FROM r",
    "SELECT CASE WHEN x IS NOT DISTINCT FROM y THEN 'a' WHEN x BETWEEN 1 AND 2 THEN 'b' ELSE NULL END FROM t",
    "SELECT * FROM a LEFT JOIN b USING (id) NATURAL JOIN c CROSS JOIN LATERAL (SELECT 1) d",
    "INSERT INTO t (a, b) VALUES (1, 'x'), (DEFAULT, E'\\n') ON CONFLICT (a) DO UPDATE SET b = excluded.b RETURNING *",
    "UPDATE t SET a = a + 1, b = $1::text WHERE id = ANY($2) AND c @> '{\"k\": 1}'::jsonb",
    "SELECT x->>'a', x #>> '{a,b}', arr[1], v <-> '[1,2,3]', $$dollar$$, $tag$q$tag$ FROM t",
    "SELECT `a`, @x := 1, @@version, 0x1F, b'101', 'a' 'b' FROM `db`.`t` LIMIT 1, 2",
    "INSERT INTO t SET a = 1 ON DUPLICATE KEY UPDATE a = VALUES(a)",
    "SELECT TOP 10 [a], @p1, N'x' FROM [dbo].[t] WITH (NOLOCK) ORDER BY 1 OFFSET 0 ROWS FETCH NEXT 5 ROWS ONLY",
    "IF EXISTS (SELECT 1 FROM t) BEGIN UPDATE t SET a = 1 END ELSE SELECT 2",
    "DECLARE @x INT = 1; SET @x = @x + 1; SELECT @x",
    "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET a = s.a WHEN NOT MATCHED THEN INSERT (id, a) VALUES (s.id, s.a);",
    "CREATE TABLE IF NOT EXISTS t (id BIGSERIAL PRIMARY KEY, a NUMERIC(10,2) DEFAULT 0 CHECK (a >= 0), b VARCHAR(20) UNIQUE, c TIMESTAMPTZ DEFAULT now(), d INT REFERENCES u(id) ON DELETE CASCADE, e vector(3), f TEXT[])",
    "ALTER TABLE t ADD COLUMN x INT NOT NULL DEFAULT 1, DROP COLUMN y, RENAME COLUMN a TO b",
    "CREATE INDEX IF NOT EXISTS i ON t USING hnsw (e vector_l2_ops)",
    "COPY t (a, b) FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ';', NULL 'x')",
    "SELECT EXTRACT(EPOCH FROM now() - interval '1 day 02:00:00'), date_trunc('hour', ts AT TIME ZONE 'UTC') FROM t",
    "SELECT * FROM t WHERE a IN (SELECT b FROM u WHERE u.c = t.c) AND NOT EXISTS (SELECT 1) OR a LIKE 'x%' ESCAPE '!'",
    "SELECT a FROM t UNION SELECT b FROM u INTERSECT SELECT c FROM v EXCEPT SELECT d FROM w",
    "SELECT lpad('a', 5, 'xy'), repeat('ab', 3), substring('abc' FROM 2 FOR 1), position('b' IN 'abc')",
    "SELECT CAST(1 AS SIGNED), CONVERT('1', UNSIGNED), 5 DIV 2, 5 MOD 2, 1 <=> NULL, a REGEXP 'x' FROM t",
];

fn corpus() -> &'static [String] {
    static CORPUS: OnceLock<Vec<String>> = OnceLock::new();
    CORPUS.get_or_init(|| {
        let mut out: Vec<String> = [include_str!("executor.rs"), include_str!("pushdown.rs")]
            .iter()
            .flat_map(|src| rust_strings(src))
            .filter(|s| {
                let first = s
                    .trim_start()
                    .split(|c: char| !c.is_ascii_alphabetic())
                    .next()
                    .unwrap_or("")
                    .to_ascii_lowercase();
                SQL_STARTS.contains(&first.as_str())
            })
            .collect();
        out.extend(EXTRA_SQL.iter().map(|s| (*s).to_owned()));
        out.sort();
        out.dedup();
        out
    })
}

// ── SQL mutations ──

/// SQL text cut into pieces: whitespace, words / numbers, quoted runs, single punctuation.
fn pieces(sql: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut rest = sql;
    while let Some(c) = rest.chars().next() {
        let len = if c.is_whitespace() {
            rest.find(|x: char| !x.is_whitespace())
                .unwrap_or(rest.len())
        } else if c.is_alphanumeric() || matches!(c, '_' | '$' | '@') {
            rest.find(|x: char| !(x.is_alphanumeric() || matches!(x, '_' | '$' | '.' | '@')))
                .unwrap_or(rest.len())
        } else if matches!(c, '\'' | '"' | '`') {
            rest[1..].find(c).map_or(rest.len(), |p| p + 2)
        } else {
            c.len_utf8()
        };
        let len = len.max(c.len_utf8());
        out.push(&rest[..len]);
        rest = &rest[len..];
    }
    out
}

const KEYWORDS: &[&str] = &[
    "SELECT",
    "FROM",
    "WHERE",
    "AND",
    "OR",
    "NOT",
    "NULL",
    "IS",
    "IN",
    "EXISTS",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    "CAST",
    "AS",
    "JOIN",
    "ON",
    "USING",
    "GROUP",
    "BY",
    "ORDER",
    "HAVING",
    "LIMIT",
    "OFFSET",
    "UNION",
    "ALL",
    "DISTINCT",
    "INSERT",
    "INTO",
    "VALUES",
    "UPDATE",
    "SET",
    "DELETE",
    "WITH",
    "RECURSIVE",
    "OVER",
    "PARTITION",
    "BETWEEN",
    "LIKE",
    "ILIKE",
    "ESCAPE",
    "ARRAY",
    "ROW",
    "INTERVAL",
    "COLLATE",
    "AT",
    "TIME",
    "ZONE",
    "DEFAULT",
    "RETURNING",
    "LATERAL",
    "FILTER",
    "EXTRACT",
    "ANY",
    "SOME",
    "TOP",
    "BEGIN",
    "IF",
    "DECLARE",
    "EXEC",
    "TABLE",
    "CREATE",
    "DROP",
    "ALTER",
    "PRIMARY",
    "KEY",
    "REFERENCES",
    "INDEX",
    "BINARY",
    "DIV",
    "MOD",
    "XOR",
    "REGEXP",
    "OUTPUT",
    "MERGE",
    "MATCHED",
    "COPY",
    "STDIN",
];

const OPERATORS: &[&str] = &[
    "(", ")", ",", ";", ".", "*", "+", "-", "/", "%", "=", "<>", "!=", "<", "<=", ">", ">=", "||",
    "::", "^", "~", "&", "|", "<<", ">>", "->", "->>", "#>", "#>>", "@>", "<@", "&&", "<->", "<=>",
    "<#>", "[", "]", "!", "{", "}", ":", "?", "$1", "$65535", "$0", "@p1", "@@x", ":name", "'",
    "\"", "`", "/*", "*/", "--", "#", "$$", "E'\\x", "X'0", "N'",
];

const EDGE_NUMBERS: &[&str] = &[
    "0",
    "-1",
    "9223372036854775807",
    "-9223372036854775808",
    "9223372036854775808",
    "18446744073709551616",
    "1e308",
    "1e309",
    "-1e-400",
    "1e999999999",
    "0.000000000000000000000000000000000000001",
    "99999999999999999999999999999999999999999",
    "2147483648",
    "65536",
    "1.5",
    ".5",
    "1.",
    "0x7fffffffffffffff",
    "NaN",
    "'Infinity'",
    "'-infinity'::float8",
];

const UNICODE: &[&str] = &[
    "é",
    "€",
    "𝄞",
    "\u{0}",
    "\u{301}",
    "\u{202e}",
    "\u{feff}",
    "\u{ffff}",
    "日本語",
    "\n",
    "\r\n",
    "\t",
    "\\",
    "'",
    "''",
    "\"",
    "ß",
    "İ",
    "\u{10ffff}",
];

/// One hostile edit of `sql` (token-level, character-level or byte-level).
fn mutate_sql(rng: &mut SweepRng, sql: &str) -> String {
    let corpus = corpus();
    let mut p: Vec<String> = pieces(sql).into_iter().map(str::to_owned).collect();
    if p.is_empty() {
        p.push(String::new());
    }
    match rng.below(12) {
        0 => {
            p.remove(rng.below(p.len()));
        }
        1 => {
            let at = rng.below(p.len());
            let x = p[at].clone();
            p.insert(at, x);
        }
        2 => {
            let (a, b) = (rng.below(p.len()), rng.below(p.len()));
            p.swap(a, b);
        }
        3 => {
            let k = KEYWORDS[rng.below(KEYWORDS.len())];
            p.insert(rng.below(p.len() + 1), format!(" {k} "));
        }
        4 => {
            let o = OPERATORS[rng.below(OPERATORS.len())];
            p.insert(rng.below(p.len() + 1), o.to_owned());
        }
        5 => p.truncate(rng.below(p.len() + 1)),
        6 => {
            let u = UNICODE[rng.below(UNICODE.len())];
            p.insert(rng.below(p.len() + 1), u.to_owned());
        }
        7 => {
            // A number (or any piece) replaced by an edge value.
            let at = if rng.below(2) == 0 {
                rng.below(p.len())
            } else {
                p.iter()
                    .position(|x| x.starts_with(|c: char| c.is_ascii_digit()))
                    .unwrap_or(0)
            };
            p[at] = EDGE_NUMBERS[rng.below(EDGE_NUMBERS.len())].to_owned();
        }
        8 => {
            // A run of pieces spliced in from another statement.
            let other: Vec<String> = pieces(&corpus[rng.below(corpus.len())])
                .into_iter()
                .map(str::to_owned)
                .collect();
            let from = rng.below(other.len() + 1);
            let to = (from + rng.below(8)).min(other.len());
            let at = rng.below(p.len() + 1);
            p.splice(at..at, other[from..to].iter().cloned());
        }
        9 => {
            // A piece repeated many times (long chains, deep nesting), up to 64 KiB.
            let at = rng.below(p.len());
            let n = (1 + rng.below(300)).min((64 << 10) / p[at].len().max(1));
            p[at] = p[at].repeat(n.max(1));
        }
        _ => {
            let mut bytes = p.concat().into_bytes();
            rng.mutate(&mut bytes);
            return String::from_utf8_lossy(&bytes).into_owned();
        }
    }
    p.concat()
}

/// A hostile statement: a corpus entry with one to four mutations.
fn hostile_sql(rng: &mut SweepRng) -> String {
    let corpus = corpus();
    let mut sql = corpus[rng.below(corpus.len())].clone();
    for _ in 0..=rng.below(4) {
        if sql.len() < 256 << 10 {
            sql = mutate_sql(rng, &sql);
        }
    }
    sql
}

/// Whether the SQL text of `e` leaves something out by design.
fn summarized(e: &Expr) -> bool {
    let any = |v: &[Expr]| v.iter().any(summarized);
    match e {
        Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => true,
        Expr::Variable { .. } | Expr::Blob(_) | Expr::Default => true,
        Expr::Function { over: Some(_), .. }
        | Expr::Like {
            escape: Some(_), ..
        } => true,
        // Garbage such as `SELECT (x)`, read as a call of a function named `select`.
        Expr::Function { name, .. } if KEYWORDS.iter().any(|k| k.eq_ignore_ascii_case(name)) => {
            true
        }
        Expr::Function { args, .. } | Expr::Tuple(args) | Expr::Array(args) => any(args),
        Expr::Unary { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::IsNull { expr, .. }
        | Expr::IsBool { expr, .. }
        | Expr::Extract { expr, .. } => summarized(expr),
        Expr::Binary { left, right, .. }
        | Expr::IsDistinctFrom { left, right, .. }
        | Expr::AnyAll { left, right, .. }
        | Expr::Like {
            expr: left,
            pattern: right,
            ..
        }
        | Expr::Index {
            expr: left,
            index: right,
        } => summarized(left) || summarized(right),
        Expr::Between {
            expr, low, high, ..
        } => summarized(expr) || summarized(low) || summarized(high),
        Expr::InList { expr, list, .. } => summarized(expr) || any(list),
        Expr::Case {
            operand,
            whens,
            else_,
        } => {
            operand.as_deref().is_some_and(summarized)
                || whens.iter().any(|(w, t)| summarized(w) || summarized(t))
                || else_.as_deref().is_some_and(summarized)
        }
        _ => false,
    }
}

fn parse_everything(sql: &str, d: Dialect) {
    timed(sql, Duration::from_secs(5), || {
        let _ = tokenize(sql, d);
        let _ = parse(sql, d);
        if let Ok(e) = parse_expr(sql, d) {
            // The SQL text of an expression parses back to the same tree (the renderer is for
            // stored defaults and checks: subqueries, windows and the like are summarized).
            if d == Dialect::Postgres && !summarized(&e) {
                let text = e.to_string();
                match parse_expr(&text, d) {
                    Ok(again) => assert_eq!(again, e, "{sql:?} rendered as {text:?}"),
                    // Parenthesized, a tree near the nesting limit may pass it.
                    Err(err) if err.code == "54001" => {}
                    Err(err) => panic!("{sql:?} rendered as {text:?}, which fails: {err}"),
                }
            }
        }
        let _ = parse_data_type(sql);
    });
}

// ── SQL text ──

#[test]
fn fuzz_sql_corpus_parses() {
    let corpus = corpus();
    assert!(
        corpus.len() > 200,
        "corpus has only {} statements",
        corpus.len()
    );
    let ok = corpus
        .iter()
        .filter(|s| DIALECTS.iter().any(|d| parse(s, *d).is_ok()))
        .count();
    assert!(ok * 10 > corpus.len() * 9, "{ok} of {} parse", corpus.len());
}

#[test]
fn fuzz_sql_truncated_at_every_token_boundary() {
    install_panic_hook();
    for sql in corpus() {
        let mut end = 0;
        for piece in pieces(sql) {
            end += piece.len();
            for d in DIALECTS {
                let _ = parse(&sql[..end], d);
            }
        }
    }
    check_allocations();
}

#[test]
fn fuzz_sql_mutations() {
    install_panic_hook();
    let mut rng = rng(0x5351_0001);
    for _ in 0..iters(200_000) {
        let sql = hostile_sql(&mut rng);
        let d = DIALECTS[rng.below(DIALECTS.len())];
        parse_everything(&sql, d);
    }
    check_allocations();
}

#[test]
fn fuzz_sql_random_text() {
    install_panic_hook();
    let mut rng = rng(0x5351_0002);
    let alphabet: Vec<&str> = KEYWORDS
        .iter()
        .chain(OPERATORS)
        .chain(EDGE_NUMBERS)
        .chain(UNICODE)
        .chain(&[" ", " ", "a", "t.b", "1", "'x'", "\"q\"", "[i]", "f("])
        .copied()
        .collect();
    for _ in 0..iters(150_000) {
        let n = rng.below(40);
        let sep = if rng.below(2) == 0 { " " } else { "" };
        let sql = (0..n)
            .map(|_| alphabet[rng.below(alphabet.len())])
            .collect::<Vec<_>>()
            .join(sep);
        let d = DIALECTS[rng.below(DIALECTS.len())];
        parse_everything(&sql, d);
        let bytes: Vec<u8> = (0..rng.below(64)).map(|_| rng.next_u64() as u8).collect();
        parse_everything(&String::from_utf8_lossy(&bytes), d);
    }
    check_allocations();
}

/// Statements built to exhaust the stack or the clock: each must fail (or pass) quickly.
fn pathological(n: usize) -> Vec<String> {
    let r = |s: &str, k: usize| s.repeat(k);
    vec![
        format!("SELECT {}1{}", r("(", n), r(")", n)),
        format!("SELECT {}true", r("NOT ", n)),
        format!("SELECT {}1", r("-", n)),
        format!("SELECT {}1", r("- ", n)),
        format!("SELECT {}1", r("~", n)),
        format!("SELECT {}1", r("+", n)),
        format!("SELECT {}x", r("BINARY ", n)),
        format!(
            "SELECT {}1{}",
            r("CASE WHEN true THEN ", n / 10),
            r(" END", n / 10)
        ),
        format!("SELECT {}1{}", r("(SELECT ", n / 10), r(")", n / 10)),
        format!("SELECT {}1{}", r("EXISTS (SELECT ", n / 10), r(")", n / 10)),
        format!(
            "SELECT 1 WHERE 1 IN {}1{}",
            r("(SELECT 1 WHERE 1 IN (", n / 10),
            r("))", n / 10)
        ),
        format!("SELECT {}1{}", r("f(", n), r(")", n)),
        format!("SELECT {}1{}", r("ARRAY[", n), r("]", n)),
        format!("SELECT {}1{}", r("CAST(", n), r(" AS int)", n)),
        format!("SELECT {}1{}", r("coalesce(", n), r(", 2)", n)),
        format!("SELECT 1{}", r("+1", n)),
        format!("SELECT 1{}", r(" OR 1=1", n)),
        format!("SELECT 1{}", r(" AND 1", n)),
        format!("SELECT 1{}", r("::int", n)),
        format!("SELECT x{}", r("[1]", n)),
        format!("SELECT 2{}", r("^2", n)),
        format!("SELECT 'a'{}", r(" || 'a'", n)),
        format!("SELECT 1{}", r(" IS NULL", n)),
        format!("SELECT x{}", r(" COLLATE \"C\"", n)),
        format!("SELECT 1{}", r(" BETWEEN 1 AND 2", n)),
        format!("SELECT 1{}", r(" UNION SELECT 1", n)),
        format!("SELECT * FROM t{}", r(" JOIN t ON true", n)),
        format!("SELECT * FROM {}t{}", r("(", n), r(")", n)),
        format!("SELECT * FROM {}SELECT 1{}", r("(", n), r(")", n)),
        format!("SELECT {}1", r("1, ", n)),
        format!("SELECT 1 IN ({}1)", r("1, ", n)),
        format!("INSERT INTO t VALUES {}(1)", r("(1), ", n)),
        format!("{}SELECT 1", r("IF 1=1 ", n)),
        format!("{}SELECT 1{}", r("BEGIN ", n), r(" END", n)),
        format!("{}SELECT 1", r("EXPLAIN ", n)),
        format!("{}1", r("/*", n)),
        format!("SELECT '{}", r("a", n * 10)),
        format!("SELECT $${}", r("a", n * 10)),
        format!("SELECT {}", r("$1", n)),
        format!("SELECT ${}", r("9", n)),
        format!("SELECT {}", r("?", n)),
        format!("SELECT E'{}'", r("\\u", n)),
        format!("SELECT X'{}'", r("f", n)),
        format!("SELECT {}", r("9", n)),
        format!("SELECT 1e{}", r("9", n)),
        format!("SELECT a{}", r(".a", n)),
        r(";", n),
        r("SELECT 1;", n / 10),
    ]
}

#[test]
fn fuzz_sql_pathological_shapes() {
    install_panic_hook();
    for n in [1_000usize, 100_000] {
        for sql in pathological(n) {
            for d in DIALECTS {
                let out = timed(&sql, Duration::from_secs(10), || parse(&sql, d));
                if let Err(e) = &out {
                    assert!(!e.message.is_empty());
                }
                let tail = sql.get(7..).unwrap_or("");
                let _ = timed(tail, Duration::from_secs(10), || parse_expr(tail, d));
            }
        }
    }
    let _ = parse_data_type(&format!("int{}", "[]".repeat(100_000)));
    let _ = parse_data_type(&format!("numeric({})", "9".repeat(10_000)));
    // Deep nesting fails with a clear error rather than overflowing the stack.
    let deep = format!("SELECT {}1{}", "(".repeat(100_000), ")".repeat(100_000));
    let e = parse(&deep, Dialect::Postgres).err().map(|e| e.message);
    assert_eq!(e.as_deref(), Some("statement nested too deeply"));
    check_allocations();
}

#[test]
fn fuzz_sql_reasonable_nesting_still_parses() {
    for sql in [
        format!("SELECT {}1{}", "(".repeat(100), ")".repeat(100)),
        format!("SELECT {}1{}", "(SELECT ".repeat(50), ")".repeat(50)),
        format!("SELECT 1 WHERE 1=1{}", " OR 1=1".repeat(900)),
        format!("SELECT 1{}", " UNION ALL SELECT 1".repeat(900)),
        format!("SELECT {}true", "NOT ".repeat(100)),
    ] {
        for d in DIALECTS {
            assert!(parse(&sql, d).is_ok(), "{d:?}: {}", &sql[..60]);
        }
    }
}

// ── values ──

const VALUES: &[&str] = &[
    "0",
    "-1.5e10",
    "1.50",
    "123456789012345678901234567890.123",
    "1e-40",
    "+.5",
    "NaN",
    "inf",
    "2024-02-29 23:59:59.999999+05:30",
    "1970-01-01T00:00:00Z",
    "2262-04-11 23:47:16.854775807",
    "1677-09-21",
    "9999-12-31",
    "0001-01-01 BC",
    "epoch",
    "now",
    "today",
    "24:00:00",
    "12:30:00.25",
    "-838:59:59",
    "1 day 02:00:00",
    "1 year 2 mons 3 days 04:05:06.5",
    "-1 week",
    "1.5 hours",
    "P1D",
    "{1,\"a b\",NULL,\"x\\\"y\"}",
    "{{1,2},{3,4}}",
    "{}",
    "1 3 5",
    "\\x0102ff",
    "\\x0",
    "\\001\\002",
    "123e4567-e89b-12d3-a456-426614174000",
    "{123e4567e89b12d3a456426614174000}",
    "t",
    "off",
    "yes",
    "[1,2.5,-3e-5]",
    "[]",
    "[NaN]",
    "{\"a\": [1, {\"b\": null}]}",
];

#[test]
fn fuzz_value_parsers() {
    install_panic_hook();
    let mut rng = rng(0x5641_0001);
    let types: Vec<_> = [
        "bool",
        "smallint",
        "int",
        "bigint",
        "numeric",
        "numeric(5,2)",
        "numeric(2,5)",
        "real",
        "double precision",
        "text",
        "varchar(3)",
        "char(2)",
        "bytea",
        "timestamp",
        "timestamptz",
        "date",
        "time",
        "interval",
        "uuid",
        "json",
        "jsonb",
        "int[]",
        "text[]",
        "vector(3)",
        "vector",
    ]
    .iter()
    .filter_map(|t| parse_data_type(t).ok())
    .collect();
    assert!(types.len() >= 24, "{} types", types.len());
    let extremes = [
        "9".repeat(100_000),
        format!("0.{}1", "0".repeat(100_000)),
        "1e2147483647".into(),
        "1e-2147483648".into(),
        "1e99999999999999999999".into(),
        format!("{{{}1{}}}", "{".repeat(10_000), "}".repeat(10_000)),
        format!("{{{}}}", "1,".repeat(100_000)),
        format!("[{}1]", "1,".repeat(100_000)),
        "1e308 years 1e308 days".into(),
        "-9223372036854775808 seconds".into(),
        "9223372036854775807 months 9223372036854775807 months".into(),
        "99999999999-01-01".into(),
        "2024-01-01 12:00:00+99999999999999".into(),
        "-9223372036854775808:00".into(),
        format!("\\x{}", "é".repeat(1000)),
        "\\x0é0".into(),
    ];
    let check = |s: &str| {
        timed(s, Duration::from_secs(5), || {
            let _ = datum::parse_dec(s);
            let _ = datum::parse_timestamp(s);
            let _ = datum::parse_time(s);
            let _ = datum::parse_interval(s);
            let _ = datum::parse_array(s);
            let _ = datum::parse_bytea(s);
            let _ = datum::parse_uuid(s);
            let _ = datum::parse_bool(s);
            let _ = vector::parse(s);
            let text = Datum::Text(s.to_owned());
            for ty in &types {
                if let Ok(v) = datum::cast(&text, ty) {
                    let _ = (v.as_text(), v.to_text(true), datum::json_of(&v));
                    let _ = vector::coerce(&v, Some(3));
                }
            }
        })
    };
    for s in &extremes {
        check(s);
    }
    for _ in 0..iters(100_000) {
        let base = VALUES[rng.below(VALUES.len())];
        let s = match rng.below(4) {
            0 => {
                let other = VALUES[rng.below(VALUES.len())];
                let at = rng.below(base.len() + 1);
                format!("{}{other}", base.get(..at).unwrap_or(base))
            }
            1 => {
                let u = UNICODE[rng.below(UNICODE.len())];
                let at = rng.below(base.len() + 1);
                match (base.get(..at), base.get(at..)) {
                    (Some(a), Some(b)) => format!("{a}{u}{b}"),
                    _ => format!("{base}{u}"),
                }
            }
            2 => {
                let n = EDGE_NUMBERS[rng.below(EDGE_NUMBERS.len())];
                base.replacen(|c: char| c.is_ascii_digit(), n, 1)
            }
            _ => String::from_utf8_lossy(&rng.hostile_copy(base.as_bytes())).into_owned(),
        };
        check(&s);
    }
    for _ in 0..iters(100_000) {
        let v: Vec<f32> = (0..rng.below(6))
            .map(|_| f32::from_bits(rng.next_u64() as u32))
            .collect();
        let raw = rng.hostile_copy(&vector::encode_binary(&v));
        let _ = vector::decode_binary(&raw);
    }
    check_allocations();
}

#[test]
fn fuzz_copy_reader() {
    install_panic_hook();
    let mut rng = rng(0x4350_0001);
    let data: [&[u8]; 4] = [
        b"1\tann\t\\N\n2\tb\\tob\t3.5\n\\.\n",
        b"id,name,score\n1,\"a, \"\"b\"\"\",\n2,,x\n\"multi\nline\",2,3\n",
        b"1;\"x\";NULL\r\n2;\"y\"\"\";\r\n",
        b"\\x\\\\\\0\\101\\n\n",
    ];
    let pick =
        |rng: &mut SweepRng, set: &[char]| (rng.below(3) == 0).then(|| set[rng.below(set.len())]);
    for _ in 0..iters(100_000) {
        let csv = rng.below(2) == 0;
        let stmt = CopyStmt {
            table: Some("t".into()),
            columns: Vec::new(),
            query: None,
            from: true,
            stdio: true,
            csv,
            binary: false,
            header: csv && rng.below(2) == 0,
            delimiter: pick(&mut rng, &[';', '|', '\t', ',', '"']),
            null: (rng.below(3) == 0).then(|| "NULL".into()),
            quote: if csv {
                pick(&mut rng, &['\'', '"', ';'])
            } else {
                None
            },
            escape: if csv {
                pick(&mut rng, &['\\', '"'])
            } else {
                None
            },
        };
        let Ok(fmt) = storage_sql::copy::Format::new(&stmt) else {
            continue;
        };
        let mut reader = storage_sql::copy::Reader::new(fmt);
        let pick_data = rng.below(data.len());
        let input = rng.hostile_copy(data[pick_data]);
        let mut at = 0;
        while at < input.len() {
            let end = (at + 1 + rng.below(8)).min(input.len());
            reader.feed(&input[at..end]);
            at = end;
            while let Ok(Some(_)) = reader.next_record(false) {}
        }
        while let Ok(Some(_)) = reader.next_record(true) {}
    }
    check_allocations();
}

// ── wire level ──

/// The three endpoints over one engine, each with TLS, plus PostgreSQL and MySQL endpoints
/// that require a password.
struct Servers {
    pg: SocketAddr,
    pg_auth: SocketAddr,
    mysql: SocketAddr,
    mysql_auth: SocketAddr,
    tds: SocketAddr,
}

fn spawn_server(f: impl FnOnce(TcpListener) + Send + 'static) -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").expect("listen");
    let addr = l.local_addr().expect("address");
    std::thread::spawn(move || f(l));
    addr
}

/// One engine with a few small tables, shared by the wire and execution sweeps.
fn engine() -> &'static Arc<Engine> {
    static ENGINE: OnceLock<Arc<Engine>> = OnceLock::new();
    ENGINE.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("storine_sql_fuzz_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let config = EngineConfig {
            lock_timeout: Duration::from_millis(100),
            ..EngineConfig::default()
        };
        let db = embedded::start(&dir, config).expect("embedded server");
        let engine: Arc<Engine> = Arc::clone(&db.engine);
        // The document server lives as long as the test process.
        std::mem::forget(db);
        let mut s = engine.new_session(Dialect::Postgres, "fuzz", None);
        for sql in [
            "CREATE TABLE users (id SERIAL PRIMARY KEY, email TEXT UNIQUE, name TEXT, age INT, created TIMESTAMPTZ DEFAULT now())",
            "CREATE TABLE posts (id SERIAL PRIMARY KEY, user_id INT, title TEXT, score NUMERIC(10,2))",
            "CREATE TABLE t (id INT PRIMARY KEY, a INT, b TEXT, c NUMERIC(10,2), d DATE, e BYTEA, f JSONB)",
            "INSERT INTO users (email, name, age) VALUES ('a@x', 'Ann', 30), ('b@x', 'Bob', 25)",
            "INSERT INTO t VALUES (1, 1, 'x', 1.5, '2024-01-01', '\\x01', '{\"k\": 1}'), (2, NULL, NULL, NULL, NULL, NULL, NULL)",
        ] {
            let (stmts, _) = parse(sql, Dialect::Postgres).expect("setup SQL");
            s.execute(&stmts[0], &[]).expect("setup");
        }
        engine
    })
}

fn servers() -> &'static Servers {
    static SERVERS: OnceLock<Servers> = OnceLock::new();
    SERVERS.get_or_init(|| {
        let engine = engine();
        let tls = Some(Arc::new(
            storage_sql::tls::TlsConfig::self_signed(&["localhost"]).expect("certificate"),
        ));
        let users: std::collections::HashMap<String, String> =
            [("app".to_owned(), "pw".to_owned())].into_iter().collect();
        let pg = |users| {
            let engine = Arc::clone(engine);
            let auth = Arc::new(storage_sql::pg::Auth {
                users,
                tls: tls.clone(),
                require_tls: false,
            });
            spawn_server(move |l| storage_sql::pg::serve(engine, l, auth))
        };
        let mysql = |users| {
            let engine = Arc::clone(engine);
            let auth = Arc::new(storage_sql::mysql::Auth {
                users,
                tls: tls.clone(),
                require_tls: false,
            });
            spawn_server(move |l| storage_sql::mysql::serve(engine, l, auth))
        };
        let tds = {
            let engine = Arc::clone(engine);
            let auth = Arc::new(storage_sql::tds::Auth {
                users: Default::default(),
                oidc: None,
                kerberos: None,
                tls: tls.clone(),
                require_tls: false,
            });
            spawn_server(move |l| storage_sql::tds::serve(engine, l, auth))
        };
        Servers {
            pg: pg(Default::default()),
            pg_auth: pg(users.clone()),
            mysql: mysql(Default::default()),
            mysql_auth: mysql(users),
            tds,
        }
    })
}

fn connect(addr: SocketAddr) -> TcpStream {
    let s = TcpStream::connect(addr).expect("connect");
    s.set_read_timeout(Some(Duration::from_secs(5)))
        .expect("timeout");
    s.set_write_timeout(Some(Duration::from_secs(5)))
        .expect("timeout");
    let _ = s.set_nodelay(true);
    s
}

/// Sends `bytes`, closes the sending side and reads until the server closes or goes quiet:
/// the server must survive whatever it was sent.
fn send_and_drain(mut s: TcpStream, bytes: &[u8]) {
    let _ = s.set_read_timeout(Some(Duration::from_millis(500)));
    let _ = s.write_all(bytes);
    let _ = s.shutdown(Shutdown::Write);
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut buf = [0u8; 16 * 1024];
    while Instant::now() < deadline {
        match s.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
    }
}

/// Words that make a statement slow or memory-hungry by design rather than by a bug; the
/// wire sweeps leave them out (`statements_that_ask_for_huge_results_fail_cleanly` checks
/// their limits).
fn too_costly(sql: &str) -> bool {
    let l = sql.to_ascii_lowercase();
    [
        "generate_series",
        "recursive",
        "repeat",
        "replicate",
        "space",
        "lpad",
        "rpad",
        "lock",
        "sleep",
        "waitfor",
        "copy",
    ]
    .iter()
    .any(|w| l.contains(w))
}

fn wire_sql(rng: &mut SweepRng) -> String {
    loop {
        let sql = hostile_sql(rng);
        if !too_costly(&sql) {
            return sql;
        }
    }
}

/// Bytes with a few hostile edits, length-like fields included.
fn hostile_bytes(rng: &mut SweepRng, base: &[u8]) -> Vec<u8> {
    let mut v = rng.hostile_copy(base);
    if v.len() >= 4 && rng.below(2) == 0 {
        let at = rng.below(v.len() - 3);
        let real = u32::from_be_bytes([v[at], v[at + 1], v[at + 2], v[at + 3]]);
        let e = rng.edge_u32(real);
        if rng.below(2) == 0 {
            v[at..at + 4].copy_from_slice(&e.to_be_bytes());
        } else {
            v[at..at + 4].copy_from_slice(&e.to_le_bytes());
        }
    }
    v
}

fn v8(b: &[u8]) -> Vec<u8> {
    [&[b.len() as u8][..], b].concat()
}

fn v16(b: &[u8]) -> Vec<u8> {
    [&(b.len() as u16).to_be_bytes()[..], b].concat()
}

/// A TLS ClientHello record (TLS 1.3 offer with an x25519 share).
fn client_hello() -> Vec<u8> {
    let ext = |k: u16, d: &[u8]| [&k.to_be_bytes()[..], &v16(d)].concat();
    let share = storage_codec::curve25519::x25519_public(&[9; 32]);
    let exts = [
        ext(0x002b, &v8(&[3, 4, 3, 3])),
        ext(0x000a, &v16(&[0, 0x1d])),
        ext(0x000d, &v16(&[8, 7])),
        ext(0x0033, &v16(&[&[0, 0x1d][..], &v16(&share)].concat())),
        ext(0x0010, &v16(&v8(b"postgresql"))),
    ]
    .concat();
    let body = [
        &[3, 3][..],
        &[7; 32],
        &v8(&[1; 32]),
        &v16(&[0x13, 0x03, 0xcc, 0xa9]),
        &v8(&[0]),
        &v16(&exts),
    ]
    .concat();
    let len = (body.len() as u32).to_be_bytes();
    let msg = [&[1u8][..], &len[1..], &body].concat();
    [&[22u8, 3, 1][..], &(msg.len() as u16).to_be_bytes(), &msg].concat()
}

// PostgreSQL client.

fn pg_msg(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut m = vec![tag];
    m.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    m.extend_from_slice(body);
    m
}

fn pg_startup(user: &str) -> Vec<u8> {
    let mut body = 196_608i32.to_be_bytes().to_vec();
    for (k, v) in [("user", user), ("application_name", "fuzz")] {
        body.extend_from_slice(k.as_bytes());
        body.push(0);
        body.extend_from_slice(v.as_bytes());
        body.push(0);
    }
    body.push(0);
    let mut p = ((body.len() + 4) as i32).to_be_bytes().to_vec();
    p.extend_from_slice(&body);
    p
}

/// Reads backend messages until ReadyForQuery: the DataRow first fields and the last error.
fn pg_until_ready(s: &mut TcpStream) -> std::io::Result<(Vec<String>, Option<String>)> {
    let mut fields = Vec::new();
    let mut error = None;
    loop {
        let mut head = [0u8; 5];
        s.read_exact(&mut head)?;
        let len = i32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
        let mut body = vec![0u8; len.saturating_sub(4)];
        s.read_exact(&mut body)?;
        match head[0] {
            b'Z' => return Ok((fields, error)),
            b'E' => error = Some(String::from_utf8_lossy(&body).into_owned()),
            b'D' if body.len() >= 6 => {
                let n = i32::from_be_bytes([body[2], body[3], body[4], body[5]]);
                if n >= 0 {
                    let end = (6 + n as usize).min(body.len());
                    fields.push(String::from_utf8_lossy(&body[6..end]).into_owned());
                }
            }
            _ => {}
        }
    }
}

fn pg_session(addr: SocketAddr) -> std::io::Result<TcpStream> {
    let mut s = connect(addr);
    s.write_all(&pg_startup("fuzz"))?;
    pg_until_ready(&mut s)?;
    Ok(s)
}

fn pg_query(s: &mut TcpStream, sql: &str) -> (Vec<String>, Option<String>) {
    let mut body = sql.as_bytes().to_vec();
    body.push(0);
    s.write_all(&pg_msg(b'Q', &body)).expect("query");
    pg_until_ready(s).expect("reply")
}

fn pg_healthy(addr: SocketAddr) {
    let mut s = pg_session(addr).expect("postgres endpoint answers");
    assert_eq!(pg_query(&mut s, "SELECT 1"), (vec!["1".to_owned()], None));
}

/// One extended-protocol round with hostile statement text, parameters and formats.
fn pg_extended(rng: &mut SweepRng, out: &mut Vec<u8>) {
    let sql = wire_sql(rng);
    let mut p = b"s\0".to_vec();
    p.extend_from_slice(sql.replace('\0', "").as_bytes());
    p.push(0);
    let ntypes = rng.below(4);
    p.extend_from_slice(&(ntypes as i16).to_be_bytes());
    let oids = [
        0u32, 16, 17, 20, 21, 23, 25, 700, 701, 1082, 1083, 1114, 1184, 1700, 2950, 1007, 3802,
    ];
    for _ in 0..ntypes {
        p.extend_from_slice(&oids[rng.below(oids.len())].to_be_bytes());
    }
    out.extend_from_slice(&pg_msg(b'P', &p));
    let mut b = b"\0s\0".to_vec();
    let nf = rng.below(3);
    b.extend_from_slice(&(nf as i16).to_be_bytes());
    for _ in 0..nf {
        b.extend_from_slice(&(rng.below(2) as i16).to_be_bytes());
    }
    let np = rng.below(4);
    b.extend_from_slice(&(np as i16).to_be_bytes());
    let samples: [&[u8]; 8] = [
        b"1",
        b"\x00\x00\x00\x07",
        b"\x7f\xff\xff\xff\xff\xff\xff\xff",
        b"2024-01-01",
        b"\x00\x01\x00\x00\x00\x00\x00\x02\x00\x01",
        b"abc",
        b"\xff\xfe",
        b"",
    ];
    for _ in 0..np {
        let v = samples[rng.below(samples.len())];
        if rng.below(5) == 0 {
            b.extend_from_slice(&(-1i32).to_be_bytes());
        } else {
            b.extend_from_slice(&(v.len() as i32).to_be_bytes());
            b.extend_from_slice(v);
        }
    }
    b.extend_from_slice(&1i16.to_be_bytes());
    b.extend_from_slice(&(rng.below(2) as i16).to_be_bytes());
    out.extend_from_slice(&pg_msg(b'B', &b));
    let describe: &[u8] = if rng.below(2) == 0 { b"P\0" } else { b"Ss\0" };
    out.extend_from_slice(&pg_msg(b'D', describe));
    out.extend_from_slice(&pg_msg(b'E', &[0, 0, 0, 0, rng.below(3) as u8]));
    if rng.below(4) == 0 {
        out.extend_from_slice(&pg_msg(b'C', b"Ss\0"));
    }
    out.extend_from_slice(&pg_msg(b'S', b""));
}

fn pg_fuzz_session(rng: &mut SweepRng, addr: SocketAddr, auth_addr: SocketAddr) {
    let s = match rng.below(8) {
        // The handshake itself: SSLRequest and a hostile ClientHello, or a hostile startup.
        0 => {
            let mut s = connect(addr);
            let _ = s.write_all(&[0, 0, 0, 8, 4, 0xd2, 0x16, 0x2f]);
            let mut answer = [0u8; 1];
            if s.read_exact(&mut answer).is_ok() && answer[0] == b'S' {
                let hello = hostile_bytes(rng, &client_hello());
                send_and_drain(s, &hello);
            }
            return;
        }
        // A hostile startup and SASL exchange (the endpoint with passwords).
        1 => {
            let mut bytes = hostile_bytes(rng, &pg_startup("app"));
            bytes.extend_from_slice(&hostile_bytes(
                rng,
                &pg_msg(
                    b'p',
                    b"SCRAM-SHA-256\0\0\0\0\x1bn,,n=app,r=abcdefghijklmnop",
                ),
            ));
            send_and_drain(connect(auth_addr), &bytes);
            return;
        }
        _ => match pg_session(addr) {
            Ok(s) => s,
            Err(_) => return,
        },
    };
    let mut bytes = Vec::new();
    for _ in 0..1 + rng.below(12) {
        match rng.below(10) {
            0..=3 => {
                let mut body = wire_sql(rng).into_bytes();
                body.push(0);
                bytes.extend_from_slice(&pg_msg(b'Q', &body));
            }
            4 | 5 => pg_extended(rng, &mut bytes),
            6 => {
                // A COPY with hostile data.
                let copy: &[u8] = if rng.below(2) == 0 {
                    b"COPY t (id, a, b) FROM STDIN\0"
                } else {
                    b"COPY t FROM STDIN (FORMAT binary)\0"
                };
                bytes.extend_from_slice(&pg_msg(b'Q', copy));
                let data =
                    rng.hostile_copy(b"PGCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0\0\x03\0\0\0\x0130\n");
                bytes.extend_from_slice(&pg_msg(b'd', &data));
                let end = if rng.below(4) == 0 { b'f' } else { b'c' };
                bytes.extend_from_slice(&pg_msg(end, b""));
            }
            7 => {
                let mut m = Vec::new();
                pg_extended(rng, &mut m);
                bytes.extend_from_slice(&hostile_bytes(rng, &m));
            }
            8 => {
                let tag = b"QPBDECSHXpdcfF?"[rng.below(15)];
                let body: Vec<u8> = (0..rng.below(32)).map(|_| rng.next_u64() as u8).collect();
                let mut m = pg_msg(tag, &body);
                if rng.below(2) == 0 {
                    let e = rng.edge_u32(body.len() as u32 + 4);
                    m[1..5].copy_from_slice(&e.to_be_bytes());
                }
                bytes.extend_from_slice(&m);
            }
            _ => bytes.extend((0..rng.below(64)).map(|_| rng.next_u64() as u8)),
        }
    }
    send_and_drain(s, &bytes);
}

// MySQL client.

fn my_packet(seq: u8, payload: &[u8]) -> Vec<u8> {
    let n = payload.len();
    let mut p = vec![n as u8, (n >> 8) as u8, (n >> 16) as u8, seq];
    p.extend_from_slice(payload);
    p
}

fn my_read(s: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut h = [0u8; 4];
    s.read_exact(&mut h)?;
    let n = usize::from(h[0]) | (usize::from(h[1]) << 8) | (usize::from(h[2]) << 16);
    let mut p = vec![0u8; n];
    s.read_exact(&mut p)?;
    Ok(p)
}

/// PROTOCOL_41, SECURE_CONNECTION, CONNECT_WITH_DB, MULTI_STATEMENTS / RESULTS, PLUGIN_AUTH,
/// PLUGIN_AUTH_LENENC, DEPRECATE_EOF.
const MY_CAPS: u32 = 0x200 | 0x8000 | 0x8 | 0x1_0000 | 0x2_0000 | 0x8_0000 | 0x20_0000 | 0x100_0000;

fn my_login(user: &str, caps: u32) -> Vec<u8> {
    let mut r = caps.to_le_bytes().to_vec();
    r.extend_from_slice(&(1u32 << 24).to_le_bytes());
    r.push(255);
    r.extend_from_slice(&[0; 23]);
    r.extend_from_slice(user.as_bytes());
    r.push(0);
    r.push(0); // empty auth response (length-encoded)
    r.extend_from_slice(b"storine\0mysql_native_password\0");
    r
}

fn my_session(addr: SocketAddr) -> std::io::Result<TcpStream> {
    let mut s = connect(addr);
    my_read(&mut s)?;
    s.write_all(&my_packet(1, &my_login("root", MY_CAPS)))?;
    let ok = my_read(&mut s)?;
    if ok.first() != Some(&0) {
        return Err(std::io::Error::other("login refused"));
    }
    Ok(s)
}

fn my_healthy(addr: SocketAddr) {
    let mut s = my_session(addr).expect("mysql endpoint answers");
    s.write_all(&my_packet(0, b"\x03SELECT 1")).expect("query");
    let mut packets = Vec::new();
    loop {
        let p = my_read(&mut s).expect("reply");
        let end = p.first() == Some(&0xfe) && p.len() < 9;
        packets.push(p);
        if end {
            break;
        }
    }
    // Column count, column definition, the row "1", OK/EOF.
    assert_eq!(packets.len(), 4, "{packets:?}");
    assert_eq!(packets[2], b"\x011");
}

fn my_fuzz_session(rng: &mut SweepRng, addr: SocketAddr, auth_addr: SocketAddr) {
    match rng.below(8) {
        0 => {
            // SSL request, then a hostile ClientHello.
            let mut s = connect(addr);
            if my_read(&mut s).is_ok() {
                let request = [&(MY_CAPS | 0x800).to_le_bytes()[..], &[0; 28]].concat();
                let _ = s.write_all(&my_packet(1, &request));
                send_and_drain(s, &hostile_bytes(rng, &client_hello()));
            }
            return;
        }
        1 => {
            let mut s = connect(if rng.below(2) == 0 { addr } else { auth_addr });
            if my_read(&mut s).is_ok() {
                let mut bytes = hostile_bytes(rng, &my_packet(1, &my_login("app", MY_CAPS)));
                bytes.extend_from_slice(&my_packet(3, &[7; 20]));
                send_and_drain(s, &bytes);
            }
            return;
        }
        _ => {}
    }
    let Ok(s) = my_session(addr) else { return };
    let mut bytes = Vec::new();
    for _ in 0..1 + rng.below(12) {
        let payload: Vec<u8> = match rng.below(10) {
            0..=3 => [&[0x03][..], wire_sql(rng).as_bytes()].concat(),
            4 => [&[0x16][..], wire_sql(rng).as_bytes()].concat(),
            5 => {
                // COM_STMT_EXECUTE of a statement prepared earlier in the session (ids from 1).
                let mut p = vec![0x17];
                p.extend_from_slice(&(1 + rng.below(3) as u32).to_le_bytes());
                p.push(0);
                p.extend_from_slice(&1u32.to_le_bytes());
                let n = rng.below(4);
                p.extend_from_slice(&vec![0u8; n.div_ceil(8)]);
                p.push(1);
                let types = [
                    1u8, 2, 3, 4, 5, 6, 7, 8, 0x0a, 0x0b, 0x0c, 0x0f, 0xf6, 0xfc, 0xfd, 0xfe, 0x42,
                ];
                let picked: Vec<u8> = (0..n).map(|_| types[rng.below(types.len())]).collect();
                for t in &picked {
                    p.extend_from_slice(&[*t, if rng.below(2) == 0 { 0x80 } else { 0 }]);
                }
                for _ in &picked {
                    let k = rng.below(16);
                    p.push(k as u8);
                    p.extend((0..k).map(|_| rng.next_u64() as u8));
                }
                hostile_bytes(rng, &p)
            }
            6 => {
                let cmds = [
                    0x02u8, 0x04, 0x09, 0x0e, 0x18, 0x19, 0x1a, 0x1b, 0x1f, 0x11, 0x99,
                ];
                let mut p = vec![cmds[rng.below(cmds.len())]];
                p.extend((0..rng.below(16)).map(|_| rng.next_u64() as u8));
                p
            }
            7 => {
                let query = [&[0x03][..], wire_sql(rng).as_bytes()].concat();
                hostile_bytes(rng, &query)
            }
            8 => {
                // A packet header claiming more than is sent.
                let mut p = my_packet(0, b"\x03SELECT 1");
                let e = rng.edge_u32(9) & 0xff_ffff;
                p[..3].copy_from_slice(&e.to_le_bytes()[..3]);
                bytes.extend_from_slice(&p);
                continue;
            }
            _ => (0..rng.below(64)).map(|_| rng.next_u64() as u8).collect(),
        };
        bytes.extend_from_slice(&my_packet(0, &payload));
    }
    send_and_drain(s, &bytes);
}

// SQL Server client.

fn tds_packet(kind: u8, payload: &[u8]) -> Vec<u8> {
    let mut p = vec![kind, 1];
    p.extend_from_slice(&((payload.len() + 8) as u16).to_be_bytes());
    p.extend_from_slice(&[0, 0, 1, 0]);
    p.extend_from_slice(payload);
    p
}

fn utf16(s: &str) -> Vec<u8> {
    s.encode_utf16().flat_map(u16::to_le_bytes).collect()
}

/// PRELOGIN with VERSION and ENCRYPTION options.
fn tds_prelogin(encryption: u8) -> Vec<u8> {
    let mut p = vec![0x00, 0, 11, 0, 6, 0x01, 0, 17, 0, 1, 0xff];
    p.extend_from_slice(&[16, 0, 0x03, 0xe8, 0, 0, encryption]);
    p
}

fn tds_login7() -> Vec<u8> {
    let mut b = vec![0u8; 94];
    b[4..8].copy_from_slice(&0x7400_0004u32.to_le_bytes());
    b[8..12].copy_from_slice(&4096u32.to_le_bytes());
    for (at, s) in [(40usize, "fuzz"), (68, "storine")] {
        let off = b.len() as u16;
        b[at..at + 2].copy_from_slice(&off.to_le_bytes());
        b[at + 2..at + 4].copy_from_slice(&(s.len() as u16).to_le_bytes());
        b.extend_from_slice(&utf16(s));
    }
    let len = b.len() as u32;
    b[0..4].copy_from_slice(&len.to_le_bytes());
    b
}

/// Reads one TDS response message (packets until end-of-message).
fn tds_read(s: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut out = Vec::new();
    loop {
        let mut h = [0u8; 8];
        s.read_exact(&mut h)?;
        let len = usize::from(u16::from_be_bytes([h[2], h[3]])).saturating_sub(8);
        let mut p = vec![0u8; len];
        s.read_exact(&mut p)?;
        out.extend_from_slice(&p);
        if h[1] & 1 != 0 {
            return Ok(out);
        }
    }
}

fn tds_session(addr: SocketAddr) -> std::io::Result<TcpStream> {
    let mut s = connect(addr);
    s.write_all(&tds_packet(0x12, &tds_prelogin(2)))?;
    tds_read(&mut s)?;
    s.write_all(&tds_packet(0x10, &tds_login7()))?;
    let reply = tds_read(&mut s)?;
    if !reply.contains(&0xad) {
        return Err(std::io::Error::other("login refused"));
    }
    Ok(s)
}

/// ALL_HEADERS (transaction descriptor) and the batch text.
fn tds_batch(sql: &str) -> Vec<u8> {
    let mut p = Vec::new();
    p.extend_from_slice(&22u32.to_le_bytes());
    p.extend_from_slice(&18u32.to_le_bytes());
    p.extend_from_slice(&2u16.to_le_bytes());
    p.extend_from_slice(&[0; 8]);
    p.extend_from_slice(&1u32.to_le_bytes());
    p.extend_from_slice(&utf16(sql));
    p
}

fn tds_healthy(addr: SocketAddr) {
    let mut s = tds_session(addr).expect("tds endpoint answers");
    s.write_all(&tds_packet(0x01, &tds_batch("SELECT 1")))
        .expect("batch");
    let reply = tds_read(&mut s).expect("reply");
    assert!(reply.contains(&0xd1) && !reply.contains(&0xaa), "{reply:?}");
}

/// `sp_executesql` with hostile statement text and two parameters.
fn tds_rpc(rng: &mut SweepRng) -> Vec<u8> {
    let mut p = tds_batch("");
    p.extend_from_slice(&0xffffu16.to_le_bytes());
    p.extend_from_slice(&[10, 0, 0, 0]);
    let nvarchar = [0xe7, 0x40, 0x1f, 0x09, 0x04, 0xd0, 0x00, 0x34];
    for text in [wire_sql(rng), "@p1 int, @p2 datetime2".to_owned()] {
        let u = utf16(&text);
        p.extend_from_slice(&[0, 0]);
        p.extend_from_slice(&nvarchar);
        p.extend_from_slice(&(u.len().min(8000) as u16).to_le_bytes());
        p.extend_from_slice(&u[..u.len().min(8000)]);
    }
    p.extend_from_slice(&[3, b'@', 0, b'p', 0, b'1', 0, 0, 0x26, 4, 4, 7, 0, 0, 0]);
    p.extend_from_slice(&[
        3, b'@', 0, b'p', 0, b'2', 0, 0, 0x2a, 7, 8, 1, 2, 3, 4, 5, 0xff, 0xff, 0xff,
    ]);
    p
}

fn tds_fuzz_session(rng: &mut SweepRng, addr: SocketAddr) {
    match rng.below(8) {
        0 => {
            // TLS bare (TDS 8 strict) or inside PRELOGIN packets (TDS 7.x).
            let mut s = connect(addr);
            if rng.below(2) == 0 {
                send_and_drain(s, &hostile_bytes(rng, &client_hello()));
            } else if s.write_all(&tds_packet(0x12, &tds_prelogin(1))).is_ok()
                && tds_read(&mut s).is_ok()
            {
                let hello = hostile_bytes(rng, &client_hello());
                send_and_drain(s, &tds_packet(0x12, &hello));
            }
            return;
        }
        1 => {
            let mut bytes = hostile_bytes(rng, &tds_packet(0x12, &tds_prelogin(2)));
            bytes.extend_from_slice(&hostile_bytes(rng, &tds_packet(0x10, &tds_login7())));
            send_and_drain(connect(addr), &bytes);
            return;
        }
        _ => {}
    }
    let Ok(s) = tds_session(addr) else { return };
    let mut bytes = Vec::new();
    for _ in 0..1 + rng.below(10) {
        match rng.below(9) {
            0..=3 => bytes.extend_from_slice(&tds_packet(0x01, &tds_batch(&wire_sql(rng)))),
            4 => bytes.extend_from_slice(&tds_packet(0x03, &tds_rpc(rng))),
            5 => {
                let rpc = tds_rpc(rng);
                bytes.extend_from_slice(&tds_packet(0x03, &hostile_bytes(rng, &rpc)));
            }
            6 => {
                // Transaction manager requests: BEGIN, COMMIT, ROLLBACK, SAVE, unknown.
                let mut t = tds_batch("");
                t.extend_from_slice(&[[5u8, 7, 8, 9, 3][rng.below(5)], 0, 2, 3]);
                t.extend((0..rng.below(16)).map(|_| rng.next_u64() as u8));
                bytes.extend_from_slice(&tds_packet(0x0e, &t));
            }
            7 => {
                let kind = [0x01u8, 0x03, 0x06, 0x0e, 0x12, 0x10, 0x07][rng.below(7)];
                let body: Vec<u8> = (0..rng.below(64)).map(|_| rng.next_u64() as u8).collect();
                let mut p = tds_packet(kind, &body);
                if rng.below(2) == 0 {
                    p[1] = 0; // not the last packet of the message
                } else {
                    let e = rng.edge_u32(p.len() as u32) as u16;
                    p[2..4].copy_from_slice(&e.to_be_bytes());
                }
                bytes.extend_from_slice(&p);
            }
            _ => {
                let batch = hostile_bytes(rng, &tds_batch("SELECT 1"));
                bytes.extend_from_slice(&tds_packet(0x01, &batch));
            }
        }
    }
    send_and_drain(s, &bytes);
}

/// Hostile sessions on four client threads; every 25 sessions per thread a fresh, valid
/// connection must still get `SELECT 1` answered, and nothing may have panicked.
fn wire_sweep(tag: u64, session: impl Fn(&mut SweepRng) + Sync, healthy: impl Fn() + Sync) {
    install_panic_hook();
    healthy();
    let per_thread = iters(2_000).div_ceil(4);
    std::thread::scope(|scope| {
        for t in 0..4 {
            let (session, healthy) = (&session, &healthy);
            scope.spawn(move || {
                let mut rng = rng(tag + t);
                for i in 0..per_thread {
                    session(&mut rng);
                    if i % 25 == 24 {
                        healthy();
                        assert_no_panics();
                    }
                }
            });
        }
    });
    healthy();
    assert_no_panics();
    check_allocations();
}

#[test]
fn fuzz_wire_postgres() {
    let s = servers();
    wire_sweep(
        0x5747_0000,
        |rng| pg_fuzz_session(rng, s.pg, s.pg_auth),
        || pg_healthy(s.pg),
    );
}

#[test]
fn fuzz_wire_mysql() {
    let s = servers();
    wire_sweep(
        0x5747_1000,
        |rng| my_fuzz_session(rng, s.mysql, s.mysql_auth),
        || my_healthy(s.mysql),
    );
}

#[test]
fn fuzz_wire_sql_server() {
    let s = servers();
    wire_sweep(
        0x5747_2000,
        |rng| tds_fuzz_session(rng, s.tds),
        || tds_healthy(s.tds),
    );
}

#[test]
fn wire_length_fields_do_not_allocate_ahead_of_the_data() {
    let s = servers();
    install_panic_hook();
    // Each claims a body far larger than what follows, then the client hangs up.
    let mut pg = pg_session(s.pg).expect("session");
    let mut q = vec![b'Q'];
    q.extend_from_slice(&((1i32 << 30) - 1).to_be_bytes());
    let _ = pg.write_all(&q);
    send_and_drain(pg, b"SELECT 1");
    let my = my_session(s.mysql).expect("session");
    send_and_drain(my, &[0xff, 0xff, 0xff, 0, 3]);
    let tds = tds_session(s.tds).expect("session");
    send_and_drain(tds, &[0x01, 0, 0xff, 0xff, 0, 0, 1, 0]);
    pg_healthy(s.pg);
    my_healthy(s.mysql);
    tds_healthy(s.tds);
    assert_no_panics();
    check_allocations();
}

#[test]
fn statements_that_ask_for_huge_results_fail_cleanly() {
    let s = servers();
    install_panic_hook();
    let mut c = pg_session(s.pg).expect("session");
    for sql in [
        "SELECT lpad('a', 9223372036854775807)",
        "SELECT rpad('a', 2147483647, 'xy')",
        "SELECT repeat('ab', 9223372036854775807)",
        "SELECT repeat(repeat('x', 1000000), 1000000)",
        "SELECT now() + interval '100000000000 years'",
        "SELECT '2262-04-11'::timestamp + interval '1 year'",
        "SELECT CAST(123.5 AS numeric(2,5))",
        "SELECT '-9223372036854775808 seconds'::interval - '1 second'::interval",
    ] {
        let (rows, error) = pg_query(&mut c, sql);
        assert!(error.is_some(), "{sql}: {rows:?}");
    }
    let (rows, error) = pg_query(
        &mut c,
        "SELECT count(*) FROM generate_series(9223372036854775806, 9223372036854775807)",
    );
    assert_eq!((rows, error), (vec!["2".to_owned()], None));
    let mut c = tds_session(s.tds).expect("session");
    for sql in [
        "SELECT SPACE(9223372036854775807)",
        "SELECT REPLICATE('ab', 4611686018427387904)",
    ] {
        c.write_all(&tds_packet(0x01, &tds_batch(sql)))
            .expect("batch");
        let reply = tds_read(&mut c).expect("reply");
        assert!(reply.contains(&0xaa), "{sql}: no error");
    }
    pg_healthy(s.pg);
    assert_no_panics();
    check_allocations();
}

// ── execution ──

/// Every lower-case word quoted in the function modules: the built-in function names (and
/// some other words, which are simply unknown functions).
fn function_names() -> Vec<String> {
    let mut names: Vec<String> = [
        include_str!("../src/exec/func.rs"),
        include_str!("../src/exec/mssql.rs"),
    ]
    .iter()
    .flat_map(|src| rust_strings(src))
    .filter(|w| {
        w.len() >= 2
            && w.bytes()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_')
            && !w.as_bytes()[0].is_ascii_digit()
            && !too_costly(w)
    })
    .collect();
    names.sort();
    names.dedup();
    names
}

const ARGS: &[&str] = &[
    "NULL",
    "0",
    "1",
    "-1",
    "2",
    "7",
    "2147483647",
    "-2147483648",
    "9223372036854775807",
    "-9223372036854775808",
    "99999999999999999999999999999999999999",
    "1.5",
    "-0.000001",
    "1e308",
    "-1e308",
    "CAST('NaN' AS double precision)",
    "''",
    "'abc'",
    "'é日本'",
    "'%s %I %L %1$s %'",
    "'a,b,,c'",
    "'[0-9]+'",
    "'(a|'",
    "'g'",
    "'day'",
    "'epoch'",
    "'yyyy-mm-dd hh24:mi:ss.ff'",
    "'%Y-%m-%d %H:%i:%s'",
    "'2024-02-29'",
    "'9999-12-31'",
    "'0001-01-01'",
    "'2262-04-11 23:47:16'",
    "'1677-09-21 00:12:44'",
    "CAST('2024-01-31 10:00:00' AS timestamp)",
    "CAST('2024-01-31' AS date)",
    "CAST('12:30:00' AS time)",
    "CAST('1 day 02:00:00' AS interval)",
    "CAST('100000000 years' AS interval)",
    "'{\"a\": [1, 2, {\"b\": null}]}'",
    "'{1,2,3}'",
    "'[1,2,3]'",
    "'$.a[1]'",
    "'{a,1}'",
    "'123e4567-e89b-12d3-a456-426614174000'",
    "true",
    "false",
];

#[test]
fn fuzz_function_calls() {
    install_panic_hook();
    let engine = engine();
    let names = function_names();
    assert!(names.len() > 200, "{} function names", names.len());
    let mut rng = rng(0x4643_0001);
    let mut session = engine.new_session(Dialect::Postgres, "fuzz", None);
    for i in 0..iters(30_000) {
        if i % 500 == 0 {
            let d = DIALECTS[rng.below(DIALECTS.len())];
            session = engine.new_session(d, "fuzz", None);
        }
        let name = &names[rng.below(names.len())];
        let args: Vec<&str> = (0..rng.below(9))
            .map(|_| ARGS[rng.below(ARGS.len())])
            .collect();
        let sql = match rng.below(4) {
            // As an aggregate over a few rows.
            0 => format!(
                "SELECT {name}(x) FROM (VALUES ({}), ({}), (NULL)) AS v(x)",
                ARGS[rng.below(ARGS.len())],
                ARGS[rng.below(ARGS.len())]
            ),
            1 => format!(
                "SELECT {name}({}) FROM t",
                ["a", "b", "c", "d", "e", "f", "id"][rng.below(7)]
            ),
            _ => format!("SELECT {name}({})", args.join(", ")),
        };
        let Ok((stmts, _)) = parse(&sql, session.dialect) else {
            continue;
        };
        let run = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            timed(&sql, Duration::from_secs(10), || {
                for st in &stmts {
                    let _ = session.execute(st, &[]);
                }
            })
        }));
        assert!(run.is_ok(), "{:?}: {sql}", session.dialect);
    }
    check_allocations();
}
