//! Dialect-aware SQL tokenizer.
//!
//! Handles each dialect's identifier quoting (`"pg"`, `` `mysql` ``, `[tsql]`), string literals
//! (`'..'` with `''`, PostgreSQL `E'..'` and `$tag$..$tag$`, MySQL backslash escapes and `".."`
//! strings, `N'..'`, `X'..'` blobs), comments (`--`, `#` in MySQL, nested `/* */`) and
//! placeholders (`$1`, `?`, `:name`, `@name`), which are numbered in order of first appearance.

use crate::error::{SqlError, SqlResult};

/// The SQL dialect a client speaks. It changes quoting, placeholders and a few keywords.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    Postgres,
    MySql,
    Sqlite,
    MsSql,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    /// An identifier or keyword. Unquoted words are kept as written (`quoted == false`).
    Word { text: String, quoted: bool },
    /// A numeric literal, exactly as written.
    Number(String),
    /// A string literal (escapes resolved).
    Str(String),
    /// A hexadecimal / binary literal (`X'..'`, `0x..`).
    Blob(Vec<u8>),
    /// A placeholder, numbered from 0.
    Param(usize),
    /// A MySQL user variable `@name` or system variable `@@name` (name without the `@`s).
    Variable { name: String, system: bool },
    /// Punctuation or an operator: `( ) , ; . * + - / % = <> != < <= > >= || :: ^ ~ & | << >> -> ->> [ ]` ...
    Op(&'static str),
}

impl Token {
    /// The lower-cased text of an unquoted word.
    pub fn keyword(&self) -> Option<String> {
        match self {
            Token::Word {
                text,
                quoted: false,
            } => Some(text.to_ascii_lowercase()),
            _ => None,
        }
    }
}

/// Every operator, longest first (matching is greedy).
const OPS: &[&str] = &[
    "->>", "#>>", "<=>", "!=", "<>", "<=", ">=", "||", "::", "<<", ">>", "->", "#>", "@>", "<@",
    "&&", "!~*", "~*", "!~", "(", ")", ",", ";", ".", "*", "+", "-", "/", "%", "=", "<", ">", "^",
    "~", "&", "|", "[", "]", "!", "{", "}", ":",
];

/// Named placeholders map to positions in order of first appearance.
#[derive(Debug, Default)]
pub struct Params {
    pub count: usize,
    pub names: Vec<String>,
}

pub fn tokenize(sql: &str, dialect: Dialect) -> SqlResult<(Vec<Token>, Params)> {
    let b = sql.as_bytes();
    let mut i = 0;
    let mut out = Vec::new();
    let mut params = Params::default();
    let mut positional = 0usize;
    let err = |msg: &str| SqlError::syntax(msg.to_owned());
    while i < b.len() {
        let c = b[i];
        // Whitespace.
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        // Comments.
        if c == b'-' && b.get(i + 1) == Some(&b'-') || (c == b'#' && dialect == Dialect::MySql) {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if c == b'/' && b.get(i + 1) == Some(&b'*') {
            let mut depth = 0;
            while i < b.len() {
                if b[i] == b'/' && b.get(i + 1) == Some(&b'*') {
                    depth += 1;
                    i += 2;
                } else if b[i] == b'*' && b.get(i + 1) == Some(&b'/') {
                    depth -= 1;
                    i += 2;
                    if depth == 0 {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            if depth != 0 {
                return Err(err("unterminated /* comment"));
            }
            continue;
        }
        // Prefixed strings: E'..' (escapes), N'..' (national), X'..' (hex), B'..' (bits).
        if matches!(c, b'e' | b'E' | b'n' | b'N' | b'x' | b'X' | b'b' | b'B')
            && b.get(i + 1) == Some(&b'\'')
        {
            let (s, next) = read_quoted(
                sql,
                i + 1,
                b'\'',
                matches!(c, b'e' | b'E') || dialect == Dialect::MySql,
            )?;
            i = next;
            match c {
                b'x' | b'X' => out.push(Token::Blob(
                    parse_hex(&s).ok_or_else(|| err("invalid hexadecimal literal"))?,
                )),
                b'b' | b'B' => out.push(Token::Blob(
                    parse_bits(&s).ok_or_else(|| err("invalid bit literal"))?,
                )),
                _ => out.push(Token::Str(s)),
            }
            continue;
        }
        // Strings.
        if c == b'\'' {
            let (s, next) = read_quoted(sql, i, b'\'', dialect == Dialect::MySql)?;
            out.push(Token::Str(s));
            i = next;
            continue;
        }
        if c == b'"' {
            let (s, next) = read_quoted(sql, i, b'"', dialect == Dialect::MySql)?;
            // MySQL treats "..." as a string (ANSI_QUOTES off); everyone else as an identifier.
            out.push(if dialect == Dialect::MySql {
                Token::Str(s)
            } else {
                Token::Word {
                    text: s,
                    quoted: true,
                }
            });
            i = next;
            continue;
        }
        if c == b'`' {
            let (s, next) = read_quoted(sql, i, b'`', false)?;
            out.push(Token::Word {
                text: s,
                quoted: true,
            });
            i = next;
            continue;
        }
        if c == b'[' && matches!(dialect, Dialect::MsSql | Dialect::Sqlite) {
            let end = sql[i + 1..]
                .find(']')
                .ok_or_else(|| err("unterminated [identifier]"))?
                + i
                + 1;
            out.push(Token::Word {
                text: sql[i + 1..end].to_owned(),
                quoted: true,
            });
            i = end + 1;
            continue;
        }
        // Dollar quoting and $n placeholders.
        if c == b'$' {
            if b.get(i + 1).is_some_and(u8::is_ascii_digit) {
                let start = i + 1;
                i += 1;
                while i < b.len() && b[i].is_ascii_digit() {
                    i += 1;
                }
                let n: usize = sql[start..i].parse().map_err(|_| err("bad placeholder"))?;
                if n == 0 || n > 65535 {
                    return Err(err("placeholders are $1 .. $65535"));
                }
                params.count = params.count.max(n);
                out.push(Token::Param(n - 1));
                continue;
            }
            let tag_end = sql[i + 1..].find('$').map(|p| p + i + 1);
            if let Some(te) = tag_end {
                let tag = &sql[i..=te];
                if tag[1..tag.len() - 1]
                    .chars()
                    .all(|ch| ch.is_alphanumeric() || ch == '_')
                {
                    let body_start = te + 1;
                    let close = sql[body_start..]
                        .find(tag)
                        .ok_or_else(|| err("unterminated dollar-quoted string"))?;
                    out.push(Token::Str(sql[body_start..body_start + close].to_owned()));
                    i = body_start + close + tag.len();
                    continue;
                }
            }
            return Err(err("unexpected '$'"));
        }
        if c == b'?' {
            out.push(Token::Param(positional));
            positional += 1;
            params.count = params.count.max(positional);
            i += 1;
            continue;
        }
        // Named placeholders (:name, @name in T-SQL) and MySQL variables.
        if c == b'@' {
            let system = b.get(i + 1) == Some(&b'@');
            let start = if system { i + 2 } else { i + 1 };
            let mut j = start;
            while j < b.len()
                && (b[j].is_ascii_alphanumeric() || matches!(b[j], b'_' | b'.' | b'$'))
            {
                j += 1;
            }
            if j > start {
                let name = sql[start..j].to_owned();
                if dialect == Dialect::MsSql && !system {
                    out.push(Token::Param(named(&mut params, &name)));
                } else {
                    out.push(Token::Variable { name, system });
                }
                i = j;
                continue;
            }
        }
        if c == b':'
            && dialect != Dialect::Postgres
            && b.get(i + 1)
                .is_some_and(|x| x.is_ascii_alphabetic() || *x == b'_')
        {
            let start = i + 1;
            let mut j = start;
            while j < b.len() && (b[j].is_ascii_alphanumeric() || b[j] == b'_') {
                j += 1;
            }
            out.push(Token::Param(named(&mut params, &sql[start..j])));
            i = j;
            continue;
        }
        // Numbers: 12, 1.5, .5, 1e10, 0x1F.
        if c.is_ascii_digit() || (c == b'.' && b.get(i + 1).is_some_and(u8::is_ascii_digit)) {
            if c == b'0'
                && matches!(b.get(i + 1), Some(b'x' | b'X'))
                && b.get(i + 2).is_some_and(u8::is_ascii_hexdigit)
            {
                let start = i + 2;
                i = start;
                while i < b.len() && b[i].is_ascii_hexdigit() {
                    i += 1;
                }
                out.push(Token::Blob(
                    parse_hex(&sql[start..i]).ok_or_else(|| err("invalid hexadecimal literal"))?,
                ));
                continue;
            }
            let start = i;
            while i < b.len() && b[i].is_ascii_digit() {
                i += 1;
            }
            if i < b.len() && b[i] == b'.' && !(b.get(i + 1) == Some(&b'.')) {
                i += 1;
                while i < b.len() && b[i].is_ascii_digit() {
                    i += 1;
                }
            }
            if i < b.len() && matches!(b[i], b'e' | b'E') {
                let mut j = i + 1;
                if j < b.len() && matches!(b[j], b'+' | b'-') {
                    j += 1;
                }
                if j < b.len() && b[j].is_ascii_digit() {
                    i = j;
                    while i < b.len() && b[i].is_ascii_digit() {
                        i += 1;
                    }
                }
            }
            out.push(Token::Number(sql[start..i].to_owned()));
            continue;
        }
        // Words.
        if c.is_ascii_alphabetic() || c == b'_' || c >= 0x80 {
            let start = i;
            while i < b.len()
                && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b'$' || b[i] >= 0x80)
            {
                i += 1;
            }
            out.push(Token::Word {
                text: sql[start..i].to_owned(),
                quoted: false,
            });
            continue;
        }
        // Operators.
        match OPS.iter().find(|op| sql[i..].starts_with(**op)) {
            Some(op) => {
                out.push(Token::Op(op));
                i += op.len();
            }
            None => {
                return Err(SqlError::syntax(format!(
                    "unexpected character {:?}",
                    sql[i..].chars().next().unwrap_or(' ')
                )))
            }
        }
    }
    Ok((out, params))
}

fn named(params: &mut Params, name: &str) -> usize {
    let lower = name.to_ascii_lowercase();
    if let Some(p) = params.names.iter().position(|n| *n == lower) {
        return p;
    }
    params.names.push(lower);
    params.count = params.count.max(params.names.len());
    params.names.len() - 1
}

/// Reads a quoted run starting at the opening quote `b[start]`; a doubled quote is a literal
/// quote; with `backslash`, C-style escapes are resolved. Returns the text and the next index.
fn read_quoted(sql: &str, start: usize, quote: u8, backslash: bool) -> SqlResult<(String, usize)> {
    let b = sql.as_bytes();
    let mut i = start + 1;
    let mut out: Vec<u8> = Vec::new();
    while i < b.len() {
        let c = b[i];
        if c == quote {
            if b.get(i + 1) == Some(&quote) {
                out.push(quote);
                i += 2;
                continue;
            }
            let s = String::from_utf8(out)
                .map_err(|_| SqlError::syntax("invalid UTF-8 in literal".into()))?;
            return Ok((s, i + 1));
        }
        if backslash && c == b'\\' && i + 1 < b.len() {
            let e = b[i + 1];
            i += 2;
            match e {
                b'n' => out.push(b'\n'),
                b't' => out.push(b'\t'),
                b'r' => out.push(b'\r'),
                b'b' => out.push(8),
                b'f' => out.push(12),
                b'0' => out.push(0),
                b'Z' => out.push(26),
                b'x' if quote == b'\'' => {
                    let hex: String = sql[i..]
                        .chars()
                        .take(2)
                        .take_while(char::is_ascii_hexdigit)
                        .collect();
                    let v = u8::from_str_radix(&hex, 16)
                        .map_err(|_| SqlError::syntax("bad \\x escape".into()))?;
                    out.push(v);
                    i += hex.len();
                }
                b'u' if quote == b'\'' => {
                    let hex: String = sql[i..].chars().take(4).collect();
                    let cp = u32::from_str_radix(&hex, 16).ok().and_then(char::from_u32);
                    let ch = cp.ok_or_else(|| SqlError::syntax("bad \\u escape".into()))?;
                    let mut buf = [0u8; 4];
                    out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
                    i += 4;
                }
                other => out.push(other),
            }
            continue;
        }
        out.push(c);
        i += 1;
    }
    Err(SqlError::syntax(
        "unterminated quoted string or identifier".into(),
    ))
}

fn parse_hex(s: &str) -> Option<Vec<u8>> {
    let s = if s.len() % 2 == 1 {
        format!("0{s}")
    } else {
        s.to_owned()
    };
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

fn parse_bits(s: &str) -> Option<Vec<u8>> {
    if !s.bytes().all(|c| c == b'0' || c == b'1') {
        return None;
    }
    let padded = format!("{}{s}", "0".repeat((8 - s.len() % 8) % 8));
    (0..padded.len())
        .step_by(8)
        .map(|i| u8::from_str_radix(&padded[i..i + 8], 2).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn toks(sql: &str, d: Dialect) -> Vec<Token> {
        tokenize(sql, d).unwrap().0
    }

    #[test]
    fn dialect_quoting_and_literals() {
        let w = |t: &str, q| Token::Word {
            text: t.into(),
            quoted: q,
        };
        assert_eq!(
            toks("SELECT \"A b\" FROM t", Dialect::Postgres),
            vec![
                w("SELECT", false),
                w("A b", true),
                w("FROM", false),
                w("t", false)
            ]
        );
        assert_eq!(
            toks("select `x`, \"s\"", Dialect::MySql),
            vec![
                w("select", false),
                w("x", true),
                Token::Op(","),
                Token::Str("s".into())
            ]
        );
        assert_eq!(toks("[my col]", Dialect::MsSql), vec![w("my col", true)]);
        assert_eq!(
            toks("'it''s' E'a\\nb' $q$x'y$q$ $$z$$", Dialect::Postgres),
            vec![
                Token::Str("it's".into()),
                Token::Str("a\nb".into()),
                Token::Str("x'y".into()),
                Token::Str("z".into())
            ]
        );
        assert_eq!(
            toks("'a\\'b'", Dialect::MySql),
            vec![Token::Str("a'b".into())]
        );
        assert_eq!(
            toks("'a\\'", Dialect::Postgres),
            vec![Token::Str("a\\".into())]
        );
        assert_eq!(
            toks("X'0aFF' 0x10", Dialect::MySql),
            vec![Token::Blob(vec![10, 255]), Token::Blob(vec![16])]
        );
        assert_eq!(
            toks("1 1.50 .5 2e3 1.5E-2", Dialect::Postgres),
            vec![
                Token::Number("1".into()),
                Token::Number("1.50".into()),
                Token::Number(".5".into()),
                Token::Number("2e3".into()),
                Token::Number("1.5E-2".into())
            ]
        );
    }

    #[test]
    fn comments_operators_and_placeholders() {
        assert_eq!(
            toks("a -- c\n/* x /* y */ */ b # c", Dialect::MySql).len(),
            2
        );
        assert_eq!(
            toks("a::text >= b || c <> d", Dialect::Postgres)[1],
            Token::Op("::")
        );
        let (t, p) = tokenize("$2 = $1", Dialect::Postgres).unwrap();
        assert_eq!((t[0].clone(), p.count), (Token::Param(1), 2));
        let (t, p) = tokenize("? ? ?", Dialect::MySql).unwrap();
        assert_eq!((t[2].clone(), p.count), (Token::Param(2), 3));
        let (t, p) = tokenize("@b = @a and @b", Dialect::MsSql).unwrap();
        assert_eq!(
            (t[0].clone(), t[2].clone(), t[4].clone(), p.count),
            (Token::Param(0), Token::Param(1), Token::Param(0), 2)
        );
        assert_eq!(
            toks("@@version @x", Dialect::MySql),
            vec![
                Token::Variable {
                    name: "version".into(),
                    system: true
                },
                Token::Variable {
                    name: "x".into(),
                    system: false
                }
            ]
        );
        assert!(tokenize("'open", Dialect::Postgres).is_err());
        assert!(tokenize("/* open", Dialect::Postgres).is_err());
    }
}
