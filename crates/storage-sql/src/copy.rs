//! `COPY` data formats: PostgreSQL text (tab-separated, backslash escapes, `\N` for NULL) and
//! CSV. The reader is incremental: data arrives in arbitrary chunks and complete records are
//! taken out as soon as they are available, so a large load never sits in memory whole.

use crate::ast::CopyStmt;
use crate::error::{SqlError, SqlResult};

/// The options of one `COPY`.
#[derive(Debug, Clone)]
pub struct Format {
    pub csv: bool,
    pub header: bool,
    delimiter: u8,
    null: String,
    quote: u8,
    escape: u8,
}

fn ascii(c: Option<char>, default: u8, what: &str) -> SqlResult<u8> {
    match c {
        None => Ok(default),
        Some(c) if c.is_ascii() && c != '\n' && c != '\r' => Ok(c as u8),
        Some(_) => Err(SqlError::new(
            "22023",
            format!("COPY {what} must be a single one-byte character"),
        )),
    }
}

impl Format {
    pub fn new(c: &CopyStmt) -> SqlResult<Format> {
        if c.binary {
            return Err(SqlError::unsupported(
                "COPY in binary format (use text or CSV)",
            ));
        }
        let delimiter = ascii(c.delimiter, if c.csv { b',' } else { b'\t' }, "delimiter")?;
        let quote = ascii(c.quote, b'"', "quote")?;
        let escape = ascii(c.escape, quote, "escape")?;
        if !c.csv && (c.quote.is_some() || c.escape.is_some() || c.header) {
            return Err(SqlError::new(
                "0A000",
                "COPY QUOTE, ESCAPE and HEADER are available only in CSV mode",
            ));
        }
        if c.csv && delimiter == quote {
            return Err(SqlError::new(
                "22023",
                "COPY delimiter and quote must be different",
            ));
        }
        Ok(Format {
            csv: c.csv,
            header: c.header,
            delimiter,
            null: c
                .null
                .clone()
                .unwrap_or_else(|| if c.csv { String::new() } else { "\\N".into() }),
            quote,
            escape,
        })
    }

    /// Appends one record (`None`: NULL) and its line end to `out`.
    pub fn encode(&self, fields: &[Option<String>], out: &mut String) {
        for (i, f) in fields.iter().enumerate() {
            if i > 0 {
                out.push(self.delimiter as char);
            }
            match f {
                None => out.push_str(&self.null),
                Some(v) if self.csv => self.encode_csv(v, out),
                Some(v) => self.encode_text(v, out),
            }
        }
        out.push('\n');
    }

    fn encode_text(&self, v: &str, out: &mut String) {
        for c in v.chars() {
            match c {
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' if self.delimiter != b'\t' => out.push_str("\\t"),
                '\u{8}' => out.push_str("\\b"),
                '\u{c}' => out.push_str("\\f"),
                '\u{b}' => out.push_str("\\v"),
                c if c as u32 == u32::from(self.delimiter) => {
                    out.push('\\');
                    out.push(c);
                }
                c => out.push(c),
            }
        }
    }

    fn encode_csv(&self, v: &str, out: &mut String) {
        let (q, e, d) = (
            self.quote as char,
            self.escape as char,
            self.delimiter as char,
        );
        let needs = v == self.null
            || v == "\\."
            || v.chars()
                .any(|c| c == d || c == q || c == '\n' || c == '\r' || (e != q && c == e));
        if !needs {
            out.push_str(v);
            return;
        }
        out.push(q);
        for c in v.chars() {
            if c == q || (e != q && c == e) {
                out.push(e);
            }
            out.push(c);
        }
        out.push(q);
    }
}

/// Splits incoming `COPY ... FROM STDIN` data into records.
#[derive(Debug)]
pub struct Reader {
    fmt: Format,
    buf: Vec<u8>,
    header_pending: bool,
    /// The `\.` end-of-data marker was seen.
    pub done: bool,
}

/// Longest record accepted (a malformed CSV quote would otherwise buffer everything).
const MAX_RECORD: usize = 64 * 1024 * 1024;

impl Reader {
    pub fn new(fmt: Format) -> Reader {
        Reader {
            header_pending: fmt.header,
            fmt,
            buf: Vec::new(),
            done: false,
        }
    }

    pub fn feed(&mut self, data: &[u8]) {
        if !self.done {
            self.buf.extend_from_slice(data);
        }
    }

    /// The next complete record, if one is buffered; with `eof`, a final unterminated one too.
    pub fn next_record(&mut self, eof: bool) -> SqlResult<Option<Vec<Option<String>>>> {
        loop {
            if self.done {
                return Ok(None);
            }
            let end = if self.fmt.csv {
                self.csv_record_end()
            } else {
                self.buf.iter().position(|b| *b == b'\n')
            };
            let (line_end, consumed) = match end {
                Some(i) => (i, i + 1),
                None if eof && !self.buf.is_empty() => (self.buf.len(), self.buf.len()),
                None => {
                    if self.buf.len() > MAX_RECORD {
                        return Err(SqlError::new(
                            "22P04",
                            "COPY record is too long (unterminated quote?)",
                        ));
                    }
                    return Ok(None);
                }
            };
            let mut line: Vec<u8> = self.buf.drain(..consumed).take(line_end).collect();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            let line = String::from_utf8(line)
                .map_err(|_| SqlError::new("22021", "COPY data is not valid UTF-8"))?;
            if line == "\\." {
                self.done = true;
                self.buf.clear();
                return Ok(None);
            }
            if self.header_pending {
                self.header_pending = false;
                continue;
            }
            return if self.fmt.csv {
                self.csv_fields(&line).map(Some)
            } else {
                Ok(Some(self.text_fields(&line)))
            };
        }
    }

    /// Index of the newline ending the first CSV record (newlines inside quotes do not count).
    fn csv_record_end(&self) -> Option<usize> {
        let (q, e) = (self.fmt.quote, self.fmt.escape);
        let mut quoted = false;
        let mut i = 0;
        while i < self.buf.len() {
            let b = self.buf[i];
            if quoted {
                if e != q && b == e && i + 1 < self.buf.len() {
                    i += 2;
                    continue;
                }
                if b == q {
                    if e == q && self.buf.get(i + 1) == Some(&q) {
                        i += 2;
                        continue;
                    }
                    quoted = false;
                }
            } else if b == q {
                quoted = true;
            } else if b == b'\n' {
                return Some(i);
            }
            i += 1;
        }
        None
    }

    fn csv_fields(&self, line: &str) -> SqlResult<Vec<Option<String>>> {
        let (q, e, d) = (
            self.fmt.quote as char,
            self.fmt.escape as char,
            self.fmt.delimiter as char,
        );
        let mut out = Vec::new();
        let mut cur = String::new();
        let mut quoted = false;
        let mut was_quoted = false;
        let mut chars = line.chars().peekable();
        while let Some(c) = chars.next() {
            if quoted {
                if c == e && e != q && matches!(chars.peek(), Some(n) if *n == q || *n == e) {
                    cur.push(chars.next().unwrap_or(c));
                } else if c == q {
                    if e == q && chars.peek() == Some(&q) {
                        chars.next();
                        cur.push(q);
                    } else {
                        quoted = false;
                    }
                } else {
                    cur.push(c);
                }
            } else if c == q {
                quoted = true;
                was_quoted = true;
            } else if c == d {
                out.push(self.csv_value(std::mem::take(&mut cur), was_quoted));
                was_quoted = false;
            } else {
                cur.push(c);
            }
        }
        if quoted {
            return Err(SqlError::new("22P04", "unterminated CSV quoted field"));
        }
        out.push(self.csv_value(cur, was_quoted));
        Ok(out)
    }

    fn csv_value(&self, v: String, quoted: bool) -> Option<String> {
        if !quoted && v == self.fmt.null {
            None
        } else {
            Some(v)
        }
    }

    fn text_fields(&self, line: &str) -> Vec<Option<String>> {
        let d = self.fmt.delimiter;
        let bytes = line.as_bytes();
        let mut out = Vec::new();
        let mut start = 0;
        let mut i = 0;
        while i <= bytes.len() {
            if i == bytes.len() || bytes[i] == d {
                let raw = &line[start..i];
                out.push(if raw == self.fmt.null {
                    None
                } else {
                    Some(unescape(raw))
                });
                start = i + 1;
            } else if bytes[i] == b'\\' {
                i += 1; // the escaped byte never ends a field
            }
            i += 1;
        }
        out
    }
}

/// Resolves the text format's backslash escapes.
fn unescape(raw: &str) -> String {
    if !raw.contains('\\') {
        return raw.to_owned();
    }
    let mut out = String::with_capacity(raw.len());
    let mut chars = raw.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        let Some(n) = chars.next() else {
            out.push('\\');
            break;
        };
        match n {
            'b' => out.push('\u{8}'),
            'f' => out.push('\u{c}'),
            'n' => out.push('\n'),
            'r' => out.push('\r'),
            't' => out.push('\t'),
            'v' => out.push('\u{b}'),
            '0'..='7' => {
                let mut v = n.to_digit(8).unwrap_or(0);
                for _ in 0..2 {
                    match chars.peek().and_then(|c| c.to_digit(8)) {
                        Some(d) => {
                            v = v * 8 + d;
                            chars.next();
                        }
                        None => break,
                    }
                }
                out.push(char::from_u32(v & 0xff).unwrap_or('?'));
            }
            'x' if chars.peek().is_some_and(char::is_ascii_hexdigit) => {
                let mut v = 0;
                for _ in 0..2 {
                    match chars.peek().and_then(|c| c.to_digit(16)) {
                        Some(d) => {
                            v = v * 16 + d;
                            chars.next();
                        }
                        None => break,
                    }
                }
                out.push(char::from_u32(v).unwrap_or('?'));
            }
            other => out.push(other),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stmt(csv: bool) -> CopyStmt {
        CopyStmt {
            table: Some("t".into()),
            columns: Vec::new(),
            query: None,
            from: true,
            stdio: true,
            csv,
            binary: false,
            header: false,
            delimiter: None,
            null: None,
            quote: None,
            escape: None,
        }
    }

    fn read_all(fmt: Format, chunks: &[&[u8]]) -> Vec<Vec<Option<String>>> {
        let mut r = Reader::new(fmt);
        let mut out = Vec::new();
        for c in chunks {
            r.feed(c);
            while let Some(rec) = r.next_record(false).unwrap() {
                out.push(rec);
            }
        }
        while let Some(rec) = r.next_record(true).unwrap() {
            out.push(rec);
        }
        out
    }

    #[test]
    fn text_format_round_trips_escapes_nulls_and_split_chunks() {
        let fmt = Format::new(&stmt(false)).unwrap();
        let rows = vec![
            vec![
                Some("a\tb".to_owned()),
                None,
                Some("back\\slash\nnew".to_owned()),
            ],
            vec![
                Some(String::new()),
                Some("\\N".to_owned()),
                Some("x".to_owned()),
            ],
        ];
        let mut text = String::new();
        for r in &rows {
            fmt.encode(r, &mut text);
        }
        text.push_str("\\.\n");
        let bytes = text.as_bytes();
        for split in 0..bytes.len() {
            let (a, b) = bytes.split_at(split);
            assert_eq!(read_all(fmt.clone(), &[a, b]), rows, "split at {split}");
        }
        assert_eq!(unescape("\\101\\x42\\q"), "ABq");
    }

    #[test]
    fn csv_format_handles_quotes_newlines_headers_and_null_versus_empty() {
        let mut s = stmt(true);
        s.header = true;
        let fmt = Format::new(&s).unwrap();
        let data = b"id,name,note\r\n1,\"Smith, J\",\"says \"\"hi\"\"\nthen leaves\"\n2,,\"\"\n";
        assert_eq!(
            read_all(fmt.clone(), &[data]),
            vec![
                vec![
                    Some("1".into()),
                    Some("Smith, J".into()),
                    Some("says \"hi\"\nthen leaves".into())
                ],
                vec![Some("2".into()), None, Some(String::new())],
            ]
        );
        let mut out = String::new();
        fmt.encode(
            &[
                Some("a,b".into()),
                None,
                Some(String::new()),
                Some("q\"".into()),
            ],
            &mut out,
        );
        assert_eq!(out, "\"a,b\",,\"\",\"q\"\"\"\n");
        let mut bad = Reader::new(Format::new(&stmt(true)).unwrap());
        bad.feed(b"1,\"open\n");
        assert!(bad.next_record(true).is_err());
    }
}
