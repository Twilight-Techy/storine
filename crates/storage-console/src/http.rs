//! A small HTTP/1.1 server: one request per connection, bodies by `Content-Length`.
//!
//! The console serves one person's browser, so it favours being hard to misuse over
//! throughput: each connection gets a thread and is closed after its response, headers and
//! bodies are capped, and every response forbids caching, framing and content sniffing.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

/// Largest request head (request line and headers).
const MAX_HEAD: usize = 64 * 1024;
/// Largest request body: a document up to the server's limits, with room for JSON overhead.
pub const MAX_BODY: usize = 64 * 1024 * 1024;

#[derive(Debug)]
pub struct Request {
    pub method: String,
    /// Path without the query string, percent-decoded per segment by [`Request::segments`].
    pub path: String,
    pub query: Vec<(String, String)>,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Request {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v.as_str())
    }

    pub fn param(&self, name: &str) -> Option<&str> {
        self.query.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str())
    }

    /// The path's segments, each percent-decoded (so a key may contain `/` as `%2F`).
    pub fn segments(&self) -> Result<Vec<String>, String> {
        self.path.trim_start_matches('/').split('/').filter(|s| !s.is_empty()).map(percent_decode).collect()
    }
}

#[derive(Debug)]
pub struct Response {
    pub status: u16,
    pub content_type: &'static str,
    pub body: Vec<u8>,
}

impl Response {
    pub fn new(status: u16, content_type: &'static str, body: impl Into<Vec<u8>>) -> Response {
        Response { status, content_type, body: body.into() }
    }

    pub fn json(status: u16, body: String) -> Response {
        Response::new(status, "application/json; charset=utf-8", body)
    }
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        413 => "Payload Too Large",
        421 => "Misdirected Request",
        422 => "Unprocessable Entity",
        429 => "Too Many Requests",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        _ => "Error",
    }
}

/// Decodes `%XX` escapes (and `+` in query strings when `plus` is set).
fn decode(s: &str, plus: bool) -> Result<String, String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' => {
                let h = b.get(i + 1..i + 3).and_then(|h| std::str::from_utf8(h).ok()).and_then(|h| u8::from_str_radix(h, 16).ok());
                out.push(h.ok_or("bad percent escape")?);
                i += 3;
            }
            b'+' if plus => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8(out).map_err(|_| "percent escape is not UTF-8".to_owned())
}

pub fn percent_decode(s: &str) -> Result<String, String> {
    decode(s, false)
}

/// Reads one request. `Err` carries the response to send instead (malformed or too large).
pub fn read_request(stream: &TcpStream) -> Result<Request, Response> {
    let bad = |m: &str| Response::new(400, "text/plain; charset=utf-8", m.to_owned());
    let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));
    let mut r = BufReader::new(stream);
    let mut head_len = 0;
    let mut line = String::new();
    let mut read_line = |r: &mut BufReader<&TcpStream>, line: &mut String| -> Result<(), Response> {
        line.clear();
        let n = r.by_ref().take((MAX_HEAD - head_len.min(MAX_HEAD)) as u64 + 1).read_line(line).map_err(|_| bad("unreadable request"))?;
        head_len += n;
        if head_len > MAX_HEAD {
            return Err(Response::new(413, "text/plain; charset=utf-8", "request head too large"));
        }
        if n == 0 {
            return Err(bad("connection closed mid-request"));
        }
        Ok(())
    };
    read_line(&mut r, &mut line)?;
    let mut parts = line.trim_end().splitn(3, ' ');
    let (method, target) = (parts.next().unwrap_or("").to_owned(), parts.next().unwrap_or("").to_owned());
    if !parts.next().is_some_and(|v| v.starts_with("HTTP/1.")) || !target.starts_with('/') {
        return Err(bad("not an HTTP/1.x request"));
    }
    let mut headers = Vec::new();
    loop {
        read_line(&mut r, &mut line)?;
        let l = line.trim_end_matches(['\r', '\n']);
        if l.is_empty() {
            break;
        }
        let (k, v) = l.split_once(':').ok_or_else(|| bad("bad header"))?;
        headers.push((k.trim().to_owned(), v.trim().to_owned()));
        if headers.len() > 100 {
            return Err(bad("too many headers"));
        }
    }
    let req_header = |name: &str| headers.iter().find(|(k, _): &&(String, String)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v.clone());
    if req_header("Transfer-Encoding").is_some() {
        return Err(bad("chunked bodies are not supported"));
    }
    let len: usize = match req_header("Content-Length") {
        Some(v) => v.parse().map_err(|_| bad("bad Content-Length"))?,
        None => 0,
    };
    if len > MAX_BODY {
        return Err(Response::new(413, "text/plain; charset=utf-8", "request body too large"));
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body).map_err(|_| bad("request body cut short"))?;
    let (path, qs) = target.split_once('?').unwrap_or((&target, ""));
    let query = qs
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            Ok((decode(k, true)?, decode(v, true)?))
        })
        .collect::<Result<_, String>>()
        .map_err(|e| bad(&e))?;
    Ok(Request { method, path: path.to_owned(), query, headers, body })
}

/// Writes `resp` with the console's fixed security headers and closes the exchange.
pub fn write_response(mut stream: &TcpStream, resp: &Response) -> std::io::Result<()> {
    let head = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\nCache-Control: no-store\r\n\
         X-Content-Type-Options: nosniff\r\nX-Frame-Options: DENY\r\nReferrer-Policy: no-referrer\r\n\
         Cross-Origin-Opener-Policy: same-origin\r\nCross-Origin-Resource-Policy: same-origin\r\n\
         Content-Security-Policy: default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'\r\n\r\n",
        resp.status,
        reason(resp.status),
        resp.content_type,
        resp.body.len()
    );
    stream.write_all(head.as_bytes())?;
    stream.write_all(&resp.body)?;
    stream.flush()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;

    fn exchange(raw: &[u8]) -> Result<Request, Response> {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut c = TcpStream::connect(l.local_addr().unwrap()).unwrap();
        c.write_all(raw).unwrap();
        c.shutdown(std::net::Shutdown::Write).unwrap();
        let (s, _) = l.accept().unwrap();
        read_request(&s)
    }

    #[test]
    fn a_request_parses_with_its_body_query_and_escaped_segments() {
        let r = exchange(b"PUT /api/c/docs/a%2Fb?version=3&q=x+y HTTP/1.1\r\nHost: h\r\nContent-Length: 2\r\n\r\n{}").unwrap();
        assert_eq!(r.method, "PUT");
        assert_eq!(r.segments().unwrap(), ["api", "c", "docs", "a/b"]);
        assert_eq!(r.param("version"), Some("3"));
        assert_eq!(r.param("q"), Some("x y"));
        assert_eq!(r.header("host"), Some("h"));
        assert_eq!(r.body, b"{}");
    }

    #[test]
    fn malformed_and_oversized_requests_are_refused() {
        assert_eq!(exchange(b"GET / FTP/1.0\r\n\r\n").unwrap_err().status, 400);
        assert_eq!(exchange(b"GET / HTTP/1.1\r\nContent-Length: 5\r\n\r\nab").unwrap_err().status, 400);
        assert_eq!(exchange(b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n").unwrap_err().status, 400);
        assert_eq!(exchange(format!("POST / HTTP/1.1\r\nContent-Length: {}\r\n\r\n", MAX_BODY + 1).as_bytes()).unwrap_err().status, 413);
        let big = format!("GET / HTTP/1.1\r\nX: {}\r\n\r\n", "a".repeat(MAX_HEAD));
        assert_eq!(exchange(big.as_bytes()).unwrap_err().status, 413);
        assert!(percent_decode("%zz").is_err());
    }
}
