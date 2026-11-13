//! A TLS 1.3 client for Storine's own endpoints: the native protocol's clients, and nodes
//! calling each other. Same scope as the server: `TLS_CHACHA20_POLY1305_SHA256`, `x25519`,
//! Ed25519 certificates, full handshakes only.
//!
//! The server is trusted through one of:
//! - [`Trust::Pinned`]: it must present exactly one of these certificates;
//! - [`Trust::Roots`]: its certificate must be one of these, or be signed directly by one of
//!   them, name the host the client asked for, and be within its validity period.
//!   Intermediate certificates are not followed: telling a CA from a leaf safely needs
//!   `basicConstraints` checks this parser does not make, and a cluster has no need for them.

use std::io::{self, Read, Write};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use storage_codec::curve25519::{ed25519_verify, x25519, x25519_public};
use storage_codec::rand::os_random_array;
use storage_codec::sha256::{ct_eq, hmac_sha256, sha256};

use crate::x509::Certificate;
use crate::{
    alert_error, derive_secret, derive_secret_noctx, ext, handshake_msg, hkdf_extract, pem_blocks, plain_record,
    HandshakeReader, Keys, TlsReader, TlsWriter, ALERT_BAD_CERTIFICATE, ALERT_DECODE_ERROR, ALERT_DECRYPT_ERROR,
    ALERT_HANDSHAKE_FAILURE, ALERT_ILLEGAL_PARAMETER, ALERT_UNEXPECTED_MESSAGE, CHACHA20_POLY1305_SHA256, CT_HANDSHAKE,
    ED25519, HRR_RANDOM, HS_CERTIFICATE, HS_CERTIFICATE_VERIFY, HS_CLIENT_HELLO, HS_ENCRYPTED_EXTENSIONS, HS_FINISHED,
    HS_SERVER_HELLO, TLS13, X25519,
};

/// What makes a server's certificate acceptable.
#[derive(Clone, Debug)]
pub enum Trust {
    /// Exactly these certificates (DER), whatever signed them.
    Pinned(Vec<Vec<u8>>),
    /// Certificates (DER, Ed25519) that are trusted themselves and trust what they sign.
    Roots(Vec<Vec<u8>>),
}

/// How a client checks the server.
#[derive(Clone, Debug)]
pub struct ClientConfig {
    trust: Trust,
}

impl ClientConfig {
    /// Trusts the CA certificates in `pem` (or a self-signed server certificate itself).
    pub fn with_roots_pem(pem: &str) -> io::Result<ClientConfig> {
        let roots = pem_blocks(pem, "CERTIFICATE");
        if roots.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "no PEM CERTIFICATE block"));
        }
        for r in &roots {
            Certificate::parse(r)?;
        }
        Ok(ClientConfig { trust: Trust::Roots(roots) })
    }

    /// Accepts only a server presenting one of the certificates in `pem`.
    pub fn pinned_pem(pem: &str) -> io::Result<ClientConfig> {
        let pinned = pem_blocks(pem, "CERTIFICATE");
        if pinned.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "no PEM CERTIFICATE block"));
        }
        Ok(ClientConfig { trust: Trust::Pinned(pinned) })
    }

    pub fn new(trust: Trust) -> ClientConfig {
        ClientConfig { trust }
    }

    /// Whether `chain` (leaf first) is acceptable for `host` at `now` (seconds since 1970).
    fn accepts(&self, chain: &[Vec<u8>], host: &str, now: i64) -> io::Result<()> {
        let leaf_der = chain.first().ok_or_else(|| alert_error(ALERT_BAD_CERTIFICATE, "no server certificate"))?;
        let fail = |msg: &str| Err(alert_error(ALERT_BAD_CERTIFICATE, msg));
        match &self.trust {
            Trust::Pinned(pinned) => {
                if pinned.iter().any(|p| ct_eq(p, leaf_der)) {
                    Ok(())
                } else {
                    fail("the server certificate is not the pinned one")
                }
            }
            Trust::Roots(roots) => {
                let leaf = Certificate::parse(leaf_der)?;
                let anchored = roots.iter().any(|root| {
                    root == leaf_der
                        || Certificate::parse(root)
                            .is_ok_and(|r| r.subject == leaf.issuer && leaf.signed_by(&r.public_key))
                });
                if !anchored {
                    return fail("the server certificate is not signed by a trusted root");
                }
                if !leaf.valid_at(now) {
                    return fail("the server certificate has expired or is not valid yet");
                }
                if !leaf.names(host) {
                    return fail("the server certificate does not name this host");
                }
                Ok(())
            }
        }
    }
}

/// Runs a TLS 1.3 client handshake with `host` on `r` / `w`; returns the encrypted streams.
/// `r` must not read ahead of what it is asked for (a raw socket, not a `BufReader`).
pub fn connect<R: Read, W: Write>(
    mut r: R,
    mut w: W,
    cfg: &ClientConfig,
    host: &str,
) -> io::Result<(TlsReader<R>, TlsWriter<W>)> {
    let (read_keys, write_keys) = handshake(&mut r, &mut w, cfg, host)?;
    let update = Arc::new(AtomicBool::new(false));
    Ok((
        TlsReader { inner: r, keys: read_keys, plain: Vec::new(), pos: 0, update: Arc::clone(&update), closed: false },
        TlsWriter { inner: w, keys: write_keys, update },
    ))
}

fn now() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_secs() as i64)
}

fn client_hello(random: &[u8; 32], session_id: &[u8; 32], public: &[u8; 32], host: &str) -> Vec<u8> {
    let mut b = vec![0x03, 0x03];
    b.extend_from_slice(random);
    b.push(32);
    b.extend_from_slice(session_id);
    b.extend_from_slice(&2u16.to_be_bytes());
    b.extend_from_slice(&CHACHA20_POLY1305_SHA256.to_be_bytes());
    b.extend_from_slice(&[1, 0]);
    let mut exts = Vec::new();
    if host.parse::<std::net::IpAddr>().is_err() && !host.is_empty() {
        let mut name = vec![0u8];
        name.extend_from_slice(&(host.len() as u16).to_be_bytes());
        name.extend_from_slice(host.as_bytes());
        let mut list = (name.len() as u16).to_be_bytes().to_vec();
        list.extend_from_slice(&name);
        exts.extend(ext(0x0000, &list));
    }
    exts.extend(ext(0x002b, &[2, 0x03, 0x04]));
    exts.extend(ext(0x000a, &[0, 2, (X25519 >> 8) as u8, X25519 as u8]));
    exts.extend(ext(0x000d, &[0, 2, (ED25519 >> 8) as u8, ED25519 as u8]));
    let mut share = (36u16).to_be_bytes().to_vec();
    share.extend_from_slice(&X25519.to_be_bytes());
    share.extend_from_slice(&32u16.to_be_bytes());
    share.extend_from_slice(public);
    exts.extend(ext(0x0033, &share));
    b.extend_from_slice(&(exts.len() as u16).to_be_bytes());
    b.extend_from_slice(&exts);
    handshake_msg(HS_CLIENT_HELLO, &b)
}

/// The server's x25519 share from a ServerHello, after checking what it chose.
fn parse_server_hello(msg: &[u8], session_id: &[u8; 32]) -> io::Result<[u8; 32]> {
    let decode = |m: &str| alert_error(ALERT_DECODE_ERROR, m);
    let b = msg.get(4..).ok_or_else(|| decode("truncated ServerHello"))?;
    if b.len() < 2 + 32 + 1 {
        return Err(decode("truncated ServerHello"));
    }
    if b[2..34] == HRR_RANDOM {
        return Err(alert_error(ALERT_HANDSHAKE_FAILURE, "unexpected HelloRetryRequest"));
    }
    let sid_len = usize::from(b[34]);
    let rest = b.get(35..).ok_or_else(|| decode("truncated ServerHello"))?;
    let (sid, rest) = (rest.get(..sid_len), rest.get(sid_len..));
    let (Some(sid), Some(rest)) = (sid, rest) else { return Err(decode("truncated ServerHello")) };
    if sid != session_id {
        return Err(alert_error(ALERT_ILLEGAL_PARAMETER, "session id not echoed"));
    }
    if rest.len() < 5 || rest[..2] != CHACHA20_POLY1305_SHA256.to_be_bytes() || rest[2] != 0 {
        return Err(alert_error(ALERT_ILLEGAL_PARAMETER, "unexpected cipher suite"));
    }
    let ext_len = usize::from(u16::from_be_bytes([rest[3], rest[4]]));
    let mut exts = rest.get(5..5 + ext_len).ok_or_else(|| decode("truncated extensions"))?;
    let (mut version_ok, mut share) = (false, None);
    while exts.len() >= 4 {
        let kind = u16::from_be_bytes([exts[0], exts[1]]);
        let len = usize::from(u16::from_be_bytes([exts[2], exts[3]]));
        let data = exts.get(4..4 + len).ok_or_else(|| decode("truncated extension"))?;
        match kind {
            0x002b => version_ok = data == TLS13.to_be_bytes(),
            0x0033 if data.len() == 36 && data[..2] == X25519.to_be_bytes() && data[2..4] == [0, 32] => {
                share = Some(data[4..36].try_into().map_err(|_| decode("bad key share"))?);
            }
            _ => {}
        }
        exts = &exts[4 + len..];
    }
    if !version_ok {
        return Err(alert_error(ALERT_ILLEGAL_PARAMETER, "the server did not choose TLS 1.3"));
    }
    share.ok_or_else(|| alert_error(ALERT_ILLEGAL_PARAMETER, "no x25519 key share"))
}

/// The certificate list (DER, in order) of a Certificate message.
fn parse_certificates(msg: &[u8]) -> io::Result<Vec<Vec<u8>>> {
    let decode = || alert_error(ALERT_DECODE_ERROR, "malformed Certificate message");
    let b = msg.get(4..).ok_or_else(decode)?;
    let ctx_len = usize::from(*b.first().ok_or_else(decode)?);
    let b = b.get(1 + ctx_len..).ok_or_else(decode)?;
    let u24 = |s: &[u8]| (usize::from(s[0]) << 16) | (usize::from(s[1]) << 8) | usize::from(s[2]);
    if b.len() < 3 {
        return Err(decode());
    }
    let mut list = b.get(3..3 + u24(b)).ok_or_else(decode)?;
    let mut certs = Vec::new();
    while !list.is_empty() {
        if list.len() < 3 {
            return Err(decode());
        }
        let len = u24(list);
        certs.push(list.get(3..3 + len).ok_or_else(decode)?.to_vec());
        let rest = list.get(3 + len..).ok_or_else(decode)?;
        if rest.len() < 2 {
            return Err(decode());
        }
        let ext_len = usize::from(u16::from_be_bytes([rest[0], rest[1]]));
        list = rest.get(2 + ext_len..).ok_or_else(decode)?;
    }
    if certs.is_empty() {
        return Err(alert_error(ALERT_BAD_CERTIFICATE, "no server certificate"));
    }
    Ok(certs)
}

fn handshake<R: Read, W: Write>(r: &mut R, w: &mut W, cfg: &ClientConfig, host: &str) -> io::Result<(Keys, Keys)> {
    let secret: [u8; 32] = os_random_array()?;
    let public = x25519_public(&secret);
    let random: [u8; 32] = os_random_array()?;
    let session_id: [u8; 32] = os_random_array()?;
    let ch = client_hello(&random, &session_id, &public, host);
    w.write_all(&plain_record(CT_HANDSHAKE, &ch))?;
    w.flush()?;
    let mut transcript = ch;

    let mut hs = HandshakeReader { buf: Vec::new(), app_keys: None, app_data: None };
    let (kind, sh) = hs.next(r, None)?;
    if kind != HS_SERVER_HELLO {
        return Err(alert_error(ALERT_UNEXPECTED_MESSAGE, "expected ServerHello"));
    }
    let server_share = parse_server_hello(&sh, &session_id)?;
    transcript.extend_from_slice(&sh);
    let shared = x25519(&secret, &server_share);
    if shared == [0; 32] {
        return Err(alert_error(ALERT_ILLEGAL_PARAMETER, "invalid x25519 share"));
    }

    let zeros = [0u8; 32];
    let early = hkdf_extract(&zeros, &zeros);
    let empty_hash = sha256(&[]);
    let derived = derive_secret(&early, "derived", &empty_hash);
    let hs_secret = hkdf_extract(&derived, &shared);
    let th = sha256(&transcript);
    let c_hs = derive_secret(&hs_secret, "c hs traffic", &th);
    let s_hs = derive_secret(&hs_secret, "s hs traffic", &th);
    let mut s_keys = Keys::new(s_hs);
    let mut c_keys = Keys::new(c_hs);

    let (kind, ee) = hs.next(r, Some(&mut s_keys))?;
    if kind != HS_ENCRYPTED_EXTENSIONS {
        return Err(alert_error(ALERT_UNEXPECTED_MESSAGE, "expected EncryptedExtensions"));
    }
    transcript.extend_from_slice(&ee);
    let (kind, cert_msg) = hs.next(r, Some(&mut s_keys))?;
    if kind != HS_CERTIFICATE {
        return Err(alert_error(ALERT_UNEXPECTED_MESSAGE, "expected Certificate"));
    }
    let chain = parse_certificates(&cert_msg)?;
    transcript.extend_from_slice(&cert_msg);
    // Trust first: a signature by a key nobody vouches for proves nothing.
    cfg.accepts(&chain, host, now())?;
    let leaf = Certificate::parse(&chain[0])?;

    let (kind, cv) = hs.next(r, Some(&mut s_keys))?;
    if kind != HS_CERTIFICATE_VERIFY {
        return Err(alert_error(ALERT_UNEXPECTED_MESSAGE, "expected CertificateVerify"));
    }
    let body = cv.get(4..).unwrap_or(&[]);
    if body.len() != 2 + 2 + 64 || body[..2] != ED25519.to_be_bytes() || body[2..4] != [0, 64] {
        return Err(alert_error(ALERT_DECODE_ERROR, "malformed CertificateVerify"));
    }
    let sig: [u8; 64] = body[4..].try_into().map_err(|_| alert_error(ALERT_DECODE_ERROR, "bad signature"))?;
    let mut signed = vec![0x20u8; 64];
    signed.extend_from_slice(b"TLS 1.3, server CertificateVerify\0");
    signed.extend_from_slice(&sha256(&transcript));
    if !ed25519_verify(&leaf.public_key, &signed, &sig) {
        return Err(alert_error(ALERT_DECRYPT_ERROR, "the server's handshake signature does not verify"));
    }
    transcript.extend_from_slice(&cv);

    let (kind, fin) = hs.next(r, Some(&mut s_keys))?;
    if kind != HS_FINISHED {
        return Err(alert_error(ALERT_UNEXPECTED_MESSAGE, "expected Finished"));
    }
    let s_finished_key = derive_secret_noctx(&s_hs, "finished");
    let expected = hmac_sha256(&s_finished_key, &sha256(&transcript));
    if fin.len() != 4 + 32 || !ct_eq(&fin[4..], &expected) {
        return Err(alert_error(ALERT_DECRYPT_ERROR, "the server Finished does not verify"));
    }
    if !hs.buf.is_empty() {
        return Err(alert_error(ALERT_UNEXPECTED_MESSAGE, "data after the server Finished"));
    }
    transcript.extend_from_slice(&fin);

    let th_server_finished = sha256(&transcript);
    let derived2 = derive_secret(&hs_secret, "derived", &empty_hash);
    let master = hkdf_extract(&derived2, &zeros);
    let c_ap = derive_secret(&master, "c ap traffic", &th_server_finished);
    let s_ap = derive_secret(&master, "s ap traffic", &th_server_finished);

    let c_finished_key = derive_secret_noctx(&c_hs, "finished");
    let verify = hmac_sha256(&c_finished_key, &th_server_finished);
    w.write_all(&c_keys.seal(CT_HANDSHAKE, &handshake_msg(HS_FINISHED, &verify))?)?;
    w.flush()?;
    Ok((Keys::new(s_ap), Keys::new(c_ap)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{accept, accept_session, issue, new_ca, TlsConfig};
    use std::net::{TcpListener, TcpStream};

    /// Runs `server` on one accepted loopback connection and `client` on the connecting side.
    fn pair<S: FnOnce(TcpStream) + Send + 'static>(server: S, client: impl FnOnce(TcpStream)) {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        let t = std::thread::spawn(move || server(l.accept().unwrap().0));
        client(TcpStream::connect(addr).unwrap());
        t.join().unwrap();
    }

    fn echo_server(cfg: TlsConfig) -> impl FnOnce(TcpStream) + Send {
        move |s| {
            let Ok((mut r, w)) = accept(s.try_clone().unwrap(), s, &cfg, "") else { return };
            let mut w = std::io::BufWriter::new(w);
            let mut buf = vec![0u8; 100_000];
            if r.read_exact(&mut buf).is_ok() {
                w.write_all(&buf).unwrap();
                w.flush().unwrap();
            }
        }
    }

    fn talk(s: TcpStream, cfg: &ClientConfig, host: &str) -> io::Result<()> {
        let (mut r, mut w) = connect(s.try_clone()?, s, cfg, host)?;
        let data: Vec<u8> = (0..100_000u32).map(|i| (i * 7) as u8).collect();
        w.write_all(&data)?;
        w.flush()?;
        let mut back = vec![0u8; data.len()];
        r.read_exact(&mut back)?;
        assert_eq!(back, data);
        Ok(())
    }

    #[test]
    fn self_signed_server_trusted_as_its_own_root() {
        let cfg = TlsConfig::self_signed(&["localhost", "127.0.0.1"]).unwrap();
        let client = ClientConfig::with_roots_pem(&cfg.certificate_pem()).unwrap();
        pair(echo_server(cfg), |s| talk(s, &client, "127.0.0.1").unwrap());
    }

    #[test]
    fn ca_issued_certificate_checks_root_name_and_pin() {
        let ca = new_ca("test cluster").unwrap();
        let node = issue(&ca, &["node1.internal", "127.0.0.1"]).unwrap();
        let roots = ClientConfig::with_roots_pem(&ca.cert_pem).unwrap();
        for host in ["127.0.0.1", "node1.internal", "NODE1.internal."] {
            let cfg = TlsConfig::from_pem(&node.cert_pem, &node.key_pem).unwrap();
            pair(echo_server(cfg), |s| talk(s, &roots, host).unwrap());
        }
        let refused = |client: ClientConfig, host: &'static str| {
            let cfg = TlsConfig::from_pem(&node.cert_pem, &node.key_pem).unwrap();
            pair(echo_server(cfg), move |s| {
                let e = talk(s, &client, host).expect_err("must be refused");
                assert!(e.to_string().contains("certificate"), "{e}");
            });
        };
        refused(roots.clone(), "node2.internal");
        refused(ClientConfig::with_roots_pem(&new_ca("other").unwrap().cert_pem).unwrap(), "127.0.0.1");
        let other_node = issue(&ca, &["127.0.0.1"]).unwrap();
        refused(ClientConfig::pinned_pem(&other_node.cert_pem).unwrap(), "127.0.0.1");
        let pinned = ClientConfig::pinned_pem(&node.cert_pem).unwrap();
        let cfg = TlsConfig::from_pem(&node.cert_pem, &node.key_pem).unwrap();
        pair(echo_server(cfg), |s| talk(s, &pinned, "127.0.0.1").unwrap());
    }

    /// A node certificate cannot vouch for another: a "certificate" signed with a leaf's key is
    /// refused even though the leaf was issued by the trusted CA.
    #[test]
    fn leaf_cannot_act_as_ca() {
        let ca = new_ca("test cluster").unwrap();
        let leaf = issue(&ca, &["127.0.0.1"]).unwrap();
        let forged = issue(&leaf, &["127.0.0.1"]).unwrap();
        let roots = ClientConfig::with_roots_pem(&ca.cert_pem).unwrap();
        let cfg = TlsConfig::from_pem(&format!("{}{}", forged.cert_pem, leaf.cert_pem), &forged.key_pem).unwrap();
        pair(echo_server(cfg), |s| {
            assert!(talk(s, &roots, "127.0.0.1").is_err());
        });
    }

    /// The reactor's path: the server handshakes with `accept_session` and then moves raw bytes
    /// through `Session::decrypt` / `encrypt`, fed a few bytes at a time.
    #[test]
    fn session_round_trip_with_partial_reads() {
        let cfg = TlsConfig::self_signed(&["127.0.0.1"]).unwrap();
        let client = ClientConfig::with_roots_pem(&cfg.certificate_pem()).unwrap();
        pair(
            move |s| {
                let mut sess = accept_session(s.try_clone().unwrap(), s.try_clone().unwrap(), &cfg, "").unwrap();
                let (mut raw, mut plain) = (Vec::new(), Vec::new());
                let mut sock = s;
                let mut chunk = [0u8; 7];
                while plain.len() < 50_000 {
                    let n = sock.read(&mut chunk).unwrap();
                    assert!(n > 0, "client closed early");
                    raw.extend_from_slice(&chunk[..n]);
                    sess.decrypt(&mut raw, &mut plain).unwrap();
                }
                let mut out = Vec::new();
                sess.encrypt(&plain, &mut out).unwrap();
                sock.write_all(&out).unwrap();
            },
            |s| {
                let (mut r, mut w) = connect(s.try_clone().unwrap(), s, &client, "127.0.0.1").unwrap();
                let data: Vec<u8> = (0..50_000u32).map(|i| (i % 251) as u8).collect();
                w.write_all(&data).unwrap();
                w.flush().unwrap();
                let mut back = vec![0u8; data.len()];
                r.read_exact(&mut back).unwrap();
                assert_eq!(back, data);
            },
        );
    }
}
