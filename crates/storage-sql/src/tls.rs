//! A zero-dependency TLS server for the PostgreSQL, MySQL and SQL Server endpoints: TLS 1.3
//! (RFC 8446), and TLS 1.2 (RFC 5246) for clients that offer nothing newer.
//!
//! Scope, chosen for what every current client library offers:
//! - TLS 1.3: cipher suite `TLS_CHACHA20_POLY1305_SHA256`, key exchange `x25519` (with a
//!   HelloRetryRequest when the client guessed another group first), certificates signed with
//!   `ed25519`;
//! - TLS 1.2: `TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256` (RFC 7905) with `x25519` and an
//!   `ed25519` signature (RFC 8422), extended master secret (RFC 7627), secure renegotiation
//!   indication (renegotiation itself is refused);
//! - full handshakes only (no session resumption, no 0-RTT), no client certificates;
//! - `KeyUpdate` in both directions, `close_notify`, fatal alerts on every protocol error.
//!
//! The certificate is either supplied (PEM certificate chain + PKCS#8 Ed25519 key, e.g. from
//! `openssl genpkey -algorithm ed25519`) or generated: a self-signed certificate for `localhost`
//! and the loopback addresses, kept in the data directory so it stays the same across restarts.

use std::io::{self, Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use storage_codec::crypto::{open_in_place, seal_in_place};
use storage_codec::curve25519::{x25519, x25519_public, Ed25519Key};
use storage_codec::rand::{os_random, os_random_array};

use crate::hash::{base64_decode, base64_encode, ct_eq, hmac_sha256, sha256};

const TLS13: u16 = 0x0304;
const CHACHA20_POLY1305_SHA256: u16 = 0x1303;
const TLS12: u16 = 0x0303;
const ECDHE_ECDSA_CHACHA20_POLY1305: u16 = 0xcca9;
const EMPTY_RENEGOTIATION_INFO_SCSV: u16 = 0x00ff;
const X25519: u16 = 0x001d;
const ED25519: u16 = 0x0807;

const CT_CHANGE_CIPHER_SPEC: u8 = 20;
const CT_ALERT: u8 = 21;
const CT_HANDSHAKE: u8 = 22;
const CT_APPLICATION_DATA: u8 = 23;

const HS_CLIENT_HELLO: u8 = 1;
const HS_SERVER_HELLO: u8 = 2;
const HS_SERVER_KEY_EXCHANGE: u8 = 12;
const HS_SERVER_HELLO_DONE: u8 = 14;
const HS_CLIENT_KEY_EXCHANGE: u8 = 16;
const HS_ENCRYPTED_EXTENSIONS: u8 = 8;
const HS_CERTIFICATE: u8 = 11;
const HS_CERTIFICATE_VERIFY: u8 = 15;
const HS_FINISHED: u8 = 20;
const HS_KEY_UPDATE: u8 = 24;
const HS_MESSAGE_HASH: u8 = 254;

const ALERT_CLOSE_NOTIFY: u8 = 0;
const ALERT_UNEXPECTED_MESSAGE: u8 = 10;
const ALERT_BAD_RECORD_MAC: u8 = 20;
const ALERT_RECORD_OVERFLOW: u8 = 22;
const ALERT_HANDSHAKE_FAILURE: u8 = 40;
const ALERT_ILLEGAL_PARAMETER: u8 = 47;
const ALERT_DECODE_ERROR: u8 = 50;
const ALERT_DECRYPT_ERROR: u8 = 51;
const ALERT_PROTOCOL_VERSION: u8 = 70;
const ALERT_INTERNAL_ERROR: u8 = 80;
const ALERT_MISSING_EXTENSION: u8 = 109;

/// Largest plaintext fragment (2^14).
const MAX_FRAGMENT: usize = 16_384;
/// Largest ciphertext record body: fragment + content type + padding allowance + tag.
const MAX_CIPHERTEXT: usize = MAX_FRAGMENT + 256;
/// Largest handshake message accepted from a client (a ClientHello is a few hundred bytes).
const MAX_HANDSHAKE: usize = 64 * 1024;

/// SHA-256("HelloRetryRequest"): the ServerHello.random of a HelloRetryRequest.
const HRR_RANDOM: [u8; 32] = [
    0xcf, 0x21, 0xad, 0x74, 0xe5, 0x9a, 0x61, 0x11, 0xbe, 0x1d, 0x8c, 0x02, 0x1e, 0x65, 0xb8, 0x91,
    0xc2, 0xa2, 0x11, 0x16, 0x7a, 0xbb, 0x8c, 0x5e, 0x07, 0x9e, 0x09, 0xe2, 0xc8, 0xa8, 0x33, 0x9c,
];

fn tls_err(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("TLS: {}", msg.into()))
}

/// The server's identity: an Ed25519 key and the certificate chain presented with it.
#[derive(Debug)]
pub struct TlsConfig {
    key: Ed25519Key,
    chain: Vec<Vec<u8>>,
}

impl TlsConfig {
    /// A certificate chain (PEM, leaf first) and its PKCS#8 Ed25519 private key (PEM).
    pub fn from_pem(cert_pem: &str, key_pem: &str) -> io::Result<TlsConfig> {
        let chain = pem_blocks(cert_pem, "CERTIFICATE");
        if chain.is_empty() {
            return Err(tls_err(
                "the certificate file holds no PEM CERTIFICATE block",
            ));
        }
        let key_der = pem_blocks(key_pem, "PRIVATE KEY")
            .into_iter()
            .next()
            .ok_or_else(|| tls_err("the key file holds no PEM PRIVATE KEY block"))?;
        let seed = ed25519_pkcs8_seed(&key_der).ok_or_else(|| {
            tls_err("only Ed25519 private keys are supported (create one with `openssl genpkey -algorithm ed25519`)")
        })?;
        Ok(TlsConfig {
            key: Ed25519Key::from_seed(&seed),
            chain,
        })
    }

    /// A new self-signed certificate for `names` (DNS names and IP addresses).
    pub fn self_signed(names: &[&str]) -> io::Result<TlsConfig> {
        let seed: [u8; 32] = os_random_array()?;
        let key = Ed25519Key::from_seed(&seed);
        let cert = self_signed_cert(&key, names)?;
        Ok(TlsConfig {
            key,
            chain: vec![cert],
        })
    }

    /// The certificate in `dir` (`server.crt` / `server.key`), created on first use.
    pub fn load_or_create(dir: &Path) -> io::Result<TlsConfig> {
        let (crt, key) = (dir.join("server.crt"), dir.join("server.key"));
        if crt.exists() && key.exists() {
            return TlsConfig::from_pem(
                &std::fs::read_to_string(&crt)?,
                &std::fs::read_to_string(&key)?,
            );
        }
        std::fs::create_dir_all(dir)?;
        let seed: [u8; 32] = os_random_array()?;
        let k = Ed25519Key::from_seed(&seed);
        let cert = self_signed_cert(&k, &["localhost", "127.0.0.1", "::1"])?;
        let mut pkcs8 = ED25519_PKCS8_PREFIX.to_vec();
        pkcs8.extend_from_slice(&seed);
        write_private(&key, &pem("PRIVATE KEY", &pkcs8))?;
        std::fs::write(&crt, pem("CERTIFICATE", &cert))?;
        Ok(TlsConfig {
            key: k,
            chain: vec![cert],
        })
    }

    /// The leaf certificate as PEM (for clients that pin it, e.g. `sslrootcert`).
    pub fn certificate_pem(&self) -> String {
        pem("CERTIFICATE", &self.chain[0])
    }
}

#[cfg(unix)]
fn write_private(path: &Path, text: &str) -> io::Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    f.write_all(text.as_bytes())
}

#[cfg(not(unix))]
fn write_private(path: &Path, text: &str) -> io::Result<()> {
    std::fs::write(path, text)
}

// ── PEM / DER ──

fn pem(label: &str, der: &[u8]) -> String {
    let b64 = base64_encode(der);
    let mut out = format!("-----BEGIN {label}-----\n");
    for chunk in b64.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(chunk).unwrap_or_default());
        out.push('\n');
    }
    out.push_str(&format!("-----END {label}-----\n"));
    out
}

fn pem_blocks(text: &str, label: &str) -> Vec<Vec<u8>> {
    let (begin, end) = (
        format!("-----BEGIN {label}-----"),
        format!("-----END {label}-----"),
    );
    let mut out = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find(&begin) {
        let after = &rest[start + begin.len()..];
        let Some(stop) = after.find(&end) else { break };
        let body: String = after[..stop]
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();
        if let Some(der) = base64_decode(&body) {
            out.push(der);
        }
        rest = &after[stop + end.len()..];
    }
    out
}

/// `PrivateKeyInfo { version 0, AlgorithmIdentifier { id-Ed25519 }, OCTET STRING { OCTET STRING seed } }`.
const ED25519_PKCS8_PREFIX: [u8; 16] = [
    0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20,
];

fn ed25519_pkcs8_seed(der: &[u8]) -> Option<[u8; 32]> {
    (der.len() == 48 && der[..16] == ED25519_PKCS8_PREFIX)
        .then(|| der[16..].try_into().ok())
        .flatten()
}

fn der(tag: u8, content: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    let n = content.len();
    if n < 128 {
        out.push(n as u8);
    } else if n < 256 {
        out.extend_from_slice(&[0x81, n as u8]);
    } else {
        out.extend_from_slice(&[0x82, (n >> 8) as u8, n as u8]);
    }
    out.extend_from_slice(content);
    out
}

fn der_seq(parts: &[Vec<u8>]) -> Vec<u8> {
    der(0x30, &parts.concat())
}

/// `UTCTime` (before 2050) or `GeneralizedTime` for a Unix timestamp.
fn der_time(unix: i64) -> Vec<u8> {
    let days = unix.div_euclid(86_400);
    let secs = unix.rem_euclid(86_400);
    let (y, m, d) = crate::datum::civil_from_days(days as i32);
    let hms = format!("{:02}{:02}{:02}", secs / 3600, secs / 60 % 60, secs % 60);
    if (1950..2050).contains(&y) {
        der(0x17, format!("{:02}{m:02}{d:02}{hms}Z", y % 100).as_bytes())
    } else {
        der(0x18, format!("{y:04}{m:02}{d:02}{hms}Z").as_bytes())
    }
}

fn self_signed_cert(key: &Ed25519Key, names: &[&str]) -> io::Result<Vec<u8>> {
    let ed25519_alg = der_seq(&[der(0x06, &[0x2b, 0x65, 0x70])]);
    let name = der_seq(&[der(
        0x31,
        &der_seq(&[der(0x06, &[0x55, 0x04, 0x03]), der(0x0c, b"storine-sql")]),
    )]);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64);
    let validity = der_seq(&[der_time(now - 86_400), der_time(now + 3650 * 86_400)]);
    let mut pk = vec![0u8];
    pk.extend_from_slice(&key.public_key());
    let spki = der_seq(&[ed25519_alg.clone(), der(0x03, &pk)]);
    let mut serial: [u8; 16] = os_random_array()?;
    serial[0] &= 0x7f; // positive
    serial[0] |= 0x01; // and minimally encoded
                       // subjectAltName: dNSName [2] and iPAddress [7].
    let mut alt = Vec::new();
    for n in names {
        match n.parse::<std::net::IpAddr>() {
            Ok(std::net::IpAddr::V4(ip)) => alt.extend(der(0x87, &ip.octets())),
            Ok(std::net::IpAddr::V6(ip)) => alt.extend(der(0x87, &ip.octets())),
            Err(_) => alt.extend(der(0x82, n.as_bytes())),
        }
    }
    let ext = |oid: &[u8], critical: bool, value: Vec<u8>| {
        let mut parts = vec![der(0x06, oid)];
        if critical {
            parts.push(der(0x01, &[0xff]));
        }
        parts.push(der(0x04, &value));
        der_seq(&parts)
    };
    let extensions = der(
        0xa3,
        &der_seq(&[
            // basicConstraints: CA (so the certificate can be its own trust anchor), critical.
            ext(&[0x55, 0x1d, 0x13], true, der_seq(&[der(0x01, &[0xff])])),
            // keyUsage: digitalSignature, keyCertSign, critical.
            ext(&[0x55, 0x1d, 0x0f], true, der(0x03, &[0x02, 0x84])),
            ext(&[0x55, 0x1d, 0x11], false, der(0x30, &alt)),
        ]),
    );
    let tbs = der_seq(&[
        der(0xa0, &der(0x02, &[0x02])), // version v3
        der(0x02, &serial),
        ed25519_alg.clone(),
        name.clone(),
        validity,
        name,
        spki,
        extensions,
    ]);
    let sig = key.sign(&tbs);
    let mut sig_bits = vec![0u8];
    sig_bits.extend_from_slice(&sig);
    Ok(der_seq(&[tbs, ed25519_alg, der(0x03, &sig_bits)]))
}

// ── key schedule (SHA-256) ──

fn hkdf_extract(salt: &[u8], ikm: &[u8]) -> [u8; 32] {
    hmac_sha256(salt, ikm)
}

fn hkdf_expand_label(secret: &[u8; 32], label: &str, context: &[u8], len: usize) -> Vec<u8> {
    let full = format!("tls13 {label}");
    let mut info = Vec::with_capacity(4 + full.len() + context.len());
    info.extend_from_slice(&(len as u16).to_be_bytes());
    info.push(full.len() as u8);
    info.extend_from_slice(full.as_bytes());
    info.push(context.len() as u8);
    info.extend_from_slice(context);
    let mut out = Vec::with_capacity(len);
    let mut t: Vec<u8> = Vec::new();
    let mut i = 1u8;
    while out.len() < len {
        let mut msg = t.clone();
        msg.extend_from_slice(&info);
        msg.push(i);
        t = hmac_sha256(secret, &msg).to_vec();
        out.extend_from_slice(&t);
        i += 1;
    }
    out.truncate(len);
    out
}

fn derive_secret(secret: &[u8; 32], label: &str, transcript_hash: &[u8; 32]) -> [u8; 32] {
    let v = hkdf_expand_label(secret, label, transcript_hash, 32);
    v.try_into().unwrap_or([0; 32])
}

/// One direction's AEAD state.
struct Keys {
    secret: [u8; 32],
    key: [u8; 32],
    iv: [u8; 12],
    seq: u64,
    /// TLS 1.2 record protection (RFC 7905): the content type is in the record header and
    /// the additional data is `seq || type || version || length`.
    tls12: bool,
}

impl Keys {
    fn new(secret: [u8; 32]) -> Keys {
        let key = hkdf_expand_label(&secret, "key", &[], 32)
            .try_into()
            .unwrap_or([0; 32]);
        let iv = hkdf_expand_label(&secret, "iv", &[], 12)
            .try_into()
            .unwrap_or([0; 12]);
        Keys {
            secret,
            key,
            iv,
            seq: 0,
            tls12: false,
        }
    }

    fn tls12(key: [u8; 32], iv: [u8; 12]) -> Keys {
        Keys {
            secret: [0; 32],
            key,
            iv,
            seq: 0,
            tls12: true,
        }
    }

    /// TLS 1.2 additional data for a record of `ct` with `len` plaintext bytes.
    fn aad12(&self, ct: u8, len: usize) -> [u8; 13] {
        let mut aad = [0u8; 13];
        aad[..8].copy_from_slice(&self.seq.to_be_bytes());
        aad[8] = ct;
        aad[9..11].copy_from_slice(&TLS12.to_be_bytes());
        aad[11..].copy_from_slice(&(len as u16).to_be_bytes());
        aad
    }

    /// The next generation of these keys (`KeyUpdate`).
    fn updated(&self) -> Keys {
        Keys::new(derive_secret_noctx(&self.secret, "traffic upd"))
    }

    fn nonce(&self) -> [u8; 12] {
        let mut n = self.iv;
        for (b, s) in n[4..].iter_mut().zip(self.seq.to_be_bytes()) {
            *b ^= s;
        }
        n
    }

    /// One encrypted record carrying `content` of type `ct`.
    fn seal(&mut self, ct: u8, content: &[u8]) -> io::Result<Vec<u8>> {
        if self.seq == u64::MAX {
            return Err(tls_err("record sequence number exhausted"));
        }
        if self.tls12 {
            let mut rec = Vec::with_capacity(5 + content.len() + 16);
            rec.extend_from_slice(&[ct, 0x03, 0x03]);
            rec.extend_from_slice(&((content.len() + 16) as u16).to_be_bytes());
            rec.extend_from_slice(content);
            let aad = self.aad12(ct, content.len());
            let tag = seal_in_place(&self.key, &self.nonce(), &aad, &mut rec[5..]);
            rec.extend_from_slice(&tag);
            self.seq += 1;
            return Ok(rec);
        }
        let mut inner = Vec::with_capacity(content.len() + 1 + 16 + 5);
        inner.extend_from_slice(&[CT_APPLICATION_DATA, 0x03, 0x03, 0, 0]);
        inner.extend_from_slice(content);
        inner.push(ct);
        let len = (inner.len() - 5 + 16) as u16;
        inner[3..5].copy_from_slice(&len.to_be_bytes());
        let (header, body) = inner.split_at_mut(5);
        let tag = seal_in_place(&self.key, &self.nonce(), header, body);
        inner.extend_from_slice(&tag);
        self.seq += 1;
        Ok(inner)
    }

    /// Decrypts a record body in place; returns the inner content type and the content length.
    fn open(&mut self, header: &[u8; 5], body: &mut Vec<u8>) -> io::Result<u8> {
        if body.len() < 17 {
            return Err(alert_error(
                ALERT_DECODE_ERROR,
                "encrypted record too short",
            ));
        }
        if self.seq == u64::MAX {
            return Err(tls_err("record sequence number exhausted"));
        }
        let tag_at = body.len() - 16;
        let tag: [u8; 16] = body[tag_at..].try_into().unwrap_or([0; 16]);
        body.truncate(tag_at);
        if self.tls12 {
            let aad = self.aad12(header[0], body.len());
            open_in_place(&self.key, &self.nonce(), &aad, body, &tag)
                .map_err(|_| alert_error(ALERT_BAD_RECORD_MAC, "record authentication failed"))?;
            self.seq += 1;
            if body.len() > MAX_FRAGMENT {
                return Err(alert_error(ALERT_RECORD_OVERFLOW, "record too large"));
            }
            return Ok(header[0]);
        }
        open_in_place(&self.key, &self.nonce(), header, body, &tag)
            .map_err(|_| alert_error(ALERT_BAD_RECORD_MAC, "record authentication failed"))?;
        self.seq += 1;
        // Strip zero padding; the last non-zero byte is the real content type.
        while body.last() == Some(&0) {
            body.pop();
        }
        let ct = body
            .pop()
            .ok_or_else(|| alert_error(ALERT_UNEXPECTED_MESSAGE, "record without content type"))?;
        if body.len() > MAX_FRAGMENT {
            return Err(alert_error(ALERT_RECORD_OVERFLOW, "record too large"));
        }
        Ok(ct)
    }
}

fn derive_secret_noctx(secret: &[u8; 32], label: &str) -> [u8; 32] {
    hkdf_expand_label(secret, label, &[], 32)
        .try_into()
        .unwrap_or([0; 32])
}

/// An error that should be reported to the peer with a fatal alert.
#[derive(Debug)]
struct Alert(u8, String);

impl std::fmt::Display for Alert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TLS: {} (alert {})", self.1, self.0)
    }
}

impl std::error::Error for Alert {}

fn alert_error(code: u8, msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, Alert(code, msg.to_owned()))
}

fn alert_code(e: &io::Error) -> Option<u8> {
    e.get_ref()
        .and_then(|x| x.downcast_ref::<Alert>())
        .map(|a| a.0)
}

// ── record layer (handshake phase) ──

fn read_record(r: &mut impl Read) -> io::Result<([u8; 5], Vec<u8>)> {
    let mut header = [0u8; 5];
    r.read_exact(&mut header)?;
    let len = usize::from(u16::from_be_bytes([header[3], header[4]]));
    if len > MAX_CIPHERTEXT {
        return Err(alert_error(ALERT_RECORD_OVERFLOW, "record too large"));
    }
    if len == 0 && header[0] != CT_APPLICATION_DATA {
        return Err(alert_error(ALERT_DECODE_ERROR, "empty record"));
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body)?;
    Ok((header, body))
}

fn plain_record(ct: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + 5);
    out.push(ct);
    out.extend_from_slice(&[0x03, 0x03]);
    out.extend_from_slice(&(body.len() as u16).to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn handshake_msg(kind: u8, body: &[u8]) -> Vec<u8> {
    let mut m = vec![kind];
    m.extend_from_slice(&(body.len() as u32).to_be_bytes()[1..]);
    m.extend_from_slice(body);
    m
}

/// Reads handshake messages, reassembling them across records.
struct HandshakeReader {
    buf: Vec<u8>,
    /// See [`accept_tds7`]: application keys that may replace the client Finished, and the
    /// first application data once they did.
    app_keys: Option<Keys>,
    app_data: Option<Vec<u8>>,
}

impl HandshakeReader {
    /// The next complete handshake message (type, full message bytes), reading plaintext
    /// records (`keys == None`) or encrypted ones. Change-cipher-spec records are skipped.
    fn next(
        &mut self,
        r: &mut impl Read,
        mut keys: Option<&mut Keys>,
    ) -> io::Result<(u8, Vec<u8>)> {
        loop {
            if self.buf.len() >= 4 {
                let len = (usize::from(self.buf[1]) << 16)
                    | (usize::from(self.buf[2]) << 8)
                    | usize::from(self.buf[3]);
                if len > MAX_HANDSHAKE {
                    return Err(alert_error(
                        ALERT_DECODE_ERROR,
                        "handshake message too large",
                    ));
                }
                if self.buf.len() >= 4 + len {
                    let msg: Vec<u8> = self.buf.drain(..4 + len).collect();
                    return Ok((msg[0], msg));
                }
            }
            let (header, mut body) = read_record(r)?;
            match (header[0], keys.as_deref_mut()) {
                (CT_CHANGE_CIPHER_SPEC, _) => {
                    if body != [1] {
                        return Err(alert_error(
                            ALERT_UNEXPECTED_MESSAGE,
                            "bad change_cipher_spec",
                        ));
                    }
                }
                (CT_ALERT, None) => return Err(peer_alert(&body)),
                (CT_HANDSHAKE, None) => self.buf.extend_from_slice(&body),
                (CT_APPLICATION_DATA, Some(k)) if self.app_keys.is_some() => {
                    let mut hs_body = body.clone();
                    match k.open(&header, &mut hs_body) {
                        Ok(CT_HANDSHAKE) => self.buf.extend_from_slice(&hs_body),
                        Ok(CT_ALERT) => return Err(peer_alert(&hs_body)),
                        Ok(_) => {
                            return Err(alert_error(
                                ALERT_UNEXPECTED_MESSAGE,
                                "unexpected record during the handshake",
                            ))
                        }
                        Err(e) => {
                            // Not under the handshake keys: application data is the only other
                            // record that authenticates (it needs the same handshake secrets).
                            let app = self.app_keys.as_mut().ok_or(e)?;
                            if app.open(&header, &mut body)? != CT_APPLICATION_DATA {
                                return Err(alert_error(
                                    ALERT_UNEXPECTED_MESSAGE,
                                    "unexpected record during the handshake",
                                ));
                            }
                            self.app_data = Some(body);
                            return Ok((0, Vec::new()));
                        }
                    }
                }
                (CT_APPLICATION_DATA, Some(k)) => match k.open(&header, &mut body)? {
                    CT_HANDSHAKE => self.buf.extend_from_slice(&body),
                    CT_ALERT => return Err(peer_alert(&body)),
                    _ => {
                        return Err(alert_error(
                            ALERT_UNEXPECTED_MESSAGE,
                            "unexpected record during the handshake",
                        ))
                    }
                },
                _ => {
                    return Err(alert_error(
                        ALERT_UNEXPECTED_MESSAGE,
                        "unexpected record during the handshake",
                    ))
                }
            }
            if self.buf.len() > MAX_HANDSHAKE + 4 {
                return Err(alert_error(
                    ALERT_DECODE_ERROR,
                    "handshake message too large",
                ));
            }
        }
    }
}

fn peer_alert(body: &[u8]) -> io::Error {
    let desc = body.get(1).copied().unwrap_or(0);
    if desc == ALERT_CLOSE_NOTIFY {
        io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "TLS: peer closed the connection",
        )
    } else {
        io::Error::new(
            io::ErrorKind::ConnectionAborted,
            format!("TLS: peer sent alert {desc}"),
        )
    }
}

// ── ClientHello ──

struct Cursor<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> io::Result<&'a [u8]> {
        let s = self
            .b
            .get(self.pos..self.pos + n)
            .ok_or_else(|| alert_error(ALERT_DECODE_ERROR, "truncated ClientHello"))?;
        self.pos += n;
        Ok(s)
    }
    fn u8(&mut self) -> io::Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> io::Result<u16> {
        let s = self.take(2)?;
        Ok(u16::from_be_bytes([s[0], s[1]]))
    }
    fn vec8(&mut self) -> io::Result<&'a [u8]> {
        let n = usize::from(self.u8()?);
        self.take(n)
    }
    fn vec16(&mut self) -> io::Result<&'a [u8]> {
        let n = usize::from(self.u16()?);
        self.take(n)
    }
    fn done(&self) -> bool {
        self.pos == self.b.len()
    }
}

fn u16_list(b: &[u8]) -> Vec<u16> {
    b.chunks_exact(2)
        .map(|c| u16::from_be_bytes([c[0], c[1]]))
        .collect()
}

#[derive(Default)]
struct ClientHello {
    legacy_version: u16,
    random: [u8; 32],
    /// TLS 1.2: extended_master_secret offered; renegotiation_info (or its SCSV) offered.
    ems: bool,
    secure_renegotiation: bool,
    session_id: Vec<u8>,
    suites: Vec<u16>,
    versions: Vec<u16>,
    groups: Vec<u16>,
    sig_algs: Vec<u16>,
    x25519_share: Option<[u8; 32]>,
    has_key_share: bool,
    alpn: Vec<Vec<u8>>,
    has_alpn: bool,
}

fn parse_client_hello(msg: &[u8]) -> io::Result<ClientHello> {
    let mut c = Cursor {
        b: &msg[4..],
        pos: 0,
    };
    let mut ch = ClientHello {
        legacy_version: c.u16()?,
        random: c.take(32)?.try_into().unwrap_or([0; 32]),
        ..ClientHello::default()
    };
    ch.session_id = c.vec8()?.to_vec();
    if ch.session_id.len() > 32 {
        return Err(alert_error(ALERT_ILLEGAL_PARAMETER, "session id too long"));
    }
    ch.suites = u16_list(c.vec16()?);
    ch.secure_renegotiation = ch.suites.contains(&EMPTY_RENEGOTIATION_INFO_SCSV);
    let compression = c.vec8()?;
    if !compression.contains(&0) {
        return Err(alert_error(
            ALERT_ILLEGAL_PARAMETER,
            "the client offers no null compression",
        ));
    }
    if c.done() {
        return Ok(ch); // no extensions: not TLS 1.3
    }
    let mut ext = Cursor {
        b: c.vec16()?,
        pos: 0,
    };
    let mut seen = Vec::new();
    while !ext.done() {
        let kind = ext.u16()?;
        let data = ext.vec16()?;
        if seen.contains(&kind) {
            return Err(alert_error(ALERT_ILLEGAL_PARAMETER, "duplicate extension"));
        }
        seen.push(kind);
        let mut d = Cursor { b: data, pos: 0 };
        match kind {
            0x002b => ch.versions = u16_list(d.vec8()?),
            0x0017 => ch.ems = true,
            0xff01 => {
                if d.vec8()?.is_empty() {
                    ch.secure_renegotiation = true;
                } else {
                    return Err(alert_error(
                        ALERT_HANDSHAKE_FAILURE,
                        "renegotiation is not supported",
                    ));
                }
            }
            0x000a => ch.groups = u16_list(d.vec16()?),
            0x000d => ch.sig_algs = u16_list(d.vec16()?),
            0x0033 => {
                ch.has_key_share = true;
                let mut shares = Cursor {
                    b: d.vec16()?,
                    pos: 0,
                };
                while !shares.done() {
                    let group = shares.u16()?;
                    let key = shares.vec16()?;
                    if group == X25519 {
                        if key.len() != 32 {
                            return Err(alert_error(
                                ALERT_ILLEGAL_PARAMETER,
                                "bad x25519 key share",
                            ));
                        }
                        ch.x25519_share = key.try_into().ok();
                    }
                }
            }
            0x0010 => {
                ch.has_alpn = true;
                let mut list = Cursor {
                    b: d.vec16()?,
                    pos: 0,
                };
                while !list.done() {
                    ch.alpn.push(list.vec8()?.to_vec());
                }
            }
            _ => {}
        }
    }
    Ok(ch)
}

// ── the handshake ──

/// Runs a TLS 1.3 server handshake on `r` / `w` and returns the encrypted streams.
/// `alpn` is the application protocol to agree on when the client offers ALPN.
pub fn accept<R: Read, W: Write>(
    r: R,
    w: W,
    cfg: &TlsConfig,
    alpn: &str,
) -> io::Result<(TlsReader<R>, TlsWriter<W>)> {
    accept_inner(r, w, cfg, alpn, false)
}

/// [`accept`] for the TLS handshake that TDS 7.x carries in PRELOGIN packets. Some clients
/// (go-mssqldb) never send their TLS 1.3 Finished there: their wrapper only flushes a
/// handshake packet before its next read, which TLS 1.2 always had and TLS 1.3 does not.
/// Their first application record is accepted in its place: it decrypts only under keys
/// derived from the full handshake through the server Finished, which proves the same as
/// the client Finished would.
pub fn accept_tds7<R: Read, W: Write>(
    r: R,
    w: W,
    cfg: &TlsConfig,
) -> io::Result<(TlsReader<R>, TlsWriter<W>)> {
    accept_inner(r, w, cfg, "", true)
}

fn accept_inner<R: Read, W: Write>(
    mut r: R,
    mut w: W,
    cfg: &TlsConfig,
    alpn: &str,
    tds7: bool,
) -> io::Result<(TlsReader<R>, TlsWriter<W>)> {
    match handshake(&mut r, &mut w, cfg, alpn, tds7) {
        Ok((read_keys, write_keys, plain)) => {
            let update = Arc::new(AtomicBool::new(false));
            Ok((
                TlsReader {
                    inner: r,
                    keys: read_keys,
                    plain,
                    pos: 0,
                    update: Arc::clone(&update),
                    closed: false,
                },
                TlsWriter {
                    inner: w,
                    keys: write_keys,
                    update,
                },
            ))
        }
        Err(e) => {
            // Best effort: tell the client why (plaintext alert; keys may not exist yet).
            let code = alert_code(&e).unwrap_or(ALERT_INTERNAL_ERROR);
            if e.kind() == io::ErrorKind::InvalidData {
                let _ = w.write_all(&plain_record(CT_ALERT, &[2, code]));
                let _ = w.flush();
            }
            Err(e)
        }
    }
}

fn handshake<R: Read, W: Write>(
    r: &mut R,
    w: &mut W,
    cfg: &TlsConfig,
    alpn: &str,
    tds7: bool,
) -> io::Result<(Keys, Keys, Vec<u8>)> {
    let mut hs = HandshakeReader {
        buf: Vec::new(),
        app_keys: None,
        app_data: None,
    };
    let (kind, mut ch_msg) = hs.next(r, None)?;
    if kind != HS_CLIENT_HELLO {
        return Err(alert_error(
            ALERT_UNEXPECTED_MESSAGE,
            "expected ClientHello",
        ));
    }
    let mut ch = parse_client_hello(&ch_msg)?;
    if !ch.versions.contains(&TLS13) && ch.legacy_version >= TLS12 {
        return handshake12(r, w, cfg, alpn, &mut hs, &ch, &ch_msg);
    }
    check_offer(&ch)?;
    let mut transcript: Vec<u8> = Vec::new();
    if ch.x25519_share.is_none() {
        // HelloRetryRequest for x25519; the transcript restarts from a hash of ClientHello1.
        if !ch.groups.contains(&X25519) {
            return Err(alert_error(
                ALERT_HANDSHAKE_FAILURE,
                "the client does not support x25519",
            ));
        }
        let first_hash = sha256(&ch_msg);
        transcript.extend_from_slice(&handshake_msg(HS_MESSAGE_HASH, &first_hash));
        let hrr = server_hello(
            &HRR_RANDOM,
            &ch.session_id,
            &[
                ext(0x002b, &TLS13.to_be_bytes()),
                ext(0x0033, &X25519.to_be_bytes()),
            ],
        );
        transcript.extend_from_slice(&hrr);
        w.write_all(&plain_record(CT_HANDSHAKE, &hrr))?;
        w.write_all(&plain_record(CT_CHANGE_CIPHER_SPEC, &[1]))?;
        w.flush()?;
        let (kind, msg) = hs.next(r, None)?;
        if kind != HS_CLIENT_HELLO {
            return Err(alert_error(
                ALERT_UNEXPECTED_MESSAGE,
                "expected the second ClientHello",
            ));
        }
        let ch2 = parse_client_hello(&msg)?;
        check_offer(&ch2)?;
        if ch2.x25519_share.is_none() || ch2.session_id != ch.session_id {
            return Err(alert_error(
                ALERT_ILLEGAL_PARAMETER,
                "the second ClientHello has no x25519 share",
            ));
        }
        ch = ch2;
        ch_msg = msg;
    }
    let client_share = ch.x25519_share.unwrap_or([0; 32]);
    transcript.extend_from_slice(&ch_msg);
    // ALPN: agree on ours when offered; otherwise continue without (lenient on purpose).
    let selected_alpn =
        (ch.has_alpn && ch.alpn.iter().any(|p| p == alpn.as_bytes())).then_some(alpn);

    // ServerHello with our key share.
    let secret: [u8; 32] = os_random_array()?;
    let public = x25519_public(&secret);
    let shared = x25519(&secret, &client_share);
    if shared == [0; 32] {
        return Err(alert_error(ALERT_ILLEGAL_PARAMETER, "invalid x25519 share"));
    }
    let mut share = X25519.to_be_bytes().to_vec();
    share.extend_from_slice(&32u16.to_be_bytes());
    share.extend_from_slice(&public);
    let random: [u8; 32] = os_random_array()?;
    let sh = server_hello(
        &random,
        &ch.session_id,
        &[ext(0x002b, &TLS13.to_be_bytes()), ext(0x0033, &share)],
    );
    transcript.extend_from_slice(&sh);
    w.write_all(&plain_record(CT_HANDSHAKE, &sh))?;
    w.write_all(&plain_record(CT_CHANGE_CIPHER_SPEC, &[1]))?;

    // Handshake secrets.
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

    // EncryptedExtensions, Certificate, CertificateVerify, Finished.
    let mut ee = Vec::new();
    if let Some(p) = selected_alpn {
        let mut list = vec![p.len() as u8];
        list.extend_from_slice(p.as_bytes());
        let mut body = (list.len() as u16).to_be_bytes().to_vec();
        body.extend_from_slice(&list);
        ee.extend_from_slice(&ext(0x0010, &body));
    }
    let mut ee_body = (ee.len() as u16).to_be_bytes().to_vec();
    ee_body.extend_from_slice(&ee);
    let mut flight = handshake_msg(HS_ENCRYPTED_EXTENSIONS, &ee_body);

    let mut entries = Vec::new();
    for cert in &cfg.chain {
        entries.extend_from_slice(&(cert.len() as u32).to_be_bytes()[1..]);
        entries.extend_from_slice(cert);
        entries.extend_from_slice(&[0, 0]);
    }
    let mut cert_body = vec![0u8]; // empty certificate_request_context
    cert_body.extend_from_slice(&(entries.len() as u32).to_be_bytes()[1..]);
    cert_body.extend_from_slice(&entries);
    flight.extend_from_slice(&handshake_msg(HS_CERTIFICATE, &cert_body));

    let mut tx = transcript.clone();
    tx.extend_from_slice(&flight);
    let mut signed = vec![0x20u8; 64];
    signed.extend_from_slice(b"TLS 1.3, server CertificateVerify\0");
    signed.extend_from_slice(&sha256(&tx));
    let sig = cfg.key.sign(&signed);
    let mut cv = ED25519.to_be_bytes().to_vec();
    cv.extend_from_slice(&(sig.len() as u16).to_be_bytes());
    cv.extend_from_slice(&sig);
    flight.extend_from_slice(&handshake_msg(HS_CERTIFICATE_VERIFY, &cv));

    let mut tx = transcript.clone();
    tx.extend_from_slice(&flight);
    let s_finished_key = derive_secret_noctx(&s_hs, "finished");
    let s_verify = hmac_sha256(&s_finished_key, &sha256(&tx));
    flight.extend_from_slice(&handshake_msg(HS_FINISHED, &s_verify));
    transcript.extend_from_slice(&flight);
    for chunk in flight.chunks(MAX_FRAGMENT) {
        w.write_all(&s_keys.seal(CT_HANDSHAKE, chunk)?)?;
    }
    w.flush()?;

    // Application secrets (from the transcript through the server Finished).
    let th_server_finished = sha256(&transcript);
    let derived2 = derive_secret(&hs_secret, "derived", &empty_hash);
    let master = hkdf_extract(&derived2, &zeros);
    let c_ap = derive_secret(&master, "c ap traffic", &th_server_finished);
    let s_ap = derive_secret(&master, "s ap traffic", &th_server_finished);

    // The client's Finished.
    if tds7 {
        hs.app_keys = Some(Keys::new(c_ap));
    }
    let (kind, fin) = hs.next(r, Some(&mut c_keys))?;
    if let (Some(data), Some(keys)) = (hs.app_data.take(), hs.app_keys.take()) {
        return Ok((keys, Keys::new(s_ap), data));
    }
    if kind != HS_FINISHED {
        return Err(alert_error(
            ALERT_UNEXPECTED_MESSAGE,
            "expected the client Finished",
        ));
    }
    let c_finished_key = derive_secret_noctx(&c_hs, "finished");
    let expected = hmac_sha256(&c_finished_key, &th_server_finished);
    if fin.len() != 4 + 32 || !ct_eq(&fin[4..], &expected) {
        return Err(alert_error(
            ALERT_DECRYPT_ERROR,
            "client Finished does not verify",
        ));
    }
    if !hs.buf.is_empty() {
        return Err(alert_error(
            ALERT_UNEXPECTED_MESSAGE,
            "data after the client Finished in the same record",
        ));
    }
    Ok((Keys::new(c_ap), Keys::new(s_ap), Vec::new()))
}

// ── TLS 1.2 ──

/// TLS 1.2's PRF: P_SHA256(secret, label || seed).
fn prf(secret: &[u8], label: &str, seed: &[u8], len: usize) -> Vec<u8> {
    let mut ls = label.as_bytes().to_vec();
    ls.extend_from_slice(seed);
    let mut a = hmac_sha256(secret, &ls).to_vec();
    let mut out = Vec::with_capacity(len + 32);
    while out.len() < len {
        let mut m = a.clone();
        m.extend_from_slice(&ls);
        out.extend_from_slice(&hmac_sha256(secret, &m));
        a = hmac_sha256(secret, &a).to_vec();
    }
    out.truncate(len);
    out
}

/// A TLS 1.2 server handshake (after ClientHello): ServerHello, Certificate,
/// ServerKeyExchange (x25519 signed with ed25519), ServerHelloDone; then the client's
/// ClientKeyExchange, ChangeCipherSpec and Finished; then ours.
fn handshake12<R: Read, W: Write>(
    r: &mut R,
    w: &mut W,
    cfg: &TlsConfig,
    alpn: &str,
    hs: &mut HandshakeReader,
    ch: &ClientHello,
    ch_msg: &[u8],
) -> io::Result<(Keys, Keys, Vec<u8>)> {
    if !ch.suites.contains(&ECDHE_ECDSA_CHACHA20_POLY1305) {
        return Err(alert_error(
            ALERT_HANDSHAKE_FAILURE,
            "the client offers neither TLS 1.3 nor TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256",
        ));
    }
    if !ch.groups.contains(&X25519) {
        return Err(alert_error(
            ALERT_HANDSHAKE_FAILURE,
            "the client does not support x25519",
        ));
    }
    if !ch.sig_algs.contains(&ED25519) {
        return Err(alert_error(
            ALERT_HANDSHAKE_FAILURE,
            "the client does not accept ed25519 certificates",
        ));
    }
    let mut transcript = ch_msg.to_vec();
    let random: [u8; 32] = os_random_array()?;

    // ServerHello.
    let mut exts = Vec::new();
    if ch.secure_renegotiation {
        exts.push(ext(0xff01, &[0]));
    }
    if ch.ems {
        exts.push(ext(0x0017, &[]));
    }
    exts.push(ext(0x000b, &[1, 0])); // ec_point_formats: uncompressed
    if ch.has_alpn && ch.alpn.iter().any(|p| p == alpn.as_bytes()) {
        let mut list = vec![alpn.len() as u8];
        list.extend_from_slice(alpn.as_bytes());
        let mut body = (list.len() as u16).to_be_bytes().to_vec();
        body.extend_from_slice(&list);
        exts.push(ext(0x0010, &body));
    }
    let mut sh = TLS12.to_be_bytes().to_vec();
    sh.extend_from_slice(&random);
    sh.push(0); // no session id: no resumption
    sh.extend_from_slice(&ECDHE_ECDSA_CHACHA20_POLY1305.to_be_bytes());
    sh.push(0);
    let all = exts.concat();
    sh.extend_from_slice(&(all.len() as u16).to_be_bytes());
    sh.extend_from_slice(&all);
    let mut flight = handshake_msg(HS_SERVER_HELLO, &sh);

    // Certificate (TLS 1.2 form: no per-certificate extensions).
    let mut entries = Vec::new();
    for cert in &cfg.chain {
        entries.extend_from_slice(&(cert.len() as u32).to_be_bytes()[1..]);
        entries.extend_from_slice(cert);
    }
    let mut cert_body = (entries.len() as u32).to_be_bytes()[1..].to_vec();
    cert_body.extend_from_slice(&entries);
    flight.extend_from_slice(&handshake_msg(HS_CERTIFICATE, &cert_body));

    // ServerKeyExchange: named curve x25519, our public key, signed with the certificate key.
    let secret: [u8; 32] = os_random_array()?;
    let public = x25519_public(&secret);
    let mut params = vec![3u8];
    params.extend_from_slice(&X25519.to_be_bytes());
    params.push(32);
    params.extend_from_slice(&public);
    let mut signed = ch.random.to_vec();
    signed.extend_from_slice(&random);
    signed.extend_from_slice(&params);
    let sig = cfg.key.sign(&signed);
    let mut ske = params;
    ske.extend_from_slice(&ED25519.to_be_bytes());
    ske.extend_from_slice(&(sig.len() as u16).to_be_bytes());
    ske.extend_from_slice(&sig);
    flight.extend_from_slice(&handshake_msg(HS_SERVER_KEY_EXCHANGE, &ske));
    flight.extend_from_slice(&handshake_msg(HS_SERVER_HELLO_DONE, &[]));
    transcript.extend_from_slice(&flight);
    for chunk in flight.chunks(MAX_FRAGMENT) {
        w.write_all(&plain_record(CT_HANDSHAKE, chunk))?;
    }
    w.flush()?;

    // ClientKeyExchange.
    let (kind, cke) = hs.next(r, None)?;
    if kind != HS_CLIENT_KEY_EXCHANGE {
        return Err(alert_error(
            ALERT_UNEXPECTED_MESSAGE,
            "expected ClientKeyExchange",
        ));
    }
    let client_pub: [u8; 32] = cke
        .get(4..)
        .and_then(|b| b.split_first())
        .filter(|(n, rest)| **n == 32 && rest.len() == 32)
        .and_then(|(_, rest)| rest.try_into().ok())
        .ok_or_else(|| alert_error(ALERT_DECODE_ERROR, "bad ClientKeyExchange"))?;
    let shared = x25519(&secret, &client_pub);
    if shared == [0; 32] {
        return Err(alert_error(ALERT_ILLEGAL_PARAMETER, "invalid x25519 share"));
    }
    transcript.extend_from_slice(&cke);
    let master = if ch.ems {
        prf(&shared, "extended master secret", &sha256(&transcript), 48)
    } else {
        let mut seed = ch.random.to_vec();
        seed.extend_from_slice(&random);
        prf(&shared, "master secret", &seed, 48)
    };
    let mut seed = random.to_vec();
    seed.extend_from_slice(&ch.random);
    let block = prf(&master, "key expansion", &seed, 88);
    let arr32 = |b: &[u8]| -> [u8; 32] { b.try_into().unwrap_or([0; 32]) };
    let arr12 = |b: &[u8]| -> [u8; 12] { b.try_into().unwrap_or([0; 12]) };
    let mut c_keys = Keys::tls12(arr32(&block[..32]), arr12(&block[64..76]));
    let mut s_keys = Keys::tls12(arr32(&block[32..64]), arr12(&block[76..88]));

    // The client's ChangeCipherSpec, then its encrypted Finished.
    if !hs.buf.is_empty() {
        return Err(alert_error(
            ALERT_UNEXPECTED_MESSAGE,
            "handshake data before ChangeCipherSpec",
        ));
    }
    let (header, body) = read_record(r)?;
    match header[0] {
        CT_CHANGE_CIPHER_SPEC if body == [1] => {}
        CT_ALERT => return Err(peer_alert(&body)),
        _ => {
            return Err(alert_error(
                ALERT_UNEXPECTED_MESSAGE,
                "expected ChangeCipherSpec",
            ))
        }
    }
    let (header, mut body) = read_record(r)?;
    if header[0] != CT_HANDSHAKE || c_keys.open(&header, &mut body)? != CT_HANDSHAKE {
        return Err(alert_error(
            ALERT_UNEXPECTED_MESSAGE,
            "expected the client Finished",
        ));
    }
    let expected = prf(&master, "client finished", &sha256(&transcript), 12);
    if body.len() != 16 || body[0] != HS_FINISHED || !ct_eq(&body[4..], &expected) {
        return Err(alert_error(
            ALERT_DECRYPT_ERROR,
            "client Finished does not verify",
        ));
    }
    transcript.extend_from_slice(&body);

    // Ours.
    let verify = prf(&master, "server finished", &sha256(&transcript), 12);
    w.write_all(&plain_record(CT_CHANGE_CIPHER_SPEC, &[1]))?;
    w.write_all(&s_keys.seal(CT_HANDSHAKE, &handshake_msg(HS_FINISHED, &verify))?)?;
    w.flush()?;
    Ok((c_keys, s_keys, Vec::new()))
}

/// What every ClientHello (including the one after a HelloRetryRequest) must offer.
fn check_offer(ch: &ClientHello) -> io::Result<()> {
    if !ch.versions.contains(&TLS13) {
        return Err(alert_error(
            ALERT_PROTOCOL_VERSION,
            "the client does not offer TLS 1.3",
        ));
    }
    if !ch.suites.contains(&CHACHA20_POLY1305_SHA256) {
        return Err(alert_error(
            ALERT_HANDSHAKE_FAILURE,
            "the client does not offer TLS_CHACHA20_POLY1305_SHA256",
        ));
    }
    if !ch.sig_algs.contains(&ED25519) {
        return Err(alert_error(
            ALERT_HANDSHAKE_FAILURE,
            "the client does not accept ed25519 certificates",
        ));
    }
    if !ch.has_key_share {
        return Err(alert_error(
            ALERT_MISSING_EXTENSION,
            "no key_share extension",
        ));
    }
    Ok(())
}

fn ext(kind: u16, data: &[u8]) -> Vec<u8> {
    let mut e = kind.to_be_bytes().to_vec();
    e.extend_from_slice(&(data.len() as u16).to_be_bytes());
    e.extend_from_slice(data);
    e
}

fn server_hello(random: &[u8; 32], session_id: &[u8], exts: &[Vec<u8>]) -> Vec<u8> {
    let mut b = vec![0x03, 0x03];
    b.extend_from_slice(random);
    b.push(session_id.len() as u8);
    b.extend_from_slice(session_id);
    b.extend_from_slice(&CHACHA20_POLY1305_SHA256.to_be_bytes());
    b.push(0);
    let all = exts.concat();
    b.extend_from_slice(&(all.len() as u16).to_be_bytes());
    b.extend_from_slice(&all);
    handshake_msg(HS_SERVER_HELLO, &b)
}

// ── application data ──

/// The decrypting half of a TLS connection.
pub struct TlsReader<R> {
    inner: R,
    keys: Keys,
    plain: Vec<u8>,
    pos: usize,
    update: Arc<AtomicBool>,
    closed: bool,
}

impl<R> std::fmt::Debug for TlsReader<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsReader")
            .field("closed", &self.closed)
            .finish_non_exhaustive()
    }
}

impl<R> TlsReader<R> {
    /// The negotiated protocol version, for logs: `"TLS 1.3"` or `"TLS 1.2"`.
    pub fn version(&self) -> &'static str {
        if self.keys.tls12 {
            "TLS 1.2"
        } else {
            "TLS 1.3"
        }
    }

    /// The underlying stream, ending TLS in this direction (TDS login-only encryption).
    pub fn into_inner(self) -> R {
        self.inner
    }
}

impl<R: Read> Read for TlsReader<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        while self.pos >= self.plain.len() {
            if self.closed {
                return Ok(0);
            }
            let (header, mut body) = read_record(&mut self.inner)?;
            if header[0] == CT_CHANGE_CIPHER_SPEC {
                return Err(alert_error(
                    ALERT_UNEXPECTED_MESSAGE,
                    "change_cipher_spec after the handshake",
                ));
            }
            if header[0] != CT_APPLICATION_DATA && !self.keys.tls12 {
                return Err(alert_error(
                    ALERT_UNEXPECTED_MESSAGE,
                    "plaintext record after the handshake",
                ));
            }
            match self.keys.open(&header, &mut body)? {
                CT_APPLICATION_DATA => {
                    self.plain = body;
                    self.pos = 0;
                }
                CT_ALERT => {
                    if body.get(1) == Some(&ALERT_CLOSE_NOTIFY) {
                        self.closed = true;
                        return Ok(0);
                    }
                    return Err(peer_alert(&body));
                }
                CT_HANDSHAKE => self.post_handshake(&body)?,
                _ => {
                    return Err(alert_error(
                        ALERT_UNEXPECTED_MESSAGE,
                        "unexpected record type",
                    ))
                }
            }
        }
        let n = out.len().min(self.plain.len() - self.pos);
        out[..n].copy_from_slice(&self.plain[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

impl<R> TlsReader<R> {
    fn post_handshake(&mut self, body: &[u8]) -> io::Result<()> {
        // Only KeyUpdate is expected from a client after the handshake.
        let mut rest = body;
        while !rest.is_empty() {
            if rest.len() < 4 {
                return Err(alert_error(
                    ALERT_DECODE_ERROR,
                    "truncated handshake message",
                ));
            }
            let len =
                (usize::from(rest[1]) << 16) | (usize::from(rest[2]) << 8) | usize::from(rest[3]);
            let msg = rest.get(4..4 + len).ok_or_else(|| {
                alert_error(ALERT_DECODE_ERROR, "fragmented post-handshake message")
            })?;
            if rest[0] != HS_KEY_UPDATE || len != 1 || msg[0] > 1 {
                return Err(alert_error(
                    ALERT_UNEXPECTED_MESSAGE,
                    "unexpected post-handshake message",
                ));
            }
            self.keys = self.keys.updated();
            if msg[0] == 1 {
                self.update.store(true, Ordering::Release);
            }
            rest = &rest[4 + len..];
        }
        Ok(())
    }
}

/// The encrypting half of a TLS connection (wrap it in a `BufWriter`: every `write` call is
/// sent as at least one record).
pub struct TlsWriter<W> {
    inner: W,
    keys: Keys,
    update: Arc<AtomicBool>,
}

impl<W> std::fmt::Debug for TlsWriter<W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsWriter").finish_non_exhaustive()
    }
}

impl<W: Write> TlsWriter<W> {
    fn answer_key_update(&mut self) -> io::Result<()> {
        if self.update.swap(false, Ordering::AcqRel) {
            let record = self
                .keys
                .seal(CT_HANDSHAKE, &handshake_msg(HS_KEY_UPDATE, &[0]))?;
            self.inner.write_all(&record)?;
            self.keys = self.keys.updated();
        }
        Ok(())
    }

    /// Sends `close_notify`.
    /// The underlying stream, ending TLS in this direction (TDS login-only encryption).
    pub fn into_inner(self) -> W {
        self.inner
    }

    pub fn close(&mut self) -> io::Result<()> {
        let record = self.keys.seal(CT_ALERT, &[1, ALERT_CLOSE_NOTIFY])?;
        self.inner.write_all(&record)?;
        self.inner.flush()
    }
}

impl<W: Write> Write for TlsWriter<W> {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.answer_key_update()?;
        let n = data.len().min(MAX_FRAGMENT);
        let record = self.keys.seal(CT_APPLICATION_DATA, &data[..n])?;
        self.inner.write_all(&record)?;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.answer_key_update()?;
        self.inner.flush()
    }
}

/// Fills `buf` with OS randomness (used for nonces elsewhere in the gateway).
pub fn random(buf: &mut [u8]) -> io::Result<()> {
    os_random(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hkdf_expand_label_matches_rfc8448() {
        // RFC 8448 section 3: early secret and the "derived" secret of the 1-RTT handshake.
        let zeros = [0u8; 32];
        let early = hkdf_extract(&zeros, &zeros);
        assert_eq!(
            crate::hash::hex(&early),
            "33ad0a1c607ec03b09e6cd9893680ce210adf300aa1f2660e1b22e10f170f92a"
        );
        let derived = derive_secret(&early, "derived", &sha256(&[]));
        assert_eq!(
            crate::hash::hex(&derived),
            "6f2615a108c702c5678f54fc9dbab69716c076189c48250cebeac3576c3611ba"
        );
    }

    #[test]
    fn self_signed_certificate_signs_and_parses() {
        let cfg = TlsConfig::self_signed(&["localhost", "127.0.0.1"]).unwrap();
        let pem = cfg.certificate_pem();
        let back = pem_blocks(&pem, "CERTIFICATE");
        assert_eq!(back, cfg.chain);
        let seed = [7u8; 32];
        let mut pkcs8 = ED25519_PKCS8_PREFIX.to_vec();
        pkcs8.extend_from_slice(&seed);
        assert_eq!(ed25519_pkcs8_seed(&pkcs8), Some(seed));
        assert_eq!(ed25519_pkcs8_seed(&pkcs8[..47]), None);
    }
}

/// Hostile-input sweeps over the TLS record and handshake parsing (`TASK-135`).
#[cfg(test)]
mod fuzz_tests {
    use super::*;
    use crate::fuzz_support as fz;

    fn vec8(b: &[u8]) -> Vec<u8> {
        let mut v = vec![b.len() as u8];
        v.extend_from_slice(b);
        v
    }

    fn vec16(b: &[u8]) -> Vec<u8> {
        let mut v = (b.len() as u16).to_be_bytes().to_vec();
        v.extend_from_slice(b);
        v
    }

    fn u16s(list: &[u16]) -> Vec<u8> {
        list.iter().flat_map(|x| x.to_be_bytes()).collect()
    }

    /// A ClientHello record: TLS 1.3 (with or without an x25519 share) or TLS 1.2 only.
    fn client_hello(tls13: bool, share: bool) -> Vec<u8> {
        let mut body = TLS12.to_be_bytes().to_vec();
        body.extend_from_slice(&[7; 32]);
        body.extend_from_slice(&vec8(&[1; 32]));
        body.extend_from_slice(&vec16(&u16s(&[
            CHACHA20_POLY1305_SHA256,
            ECDHE_ECDSA_CHACHA20_POLY1305,
            EMPTY_RENEGOTIATION_INFO_SCSV,
        ])));
        body.extend_from_slice(&vec8(&[0]));
        let mut exts = Vec::new();
        if tls13 {
            exts.push(ext(0x002b, &vec8(&u16s(&[TLS13, TLS12]))));
        }
        exts.push(ext(0x000a, &vec16(&u16s(&[0x0017, X25519]))));
        exts.push(ext(0x000d, &vec16(&u16s(&[0x0403, ED25519]))));
        exts.push(ext(0x0017, &[]));
        exts.push(ext(0x0010, &vec16(&vec8(b"postgresql"))));
        if share {
            let mut ks = X25519.to_be_bytes().to_vec();
            ks.extend_from_slice(&vec16(&x25519_public(&[5; 32])));
            exts.push(ext(0x0033, &vec16(&ks)));
        }
        body.extend_from_slice(&vec16(&exts.concat()));
        plain_record(CT_HANDSHAKE, &handshake_msg(HS_CLIENT_HELLO, &body))
    }

    #[test]
    fn fuzz_client_hello_parsing() {
        let mut rng = fz::rng(0x544c_0001);
        let seeds = [
            client_hello(true, true),
            client_hello(true, false),
            client_hello(false, false),
        ];
        let ch = parse_client_hello(&seeds[0][5..]).ok();
        assert!(ch.is_some_and(|c| c.x25519_share.is_some() && c.versions.contains(&TLS13)));
        for _ in 0..fz::iters(300_000) {
            let pick = rng.below(seeds.len());
            let input = fz::variant(&mut rng, &seeds[pick]);
            fz::bounded(&input, || {
                let mut hs = HandshakeReader {
                    buf: Vec::new(),
                    app_keys: None,
                    app_data: None,
                };
                if let Ok((_, msg)) = hs.next(&mut &input[..], None) {
                    let _ = parse_client_hello(&msg);
                }
                if input.len() >= 4 {
                    let _ = parse_client_hello(&input);
                }
            });
        }
    }

    #[test]
    fn fuzz_handshakes() {
        let mut rng = fz::rng(0x544c_0002);
        let cfg = TlsConfig::self_signed(&["localhost"]).expect("certificate");
        let seeds = [
            client_hello(true, true),
            client_hello(true, false),
            client_hello(false, false),
        ];
        for _ in 0..fz::iters(5_000) {
            let pick = rng.below(seeds.len());
            let mut input = fz::variant(&mut rng, &seeds[pick]);
            // Sometimes a valid hello followed by hostile records.
            if rng.below(3) == 0 {
                input = seeds[pick].clone();
                let n = rng.below(64);
                let mut rec = vec![
                    [
                        CT_HANDSHAKE,
                        CT_APPLICATION_DATA,
                        CT_ALERT,
                        CT_CHANGE_CIPHER_SPEC,
                    ][rng.below(4)],
                    3,
                    3,
                ];
                rec.extend_from_slice(&(n as u16).to_be_bytes());
                rec.extend_from_slice(&fz::junk(&mut rng, n));
                fz::poke_len(&mut rng, &mut rec);
                input.extend_from_slice(&rec);
            }
            let tds7 = rng.below(2) == 0;
            fz::bounded(&input, || {
                let mut out = Vec::new();
                let _ = accept_inner(&input[..], &mut out, &cfg, "postgresql", tds7);
            });
        }
    }

    #[test]
    fn fuzz_application_records() {
        let mut rng = fz::rng(0x544c_0003);
        let mut keys = Keys::new([3; 32]);
        let mut stream = Vec::new();
        for (ct, body) in [
            (CT_APPLICATION_DATA, &b"hello"[..]),
            (CT_HANDSHAKE, &handshake_msg(HS_KEY_UPDATE, &[1])[..]),
            (CT_ALERT, &[1, ALERT_CLOSE_NOTIFY][..]),
        ] {
            stream.extend_from_slice(&keys.seal(ct, body).expect("seal"));
        }
        for _ in 0..fz::iters(100_000) {
            let input = fz::variant(&mut rng, &stream);
            fz::bounded(&input, || {
                let mut r = TlsReader {
                    inner: &input[..],
                    keys: Keys::new([3; 32]),
                    plain: Vec::new(),
                    pos: 0,
                    update: Arc::new(AtomicBool::new(false)),
                    closed: false,
                };
                let mut sink = Vec::new();
                let _ = r.read_to_end(&mut sink);
                let _ = r.post_handshake(&input);
                let _ = read_record(&mut &input[..]);
            });
        }
    }
}
