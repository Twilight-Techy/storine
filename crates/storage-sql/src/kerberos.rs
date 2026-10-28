//! Kerberos 5 service-side authentication (RFC 4120) for SQL Server integrated logins.
//!
//! The client presents a service ticket in an AP-REQ, wrapped as a GSS-API Kerberos token
//! (RFC 4121) and usually inside SPNEGO (RFC 4178). The ticket is decrypted with the service's
//! long-term key from a keytab, the authenticator with the ticket's session key, and, when the
//! client asks for mutual authentication, an AP-REP proves the server holds the key too. Any
//! realm works: MIT, Heimdal, FreeIPA, Samba AD, Active Directory.
//!
//! Encryption types: aes256-cts-hmac-sha1-96 (18), aes128-cts-hmac-sha1-96 (17) (RFC 3962),
//! aes128-cts-hmac-sha256-128 (19) and aes256-cts-hmac-sha384-192 (20) (RFC 8009). RC4 and DES
//! are refused.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::aes::Aes;
use crate::rsa::Hash;

/// Allowed clock skew between client, KDC and server (seconds), as in MIT's default.
const SKEW: i64 = 300;

const OID_KRB5: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x12, 0x01, 0x02, 0x02];
/// The OID Microsoft clients announce for Kerberos (1.2.840.48018.1.2.2).
const OID_MS_KRB5: &[u8] = &[0x2a, 0x86, 0x48, 0x82, 0xf7, 0x12, 0x01, 0x02, 0x02];
const OID_SPNEGO: &[u8] = &[0x2b, 0x06, 0x01, 0x05, 0x05, 0x02];
const OID_NTLM: &[u8] = &[0x2b, 0x06, 0x01, 0x04, 0x01, 0x82, 0x37, 0x02, 0x02, 0x0a];

// ── Crypto (RFC 3961 / 3962 / 8009) ──

fn hmac(hash: Hash, key: &[u8], msg: &[u8]) -> Vec<u8> {
    let block = match hash {
        Hash::Sha384 | Hash::Sha512 => 128,
        _ => 64,
    };
    let mut k = if key.len() > block {
        hash.digest(key)
    } else {
        key.to_vec()
    };
    k.resize(block, 0);
    let mut inner: Vec<u8> = k.iter().map(|b| b ^ 0x36).collect();
    inner.extend_from_slice(msg);
    let mut outer: Vec<u8> = k.iter().map(|b| b ^ 0x5c).collect();
    outer.extend_from_slice(&hash.digest(&inner));
    hash.digest(&outer)
}

/// RFC 3961 n-fold of `input` to `out_len` bytes.
fn nfold(input: &[u8], out_len: usize) -> Vec<u8> {
    let (inb, outb) = (input.len(), out_len);
    let gcd = {
        let (mut a, mut b) = (outb, inb);
        while b != 0 {
            (a, b) = (b, a % b);
        }
        a
    };
    let lcm = outb * inb / gcd;
    let mut out = vec![0u8; outb];
    let mut byte: u32 = 0;
    for i in (0..lcm).rev() {
        let msbit = ((inb << 3) - 1 + ((inb << 3) + 13) * (i / inb) + ((inb - (i % inb)) << 3))
            % (inb << 3);
        let hi = u32::from(input[((inb - 1) - (msbit >> 3)) % inb]);
        let lo = u32::from(input[(inb - (msbit >> 3)) % inb]);
        byte += ((hi << 8 | lo) >> ((msbit & 7) + 1)) & 0xff;
        byte += u32::from(out[i % outb]);
        out[i % outb] = byte as u8;
        byte >>= 8;
    }
    if byte != 0 {
        for o in out.iter_mut().rev() {
            byte += u32::from(*o);
            *o = byte as u8;
            byte >>= 8;
        }
    }
    out
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Etype {
    Aes128Sha1,
    Aes256Sha1,
    Aes128Sha256,
    Aes256Sha384,
}

impl Etype {
    fn from_id(id: i64) -> Option<Etype> {
        Some(match id {
            17 => Etype::Aes128Sha1,
            18 => Etype::Aes256Sha1,
            19 => Etype::Aes128Sha256,
            20 => Etype::Aes256Sha384,
            _ => return None,
        })
    }

    fn id(self) -> i64 {
        match self {
            Etype::Aes128Sha1 => 17,
            Etype::Aes256Sha1 => 18,
            Etype::Aes128Sha256 => 19,
            Etype::Aes256Sha384 => 20,
        }
    }

    fn key_len(self) -> usize {
        match self {
            Etype::Aes128Sha1 | Etype::Aes128Sha256 => 16,
            Etype::Aes256Sha1 | Etype::Aes256Sha384 => 32,
        }
    }

    /// Length of the integrity tag appended to the ciphertext.
    fn mac_len(self) -> usize {
        match self {
            Etype::Aes128Sha1 | Etype::Aes256Sha1 => 12,
            Etype::Aes128Sha256 => 16,
            Etype::Aes256Sha384 => 24,
        }
    }

    /// The encryption key Ke and integrity key Ki for `usage`.
    fn derive(self, base: &[u8], usage: u32) -> Option<(Aes, Vec<u8>)> {
        if base.len() != self.key_len() {
            return None;
        }
        let label = |b: u8| {
            let mut l = usage.to_be_bytes().to_vec();
            l.push(b);
            l
        };
        match self {
            Etype::Aes128Sha1 | Etype::Aes256Sha1 => {
                // DK(base, constant) = the AES "random-to-key" of chained single-block
                // encryptions of the n-folded constant (RFC 3961 §5.1, RFC 3962).
                let dk = |constant: &[u8]| -> Option<Vec<u8>> {
                    let aes = Aes::new(base)?;
                    let mut block: [u8; 16] = nfold(constant, 16).try_into().ok()?;
                    let mut out = Vec::with_capacity(32);
                    while out.len() < base.len() {
                        aes.encrypt_block(&mut block);
                        out.extend_from_slice(&block);
                    }
                    out.truncate(base.len());
                    Some(out)
                };
                Some((Aes::new(&dk(&label(0xaa))?)?, dk(&label(0x55))?))
            }
            Etype::Aes128Sha256 | Etype::Aes256Sha384 => {
                // KDF-HMAC-SHA2 (RFC 8009 §3).
                let (hash, ke_bits, ki_bits) = if self == Etype::Aes128Sha256 {
                    (Hash::Sha256, 128u32, 128u32)
                } else {
                    (Hash::Sha384, 256, 192)
                };
                let kdf = |l: &[u8], bits: u32| {
                    let mut msg = 1u32.to_be_bytes().to_vec();
                    msg.extend_from_slice(l);
                    msg.push(0);
                    msg.extend_from_slice(&bits.to_be_bytes());
                    let mut k = hmac(hash, base, &msg);
                    k.truncate(bits as usize / 8);
                    k
                };
                Some((
                    Aes::new(&kdf(&label(0xaa), ke_bits))?,
                    kdf(&label(0x55), ki_bits),
                ))
            }
        }
    }

    fn mac_hash(self) -> Hash {
        match self {
            Etype::Aes128Sha1 | Etype::Aes256Sha1 => Hash::Sha1,
            Etype::Aes128Sha256 => Hash::Sha256,
            Etype::Aes256Sha384 => Hash::Sha384,
        }
    }

    fn sha2(self) -> bool {
        matches!(self, Etype::Aes128Sha256 | Etype::Aes256Sha384)
    }

    /// Decrypts and authenticates `data` (confounder ‖ plaintext, then the tag).
    fn decrypt(self, key: &[u8], usage: u32, data: &[u8]) -> Option<Vec<u8>> {
        let (ke, ki) = self.derive(key, usage)?;
        let h = self.mac_len();
        if data.len() < 16 + h {
            return None;
        }
        let (ct, tag) = data.split_at(data.len() - h);
        let mut pt = ct.to_vec();
        ke.cts_decrypt(&[0; 16], &mut pt)?;
        let mac = if self.sha2() {
            let mut m = vec![0u8; 16]; // the initial cipher state
            m.extend_from_slice(ct);
            hmac(self.mac_hash(), &ki, &m)
        } else {
            hmac(self.mac_hash(), &ki, &pt)
        };
        if !crate::hash::ct_eq(&mac[..h], tag) {
            return None;
        }
        Some(pt.split_off(16))
    }

    fn encrypt(self, key: &[u8], usage: u32, plaintext: &[u8]) -> Option<Vec<u8>> {
        let (ke, ki) = self.derive(key, usage)?;
        let mut data = crate::hash::random_bytes(16);
        data.extend_from_slice(plaintext);
        let mac = if self.sha2() {
            None
        } else {
            Some(hmac(self.mac_hash(), &ki, &data))
        };
        ke.cts_encrypt(&[0; 16], &mut data)?;
        let mac = mac.unwrap_or_else(|| {
            let mut m = vec![0u8; 16];
            m.extend_from_slice(&data);
            hmac(self.mac_hash(), &ki, &m)
        });
        data.extend_from_slice(&mac[..self.mac_len()]);
        Some(data)
    }
}

// ── DER ──

/// One TLV: tag byte and contents.
struct Der<'a> {
    rest: &'a [u8],
}

impl<'a> Der<'a> {
    fn new(b: &'a [u8]) -> Der<'a> {
        Der { rest: b }
    }

    fn next(&mut self) -> Option<(u8, &'a [u8])> {
        let b = self.rest;
        let tag = *b.first()?;
        if tag & 0x1f == 0x1f {
            return None; // multi-byte tags do not occur in Kerberos
        }
        let first = *b.get(1)?;
        let (len, head) = if first < 0x80 {
            (usize::from(first), 2)
        } else {
            let n = usize::from(first & 0x7f);
            if n == 0 || n > 4 {
                return None;
            }
            let mut len = 0usize;
            for &x in b.get(2..2 + n)? {
                len = len << 8 | usize::from(x);
            }
            (len, 2 + n)
        };
        let body = b.get(head..head.checked_add(len)?)?;
        self.rest = &b[head + len..];
        Some((tag, body))
    }

    /// The contents of a TLV with `tag`.
    fn expect(&mut self, tag: u8) -> Option<&'a [u8]> {
        self.next().filter(|(t, _)| *t == tag).map(|(_, b)| b)
    }
}

/// The context-tagged fields `[n]` of a SEQUENCE body, by `n`.
fn fields(body: &[u8]) -> Option<HashMap<u8, &[u8]>> {
    let mut d = Der::new(body);
    let mut m = HashMap::new();
    while !d.rest.is_empty() {
        let (tag, inner) = d.next()?;
        if tag & 0xe0 != 0xa0 {
            return None;
        }
        m.insert(tag & 0x1f, inner);
    }
    Some(m)
}

/// The fields of `[APPLICATION app] SEQUENCE { ... }` (or a plain SEQUENCE when `app` is None).
fn app_fields(b: &[u8], app: Option<u8>) -> Option<HashMap<u8, &[u8]>> {
    let mut d = Der::new(b);
    let body = match app {
        Some(n) => Der::new(d.expect(0x60 | n)?).expect(0x30)?,
        None => d.expect(0x30)?,
    };
    fields(body)
}

fn int(b: &[u8]) -> Option<i64> {
    let v = Der::new(b).expect(0x02)?;
    if v.is_empty() || v.len() > 8 {
        return None;
    }
    let mut n: i64 = if v[0] & 0x80 != 0 { -1 } else { 0 };
    for &x in v {
        n = n << 8 | i64::from(x);
    }
    Some(n)
}

fn octets(b: &[u8]) -> Option<&[u8]> {
    Der::new(b).expect(0x04)
}

fn string(b: &[u8]) -> Option<String> {
    let (tag, v) = Der::new(b).next()?;
    // GeneralString (0x1b) is what Kerberos uses; accept the other string types too.
    matches!(tag, 0x1b | 0x0c | 0x13 | 0x16)
        .then(|| String::from_utf8(v.to_vec()).ok())
        .flatten()
}

/// KerberosTime (GeneralizedTime `YYYYMMDDHHMMSSZ`) as Unix seconds.
fn time(b: &[u8]) -> Option<i64> {
    let v = Der::new(b).expect(0x18)?;
    let s = std::str::from_utf8(v).ok()?;
    if s.len() != 15 || !s.ends_with('Z') {
        return None;
    }
    let n = |r: std::ops::Range<usize>| s.get(r)?.parse::<i64>().ok();
    let (y, mo, d) = (n(0..4)?, n(4..6)?, n(6..8)?);
    let (h, mi, se) = (n(8..10)?, n(10..12)?, n(12..14)?);
    // Days from the civil date (Howard Hinnant's algorithm).
    let y = if mo <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((mo + 9) % 12) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(days * 86_400 + h * 3600 + mi * 60 + se)
}

/// PrincipalName → (name type, `a/b` components).
fn principal(b: &[u8]) -> Option<String> {
    let f = app_fields(b, None)?;
    let mut d = Der::new(Der::new(f.get(&1)?).expect(0x30)?);
    let mut parts = Vec::new();
    while !d.rest.is_empty() {
        let (tag, v) = d.next()?;
        if tag != 0x1b && tag != 0x0c {
            return None;
        }
        parts.push(String::from_utf8(v.to_vec()).ok()?);
    }
    Some(parts.join("/"))
}

/// EncryptedData → (etype, kvno, cipher).
fn encrypted(b: &[u8]) -> Option<(i64, Option<i64>, &[u8])> {
    let f = app_fields(b, None)?;
    Some((
        int(f.get(&0)?)?,
        f.get(&1).and_then(|k| int(k)),
        octets(f.get(&2)?)?,
    ))
}

fn tlv(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    let n = body.len();
    if n < 0x80 {
        out.push(n as u8);
    } else {
        let bytes: Vec<u8> = n
            .to_be_bytes()
            .into_iter()
            .skip_while(|&b| b == 0)
            .collect();
        out.push(0x80 | bytes.len() as u8);
        out.extend_from_slice(&bytes);
    }
    out.extend_from_slice(body);
    out
}

fn der_int(n: i64) -> Vec<u8> {
    let mut bytes: Vec<u8> = n.to_be_bytes().to_vec();
    while bytes.len() > 1
        && ((bytes[0] == 0 && bytes[1] & 0x80 == 0) || (bytes[0] == 0xff && bytes[1] & 0x80 != 0))
    {
        bytes.remove(0);
    }
    tlv(0x02, &bytes)
}

fn ctx(n: u8, body: &[u8]) -> Vec<u8> {
    tlv(0xa0 | n, body)
}

/// A GSS-API initial-context token framing (RFC 2743 §3.1): `[APPLICATION 0] { OID, inner }`.
fn gss_frame(oid: &[u8], inner: &[u8]) -> Vec<u8> {
    let mut body = tlv(0x06, oid);
    body.extend_from_slice(inner);
    tlv(0x60, &body)
}

// ── Keytab ──

#[derive(Debug)]
struct KeytabEntry {
    principal: String,
    realm: String,
    kvno: u32,
    etype: i64,
    key: Vec<u8>,
}

impl Drop for KeytabEntry {
    fn drop(&mut self) {
        self.key.iter_mut().for_each(|b| *b = 0);
    }
}

/// Parses an MIT / Heimdal keytab (format 0x0502, also produced by `ktpass` and `adcli`).
fn parse_keytab(b: &[u8]) -> Result<Vec<KeytabEntry>, String> {
    let bad = || "not a keytab file (or an unsupported keytab version)".to_owned();
    if b.get(..2) != Some(&[0x05, 0x02]) {
        return Err(bad());
    }
    let mut at = 2;
    let mut out = Vec::new();
    let be32 = |i: usize| {
        b.get(i..i + 4)
            .map(|v| u32::from_be_bytes([v[0], v[1], v[2], v[3]]))
    };
    while at + 4 <= b.len() {
        let size = be32(at).ok_or_else(bad)? as i32;
        at += 4;
        if size < 0 {
            at += size.unsigned_abs() as usize; // a hole left by a deleted entry
            continue;
        }
        let end = at + size as usize;
        let e = b.get(at..end).ok_or_else(bad)?;
        let mut p = 0;
        let counted = |p: &mut usize| -> Option<Vec<u8>> {
            let n = usize::from(u16::from_be_bytes([*e.get(*p)?, *e.get(*p + 1)?]));
            let v = e.get(*p + 2..*p + 2 + n)?.to_vec();
            *p += 2 + n;
            Some(v)
        };
        let parse = |p: &mut usize| -> Option<KeytabEntry> {
            let comps = usize::from(u16::from_be_bytes([*e.first()?, *e.get(1)?]));
            *p = 2;
            let realm = String::from_utf8(counted(p)?).ok()?;
            let mut names = Vec::new();
            for _ in 0..comps {
                names.push(String::from_utf8(counted(p)?).ok()?);
            }
            *p += 4 + 4; // name type, timestamp
            let vno8 = u32::from(*e.get(*p)?);
            *p += 1;
            let etype = i64::from(u16::from_be_bytes([*e.get(*p)?, *e.get(*p + 1)?]));
            *p += 2;
            let key = counted(p)?;
            let kvno = match e.get(*p..*p + 4) {
                Some(v) => match u32::from_be_bytes([v[0], v[1], v[2], v[3]]) {
                    0 => vno8,
                    n => n,
                },
                None => vno8,
            };
            Some(KeytabEntry {
                principal: names.join("/"),
                realm,
                kvno,
                etype,
                key,
            })
        };
        out.push(parse(&mut p).ok_or_else(bad)?);
        at = end;
    }
    Ok(out)
}

// ── Acceptor ──

/// Kerberos settings: the service keytab, re-read whenever it changes.
#[derive(Debug)]
pub struct Kerberos {
    pub keytab: PathBuf,
    /// Keep the `@REALM` suffix of user names even for the service's own realm.
    pub keep_realm: bool,
    keys: Mutex<Option<(SystemTime, Arc<Vec<KeytabEntry>>)>>,
    /// Authenticators seen recently (replay cache): (client, ctime, cusec) → expiry.
    seen: Mutex<HashMap<(String, i64, i64), i64>>,
}

/// A successful login.
#[derive(Debug)]
pub struct Accepted {
    /// The session user: `name` for the service's realm, otherwise `name@REALM`.
    pub user: String,
    /// The token to return to the client (AP-REP, possibly in SPNEGO); empty for none.
    pub reply: Vec<u8>,
}

impl Kerberos {
    pub fn new(keytab: PathBuf, keep_realm: bool) -> Result<Kerberos, String> {
        let k = Kerberos {
            keytab,
            keep_realm,
            keys: Mutex::new(None),
            seen: Mutex::new(HashMap::new()),
        };
        let keys = k.keys()?;
        if !keys.iter().any(|e| Etype::from_id(e.etype).is_some()) {
            return Err(format!(
                "{}: no AES keys (aes256-cts-hmac-sha1-96, aes128-cts-hmac-sha1-96, \
                 aes256-cts-hmac-sha384-192 or aes128-cts-hmac-sha256-128)",
                k.keytab.display()
            ));
        }
        Ok(k)
    }

    /// The service principals in the keytab (`MSSQLSvc/host:port@REALM`).
    pub fn principals(&self) -> Vec<String> {
        let mut v: Vec<String> = self
            .keys()
            .map(|keys| {
                keys.iter()
                    .map(|e| format!("{}@{}", e.principal, e.realm))
                    .collect()
            })
            .unwrap_or_default();
        v.dedup();
        v
    }

    fn keys(&self) -> Result<Arc<Vec<KeytabEntry>>, String> {
        let path = &self.keytab;
        let modified = std::fs::metadata(path)
            .and_then(|m| m.modified())
            .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
        let mut cache = self.keys.lock().unwrap_or_else(|p| p.into_inner());
        if let Some((at, keys)) = cache.as_ref() {
            if *at == modified {
                return Ok(Arc::clone(keys));
            }
        }
        let bytes =
            std::fs::read(path).map_err(|e| format!("cannot read {}: {e}", path.display()))?;
        let keys = Arc::new(parse_keytab(&bytes).map_err(|e| format!("{}: {e}", path.display()))?);
        *cache = Some((modified, Arc::clone(&keys)));
        Ok(keys)
    }

    /// Verifies the client's first security token and returns the user and the reply token.
    pub fn accept(&self, token: &[u8]) -> Result<Accepted, String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs() as i64);
        self.accept_at(token, now)
    }

    fn accept_at(&self, token: &[u8], now: i64) -> Result<Accepted, String> {
        if token.starts_with(b"NTLMSSP\0") {
            return Err("NTLM is not supported; use Kerberos".into());
        }
        let malformed = || "malformed security token".to_owned();
        // Unwrap SPNEGO and the GSS-API framing down to the AP-REQ.
        let mut spnego_mech: Option<Vec<u8>> = None;
        let mut inner = token;
        if token.first() == Some(&0x60) {
            let mut d = Der::new(Der::new(token).expect(0x60).ok_or_else(malformed)?);
            let oid = d.expect(0x06).ok_or_else(malformed)?;
            if oid == OID_SPNEGO {
                let init = Der::new(d.expect(0xa0).ok_or_else(malformed)?)
                    .expect(0x30)
                    .ok_or_else(malformed)?;
                let f = fields(init).ok_or_else(malformed)?;
                let mut mechs = Der::new(
                    Der::new(f.get(&0).ok_or_else(malformed)?)
                        .expect(0x30)
                        .ok_or_else(malformed)?,
                );
                let first = mechs.expect(0x06).ok_or_else(malformed)?;
                if first != OID_KRB5 && first != OID_MS_KRB5 {
                    return Err(if first == OID_NTLM {
                        "the client offered NTLM; only Kerberos is supported (is there a \
                         Kerberos ticket for this server's service principal?)"
                            .into()
                    } else {
                        "the client did not offer Kerberos first; only Kerberos is supported".into()
                    });
                }
                spnego_mech = Some(first.to_vec());
                let mech_token = octets(f.get(&2).ok_or("the client sent no Kerberos token")?)
                    .ok_or_else(malformed)?;
                let mut g = Der::new(Der::new(mech_token).expect(0x60).ok_or_else(malformed)?);
                let oid = g.expect(0x06).ok_or_else(malformed)?;
                if oid != OID_KRB5 && oid != OID_MS_KRB5 {
                    return Err(malformed());
                }
                inner = g.rest;
            } else if oid == OID_KRB5 || oid == OID_MS_KRB5 {
                inner = d.rest;
            } else {
                return Err("unsupported security mechanism; only Kerberos is supported".into());
            }
            // RFC 4121 token id 01 00: KRB_AP_REQ.
            inner = inner.strip_prefix(&[0x01, 0x00]).ok_or_else(malformed)?;
        }
        let (user, ap_rep) = self.ap_req(inner, now)?;
        let reply = match (ap_rep, spnego_mech) {
            (rep, Some(mech)) => {
                // NegTokenResp: accept-completed, the client's mechanism, the AP-REP.
                let mut body = ctx(0, &tlv(0x0a, &[0]));
                body.extend(ctx(1, &tlv(0x06, &mech)));
                if let Some(rep) = rep {
                    body.extend(ctx(2, &tlv(0x04, &rep)));
                }
                ctx(1, &tlv(0x30, &body))
            }
            (Some(rep), None) => rep,
            (None, None) => Vec::new(),
        };
        Ok(Accepted { user, reply })
    }

    /// Checks an AP-REQ; returns the user and, if mutual authentication was asked for, the
    /// GSS-framed AP-REP.
    fn ap_req(&self, b: &[u8], now: i64) -> Result<(String, Option<Vec<u8>>), String> {
        let malformed = || "malformed Kerberos AP-REQ".to_owned();
        let f = app_fields(b, Some(14)).ok_or_else(malformed)?;
        if int(f.get(&0).ok_or_else(malformed)?) != Some(5)
            || int(f.get(&1).ok_or_else(malformed)?) != Some(14)
        {
            return Err(malformed());
        }
        let options = Der::new(f.get(&2).ok_or_else(malformed)?)
            .expect(0x03)
            .ok_or_else(malformed)?;
        // BIT STRING: unused-bits byte, then bit 1 use-session-key, bit 2 mutual-required.
        let flags = options.get(1).copied().unwrap_or(0);
        if flags & 0x40 != 0 {
            return Err("user-to-user tickets are not supported".into());
        }
        let mutual = flags & 0x20 != 0;
        // The ticket, decrypted with the service key.
        let t = app_fields(f.get(&3).ok_or_else(malformed)?, Some(1)).ok_or_else(malformed)?;
        let realm = string(t.get(&1).ok_or_else(malformed)?).ok_or_else(malformed)?;
        let sname = principal(t.get(&2).ok_or_else(malformed)?).ok_or_else(malformed)?;
        let (etype_id, kvno, cipher) =
            encrypted(t.get(&3).ok_or_else(malformed)?).ok_or_else(malformed)?;
        let etype = Etype::from_id(etype_id).ok_or_else(|| {
            format!(
                "the ticket uses encryption type {etype_id}; only AES (17, 18, 19, 20) is accepted"
            )
        })?;
        let keys = self.keys()?;
        let mut candidates: Vec<&KeytabEntry> = keys
            .iter()
            .filter(|e| e.etype == etype_id && kvno.is_none_or(|k| k as u32 == e.kvno))
            .collect();
        // The ticket's own principal first; then any other key of the same type (a keytab
        // holding the service under several host names).
        candidates.sort_by_key(|e| !(e.principal == sname && e.realm == realm));
        if candidates.is_empty() {
            return Err(format!(
                "no key for {sname}@{realm} (encryption type {etype_id}, kvno {}) in the keytab",
                kvno.map_or("-".into(), |k| k.to_string())
            ));
        }
        let enc_ticket = candidates
            .iter()
            .find_map(|e| etype.decrypt(&e.key, 2, cipher))
            .ok_or_else(|| {
                format!("the ticket for {sname}@{realm} does not decrypt with the keytab's key (an old kvno?)")
            })?;
        let et = app_fields(&enc_ticket, Some(3)).ok_or_else(malformed)?;
        let ticket_flags = Der::new(et.get(&0).ok_or_else(malformed)?)
            .expect(0x03)
            .ok_or_else(malformed)?;
        if ticket_flags.get(1).is_some_and(|b| b & 0x01 != 0) {
            return Err("the ticket is marked invalid".into()); // bit 7
        }
        let session = app_fields(et.get(&1).ok_or_else(malformed)?, None).ok_or_else(malformed)?;
        let session_etype =
            Etype::from_id(int(session.get(&0).ok_or_else(malformed)?).ok_or_else(malformed)?)
                .ok_or("the session key uses an unsupported encryption type")?;
        let session_key = octets(session.get(&1).ok_or_else(malformed)?).ok_or_else(malformed)?;
        let crealm = string(et.get(&2).ok_or_else(malformed)?).ok_or_else(malformed)?;
        let cname = principal(et.get(&3).ok_or_else(malformed)?).ok_or_else(malformed)?;
        let authtime = time(et.get(&5).ok_or_else(malformed)?).ok_or_else(malformed)?;
        let start = et.get(&6).and_then(|t| time(t)).unwrap_or(authtime);
        let end = time(et.get(&7).ok_or_else(malformed)?).ok_or_else(malformed)?;
        if now + SKEW < start {
            return Err("the ticket is not valid yet (check the clocks)".into());
        }
        if now - SKEW > end {
            return Err("the ticket has expired".into());
        }
        // The authenticator, decrypted with the session key.
        let (a_etype, _, a_cipher) =
            encrypted(f.get(&4).ok_or_else(malformed)?).ok_or_else(malformed)?;
        if a_etype != session_etype.id() {
            return Err(malformed());
        }
        let auth = session_etype
            .decrypt(session_key, 11, a_cipher)
            .ok_or("the authenticator does not decrypt with the ticket's session key")?;
        let a = app_fields(&auth, Some(2)).ok_or_else(malformed)?;
        let a_realm = string(a.get(&1).ok_or_else(malformed)?).ok_or_else(malformed)?;
        let a_name = principal(a.get(&2).ok_or_else(malformed)?).ok_or_else(malformed)?;
        if a_realm != crealm || a_name != cname {
            return Err("the authenticator names another client than the ticket".into());
        }
        let cusec_der = a.get(&4).ok_or_else(malformed)?;
        let ctime_der = a.get(&5).ok_or_else(malformed)?;
        let cusec = int(cusec_der).ok_or_else(malformed)?;
        let ctime = time(ctime_der).ok_or_else(malformed)?;
        if (now - ctime).abs() > SKEW {
            return Err(
                "the authenticator's time is too far from the server's (check the clocks)".into(),
            );
        }
        {
            let mut seen = self.seen.lock().unwrap_or_else(|p| p.into_inner());
            seen.retain(|_, exp| *exp > now);
            let key = (format!("{cname}@{crealm}"), ctime, cusec);
            if seen.contains_key(&key) {
                return Err("replayed authenticator".into());
            }
            seen.insert(key, ctime + 2 * SKEW);
        }
        let user = if crealm == realm && !self.keep_realm {
            cname
        } else {
            format!("{cname}@{crealm}")
        };
        if !mutual {
            return Ok((user, None));
        }
        // AP-REP { pvno 5, msg-type 15, enc-part EncAPRepPart { ctime, cusec } } under the
        // session key (key usage 12).
        let mut part = ctx(0, ctime_der);
        part.extend(ctx(1, cusec_der));
        let enc_part = tlv(0x7b, &tlv(0x30, &part));
        let sealed = session_etype
            .encrypt(session_key, 12, &enc_part)
            .ok_or("cannot seal the AP-REP")?;
        let mut ed = ctx(0, &der_int(session_etype.id()));
        ed.extend(ctx(2, &tlv(0x04, &sealed)));
        let mut rep = ctx(0, &der_int(5));
        rep.extend(ctx(1, &der_int(15)));
        rep.extend(ctx(2, &tlv(0x30, &ed)));
        let ap_rep = tlv(0x6f, &tlv(0x30, &rep));
        let mut inner = vec![0x02, 0x00]; // token id: KRB_AP_REP
        inner.extend(ap_rep);
        Ok((user, Some(gss_frame(OID_KRB5, &inner))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unhex(s: &str) -> Vec<u8> {
        let s: String = s.split_whitespace().collect();
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn rfc3961_nfold() {
        assert_eq!(nfold(b"012345", 8), unhex("be072631276b1955"));
        assert_eq!(nfold(b"password", 7), unhex("78a07b6caf85fa"));
        assert_eq!(
            nfold(b"kerberos", 16),
            unhex("6b65726265726f737b9b5b2b93132b93")
        );
        assert_eq!(nfold(b"kerberos", 8), unhex("6b65726265726f73"));
    }

    // RFC 8009 Appendix A: key derivation and an encryption with a known confounder.
    #[test]
    fn rfc8009_vectors() {
        let base = unhex("3705D96080C17728A0E800EAB6E0D23C");
        let (_, ki) = Etype::Aes128Sha256.derive(&base, 2).unwrap();
        assert_eq!(ki, unhex("9FDA0E56AB2D85E1569A688696C26A6C"));
        let base = unhex("6D404D37FAF79F9DF0D33568D320669800EB4836472EA8A026D16B7182460C52");
        let (_, ki) = Etype::Aes256Sha384.derive(&base, 2).unwrap();
        assert_eq!(
            ki,
            unhex("69B16514E3CD8E56B82010D5C73012B622C4D00FFC23ED1F")
        );
    }

    #[test]
    fn encrypt_decrypt_every_etype() {
        for etype in [
            Etype::Aes128Sha1,
            Etype::Aes256Sha1,
            Etype::Aes128Sha256,
            Etype::Aes256Sha384,
        ] {
            let key = vec![7u8; etype.key_len()];
            for len in [0, 1, 15, 16, 17, 100] {
                let msg: Vec<u8> = (0..len as u8).collect();
                let ct = etype.encrypt(&key, 12, &msg).unwrap();
                assert_eq!(etype.decrypt(&key, 12, &ct).unwrap(), msg);
                assert!(etype.decrypt(&key, 11, &ct).is_none(), "usage must matter");
                let mut bad = ct.clone();
                bad[3] ^= 1;
                assert!(etype.decrypt(&key, 12, &bad).is_none());
            }
        }
    }

    fn gtime(t: i64) -> Vec<u8> {
        let days = t.div_euclid(86_400);
        let secs = t.rem_euclid(86_400);
        // Civil date from days (inverse of `time`).
        let z = days + 719_468;
        let era = z.div_euclid(146_097);
        let doe = z - era * 146_097;
        let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let d = doy - (153 * mp + 2) / 5 + 1;
        let m = if mp < 10 { mp + 3 } else { mp - 9 };
        let y = yoe + era * 400 + i64::from(m <= 2);
        let s = format!(
            "{y:04}{m:02}{d:02}{:02}{:02}{:02}Z",
            secs / 3600,
            secs / 60 % 60,
            secs % 60
        );
        tlv(0x18, s.as_bytes())
    }

    fn name(parts: &[&str]) -> Vec<u8> {
        let mut strings = Vec::new();
        for p in parts {
            strings.extend(tlv(0x1b, p.as_bytes()));
        }
        let mut body = ctx(0, &der_int(1));
        body.extend(ctx(1, &tlv(0x30, &strings)));
        tlv(0x30, &body)
    }

    fn enc_data(etype: Etype, kvno: Option<i64>, cipher: &[u8]) -> Vec<u8> {
        let mut body = ctx(0, &der_int(etype.id()));
        if let Some(k) = kvno {
            body.extend(ctx(1, &der_int(k)));
        }
        body.extend(ctx(2, &tlv(0x04, cipher)));
        tlv(0x30, &body)
    }

    struct Client {
        etype: Etype,
        service_key: Vec<u8>,
        session_key: Vec<u8>,
        start: i64,
        end: i64,
        ctime: i64,
        cusec: i64,
        mutual: bool,
        service: Vec<&'static str>,
    }

    /// An AP-REQ as a KDC and a client would produce it, framed as GSS / SPNEGO.
    fn ap_req(c: &Client, spnego: bool) -> Vec<u8> {
        let mut key = ctx(0, &der_int(c.etype.id()));
        key.extend(ctx(1, &tlv(0x04, &c.session_key)));
        let mut etp = ctx(0, &tlv(0x03, &[0, 0x40, 0x81, 0, 0]));
        etp.extend(ctx(1, &tlv(0x30, &key)));
        etp.extend(ctx(2, &tlv(0x1b, b"STORINE.TEST")));
        etp.extend(ctx(3, &name(&["ann"])));
        etp.extend(ctx(
            4,
            &tlv(
                0x30,
                &[ctx(0, &der_int(1)), ctx(1, &tlv(0x04, b""))].concat(),
            ),
        ));
        etp.extend(ctx(5, &gtime(c.start)));
        etp.extend(ctx(6, &gtime(c.start)));
        etp.extend(ctx(7, &gtime(c.end)));
        let enc_ticket = c
            .etype
            .encrypt(&c.service_key, 2, &tlv(0x63, &tlv(0x30, &etp)))
            .unwrap();
        let mut ticket = ctx(0, &der_int(5));
        ticket.extend(ctx(1, &tlv(0x1b, b"STORINE.TEST")));
        ticket.extend(ctx(2, &name(&c.service)));
        ticket.extend(ctx(3, &enc_data(c.etype, Some(2), &enc_ticket)));
        let mut auth = ctx(0, &der_int(5));
        auth.extend(ctx(1, &tlv(0x1b, b"STORINE.TEST")));
        auth.extend(ctx(2, &name(&["ann"])));
        auth.extend(ctx(4, &der_int(c.cusec)));
        auth.extend(ctx(5, &gtime(c.ctime)));
        let enc_auth = c
            .etype
            .encrypt(&c.session_key, 11, &tlv(0x62, &tlv(0x30, &auth)))
            .unwrap();
        let mut req = ctx(0, &der_int(5));
        req.extend(ctx(1, &der_int(14)));
        req.extend(ctx(
            2,
            &tlv(0x03, &[0, if c.mutual { 0x20 } else { 0 }, 0, 0, 0]),
        ));
        req.extend(ctx(3, &tlv(0x61, &tlv(0x30, &ticket))));
        req.extend(ctx(4, &enc_data(c.etype, None, &enc_auth)));
        let mut inner = vec![0x01, 0x00];
        inner.extend(tlv(0x6e, &tlv(0x30, &req)));
        let krb = gss_frame(OID_KRB5, &inner);
        if !spnego {
            return krb;
        }
        let mut init = ctx(
            0,
            &tlv(0x30, &[tlv(0x06, OID_KRB5), tlv(0x06, OID_NTLM)].concat()),
        );
        init.extend(ctx(2, &tlv(0x04, &krb)));
        gss_frame(OID_SPNEGO, &ctx(0, &tlv(0x30, &init)))
    }

    fn keytab(entries: &[(&str, Etype, &[u8])]) -> Vec<u8> {
        let mut out = vec![0x05, 0x02];
        for (principal, etype, key) in entries {
            let mut e = Vec::new();
            let parts: Vec<&str> = principal.split('/').collect();
            e.extend((parts.len() as u16).to_be_bytes());
            let counted = |e: &mut Vec<u8>, s: &[u8]| {
                e.extend((s.len() as u16).to_be_bytes());
                e.extend_from_slice(s);
            };
            counted(&mut e, b"STORINE.TEST");
            for p in parts {
                counted(&mut e, p.as_bytes());
            }
            e.extend(1u32.to_be_bytes()); // name type
            e.extend(0u32.to_be_bytes()); // timestamp
            e.push(2); // kvno
            e.extend((etype.id() as u16).to_be_bytes());
            counted(&mut e, key);
            e.extend(2u32.to_be_bytes());
            out.extend((e.len() as i32).to_be_bytes());
            out.extend(e);
        }
        // A hole left by a deleted entry.
        out.extend((-6i32).to_be_bytes());
        out.extend([0u8; 6]);
        out
    }

    #[test]
    fn accepts_tickets_and_refuses_bad_ones() {
        let now = 1_800_000_000;
        let service_key = vec![0x42u8; 32];
        let path = std::env::temp_dir().join(format!("storine-krb5-{}.keytab", std::process::id()));
        std::fs::write(
            &path,
            keytab(&[
                (
                    "MSSQLSvc/db.storine.test:1433",
                    Etype::Aes256Sha1,
                    &service_key,
                ),
                (
                    "MSSQLSvc/db.storine.test:1433",
                    Etype::Aes256Sha384,
                    &service_key,
                ),
            ]),
        )
        .unwrap();
        let k = Kerberos::new(path.clone(), false).unwrap();
        assert_eq!(
            k.principals(),
            vec!["MSSQLSvc/db.storine.test:1433@STORINE.TEST"]
        );
        let base = |etype| Client {
            etype,
            service_key: service_key.clone(),
            session_key: vec![0x17u8; etype.key_len()],
            start: now - 100,
            end: now + 3600,
            ctime: now - 1,
            cusec: 1,
            mutual: true,
            service: vec!["MSSQLSvc", "db.storine.test:1433"],
        };
        for (i, etype) in [Etype::Aes256Sha1, Etype::Aes256Sha384]
            .into_iter()
            .enumerate()
        {
            for spnego in [false, true] {
                let c = Client {
                    cusec: (i * 2 + usize::from(spnego)) as i64,
                    ..base(etype)
                };
                let a = k.accept_at(&ap_req(&c, spnego), now).unwrap();
                assert_eq!(a.user, "ann");
                // The AP-REP decrypts under the session key and echoes the authenticator time.
                let rep = if spnego {
                    let resp = Der::new(&a.reply).expect(0xa1).unwrap();
                    let f = fields(Der::new(resp).expect(0x30).unwrap()).unwrap();
                    assert_eq!(f.get(&0).copied(), Some(&[0x0a, 1, 0][..]));
                    octets(f[&2]).unwrap().to_vec()
                } else {
                    a.reply.clone()
                };
                let mut g = Der::new(Der::new(&rep).expect(0x60).unwrap());
                assert_eq!(g.expect(0x06), Some(OID_KRB5));
                let ap = g.rest.strip_prefix(&[2, 0]).unwrap();
                let f = app_fields(ap, Some(15)).unwrap();
                let (_, _, cipher) = encrypted(f[&2]).unwrap();
                let part = etype.decrypt(&c.session_key, 12, cipher).unwrap();
                let p = app_fields(&part, Some(27)).unwrap();
                assert_eq!(time(p[&0]), Some(c.ctime));
                // The same authenticator again is a replay.
                let again = k.accept_at(&ap_req(&c, spnego), now).unwrap_err();
                assert!(again.contains("replay"), "{again}");
            }
        }
        let refuse = |c: Client, want: &str| {
            let e = k.accept_at(&ap_req(&c, true), now).unwrap_err();
            assert!(e.contains(want), "{e} (wanted {want})");
        };
        refuse(
            Client {
                cusec: 10,
                end: now - 400,
                ..base(Etype::Aes256Sha1)
            },
            "expired",
        );
        refuse(
            Client {
                cusec: 11,
                start: now + 400,
                ..base(Etype::Aes256Sha1)
            },
            "not valid yet",
        );
        refuse(
            Client {
                cusec: 12,
                ctime: now - 600,
                ..base(Etype::Aes256Sha1)
            },
            "too far",
        );
        refuse(
            Client {
                cusec: 13,
                service_key: vec![1; 32],
                ..base(Etype::Aes256Sha1)
            },
            "does not decrypt",
        );
        refuse(
            Client {
                cusec: 14,
                service_key: vec![1; 16],
                ..base(Etype::Aes128Sha1)
            },
            "no key",
        );
        // Without mutual authentication there is no AP-REP (SPNEGO still completes).
        let c = Client {
            cusec: 15,
            mutual: false,
            ..base(Etype::Aes256Sha1)
        };
        assert!(k
            .accept_at(&ap_req(&c, false), now)
            .unwrap()
            .reply
            .is_empty());
        assert!(k
            .accept_at(b"NTLMSSP\0\x01\0\0\0", now)
            .unwrap_err()
            .contains("NTLM"));
        assert!(k.accept_at(&[0x60, 0x03, 0x06, 0x01, 0x00], now).is_err());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn der_and_time() {
        assert_eq!(time(&gtime(1_793_217_464)), Some(1_793_217_464));
        assert_eq!(time(&tlv(0x18, b"19700101000000Z")), Some(0));
        assert_eq!(time(&tlv(0x18, b"20261028195744Z")), Some(1_793_217_464));
        assert_eq!(int(&der_int(-129)), Some(-129));
        assert_eq!(int(&der_int(65_537)), Some(65_537));
        let long = tlv(0x04, &[1u8; 300]);
        assert_eq!(octets(&long).map(<[u8]>::len), Some(300));
        assert!(Der::new(&long[..long.len() - 1]).next().is_none());
    }
}
