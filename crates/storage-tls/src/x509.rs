//! Just enough X.509 (RFC 5280) to check an Ed25519 server certificate: the signed bytes and
//! signature, issuer and subject, validity, the Ed25519 public key and the subject alternative
//! names. Every length is checked against the bytes present; anything unexpected is an error.

use std::io;

use storage_codec::curve25519::ed25519_verify;

/// `id-Ed25519` (1.3.101.112).
const OID_ED25519: &[u8] = &[0x2b, 0x65, 0x70];
/// `id-ce-subjectAltName` (2.5.29.17).
const OID_SUBJECT_ALT_NAME: &[u8] = &[0x55, 0x1d, 0x11];

fn bad(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("certificate: {msg}"))
}

/// One DER element: its tag, its content, and everything after it.
fn element(b: &[u8]) -> io::Result<(u8, &[u8], &[u8])> {
    let (&tag, rest) = b.split_first().ok_or_else(|| bad("truncated"))?;
    let (&first, rest) = rest.split_first().ok_or_else(|| bad("truncated"))?;
    let (len, rest) = if first < 0x80 {
        (usize::from(first), rest)
    } else {
        let n = usize::from(first & 0x7f);
        if n == 0 || n > 3 || rest.len() < n {
            return Err(bad("bad length"));
        }
        let len = rest[..n].iter().fold(0usize, |a, &x| (a << 8) | usize::from(x));
        (len, &rest[n..])
    };
    if rest.len() < len {
        return Err(bad("truncated"));
    }
    Ok((tag, &rest[..len], &rest[len..]))
}

fn expect(b: &[u8], tag: u8) -> io::Result<(&[u8], &[u8])> {
    let (t, content, rest) = element(b)?;
    if t != tag {
        return Err(bad("unexpected element"));
    }
    Ok((content, rest))
}

/// The parts of a certificate that verification needs.
#[derive(Debug)]
pub(crate) struct Certificate<'a> {
    /// The whole `tbsCertificate` element (what the issuer signed).
    tbs: &'a [u8],
    signature: [u8; 64],
    pub(crate) issuer: &'a [u8],
    pub(crate) subject: &'a [u8],
    /// Validity, in seconds since 1970.
    not_before: i64,
    not_after: i64,
    pub(crate) public_key: [u8; 32],
    dns_names: Vec<&'a [u8]>,
    ip_addresses: Vec<&'a [u8]>,
}

impl<'a> Certificate<'a> {
    pub(crate) fn parse(der: &'a [u8]) -> io::Result<Certificate<'a>> {
        let (cert, rest) = expect(der, 0x30)?;
        if !rest.is_empty() {
            return Err(bad("trailing bytes"));
        }
        let (tbs_tag, tbs_content, after_tbs) = element(cert)?;
        if tbs_tag != 0x30 {
            return Err(bad("no tbsCertificate"));
        }
        let tbs = &cert[..cert.len() - after_tbs.len()];
        let (sig_alg, after_alg) = expect(after_tbs, 0x30)?;
        let (alg_oid, _) = expect(sig_alg, 0x06)?;
        if alg_oid != OID_ED25519 {
            return Err(bad("not signed with Ed25519"));
        }
        let (sig_bits, _) = expect(after_alg, 0x03)?;
        let signature: [u8; 64] = match sig_bits.split_first() {
            Some((0, s)) if s.len() == 64 => s.try_into().map_err(|_| bad("bad signature"))?,
            _ => return Err(bad("bad signature")),
        };

        let mut b = tbs_content;
        // [0] version (optional), serial, signature algorithm.
        let (t, _, r) = element(b)?;
        if t == 0xa0 {
            b = r;
        }
        let (_, _, r) = element(b)?; // serial
        let (_, r) = expect(r, 0x30)?; // signature algorithm (checked above in the outer one)
        let (issuer_tag, issuer, r) = element(r)?;
        if issuer_tag != 0x30 {
            return Err(bad("bad issuer"));
        }
        let (validity, r) = expect(r, 0x30)?;
        let (nb_tag, nb, after_nb) = element(validity)?;
        let (na_tag, na, _) = element(after_nb)?;
        let not_before = parse_time(nb_tag, nb)?;
        let not_after = parse_time(na_tag, na)?;
        let (subject, r) = expect(r, 0x30)?;
        let (spki, mut r) = expect(r, 0x30)?;
        let (key_alg, after_key_alg) = expect(spki, 0x30)?;
        let (key_oid, _) = expect(key_alg, 0x06)?;
        if key_oid != OID_ED25519 {
            return Err(bad("public key is not Ed25519"));
        }
        let (key_bits, _) = expect(after_key_alg, 0x03)?;
        let public_key: [u8; 32] = match key_bits.split_first() {
            Some((0, k)) if k.len() == 32 => k.try_into().map_err(|_| bad("bad public key"))?,
            _ => return Err(bad("bad public key")),
        };
        let mut dns_names = Vec::new();
        let mut ip_addresses = Vec::new();
        while !r.is_empty() {
            let (t, content, rest) = element(r)?;
            r = rest;
            if t != 0xa3 {
                continue; // issuer/subject unique ids
            }
            let (mut exts, _) = expect(content, 0x30)?;
            while !exts.is_empty() {
                let (e, rest) = expect(exts, 0x30)?;
                exts = rest;
                let (oid, mut fields) = expect(e, 0x06)?;
                if let Ok((0x01, _, after_critical)) = element(fields) {
                    fields = after_critical;
                }
                let (value, _) = expect(fields, 0x04)?;
                if oid == OID_SUBJECT_ALT_NAME {
                    let (mut names, _) = expect(value, 0x30)?;
                    while !names.is_empty() {
                        let (t, n, rest) = element(names)?;
                        names = rest;
                        match t {
                            0x82 => dns_names.push(n),
                            0x87 => ip_addresses.push(n),
                            _ => {}
                        }
                    }
                }
            }
        }
        Ok(Certificate {
            tbs,
            signature,
            issuer,
            subject,
            not_before,
            not_after,
            public_key,
            dns_names,
            ip_addresses,
        })
    }

    /// Whether `issuer_key` signed this certificate.
    pub(crate) fn signed_by(&self, issuer_key: &[u8; 32]) -> bool {
        ed25519_verify(issuer_key, self.tbs, &self.signature)
    }

    /// Whether `now` (seconds since 1970) is within the validity period.
    pub(crate) fn valid_at(&self, now: i64) -> bool {
        self.not_before <= now && now <= self.not_after
    }

    /// Whether the certificate names `host`: an IP address against its IP SANs, otherwise a
    /// DNS name against its DNS SANs (case-insensitive; `*.` matches exactly one label).
    pub(crate) fn names(&self, host: &str) -> bool {
        if let Ok(ip) = host.trim_start_matches('[').trim_end_matches(']').parse::<std::net::IpAddr>() {
            let octets = match ip {
                std::net::IpAddr::V4(v4) => v4.octets().to_vec(),
                std::net::IpAddr::V6(v6) => v6.octets().to_vec(),
            };
            return self.ip_addresses.contains(&octets.as_slice());
        }
        let host = host.trim_end_matches('.').to_ascii_lowercase();
        self.dns_names.iter().any(|n| {
            let Ok(n) = std::str::from_utf8(n) else { return false };
            let n = n.to_ascii_lowercase();
            match n.strip_prefix("*.") {
                Some(suffix) => host
                    .split_once('.')
                    .is_some_and(|(label, rest)| !label.is_empty() && rest == suffix),
                None => n == host,
            }
        })
    }
}

/// UTCTime (`YYMMDDHHMMSSZ`) or GeneralizedTime (`YYYYMMDDHHMMSSZ`) as seconds since 1970.
fn parse_time(tag: u8, t: &[u8]) -> io::Result<i64> {
    let digits = |s: &[u8]| -> io::Result<i64> {
        s.iter().try_fold(0i64, |a, &c| {
            if c.is_ascii_digit() {
                Ok(a * 10 + i64::from(c - b'0'))
            } else {
                Err(bad("bad time"))
            }
        })
    };
    let (year, rest) = match (tag, t.len()) {
        (0x17, 13) => {
            let yy = digits(&t[..2])?;
            (if yy >= 50 { 1900 + yy } else { 2000 + yy }, &t[2..])
        }
        (0x18, 15) => (digits(&t[..4])?, &t[4..]),
        _ => return Err(bad("unsupported time format")),
    };
    if rest.last() != Some(&b'Z') {
        return Err(bad("time not in UTC"));
    }
    let (mo, d, h, mi, s) = (
        digits(&rest[0..2])?,
        digits(&rest[2..4])?,
        digits(&rest[4..6])?,
        digits(&rest[6..8])?,
        digits(&rest[8..10])?,
    );
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) || h > 23 || mi > 59 || s > 60 {
        return Err(bad("bad time"));
    }
    Ok(days_from_civil(year, mo, d) * 86_400 + h * 3600 + mi * 60 + s)
}

/// Days since 1970-01-01 of a civil date (Howard Hinnant's `days_from_civil`).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}
