//! TLS application data for non-blocking sockets: the caller moves bytes, [`Session`] turns
//! records into plaintext and plaintext into records. The native server's reactor uses it after
//! a handshake run elsewhere ([`crate::accept_session`]), so no event loop thread ever blocks on
//! a slow client's handshake.

use std::io;

use crate::{
    alert_error, handshake_msg, peer_alert, Keys, ALERT_CLOSE_NOTIFY, ALERT_DECODE_ERROR, ALERT_RECORD_OVERFLOW,
    ALERT_UNEXPECTED_MESSAGE, CT_ALERT, CT_APPLICATION_DATA, CT_HANDSHAKE, HS_KEY_UPDATE, MAX_CIPHERTEXT,
    MAX_FRAGMENT,
};

/// The keys of an established connection, both directions.
pub struct Session {
    read: Keys,
    write: Keys,
    /// The peer asked for a `KeyUpdate` in return: answered before the next record we send.
    answer_update: bool,
    closed: bool,
    /// One record's body while it is decrypted (reused).
    scratch: Vec<u8>,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session").field("closed", &self.closed).finish_non_exhaustive()
    }
}

impl Session {
    pub(crate) fn new(read: Keys, write: Keys) -> Session {
        Session { read, write, answer_update: false, closed: false, scratch: Vec::new() }
    }

    /// Whether the peer sent `close_notify`.
    pub fn closed(&self) -> bool {
        self.closed
    }

    /// Decrypts every complete record at the front of `input` (removing them) and appends their
    /// application data to `out`. An incomplete record stays in `input` for the next call.
    pub fn decrypt(&mut self, input: &mut Vec<u8>, out: &mut Vec<u8>) -> io::Result<()> {
        let mut at = 0;
        while !self.closed && input.len() - at >= 5 {
            let header: [u8; 5] = input[at..at + 5].try_into().map_err(|_| alert_error(ALERT_DECODE_ERROR, "header"))?;
            let len = usize::from(u16::from_be_bytes([header[3], header[4]]));
            if len > MAX_CIPHERTEXT {
                return Err(alert_error(ALERT_RECORD_OVERFLOW, "record too large"));
            }
            if input.len() - at < 5 + len {
                break;
            }
            if header[0] != CT_APPLICATION_DATA && !self.read.tls12 {
                return Err(alert_error(ALERT_UNEXPECTED_MESSAGE, "plaintext record after the handshake"));
            }
            self.scratch.clear();
            self.scratch.extend_from_slice(&input[at + 5..at + 5 + len]);
            at += 5 + len;
            match self.read.open(&header, &mut self.scratch)? {
                CT_APPLICATION_DATA => out.extend_from_slice(&self.scratch),
                CT_ALERT => {
                    if self.scratch.get(1) == Some(&ALERT_CLOSE_NOTIFY) {
                        self.closed = true;
                    } else {
                        return Err(peer_alert(&self.scratch));
                    }
                }
                CT_HANDSHAKE => self.post_handshake()?,
                _ => return Err(alert_error(ALERT_UNEXPECTED_MESSAGE, "unexpected record type")),
            }
        }
        input.drain(..at);
        Ok(())
    }

    /// `KeyUpdate` is the only post-handshake message expected.
    fn post_handshake(&mut self) -> io::Result<()> {
        let mut rest = &self.scratch[..];
        let mut updates = Vec::new();
        while !rest.is_empty() {
            if rest.len() < 4 {
                return Err(alert_error(ALERT_DECODE_ERROR, "truncated handshake message"));
            }
            let len = (usize::from(rest[1]) << 16) | (usize::from(rest[2]) << 8) | usize::from(rest[3]);
            let msg = rest
                .get(4..4 + len)
                .ok_or_else(|| alert_error(ALERT_DECODE_ERROR, "fragmented post-handshake message"))?;
            if rest[0] != HS_KEY_UPDATE || len != 1 || msg[0] > 1 {
                return Err(alert_error(ALERT_UNEXPECTED_MESSAGE, "unexpected post-handshake message"));
            }
            updates.push(msg[0] == 1);
            rest = &rest[4 + len..];
        }
        for requested in updates {
            self.read = self.read.updated();
            self.answer_update |= requested;
        }
        Ok(())
    }

    /// Encrypts `data` into records appended to `out` (at most 16 KiB of data each).
    pub fn encrypt(&mut self, data: &[u8], out: &mut Vec<u8>) -> io::Result<()> {
        if self.answer_update {
            self.write.seal_into(CT_HANDSHAKE, &handshake_msg(HS_KEY_UPDATE, &[0]), out)?;
            self.write = self.write.updated();
            self.answer_update = false;
        }
        for chunk in data.chunks(MAX_FRAGMENT) {
            self.write.seal_into(CT_APPLICATION_DATA, chunk, out)?;
        }
        Ok(())
    }

    /// Encrypts the concatenation of `parts` into records appended to `out`, without first
    /// copying the parts together (a response's header and body become one record).
    pub fn encrypt_parts(&mut self, parts: &[&[u8]], out: &mut Vec<u8>) -> io::Result<()> {
        if let [one] = parts {
            return self.encrypt(one, out);
        }
        let total: usize = parts.iter().map(|p| p.len()).sum();
        if total > MAX_FRAGMENT {
            let joined = parts.concat();
            return self.encrypt(&joined, out);
        }
        if self.answer_update {
            self.encrypt(&[], out)?;
        }
        self.write.seal_parts_into(parts, out)
    }

    /// Appends `close_notify` to `out`.
    pub fn close(&mut self, out: &mut Vec<u8>) -> io::Result<()> {
        self.write.seal_into(CT_ALERT, &[1, ALERT_CLOSE_NOTIFY], out)
    }
}
