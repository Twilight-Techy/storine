//! AES-128 / AES-256 (FIPS 197) and the CBC ciphertext-stealing mode Kerberos uses (RFC 3962).
//!
//! On x86-64 with AES-NI (detected at run time) every block goes through the hardware
//! instructions, which run in constant time. Elsewhere a byte-oriented software version is
//! used, whose S-box lookups depend on secret data.

const SBOX: [u8; 256] = {
    // Generated from the multiplicative inverse in GF(2^8) and the affine map.
    let mut s = [0u8; 256];
    let mut p: u8 = 1;
    let mut q: u8 = 1;
    loop {
        // p * 3
        p = p ^ (p << 1) ^ if p & 0x80 != 0 { 0x1b } else { 0 };
        // q / 3
        q ^= q << 1;
        q ^= q << 2;
        q ^= q << 4;
        if q & 0x80 != 0 {
            q ^= 0x09;
        }
        let x = q ^ q.rotate_left(1) ^ q.rotate_left(2) ^ q.rotate_left(3) ^ q.rotate_left(4);
        s[p as usize] = x ^ 0x63;
        if p == 1 {
            break;
        }
    }
    s[0] = 0x63;
    s
};

const INV_SBOX: [u8; 256] = {
    let mut inv = [0u8; 256];
    let mut i = 0;
    while i < 256 {
        inv[SBOX[i] as usize] = i as u8;
        i += 1;
    }
    inv
};

fn xtime(x: u8) -> u8 {
    (x << 1) ^ if x & 0x80 != 0 { 0x1b } else { 0 }
}

fn gmul(mut a: u8, mut b: u8) -> u8 {
    let mut p = 0;
    while b != 0 {
        if b & 1 != 0 {
            p ^= a;
        }
        a = xtime(a);
        b >>= 1;
    }
    p
}

/// An expanded AES key (10 or 14 rounds).
#[derive(Clone)]
pub struct Aes {
    rounds: usize,
    /// Encryption round keys, 16 bytes each.
    enc: Vec<[u8; 16]>,
    #[cfg(target_arch = "x86_64")]
    ni: bool,
}

impl std::fmt::Debug for Aes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Aes({} rounds)", self.rounds)
    }
}

impl Drop for Aes {
    fn drop(&mut self) {
        for k in &mut self.enc {
            *k = [0; 16];
        }
    }
}

impl Aes {
    /// `key` is 16 or 32 bytes; `None` otherwise.
    pub fn new(key: &[u8]) -> Option<Aes> {
        let nk = match key.len() {
            16 => 4,
            32 => 8,
            _ => return None,
        };
        let rounds = nk + 6;
        let total = 4 * (rounds + 1);
        let mut w: Vec<[u8; 4]> = key.chunks(4).map(|c| [c[0], c[1], c[2], c[3]]).collect();
        let mut rcon = 1u8;
        for i in nk..total {
            let mut t = w[i - 1];
            if i % nk == 0 {
                t = [
                    SBOX[t[1] as usize] ^ rcon,
                    SBOX[t[2] as usize],
                    SBOX[t[3] as usize],
                    SBOX[t[0] as usize],
                ];
                rcon = xtime(rcon);
            } else if nk > 6 && i % nk == 4 {
                t = t.map(|b| SBOX[b as usize]);
            }
            let prev = w[i - nk];
            w.push([
                prev[0] ^ t[0],
                prev[1] ^ t[1],
                prev[2] ^ t[2],
                prev[3] ^ t[3],
            ]);
        }
        let enc = w
            .chunks(4)
            .map(|c| {
                let mut k = [0u8; 16];
                for (j, word) in c.iter().enumerate() {
                    k[j * 4..j * 4 + 4].copy_from_slice(word);
                }
                k
            })
            .collect();
        Some(Aes {
            rounds,
            enc,
            #[cfg(target_arch = "x86_64")]
            ni: std::arch::is_x86_feature_detected!("aes")
                && std::arch::is_x86_feature_detected!("sse2"),
        })
    }

    pub fn encrypt_block(&self, b: &mut [u8; 16]) {
        #[cfg(target_arch = "x86_64")]
        if self.ni {
            // SAFETY: AES-NI and SSE2 were detected at run time.
            unsafe { ni::encrypt(&self.enc, b) };
            return;
        }
        let k = &self.enc;
        xor16(b, &k[0]);
        for (r, key) in k.iter().enumerate().take(self.rounds + 1).skip(1) {
            for x in b.iter_mut() {
                *x = SBOX[*x as usize];
            }
            shift_rows(b);
            if r != self.rounds {
                mix_columns(b);
            }
            xor16(b, key);
        }
    }

    pub fn decrypt_block(&self, b: &mut [u8; 16]) {
        #[cfg(target_arch = "x86_64")]
        if self.ni {
            // SAFETY: AES-NI and SSE2 were detected at run time.
            unsafe { ni::decrypt(&self.enc, b) };
            return;
        }
        let k = &self.enc;
        xor16(b, &k[self.rounds]);
        for r in (0..self.rounds).rev() {
            inv_shift_rows(b);
            for x in b.iter_mut() {
                *x = INV_SBOX[*x as usize];
            }
            xor16(b, &k[r]);
            if r != 0 {
                inv_mix_columns(b);
            }
        }
    }

    /// CBC with ciphertext stealing as RFC 3962 defines it (the last two blocks swapped, the
    /// final one truncated); `data` must be at least one block. `iv` is updated as for CBC.
    pub fn cts_encrypt(&self, iv: &[u8; 16], data: &mut [u8]) -> Option<()> {
        let len = data.len();
        if len < 16 {
            return None;
        }
        if len == 16 {
            let mut b = block(data, 0);
            xor16(&mut b, iv);
            self.encrypt_block(&mut b);
            data.copy_from_slice(&b);
            return Some(());
        }
        let n = len.div_ceil(16);
        let r = len - 16 * (n - 1);
        let mut prev = *iv;
        let mut out = Vec::with_capacity(n * 16);
        for i in 0..n {
            let mut b = [0u8; 16];
            let end = (i * 16 + 16).min(len);
            b[..end - i * 16].copy_from_slice(&data[i * 16..end]);
            xor16(&mut b, &prev);
            self.encrypt_block(&mut b);
            out.extend_from_slice(&b);
            prev = b;
        }
        // E1 .. E(n-2), En, E(n-1) truncated to r bytes.
        let (head, tail) = out.split_at((n - 2) * 16);
        data[..head.len()].copy_from_slice(head);
        data[head.len()..head.len() + 16].copy_from_slice(&tail[16..32]);
        data[head.len() + 16..].copy_from_slice(&tail[..r]);
        Some(())
    }

    /// Inverse of [`Aes::cts_encrypt`].
    pub fn cts_decrypt(&self, iv: &[u8; 16], data: &mut [u8]) -> Option<()> {
        let len = data.len();
        if len < 16 {
            return None;
        }
        if len == 16 {
            let mut b = block(data, 0);
            self.decrypt_block(&mut b);
            xor16(&mut b, iv);
            data.copy_from_slice(&b);
            return Some(());
        }
        let n = len.div_ceil(16);
        let r = len - 16 * (n - 1);
        let mut prev = *iv;
        // Plain CBC for the first n - 2 blocks.
        for i in 0..n - 2 {
            let c = block(data, i * 16);
            let mut b = c;
            self.decrypt_block(&mut b);
            xor16(&mut b, &prev);
            data[i * 16..i * 16 + 16].copy_from_slice(&b);
            prev = c;
        }
        let at = (n - 2) * 16;
        let c_last_full = block(data, at); // E(n), encrypted last block
        let mut d = c_last_full;
        self.decrypt_block(&mut d);
        let mut tail = [0u8; 16];
        tail[..r].copy_from_slice(&data[at + 16..]);
        // The stolen bytes complete E(n-1).
        let mut c_prev = d;
        c_prev[..r].copy_from_slice(&tail[..r]);
        let mut p_last = d;
        xor16(&mut p_last, &c_prev);
        let mut p_prev = c_prev;
        self.decrypt_block(&mut p_prev);
        xor16(&mut p_prev, &prev);
        data[at..at + 16].copy_from_slice(&p_prev);
        data[at + 16..].copy_from_slice(&p_last[..r]);
        Some(())
    }
}

fn block(data: &[u8], at: usize) -> [u8; 16] {
    let mut b = [0u8; 16];
    b.copy_from_slice(&data[at..at + 16]);
    b
}

fn xor16(a: &mut [u8; 16], b: &[u8; 16]) {
    for (x, y) in a.iter_mut().zip(b) {
        *x ^= y;
    }
}

fn shift_rows(s: &mut [u8; 16]) {
    let t = *s;
    for c in 0..4 {
        for r in 0..4 {
            s[c * 4 + r] = t[((c + r) % 4) * 4 + r];
        }
    }
}

fn inv_shift_rows(s: &mut [u8; 16]) {
    let t = *s;
    for c in 0..4 {
        for r in 0..4 {
            s[((c + r) % 4) * 4 + r] = t[c * 4 + r];
        }
    }
}

fn mix_columns(s: &mut [u8; 16]) {
    for c in s.chunks_mut(4) {
        let (a0, a1, a2, a3) = (c[0], c[1], c[2], c[3]);
        let all = a0 ^ a1 ^ a2 ^ a3;
        c[0] ^= all ^ xtime(a0 ^ a1);
        c[1] ^= all ^ xtime(a1 ^ a2);
        c[2] ^= all ^ xtime(a2 ^ a3);
        c[3] ^= all ^ xtime(a3 ^ a0);
    }
}

fn inv_mix_columns(s: &mut [u8; 16]) {
    for c in s.chunks_mut(4) {
        let a = [c[0], c[1], c[2], c[3]];
        for i in 0..4 {
            c[i] = gmul(a[i], 14)
                ^ gmul(a[(i + 1) % 4], 11)
                ^ gmul(a[(i + 2) % 4], 13)
                ^ gmul(a[(i + 3) % 4], 9);
        }
    }
}

#[cfg(target_arch = "x86_64")]
mod ni {
    use std::arch::x86_64::*;

    #[target_feature(enable = "aes,sse2")]
    #[allow(unused_unsafe)]
    pub unsafe fn encrypt(keys: &[[u8; 16]], b: &mut [u8; 16]) {
        let rounds = keys.len() - 1;
        // SAFETY: every pointer is to a 16-byte array (unaligned `loadu` / `storeu` are allowed);
        // the caller detected AES-NI and SSE2, which this function enables.
        unsafe {
            let load = |k: &[u8; 16]| _mm_loadu_si128(k.as_ptr().cast());
            let mut s = _mm_xor_si128(_mm_loadu_si128(b.as_ptr().cast()), load(&keys[0]));
            for k in &keys[1..rounds] {
                s = _mm_aesenc_si128(s, load(k));
            }
            s = _mm_aesenclast_si128(s, load(&keys[rounds]));
            _mm_storeu_si128(b.as_mut_ptr().cast(), s);
        }
    }

    #[target_feature(enable = "aes,sse2")]
    #[allow(unused_unsafe)]
    pub unsafe fn decrypt(keys: &[[u8; 16]], b: &mut [u8; 16]) {
        let rounds = keys.len() - 1;
        // SAFETY: as in `encrypt`.
        unsafe {
            let load = |k: &[u8; 16]| _mm_loadu_si128(k.as_ptr().cast());
            let mut s = _mm_xor_si128(_mm_loadu_si128(b.as_ptr().cast()), load(&keys[rounds]));
            for k in keys[1..rounds].iter().rev() {
                s = _mm_aesdec_si128(s, _mm_aesimc_si128(load(k)));
            }
            s = _mm_aesdeclast_si128(s, load(&keys[0]));
            _mm_storeu_si128(b.as_mut_ptr().cast(), s);
        }
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

    fn both(key: &[u8]) -> Vec<Aes> {
        let hw = Aes::new(key).unwrap();
        let mut sw = hw.clone();
        #[cfg(target_arch = "x86_64")]
        {
            sw.ni = false;
        }
        vec![hw, sw]
    }

    #[test]
    fn fips197_vectors() {
        assert_eq!(SBOX[0x53], 0xed);
        assert_eq!(INV_SBOX[0xed], 0x53);
        let pt: [u8; 16] = unhex("00112233445566778899aabbccddeeff")
            .try_into()
            .unwrap();
        for (key, ct) in [
            (
                "000102030405060708090a0b0c0d0e0f",
                "69c4e0d86a7b0430d8cdb78070b4c55a",
            ),
            (
                "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
                "8ea2b7ca516745bfeafc49904b496089",
            ),
        ] {
            for aes in both(&unhex(key)) {
                let mut b = pt;
                aes.encrypt_block(&mut b);
                assert_eq!(b.to_vec(), unhex(ct));
                aes.decrypt_block(&mut b);
                assert_eq!(b, pt);
            }
        }
    }

    // RFC 3962 Appendix B.
    #[test]
    fn rfc3962_cts_vectors() {
        let key = unhex("636869636b656e207465726979616b69");
        let input = unhex("4920776f756c64206c696b652074686520 47656e6572616c20476175277320436869 636b656e2c20706c656173652c 20616e6420776f6e746f6e20736f75702e");
        let cases = [
            (17, "c6353568f2bf8cb4d8a580362da7ff7f 97"),
            (31, "fc00783e0efdb2c1d445d4c8eff7ed22 97687268d6ecccc0c07b25e25ecfe5"),
            (32, "39312523a78662d5be7fcbcc98ebf5a8 97687268d6ecccc0c07b25e25ecfe584"),
            (47, "97687268d6ecccc0c07b25e25ecfe584 b3fffd940c16a18c1b5549d2f838029e 39312523a78662d5be7fcbcc98ebf5"),
            (48, "97687268d6ecccc0c07b25e25ecfe584 9dad8bbb96c4cdc03bc103e1a194bbd8 39312523a78662d5be7fcbcc98ebf5a8"),
            (64, "97687268d6ecccc0c07b25e25ecfe584 39312523a78662d5be7fcbcc98ebf5a8 4807efe836ee89a526730dbc2f7bc840 9dad8bbb96c4cdc03bc103e1a194bbd8"),
        ];
        for aes in both(&key) {
            for (len, want) in cases {
                let mut buf = input[..len].to_vec();
                aes.cts_encrypt(&[0; 16], &mut buf).unwrap();
                assert_eq!(buf, unhex(want), "encrypt {len}");
                aes.cts_decrypt(&[0; 16], &mut buf).unwrap();
                assert_eq!(buf, input[..len], "decrypt {len}");
            }
        }
    }
}
