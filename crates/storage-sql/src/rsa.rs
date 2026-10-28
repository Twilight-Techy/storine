//! RSA signature verification (RSASSA-PKCS1-v1_5, RFC 8017 §8.2.2) over a small unsigned
//! big-integer type. Verification only, with public values, so nothing here needs to run in
//! constant time.

/// An unsigned integer as little-endian 64-bit limbs, without leading zero limbs.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Big(Vec<u64>);

impl Big {
    fn from_be(bytes: &[u8]) -> Big {
        let mut limbs = Vec::with_capacity(bytes.len() / 8 + 1);
        for chunk in bytes.rchunks(8) {
            let mut w = [0u8; 8];
            w[8 - chunk.len()..].copy_from_slice(chunk);
            limbs.push(u64::from_be_bytes(w));
        }
        let mut b = Big(limbs);
        b.trim();
        b
    }

    /// Big-endian bytes, left-padded to `len`; `None` when the value does not fit.
    fn to_be(&self, len: usize) -> Option<Vec<u8>> {
        let mut out = vec![0u8; len];
        for (i, limb) in self.0.iter().enumerate() {
            for (j, byte) in limb.to_le_bytes().iter().enumerate() {
                let at = i * 8 + j;
                if at >= len {
                    if *byte != 0 {
                        return None;
                    }
                } else {
                    out[len - 1 - at] = *byte;
                }
            }
        }
        Some(out)
    }

    fn trim(&mut self) {
        while self.0.last() == Some(&0) {
            self.0.pop();
        }
    }

    fn bits(&self) -> usize {
        self.0
            .last()
            .map_or(0, |top| self.0.len() * 64 - top.leading_zeros() as usize)
    }

    fn bit(&self, i: usize) -> bool {
        self.0.get(i / 64).is_some_and(|w| w >> (i % 64) & 1 == 1)
    }

    fn cmp(&self, o: &Big) -> std::cmp::Ordering {
        self.0
            .len()
            .cmp(&o.0.len())
            .then_with(|| self.0.iter().rev().cmp(o.0.iter().rev()))
    }

    fn mul(&self, o: &Big) -> Big {
        let mut out = vec![0u64; self.0.len() + o.0.len()];
        for (i, &a) in self.0.iter().enumerate() {
            let mut carry = 0u128;
            for (j, &b) in o.0.iter().enumerate() {
                let t = u128::from(a) * u128::from(b) + u128::from(out[i + j]) + carry;
                out[i + j] = t as u64;
                carry = t >> 64;
            }
            out[i + o.0.len()] = carry as u64;
        }
        let mut b = Big(out);
        b.trim();
        b
    }

    /// `self mod n` by binary long division (n > 0).
    fn rem(&self, n: &Big) -> Big {
        let mut r = Big(Vec::with_capacity(n.0.len() + 1));
        for i in (0..self.bits()).rev() {
            // r = 2r + bit
            let mut carry = u64::from(self.bit(i));
            for w in r.0.iter_mut() {
                let next = *w >> 63;
                *w = *w << 1 | carry;
                carry = next;
            }
            if carry != 0 {
                r.0.push(carry);
            }
            if r.cmp(n) != std::cmp::Ordering::Less {
                r.sub_assign(n);
            }
        }
        r
    }

    fn sub_assign(&mut self, o: &Big) {
        let mut borrow = 0u64;
        for i in 0..self.0.len() {
            let b = o.0.get(i).copied().unwrap_or(0);
            let (d1, o1) = self.0[i].overflowing_sub(b);
            let (d2, o2) = d1.overflowing_sub(borrow);
            self.0[i] = d2;
            borrow = u64::from(o1 || o2);
        }
        self.trim();
    }

    fn pow_mod(&self, e: &Big, n: &Big) -> Big {
        let mut result = Big(vec![1]).rem(n);
        let base = self.rem(n);
        for i in (0..e.bits()).rev() {
            result = result.mul(&result).rem(n);
            if e.bit(i) {
                result = result.mul(&base).rem(n);
            }
        }
        result
    }
}

/// The hash a signature covers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Hash {
    Sha1,
    Sha256,
    Sha384,
    Sha512,
}

impl Hash {
    pub fn digest(self, data: &[u8]) -> Vec<u8> {
        match self {
            Hash::Sha1 => crate::hash::sha1(data).to_vec(),
            Hash::Sha256 => crate::hash::sha256(data).to_vec(),
            Hash::Sha384 => storage_codec::sha512::sha384(data).to_vec(),
            Hash::Sha512 => storage_codec::sha512::sha512(&[data]).to_vec(),
        }
    }

    /// DER `DigestInfo` header that precedes the digest in an EMSA-PKCS1-v1_5 encoding.
    fn digest_info(self) -> &'static [u8] {
        match self {
            Hash::Sha1 => &[
                0x30, 0x21, 0x30, 0x09, 0x06, 0x05, 0x2b, 0x0e, 0x03, 0x02, 0x1a, 0x05, 0x00, 0x04,
                0x14,
            ],
            Hash::Sha256 => &[
                0x30, 0x31, 0x30, 0x0d, 0x06, 0x09, 0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02,
                0x01, 0x05, 0x00, 0x04, 0x20,
            ],
            Hash::Sha384 => &[
                0x30, 0x41, 0x30, 0x0d, 0x06, 0x09, 0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02,
                0x02, 0x05, 0x00, 0x04, 0x30,
            ],
            Hash::Sha512 => &[
                0x30, 0x51, 0x30, 0x0d, 0x06, 0x09, 0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02,
                0x03, 0x05, 0x00, 0x04, 0x40,
            ],
        }
    }
}

/// Smallest modulus accepted (bits); shorter RSA keys are breakable.
pub const MIN_MODULUS_BITS: usize = 2048;

/// Checks an RSASSA-PKCS1-v1_5 signature of `msg` under the public key (`n`, `e`), both
/// big-endian unsigned integers.
pub fn verify_pkcs1_v15(n: &[u8], e: &[u8], hash: Hash, msg: &[u8], sig: &[u8]) -> bool {
    let n = Big::from_be(n);
    let e = Big::from_be(e);
    let k = n.bits().div_ceil(8);
    if n.bits() < MIN_MODULUS_BITS || e.bits() < 2 || e.bits() > 64 || sig.len() != k {
        return false;
    }
    let s = Big::from_be(sig);
    if s.cmp(&n) != std::cmp::Ordering::Less {
        return false;
    }
    let Some(em) = s.pow_mod(&e, &n).to_be(k) else {
        return false;
    };
    let digest = hash.digest(msg);
    let info = hash.digest_info();
    let t_len = info.len() + digest.len();
    if k < t_len + 11 {
        return false;
    }
    let mut expected = vec![0xffu8; k];
    expected[0] = 0;
    expected[1] = 1;
    expected[k - t_len - 1] = 0;
    expected[k - t_len..k - digest.len()].copy_from_slice(info);
    expected[k - digest.len()..].copy_from_slice(&digest);
    crate::hash::ct_eq(&em, &expected)
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
    fn big_arithmetic() {
        let a = Big::from_be(&unhex("0123456789abcdef0123456789abcdef"));
        let n = Big::from_be(&unhex("fedcba9876543211"));
        // 3^200 mod 1000003 cross-checked by repeated multiplication.
        let three = Big(vec![3]);
        let m = Big(vec![1_000_003]);
        let mut slow = 1u128;
        for _ in 0..200 {
            slow = slow * 3 % 1_000_003;
        }
        assert_eq!(three.pow_mod(&Big(vec![200]), &m), Big(vec![slow as u64]));
        let r = a.rem(&n);
        let q = 0x0123456789abcdef0123456789abcdefu128 % 0xfedcba9876543211u128;
        assert_eq!(r, Big::from_be(&q.to_be_bytes()));
        assert_eq!(Big::from_be(&[0, 0, 5]).to_be(4).unwrap(), vec![0, 0, 0, 5]);
        assert!(Big::from_be(&[1, 0]).to_be(1).is_none());
    }

    // RFC 7515 Appendix A.2: the RS256 example JWS.
    const N: &str = "ofgWCuLjybRlzo0tZWJjNiuSfb4p4fAkd_wWJcyQoTbji9k0l8W26mPddxHmfHQp-Vaw-4qPCJrcS2mJPMEzP1Pt0Bm4d4QlL-yRT-SFd2lZS-pCgNMsD1W_YpRPEwOWvG6b32690r2jZ47soMZo9wGzjb_7OMg0LOL-bSf63kpaSHSXndS5z5rexMdbBYUsLA9e-KXBdQOS-UTo7WTBEMa2R2CapHg665xsmtdVMTBQY4uDZlxvb3qCo5ZwKh9kG4LT6_I5IhlJH7aGhyxXFvUK-DWNmoudF8NAco9_h9iaGNj8q2ethFkMLs91kzk2PAcDTW9gb54h4FRWyuXpoQ";
    const MSG: &str = "eyJhbGciOiJSUzI1NiJ9.eyJpc3MiOiJqb2UiLA0KICJleHAiOjEzMDA4MTkzODAsDQogImh0dHA6Ly9leGFtcGxlLmNvbS9pc19yb290Ijp0cnVlfQ";
    const SIG: &str = "cC4hiUPoj9Eetdgtv3hF80EGrhuB__dzERat0XF9g2VtQgr9PJbu3XOiZj5RZmh7AAuHIm4Bh-0Qc_lF5YKt_O8W2Fp5jujGbds9uJdbF9CUAr7t1dnZcAcQjbKBYNX4BAynRFdiuB--f_nZLgrnbyTyWzO75vRK5h6xBArLIARNPvkSjtQBMHlb1L07Qe7K0GarZRmB_eSN9383LcOLn6_dO--xi12jzDwusC-eOkHWEsqtFZESc6BfI7noOPqvhJ1phCnvWh6IeYI2w9QOYEUipUTI8np6LbgGY9Fs98rqVt5AXLIhWkWywlVmtVrBp0igcN_IoypGlUPQGe77Rw";

    fn b64url(s: &str) -> Vec<u8> {
        let mut t: String = s.replace('-', "+").replace('_', "/");
        while t.len() % 4 != 0 {
            t.push('=');
        }
        crate::hash::base64_decode(&t).unwrap()
    }

    #[test]
    fn rfc7515_rs256_example() {
        let (n, e, sig) = (b64url(N), [1, 0, 1], b64url(SIG));
        assert!(verify_pkcs1_v15(&n, &e, Hash::Sha256, MSG.as_bytes(), &sig));
        assert!(!verify_pkcs1_v15(&n, &e, Hash::Sha256, b"other", &sig));
        assert!(!verify_pkcs1_v15(
            &n,
            &e,
            Hash::Sha384,
            MSG.as_bytes(),
            &sig
        ));
        let mut bad = sig.clone();
        bad[10] ^= 1;
        assert!(!verify_pkcs1_v15(
            &n,
            &e,
            Hash::Sha256,
            MSG.as_bytes(),
            &bad
        ));
        assert!(!verify_pkcs1_v15(
            &n,
            &e,
            Hash::Sha256,
            MSG.as_bytes(),
            &sig[1..]
        ));
    }
}
