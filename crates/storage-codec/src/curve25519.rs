//! Curve25519: X25519 key agreement (RFC 7748) and Ed25519 signatures (RFC 8032).
//!
//! Field elements are five 51-bit limbs; every operation on secret data runs in constant time
//! (fixed loops, masked selects and swaps, no secret-dependent branches or indexes). Derived
//! constants (`d`, `sqrt(-1)`, the base point, `floor(2^512 / L)`) are computed once at first
//! use from their definitions instead of being transcribed.

use std::sync::OnceLock;

use crate::sha512::sha512;

const MASK51: u64 = (1 << 51) - 1;

fn ct_eq(a: &[u8; 32], b: &[u8; 32]) -> bool {
    let mut d = 0u8;
    for (x, y) in a.iter().zip(b) {
        d |= x ^ y;
    }
    d == 0
}

/// An element of GF(2^255 - 19).
#[derive(Clone, Copy, Debug)]
struct Fe([u64; 5]);

impl Fe {
    const ZERO: Fe = Fe([0; 5]);
    const ONE: Fe = Fe([1, 0, 0, 0, 0]);

    fn from_u64(v: u64) -> Fe {
        Fe([v & MASK51, v >> 51, 0, 0, 0])
    }

    /// Little-endian bytes; the top bit is ignored.
    fn from_bytes(b: &[u8; 32]) -> Fe {
        let load = |i: usize| u64::from_le_bytes(b[i..i + 8].try_into().unwrap_or([0; 8]));
        Fe([
            load(0) & MASK51,
            (load(6) >> 3) & MASK51,
            (load(12) >> 6) & MASK51,
            (load(19) >> 1) & MASK51,
            (load(24) >> 12) & MASK51,
        ])
    }

    fn carry(mut self) -> Fe {
        let l = &mut self.0;
        let mut c;
        c = l[0] >> 51;
        l[0] &= MASK51;
        l[1] += c;
        c = l[1] >> 51;
        l[1] &= MASK51;
        l[2] += c;
        c = l[2] >> 51;
        l[2] &= MASK51;
        l[3] += c;
        c = l[3] >> 51;
        l[3] &= MASK51;
        l[4] += c;
        c = l[4] >> 51;
        l[4] &= MASK51;
        l[0] += c * 19;
        self
    }

    /// Canonical little-endian encoding (fully reduced).
    fn to_bytes(self) -> [u8; 32] {
        // Three passes leave every limb below 2^51.
        let mut t = self.carry().carry().carry().0;
        // Subtract p if t >= p: q is 1 exactly when t + 19 overflows 2^255.
        let mut q = (t[0] + 19) >> 51;
        q = (t[1] + q) >> 51;
        q = (t[2] + q) >> 51;
        q = (t[3] + q) >> 51;
        q = (t[4] + q) >> 51;
        t[0] += 19 * q;
        for i in 0..4 {
            t[i + 1] += t[i] >> 51;
            t[i] &= MASK51;
        }
        t[4] &= MASK51;
        let mut out = [0u8; 32];
        let words = [
            t[0] | (t[1] << 51),
            (t[1] >> 13) | (t[2] << 38),
            (t[2] >> 26) | (t[3] << 25),
            (t[3] >> 39) | (t[4] << 12),
        ];
        for (i, w) in words.iter().enumerate() {
            out[i * 8..i * 8 + 8].copy_from_slice(&w.to_le_bytes());
        }
        out
    }

    fn add(self, o: Fe) -> Fe {
        let mut r = [0u64; 5];
        for (i, x) in r.iter_mut().enumerate() {
            *x = self.0[i] + o.0[i];
        }
        Fe(r).carry()
    }

    fn sub(self, o: Fe) -> Fe {
        // Add 4p first so limbs never underflow (inputs are carried: limbs < 2^52).
        const FOUR_P: [u64; 5] = [
            0x1f_ffff_ffff_ffb4,
            0x1f_ffff_ffff_fffc,
            0x1f_ffff_ffff_fffc,
            0x1f_ffff_ffff_fffc,
            0x1f_ffff_ffff_fffc,
        ];
        let mut r = [0u64; 5];
        for (i, x) in r.iter_mut().enumerate() {
            *x = self.0[i] + FOUR_P[i] - o.0[i];
        }
        Fe(r).carry()
    }

    fn neg(self) -> Fe {
        Fe::ZERO.sub(self)
    }

    fn mul(self, o: Fe) -> Fe {
        let a = self.0.map(u128::from);
        let b = o.0.map(u128::from);
        let b19 = [b[1] * 19, b[2] * 19, b[3] * 19, b[4] * 19];
        let r0 = a[0] * b[0] + a[1] * b19[3] + a[2] * b19[2] + a[3] * b19[1] + a[4] * b19[0];
        let r1 = a[0] * b[1] + a[1] * b[0] + a[2] * b19[3] + a[3] * b19[2] + a[4] * b19[1];
        let r2 = a[0] * b[2] + a[1] * b[1] + a[2] * b[0] + a[3] * b19[3] + a[4] * b19[2];
        let r3 = a[0] * b[3] + a[1] * b[2] + a[2] * b[1] + a[3] * b[0] + a[4] * b19[3];
        let r4 = a[0] * b[4] + a[1] * b[3] + a[2] * b[2] + a[3] * b[1] + a[4] * b[0];
        let m = u128::from(MASK51);
        let c0 = r0 >> 51;
        let r1 = r1 + c0;
        let c1 = r1 >> 51;
        let r2 = r2 + c1;
        let c2 = r2 >> 51;
        let r3 = r3 + c2;
        let c3 = r3 >> 51;
        let r4 = r4 + c3;
        let c4 = r4 >> 51;
        // c4 * 19 can exceed 64 bits: fold it in 128-bit arithmetic first.
        let l0 = (r0 & m) + c4 * 19;
        let l1 = (r1 & m) + (l0 >> 51);
        Fe([
            (l0 & m) as u64,
            l1 as u64,
            (r2 & m) as u64,
            (r3 & m) as u64,
            (r4 & m) as u64,
        ])
        .carry()
    }

    fn square(self) -> Fe {
        self.mul(self)
    }

    /// `self^e` for a public exponent (little-endian bytes).
    fn pow(self, e: &[u8; 32]) -> Fe {
        let mut r = Fe::ONE;
        for i in (0..256).rev() {
            r = r.square();
            if (e[i / 8] >> (i % 8)) & 1 == 1 {
                r = r.mul(self);
            }
        }
        r
    }

    fn invert(self) -> Fe {
        self.pow(&P_MINUS_2)
    }

    fn is_zero(self) -> bool {
        self.to_bytes() == [0; 32]
    }

    fn is_negative(self) -> bool {
        self.to_bytes()[0] & 1 == 1
    }

    fn eq(self, o: Fe) -> bool {
        ct_eq(&self.to_bytes(), &o.to_bytes())
    }

    /// `b ? o : self` without branching on `b`.
    fn select(self, o: Fe, b: u64) -> Fe {
        let mask = 0u64.wrapping_sub(b);
        let mut r = self.0;
        for (x, y) in r.iter_mut().zip(o.0) {
            *x ^= mask & (*x ^ y);
        }
        Fe(r)
    }
}

/// p - 2 = 2^255 - 21 (little-endian), the inversion exponent.
const P_MINUS_2: [u8; 32] = {
    let mut e = [0xff; 32];
    e[0] = 0xeb;
    e[31] = 0x7f;
    e
};

/// (p - 5) / 8 = 2^252 - 3 (little-endian), the square-root exponent.
const P_MINUS_5_OVER_8: [u8; 32] = {
    let mut e = [0xff; 32];
    e[0] = 0xfd;
    e[31] = 0x0f;
    e
};

fn cswap(a: &mut Fe, b: &mut Fe, swap: u64) {
    let mask = 0u64.wrapping_sub(swap);
    for i in 0..5 {
        let t = mask & (a.0[i] ^ b.0[i]);
        a.0[i] ^= t;
        b.0[i] ^= t;
    }
}

// ── X25519 ──

/// The X25519 base point (u = 9).
pub const X25519_BASEPOINT: [u8; 32] = {
    let mut b = [0u8; 32];
    b[0] = 9;
    b
};

/// X25519(scalar, u) (RFC 7748 section 5); the scalar is clamped here.
pub fn x25519(scalar: &[u8; 32], u: &[u8; 32]) -> [u8; 32] {
    let mut k = *scalar;
    k[0] &= 248;
    k[31] &= 127;
    k[31] |= 64;
    let x1 = Fe::from_bytes(u);
    let (mut x2, mut z2, mut x3, mut z3) = (Fe::ONE, Fe::ZERO, x1, Fe::ONE);
    let a24 = Fe::from_u64(121_665);
    let mut swap = 0u64;
    for t in (0..255).rev() {
        let bit = u64::from((k[t / 8] >> (t % 8)) & 1);
        swap ^= bit;
        cswap(&mut x2, &mut x3, swap);
        cswap(&mut z2, &mut z3, swap);
        swap = bit;
        let a = x2.add(z2);
        let aa = a.square();
        let b = x2.sub(z2);
        let bb = b.square();
        let e = aa.sub(bb);
        let c = x3.add(z3);
        let d = x3.sub(z3);
        let da = d.mul(a);
        let cb = c.mul(b);
        x3 = da.add(cb).square();
        z3 = x1.mul(da.sub(cb).square());
        x2 = aa.mul(bb);
        z2 = e.mul(aa.add(a24.mul(e)));
    }
    cswap(&mut x2, &mut x3, swap);
    cswap(&mut z2, &mut z3, swap);
    x2.mul(z2.invert()).to_bytes()
}

/// The X25519 public key of `secret`.
pub fn x25519_public(secret: &[u8; 32]) -> [u8; 32] {
    x25519(secret, &X25519_BASEPOINT)
}

// ── Edwards25519 ──

struct Consts {
    d: Fe,
    d2: Fe,
    sqrt_m1: Fe,
    base: Point,
    /// floor(2^512 / L) as 64-bit limbs, for Barrett reduction.
    mu: [u64; 5],
}

fn consts() -> &'static Consts {
    static C: OnceLock<Consts> = OnceLock::new();
    C.get_or_init(|| {
        let d = Fe::from_u64(121_665)
            .neg()
            .mul(Fe::from_u64(121_666).invert());
        // sqrt(-1) = 2^((p - 1) / 4); (p - 1) / 4 = 2^253 - 5.
        let mut e = [0xff; 32];
        e[0] = 0xfb;
        e[31] = 0x1f;
        let sqrt_m1 = Fe::from_u64(2).pow(&e);
        let mut c = Consts {
            d,
            d2: d.add(d),
            sqrt_m1,
            base: Point::IDENTITY,
            mu: [0; 5],
        };
        // The base point: y = 4/5, x even.
        let y = Fe::from_u64(4).mul(Fe::from_u64(5).invert());
        c.base = Point::decode_with(&y.to_bytes(), &c).unwrap_or(Point::IDENTITY);
        c.mu = barrett_mu();
        c
    })
}

/// A point in extended coordinates (X:Y:Z:T), x = X/Z, y = Y/Z, xy = T/Z.
#[derive(Clone, Copy, Debug)]
struct Point {
    x: Fe,
    y: Fe,
    z: Fe,
    t: Fe,
}

impl Point {
    const IDENTITY: Point = Point {
        x: Fe::ZERO,
        y: Fe::ONE,
        z: Fe::ONE,
        t: Fe::ZERO,
    };

    /// Unified addition (also valid for doubling), a = -1 twisted Edwards.
    fn add(&self, o: &Point) -> Point {
        let d2 = consts().d2;
        let a = self.y.sub(self.x).mul(o.y.sub(o.x));
        let b = self.y.add(self.x).mul(o.y.add(o.x));
        let c = self.t.mul(d2).mul(o.t);
        let d = self.z.add(self.z).mul(o.z);
        let (e, f, g, h) = (b.sub(a), d.sub(c), d.add(c), b.add(a));
        Point {
            x: e.mul(f),
            y: g.mul(h),
            z: f.mul(g),
            t: e.mul(h),
        }
    }

    fn select(&self, o: &Point, b: u64) -> Point {
        Point {
            x: self.x.select(o.x, b),
            y: self.y.select(o.y, b),
            z: self.z.select(o.z, b),
            t: self.t.select(o.t, b),
        }
    }

    /// `scalar * self` (little-endian 256-bit scalar), constant time.
    fn mul(&self, scalar: &[u8; 32]) -> Point {
        let mut q = Point::IDENTITY;
        for i in (0..256).rev() {
            q = q.add(&q);
            let r = q.add(self);
            q = q.select(&r, u64::from((scalar[i / 8] >> (i % 8)) & 1));
        }
        q
    }

    fn encode(&self) -> [u8; 32] {
        let zi = self.z.invert();
        let x = self.x.mul(zi);
        let mut out = self.y.mul(zi).to_bytes();
        out[31] |= u8::from(x.is_negative()) << 7;
        out
    }

    fn decode(b: &[u8; 32]) -> Option<Point> {
        Point::decode_with(b, consts())
    }

    fn decode_with(b: &[u8; 32], c: &Consts) -> Option<Point> {
        let sign = b[31] >> 7;
        let mut yb = *b;
        yb[31] &= 0x7f;
        let y = Fe::from_bytes(&yb);
        if y.to_bytes() != yb {
            return None; // non-canonical y
        }
        let y2 = y.square();
        let u = y2.sub(Fe::ONE);
        let v = c.d.mul(y2).add(Fe::ONE);
        let v3 = v.square().mul(v);
        let v7 = v3.square().mul(v);
        let mut x = u.mul(v3).mul(u.mul(v7).pow(&P_MINUS_5_OVER_8));
        let vx2 = v.mul(x.square());
        if !vx2.eq(u) {
            if vx2.eq(u.neg()) {
                x = x.mul(c.sqrt_m1);
            } else {
                return None;
            }
        }
        if x.is_zero() && sign == 1 {
            return None;
        }
        if u8::from(x.is_negative()) != sign {
            x = x.neg();
        }
        Some(Point {
            x,
            y,
            z: Fe::ONE,
            t: x.mul(y),
        })
    }
}

// ── scalars mod L = 2^252 + 27742317777372353535851937790883648493 ──

const L: [u64; 4] = [
    0x5812631a5cf5d3ed,
    0x14def9dea2f79cd6,
    0,
    0x1000000000000000,
];

/// floor(2^512 / L), by long division (public data, computed once).
fn barrett_mu() -> [u64; 5] {
    let mut q = [0u64; 9];
    let mut r = [0u64; 5]; // remainder < L < 2^253, with headroom for one shift
    for bit in (0..=512).rev() {
        // r = 2r + (bit of 2^512)
        let mut carry = u64::from(bit == 512);
        for limb in r.iter_mut() {
            let next = *limb >> 63;
            *limb = (*limb << 1) | carry;
            carry = next;
        }
        if !lt(&r, &[L[0], L[1], L[2], L[3], 0]) {
            sub_in_place(&mut r, &[L[0], L[1], L[2], L[3], 0]);
            q[bit / 64] |= 1 << (bit % 64);
        }
    }
    [q[0], q[1], q[2], q[3], q[4]]
}

fn lt(a: &[u64], b: &[u64]) -> bool {
    for i in (0..a.len()).rev() {
        if a[i] != b[i] {
            return a[i] < b[i];
        }
    }
    false
}

/// `a -= b` (mod 2^(64 n)); returns the borrow.
fn sub_in_place(a: &mut [u64], b: &[u64]) -> u64 {
    let mut borrow = 0u64;
    for i in 0..a.len() {
        let (d1, b1) = a[i].overflowing_sub(b[i]);
        let (d2, b2) = d1.overflowing_sub(borrow);
        a[i] = d2;
        borrow = u64::from(b1 | b2);
    }
    borrow
}

fn mul_wide(a: &[u64], b: &[u64], out: &mut [u64]) {
    for o in out.iter_mut() {
        *o = 0;
    }
    for i in 0..a.len() {
        let mut carry = 0u128;
        for j in 0..b.len() {
            if i + j >= out.len() {
                break;
            }
            let t = u128::from(a[i]) * u128::from(b[j]) + u128::from(out[i + j]) + carry;
            out[i + j] = t as u64;
            carry = t >> 64;
        }
        if i + b.len() < out.len() {
            out[i + b.len()] = carry as u64;
        }
    }
}

/// `x mod L` for a 512-bit `x` (Barrett, HAC 14.42), constant time.
fn reduce512(x: &[u64; 8]) -> [u64; 4] {
    let mu = consts().mu;
    // q1 = x / b^(k-1) with k = 4 limbs of L: x >> 192.
    let q1 = [x[3], x[4], x[5], x[6], x[7]];
    let mut q2 = [0u64; 10];
    mul_wide(&q1, &mu, &mut q2);
    let q3 = [q2[5], q2[6], q2[7], q2[8], q2[9]];
    // r = (x mod b^5) - (q3 * L mod b^5), wrapping.
    let mut r = [x[0], x[1], x[2], x[3], x[4]];
    let mut q3l = [0u64; 5];
    mul_wide(&q3, &L, &mut q3l);
    sub_in_place(&mut r, &q3l);
    // At most two subtractions of L remain; do both, keeping each only if it did not borrow.
    let l5 = [L[0], L[1], L[2], L[3], 0];
    for _ in 0..2 {
        let mut t = r;
        let borrow = sub_in_place(&mut t, &l5);
        let keep = 1 - borrow; // borrow == 0: r >= L, take t
        let mask = 0u64.wrapping_sub(keep);
        for (ri, ti) in r.iter_mut().zip(t) {
            *ri ^= mask & (*ri ^ ti);
        }
    }
    [r[0], r[1], r[2], r[3]]
}

fn scalar_from_wide(bytes: &[u8; 64]) -> [u64; 4] {
    let mut x = [0u64; 8];
    for (i, w) in x.iter_mut().enumerate() {
        *w = u64::from_le_bytes(bytes[i * 8..i * 8 + 8].try_into().unwrap_or([0; 8]));
    }
    reduce512(&x)
}

fn scalar_bytes(s: &[u64; 4]) -> [u8; 32] {
    let mut out = [0u8; 32];
    for (i, w) in s.iter().enumerate() {
        out[i * 8..i * 8 + 8].copy_from_slice(&w.to_le_bytes());
    }
    out
}

fn scalar_from_bytes32(b: &[u8; 32]) -> [u64; 4] {
    let mut wide = [0u8; 64];
    wide[..32].copy_from_slice(b);
    scalar_from_wide(&wide)
}

/// `(a * b + c) mod L`.
fn scalar_muladd(a: &[u64; 4], b: &[u64; 4], c: &[u64; 4]) -> [u64; 4] {
    let mut prod = [0u64; 8];
    mul_wide(a, b, &mut prod);
    let mut carry = 0u128;
    for i in 0..8 {
        let t = u128::from(prod[i]) + u128::from(if i < 4 { c[i] } else { 0 }) + carry;
        prod[i] = t as u64;
        carry = t >> 64;
    }
    reduce512(&prod)
}

// ── Ed25519 ──

/// An Ed25519 signing key (the 32-byte seed) with its expanded secret and public key.
#[derive(Clone)]
pub struct Ed25519Key {
    scalar: [u8; 32],
    prefix: [u8; 32],
    public: [u8; 32],
}

impl std::fmt::Debug for Ed25519Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ed25519Key")
            .field("public", &self.public)
            .finish_non_exhaustive()
    }
}

impl Drop for Ed25519Key {
    fn drop(&mut self) {
        self.scalar = [0; 32];
        self.prefix = [0; 32];
    }
}

impl Ed25519Key {
    pub fn from_seed(seed: &[u8; 32]) -> Ed25519Key {
        let h = sha512(&[seed]);
        let mut scalar: [u8; 32] = h[..32].try_into().unwrap_or([0; 32]);
        scalar[0] &= 248;
        scalar[31] &= 127;
        scalar[31] |= 64;
        let prefix: [u8; 32] = h[32..].try_into().unwrap_or([0; 32]);
        let public = consts().base.mul(&scalar).encode();
        Ed25519Key {
            scalar,
            prefix,
            public,
        }
    }

    pub fn public_key(&self) -> [u8; 32] {
        self.public
    }

    /// The RFC 8032 (pure Ed25519) signature of `msg`.
    pub fn sign(&self, msg: &[u8]) -> [u8; 64] {
        let r = scalar_from_wide(&sha512(&[&self.prefix, msg]));
        let big_r = consts().base.mul(&scalar_bytes(&r)).encode();
        let k = scalar_from_wide(&sha512(&[&big_r, &self.public, msg]));
        let a = scalar_from_bytes32(&self.scalar);
        let s = scalar_muladd(&k, &a, &r);
        let mut sig = [0u8; 64];
        sig[..32].copy_from_slice(&big_r);
        sig[32..].copy_from_slice(&scalar_bytes(&s));
        sig
    }
}

/// Verifies an Ed25519 signature (cofactorless equation `[S]B = R + [k]A`).
pub fn ed25519_verify(public: &[u8; 32], msg: &[u8], sig: &[u8; 64]) -> bool {
    let Some(a) = Point::decode(public) else {
        return false;
    };
    let r_bytes: [u8; 32] = sig[..32].try_into().unwrap_or([0; 32]);
    let s_bytes: [u8; 32] = sig[32..].try_into().unwrap_or([0; 32]);
    let mut s4 = [0u64; 4];
    for (i, w) in s4.iter_mut().enumerate() {
        *w = u64::from_le_bytes(s_bytes[i * 8..i * 8 + 8].try_into().unwrap_or([0; 8]));
    }
    if !lt(&s4, &L) {
        return false;
    }
    let Some(r) = Point::decode(&r_bytes) else {
        return false;
    };
    let k = scalar_from_wide(&sha512(&[&r_bytes, public, msg]));
    let lhs = consts().base.mul(&s_bytes).encode();
    let rhs = r.add(&a.mul(&scalar_bytes(&k))).encode();
    ct_eq(&lhs, &rhs)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unhex<const N: usize>(s: &str) -> [u8; N] {
        let v: Vec<u8> = (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect();
        v.try_into().unwrap()
    }

    fn hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{x:02x}")).collect()
    }

    #[test]
    fn x25519_rfc7748_vectors() {
        let k: [u8; 32] = unhex("a546e36bf0527c9d3b16154b82465edd62144c0ac1fc5a18506a2244ba449ac4");
        let u: [u8; 32] = unhex("e6db6867583030db3594c1a424b15f7c726624ec26b3353b10a903a6d0ab1c4c");
        assert_eq!(
            hex(&x25519(&k, &u)),
            "c3da55379de9c6908e94ea4df28d084f32eccf03491c71f754b4075577a28552"
        );
        // Diffie-Hellman example (section 6.1).
        let a: [u8; 32] = unhex("77076d0a7318a57d3c16c17251b26645df4c2f87ebc0992ab177fba51db92c2a");
        let b: [u8; 32] = unhex("5dab087e624a8a4b79e17f8b83800ee66f3bb1292618b6fd1c2f8b27ff88e0eb");
        assert_eq!(
            hex(&x25519_public(&a)),
            "8520f0098930a754748b7ddcb43ef75a0dbf3a0d26381af4eba4a98eaa9b4e6a"
        );
        assert_eq!(
            hex(&x25519_public(&b)),
            "de9edb7d7b7dc1b4d35b61c2ece435373f8343c85b78674dadfc7e146f882b4f"
        );
        let shared = "4a5d9d5ba4ce2de1728e3bf480350f25e07e21c947d19e3376f09b3c1e161742";
        assert_eq!(hex(&x25519(&a, &x25519_public(&b))), shared);
        assert_eq!(hex(&x25519(&b, &x25519_public(&a))), shared);
        // Iterated (1,000 rounds).
        let (mut k, mut u) = (X25519_BASEPOINT, X25519_BASEPOINT);
        for _ in 0..1000 {
            let next = x25519(&k, &u);
            u = k;
            k = next;
        }
        assert_eq!(
            hex(&k),
            "684cf59ba83309552800ef566f2f4d3c1c3887c49360e3875f2eb94d99532c51"
        );
    }

    #[test]
    fn ed25519_rfc8032_vectors() {
        let cases = [
            (
                "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60",
                "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a",
                "",
                "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e065224901555fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b",
            ),
            (
                "4ccd089b28ff96da9db6c346ec114e0f5b8a319f35aba624da8cf6ed4fb8a6fb",
                "3d4017c3e843895a92b70aa74d1b7ebc9c982ccf2ec4968cc0cd55f12af4660c",
                "72",
                "92a009a9f0d4cab8720e820b5f642540a2b27b5416503f8fb3762223ebdb69da085ac1e43e15996e458f3613d0f11d8c387b2eaeb4302aeeb00d291612bb0c00",
            ),
            (
                "c5aa8df43f9f837bedb7442f31dcb7b166d38535076f094b85ce3a2e0b4458f7",
                "fc51cd8e6218a1a38da47ed00230f0580816ed13ba3303ac5deb911548908025",
                "af82",
                "6291d657deec24024827e69c3abe01a30ce548a284743a445e3680d7db5ac3ac18ff9b538d16f290ae67f760984dc6594a7c15e9716ed28dc027beceea1ec40a",
            ),
        ];
        for (seed, public, msg, sig) in cases {
            let key = Ed25519Key::from_seed(&unhex(seed));
            assert_eq!(hex(&key.public_key()), public);
            let m: Vec<u8> = (0..msg.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&msg[i..i + 2], 16).unwrap())
                .collect();
            let s = key.sign(&m);
            assert_eq!(hex(&s), sig);
            assert!(ed25519_verify(&key.public_key(), &m, &s));
            let mut bad = s;
            bad[5] ^= 1;
            assert!(!ed25519_verify(&key.public_key(), &m, &bad));
            assert!(!ed25519_verify(&key.public_key(), b"other", &s));
        }
    }

    #[test]
    fn scalar_reduction_matches_long_division() {
        // (L - 1) * (L - 1) + (L - 1) = L * (L - 1) ≡ 0 (mod L).
        let lm1 = [L[0] - 1, L[1], L[2], L[3]];
        assert_eq!(scalar_muladd(&lm1, &lm1, &lm1), [0; 4]);
        // 2^512 - 1 reduces to (2^512 - 1) mod L; check via (x - x mod L) divisible: compare with
        // reducing x - 1 and adding 1.
        let all = [u64::MAX; 8];
        let r = reduce512(&all);
        let mut minus1 = all;
        minus1[0] -= 1;
        let r2 = reduce512(&minus1);
        let mut r2p = [0u64; 5];
        r2p[..4].copy_from_slice(&r2);
        let one = [1u64, 0, 0, 0, 0];
        let mut sum = [0u64; 5];
        let mut carry = 0u128;
        for i in 0..5 {
            let t = u128::from(r2p[i]) + u128::from(one[i]) + carry;
            sum[i] = t as u64;
            carry = t >> 64;
        }
        if !lt(&sum, &[L[0], L[1], L[2], L[3], 0]) {
            sub_in_place(&mut sum, &[L[0], L[1], L[2], L[3], 0]);
        }
        assert_eq!([sum[0], sum[1], sum[2], sum[3]], r);
    }
}
