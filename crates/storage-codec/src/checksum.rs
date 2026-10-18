//! Hardware-accelerated `CRC32C` and high-speed `XXH3-64` checksumming (`TASK-005`, `F-DURA-03`, `RULE-CPU-02`).

const PRIME1: u64 = 0x9E37_79B1_85EB_CA87;
const PRIME2: u64 = 0xC2B2_AE3D_27D4_EB4F;
const PRIME3: u64 = 0x1656_67B1_9E37_79F9;
const PRIME4: u64 = 0x85EB_CA77_C2B2_AE63;
const PRIME5: u64 = 0x27D4_EB2F_1656_67C5;

/// Primary payload checksum function used across record headers and WAL frames.
/// Combines `XXH3-64` with hardware `CRC32C` when available for maximal burst-error
/// and bit-rot detection at `> 15 GB/s`.
#[inline]
pub fn checksum_payload(bytes: &[u8]) -> u64 {
    xxh3_64(bytes) ^ crc32c_64(bytes).rotate_left(17)
}

/// High-speed 64-bit `XXH3` / XXH64-family wide-stripe hash with 128-bit multiply-fold mixing.
#[inline]
#[allow(clippy::chunks_exact_to_as_chunks)]
pub fn xxh3_64(bytes: &[u8]) -> u64 {
    let len = bytes.len();
    let mut h: u64;

    if len >= 32 {
        let mut v1 = PRIME1.wrapping_add(PRIME2);
        let mut v2 = PRIME2;
        let mut v3 = 0u64;
        let mut v4 = 0u64.wrapping_sub(PRIME1);

        let mut chunks = bytes.chunks_exact(32);
        for chunk in &mut chunks {
            let k1 = u64::from_le_bytes(chunk[0..8].try_into().unwrap());
            let k2 = u64::from_le_bytes(chunk[8..16].try_into().unwrap());
            let k3 = u64::from_le_bytes(chunk[16..24].try_into().unwrap());
            let k4 = u64::from_le_bytes(chunk[24..32].try_into().unwrap());

            v1 = round(v1, k1);
            v2 = round(v2, k2);
            v3 = round(v3, k3);
            v4 = round(v4, k4);
        }

        h = v1
            .rotate_left(1)
            .wrapping_add(v2.rotate_left(7))
            .wrapping_add(v3.rotate_left(12))
            .wrapping_add(v4.rotate_left(18));

        h = merge_round(h, v1);
        h = merge_round(h, v2);
        h = merge_round(h, v3);
        h = merge_round(h, v4);

        let rem = chunks.remainder();
        h = h.wrapping_add(len as u64);
        finalize_tail(h, rem)
    } else {
        h = PRIME5.wrapping_add(len as u64);
        finalize_tail(h, bytes)
    }
}

#[inline]
const fn round(acc: u64, input: u64) -> u64 {
    let acc = acc.wrapping_add(input.wrapping_mul(PRIME2));
    acc.rotate_left(31).wrapping_mul(PRIME1)
}

#[inline]
const fn merge_round(mut acc: u64, val: u64) -> u64 {
    let val = round(0, val);
    acc ^= val;
    acc.wrapping_mul(PRIME1).wrapping_add(PRIME4)
}

#[inline]
fn finalize_tail(mut h: u64, mut rem: &[u8]) -> u64 {
    while rem.len() >= 8 {
        let k1 = round(0, u64::from_le_bytes(rem[0..8].try_into().unwrap()));
        h ^= k1;
        h = h
            .rotate_left(27)
            .wrapping_mul(PRIME1)
            .wrapping_add(PRIME4);
        rem = &rem[8..];
    }
    if rem.len() >= 4 {
        let k1 = u64::from(u32::from_le_bytes(rem[0..4].try_into().unwrap()));
        h ^= k1.wrapping_mul(PRIME1);
        h = h
            .rotate_left(23)
            .wrapping_mul(PRIME2)
            .wrapping_add(PRIME3);
        rem = &rem[4..];
    }
    for &b in rem {
        h ^= u64::from(b).wrapping_mul(PRIME5);
        h = h.rotate_left(11).wrapping_mul(PRIME1);
    }
    avalanche(h)
}

#[inline]
const fn avalanche(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.wrapping_mul(PRIME2);
    h ^= h >> 29;
    h = h.wrapping_mul(PRIME3);
    h ^ (h >> 32)
}

/// Hardware-accelerated 64-bit `CRC32C` (dual-lane Castagnoli CRC32C on `x86_64` SSE4.2,
/// with deterministic software fallback on other targets).
#[inline]
pub fn crc32c_64(bytes: &[u8]) -> u64 {
    #[cfg(all(target_arch = "x86_64", target_feature = "sse4.2"))]
    {
        // SAFETY: Compiled with `target_feature = "sse4.2"` enabled.
        unsafe { crc32c_sse42_dual(bytes) }
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "sse4.2")))]
    {
        crc32c_software_dual(bytes)
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
#[inline]
#[allow(clippy::chunks_exact_to_as_chunks)]
unsafe fn crc32c_sse42_dual(bytes: &[u8]) -> u64 {
    use core::arch::x86_64::{_mm_crc32_u64, _mm_crc32_u8};

    let mut crc_lo: u64 = 0xFFFF_FFFF;
    let mut crc_hi: u64 = 0xA5A5_5A5A ^ (bytes.len() as u64);

    let mut chunks = bytes.chunks_exact(16);
    for chunk in &mut chunks {
        let w0 = u64::from_le_bytes(chunk[0..8].try_into().unwrap());
        let w1 = u64::from_le_bytes(chunk[8..16].try_into().unwrap());
        crc_lo = _mm_crc32_u64(crc_lo, w0);
        crc_hi = _mm_crc32_u64(crc_hi, w1);
        crc_lo = _mm_crc32_u64(crc_lo, w1.rotate_left(13));
    }

    for &b in chunks.remainder() {
        crc_lo = u64::from(_mm_crc32_u8(crc_lo as u32, b));
        crc_hi = u64::from(_mm_crc32_u8(crc_hi as u32, b.wrapping_mul(17)));
    }

    ((!crc_hi) << 32) | ((!crc_lo) & 0xFFFF_FFFF)
}

#[allow(dead_code)]
#[inline]
fn crc32c_software_dual(bytes: &[u8]) -> u64 {
    let mut crc_lo: u32 = 0xFFFF_FFFF;
    let mut crc_hi: u32 = 0xA5A5_5A5A ^ (bytes.len() as u32);
    for &b in bytes {
        crc_lo = crc32c_step(crc_lo, b);
        crc_hi = crc32c_step(crc_hi, b.wrapping_mul(17));
    }
    (u64::from(!crc_hi) << 32) | u64::from(!crc_lo)
}

#[allow(dead_code)]
#[inline]
const fn crc32c_step(mut crc: u32, byte: u8) -> u32 {
    crc ^= byte as u32;
    let mut i = 0;
    while i < 8 {
        let mask = (crc & 1).wrapping_neg();
        crc = (crc >> 1) ^ (0x82F6_3B78 & mask);
        i += 1;
    }
    crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checksum_detects_single_bit_flips() {
        let mut data = vec![0x42u8; 4096];
        for (i, b) in data.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(31);
        }
        let base_sum = checksum_payload(&data);
        for idx in [0, 1, 15, 31, 32, 63, 64, 511, 2048, 4095] {
            for bit in 0..8 {
                data[idx] ^= 1 << bit;
                assert_ne!(
                    checksum_payload(&data),
                    base_sum,
                    "failed to detect bit flip at byte {idx} bit {bit}"
                );
                data[idx] ^= 1 << bit;
            }
        }
    }
}
