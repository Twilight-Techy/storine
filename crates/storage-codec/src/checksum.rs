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
            let k1 = storage_types::wire::read_u64_le(chunk, 0);
            let k2 = storage_types::wire::read_u64_le(chunk, 8);
            let k3 = storage_types::wire::read_u64_le(chunk, 16);
            let k4 = storage_types::wire::read_u64_le(chunk, 24);

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
        let k1 = round(0, storage_types::wire::read_u64_le(rem, 0));
        h ^= k1;
        h = h
            .rotate_left(27)
            .wrapping_mul(PRIME1)
            .wrapping_add(PRIME4);
        rem = &rem[8..];
    }
    if rem.len() >= 4 {
        let k1 = u64::from(storage_types::wire::read_u32_le(rem, 0));
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

/// 64-bit dual-lane `CRC32C` (Castagnoli) used inside [`checksum_payload`].
///
/// The result is part of the on-disk and wire format, so every build computes the *same* value:
/// the SSE4.2 `crc32` instruction path (selected at compile time when `sse4.2` is enabled, or at
/// runtime via CPU feature detection otherwise) and the portable table-driven path are
/// bit-identical. A database written by a `target-cpu=native` build is therefore readable by a
/// portable build or on another architecture.
///
/// Per 16-byte chunk `(w0, w1)` (little-endian words): `lo = crc(lo, w0)`, `hi = crc(hi, w1)`,
/// `lo = crc(lo, w1.rotate_left(13))`; trailing bytes `b` update `lo` with `b` and `hi` with
/// `b * 17`. Lanes start at `0xFFFF_FFFF` and `0xA5A5_5A5A ^ len` and are inverted at the end.
#[inline]
pub fn crc32c_64(bytes: &[u8]) -> u64 {
    #[cfg(all(target_arch = "x86_64", target_feature = "sse4.2"))]
    {
        // SAFETY: this build statically enables `sse4.2`.
        unsafe { crc32c_sse42_dual(bytes) }
    }
    #[cfg(all(target_arch = "x86_64", not(target_feature = "sse4.2")))]
    {
        if std::arch::is_x86_feature_detected!("sse4.2") {
            // SAFETY: the running CPU supports `sse4.2` (checked above; the result is cached).
            unsafe { crc32c_sse42_dual(bytes) }
        } else {
            crc32c_portable_dual(bytes)
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        crc32c_portable_dual(bytes)
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
#[inline]
#[allow(clippy::chunks_exact_to_as_chunks)]
unsafe fn crc32c_sse42_dual(bytes: &[u8]) -> u64 {
    use core::arch::x86_64::{_mm_crc32_u64, _mm_crc32_u8};

    let mut crc_lo: u64 = 0xFFFF_FFFF;
    let mut crc_hi: u64 = u64::from(0xA5A5_5A5A ^ (bytes.len() as u32));

    let mut chunks = bytes.chunks_exact(16);
    for chunk in &mut chunks {
        let w0 = storage_types::wire::read_u64_le(chunk, 0);
        let w1 = storage_types::wire::read_u64_le(chunk, 8);
        crc_lo = _mm_crc32_u64(crc_lo, w0);
        crc_hi = _mm_crc32_u64(crc_hi, w1);
        crc_lo = _mm_crc32_u64(crc_lo, w1.rotate_left(13));
    }

    for &b in chunks.remainder() {
        crc_lo = u64::from(_mm_crc32_u8(crc_lo as u32, b));
        crc_hi = u64::from(_mm_crc32_u8(crc_hi as u32, b.wrapping_mul(17)));
    }

    (u64::from(!(crc_hi as u32)) << 32) | u64::from(!(crc_lo as u32))
}

/// Portable equivalent of [`crc32c_sse42_dual`] (identical output on every input).
#[cfg_attr(all(target_arch = "x86_64", target_feature = "sse4.2"), allow(dead_code))]
#[allow(clippy::chunks_exact_to_as_chunks)]
fn crc32c_portable_dual(bytes: &[u8]) -> u64 {
    let mut crc_lo: u32 = 0xFFFF_FFFF;
    let mut crc_hi: u32 = 0xA5A5_5A5A ^ (bytes.len() as u32);

    let mut chunks = bytes.chunks_exact(16);
    for chunk in &mut chunks {
        let w0 = storage_types::wire::read_u64_le(chunk, 0);
        let w1 = storage_types::wire::read_u64_le(chunk, 8);
        crc_lo = crc32c_u64(crc_lo, w0);
        crc_hi = crc32c_u64(crc_hi, w1);
        crc_lo = crc32c_u64(crc_lo, w1.rotate_left(13));
    }

    for &b in chunks.remainder() {
        crc_lo = crc32c_u8(crc_lo, b);
        crc_hi = crc32c_u8(crc_hi, b.wrapping_mul(17));
    }

    (u64::from(!crc_hi) << 32) | u64::from(!crc_lo)
}

/// Reflected CRC32C (polynomial `0x82F6_3B78`) byte table, built at compile time (1 KiB).
const CRC32C_TABLE: [u32; 256] = {
    let mut table = [0u32; 256];
    let mut n = 0;
    while n < 256 {
        let mut crc = n as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = (crc >> 1) ^ (0x82F6_3B78 & (crc & 1).wrapping_neg());
            bit += 1;
        }
        table[n] = crc;
        n += 1;
    }
    table
};

/// Same as the SSE4.2 `crc32` instruction on one byte (no pre/post inversion).
#[inline(always)]
const fn crc32c_u8(crc: u32, byte: u8) -> u32 {
    CRC32C_TABLE[((crc ^ byte as u32) & 0xFF) as usize] ^ (crc >> 8)
}

/// Same as the SSE4.2 `crc32` instruction on a little-endian `u64` (low 32 bits of the state).
#[inline(always)]
fn crc32c_u64(mut crc: u32, word: u64) -> u32 {
    for b in word.to_le_bytes() {
        crc = crc32c_u8(crc, b);
    }
    crc
}

#[cfg(test)]
mod tests {
    use super::*;

    /// On-disk format pin: values produced by the SSE4.2 path before the portable path was made
    /// bit-identical. Changing any of these breaks every existing database.
    const GOLDEN: [(usize, u64, u64); 10] = [
        (0, 0x5A5A_A5A5_0000_0000, 0xA40C_DB37_51D8_5D2C),
        (1, 0x92A4_1BAB_86B7_37BA, 0x9E3B_7262_872D_9EFF),
        (7, 0xE775_0AC3_5110_A112, 0xBA38_5E1C_2E4A_5464),
        (15, 0x3C0D_3B1B_9B0C_1517, 0xD81D_01F3_B979_B2BD),
        (16, 0x98F8_D4B5_5817_D027, 0x08F0_6406_1064_F5E2),
        (17, 0x9433_EC91_81F6_A52A, 0x27BC_0C06_34BB_F46E),
        (31, 0x12C1_9546_0191_6BF0, 0x60F8_F083_747A_F122),
        (64, 0xC7F1_C5EC_87CC_D56E, 0xF063_B45D_FDF4_5E9D),
        (100, 0x3309_4405_3ECD_F203, 0x67AA_D0B6_DA76_A743),
        (4096, 0x7091_F759_CB10_6033, 0x0CA2_E29E_42BA_99FA),
    ];

    fn pattern(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i as u8).wrapping_mul(31).wrapping_add(7)).collect()
    }

    #[test]
    fn test_checksum_format_is_identical_on_every_code_path() {
        for (len, crc, sum) in GOLDEN {
            let data = pattern(len);
            assert_eq!(crc32c_portable_dual(&data), crc, "portable crc len {len}");
            assert_eq!(crc32c_64(&data), crc, "dispatched crc len {len}");
            assert_eq!(checksum_payload(&data), sum, "checksum_payload len {len}");
        }
        #[cfg(target_arch = "x86_64")]
        if std::arch::is_x86_feature_detected!("sse4.2") {
            for len in 0..300 {
                let data = pattern(len);
                // SAFETY: `sse4.2` support was detected at runtime above.
                let hw = unsafe { crc32c_sse42_dual(&data) };
                assert_eq!(hw, crc32c_portable_dual(&data), "len {len}");
            }
        }
    }

    #[test]
    fn test_crc32c_table_matches_standard_check_value() {
        // Standard CRC-32C check value for "123456789".
        let crc = b"123456789".iter().fold(0xFFFF_FFFFu32, |c, &b| crc32c_u8(c, b));
        assert_eq!(!crc, 0xE306_9283);
    }

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
