//! Little-endian wire-format readers (`RULE-ERR-01`).
//!
//! These replace `try_into().unwrap()` conversions with direct indexing. They do not validate
//! lengths: callers MUST check `buf.len()` first. Indexing is always bounds-checked by Rust, so
//! an out-of-range offset panics (in every build profile) but can never cause Undefined Behavior.

/// Read a `u16` from `buf[off..off+2]` in little-endian order.
#[inline(always)]
pub const fn read_u16_le(buf: &[u8], off: usize) -> u16 {
    u16::from_le_bytes([buf[off], buf[off + 1]])
}

/// Read a `u32` from `buf[off..off+4]` in little-endian order.
#[inline(always)]
pub const fn read_u32_le(buf: &[u8], off: usize) -> u32 {
    u32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]])
}

/// Read a `u64` from `buf[off..off+8]` in little-endian order.
#[inline(always)]
pub const fn read_u64_le(buf: &[u8], off: usize) -> u64 {
    u64::from_le_bytes([
        buf[off],
        buf[off + 1],
        buf[off + 2],
        buf[off + 3],
        buf[off + 4],
        buf[off + 5],
        buf[off + 6],
        buf[off + 7],
    ])
}

/// Read an `i64` from `buf[off..off+8]` in little-endian order.
#[inline(always)]
pub const fn read_i64_le(buf: &[u8], off: usize) -> i64 {
    i64::from_le_bytes([
        buf[off],
        buf[off + 1],
        buf[off + 2],
        buf[off + 3],
        buf[off + 4],
        buf[off + 5],
        buf[off + 6],
        buf[off + 7],
    ])
}

/// Copy 24 bytes from `buf[off..off+24]` into a `[u8; 24]`.
#[inline(always)]
pub fn read_array_24(buf: &[u8], off: usize) -> [u8; 24] {
    let mut out = [0u8; 24];
    out.copy_from_slice(&buf[off..off + 24]);
    out
}
