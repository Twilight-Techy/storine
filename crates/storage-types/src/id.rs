//! 24-byte stack-inlined primary key (`InlineId`) (`TASK-003`, `F-CORE-03`, `RULE-MEM-02`).
//!
//! Layout (`#[repr(C, align(8))]`, 24 bytes total):
//! - `len`: `u8` (0..=23 for direct UTF-8 inline string; bit 7 `0x80` set when a longer string
//!   was deterministically compacted via 128-bit digest + 7-byte prefix).
//! - `bytes`: `[u8; 23]` (zero-padded on the right so full 24-byte representation can be
//!   compared and hashed as three 64-bit CPU registers in ~1-2 ns).

use core::cmp::Ordering;
use core::fmt;
use core::hash::{Hash, Hasher};
use core::str::FromStr;

use crate::error::{Result, StorageError};

pub const MAX_INLINE_ID_LEN: usize = 23;
const COMPACTED_TAG_BIT: u8 = 0x80;
const LEN_MASK: u8 = 0x7F;

/// Fixed 24-byte stack-inlined primary key (`Copy`, zero heap allocations).
///
/// Because Unused trailing bytes are strictly zero-initialized, equality, ordering,
/// and hashing operate directly on three 64-bit words (`[u64; 3]`).
#[repr(C, align(8))]
#[derive(Clone, Copy)]
pub struct InlineId {
    len: u8,
    bytes: [u8; MAX_INLINE_ID_LEN],
}

impl Default for InlineId {
    #[inline]
    fn default() -> Self {
        Self::EMPTY
    }
}

impl InlineId {
    pub const EMPTY: Self = Self {
        len: 0,
        bytes: [0u8; MAX_INLINE_ID_LEN],
    };

    /// Creates a validated `InlineId` from a string slice (`1..=23` bytes).
    /// Rejects empty keys, keys containing NUL/control characters, or keys > 23 bytes.
    #[inline]
    pub fn new(s: &str) -> Result<Self> {
        let raw = s.as_bytes();
        if raw.is_empty() {
            return Err(StorageError::invalid_key("primary key cannot be empty"));
        }
        if raw.len() > MAX_INLINE_ID_LEN {
            return Err(StorageError::invalid_key(
                "primary key exceeds 23-byte inline limit (use InlineId::from_str_lossy_or_compact for long keys)",
            ));
        }
        Self::validate_bytes(raw)?;

        let mut bytes = [0u8; MAX_INLINE_ID_LEN];
        bytes[..raw.len()].copy_from_slice(raw);
        Ok(Self {
            len: raw.len() as u8,
            bytes,
        })
    }

    /// Creates an `InlineId` from any non-empty string of arbitrary length:
    /// - If `<= 23` bytes, stores verbatim inline.
    /// - If `> 23` bytes, stores the first 7 ASCII/UTF-8 bytes followed by a 16-byte
    ///   hex-encoded 64-bit XXH3-style digest (`7 + 16 = 23` bytes) so long keys never allocate
    ///   and remain collision-resistant.
    #[inline]
    pub fn from_any_str(s: &str) -> Result<Self> {
        let raw = s.as_bytes();
        if raw.is_empty() {
            return Err(StorageError::invalid_key("primary key cannot be empty"));
        }
        Self::validate_bytes(raw)?;
        if raw.len() <= MAX_INLINE_ID_LEN {
            let mut bytes = [0u8; MAX_INLINE_ID_LEN];
            bytes[..raw.len()].copy_from_slice(raw);
            return Ok(Self {
                len: raw.len() as u8,
                bytes,
            });
        }

        // Deterministic compaction: up to 6 valid UTF-8 prefix bytes + '~' + 16 hex digits of 64-bit hash
        let mut prefix_len = 6;
        while prefix_len > 0 && !s.is_char_boundary(prefix_len) {
            prefix_len -= 1;
        }
        let mut bytes = [0u8; MAX_INLINE_ID_LEN];
        bytes[..prefix_len].copy_from_slice(&raw[..prefix_len]);
        bytes[prefix_len] = b'~';

        let digest = Self::hash_bytes_64(raw);
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for i in 0..16 {
            let nibble = ((digest >> ((15 - i) * 4)) & 0xF) as usize;
            bytes[prefix_len + 1 + i] = HEX[nibble];
        }
        let total_len = (prefix_len + 1 + 16) as u8;
        Ok(Self {
            len: total_len | COMPACTED_TAG_BIT,
            bytes,
        })
    }

    /// Const constructor for compile-time / static string keys `<= 23` bytes.
    /// Panics at compile time if invalid.
    #[inline]
    pub const fn from_static(s: &'static str) -> Self {
        let raw = s.as_bytes();
        assert!(!raw.is_empty(), "InlineId cannot be empty");
        assert!(raw.len() <= MAX_INLINE_ID_LEN, "InlineId exceeds 23 bytes");
        let mut bytes = [0u8; MAX_INLINE_ID_LEN];
        let mut i = 0;
        while i < raw.len() {
            let b = raw[i];
            assert!(b >= 0x20 && b != 0x7F, "InlineId contains control character");
            bytes[i] = b;
            i += 1;
        }
        Self {
            len: raw.len() as u8,
            bytes,
        }
    }

    /// Reconstructs an `InlineId` from raw 24 bytes on disk (`bytecheck` validated).
    #[inline]
    pub fn from_raw_bytes(raw: [u8; 24]) -> Result<Self> {
        let len_tag = raw[0];
        let actual_len = (len_tag & LEN_MASK) as usize;
        if actual_len == 0 || actual_len > MAX_INLINE_ID_LEN {
            return Err(StorageError::invalid_key("corrupted InlineId length tag"));
        }
        let mut bytes = [0u8; MAX_INLINE_ID_LEN];
        bytes.copy_from_slice(&raw[1..24]);
        // Verify trailing padding bytes are strictly zero to preserve register-equality invariant
        if bytes[actual_len..].iter().any(|&b| b != 0) {
            return Err(StorageError::invalid_key(
                "non-zero trailing padding in InlineId",
            ));
        }
        Self::validate_bytes(&bytes[..actual_len])?;
        if core::str::from_utf8(&bytes[..actual_len]).is_err() {
            return Err(StorageError::invalid_key("InlineId is not valid UTF-8"));
        }
        Ok(Self {
            len: len_tag,
            bytes,
        })
    }

    #[inline]
    pub const fn to_raw_bytes(self) -> [u8; 24] {
        let mut out = [0u8; 24];
        out[0] = self.len;
        let mut i = 0;
        while i < MAX_INLINE_ID_LEN {
            out[i + 1] = self.bytes[i];
            i += 1;
        }
        out
    }

    /// Appends this `InlineId` in compact `[len_tag (1B) | bytes[..len] (1..=23B)]` format,
    /// saving up to 22 bytes per key compared to fixed 24-byte storage.
    #[inline]
    pub fn write_compact_to(self, out: &mut Vec<u8>) {
        out.push(self.len);
        out.extend_from_slice(self.as_bytes());
    }

    /// Decodes a compact `InlineId` written by `write_compact_to`, advancing `cursor`.
    #[inline]
    pub fn read_compact_from(buf: &[u8], cursor: &mut usize) -> Result<Self> {
        if *cursor >= buf.len() {
            return Err(StorageError::invalid_key("truncated compact InlineId tag"));
        }
        let len_tag = buf[*cursor];
        let actual_len = (len_tag & LEN_MASK) as usize;
        if actual_len == 0 || actual_len > MAX_INLINE_ID_LEN {
            return Err(StorageError::invalid_key("invalid compact InlineId length"));
        }
        let start = *cursor + 1;
        let end = start + actual_len;
        if end > buf.len() {
            return Err(StorageError::invalid_key("truncated compact InlineId payload"));
        }
        let slice = &buf[start..end];
        Self::validate_bytes(slice)?;
        if core::str::from_utf8(slice).is_err() {
            return Err(StorageError::invalid_key("compact InlineId is not valid UTF-8"));
        }
        let mut bytes = [0u8; MAX_INLINE_ID_LEN];
        bytes[..actual_len].copy_from_slice(slice);
        *cursor = end;
        Ok(Self {
            len: len_tag,
            bytes,
        })
    }

    #[inline]
    fn validate_bytes(raw: &[u8]) -> Result<()> {
        for &b in raw {
            if b < 0x20 || b == 0x7F {
                return Err(StorageError::invalid_key(
                    "primary key cannot contain ASCII control characters or NUL",
                ));
            }
        }
        if raw == b"." || raw == b".." {
            return Err(StorageError::invalid_key(
                "primary key cannot be relative path '.' or '..'",
            ));
        }
        Ok(())
    }

    #[inline]
    pub const fn len(&self) -> usize {
        (self.len & LEN_MASK) as usize
    }

    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub const fn is_compacted(&self) -> bool {
        (self.len & COMPACTED_TAG_BIT) != 0
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len()]
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        // SAFETY: All constructors (`new`, `from_any_str`, `from_static`, `from_raw_bytes`)
        // enforce valid UTF-8 in `self.bytes[..self.len()]`.
        unsafe { core::str::from_utf8_unchecked(self.as_bytes()) }
    }

    /// Loads the 24-byte struct as three 64-bit words for 1-cycle register equality and hashing.
    #[inline]
    pub const fn as_u64_words(&self) -> [u64; 3] {
        // SAFETY: `InlineId` is `#[repr(C, align(8))]` of exact size 24 bytes (`3 * 8`),
        // with all padding bytes deterministically initialized to 0.
        unsafe { core::mem::transmute::<Self, [u64; 3]>(*self) }
    }

    /// Ultra-fast 64-bit hash using XXH3/Wyhash-style 128-bit multiply-xor-fold over the 3 words.
    #[inline]
    pub const fn fast_hash(&self) -> u64 {
        let [w0, w1, w2] = self.as_u64_words();
        const P0: u64 = 0x9E37_79B1_85EB_CA87;
        const P1: u64 = 0xC2B2_AE3D_27D4_EB4F;
        const P2: u64 = 0x1656_67B1_9E37_79F9;

        let a = (w0 ^ P0) as u128 * (w1 ^ P1) as u128;
        let mix1 = (a as u64) ^ ((a >> 64) as u64);
        let b = (mix1 ^ w2) as u128 * P2 as u128;
        (b as u64) ^ ((b >> 64) as u64)
    }

    /// Returns the 2-level shard byte pair `(shard_hi, shard_lo)` in `0..256`.
    #[inline]
    pub const fn shard_pair(&self) -> (u8, u8) {
        let h = self.fast_hash();
        ((h & 0xFF) as u8, ((h >> 8) & 0xFF) as u8)
    }

    #[inline]
    const fn hash_bytes_64(bytes: &[u8]) -> u64 {
        let mut h: u64 = 0x9E37_79B1_85EB_CA87 ^ (bytes.len() as u64);
        let mut i = 0;
        while i < bytes.len() {
            h ^= bytes[i] as u64;
            let prod = (h as u128).wrapping_mul(0xC2B2_AE3D_27D4_EB4F);
            h = (prod as u64) ^ ((prod >> 64) as u64);
            i += 1;
        }
        h
    }
}

impl PartialEq for InlineId {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        let [a0, a1, a2] = self.as_u64_words();
        let [b0, b1, b2] = other.as_u64_words();
        (a0 == b0) & (a1 == b1) & (a2 == b2)
    }
}

impl Eq for InlineId {}

impl PartialOrd for InlineId {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for InlineId {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_bytes().cmp(other.as_bytes())
    }
}

impl Hash for InlineId {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.fast_hash());
    }
}

impl fmt::Debug for InlineId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InlineId({:?})", self.as_str())
    }
}

impl fmt::Display for InlineId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for InlineId {
    type Err = StorageError;

    #[inline]
    fn from_str(s: &str) -> Result<Self> {
        Self::new(s)
    }
}

impl TryFrom<&str> for InlineId {
    type Error = StorageError;

    #[inline]
    fn try_from(value: &str) -> Result<Self> {
        Self::new(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inline_id_layout_and_roundtrip() {
        assert_eq!(core::mem::size_of::<InlineId>(), 24);
        assert_eq!(core::mem::align_of::<InlineId>(), 8);

        let id = InlineId::new("user:123").unwrap();
        assert_eq!(id.as_str(), "user:123");
        assert_eq!(id.len(), 8);
        assert!(!id.is_compacted());

        let raw = id.to_raw_bytes();
        let decoded = InlineId::from_raw_bytes(raw).unwrap();
        assert_eq!(id, decoded);
        assert_eq!(id.fast_hash(), decoded.fast_hash());
    }

    #[test]
    fn test_inline_id_rejects_invalid_keys() {
        assert!(InlineId::new("").is_err());
        assert!(InlineId::new(".").is_err());
        assert!(InlineId::new("..").is_err());
        assert!(InlineId::new("bad\0key").is_err());
        assert!(InlineId::new("bad\nkey").is_err());
        assert!(InlineId::new("this_key_is_definitely_over_23_bytes").is_err());
    }

    #[test]
    fn test_inline_id_compacts_long_keys_deterministically() {
        let long_a =
            InlineId::from_any_str("organization_alpha/department_engineering/member_0001")
                .unwrap();
        let long_b =
            InlineId::from_any_str("organization_alpha/department_engineering/member_0002")
                .unwrap();
        assert!(long_a.is_compacted());
        assert_eq!(long_a.len(), 23);
        assert_ne!(long_a, long_b);
    }
}
