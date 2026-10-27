//! 64-byte cache-line-aligned `RecordHeader` (`TASK-004`, `F-CORE-02`, `RULE-CPU-01`).

use crate::error::{Result, StorageError};
use crate::Version;

/// Magic bytes `"STGE"` (`0x5354_4745` in little-endian).
pub const RECORD_MAGIC: u32 = 0x5354_4745;

/// Current binary wire format version.
pub const WIRE_FORMAT_VERSION: u16 = 1;

/// Exact size of `RecordHeader` in bytes (1 full CPU cache line).
pub const HEADER_SIZE: usize = 64;

/// Maximum single record payload size (256 MiB) to prevent corrupt-header OOM attacks.
pub const MAX_PAYLOAD_SIZE: u32 = 256 * 1024 * 1024;

/// Bitflags stored in `RecordHeader::flags`.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct RecordFlags(pub u16);

impl RecordFlags {
    pub const NONE: Self = Self(0);
    /// Record has been logically deleted (tombstone marker in WAL or arena segment).
    pub const TOMBSTONE: Self = Self(1 << 0);
    /// Payload is compressed with fast LZ4-style block compression.
    pub const COMPRESSED_LZ4: Self = Self(1 << 1);
    /// Record mapping is pinned in memory and exempt from `MmapPool` eviction.
    pub const PINNED: Self = Self(1 << 2);
    /// Ping-pong double-slot indicator: when set, Slot 1 is active instead of Slot 0.
    pub const ACTIVE_SLOT_1: Self = Self(1 << 3);
    /// Record has an active TTL expiration timestamp in `ttl_expires_at_ns`.
    pub const HAS_TTL: Self = Self(1 << 4);
    /// Payload is encrypted at rest (`TASK-044`, `F-SEC-02`).
    pub const ENCRYPTED: Self = Self(1 << 5);
    /// Payload is plaintext and followed by a 32-byte MAC trailer `[key_id | nonce | tag]`
    /// authenticating it (`F-SEC-02`, authenticated-only protection).
    pub const AUTHENTICATED: Self = Self(1 << 6);

    #[inline]
    pub const fn contains(self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }

    #[inline]
    pub const fn with(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    #[inline]
    pub const fn without(self, other: Self) -> Self {
        Self(self.0 & !other.0)
    }
}

/// Fixed 64-byte cache-line-aligned record header (`#[repr(C, align(64))]`).
///
/// Layout:
/// - `0..4`:   `magic: u32` (`RECORD_MAGIC`)
/// - `4..6`:   `format_version: u16` (`WIRE_FORMAT_VERSION`)
/// - `6..8`:   `schema_version: u16`
/// - `8..16`:  `record_version: u64` (OCC monotonic version)
/// - `16..24`: `updated_at_ns: u64` (UTC UNIX timestamp in nanoseconds)
/// - `24..32`: `payload_checksum: u64` (`XXH3-64` / hardware `CRC32C` of payload bytes)
/// - `32..36`: `payload_len: u32` (byte length of payload on disk)
/// - `36..40`: `uncompressed_len: u32` (uncompressed byte length)
/// - `40..44`: `root_offset: u32` (relative root offset of `Archived<T>` inside payload)
/// - `44..46`: `flags: u16` (`RecordFlags`)
/// - `46..48`: `collection_id: u16`
/// - `48..56`: `ttl_expires_at_ns: u64` (0 = never expires)
/// - `56..64`: `header_checksum: u64` (integrity hash of header bytes `0..56`)
#[repr(C, align(64))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordHeader {
    pub magic: u32,
    pub format_version: u16,
    pub schema_version: u16,
    pub record_version: u64,
    pub updated_at_ns: u64,
    pub payload_checksum: u64,
    pub payload_len: u32,
    pub uncompressed_len: u32,
    pub root_offset: u32,
    pub flags: u16,
    pub collection_id: u16,
    pub ttl_expires_at_ns: u64,
    pub header_checksum: u64,
}

impl RecordHeader {
    #[inline]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        schema_version: u16,
        record_version: Version,
        updated_at_ns: u64,
        payload_checksum: u64,
        payload_len: u32,
        uncompressed_len: u32,
        root_offset: u32,
        flags: RecordFlags,
        collection_id: u16,
        ttl_expires_at_ns: u64,
    ) -> Self {
        let mut hdr = Self {
            magic: RECORD_MAGIC,
            format_version: WIRE_FORMAT_VERSION,
            schema_version,
            record_version: record_version.0,
            updated_at_ns,
            payload_checksum,
            payload_len,
            uncompressed_len,
            root_offset,
            flags: flags.0,
            collection_id,
            ttl_expires_at_ns,
            header_checksum: 0,
        };
        hdr.header_checksum = hdr.compute_header_checksum();
        hdr
    }

    #[inline]
    pub const fn flags_typed(&self) -> RecordFlags {
        RecordFlags(self.flags)
    }

    #[inline]
    pub const fn version(&self) -> Version {
        Version(self.record_version)
    }

    #[inline]
    pub const fn is_tombstone(&self) -> bool {
        self.flags_typed().contains(RecordFlags::TOMBSTONE)
    }

    #[inline]
    pub const fn is_expired(&self, now_ns: u64) -> bool {
        self.flags_typed().contains(RecordFlags::HAS_TTL)
            && self.ttl_expires_at_ns > 0
            && now_ns >= self.ttl_expires_at_ns
    }

    /// Computes a fast 64-bit integrity hash over bytes `0..56` of the header.
    #[inline]
    pub const fn compute_header_checksum(&self) -> u64 {
        let w0 = (self.magic as u64)
            | ((self.format_version as u64) << 32)
            | ((self.schema_version as u64) << 48);
        let w1 = self.record_version;
        let w2 = self.updated_at_ns;
        let w3 = self.payload_checksum;
        let w4 = (self.payload_len as u64) | ((self.uncompressed_len as u64) << 32);
        let w5 = (self.root_offset as u64)
            | ((self.flags as u64) << 32)
            | ((self.collection_id as u64) << 48);
        let w6 = self.ttl_expires_at_ns;

        let mut h = 0x517C_C1B7_2722_0A95u64;
        h = Self::mix64(h ^ w0);
        h = Self::mix64(h ^ w1);
        h = Self::mix64(h ^ w2);
        h = Self::mix64(h ^ w3);
        h = Self::mix64(h ^ w4);
        h = Self::mix64(h ^ w5);
        h = Self::mix64(h ^ w6);
        h
    }

    #[inline]
    const fn mix64(mut z: u64) -> u64 {
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Encodes this header into a 64-byte little-endian array.
    #[inline]
    pub fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..6].copy_from_slice(&self.format_version.to_le_bytes());
        buf[6..8].copy_from_slice(&self.schema_version.to_le_bytes());
        buf[8..16].copy_from_slice(&self.record_version.to_le_bytes());
        buf[16..24].copy_from_slice(&self.updated_at_ns.to_le_bytes());
        buf[24..32].copy_from_slice(&self.payload_checksum.to_le_bytes());
        buf[32..36].copy_from_slice(&self.payload_len.to_le_bytes());
        buf[36..40].copy_from_slice(&self.uncompressed_len.to_le_bytes());
        buf[40..44].copy_from_slice(&self.root_offset.to_le_bytes());
        buf[44..46].copy_from_slice(&self.flags.to_le_bytes());
        buf[46..48].copy_from_slice(&self.collection_id.to_le_bytes());
        buf[48..56].copy_from_slice(&self.ttl_expires_at_ns.to_le_bytes());
        buf[56..64].copy_from_slice(&self.header_checksum.to_le_bytes());
        buf
    }

    /// Decodes and validates a `RecordHeader` from a byte slice.
    #[inline]
    pub fn from_slice_verified(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < HEADER_SIZE {
            return Err(StorageError::corrupted_header(
                "slice shorter than 64-byte RecordHeader",
            ));
        }
        let magic = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        if magic != RECORD_MAGIC {
            return Err(StorageError::corrupted_header("invalid magic bytes"));
        }
        let format_version = u16::from_le_bytes([bytes[4], bytes[5]]);
        if format_version != WIRE_FORMAT_VERSION {
            return Err(StorageError::corrupted_header(
                "unsupported wire format version",
            ));
        }
        let schema_version = u16::from_le_bytes([bytes[6], bytes[7]]);
        let record_version = crate::wire::read_u64_le(bytes, 8);
        let updated_at_ns = crate::wire::read_u64_le(bytes, 16);
        let payload_checksum = crate::wire::read_u64_le(bytes, 24);
        let payload_len = crate::wire::read_u32_le(bytes, 32);
        let uncompressed_len = crate::wire::read_u32_le(bytes, 36);
        let root_offset = crate::wire::read_u32_le(bytes, 40);
        let flags = u16::from_le_bytes([bytes[44], bytes[45]]);
        let collection_id = u16::from_le_bytes([bytes[46], bytes[47]]);
        let ttl_expires_at_ns = crate::wire::read_u64_le(bytes, 48);
        let header_checksum = crate::wire::read_u64_le(bytes, 56);

        let hdr = Self {
            magic,
            format_version,
            schema_version,
            record_version,
            updated_at_ns,
            payload_checksum,
            payload_len,
            uncompressed_len,
            root_offset,
            flags,
            collection_id,
            ttl_expires_at_ns,
            header_checksum,
        };

        if hdr.compute_header_checksum() != header_checksum {
            return Err(StorageError::corrupted_header(
                "header checksum verification failed",
            ));
        }
        if payload_len > MAX_PAYLOAD_SIZE || uncompressed_len > MAX_PAYLOAD_SIZE {
            return Err(StorageError::corrupted_header(
                "payload length exceeds maximum safety ceiling (256 MiB)",
            ));
        }
        let is_tombstone = RecordFlags(flags).contains(RecordFlags::TOMBSTONE);
        if !is_tombstone && payload_len > 0 && root_offset >= uncompressed_len {
            return Err(StorageError::corrupted_header(
                "root_offset points outside uncompressed payload bounds",
            ));
        }
        Ok(hdr)
    }
    // Zero-copy access to a header inside a mapped frame is provided by
    // `storage_codec::RecordCodec::header_ref` (unsafe code is confined to storage-codec / storage-mmap).
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_header_size_alignment_and_roundtrip() {
        assert_eq!(core::mem::size_of::<RecordHeader>(), 64);
        assert_eq!(core::mem::align_of::<RecordHeader>(), 64);

        let hdr = RecordHeader::new(
            1,
            Version(42),
            1_700_000_000_000,
            0xDEAD_BEEF_CAFE_BABE,
            128,
            128,
            64,
            RecordFlags::PINNED,
            3,
            0,
        );
        let raw = hdr.to_bytes();
        let decoded = RecordHeader::from_slice_verified(&raw).unwrap();
        assert_eq!(hdr, decoded);

        // Flip 1 bit in payload_len and verify header_checksum catches it immediately
        let mut corrupted = raw;
        corrupted[32] ^= 0x01;
        assert!(RecordHeader::from_slice_verified(&corrupted).is_err());
    }
}
