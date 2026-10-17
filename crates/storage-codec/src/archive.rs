//! Zero-copy relative-offset archive system (`TASK-006`, `F-CORE-01`, `F-DURA-05`, `RULE-SAFE-04`).
//!
//! Provides `ZeroCopyRecord`, `ArchivedString`, `ArchivedBytes`, `ArchivedSlice<T>`, and
//! `ArchivedOption<T>` so on-disk `.rec` / `.seg` memory-mapped bytes can be dereferenced
//! directly as `&T::Archived` with **zero heap allocations** and **zero copies**.

use core::fmt;
use core::marker::PhantomData;
use storage_types::{
    InlineId, RecordFlags, RecordHeader, Result, StorageError, Version, HEADER_SIZE,
};

use crate::checksum::checksum_payload;
use crate::compress::{compress_lz4, decompress_lz4};
use crate::scratch::{with_thread_scratch, AlignedBytes, AlignedScratchBuf};

/// Relative-offset UTF-8 string stored inside an archived payload (`8 bytes`, `#[repr(C)]`).
/// Resolves to `&str` in 1 instruction directly from the `mmap` page with zero heap allocation.
#[repr(C, align(4))]
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct ArchivedString {
    pub offset: u32,
    pub len: u32,
}

impl ArchivedString {
    pub const EMPTY: Self = Self { offset: 0, len: 0 };

    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub const fn len(&self) -> usize {
        self.len as usize
    }

    /// Validates that `offset..offset+len` lies within `payload` and contains valid UTF-8.
    /// Called once when a disk page is first mapped (`RULE-SAFE-04`).
    #[inline]
    pub fn validate(&self, payload: &[u8]) -> Result<()> {
        if self.len == 0 {
            return Ok(());
        }
        let start = self.offset as usize;
        let end = start
            .checked_add(self.len as usize)
            .ok_or_else(|| StorageError::invalid_archive("ArchivedString offset overflow"))?;
        let slice = payload.get(start..end).ok_or_else(|| {
            StorageError::invalid_archive("ArchivedString out of payload bounds")
        })?;
        if core::str::from_utf8(slice).is_err() {
            return Err(StorageError::invalid_archive(
                "ArchivedString contains invalid UTF-8 bytes",
            ));
        }
        Ok(())
    }

    /// Zero-copy string access on a verified payload buffer (`0` allocations, bounds-checked safe slice).
    #[inline]
    pub fn as_str<'a>(&self, payload: &'a [u8]) -> &'a str {
        if self.len == 0 {
            return "";
        }
        let start = self.offset as usize;
        let end = start + (self.len as usize);
        debug_assert!(end <= payload.len());
        // SAFETY: `ArchivedString::validate` is executed once upon mapping/loading before any
        // `RecordView` is constructed, proving both slice bounds and UTF-8 validity.
        unsafe {
            let bytes = payload.get_unchecked(start..end);
            core::str::from_utf8_unchecked(bytes)
        }
    }
}

impl fmt::Debug for ArchivedString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ArchivedString(off={}, len={})", self.offset, self.len)
    }
}

/// Relative-offset byte slice stored inside an archived payload (`8 bytes`, `#[repr(C)]`).
#[repr(C, align(4))]
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub struct ArchivedBytes {
    pub offset: u32,
    pub len: u32,
}

impl ArchivedBytes {
    pub const EMPTY: Self = Self { offset: 0, len: 0 };

    #[inline]
    pub fn validate(&self, payload: &[u8]) -> Result<()> {
        if self.len == 0 {
            return Ok(());
        }
        let start = self.offset as usize;
        let end = start
            .checked_add(self.len as usize)
            .ok_or_else(|| StorageError::invalid_archive("ArchivedBytes offset overflow"))?;
        if end > payload.len() {
            return Err(StorageError::invalid_archive(
                "ArchivedBytes out of payload bounds",
            ));
        }
        Ok(())
    }

    #[inline]
    pub fn as_slice<'a>(&self, payload: &'a [u8]) -> &'a [u8] {
        if self.len == 0 {
            return &[];
        }
        let start = self.offset as usize;
        let end = start + (self.len as usize);
        debug_assert!(end <= payload.len());
        // SAFETY: Validated once during archive verification (`ArchivedBytes::validate`).
        unsafe { payload.get_unchecked(start..end) }
    }
}

/// Relative-offset typed slice `&[A]` stored inside an archived payload (`8 bytes`, `#[repr(C)]`).
/// Enables zero-copy access to `&[InlineId]`, `&[f32]`, `&[u64]`, etc., directly inside `mmap`.
#[repr(C, align(4))]
pub struct ArchivedSlice<A> {
    pub offset: u32,
    pub len: u32,
    _marker: PhantomData<A>,
}

impl<A> Clone for ArchivedSlice<A> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<A> Copy for ArchivedSlice<A> {}

impl<A> Default for ArchivedSlice<A> {
    #[inline]
    fn default() -> Self {
        Self {
            offset: 0,
            len: 0,
            _marker: PhantomData,
        }
    }
}

impl<A> fmt::Debug for ArchivedSlice<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ArchivedSlice(off={}, len={})", self.offset, self.len)
    }
}

impl<A: Copy> ArchivedSlice<A> {
    pub const EMPTY: Self = Self {
        offset: 0,
        len: 0,
        _marker: PhantomData,
    };

    #[inline]
    pub const fn len(&self) -> usize {
        self.len as usize
    }

    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Validates bounds and alignment of the slice within `payload`.
    #[inline]
    pub fn validate(&self, payload: &[u8]) -> Result<()> {
        if self.len == 0 {
            return Ok(());
        }
        let elem_size = core::mem::size_of::<A>();
        let elem_align = core::mem::align_of::<A>();
        let start = self.offset as usize;
        if (start & (elem_align - 1)) != 0 {
            return Err(StorageError::invalid_archive(
                "ArchivedSlice offset is not aligned for element type",
            ));
        }
        let byte_len = (self.len as usize)
            .checked_mul(elem_size)
            .ok_or_else(|| StorageError::invalid_archive("ArchivedSlice byte length overflow"))?;
        let end = start
            .checked_add(byte_len)
            .ok_or_else(|| StorageError::invalid_archive("ArchivedSlice end offset overflow"))?;
        if end > payload.len() {
            return Err(StorageError::invalid_archive(
                "ArchivedSlice extends beyond payload bounds",
            ));
        }
        if ((payload.as_ptr() as usize).wrapping_add(start) & (elem_align - 1)) != 0 {
            return Err(StorageError::invalid_archive(
                "ArchivedSlice runtime address is unaligned",
            ));
        }
        Ok(())
    }

    /// Returns a zero-copy slice `&'a [A]` pointing directly into the verified `payload`.
    #[inline]
    pub fn as_slice<'a>(&self, payload: &'a [u8]) -> &'a [A] {
        if self.len == 0 {
            return &[];
        }
        let start = self.offset as usize;
        debug_assert!(start + (self.len as usize) * core::mem::size_of::<A>() <= payload.len());
        // SAFETY: `validate` verified alignment and bounds of `start..start + len * size_of::<A>()`
        // inside the 64-byte-aligned payload buffer.
        unsafe {
            let ptr = payload.as_ptr().add(start) as *const A;
            core::slice::from_raw_parts(ptr, self.len as usize)
        }
    }
}

/// Fixed-layout `#[repr(C)]` optional value for zero-copy archived structs.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct ArchivedOption<A> {
    pub is_some: u8,
    pub value: A,
}

impl<A: Copy + Default> ArchivedOption<A> {
    #[inline]
    pub fn from_option(opt: Option<A>) -> Self {
        match opt {
            Some(v) => Self {
                is_some: 1,
                value: v,
            },
            None => Self {
                is_some: 0,
                value: A::default(),
            },
        }
    }

    #[inline]
    pub const fn as_option(&self) -> Option<&A> {
        if self.is_some != 0 {
            Some(&self.value)
        } else {
            None
        }
    }

    #[inline]
    pub const fn to_option(self) -> Option<A> {
        if self.is_some != 0 {
            Some(self.value)
        } else {
            None
        }
    }
}

/// Builder that serializes variable-length child fields into a 64-byte-aligned payload buffer
/// before placing the fixed-size `#[repr(C)]` root struct at `root_offset`.
#[derive(Debug)]
pub struct ArchiveBuilder<'a> {
    buf: &'a mut AlignedScratchBuf,
    payload_base: usize,
}

impl<'a> ArchiveBuilder<'a> {
    #[inline]
    pub fn new(buf: &'a mut AlignedScratchBuf, payload_base: usize) -> Self {
        Self { buf, payload_base }
    }

    #[inline]
    pub fn write_str(&mut self, s: &str) -> ArchivedString {
        if s.is_empty() {
            return ArchivedString::EMPTY;
        }
        let abs_off = self.buf.extend_from_slice(s.as_bytes());
        let rel_off = (abs_off - self.payload_base) as u32;
        ArchivedString {
            offset: rel_off,
            len: s.len() as u32,
        }
    }

    #[inline]
    pub fn write_bytes(&mut self, bytes: &[u8]) -> ArchivedBytes {
        if bytes.is_empty() {
            return ArchivedBytes::EMPTY;
        }
        let abs_off = self.buf.extend_from_slice(bytes);
        let rel_off = (abs_off - self.payload_base) as u32;
        ArchivedBytes {
            offset: rel_off,
            len: bytes.len() as u32,
        }
    }

    #[inline]
    pub fn write_slice<A: Copy>(&mut self, items: &[A]) -> ArchivedSlice<A> {
        if items.is_empty() {
            return ArchivedSlice::EMPTY;
        }
        let align = core::mem::align_of::<A>();
        self.buf.align_to(align);
        let byte_len = core::mem::size_of_val(items);
        // SAFETY: `A: Copy` is written into a properly aligned region of the same endianness.
        let raw_bytes =
            unsafe { core::slice::from_raw_parts(items.as_ptr() as *const u8, byte_len) };
        let abs_off = self.buf.extend_from_slice(raw_bytes);
        let rel_off = (abs_off - self.payload_base) as u32;
        ArchivedSlice {
            offset: rel_off,
            len: items.len() as u32,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn write_root<R: Copy>(&mut self, root: &R) -> u32 {
        let align = core::mem::align_of::<R>().max(8);
        self.buf.align_to(align);
        let size = core::mem::size_of::<R>();
        // SAFETY: `R: Copy` is a `#[repr(C)]` archived root struct.
        let raw_bytes = unsafe { core::slice::from_raw_parts(root as *const R as *const u8, size) };
        let abs_off = self.buf.extend_from_slice(raw_bytes);
        (abs_off - self.payload_base) as u32
    }
}

/// Trait implemented by domain types stored in the engine.
///
/// Separates the ergonomic owned Rust struct (`Self`, mutated inside `db.update` closures)
/// from its `#[repr(C)]` zero-copy on-disk representation (`Self::Archived`, read directly
/// from `mmap` pages without heap allocations).
pub trait ZeroCopyRecord: Sized + Clone + Send + Sync + 'static {
    /// Fixed-size `#[repr(C)]` archived root representation mapped directly in virtual memory.
    type Archived: Copy + fmt::Debug + Send + Sync + 'static;

    /// Current schema version of this record type.
    const SCHEMA_VERSION: u16 = 1;

    /// Collection identifier tag (`0..65535`).
    const COLLECTION_ID: u16 = 1;

    /// Writes variable-length children into `builder` and returns the `Self::Archived` root.
    fn archive(&self, builder: &mut ArchiveBuilder<'_>) -> Self::Archived;

    /// Validates all relative offsets (`ArchivedString`, `ArchivedSlice`, `InlineId`) inside `root`
    /// once when a disk page or WAL frame is first loaded (`RULE-SAFE-04`).
    fn validate_archived(root: &Self::Archived, payload: &[u8]) -> Result<()>;

    /// Reconstructs an owned mutable `Self` from the verified zero-copy `root` + `payload`
    /// (used only when entering a `db.update(id, |draft: &mut Self| ...)` mutation closure).
    fn unarchive(root: &Self::Archived, payload: &[u8]) -> Result<Self>;
}

/// Zero-copy read view borrowing directly from an `Mmap` page or `HotDirtyArc` buffer.
/// Performs **zero heap allocations** on creation, field access, or drop (`RULE-MEM-01`).
pub struct RecordView<'a, T: ZeroCopyRecord> {
    header: &'a RecordHeader,
    payload: &'a [u8],
    root: &'a T::Archived,
}

impl<'a, T: ZeroCopyRecord> Clone for RecordView<'a, T> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, T: ZeroCopyRecord> Copy for RecordView<'a, T> {}

impl<'a, T: ZeroCopyRecord> RecordView<'a, T> {
    #[inline]
    pub const fn header(&self) -> &'a RecordHeader {
        self.header
    }

    #[inline]
    pub const fn payload(&self) -> &'a [u8] {
        self.payload
    }

    #[inline]
    pub const fn archived(&self) -> &'a T::Archived {
        self.root
    }

    #[inline]
    pub const fn version(&self) -> Version {
        self.header.version()
    }

    /// Resolves an `ArchivedString` field directly to `&'a str` with zero allocation.
    #[inline]
    pub fn get_str(&self, field: &ArchivedString) -> &'a str {
        field.as_str(self.payload)
    }

    /// Resolves an `ArchivedBytes` field directly to `&'a [u8]` with zero allocation.
    #[inline]
    pub fn get_bytes(&self, field: &ArchivedBytes) -> &'a [u8] {
        field.as_slice(self.payload)
    }

    /// Resolves an `ArchivedSlice<A>` field directly to `&'a [A]` with zero allocation.
    #[inline]
    pub fn get_slice<A: Copy>(&self, field: &ArchivedSlice<A>) -> &'a [A] {
        field.as_slice(self.payload)
    }

    /// Materializes an owned copy of `T` (only needed when mutating or exporting).
    #[inline]
    pub fn to_owned_record(&self) -> Result<T> {
        T::unarchive(self.root, self.payload)
    }
}

impl<'a, T: ZeroCopyRecord> core::ops::Deref for RecordView<'a, T> {
    type Target = T::Archived;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.root
    }
}

impl<'a, T: ZeroCopyRecord> fmt::Debug for RecordView<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecordView")
            .field("version", &self.header.version())
            .field("payload_len", &self.header.payload_len)
            .field("root", &self.root)
            .finish()
    }
}

/// High-speed encoder and decoder for `[RecordHeader (64B) | Payload]`.
#[derive(Debug, Default)]
pub struct RecordCodec;

impl RecordCodec {
    /// Encodes `record` into a 64-byte-aligned `AlignedBytes` frame (`[RecordHeader | Payload]`)
    /// using the thread-local scratch buffer so intermediate serialization performs zero heap allocations.
    #[inline]
    pub fn encode_aligned<T: ZeroCopyRecord>(
        record: &T,
        version: Version,
        updated_at_ns: u64,
        flags: RecordFlags,
        ttl_expires_at_ns: u64,
    ) -> AlignedBytes {
        with_thread_scratch(|scratch| {
            Self::encode_into_scratch(
                scratch,
                record,
                version,
                updated_at_ns,
                flags,
                ttl_expires_at_ns,
            );
            scratch.freeze_copy()
        })
    }

    /// Encodes `record` directly into `scratch` starting at offset 0.
    #[inline]
    pub fn encode_into_scratch<T: ZeroCopyRecord>(
        scratch: &mut AlignedScratchBuf,
        record: &T,
        version: Version,
        updated_at_ns: u64,
        flags: RecordFlags,
        ttl_expires_at_ns: u64,
    ) -> RecordHeader {
        scratch.clear();
        // Reserve 64-byte header slot at 0..64
        let header_off = scratch.allocate_zeroed(HEADER_SIZE, HEADER_SIZE);
        debug_assert_eq!(header_off, 0);

        let mut builder = ArchiveBuilder::new(scratch, HEADER_SIZE);
        let root_struct = record.archive(&mut builder);
        let root_offset = builder.write_root(&root_struct);
        scratch.align_to(8);

        let payload_slice = &scratch.as_slice()[HEADER_SIZE..];
        let uncompressed_len = payload_slice.len() as u32;

        if flags.contains(RecordFlags::COMPRESSED_LZ4) {
            let compressed = compress_lz4(payload_slice);
            let payload_checksum = checksum_payload(&compressed);
            let payload_len = compressed.len() as u32;
            scratch.clear();
            scratch.allocate_zeroed(HEADER_SIZE, HEADER_SIZE);
            scratch.extend_from_slice(&compressed);

            let hdr = RecordHeader::new(
                T::SCHEMA_VERSION,
                version,
                updated_at_ns,
                payload_checksum,
                payload_len,
                uncompressed_len,
                root_offset,
                flags,
                T::COLLECTION_ID,
                ttl_expires_at_ns,
            );
            scratch.as_mut_slice()[..HEADER_SIZE].copy_from_slice(&hdr.to_bytes());
            hdr
        } else {
            let payload_checksum = checksum_payload(payload_slice);
            let payload_len = uncompressed_len;
            let hdr = RecordHeader::new(
                T::SCHEMA_VERSION,
                version,
                updated_at_ns,
                payload_checksum,
                payload_len,
                uncompressed_len,
                root_offset,
                flags,
                T::COLLECTION_ID,
                ttl_expires_at_ns,
            );
            scratch.as_mut_slice()[..HEADER_SIZE].copy_from_slice(&hdr.to_bytes());
            hdr
        }
    }

    /// Performs full header, checksum, alignment, and `T::validate_archived` bytecheck verification
    /// on a raw frame (`RULE-SAFE-04`).
    ///
    /// Called once when a `.rec` or `.seg` region is first mapped from disk.
    pub fn validate_frame<T: ZeroCopyRecord>(frame: &[u8]) -> Result<RecordHeader> {
        if ((frame.as_ptr() as usize) & (HEADER_SIZE - 1)) != 0 {
            return Err(StorageError::corrupted_header(
                "record frame buffer is not 64-byte aligned",
            ));
        }
        let hdr = RecordHeader::from_slice_verified(frame)?;
        if hdr.is_tombstone() {
            return Ok(hdr);
        }
        if hdr.collection_id != T::COLLECTION_ID {
            return Err(StorageError::invalid_archive(
                "record collection_id does not match requested type T::COLLECTION_ID",
            ));
        }
        if hdr.schema_version != T::SCHEMA_VERSION {
            return Err(StorageError::SchemaVersionMismatch {
                found: hdr.schema_version,
                expected: T::SCHEMA_VERSION,
            });
        }
        let total_needed = HEADER_SIZE
            .checked_add(hdr.payload_len as usize)
            .ok_or_else(|| StorageError::corrupted_header("frame length overflow"))?;
        let payload = frame
            .get(HEADER_SIZE..total_needed)
            .ok_or_else(|| StorageError::corrupted_header("frame truncated before payload_len"))?;

        let actual_checksum = checksum_payload(payload);
        if actual_checksum != hdr.payload_checksum {
            return Err(StorageError::checksum_mismatch(
                hdr.payload_checksum,
                actual_checksum,
            ));
        }

        if hdr.flags_typed().contains(RecordFlags::COMPRESSED_LZ4) {
            let decompressed = decompress_lz4(payload, hdr.uncompressed_len as usize)?;
            let aligned = AlignedBytes::copy_from_slice(&decompressed);
            Self::validate_payload_layout::<T>(aligned.as_slice(), hdr.root_offset)?;
        } else {
            Self::validate_payload_layout::<T>(payload, hdr.root_offset)?;
        }
        Ok(hdr)
    }

    #[inline]
    fn validate_payload_layout<T: ZeroCopyRecord>(
        payload: &[u8],
        root_offset: u32,
    ) -> Result<()> {
        let root_off = root_offset as usize;
        let root_size = core::mem::size_of::<T::Archived>();
        let root_align = core::mem::align_of::<T::Archived>();

        if (root_off & (root_align - 1)) != 0 {
            return Err(StorageError::invalid_archive(
                "root_offset is not aligned for T::Archived",
            ));
        }
        let root_end = root_off
            .checked_add(root_size)
            .ok_or_else(|| StorageError::invalid_archive("root_offset + size overflow"))?;
        if root_end > payload.len() {
            return Err(StorageError::invalid_archive(
                "T::Archived extends past payload end",
            ));
        }
        let root_addr = (payload.as_ptr() as usize).wrapping_add(root_off);
        if (root_addr & (root_align - 1)) != 0 {
            return Err(StorageError::invalid_archive(
                "runtime address of T::Archived is unaligned",
            ));
        }
        // SAFETY: Bounds and alignment of `root_off..root_end` within `payload` were just verified.
        let root_ref = unsafe { &*(payload.as_ptr().add(root_off) as *const T::Archived) };
        T::validate_archived(root_ref, payload)
    }

    /// Validates `frame` and returns a zero-copy `RecordView<'a, T>`.
    #[inline]
    pub fn decode_verified<T: ZeroCopyRecord>(frame: &[u8]) -> Result<RecordView<'_, T>> {
        let hdr = Self::validate_frame::<T>(frame)?;
        if hdr.is_tombstone() {
            return Err(StorageError::corrupted_header(
                "cannot decode tombstone frame as active record",
            ));
        }
        if hdr.flags_typed().contains(RecordFlags::COMPRESSED_LZ4) {
            return Err(StorageError::invalid_archive(
                "compressed cold record must be decompressed via decode_owned_or_decompress",
            ));
        }
        // SAFETY: `validate_frame` verified 64-byte alignment, header bounds, payload bounds,
        // checksum, and `T::Archived` alignment/validity.
        Ok(unsafe { Self::decode_unchecked(frame) })
    }

    /// Decompresses or decodes any frame (including cold LZ4-compressed frames) into an owned `T`.
    pub fn decode_owned<T: ZeroCopyRecord>(frame: &[u8]) -> Result<(RecordHeader, T)> {
        let hdr = Self::validate_frame::<T>(frame)?;
        let payload = &frame[HEADER_SIZE..HEADER_SIZE + (hdr.payload_len as usize)];
        if hdr.flags_typed().contains(RecordFlags::COMPRESSED_LZ4) {
            let decompressed = decompress_lz4(payload, hdr.uncompressed_len as usize)?;
            let aligned = AlignedBytes::copy_from_slice(&decompressed);
            let slice = aligned.as_slice();
            // SAFETY: Validated in `validate_frame`.
            let root = unsafe { &*(slice.as_ptr().add(hdr.root_offset as usize) as *const T::Archived) };
            let owned = T::unarchive(root, slice)?;
            Ok((hdr, owned))
        } else {
            let view = unsafe { Self::decode_unchecked::<T>(frame) };
            let owned = view.to_owned_record()?;
            Ok((hdr, owned))
        }
    }

    /// 1-cycle zero-copy hot read path for an already-verified 64-byte-aligned frame (`RULE-SAFE-04`).
    ///
    /// # Safety
    /// Caller must guarantee that `frame` is 64-byte-aligned and has previously passed
    /// `RecordCodec::validate_frame::<T>` (or was freshly encoded in `HotDirtyArc` by `encode_aligned`).
    #[inline]
    pub unsafe fn decode_unchecked<T: ZeroCopyRecord>(frame: &[u8]) -> RecordView<'_, T> {
        // SAFETY: Caller guarantees `frame` passed `validate_frame::<T>`.
        unsafe {
            let header = RecordHeader::from_aligned_unchecked(frame);
            let payload_end = HEADER_SIZE + (header.payload_len as usize);
            let payload = frame.get_unchecked(HEADER_SIZE..payload_end);
            let root = &*(payload.as_ptr().add(header.root_offset as usize) as *const T::Archived);
            RecordView {
                header,
                payload,
                root,
            }
        }
    }
}

// Built-in implementation of ZeroCopyRecord for InlineId so simple key-only records work out of the box
impl ZeroCopyRecord for InlineId {
    type Archived = InlineId;

    #[inline]
    fn archive(&self, _builder: &mut ArchiveBuilder<'_>) -> Self::Archived {
        *self
    }

    #[inline]
    fn validate_archived(root: &Self::Archived, _payload: &[u8]) -> Result<()> {
        let _ = InlineId::from_raw_bytes(root.to_raw_bytes())?;
        Ok(())
    }

    #[inline]
    fn unarchive(root: &Self::Archived, _payload: &[u8]) -> Result<Self> {
        Ok(*root)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, PartialEq)]
    struct SampleUser {
        id: InlineId,
        age: u32,
        score: u64,
        name: String,
        tags: Vec<InlineId>,
    }

    #[repr(C)]
    #[derive(Clone, Copy, Debug)]
    struct ArchivedSampleUser {
        id: InlineId,
        score: u64,
        age: u32,
        name: ArchivedString,
        tags: ArchivedSlice<InlineId>,
    }

    impl ZeroCopyRecord for SampleUser {
        type Archived = ArchivedSampleUser;

        fn archive(&self, builder: &mut ArchiveBuilder<'_>) -> Self::Archived {
            let name = builder.write_str(&self.name);
            let tags = builder.write_slice(&self.tags);
            ArchivedSampleUser {
                id: self.id,
                score: self.score,
                age: self.age,
                name,
                tags,
            }
        }

        fn validate_archived(root: &Self::Archived, payload: &[u8]) -> Result<()> {
            InlineId::from_raw_bytes(root.id.to_raw_bytes())?;
            root.name.validate(payload)?;
            root.tags.validate(payload)?;
            for tag in root.tags.as_slice(payload) {
                InlineId::from_raw_bytes(tag.to_raw_bytes())?;
            }
            Ok(())
        }

        fn unarchive(root: &Self::Archived, payload: &[u8]) -> Result<Self> {
            Ok(Self {
                id: root.id,
                age: root.age,
                score: root.score,
                name: root.name.as_str(payload).to_owned(),
                tags: root.tags.as_slice(payload).to_vec(),
            })
        }
    }

    #[test]
    fn test_zero_copy_codec_roundtrip_and_unchecked_read() {
        let user = SampleUser {
            id: InlineId::new("user_123").unwrap(),
            age: 29,
            score: 99_500,
            name: "Sample User".to_string(),
            tags: vec![
                InlineId::new("rust").unwrap(),
                InlineId::new("systems").unwrap(),
            ],
        };

        let frame = RecordCodec::encode_aligned(&user, Version(7), 123_456_789, RecordFlags::NONE, 0);
        let view = RecordCodec::decode_verified::<SampleUser>(&frame).unwrap();

        assert_eq!(view.version(), Version(7));
        assert_eq!(view.age, 29);
        assert_eq!(view.score, 99_500);
        assert_eq!(view.get_str(&view.name), "Sample User");
        assert_eq!(view.get_slice(&view.tags).len(), 2);
        assert_eq!(view.get_slice(&view.tags)[0].as_str(), "rust");
        assert_eq!(view.to_owned_record().unwrap(), user);
    }
}
