//! Zero-copy relative-offset archive system (`TASK-006`, `F-CORE-01`, `F-DURA-05`, `RULE-SAFE-04`).
//!
//! Provides `ZeroCopyRecord`, `ArchivedString`, `ArchivedBytes`, and `ArchivedSlice<T>` so
//! on-disk `.rec` / `.seg` memory-mapped bytes can be dereferenced directly as `&T::Archived`
//! with **zero heap allocations** and **zero copies**.
//!
//! Soundness model: every type that is reinterpreted from raw bytes must implement the
//! `unsafe` marker trait [`ArchivePod`] (any bit pattern valid, no padding, no pointers).
//! All safe accessors (`as_str`, `as_slice`, ...) are bounds-checked, so they never cause
//! Undefined Behavior even on unvalidated or corrupted payloads; validation (`validate`) is
//! what guarantees that they return the *intended* data.

use core::fmt;
use core::marker::PhantomData;
use storage_types::{
    InlineId, RecordFlags, RecordHeader, Result, StorageError, Version, HEADER_SIZE,
};

use crate::checksum::checksum_payload;
use crate::compress::{compress_lz4, decompress_lz4};
use crate::scratch::{with_thread_scratch, AlignedBytes, AlignedScratchBuf};

/// Marker for plain-old-data types that may be reinterpreted directly from archived bytes.
///
/// # Safety
/// Implementors must guarantee that:
/// - every possible bit pattern of `size_of::<Self>()` bytes is a valid value of `Self`
///   (so no `bool`, `char`, enums, references, or `NonNull`/`NonZero*` fields);
/// - the type has no padding bytes (they would be read as uninitialized memory when archiving);
/// - the type has a stable layout (`#[repr(C)]` / `#[repr(transparent)]` or a primitive);
/// - the type contains no pointers or interior mutability.
pub unsafe trait ArchivePod: Copy + Send + Sync + 'static {
    /// Returns the all-zero value (valid for every `ArchivePod` type by its contract).
    #[inline]
    fn zeroed() -> Self {
        // SAFETY: `ArchivePod` guarantees every bit pattern, including all zeros, is valid.
        unsafe { core::mem::zeroed() }
    }
}

/// Safely implements [`ArchivePod`] for a plain struct by proving its contract at compile time,
/// so record crates can stay `#![forbid(unsafe_code)]`:
///
/// - every listed field has exactly the listed type (type-checked against the real field),
/// - every field type is itself `ArchivePod`,
/// - fields are laid out back-to-back in the listed order starting at offset 0 and the struct
///   size equals the sum of the field sizes (so there is no padding anywhere).
///
/// All fields must be listed, in declaration order:
/// `impl_archive_pod!(ArchivedUser { id: InlineId, score: i64, name: ArchivedString });`
#[macro_export]
macro_rules! impl_archive_pod {
    ($ty:ty { $($field:ident : $fty:ty),+ $(,)? }) => {
        const _: () = {
            let mut expected = 0usize;
            $(
                assert!(
                    ::core::mem::offset_of!($ty, $field) == expected,
                    "impl_archive_pod!: padding or reordered field before this field"
                );
                expected += ::core::mem::size_of::<$fty>();
            )+
            assert!(
                ::core::mem::size_of::<$ty>() == expected,
                "impl_archive_pod!: trailing padding or unlisted fields"
            );
        };
        const _: fn(&$ty) = |v: &$ty| {
            fn assert_pod<P: $crate::ArchivePod>(_: &P) {}
            $( assert_pod::<$fty>(&v.$field); )+
        };
        // SAFETY: the compile-time checks above prove the struct consists solely of the listed
        // `ArchivePod` fields packed without padding, so every bit pattern is a valid value.
        unsafe impl $crate::ArchivePod for $ty {}
    };
}

macro_rules! impl_archive_pod_primitive {
    ($($t:ty),* $(,)?) => {
        $(
            // SAFETY: primitive integer / float types accept every bit pattern and have no padding.
            unsafe impl ArchivePod for $t {}
        )*
    };
}
impl_archive_pod_primitive!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128, f32, f64);

// SAFETY: an array of pod elements has no padding between elements and accepts any bit pattern.
unsafe impl<A: ArchivePod, const N: usize> ArchivePod for [A; N] {}

// SAFETY: `InlineId` is `#[repr(C, align(8))]` with a `u8` + `[u8; 23]` (24 bytes, no padding);
// every accessor tolerates arbitrary bytes (lengths are clamped and UTF-8 is re-checked).
unsafe impl ArchivePod for InlineId {}

/// Relative-offset UTF-8 string stored inside an archived payload (`8 bytes`, `#[repr(C)]`).
/// Resolves to `&str` directly from the `mmap` page with zero heap allocation (bounds plus an
/// ASCII / UTF-8 check per call, so hostile bytes can never produce an invalid `&str`).
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

    /// Zero-copy string access (`0` allocations).
    ///
    /// Bounds and UTF-8 are checked on every call, so this is safe on any payload; it returns
    /// `""` when the reference is out of bounds or not valid UTF-8 (both rejected by `validate`).
    #[inline]
    pub fn as_str<'a>(&self, payload: &'a [u8]) -> &'a str {
        if self.len == 0 {
            return "";
        }
        let start = self.offset as usize;
        let Some(end) = start.checked_add(self.len as usize) else {
            return "";
        };
        let Some(bytes) = payload.get(start..end) else {
            return "";
        };
        if is_ascii_fast(bytes) {
            // SAFETY: ASCII bytes (all `< 0x80`, checked word-at-a-time above) are valid UTF-8.
            return unsafe { core::str::from_utf8_unchecked(bytes) };
        }
        core::str::from_utf8(bytes).unwrap_or("")
    }

    /// Zero-copy equality check against `expected` (`TASK-057`).
    #[inline]
    pub fn eq_str(&self, payload: &[u8], expected: &str) -> bool {
        self.as_str(payload) == expected
    }

    /// Zero-copy prefix check (`STARTS WITH`, `TASK-057`).
    #[inline]
    pub fn starts_with(&self, payload: &[u8], prefix: &str) -> bool {
        self.as_str(payload).starts_with(prefix)
    }

    /// Zero-copy suffix check (`ENDS WITH`, `TASK-057`).
    #[inline]
    pub fn ends_with(&self, payload: &[u8], suffix: &str) -> bool {
        self.as_str(payload).ends_with(suffix)
    }

    /// Zero-copy substring check (`CONTAINS`, `TASK-057`).
    #[inline]
    pub fn contains(&self, payload: &[u8], needle: &str) -> bool {
        self.as_str(payload).contains(needle)
    }

    /// Zero-copy ASCII case-insensitive equality check (`TASK-057`).
    #[inline]
    pub fn eq_ignore_ascii_case(&self, payload: &[u8], expected: &str) -> bool {
        self.as_str(payload).eq_ignore_ascii_case(expected)
    }

    /// Zero-copy ASCII case-insensitive substring check (`ILIKE '%needle%'`, `0` heap allocations, `TASK-057`).
    #[inline]
    pub fn contains_ignore_ascii_case(&self, payload: &[u8], needle: &str) -> bool {
        let n_bytes = needle.as_bytes();
        if n_bytes.is_empty() {
            return true;
        }
        let hay = self.as_str(payload).as_bytes();
        if n_bytes.len() > hay.len() {
            return false;
        }
        hay.windows(n_bytes.len())
            .any(|w| w.eq_ignore_ascii_case(n_bytes))
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

    /// Zero-copy byte access; returns `&[]` when the reference is out of bounds.
    #[inline]
    pub fn as_slice<'a>(&self, payload: &'a [u8]) -> &'a [u8] {
        if self.len == 0 {
            return &[];
        }
        let start = self.offset as usize;
        let Some(end) = start.checked_add(self.len as usize) else {
            return &[];
        };
        payload.get(start..end).unwrap_or(&[])
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

// SAFETY: `ArchivedString` is two `u32`s (8 bytes, no padding); every bit pattern is valid.
unsafe impl ArchivePod for ArchivedString {}
// SAFETY: `ArchivedBytes` is two `u32`s (8 bytes, no padding); every bit pattern is valid.
unsafe impl ArchivePod for ArchivedBytes {}
// SAFETY: `ArchivedSlice` is two `u32`s plus a zero-sized marker (8 bytes, no padding).
unsafe impl<A: ArchivePod> ArchivePod for ArchivedSlice<A> {}

impl<A: ArchivePod> ArchivedSlice<A> {
    pub const EMPTY: Self = Self {
        offset: 0,
        len: 0,
        _marker: PhantomData,
    };

    /// A slice reference from raw parts (validated by [`Self::validate`] / [`Self::as_slice`]).
    #[inline]
    pub const fn from_parts(offset: u32, len: u32) -> Self {
        Self {
            offset,
            len,
            _marker: PhantomData,
        }
    }

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

    /// Returns a zero-copy slice `&'a [A]` pointing directly into `payload`.
    ///
    /// Bounds and alignment are checked on every call (a few integer ops), returning `&[]`
    /// when they do not hold, so this is safe even on unvalidated payloads.
    #[inline]
    pub fn as_slice<'a>(&self, payload: &'a [u8]) -> &'a [A] {
        if self.len == 0 {
            return &[];
        }
        let start = self.offset as usize;
        let Some(byte_len) = (self.len as usize).checked_mul(core::mem::size_of::<A>()) else {
            return &[];
        };
        let Some(end) = start.checked_add(byte_len) else {
            return &[];
        };
        let Some(bytes) = payload.get(start..end) else {
            return &[];
        };
        if (bytes.as_ptr() as usize) & (core::mem::align_of::<A>() - 1) != 0 {
            return &[];
        }
        // SAFETY: `bytes` covers exactly `len * size_of::<A>()` in-bounds bytes, its start is
        // aligned for `A` (checked above), and `A: ArchivePod` accepts every bit pattern.
        unsafe { core::slice::from_raw_parts(bytes.as_ptr().cast::<A>(), self.len as usize) }
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
    pub fn write_slice<A: ArchivePod>(&mut self, items: &[A]) -> ArchivedSlice<A> {
        if items.is_empty() {
            return ArchivedSlice::EMPTY;
        }
        let align = core::mem::align_of::<A>();
        self.buf.align_to(align);
        let byte_len = core::mem::size_of_val(items);
        // SAFETY: `A: ArchivePod` has no padding, so all `byte_len` bytes are initialized.
        let raw_bytes =
            unsafe { core::slice::from_raw_parts(items.as_ptr().cast::<u8>(), byte_len) };
        let abs_off = self.buf.extend_from_slice(raw_bytes);
        let rel_off = (abs_off - self.payload_base) as u32;
        ArchivedSlice {
            offset: rel_off,
            len: items.len() as u32,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn write_root<R: ArchivePod>(&mut self, root: &R) -> u32 {
        let align = core::mem::align_of::<R>().max(8);
        self.buf.align_to(align);
        let size = core::mem::size_of::<R>();
        // SAFETY: `R: ArchivePod` has no padding, so all `size` bytes are initialized.
        let raw_bytes = unsafe { core::slice::from_raw_parts((root as *const R).cast::<u8>(), size) };
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
    ///
    /// Must implement [`ArchivePod`] (stable layout, no padding, every bit pattern valid).
    type Archived: ArchivePod + fmt::Debug;

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

    /// Zero-copy exact string comparison on an `ArchivedString` field (`TASK-057`).
    #[inline]
    pub fn str_eq(&self, field: &ArchivedString, expected: &str) -> bool {
        field.eq_str(self.payload, expected)
    }

    /// Zero-copy prefix check (`STARTS WITH`) on an `ArchivedString` field (`TASK-057`).
    #[inline]
    pub fn str_starts_with(&self, field: &ArchivedString, prefix: &str) -> bool {
        field.starts_with(self.payload, prefix)
    }

    /// Zero-copy suffix check (`ENDS WITH`) on an `ArchivedString` field (`TASK-057`).
    #[inline]
    pub fn str_ends_with(&self, field: &ArchivedString, suffix: &str) -> bool {
        field.ends_with(self.payload, suffix)
    }

    /// Zero-copy substring check (`CONTAINS`) on an `ArchivedString` field (`TASK-057`).
    #[inline]
    pub fn str_contains(&self, field: &ArchivedString, needle: &str) -> bool {
        field.contains(self.payload, needle)
    }

    /// Zero-copy ASCII case-insensitive substring check (`ILIKE '%needle%'`) on an `ArchivedString` field (`TASK-057`).
    #[inline]
    pub fn str_contains_ignore_ascii_case(&self, field: &ArchivedString, needle: &str) -> bool {
        field.contains_ignore_ascii_case(self.payload, needle)
    }

    /// Resolves an `ArchivedBytes` field directly to `&'a [u8]` with zero allocation.
    #[inline]
    pub fn get_bytes(&self, field: &ArchivedBytes) -> &'a [u8] {
        field.as_slice(self.payload)
    }

    /// Resolves an `ArchivedSlice<A>` field directly to `&'a [A]` with zero allocation.
    #[inline]
    pub fn get_slice<A: ArchivePod>(&self, field: &ArchivedSlice<A>) -> &'a [A] {
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
        Self::validate_frame_inner::<T>(frame).map(|(hdr, _)| hdr)
    }

    /// [`Self::validate_frame`] that also returns the 64-byte-aligned decompressed payload of a
    /// compressed frame, so callers decode the exact buffer that was validated (and decompress once).
    fn validate_frame_inner<T: ZeroCopyRecord>(
        frame: &[u8],
    ) -> Result<(RecordHeader, Option<AlignedBytes>)> {
        if ((frame.as_ptr() as usize) & (HEADER_SIZE - 1)) != 0 {
            return Err(StorageError::corrupted_header(
                "record frame buffer is not 64-byte aligned",
            ));
        }
        let hdr = RecordHeader::from_slice_verified(frame)?;
        if hdr.flags_typed().contains(RecordFlags::ENCRYPTED) {
            return Err(StorageError::invalid_archive(
                "encrypted frame must be decrypted (RecordCodec::decrypt_frame) before decoding",
            ));
        }
        if hdr.is_tombstone() {
            return Ok((hdr, None));
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
            let aligned = AlignedBytes::try_copy_from_slice(&decompressed)?;
            Self::validate_payload_layout::<T>(aligned.as_slice(), hdr.root_offset)?;
            Ok((hdr, Some(aligned)))
        } else {
            Self::validate_payload_layout::<T>(payload, hdr.root_offset)?;
            Ok((hdr, None))
        }
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
        // SAFETY: Bounds and alignment of `root_off..root_end` within `payload` were just verified,
        // and `T::Archived: ArchivePod` accepts any bit pattern.
        let root_ref = unsafe { &*payload.as_ptr().add(root_off).cast::<T::Archived>() };
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
        let (hdr, decompressed) = Self::validate_frame_inner::<T>(frame)?;
        if hdr.is_tombstone() {
            return Err(StorageError::corrupted_header(
                "cannot decode tombstone frame as active record",
            ));
        }
        let owned = match decompressed {
            Some(aligned) => {
                let slice = aligned.as_slice();
                let root = Self::checked_root::<T>(slice, hdr.root_offset)?;
                T::unarchive(root, slice)?
            }
            None => Self::decode_fast::<T>(frame)?.to_owned_record()?,
        };
        Ok((hdr, owned))
    }

    /// Bounds- and alignment-checked `&T::Archived` at `root_offset` inside `payload`.
    #[inline]
    fn checked_root<T: ZeroCopyRecord>(payload: &[u8], root_offset: u32) -> Result<&T::Archived> {
        let root_off = root_offset as usize;
        let root_bytes = root_off
            .checked_add(core::mem::size_of::<T::Archived>())
            .and_then(|end| payload.get(root_off..end))
            .ok_or_else(|| StorageError::invalid_archive("T::Archived extends past payload end"))?;
        if (root_bytes.as_ptr() as usize) & (core::mem::align_of::<T::Archived>() - 1) != 0 {
            return Err(StorageError::invalid_archive(
                "runtime address of T::Archived is unaligned",
            ));
        }
        // SAFETY: `root_bytes` covers exactly `size_of::<T::Archived>()` in-bounds bytes at an
        // address aligned for `T::Archived`, and `T::Archived: ArchivePod` accepts any bit pattern.
        Ok(unsafe { &*root_bytes.as_ptr().cast::<T::Archived>() })
    }

    /// Returns a reference to the 64-byte header at the start of `frame`.
    ///
    /// Checks length and 64-byte alignment only (O(1)); the header checksum is NOT verified.
    #[inline]
    pub fn header_ref(frame: &[u8]) -> Result<&RecordHeader> {
        if frame.len() < HEADER_SIZE || (frame.as_ptr() as usize) & (HEADER_SIZE - 1) != 0 {
            return Err(StorageError::corrupted_header(
                "record frame shorter than 64 bytes or not 64-byte aligned",
            ));
        }
        // SAFETY: `frame` holds at least 64 bytes at a 64-byte-aligned address, and
        // `RecordHeader` is `#[repr(C, align(64))]` over integers only (64 bytes, no padding),
        // so every bit pattern is a valid value.
        Ok(unsafe { &*frame.as_ptr().cast::<RecordHeader>() })
    }

    /// Safe O(1) zero-copy decode for frames that were validated earlier (`RULE-SAFE-04`).
    ///
    /// Skips the payload checksum and `T::validate_archived` (done once by `validate_frame`),
    /// but checks everything memory safety depends on: header alignment and length, payload
    /// bounds, root bounds and alignment, collection/schema identity, and that the frame is not
    /// compressed or a tombstone. Because `T::Archived: ArchivePod` and every archive accessor is
    /// bounds-checked, a view over unexpected bytes can return wrong data but never cause UB.
    #[inline]
    pub fn decode_fast<T: ZeroCopyRecord>(frame: &[u8]) -> Result<RecordView<'_, T>> {
        let header = Self::header_ref(frame)?;
        // One mask test: tombstones, compressed and encrypted frames are not zero-copy readable.
        if header.flags
            & (RecordFlags::TOMBSTONE.0 | RecordFlags::COMPRESSED_LZ4.0 | RecordFlags::ENCRYPTED.0)
            != 0
        {
            return Err(StorageError::invalid_archive(
                "compressed, encrypted or tombstone record must be read via decode_owned",
            ));
        }
        if header.collection_id != T::COLLECTION_ID || header.schema_version != T::SCHEMA_VERSION {
            return Err(StorageError::SchemaVersionMismatch {
                found: header.schema_version,
                expected: T::SCHEMA_VERSION,
            });
        }
        let payload = HEADER_SIZE
            .checked_add(header.payload_len as usize)
            .and_then(|end| frame.get(HEADER_SIZE..end))
            .ok_or_else(|| StorageError::corrupted_header("frame truncated before payload_len"))?;
        let root_off = header.root_offset as usize;
        let root_bytes = root_off
            .checked_add(core::mem::size_of::<T::Archived>())
            .and_then(|end| payload.get(root_off..end))
            .ok_or_else(|| StorageError::invalid_archive("T::Archived extends past payload end"))?;
        if (root_bytes.as_ptr() as usize) & (core::mem::align_of::<T::Archived>() - 1) != 0 {
            return Err(StorageError::invalid_archive(
                "runtime address of T::Archived is unaligned",
            ));
        }
        // SAFETY: `root_bytes` covers exactly `size_of::<T::Archived>()` in-bounds bytes at an
        // address aligned for `T::Archived`, and `T::Archived: ArchivePod` accepts any bit pattern.
        let root = unsafe { &*root_bytes.as_ptr().cast::<T::Archived>() };
        Ok(RecordView {
            header,
            payload,
            root,
        })
    }

    /// Encrypts `frame` for record `id` at rest (`F-SEC-02`) with key `key_id`: the payload becomes
    /// `[key_id u32 | 12 B nonce | ChaCha20 ciphertext | 16 B Poly1305 tag]`, the header gains
    /// `RecordFlags::ENCRYPTED`, and the authenticated data binds the ciphertext to `id`, `key_id`
    /// and every header field that is not recomputed on decryption (schema, version, timestamps,
    /// TTL, flags, root offset, lengths), so frames cannot be swapped between records or have their
    /// metadata edited without detection.
    pub fn encrypt_frame(
        key: &crate::crypto::EncryptionKey,
        key_id: u32,
        id: InlineId,
        frame: &[u8],
    ) -> Result<AlignedBytes> {
        let hdr = RecordHeader::from_slice_verified(frame)?;
        if hdr.flags_typed().contains(RecordFlags::ENCRYPTED) {
            return Err(StorageError::invalid_archive("frame is already encrypted"));
        }
        let payload = HEADER_SIZE
            .checked_add(hdr.payload_len as usize)
            .and_then(|end| frame.get(HEADER_SIZE..end))
            .ok_or_else(|| StorageError::corrupted_header("frame truncated before payload_len"))?;
        let aad = sealed_frame_aad(&hdr, id, key_id);
        let nonce = crate::crypto::generate_unique_nonce();
        let body_len = SEALED_OVERHEAD + payload.len();
        let body_len_u32 = u32::try_from(body_len)
            .map_err(|_| StorageError::invalid_archive("encrypted payload exceeds u32"))?;
        // Built in place in the final buffer: `[header | key_id | nonce | ciphertext | tag]`.
        AlignedBytes::try_build(HEADER_SIZE + body_len, |out| {
            let (head, body) = split_mut(out, HEADER_SIZE)?;
            {
                let (kid, rest) = split_mut(body, 4)?;
                kid.copy_from_slice(&key_id.to_le_bytes());
                let (nonce_out, rest) = split_mut(rest, crate::crypto::AEAD_NONCE_LEN)?;
                nonce_out.copy_from_slice(&nonce);
                let (ct, tag_out) = split_mut(rest, payload.len())?;
                ct.copy_from_slice(payload);
                tag_out.copy_from_slice(&crate::crypto::seal_in_place(key.bytes(), &nonce, &aad, ct));
            }
            let enc_hdr = RecordHeader::new(
                hdr.schema_version,
                Version(hdr.record_version),
                hdr.updated_at_ns,
                checksum_payload(body),
                body_len_u32,
                hdr.uncompressed_len,
                hdr.root_offset,
                hdr.flags_typed().with(RecordFlags::ENCRYPTED),
                hdr.collection_id,
                hdr.ttl_expires_at_ns,
            );
            head.copy_from_slice(&enc_hdr.to_bytes());
            Ok(())
        })
    }

    /// Authenticates `frame` without encrypting it (`F-SEC-02`, authenticated-only protection):
    /// the payload stays plaintext and in place, the header gains `RecordFlags::AUTHENTICATED`,
    /// and a 32-byte trailer `[key_id u32 | 12 B nonce | 16 B Poly1305 tag]` follows the payload.
    /// The tag covers the record id, the key id, every header field and the whole payload.
    pub fn authenticate_frame(
        key: &crate::crypto::EncryptionKey,
        key_id: u32,
        id: InlineId,
        frame: &[u8],
    ) -> Result<AlignedBytes> {
        let hdr = RecordHeader::from_slice_verified(frame)?;
        let flags = hdr.flags_typed();
        if flags.contains(RecordFlags::ENCRYPTED) || flags.contains(RecordFlags::AUTHENTICATED) {
            return Err(StorageError::invalid_archive("frame is already protected"));
        }
        let payload = HEADER_SIZE
            .checked_add(hdr.payload_len as usize)
            .and_then(|end| frame.get(HEADER_SIZE..end))
            .ok_or_else(|| StorageError::corrupted_header("frame truncated before payload_len"))?;
        let auth_hdr = RecordHeader::new(
            hdr.schema_version,
            Version(hdr.record_version),
            hdr.updated_at_ns,
            hdr.payload_checksum,
            hdr.payload_len,
            hdr.uncompressed_len,
            hdr.root_offset,
            flags.with(RecordFlags::AUTHENTICATED),
            hdr.collection_id,
            hdr.ttl_expires_at_ns,
        );
        let nonce = crate::crypto::generate_unique_nonce();
        let aad = sealed_frame_aad(&auth_hdr, id, key_id);
        let tag = crate::crypto::aead_tag(key.bytes(), &nonce, &aad, payload);
        AlignedBytes::try_build(HEADER_SIZE + payload.len() + AUTH_TRAILER_LEN, |out| {
            let (head, rest) = split_mut(out, HEADER_SIZE)?;
            head.copy_from_slice(&auth_hdr.to_bytes());
            let (body, trailer) = split_mut(rest, payload.len())?;
            body.copy_from_slice(payload);
            let (kid, rest) = split_mut(trailer, 4)?;
            kid.copy_from_slice(&key_id.to_le_bytes());
            let (nonce_out, tag_out) = split_mut(rest, crate::crypto::AEAD_NONCE_LEN)?;
            nonce_out.copy_from_slice(&nonce);
            tag_out.copy_from_slice(&tag);
            Ok(())
        })
    }

    /// Verifies a frame produced by [`Self::authenticate_frame`] for record `id` (constant-time
    /// tag comparison) and returns the plain frame (flag and trailer removed).
    pub fn strip_authentication(
        keys: &crate::crypto::KeyRing,
        id: InlineId,
        frame: &[u8],
    ) -> Result<AlignedBytes> {
        let hdr = RecordHeader::from_slice_verified(frame)?;
        if !hdr.flags_typed().contains(RecordFlags::AUTHENTICATED) {
            return Err(StorageError::invalid_archive("frame is not authenticated"));
        }
        let payload_end = HEADER_SIZE
            .checked_add(hdr.payload_len as usize)
            .ok_or_else(|| StorageError::corrupted_header("frame length overflow"))?;
        let payload = frame
            .get(HEADER_SIZE..payload_end)
            .ok_or_else(|| StorageError::corrupted_header("frame truncated before payload_len"))?;
        let trailer = frame
            .get(payload_end..payload_end + AUTH_TRAILER_LEN)
            .ok_or_else(|| StorageError::corrupted_header("authenticated frame missing its MAC trailer"))?;
        let key_id = u32::from_le_bytes([trailer[0], trailer[1], trailer[2], trailer[3]]);
        let mut nonce = [0u8; 12];
        nonce.copy_from_slice(&trailer[4..16]);
        let mut tag = [0u8; 16];
        tag.copy_from_slice(&trailer[16..32]);
        let key = keys
            .get(key_id)
            .ok_or_else(|| StorageError::invalid_key("frame authenticated with a key id not in the key ring"))?;
        let aad = sealed_frame_aad(&hdr, id, key_id);
        let expected = crate::crypto::aead_tag(key.bytes(), &nonce, &aad, payload);
        if !crate::crypto::verify_mac_tag_ct(&tag, &expected) {
            return Err(StorageError::corrupted_header(
                "record authentication tag mismatch (tampered frame or wrong key)",
            ));
        }
        let plain_hdr = RecordHeader::new(
            hdr.schema_version,
            Version(hdr.record_version),
            hdr.updated_at_ns,
            hdr.payload_checksum,
            hdr.payload_len,
            hdr.uncompressed_len,
            hdr.root_offset,
            hdr.flags_typed().without(RecordFlags::AUTHENTICATED),
            hdr.collection_id,
            hdr.ttl_expires_at_ns,
        );
        AlignedBytes::try_build(HEADER_SIZE + payload.len(), |out| {
            let (head, body) = split_mut(out, HEADER_SIZE)?;
            head.copy_from_slice(&plain_hdr.to_bytes());
            body.copy_from_slice(payload);
            Ok(())
        })
    }

    /// How `frame` is protected at rest (no verification).
    pub fn frame_protection(frame: &[u8]) -> FrameProtection {
        let Ok(hdr) = Self::header_ref(frame) else {
            return FrameProtection::Plain;
        };
        let flags = hdr.flags_typed();
        if flags.contains(RecordFlags::ENCRYPTED) {
            return Self::sealed_key_id(frame).map_or(FrameProtection::Plain, FrameProtection::Encrypted);
        }
        if flags.contains(RecordFlags::AUTHENTICATED) {
            let at = HEADER_SIZE + hdr.payload_len as usize;
            return frame
                .get(at..at + 4)
                .map_or(FrameProtection::Plain, |b| {
                    FrameProtection::Authenticated(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
                });
        }
        FrameProtection::Plain
    }

    /// Returns the plain frame of `frame` whatever its protection: decrypts `ENCRYPTED` frames,
    /// verifies and strips `AUTHENTICATED` ones, and returns plaintext frames unchanged (as a
    /// copy) — callers decide whether plaintext is acceptable.
    pub fn unprotect_frame(
        keys: &crate::crypto::KeyRing,
        id: InlineId,
        frame: &[u8],
    ) -> Result<AlignedBytes> {
        match Self::frame_protection(frame) {
            FrameProtection::Encrypted(_) => Self::decrypt_frame(keys, id, frame),
            FrameProtection::Authenticated(_) => Self::strip_authentication(keys, id, frame),
            FrameProtection::Plain => AlignedBytes::try_copy_from_slice(frame),
        }
    }

    /// Id of the key that sealed `frame` (`None` for a frame that is not encrypted).
    pub fn sealed_key_id(frame: &[u8]) -> Option<u32> {
        let hdr = Self::header_ref(frame).ok()?;
        if !hdr.flags_typed().contains(RecordFlags::ENCRYPTED) {
            return None;
        }
        frame
            .get(HEADER_SIZE..HEADER_SIZE + 4)
            .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// Authenticates and decrypts a frame produced by [`Self::encrypt_frame`] for record `id`,
    /// using the key it names from `keys`, and returns the original plaintext frame (validate it
    /// with [`Self::validate_frame`] before zero-copy use). Fails on an unknown key id, a wrong
    /// key, a wrong `id`, or any modified byte.
    pub fn decrypt_frame(
        keys: &crate::crypto::KeyRing,
        id: InlineId,
        frame: &[u8],
    ) -> Result<AlignedBytes> {
        let hdr = RecordHeader::from_slice_verified(frame)?;
        if !hdr.flags_typed().contains(RecordFlags::ENCRYPTED) {
            return Err(StorageError::invalid_archive("frame is not encrypted"));
        }
        let body = HEADER_SIZE
            .checked_add(hdr.payload_len as usize)
            .and_then(|end| frame.get(HEADER_SIZE..end))
            .ok_or_else(|| StorageError::corrupted_header("frame truncated before payload_len"))?;
        let (id_bytes, sealed) = body
            .split_first_chunk::<4>()
            .ok_or_else(|| StorageError::corrupted_header("encrypted payload missing key id"))?;
        let key_id = u32::from_le_bytes(*id_bytes);
        let key = keys
            .get(key_id)
            .ok_or_else(|| StorageError::invalid_key("frame sealed with a key id not in the key ring"))?;
        let plain_flags = hdr.flags_typed().without(RecordFlags::ENCRYPTED);
        let plain_hdr_for_aad = RecordHeader { flags: plain_flags.0, ..hdr };
        let aad = sealed_frame_aad(&plain_hdr_for_aad, id, key_id);
        let short = || StorageError::corrupted_header("sealed payload shorter than nonce + authentication tag");
        let (nonce, rest) = sealed.split_first_chunk::<12>().ok_or_else(short)?;
        let (ciphertext, tag) = rest.split_last_chunk::<16>().ok_or_else(short)?;
        let payload_len = u32::try_from(ciphertext.len())
            .map_err(|_| StorageError::invalid_archive("decrypted payload exceeds u32"))?;
        // Verified and decrypted in place in the final buffer (no intermediate plaintext copy).
        AlignedBytes::try_build(HEADER_SIZE + ciphertext.len(), |out| {
            let (head, body) = split_mut(out, HEADER_SIZE)?;
            body.copy_from_slice(ciphertext);
            crate::crypto::open_in_place(key.bytes(), nonce, &aad, body, tag)?;
            let plain_hdr = RecordHeader::new(
                hdr.schema_version,
                Version(hdr.record_version),
                hdr.updated_at_ns,
                checksum_payload(body),
                payload_len,
                hdr.uncompressed_len,
                hdr.root_offset,
                plain_flags,
                hdr.collection_id,
                hdr.ttl_expires_at_ns,
            );
            head.copy_from_slice(&plain_hdr.to_bytes());
            Ok(())
        })
    }

    /// `true` if `frame`'s header carries `RecordFlags::ENCRYPTED` (no other validation).
    #[inline]
    pub fn is_encrypted(frame: &[u8]) -> bool {
        Self::header_ref(frame).is_ok_and(|h| h.flags_typed().contains(RecordFlags::ENCRYPTED))
    }

    /// Like [`Self::decode_fast`], but never fails: when `frame` does not decode (only possible if
    /// previously validated bytes were modified underneath us), returns an all-zero view backed by
    /// a shared static page (no per-call storage).
    #[inline]
    pub fn decode_fast_or<T: ZeroCopyRecord>(frame: &[u8]) -> RecordView<'_, T> {
        match Self::decode_fast::<T>(frame) {
            Ok(view) => view,
            Err(_) => Self::zeroed_view::<T>(),
        }
    }

    /// All-zero view over [`ZERO_PAGE`]: header bytes `0..64`, root at payload offset 0.
    #[cold]
    fn zeroed_view<T: ZeroCopyRecord>() -> RecordView<'static, T> {
        const {
            assert!(
                core::mem::size_of::<T::Archived>() <= ZERO_PAGE_LEN - HEADER_SIZE
                    && core::mem::align_of::<T::Archived>() <= HEADER_SIZE,
                "T::Archived must fit in ZERO_PAGE (<= 4032 bytes, align <= 64)"
            );
        }
        let page: &'static [u8] = &ZERO_PAGE.0;
        let root_bytes = &page[HEADER_SIZE..HEADER_SIZE + core::mem::size_of::<T::Archived>()];
        // SAFETY: `ZERO_PAGE` is 64-byte aligned and 4096 bytes long, so bytes `0..64` are a
        // 64-aligned `RecordHeader` and `root_bytes` (offset 64, at most 4032 bytes, alignment
        // <= 64 by the const assertion) is in bounds and aligned for `T::Archived`. Both types
        // are plain-old-data for which all-zero bytes are a valid value.
        let (header, root) = unsafe {
            (
                &*page.as_ptr().cast::<RecordHeader>(),
                &*root_bytes.as_ptr().cast::<T::Archived>(),
            )
        };
        RecordView {
            header,
            payload: &[],
            root,
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
            let header = &*frame.as_ptr().cast::<RecordHeader>();
            let payload_end = HEADER_SIZE + (header.payload_len as usize);
            let payload = frame.get_unchecked(HEADER_SIZE..payload_end);
            let root = &*payload.as_ptr().add(header.root_offset as usize).cast::<T::Archived>();
            RecordView {
                header,
                payload,
                root,
            }
        }
    }
}

/// Bytes an encrypted payload adds to the plaintext: `key_id | nonce | ... | tag`.
const SEALED_OVERHEAD: usize = 4 + crate::crypto::AEAD_NONCE_LEN + crate::crypto::AEAD_TAG_LEN;

/// `split_at_mut` that reports a short buffer as corruption instead of panicking.
#[inline]
fn split_mut(buf: &mut [u8], at: usize) -> Result<(&mut [u8], &mut [u8])> {
    buf.split_at_mut_checked(at)
        .ok_or_else(|| StorageError::corrupted_header("frame buffer shorter than its layout"))
}

/// Size of the MAC trailer of an `AUTHENTICATED` frame (`key_id | nonce | tag`).
pub const AUTH_TRAILER_LEN: usize = 4 + 12 + 16;

/// How a stored frame is protected at rest.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameProtection {
    /// Neither encrypted nor authenticated.
    Plain,
    /// `RecordFlags::ENCRYPTED`, sealed with key id `.0`.
    Encrypted(u32),
    /// `RecordFlags::AUTHENTICATED` (plaintext payload, MAC trailer) with key id `.0`.
    Authenticated(u32),
}

/// Authenticated data of an encrypted frame: the record id, the sealing key id, and every
/// plaintext-frame header field that decryption does not recompute (`flags` must be the
/// plaintext flags).
fn sealed_frame_aad(hdr: &RecordHeader, id: InlineId, key_id: u32) -> [u8; 66] {
    let mut aad = [0u8; 66];
    aad[62..66].copy_from_slice(&key_id.to_le_bytes());
    aad[0..24].copy_from_slice(&id.to_raw_bytes());
    aad[24..26].copy_from_slice(&hdr.collection_id.to_le_bytes());
    aad[26..28].copy_from_slice(&hdr.schema_version.to_le_bytes());
    aad[28..30].copy_from_slice(&hdr.flags.to_le_bytes());
    aad[30..38].copy_from_slice(&hdr.record_version.to_le_bytes());
    aad[38..46].copy_from_slice(&hdr.updated_at_ns.to_le_bytes());
    aad[46..50].copy_from_slice(&hdr.uncompressed_len.to_le_bytes());
    aad[50..54].copy_from_slice(&hdr.root_offset.to_le_bytes());
    aad[54..62].copy_from_slice(&hdr.ttl_expires_at_ns.to_le_bytes());
    aad
}

/// Length of [`ZERO_PAGE`]: the largest archived root a zeroed fallback view can cover is
/// `ZERO_PAGE_LEN - HEADER_SIZE` bytes (enforced at compile time in [`RecordCodec::decode_fast_or`]).
const ZERO_PAGE_LEN: usize = 4096;

/// A shared, 64-byte-aligned all-zero page backing the infallible fallback view of
/// [`RecordCodec::decode_fast_or`] without any per-call or per-`RecordRef` storage.
#[repr(C, align(64))]
struct ZeroPage([u8; ZERO_PAGE_LEN]);

static ZERO_PAGE: ZeroPage = ZeroPage([0; ZERO_PAGE_LEN]);

/// Branch-light ASCII check for short strings (`RULE-CPU-01`): ORs 8-byte words (the last one
/// overlapping) and tests the high bit once. About 3x faster than `<[u8]>::is_ascii` for the
/// 8..64-byte strings typical of record fields.
#[inline(always)]
fn is_ascii_fast(bytes: &[u8]) -> bool {
    #[inline(always)]
    fn word(bytes: &[u8], at: usize) -> u64 {
        match bytes.get(at..at + 8) {
            Some(w) => u64::from_ne_bytes([w[0], w[1], w[2], w[3], w[4], w[5], w[6], w[7]]),
            None => u64::MAX,
        }
    }
    let n = bytes.len();
    if n < 8 {
        return bytes.iter().fold(0u8, |acc, &b| acc | b) < 0x80;
    }
    let mut acc = word(bytes, n - 8);
    let mut i = 0;
    while i + 8 < n {
        acc |= word(bytes, i);
        i += 8;
    }
    acc & 0x8080_8080_8080_8080 == 0
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

    #[test]
    fn test_is_ascii_fast_matches_std_for_every_length_and_position() {
        for len in 0..80usize {
            let clean = vec![b'a'; len];
            assert!(is_ascii_fast(&clean), "len {len}");
            for bad in 0..len {
                let mut b = clean.clone();
                b[bad] = 0x80;
                assert_eq!(is_ascii_fast(&b), b.is_ascii(), "len {len} bad {bad}");
            }
        }
    }

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
        _pad: u32,
        name: ArchivedString,
        tags: ArchivedSlice<InlineId>,
    }

    crate::impl_archive_pod!(ArchivedSampleUser {
        id: InlineId,
        score: u64,
        age: u32,
        _pad: u32,
        name: ArchivedString,
        tags: ArchivedSlice<InlineId>,
    });

    impl ZeroCopyRecord for SampleUser {
        type Archived = ArchivedSampleUser;

        fn archive(&self, builder: &mut ArchiveBuilder<'_>) -> Self::Archived {
            let name = builder.write_str(&self.name);
            let tags = builder.write_slice(&self.tags);
            ArchivedSampleUser {
                id: self.id,
                score: self.score,
                age: self.age,
                _pad: 0,
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

    use storage_types::testing::SweepRng;

    /// Exercises every decoder entry point on `frame`; must never panic (`RULE-ERR-01`).
    fn decode_all(frame: &[u8]) {
        let _ = RecordCodec::validate_frame::<SampleUser>(frame);
        let _ = RecordCodec::decode_owned::<SampleUser>(frame);
        let _ = RecordCodec::header_ref(frame);
        let _ = RecordCodec::decode_fast::<SampleUser>(frame);
        let view = RecordCodec::decode_fast_or::<SampleUser>(frame);
        let _ = view.get_str(&view.name).len();
        let _ = view.get_slice(&view.tags).len();
        let _ = view.to_owned_record();
        let key = crate::crypto::EncryptionKey::from_bytes([1u8; 32]);
        let ring = crate::crypto::KeyRing::new().with_key(1, key.clone());
        let id = InlineId::new("user:hostile").unwrap();
        let _ = RecordCodec::encrypt_frame(&key, 1, id, frame);
        let _ = RecordCodec::decrypt_frame(&ring, id, frame);
        let _ = RecordCodec::sealed_key_id(frame);
        let _ = RecordCodec::authenticate_frame(&key, 1, id, frame);
        let _ = RecordCodec::strip_authentication(&ring, id, frame);
        let _ = RecordCodec::unprotect_frame(&ring, id, frame);
        let _ = RecordCodec::frame_protection(frame);
    }

    #[test]
    fn test_hostile_frames_never_panic() {
        let user = SampleUser {
            id: InlineId::new("user:hostile").unwrap(),
            age: 33,
            score: 99,
            name: "a fairly long name, repeated: ".repeat(20),
            tags: (0..12).map(|i| InlineId::new(&format!("tag_{i}")).unwrap()).collect(),
        };
        let mut rng = SweepRng::new(0x9E37_79B9_7F4A_7C15);
        for flags in [RecordFlags::NONE, RecordFlags::COMPRESSED_LZ4] {
            let frame = RecordCodec::encode_aligned(&user, Version(3), 77, flags, 0);
            let hdr = *RecordCodec::header_ref(&frame).unwrap();
            let payload = frame.as_slice()[HEADER_SIZE..].to_vec();

            // 0. Hostile encrypted frames: truncations and mutations must fail authentication.
            let key = crate::crypto::EncryptionKey::from_bytes([1u8; 32]);
            let ring = crate::crypto::KeyRing::new().with_key(1, key.clone());
            let enc = RecordCodec::encrypt_frame(&key, 1, user.id, &frame).unwrap();
            for cut in (0..enc.len()).step_by(7) {
                decode_all(&enc.as_slice()[..cut]);
            }
            for _ in 0..500 {
                let mut bad = enc.as_slice().to_vec();
                rng.mutate(&mut bad);
                let bad = AlignedBytes::copy_from_slice(&bad);
                if bad.as_slice() != enc.as_slice() {
                    assert!(RecordCodec::decrypt_frame(&ring, user.id, &bad).is_err());
                }
            }

            // 0b. Hostile authenticated frames.
            let auth = RecordCodec::authenticate_frame(&key, 1, user.id, &frame).unwrap();
            for cut in (0..auth.len()).step_by(5) {
                decode_all(&auth.as_slice()[..cut]);
            }
            for _ in 0..500 {
                let mut bad = auth.as_slice().to_vec();
                rng.mutate(&mut bad);
                let bad = AlignedBytes::copy_from_slice(&bad);
                if bad.as_slice() != auth.as_slice() {
                    assert!(RecordCodec::strip_authentication(&ring, user.id, &bad).is_err());
                }
            }

            // 1. Every truncation, and an unaligned copy.
            for cut in 0..=frame.len() {
                decode_all(&frame.as_slice()[..cut]);
            }
            let mut unaligned = vec![0u8; frame.len() + 1];
            unaligned[1..].copy_from_slice(&frame);
            decode_all(&unaligned[1..]);

            // 2. Raw bit flips anywhere (normally stopped by the checksums).
            for _ in 0..500 {
                let mut bad = frame.as_slice().to_vec();
                rng.mutate(&mut bad);
                decode_all(&AlignedBytes::copy_from_slice(&bad));
            }

            // 3. Crafted frames: hostile payload *and* header fields with valid checksums, so the
            //    structural validators (not just the checksums) are exercised.
            for _ in 0..4_000 {
                let mut p = payload.clone();
                rng.mutate(&mut p);
                if rng.below(4) == 0 {
                    p.truncate(rng.below(p.len() + 1));
                }
                let flag_bits = if rng.below(8) == 0 { rng.next_u64() as u16 } else { hdr.flags };
                let forged = RecordHeader::new(
                    hdr.schema_version,
                    Version(hdr.record_version),
                    hdr.updated_at_ns,
                    checksum_payload(&p),
                    rng.edge_u32(p.len() as u32),
                    rng.edge_u32(hdr.uncompressed_len),
                    rng.edge_u32(hdr.root_offset),
                    RecordFlags(flag_bits),
                    hdr.collection_id,
                    0,
                );
                let mut f = forged.to_bytes().to_vec();
                f.extend_from_slice(&p);
                decode_all(&AlignedBytes::copy_from_slice(&f));
            }
        }
    }

    #[test]
    fn test_hostile_lz4_never_panics_or_overallocates() {
        let data: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8 ^ (i / 97) as u8).collect();
        let good = compress_lz4(&data);
        assert_eq!(decompress_lz4(&good, data.len()).unwrap(), data);
        // A 3-byte input claiming 256 MiB must be rejected before any allocation.
        assert!(decompress_lz4(&[0x01, 0xF0, 0x00], storage_types::MAX_PAYLOAD_SIZE as usize).is_err());
        let mut rng = SweepRng::new(42);
        for _ in 0..5_000 {
            let mut bad = good.clone();
            rng.mutate(&mut bad);
            bad.truncate(rng.below(bad.len() + 1));
            let claimed = match rng.below(3) {
                0 => data.len(),
                1 => rng.below(1 << 20),
                _ => rng.edge_u32(data.len() as u32) as usize,
            };
            let _ = decompress_lz4(&bad, claimed);
        }
    }

    #[test]
    fn test_frame_encryption_roundtrip_and_tamper_detection() {
        use crate::crypto::{EncryptionKey, KeyRing};
        let key = EncryptionKey::from_bytes([7u8; 32]);
        let ring = KeyRing::new().with_key(1, key.clone());
        let id = InlineId::new("secret_user").unwrap();
        let user = SampleUser {
            id,
            age: 41,
            score: 9_001,
            name: "a confidential name".to_string(),
            tags: vec![InlineId::new("vip").unwrap()],
        };
        for flags in [RecordFlags::NONE, RecordFlags::COMPRESSED_LZ4] {
            let plain = RecordCodec::encode_aligned(&user, Version(3), 77, flags, 123);
            let enc = RecordCodec::encrypt_frame(&key, 1, id, &plain).unwrap();
            assert_eq!(RecordCodec::sealed_key_id(&enc), Some(1));
            assert!(RecordCodec::is_encrypted(&enc));
            // The plaintext is not visible in the ciphertext.
            assert!(!enc.windows(12).any(|w| w == b"confidential"));
            // Plaintext decoders refuse encrypted frames instead of misreading them.
            assert!(RecordCodec::validate_frame::<SampleUser>(&enc).is_err());
            assert!(RecordCodec::decode_fast::<SampleUser>(&enc).is_err());
            assert!(RecordCodec::decode_owned::<SampleUser>(&enc).is_err());
            // Round trip restores the exact frame.
            let dec = RecordCodec::decrypt_frame(&ring, id, &enc).unwrap();
            assert_eq!(dec.as_slice(), plain.as_slice());
            assert_eq!(RecordCodec::decode_owned::<SampleUser>(&dec).unwrap().1, user);
            // Fresh nonce per encryption.
            let enc2 = RecordCodec::encrypt_frame(&key, 1, id, &plain).unwrap();
            assert_ne!(enc.as_slice(), enc2.as_slice());
            // Wrong key, wrong record id (frame swap), tampered ciphertext all fail.
            assert!(RecordCodec::decrypt_frame(&KeyRing::new().with_key(1, EncryptionKey::from_bytes([8u8; 32])), id, &enc).is_err());
            assert!(RecordCodec::decrypt_frame(&KeyRing::new().with_key(2, key.clone()), id, &enc).is_err(), "unknown key id");
            assert!(RecordCodec::decrypt_frame(&ring, InlineId::new("other").unwrap(), &enc).is_err());
            let mut bad = enc.as_slice().to_vec();
            let last = bad.len() - 20;
            bad[last] ^= 1;
            assert!(RecordCodec::decrypt_frame(&ring, id, &AlignedBytes::copy_from_slice(&bad)).is_err());
            // Editing an authenticated header field (TTL) with a re-sealed header checksum fails.
            let h = *RecordCodec::header_ref(&enc).unwrap();
            let forged = RecordHeader::new(
                h.schema_version,
                Version(h.record_version),
                h.updated_at_ns,
                h.payload_checksum,
                h.payload_len,
                h.uncompressed_len,
                h.root_offset,
                h.flags_typed(),
                h.collection_id,
                1, // expire the record
            );
            let mut f = forged.to_bytes().to_vec();
            f.extend_from_slice(&enc.as_slice()[HEADER_SIZE..]);
            assert!(RecordCodec::decrypt_frame(&ring, id, &AlignedBytes::copy_from_slice(&f)).is_err());
        }
    }

    #[test]
    fn test_authenticated_frames_stay_readable_and_detect_tampering() {
        use crate::crypto::{EncryptionKey, KeyRing};
        let key = EncryptionKey::from_bytes([9u8; 32]);
        let ring = KeyRing::new().with_key(4, key.clone());
        let id = InlineId::new("public_user").unwrap();
        let user = SampleUser {
            id,
            age: 30,
            score: 5,
            name: "visible but tamper-proof".to_string(),
            tags: vec![],
        };
        let plain = RecordCodec::encode_aligned(&user, Version(2), 9, RecordFlags::NONE, 0);
        let auth = RecordCodec::authenticate_frame(&key, 4, id, &plain).unwrap();
        assert_eq!(RecordCodec::frame_protection(&auth), FrameProtection::Authenticated(4));
        assert_eq!(RecordCodec::frame_protection(&plain), FrameProtection::Plain);
        // Payload is still plaintext and zero-copy readable in place.
        assert_eq!(RecordCodec::decode_fast::<SampleUser>(&auth).unwrap().score, 5);
        assert!(RecordCodec::validate_frame::<SampleUser>(&auth).is_ok());
        // Stripping restores the exact plain frame.
        assert_eq!(RecordCodec::strip_authentication(&ring, id, &auth).unwrap().as_slice(), plain.as_slice());
        assert_eq!(RecordCodec::unprotect_frame(&ring, id, &auth).unwrap().as_slice(), plain.as_slice());
        // Any modification, wrong record id, wrong or unknown key: rejected.
        let reject = |bytes: &[u8]| {
            assert!(RecordCodec::strip_authentication(&ring, id, &AlignedBytes::copy_from_slice(bytes)).is_err());
        };
        let mut b = auth.as_slice().to_vec();
        b[HEADER_SIZE + 3] ^= 1; // payload
        let h = *RecordCodec::header_ref(&auth).unwrap();
        let forged = RecordHeader::new(h.schema_version, Version(h.record_version), h.updated_at_ns,
            checksum_payload(&b[HEADER_SIZE..HEADER_SIZE + h.payload_len as usize]), h.payload_len,
            h.uncompressed_len, h.root_offset, h.flags_typed(), h.collection_id, h.ttl_expires_at_ns);
        b[..HEADER_SIZE].copy_from_slice(&forged.to_bytes());
        reject(&b);
        let mut b = auth.as_slice().to_vec();
        let n = b.len();
        b[n - 1] ^= 1; // tag
        reject(&b);
        assert!(RecordCodec::strip_authentication(&ring, InlineId::new("x").unwrap(), &auth).is_err());
        assert!(RecordCodec::strip_authentication(&KeyRing::new().with_key(4, EncryptionKey::from_bytes([1; 32])), id, &auth).is_err());
        assert!(RecordCodec::strip_authentication(&KeyRing::new(), id, &auth).is_err());
        // Cannot double-protect.
        assert!(RecordCodec::authenticate_frame(&key, 4, id, &auth).is_err());
    }

    #[test]
    fn test_decode_fast_is_safe_on_corrupted_frames() {
        let user = SampleUser {
            id: InlineId::new("u").unwrap(),
            age: 1,
            score: 2,
            name: "n".to_string(),
            tags: vec![InlineId::new("t").unwrap()],
        };
        let frame = RecordCodec::encode_aligned(&user, Version(1), 1, RecordFlags::NONE, 0);
        assert_eq!(RecordCodec::decode_fast::<SampleUser>(&frame).unwrap().score, 2);

        // Corrupt payload_len / root_offset: decode must fail cleanly, never read out of bounds.
        for (off, val) in [(32usize, u32::MAX), (40usize, 10_000u32)] {
            let mut bad = frame.as_slice().to_vec();
            bad[off..off + 4].copy_from_slice(&val.to_le_bytes());
            let aligned = AlignedBytes::copy_from_slice(&bad);
            assert!(RecordCodec::decode_fast::<SampleUser>(&aligned).is_err());
            assert_eq!(RecordCodec::decode_fast_or::<SampleUser>(&aligned).score, 0);
        }
        // Garbage string offsets resolve to "" instead of UB.
        let s = ArchivedString { offset: u32::MAX - 2, len: 100 };
        assert_eq!(s.as_str(&[1, 2, 3]), "");
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
