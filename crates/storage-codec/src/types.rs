//! Full-spectrum zero-copy data types (`ArchivedBool`, `ArchivedChar`, `ArchivedU128`,
//! `ArchivedI128`, `ArchivedUuid`, `ArchivedStringSlice`, `PropertyValue`, `ArchivedMap`).
//!
//! Ensures every primitive, 128-bit number, Unicode scalar, boolean, string array, and
//! schema-less key-value map can be dereferenced directly from `mmap` with **zero heap
//! allocations** and **zero Undefined Behavior** on untrusted disk bytes.

use std::collections::BTreeMap;
use storage_types::{InlineId, Result, StorageError};

use crate::archive::{ArchiveBuilder, ArchivePod, ArchivedBytes, ArchivedSlice, ArchivedString};

// SAFETY: (this note covers every impl below) each type is `#[repr(C)]`/`#[repr(transparent)]` over integers or
// other `ArchivePod` types with no implicit padding, and accepts every bit pattern (validity
// constraints such as `ArchivedBool <= 1` or valid `char` codepoints are checked by `validate`
// and tolerated by every accessor).
unsafe impl ArchivePod for ArchivedBool {}
// SAFETY: see above (`u32`).
unsafe impl ArchivePod for ArchivedChar {}
// SAFETY: see above (two `u64`).
unsafe impl ArchivePod for ArchivedU128 {}
// SAFETY: see above (two `u64`).
unsafe impl ArchivePod for ArchivedI128 {}
// SAFETY: see above (`[u8; 16]`).
unsafe impl ArchivePod for ArchivedUuid {}
// SAFETY: see above (one `ArchivedSlice`).
unsafe impl ArchivePod for ArchivedStringSlice {}
// SAFETY: see above (`u8` tag + explicit `[u8; 7]` pad + `[u64; 3]` = 32 bytes).
unsafe impl ArchivePod for ArchivedPropertyValue {}
// SAFETY: see above (8-byte `ArchivedString` + 32-byte value at offset 8 = 40 bytes).
unsafe impl ArchivePod for ArchivedMapEntry {}
// SAFETY: see above (one `ArchivedSlice`).
unsafe impl ArchivePod for ArchivedMap {}

/// Memory-safe zero-copy boolean (`#[repr(transparent)]`, 1 byte).
///
/// Unlike raw Rust `bool` (where byte values `2..=255` cause immediate Undefined Behavior
/// if cast from untrusted `mmap` bytes), `ArchivedBool` stores a `u8` and validates `0 | 1`.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ArchivedBool(pub u8);

impl ArchivedBool {
    pub const FALSE: Self = Self(0);
    pub const TRUE: Self = Self(1);

    #[inline]
    pub const fn new(val: bool) -> Self {
        Self(if val { 1 } else { 0 })
    }

    #[inline]
    pub const fn as_bool(self) -> bool {
        self.0 != 0
    }

    #[inline]
    pub fn validate(self) -> Result<()> {
        if self.0 > 1 {
            return Err(StorageError::invalid_archive(
                "ArchivedBool byte is not 0 or 1",
            ));
        }
        Ok(())
    }
}

impl From<bool> for ArchivedBool {
    #[inline]
    fn from(v: bool) -> Self {
        Self::new(v)
    }
}

impl From<ArchivedBool> for bool {
    #[inline]
    fn from(v: ArchivedBool) -> Self {
        v.as_bool()
    }
}

/// Memory-safe zero-copy Unicode scalar `char` (`#[repr(transparent)]`, 4 bytes).
///
/// Validates `char::from_u32` once on page load so surrogate or out-of-range codepoints
/// in corrupted files never trigger Rust `char` Undefined Behavior.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ArchivedChar(pub u32);

impl ArchivedChar {
    #[inline]
    pub const fn new(c: char) -> Self {
        Self(c as u32)
    }

    #[inline]
    pub fn validate(self) -> Result<()> {
        if char::from_u32(self.0).is_none() {
            return Err(StorageError::invalid_archive(
                "ArchivedChar contains invalid Unicode scalar value",
            ));
        }
        Ok(())
    }

    #[inline]
    pub fn as_char(self) -> char {
        char::from_u32(self.0).unwrap_or('\u{FFFD}')
    }
}

/// 8-byte-aligned 128-bit unsigned integer (`u128`) for zero-copy `#[repr(C)]` structs.
#[repr(C, align(8))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct ArchivedU128 {
    pub lo: u64,
    pub hi: u64,
}

impl ArchivedU128 {
    #[inline]
    pub const fn new(val: u128) -> Self {
        Self {
            lo: val as u64,
            hi: (val >> 64) as u64,
        }
    }

    #[inline]
    pub const fn get(self) -> u128 {
        (self.lo as u128) | ((self.hi as u128) << 64)
    }
}

impl From<u128> for ArchivedU128 {
    #[inline]
    fn from(v: u128) -> Self {
        Self::new(v)
    }
}

impl From<ArchivedU128> for u128 {
    #[inline]
    fn from(v: ArchivedU128) -> Self {
        v.get()
    }
}

/// 8-byte-aligned 128-bit signed integer (`i128` / fixed-point decimal) for zero-copy structs.
#[repr(C, align(8))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct ArchivedI128 {
    pub lo: u64,
    pub hi: u64,
}

impl ArchivedI128 {
    #[inline]
    pub const fn new(val: i128) -> Self {
        let u = val as u128;
        Self {
            lo: u as u64,
            hi: (u >> 64) as u64,
        }
    }

    #[inline]
    pub const fn get(self) -> i128 {
        ((self.lo as u128) | ((self.hi as u128) << 64)) as i128
    }
}

impl From<i128> for ArchivedI128 {
    #[inline]
    fn from(v: i128) -> Self {
        Self::new(v)
    }
}

impl From<ArchivedI128> for i128 {
    #[inline]
    fn from(v: ArchivedI128) -> Self {
        v.get()
    }
}

/// Zero-copy 16-byte UUID (`v4` / `v7`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ArchivedUuid(pub [u8; 16]);

impl ArchivedUuid {
    pub const NIL: Self = Self([0u8; 16]);

    #[inline]
    pub const fn new(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    #[inline]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

/// Zero-copy slice of variable-length UTF-8 strings (`Vec<String>` -> `&[ArchivedString]`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct ArchivedStringSlice {
    pub slice: ArchivedSlice<ArchivedString>,
}

impl ArchivedStringSlice {
    pub const EMPTY: Self = Self {
        slice: ArchivedSlice::EMPTY,
    };

    pub fn archive<S: AsRef<str>>(builder: &mut ArchiveBuilder<'_>, items: &[S]) -> Self {
        if items.is_empty() {
            return Self::EMPTY;
        }
        let mut headers = Vec::with_capacity(items.len());
        for s in items {
            headers.push(builder.write_str(s.as_ref()));
        }
        Self {
            slice: builder.write_slice(&headers),
        }
    }

    #[inline]
    pub const fn len(&self) -> usize {
        self.slice.len()
    }

    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.slice.is_empty()
    }

    pub fn validate(&self, payload: &[u8]) -> Result<()> {
        self.slice.validate(payload)?;
        for item in self.slice.as_slice(payload) {
            item.validate(payload)?;
        }
        Ok(())
    }

    /// Accesses the `idx`-th string zero-copy (`0` heap allocations).
    #[inline]
    pub fn get<'a>(&self, payload: &'a [u8], idx: usize) -> Option<&'a str> {
        self.slice
            .as_slice(payload)
            .get(idx)
            .map(|s| s.as_str(payload))
    }

    /// Iterates all `&'a str` elements zero-copy (`0` heap allocations).
    #[inline]
    pub fn iter<'a>(&self, payload: &'a [u8]) -> impl Iterator<Item = &'a str> + 'a {
        self.slice
            .as_slice(payload)
            .iter()
            .map(move |s| s.as_str(payload))
    }

    pub fn to_vec(&self, payload: &[u8]) -> Vec<String> {
        self.iter(payload).map(str::to_owned).collect()
    }
}

/// Owned dynamic property value supporting all scalar, temporal, binary, and text types.
#[derive(Clone, Debug, PartialEq)]
pub enum PropertyValue {
    Null,
    Bool(bool),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    Int128(i128),
    TimestampNs(i64),
    Uuid([u8; 16]),
    Id(InlineId),
    Text(String),
    Bytes(Vec<u8>),
}

/// Tag discriminant for `ArchivedPropertyValue`.
const TAG_NULL: u8 = 0;
const TAG_BOOL: u8 = 1;
const TAG_INT64: u8 = 2;
const TAG_UINT64: u8 = 3;
const TAG_FLOAT64: u8 = 4;
const TAG_INT128: u8 = 5;
const TAG_TIMESTAMP_NS: u8 = 6;
const TAG_UUID: u8 = 7;
const TAG_INLINE_ID: u8 = 8;
const TAG_TEXT: u8 = 9;
const TAG_BYTES: u8 = 10;

/// Zero-copy `#[repr(C)]` archived representation of `PropertyValue` (32 bytes, 8-byte aligned).
#[repr(C, align(8))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ArchivedPropertyValue {
    pub tag: u8,
    pub _pad: [u8; 7],
    pub slots: [u64; 3],
}

/// Borrowed zero-copy view of an `ArchivedPropertyValue` resolved against `payload`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PropertyValueRef<'a> {
    Null,
    Bool(bool),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    Int128(i128),
    TimestampNs(i64),
    Uuid([u8; 16]),
    Id(InlineId),
    Text(&'a str),
    Bytes(&'a [u8]),
}

impl PropertyValue {
    pub fn archive(&self, builder: &mut ArchiveBuilder<'_>) -> ArchivedPropertyValue {
        let mut slots = [0u64; 3];
        let tag = match *self {
            Self::Null => TAG_NULL,
            Self::Bool(b) => {
                slots[0] = u64::from(b);
                TAG_BOOL
            }
            Self::Int64(v) => {
                slots[0] = v as u64;
                TAG_INT64
            }
            Self::UInt64(v) => {
                slots[0] = v;
                TAG_UINT64
            }
            Self::Float64(v) => {
                slots[0] = v.to_bits();
                TAG_FLOAT64
            }
            Self::Int128(v) => {
                let u = v as u128;
                slots[0] = u as u64;
                slots[1] = (u >> 64) as u64;
                TAG_INT128
            }
            Self::TimestampNs(ns) => {
                slots[0] = ns as u64;
                TAG_TIMESTAMP_NS
            }
            Self::Uuid(bytes) => {
                slots[0] = storage_types::wire::read_u64_le(&bytes, 0);
                slots[1] = storage_types::wire::read_u64_le(&bytes, 8);
                TAG_UUID
            }
            Self::Id(id) => {
                let raw = id.to_raw_bytes();
                slots[0] = storage_types::wire::read_u64_le(&raw, 0);
                slots[1] = storage_types::wire::read_u64_le(&raw, 8);
                slots[2] = storage_types::wire::read_u64_le(&raw, 16);
                TAG_INLINE_ID
            }
            Self::Text(ref s) => {
                let arch = builder.write_str(s);
                slots[0] = u64::from(arch.offset);
                slots[1] = u64::from(arch.len);
                TAG_TEXT
            }
            Self::Bytes(ref b) => {
                let arch = builder.write_bytes(b);
                slots[0] = u64::from(arch.offset);
                slots[1] = u64::from(arch.len);
                TAG_BYTES
            }
        };
        ArchivedPropertyValue {
            tag,
            _pad: [0; 7],
            slots,
        }
    }
}

impl ArchivedPropertyValue {
    pub fn validate(&self, payload: &[u8]) -> Result<()> {
        match self.tag {
            TAG_NULL | TAG_INT64 | TAG_UINT64 | TAG_FLOAT64 | TAG_INT128 | TAG_TIMESTAMP_NS
            | TAG_UUID => Ok(()),
            TAG_BOOL => {
                if self.slots[0] > 1 {
                    return Err(StorageError::invalid_archive(
                        "ArchivedPropertyValue Bool out of range",
                    ));
                }
                Ok(())
            }
            TAG_INLINE_ID => {
                let mut raw = [0u8; 24];
                raw[0..8].copy_from_slice(&self.slots[0].to_le_bytes());
                raw[8..16].copy_from_slice(&self.slots[1].to_le_bytes());
                raw[16..24].copy_from_slice(&self.slots[2].to_le_bytes());
                InlineId::from_raw_bytes(raw)?;
                Ok(())
            }
            TAG_TEXT => {
                let s = ArchivedString {
                    offset: self.slots[0] as u32,
                    len: self.slots[1] as u32,
                };
                s.validate(payload)
            }
            TAG_BYTES => {
                let b = ArchivedBytes {
                    offset: self.slots[0] as u32,
                    len: self.slots[1] as u32,
                };
                b.validate(payload)
            }
            _ => Err(StorageError::invalid_archive(
                "unknown ArchivedPropertyValue discriminant tag",
            )),
        }
    }

    /// Resolves this archived property to a zero-copy `PropertyValueRef<'a>` (`0` heap allocations).
    #[inline]
    pub fn as_ref<'a>(&self, payload: &'a [u8]) -> PropertyValueRef<'a> {
        match self.tag {
            TAG_BOOL => PropertyValueRef::Bool(self.slots[0] != 0),
            TAG_INT64 => PropertyValueRef::Int64(self.slots[0] as i64),
            TAG_UINT64 => PropertyValueRef::UInt64(self.slots[0]),
            TAG_FLOAT64 => PropertyValueRef::Float64(f64::from_bits(self.slots[0])),
            TAG_INT128 => {
                let u = u128::from(self.slots[0]) | (u128::from(self.slots[1]) << 64);
                PropertyValueRef::Int128(u as i128)
            }
            TAG_TIMESTAMP_NS => PropertyValueRef::TimestampNs(self.slots[0] as i64),
            TAG_UUID => {
                let mut b = [0u8; 16];
                b[0..8].copy_from_slice(&self.slots[0].to_le_bytes());
                b[8..16].copy_from_slice(&self.slots[1].to_le_bytes());
                PropertyValueRef::Uuid(b)
            }
            TAG_INLINE_ID => {
                let mut raw = [0u8; 24];
                raw[0..8].copy_from_slice(&self.slots[0].to_le_bytes());
                raw[8..16].copy_from_slice(&self.slots[1].to_le_bytes());
                raw[16..24].copy_from_slice(&self.slots[2].to_le_bytes());
                PropertyValueRef::Id(InlineId::from_raw_bytes(raw).unwrap_or(InlineId::EMPTY))
            }
            TAG_TEXT => {
                let s = ArchivedString {
                    offset: self.slots[0] as u32,
                    len: self.slots[1] as u32,
                };
                PropertyValueRef::Text(s.as_str(payload))
            }
            TAG_BYTES => {
                let b = ArchivedBytes {
                    offset: self.slots[0] as u32,
                    len: self.slots[1] as u32,
                };
                PropertyValueRef::Bytes(b.as_slice(payload))
            }
            _ => PropertyValueRef::Null,
        }
    }

    pub fn to_owned_value(&self, payload: &[u8]) -> PropertyValue {
        match self.as_ref(payload) {
            PropertyValueRef::Null => PropertyValue::Null,
            PropertyValueRef::Bool(b) => PropertyValue::Bool(b),
            PropertyValueRef::Int64(v) => PropertyValue::Int64(v),
            PropertyValueRef::UInt64(v) => PropertyValue::UInt64(v),
            PropertyValueRef::Float64(v) => PropertyValue::Float64(v),
            PropertyValueRef::Int128(v) => PropertyValue::Int128(v),
            PropertyValueRef::TimestampNs(v) => PropertyValue::TimestampNs(v),
            PropertyValueRef::Uuid(u) => PropertyValue::Uuid(u),
            PropertyValueRef::Id(id) => PropertyValue::Id(id),
            PropertyValueRef::Text(s) => PropertyValue::Text(s.to_owned()),
            PropertyValueRef::Bytes(b) => PropertyValue::Bytes(b.to_vec()),
        }
    }
}

/// Entry inside a sorted zero-copy `ArchivedMap`.
#[repr(C, align(8))]
#[derive(Clone, Copy, Debug, Default)]
pub struct ArchivedMapEntry {
    pub key: ArchivedString,
    pub value: ArchivedPropertyValue,
}

/// Zero-copy sorted key-value map (`BTreeMap<String, PropertyValue>`) supporting $O(\log N)$
/// binary-search lookup directly on the `mmap` page with **zero heap allocations**.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct ArchivedMap {
    pub entries: ArchivedSlice<ArchivedMapEntry>,
}

impl ArchivedMap {
    pub const EMPTY: Self = Self {
        entries: ArchivedSlice::EMPTY,
    };

    pub fn archive(
        builder: &mut ArchiveBuilder<'_>,
        map: &BTreeMap<String, PropertyValue>,
    ) -> Self {
        if map.is_empty() {
            return Self::EMPTY;
        }
        let mut out = Vec::with_capacity(map.len());
        for (k, v) in map {
            let key = builder.write_str(k);
            let value = v.archive(builder);
            out.push(ArchivedMapEntry { key, value });
        }
        Self {
            entries: builder.write_slice(&out),
        }
    }

    #[inline]
    pub const fn len(&self) -> usize {
        self.entries.len()
    }

    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn validate(&self, payload: &[u8]) -> Result<()> {
        self.entries.validate(payload)?;
        for entry in self.entries.as_slice(payload) {
            entry.key.validate(payload)?;
            entry.value.validate(payload)?;
        }
        Ok(())
    }

    /// Looks up `key` in $O(\log N)$ binary search directly inside `payload` (`0` heap allocations).
    #[inline]
    pub fn get<'a>(&self, payload: &'a [u8], key: &str) -> Option<PropertyValueRef<'a>> {
        let slice = self.entries.as_slice(payload);
        let idx = slice
            .binary_search_by(|entry| entry.key.as_str(payload).cmp(key))
            .ok()?;
        Some(slice[idx].value.as_ref(payload))
    }

    pub fn unarchive(&self, payload: &[u8]) -> BTreeMap<String, PropertyValue> {
        let mut map = BTreeMap::new();
        for entry in self.entries.as_slice(payload) {
            map.insert(
                entry.key.as_str(payload).to_owned(),
                entry.value.to_owned_value(payload),
            );
        }
        map
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scratch:: with_thread_scratch;

    #[test]
    fn test_all_zero_copy_data_types_and_archived_map() {
        let mut props = BTreeMap::new();
        props.insert("active".to_string(), PropertyValue::Bool(true));
        props.insert("balance_cents".to_string(), PropertyValue::Int128(-999_999_999_999_999_999_999i128));
        props.insert("pi".to_string(), PropertyValue::Float64(core::f64::consts::PI));
        props.insert("owner".to_string(), PropertyValue::Id(InlineId::from_static("user_01")));
        props.insert("label".to_string(), PropertyValue::Text("zero-copy".to_string()));
        props.insert("raw".to_string(), PropertyValue::Bytes(vec![1, 2, 3, 4]));
        props.insert("uuid".to_string(), PropertyValue::Uuid([0xAB; 16]));

        let strings = vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()];

        with_thread_scratch(|scratch| {
            scratch.clear();
            let mut builder = ArchiveBuilder::new(scratch, 0);
            let arch_map = ArchivedMap::archive(&mut builder, &props);
            let arch_strs = ArchivedStringSlice::archive(&mut builder, &strings);

            let payload = scratch.as_slice();
            arch_map.validate(payload).unwrap();
            arch_strs.validate(payload).unwrap();

            assert_eq!(arch_map.get(payload, "active"), Some(PropertyValueRef::Bool(true)));
            assert_eq!(
                arch_map.get(payload, "balance_cents"),
                Some(PropertyValueRef::Int128(-999_999_999_999_999_999_999i128))
            );
            assert_eq!(
                arch_map.get(payload, "pi"),
                Some(PropertyValueRef::Float64(core::f64::consts::PI))
            );
            assert_eq!(
                arch_map.get(payload, "label"),
                Some(PropertyValueRef::Text("zero-copy"))
            );
            assert_eq!(arch_map.unarchive(payload), props);

            assert_eq!(arch_strs.len(), 3);
            assert_eq!(arch_strs.get(payload, 1), Some("beta"));
            assert_eq!(arch_strs.to_vec(payload), strings);
        });
    }
}
