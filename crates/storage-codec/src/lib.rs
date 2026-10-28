//! Zero-copy relative-offset archive codec, SIMD-accelerated `XXH3-64` and `CRC32C`
//! checksums, 64-byte-aligned pooled buffers, and fast LZ4 block compression.

#![deny(unsafe_op_in_unsafe_fn)]
#![warn(missing_debug_implementations, rust_2018_idioms)]

pub mod archive;
pub mod checksum;
pub mod relations;
pub mod compress;
pub mod crypto;
pub mod curve25519;
pub mod rand;
pub mod sha512;
pub mod field_ops;
pub mod scratch;
pub mod sealed_file;
pub mod types;
pub mod value_wire;

pub use archive::{
    ArchiveBuilder, ArchivePod, ArchivedBytes, ArchivedSlice, ArchivedString, FrameProtection, RecordCodec, AUTH_TRAILER_LEN,
    RecordView, ZeroCopyRecord,
};
pub use relations::{ArchivedManyToMany, ArchivedOneToMany, ArchivedOneToOne};
pub use checksum::{checksum_payload, crc32c_64, xxh3_64};
pub use compress::{compress_lz4, decompress_lz4};
pub use crypto::{
    chacha20_xor_in_place, decrypt_payload_aead, encrypt_payload_aead, generate_unique_nonce,
    open as aead_open, poly1305_tag, seal as aead_seal, chacha20_xor_at, open_in_place as aead_open_in_place,
    seal_in_place as aead_seal_in_place, EncryptionKey, KeyRing, RegionSealer, AEAD_NONCE_LEN, AEAD_TAG_LEN,
};
pub use scratch::{AlignedBytes, AlignedScratchBuf, with_thread_scratch};
pub use sealed_file::{is_sealed, FileSeal, SEALED_FILE_HEADER_LEN, SEALED_FILE_MAGIC};
pub use types::{
    ArchivedBool, ArchivedChar, ArchivedI128, ArchivedMap, ArchivedMapEntry, ArchivedPropertyValue,
    ArchivedStringSlice, ArchivedU128, ArchivedUuid, ListRef, MapRef, PropertyValue, PropertyValueRef,
    MAX_VALUE_DEPTH,
};
