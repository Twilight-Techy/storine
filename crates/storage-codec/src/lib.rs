//! Zero-copy relative-offset archive codec, SIMD-accelerated `XXH3-64` and `CRC32C`
//! checksums, 64-byte-aligned pooled buffers, and fast LZ4 block compression.

#![deny(unsafe_op_in_unsafe_fn)]
#![warn(missing_debug_implementations, rust_2018_idioms)]

pub mod archive;
pub mod checksum;
pub mod relations;
pub mod compress;
pub mod crypto;
pub mod scratch;
pub mod types;

pub use archive::{
    ArchiveBuilder, ArchivePod, ArchivedBytes, ArchivedSlice, ArchivedString, RecordCodec,
    RecordView, ZeroCopyRecord,
};
pub use relations::{ArchivedManyToMany, ArchivedOneToMany, ArchivedOneToOne};
pub use checksum::{checksum_payload, crc32c_64, xxh3_64};
pub use compress::{compress_lz4, decompress_lz4};
pub use crypto::{
    chacha20_xor_in_place, decrypt_payload_aead, encrypt_payload_aead, generate_unique_nonce,
    open as aead_open, poly1305_tag, seal as aead_seal, EncryptionKey, AEAD_NONCE_LEN, AEAD_TAG_LEN,
};
pub use scratch::{AlignedBytes, AlignedScratchBuf, with_thread_scratch};
pub use types::{
    ArchivedBool, ArchivedChar, ArchivedI128, ArchivedMap, ArchivedMapEntry, ArchivedPropertyValue,
    ArchivedStringSlice, ArchivedU128, ArchivedUuid, PropertyValue, PropertyValueRef,
};
