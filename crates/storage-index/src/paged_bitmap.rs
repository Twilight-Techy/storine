//! Disk-Backed Paged Bitmap Tag Index (`TASK-035`, `TASK-083`, `F-PIDX-02`, `RULE-MEM-01`,
//! `RULE-CONC-01`).
//!
//! Posting bitmaps of every `(field_id, tag)` live in 512-byte `[u64; 64]` blocks of an immutable,
//! memory-mapped generation file; only a directory (`(field, tag) -> chunk refs`, 8 bytes per
//! non-empty 4096-ordinal chunk) and small deltas live in RAM.
//!
//! ## Visibility
//! For a key `k = (field_id, tag)` the posting set is
//! `((base[k] - frozen.removed[k]) ∪ frozen.hot[k] - removed[k]) ∪ hot[k]`, where `frozen` is the
//! delta being merged by a running checkpoint.
//!
//! ## Checkpoints
//! Freeze the delta (`O(1)` under the lock), then — without any lock — stream-merge chunk by chunk
//! (`O(512 B)` working memory per key, even for a tag covering every record) into a new
//! generation file, publish it through the manifest, and swap it in under a brief lock.
//!
//! ## Generation file (`"BMX3"`)
//! - `[0..4096)` header: `magic | block_count u32 | dir_offset u64 | dir_len u32 | 0 u32 |
//!   dir_crc u64 | blocks_crc u64 | header_crc u64`;
//! - blocks: `block_count` x 512 B starting at 4096;
//! - directory at `dir_offset`: `tag_count u32`, then per key
//!   `field u16 | tag [u8; 24] | chunk_count u32 | cardinality u64 | (chunk_id u32, block u32)*`
//!   with strictly increasing chunk ids.

#[cfg(test)]
use std::fs;
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use storage_codec::{checksum_payload, FileSeal};
use storage_mmap::MmapRegion;
use storage_types::wire::{read_array_24, read_u16_le, read_u32_le, read_u64_le};
use storage_types::{FastHashMap, InlineId, RecordOrdinal, Result, StorageError};

use crate::bitmap::{BitmapTagIndex, CompressedBitmap};
use crate::paged_files::{
    generation_path, map_path_read_only, open_generation_cipher, publish_generation, read_manifest,
    remove_stale_generations, temp_generation_path, write_manifest, GenerationSealer, PageCipher,
    SealSettings, ENC_SECTION_LEN, ENC_SECTION_OFFSET,
};

const BITMAP_FILE_MAGIC: u32 = 0x424D_5833; // "BMX3"
const BITMAP_MANIFEST_MAGIC: u32 = 0x424D_4D46; // "BMMF"
const BITMAP_BLOCK_BYTES: usize = 512; // 64 x u64 words
const HEADER_REGION: usize = 4096;
const DIR_ENTRY_FIXED: usize = 2 + 24 + 4 + 8;

type Key = (u16, InlineId);
type Words = [u64; 64];

/// Pointer to a 512-byte `[u64; 64]` chunk stored in a generation file (`8` bytes).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MappedChunkRef {
    pub chunk_id: u32,
    pub block_index: u32,
}

#[derive(Debug)]
struct DirEntry {
    chunks: Vec<MappedChunkRef>,
    cardinality: u64,
}

struct BaseGeneration {
    mmap: MmapRegion,
    directory: FastHashMap<Key, DirEntry>,
    path: PathBuf,
    /// Set when the generation's blocks and directory are encrypted at rest (`TASK-088`).
    cipher: Option<PageCipher>,
}

impl BaseGeneration {
    #[inline]
    fn block(&self, block_index: u32) -> Option<Words> {
        let off = HEADER_REGION + block_index as usize * BITMAP_BLOCK_BYTES;
        let mapped = self.mmap.as_slice().get(off..off + BITMAP_BLOCK_BYTES)?;
        let mut plain = [0u8; BITMAP_BLOCK_BYTES];
        let bytes: &[u8] = match self.cipher {
            Some(ref cipher) => {
                plain.copy_from_slice(mapped);
                cipher.decrypt_at(off - HEADER_REGION, &mut plain).ok()?;
                &plain
            }
            None => mapped,
        };
        let mut words = [0u64; 64];
        for (i, w) in words.iter_mut().enumerate() {
            *w = read_u64_le(bytes, i * 8);
        }
        Some(words)
    }

    /// `true` if the base bitmap of `key` holds `ord` (one binary search + one word read).
    fn contains(&self, key: &Key, ord: RecordOrdinal) -> bool {
        let Some(entry) = self.directory.get(key) else {
            return false;
        };
        let chunk_id = ord.0 / 4096;
        let Ok(i) = entry.chunks.binary_search_by_key(&chunk_id, |c| c.chunk_id) else {
            return false;
        };
        let bit = (ord.0 % 4096) as usize;
        let off = HEADER_REGION
            + entry.chunks[i].block_index as usize * BITMAP_BLOCK_BYTES
            + (bit / 64) * 8;
        let Some(mapped) = self.mmap.as_slice().get(off..off + 8) else {
            return false;
        };
        let mut word = [0u8; 8];
        word.copy_from_slice(mapped);
        if let Some(ref cipher) = self.cipher {
            if cipher.decrypt_at(off - HEADER_REGION, &mut word).is_err() {
                return false;
            }
        }
        (u64::from_le_bytes(word) >> (bit % 64)) & 1 == 1
    }

    /// Base chunks of `key`, ascending by chunk id.
    fn chunks(&self, key: &Key) -> Vec<(u32, Words)> {
        self.directory.get(key).map_or_else(Vec::new, |entry| {
            entry
                .chunks
                .iter()
                .filter_map(|c| self.block(c.block_index).map(|w| (c.chunk_id, w)))
                .collect()
        })
    }
}

/// In-progress checkpoint returned by `begin_checkpoint` (holds the checkpoint lock).
pub struct PendingBitmapCheckpoint<'a> {
    _guard: std::sync::MutexGuard<'a, ()>,
    frozen: Arc<FrozenDelta>,
    base: Option<Arc<BaseGeneration>>,
    generation: u64,
}

impl core::fmt::Debug for PendingBitmapCheckpoint<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("PendingBitmapCheckpoint")
            .field("generation", &self.generation)
            .finish()
    }
}

#[derive(Default)]
struct FrozenDelta {
    hot: BitmapTagIndex,
    removed: FastHashMap<Key, CompressedBitmap>,
}

#[derive(Default)]
struct PagedBitmapState {
    base: Option<Arc<BaseGeneration>>,
    frozen: Option<Arc<FrozenDelta>>,
    hot: BitmapTagIndex,
    removed: FastHashMap<Key, CompressedBitmap>,
    generation: u64,
    stamp: u64,
}

impl PagedBitmapState {
    /// The visible posting bitmap of `key` (see the module docs).
    fn bitmap(&self, key: &Key) -> CompressedBitmap {
        let mut bm = match self.base {
            Some(ref base) => CompressedBitmap::from_raw_chunks(base.chunks(key)),
            None => CompressedBitmap::new(),
        };
        if let Some(ref frozen) = self.frozen {
            if let Some(r) = frozen.removed.get(key) {
                bm.and_not_in_place(r);
            }
            let fh = frozen.hot.get_bitmap(key.0, key.1);
            if !fh.is_empty() {
                bm = bm.union(&fh);
            }
        }
        if let Some(r) = self.removed.get(key) {
            bm.and_not_in_place(r);
        }
        let hot = self.hot.get_bitmap(key.0, key.1);
        if !hot.is_empty() {
            bm = bm.union(&hot);
        }
        bm
    }

    /// `true` if `ord` is visible under `key`, without materializing any bitmap.
    fn contains(&self, key: &Key, ord: RecordOrdinal) -> bool {
        if self.hot.contains(key.0, key.1, ord) {
            return true;
        }
        if self.removed.get(key).is_some_and(|r| r.contains(ord)) {
            return false;
        }
        if let Some(ref frozen) = self.frozen {
            if frozen.hot.contains(key.0, key.1, ord) {
                return true;
            }
            if frozen.removed.get(key).is_some_and(|r| r.contains(ord)) {
                return false;
            }
        }
        self.base.as_ref().is_some_and(|b| b.contains(key, ord))
    }

    /// Upper bound of `bitmap(key).len()` without materializing it (never `0` for a non-empty
    /// key, so cost-based short-circuits stay correct).
    fn cardinality_upper_bound(&self, key: &Key) -> usize {
        let base = self
            .base
            .as_ref()
            .and_then(|b| b.directory.get(key))
            .map_or(0, |e| e.cardinality as usize);
        let frozen = self
            .frozen
            .as_ref()
            .map_or(0, |f| f.hot.cardinality(key.0, key.1));
        base + frozen + self.hot.cardinality(key.0, key.1)
    }

    /// Every key that may be non-empty, filtered by `keep`.
    fn keys(&self, keep: impl Fn(&Key) -> bool) -> Vec<Key> {
        let mut keys: Vec<Key> = self
            .base
            .as_ref()
            .map(|b| b.directory.keys().copied().filter(|k| keep(k)).collect())
            .unwrap_or_default();
        if let Some(ref frozen) = self.frozen {
            keys.extend(frozen.hot.keys().into_iter().filter(|k| keep(k)));
        }
        keys.extend(self.hot.keys().into_iter().filter(|k| keep(k)));
        keys.sort_unstable();
        keys.dedup();
        keys
    }

    fn union_of(&self, keys: &[Key]) -> CompressedBitmap {
        let mut acc = CompressedBitmap::new();
        for key in keys {
            let bm = self.bitmap(key);
            if !bm.is_empty() {
                acc = if acc.is_empty() { bm } else { acc.union(&bm) };
            }
        }
        acc
    }
}

/// Disk-backed Paged Bitmap Tag Index (`TASK-035`, `TASK-083`, `F-PIDX-02`).
pub struct PagedBitmapIndex {
    path: PathBuf,
    state: RwLock<PagedBitmapState>,
    /// Protection of generation files written from now on (`TASK-088`).
    seal: RwLock<SealSettings>,
    checkpoint_lock: Mutex<()>,
}

impl core::fmt::Debug for PagedBitmapIndex {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("PagedBitmapIndex")
            .field("path", &self.path)
            .field("mapped_tags", &self.mapped_tag_count())
            .finish()
    }
}

impl PagedBitmapIndex {
    /// Opens (or creates) the index whose manifest is `path`. A corrupted generation is an error
    /// (the caller rebuilds); stale generation files are removed.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        Self::open_sealed(path, &FileSeal::PLAIN)
    }

    /// [`Self::open`] with encryption at rest (`TASK-088`): an encrypted generation is verified
    /// with its key from `seal`; a plaintext one is refused when `seal` forbids plaintext. New
    /// generations are written under `seal` (see [`Self::set_file_seal`]).
    pub fn open_sealed(path: impl Into<PathBuf>, seal: &FileSeal<'_>) -> Result<Self> {
        let path = path.into();
        let mut state = PagedBitmapState::default();
        if let Some((generation, stamp)) = read_manifest(&path, BITMAP_MANIFEST_MAGIC)? {
            let data_path = generation_path(&path, generation);
            let mmap = map_path_read_only(&data_path)?;
            let header = mmap.as_slice().get(..HEADER_REGION).unwrap_or_default();
            let enc = open_generation_cipher(header, seal)?;
            let directory = load_generation(
                &mmap,
                enc.as_ref().map(|(cipher, tag)| (cipher, tag, data_path.as_path())),
            )?;
            state.base = Some(Arc::new(BaseGeneration {
                mmap,
                directory,
                path: data_path,
                cipher: enc.map(|(cipher, _)| cipher),
            }));
            state.generation = generation;
            state.stamp = stamp;
        }
        remove_stale_generations(&path, state.base.as_ref().map(|b| b.path.as_path()));
        Ok(Self {
            path,
            state: RwLock::new(state),
            seal: RwLock::new(SealSettings::from_seal(seal)),
            checkpoint_lock: Mutex::new(()),
        })
    }

    /// Protection for generation files written from now on (the next checkpoint rewrites every
    /// block and the directory under it).
    pub fn set_file_seal(&self, seal: &FileSeal<'_>) {
        *self.seal.write().unwrap_or_else(|poisoned| poisoned.into_inner()) = SealSettings::from_seal(seal);
    }

    #[inline]
    fn read(&self) -> std::sync::RwLockReadGuard<'_, PagedBitmapState> {
        self.state.read().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[inline]
    fn write(&self) -> std::sync::RwLockWriteGuard<'_, PagedBitmapState> {
        self.state.write().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Stamp recorded by the last checkpoint (`0` if never checkpointed).
    pub fn stamp(&self) -> u64 {
        self.read().stamp
    }

    /// Number of `(field, tag)` keys in the mapped base generation.
    pub fn mapped_tag_count(&self) -> usize {
        self.read().base.as_ref().map_or(0, |b| b.directory.len())
    }

    pub fn insert(&self, field_id: u16, tag: InlineId, ord: RecordOrdinal) {
        let mut st = self.write();
        if let Some(r) = st.removed.get_mut(&(field_id, tag)) {
            r.remove(ord);
        }
        st.hot.insert(field_id, tag, ord);
    }

    pub fn remove(&self, field_id: u16, tag: InlineId, ord: RecordOrdinal) {
        let mut st = self.write();
        st.hot.remove(field_id, tag, ord);
        st.removed.entry((field_id, tag)).or_default().insert(ord);
    }

    pub fn get_bitmap(&self, field_id: u16, tag: InlineId) -> CompressedBitmap {
        self.read().bitmap(&(field_id, tag))
    }

    /// Upper-bound cardinality estimate for query planning (`0` only for empty keys).
    pub fn cardinality(&self, field_id: u16, tag: InlineId) -> usize {
        self.read().cardinality_upper_bound(&(field_id, tag))
    }

    /// Intersects `predicates`, smallest (estimated) posting list first.
    pub fn intersect_all(&self, predicates: &[(u16, InlineId)]) -> CompressedBitmap {
        if predicates.is_empty() {
            return CompressedBitmap::new();
        }
        let st = self.read();
        let mut keys = predicates.to_vec();
        keys.sort_by_key(|k| st.cardinality_upper_bound(k));
        let mut acc = st.bitmap(&keys[0]);
        for key in &keys[1..] {
            if acc.is_empty() {
                break;
            }
            acc = acc.intersect(&st.bitmap(key));
        }
        acc
    }

    /// Union of `tags` on `field_id` (`IN`).
    pub fn union_tags(&self, field_id: u16, tags: &[InlineId]) -> CompressedBitmap {
        let keys: Vec<Key> = tags.iter().map(|&t| (field_id, t)).collect();
        self.read().union_of(&keys)
    }

    /// Union of every tag on `field_id` starting with `prefix`.
    pub fn union_tag_prefix(&self, field_id: u16, prefix: &str) -> CompressedBitmap {
        let st = self.read();
        let keys = st.keys(|&(f, t)| f == field_id && t.starts_with_str(prefix));
        st.union_of(&keys)
    }

    /// Union of every tag on `field_id` (`IS NOT NULL`).
    pub fn all_on_field_bitmap(&self, field_id: u16) -> CompressedBitmap {
        let st = self.read();
        let keys = st.keys(|&(f, _)| f == field_id);
        st.union_of(&keys)
    }

    /// `true` if `ord` has every `must_tags` key, none of `not_tags`, and at least one tag of
    /// every `any_tag_groups` group (same semantics as `BitmapTagIndex::matches_ordinal`), using
    /// point lookups only.
    pub fn matches_ordinal(
        &self,
        ord: RecordOrdinal,
        must_tags: &[(u16, InlineId)],
        not_tags: &[(u16, InlineId)],
        any_tag_groups: &[(u16, &[InlineId])],
    ) -> bool {
        if must_tags.is_empty() && not_tags.is_empty() && any_tag_groups.is_empty() {
            return true;
        }
        let st = self.read();
        must_tags.iter().all(|k| st.contains(k, ord))
            && !not_tags.iter().any(|k| st.contains(k, ord))
            && any_tag_groups.iter().all(|&(field, group)| {
                !group.is_empty() && group.iter().any(|&t| st.contains(&(field, t), ord))
            })
    }

    /// Removes every ordinal of `not_predicates` from `target` (`AND NOT`).
    pub fn exclude_from_bitmap(&self, not_predicates: &[(u16, InlineId)], target: &mut CompressedBitmap) {
        if not_predicates.is_empty() || target.is_empty() {
            return;
        }
        let st = self.read();
        for key in not_predicates {
            target.and_not_in_place(&st.bitmap(key));
            if target.is_empty() {
                return;
            }
        }
    }

    /// Keeps only the `candidates` present in every one of `predicates`.
    pub fn filter_in_place(&self, predicates: &[(u16, InlineId)], candidates: &mut Vec<RecordOrdinal>) {
        if predicates.is_empty() || candidates.is_empty() {
            return;
        }
        let st = self.read();
        for key in predicates {
            let bm = st.bitmap(key);
            candidates.retain(|&ord| bm.contains(ord));
            if candidates.is_empty() {
                return;
            }
        }
    }

    /// Checkpoints with the current stamp unchanged. Returns the number of blocks written.
    #[inline]
    pub fn checkpoint_to_disk(&self) -> Result<usize> {
        let stamp = self.stamp();
        self.checkpoint_with_stamp(stamp)
    }

    /// Stream-merges the base and the delta into a new generation tagged with `stamp`. Only the
    /// freeze and swap take the state lock; on error the frozen delta is folded back.
    pub fn checkpoint_with_stamp(&self, stamp: u64) -> Result<usize> {
        let pending = self.begin_checkpoint();
        self.finish_checkpoint(pending, stamp)
    }

    /// Freezes the current delta (`O(1)`, brief state lock). Pair with
    /// [`Self::finish_checkpoint`]; the returned token serializes checkpoints. Callers that need a
    /// consistent cut across several structures freeze them all while their writers are paused.
    pub fn begin_checkpoint(&self) -> PendingBitmapCheckpoint<'_> {
        let guard = self
            .checkpoint_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut st = self.write();
        let frozen = Arc::new(FrozenDelta {
            hot: core::mem::take(&mut st.hot),
            removed: core::mem::take(&mut st.removed),
        });
        st.frozen = Some(Arc::clone(&frozen));
        PendingBitmapCheckpoint {
            _guard: guard,
            frozen,
            base: st.base.clone(),
            generation: st.generation + 1,
        }
    }

    /// Writes and publishes the generation frozen by [`Self::begin_checkpoint`] (no state lock is
    /// held during I/O). Returns the number of blocks written. On error the frozen delta is
    /// folded back, so no write is lost.
    pub fn finish_checkpoint(&self, pending: PendingBitmapCheckpoint<'_>, stamp: u64) -> Result<usize> {
        let PendingBitmapCheckpoint {
            _guard,
            frozen,
            base,
            generation,
        } = pending;
        match self.write_generation(&frozen, base.as_deref(), generation, stamp) {
            Ok((new_base, written)) => {
                let old = {
                    let mut st = self.write();
                    st.frozen = None;
                    st.generation = generation;
                    st.stamp = stamp;
                    st.base.replace(Arc::new(new_base))
                };
                drop(base);
                if let Some(old) = old {
                    let old_path = old.path.clone();
                    drop(old);
                    // Best effort: a leftover generation is removed at the next open.
                    let _ = storage_types::vfs::at(&old_path).remove_file(&old_path);
                }
                Ok(written)
            }
            Err(e) => {
                self.unfreeze(&frozen);
                Err(e)
            }
        }
    }

    /// Drops every entry and publishes an empty generation tagged with `stamp` (used before a
    /// rebuild from the records).
    pub fn clear_with_stamp(&self, stamp: u64) -> Result<()> {
        let _one_at_a_time = self
            .checkpoint_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let (old, generation) = {
            let mut st = self.write();
            st.hot = Default::default();
            st.removed = Default::default();
            st.frozen = None;
            (st.base.take(), st.generation + 1)
        };
        let (new_base, _) = self.write_generation(&FrozenDelta::default(), None, generation, stamp)?;
        {
            let mut st = self.write();
            st.generation = generation;
            st.stamp = stamp;
            st.base = Some(Arc::new(new_base));
        }
        if let Some(old) = old {
            let old_path = old.path.clone();
            drop(old);
            let _ = storage_types::vfs::at(&old_path).remove_file(&old_path);
        }
        Ok(())
    }

    /// Folds `frozen` back after a failed checkpoint: `hot' = hot ∪ (frozen.hot - removed)`,
    /// `removed' = (removed ∪ frozen.removed) - hot'`, which preserves every visible set.
    fn unfreeze(&self, frozen: &FrozenDelta) {
        let mut st = self.write();
        st.frozen = None;
        for (key, mut bm) in frozen.hot.snapshot_all() {
            if let Some(r) = st.removed.get(&key) {
                bm.and_not_in_place(r);
            }
            for ord in bm.to_vec() {
                st.hot.insert(key.0, key.1, ord);
            }
        }
        for (key, fr) in &frozen.removed {
            let entry = st.removed.entry(*key).or_default();
            *entry = entry.union(fr);
        }
        for (key, hot) in st.hot.snapshot_all() {
            if let Some(r) = st.removed.get_mut(&key) {
                r.and_not_in_place(&hot);
            }
        }
    }

    fn write_generation(
        &self,
        frozen: &FrozenDelta,
        base: Option<&BaseGeneration>,
        generation: u64,
        stamp: u64,
    ) -> Result<(BaseGeneration, usize)> {
        let io = |what: &'static str| move |e: std::io::Error| StorageError::io(what, e);
        if let Some(parent) = self.path.parent() {
            storage_types::vfs::at(parent).create_dir_all(parent).map_err(io("create paged bitmap dir"))?;
        }
        let data_path = generation_path(&self.path, generation);
        let tmp_path = temp_generation_path(&self.path, generation);
        let file = storage_types::vfs::at(&tmp_path).open(&tmp_path, storage_types::vfs::OpenOpts::create_truncate())
            .map_err(io("create bitmap generation file"))?;
        let mut writer = BufWriter::with_capacity(64 * BITMAP_BLOCK_BYTES, file);
        writer
            .write_all(&[0u8; HEADER_REGION])
            .map_err(io("write bitmap header placeholder"))?;
        let settings = self
            .seal
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let mut sealer = GenerationSealer::begin(&settings.as_seal(), &data_path);

        let mut keys: Vec<Key> = base
            .map(|b| b.directory.keys().copied().collect())
            .unwrap_or_default();
        keys.extend(frozen.hot.keys());
        keys.sort_unstable();
        keys.dedup();

        let mut dir = Vec::new();
        dir.extend_from_slice(&0u32.to_le_bytes()); // tag count, patched below
        let mut tag_count = 0u32;
        let mut block_index = 0u32;
        let mut block_bytes = [0u8; BITMAP_BLOCK_BYTES];
        for key in keys {
            let base_chunks = base.map(|b| b.chunks(&key)).unwrap_or_default();
            let removed = frozen.removed.get(&key);
            let hot = frozen.hot.get_bitmap(key.0, key.1);
            let mut refs: Vec<MappedChunkRef> = Vec::new();
            let mut cardinality = 0u64;
            let mut write_err = None;
            merge_chunks(
                &base_chunks,
                removed.map_or(&[][..], |r| r.chunks_slice()),
                hot.chunks_slice(),
                |chunk_id, words| {
                    for (i, w) in words.iter().enumerate() {
                        block_bytes[i * 8..i * 8 + 8].copy_from_slice(&w.to_le_bytes());
                    }
                    if write_err.is_none() {
                        let sealed = match sealer.as_mut() {
                            Some(s) => s.seal(&mut block_bytes),
                            None => Ok(()),
                        };
                        if let Err(e) = sealed.and_then(|()| writer.write_all(&block_bytes)) {
                            write_err = Some(e);
                        }
                    }
                    cardinality += words.iter().map(|w| u64::from(w.count_ones())).sum::<u64>();
                    refs.push(MappedChunkRef { chunk_id, block_index });
                    block_index += 1;
                },
            );
            if let Some(e) = write_err {
                return Err(StorageError::io("write bitmap block", e));
            }
            if refs.is_empty() {
                continue;
            }
            tag_count += 1;
            dir.extend_from_slice(&key.0.to_le_bytes());
            dir.extend_from_slice(&key.1.to_raw_bytes());
            dir.extend_from_slice(&(refs.len() as u32).to_le_bytes());
            dir.extend_from_slice(&cardinality.to_le_bytes());
            for r in &refs {
                dir.extend_from_slice(&r.chunk_id.to_le_bytes());
                dir.extend_from_slice(&r.block_index.to_le_bytes());
            }
        }
        dir[0..4].copy_from_slice(&tag_count.to_le_bytes());
        if let Some(s) = sealer.as_mut() {
            s.seal(&mut dir).map_err(io("seal bitmap directory"))?;
        }
        writer.write_all(&dir).map_err(io("write bitmap directory"))?;
        let mut file = writer
            .into_inner()
            .map_err(|e| StorageError::io("flush bitmap generation file", e.into_error()))?;
        file.sync_all().map_err(io("fsync bitmap generation file"))?;

        // Checksums over the written regions (streamed writes cannot checksum incrementally).
        let blocks_len = block_index as usize * BITMAP_BLOCK_BYTES;
        let dir_offset = (HEADER_REGION + blocks_len) as u64;
        let blocks_crc = {
            let mapped = map_path_read_only(&tmp_path)?;
            let region = mapped
                .as_slice()
                .get(HEADER_REGION..HEADER_REGION + blocks_len)
                .ok_or_else(|| StorageError::corrupted_header("bitmap generation shorter than written"))?;
            checksum_payload(region)
        };
        let mut header = [0u8; ENC_SECTION_OFFSET + ENC_SECTION_LEN];
        header[0..4].copy_from_slice(&BITMAP_FILE_MAGIC.to_le_bytes());
        header[4..8].copy_from_slice(&block_index.to_le_bytes());
        header[8..16].copy_from_slice(&dir_offset.to_le_bytes());
        header[16..20].copy_from_slice(&(dir.len() as u32).to_le_bytes());
        header[24..32].copy_from_slice(&checksum_payload(&dir).to_le_bytes());
        header[32..40].copy_from_slice(&blocks_crc.to_le_bytes());
        let header_crc = checksum_payload(&header[0..40]);
        header[40..48].copy_from_slice(&header_crc.to_le_bytes());
        let enc = sealer.map(|s| {
            let (section, cipher) = s.finish();
            header[ENC_SECTION_OFFSET..].copy_from_slice(&section);
            let mut tag = [0u8; 16];
            tag.copy_from_slice(&section[20..36]);
            (cipher, tag)
        });
        file.seek(SeekFrom::Start(0))
            .and_then(|_| file.write_all(&header))
            .and_then(|()| file.sync_all())
            .map_err(io("finalize bitmap generation file"))?;
        drop(file);
        publish_generation(&tmp_path, &data_path)?;
        write_manifest(&self.path, BITMAP_MANIFEST_MAGIC, generation, stamp)?;

        let mmap = map_path_read_only(&data_path)?;
        let directory = load_generation(
            &mmap,
            enc.as_ref().map(|(cipher, tag)| (cipher, tag, data_path.as_path())),
        )?;
        Ok((
            BaseGeneration {
                mmap,
                directory,
                path: data_path,
                cipher: enc.map(|(cipher, _)| cipher),
            },
            block_index as usize,
        ))
    }
}

/// Streams `(base - removed) ∪ hot` chunk by chunk in ascending chunk id, calling `emit` for every
/// non-empty result chunk. All inputs are sorted by chunk id.
fn merge_chunks(
    base: &[(u32, Words)],
    removed: &[(u32, Words)],
    hot: &[(u32, Words)],
    mut emit: impl FnMut(u32, &Words),
) {
    let (mut bi, mut ri, mut hi) = (0usize, 0usize, 0usize);
    loop {
        let next = [base.get(bi), hot.get(hi)]
            .into_iter()
            .flatten()
            .map(|c| c.0)
            .min();
        let Some(chunk_id) = next else {
            return;
        };
        let mut words = [0u64; 64];
        if base.get(bi).is_some_and(|c| c.0 == chunk_id) {
            words = base[bi].1;
            bi += 1;
            while removed.get(ri).is_some_and(|c| c.0 < chunk_id) {
                ri += 1;
            }
            if removed.get(ri).is_some_and(|c| c.0 == chunk_id) {
                for (w, r) in words.iter_mut().zip(removed[ri].1.iter()) {
                    *w &= !r;
                }
            }
        }
        if hot.get(hi).is_some_and(|c| c.0 == chunk_id) {
            for (w, h) in words.iter_mut().zip(hot[hi].1.iter()) {
                *w |= h;
            }
            hi += 1;
        }
        if words.iter().any(|&w| w != 0) {
            emit(chunk_id, &words);
        }
    }
}

/// Validates a generation file once at open (`RULE-SAFE-04`): header checksum, directory and
/// block region bounds and checksums, chunk refs inside the block region, strictly increasing
/// chunk ids, unique keys. Allocations are bounded by the bytes present.
///
/// For an encrypted generation (`enc` = cipher, tag, file path) the tag is verified over the
/// block region and the directory, and the directory is decrypted into a copy for parsing.
fn load_generation(
    mmap: &MmapRegion,
    enc: Option<(&PageCipher, &[u8; 16], &Path)>,
) -> Result<FastHashMap<Key, DirEntry>> {
    let corrupt = StorageError::corrupted_header;
    let slice = mmap.as_slice();
    if slice.len() < HEADER_REGION {
        return Err(corrupt("bitmap generation truncated"));
    }
    if read_u32_le(slice, 0) != BITMAP_FILE_MAGIC {
        return Err(corrupt("bitmap generation invalid magic or unsupported format version"));
    }
    let expected_header_crc = read_u64_le(slice, 40);
    let actual_header_crc = checksum_payload(&slice[0..40]);
    if expected_header_crc != actual_header_crc {
        return Err(StorageError::checksum_mismatch(expected_header_crc, actual_header_crc));
    }
    let block_count = read_u32_le(slice, 4) as usize;
    let dir_offset = usize::try_from(read_u64_le(slice, 8)).map_err(|_| corrupt("bitmap dir offset"))?;
    let dir_len = read_u32_le(slice, 16) as usize;
    let blocks_end = block_count
        .checked_mul(BITMAP_BLOCK_BYTES)
        .and_then(|len| HEADER_REGION.checked_add(len))
        .ok_or_else(|| corrupt("bitmap block region overflow"))?;
    if blocks_end != dir_offset {
        return Err(corrupt("bitmap directory does not follow the block region"));
    }
    let blocks = slice
        .get(HEADER_REGION..blocks_end)
        .ok_or_else(|| corrupt("bitmap block region out of bounds"))?;
    let dir = dir_offset
        .checked_add(dir_len)
        .and_then(|end| slice.get(dir_offset..end))
        .ok_or_else(|| corrupt("bitmap directory out of bounds"))?;
    for (expected, region) in [(read_u64_le(slice, 24), dir), (read_u64_le(slice, 32), blocks)] {
        let actual = checksum_payload(region);
        if actual != expected {
            return Err(StorageError::checksum_mismatch(expected, actual));
        }
    }
    let dir_plain;
    let dir: &[u8] = match enc {
        Some((cipher, tag, path)) => {
            let mut verifier = cipher.verifier(path);
            verifier.absorb_ciphertext(blocks)?;
            verifier.absorb_ciphertext(dir)?;
            verifier.verify(tag)?;
            let mut copy = Vec::new();
            copy.try_reserve_exact(dir.len())
                .map_err(|_| StorageError::resource_exhausted("bitmap directory memory"))?;
            copy.extend_from_slice(dir);
            cipher.decrypt_at(dir_offset - HEADER_REGION, &mut copy)?;
            dir_plain = copy;
            &dir_plain
        }
        None => dir,
    };

    let mut out = FastHashMap::default();
    if dir.len() < 4 {
        return Err(corrupt("bitmap directory truncated"));
    }
    let tag_count = read_u32_le(dir, 0) as usize;
    let mut off = 4usize;
    for _ in 0..tag_count {
        let fixed = dir
            .get(off..off + DIR_ENTRY_FIXED)
            .ok_or_else(|| corrupt("bitmap tag entry truncated"))?;
        let field = read_u16_le(fixed, 0);
        let tag = InlineId::from_raw_bytes(read_array_24(fixed, 2))?;
        let chunk_count = read_u32_le(fixed, 26) as usize;
        let cardinality = read_u64_le(fixed, 30);
        off += DIR_ENTRY_FIXED;
        let refs_bytes = chunk_count
            .checked_mul(8)
            .and_then(|len| off.checked_add(len))
            .and_then(|end| dir.get(off..end))
            .ok_or_else(|| corrupt("bitmap chunk refs truncated"))?;
        let mut chunks = Vec::with_capacity(chunk_count);
        let mut prev: Option<u32> = None;
        for raw in refs_bytes.chunks_exact(8) {
            let chunk_id = read_u32_le(raw, 0);
            let block_index = read_u32_le(raw, 4);
            if block_index as usize >= block_count {
                return Err(corrupt("bitmap chunk ref points past the last block"));
            }
            if prev.is_some_and(|p| chunk_id <= p) {
                return Err(corrupt("bitmap chunk ids are not strictly increasing"));
            }
            prev = Some(chunk_id);
            chunks.push(MappedChunkRef { chunk_id, block_index });
        }
        off += refs_bytes.len();
        if out.insert((field, tag), DirEntry { chunks, cardinality }).is_some() {
            return Err(corrupt("bitmap duplicate (field, tag) directory entry"));
        }
    }
    if off != dir.len() {
        return Err(corrupt("trailing bytes after bitmap directory"));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sealed_generations_hide_tags_and_verify() {
        use storage_codec::{EncryptionKey, KeyRing};
        let path = temp_path("sealed");
        let ring = KeyRing::new().with_key(2, EncryptionKey::from_bytes([2; 32]));
        let seal = FileSeal { keys: Some(&ring), active_key: Some(2), accept_plain: false };
        let tag = InlineId::new("very-secret-tag").unwrap();
        {
            let idx = PagedBitmapIndex::open_sealed(&path, &seal).unwrap();
            for i in (0..20_000u32).step_by(3) {
                idx.insert(1, tag, RecordOrdinal(i));
            }
            idx.checkpoint_with_stamp(9).unwrap();
        }
        let generation = fs::read_dir(path.parent().unwrap())
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .find(|p| p.to_string_lossy().contains(".idx."))
            .unwrap();
        let bytes = fs::read(&generation).unwrap();
        assert_eq!(&bytes[128..132], b"PENC");
        assert!(!bytes.windows(15).any(|w| w == b"very-secret-tag"), "tag text on disk");

        let idx = PagedBitmapIndex::open_sealed(&path, &seal).unwrap();
        assert_eq!(idx.get_bitmap(1, tag).len(), 6667);
        // Word-level probes decrypt single words of the encrypted base.
        for ord in [0u32, 3, 4, 4095, 4096, 4098, 19_998, 19_999] {
            assert_eq!(idx.matches_ordinal(RecordOrdinal(ord), &[(1, tag)], &[], &[]), ord % 3 == 0, "ord {ord}");
        }
        idx.remove(1, tag, RecordOrdinal(3));
        assert_eq!(idx.get_bitmap(1, tag).len(), 6666);
        drop(idx);
        assert!(PagedBitmapIndex::open(&path).is_err(), "no key: refused");

        // Any flipped bit in the block region or directory fails the tag.
        for at in [4096 + 7, bytes.len() - 3] {
            let mut bad = bytes.clone();
            bad[at] ^= 0x04;
            fs::write(&generation, &bad).unwrap();
            assert!(PagedBitmapIndex::open_sealed(&path, &seal).is_err(), "tamper at {at}");
        }
        let _ = fs::remove_dir_all(path.parent().unwrap());
    }

    fn temp_path(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("storine_paged_bitmap_{name}_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir.join("bitmap.idx")
    }

    #[test]
    fn test_paged_bitmap_index_checkpoint_and_mmap_intersection() -> Result<()> {
        let path = temp_path("basic");
        let tag_a = InlineId::new("tag_alpha")?;
        let tag_b = InlineId::new("tag_beta")?;
        let idx = PagedBitmapIndex::open(&path)?;
        for i in (0..8_000u32).step_by(2) {
            idx.insert(1, tag_a, RecordOrdinal(i));
        }
        for i in (0..8_000u32).step_by(3) {
            idx.insert(1, tag_b, RecordOrdinal(i));
        }
        let blocks = idx.checkpoint_with_stamp(3)?;
        assert!(blocks >= 4);
        let reopened = PagedBitmapIndex::open(&path)?;
        assert_eq!(reopened.stamp(), 3);
        let both = reopened.intersect_all(&[(1, tag_a), (1, tag_b)]);
        assert_eq!(both.len(), 1334); // multiples of 6 in 0..8000
        Ok(())
    }

    /// Every query matches the in-memory `BitmapTagIndex` across random writes, checkpoints
    /// and a reopen.
    #[test]
    fn test_paged_bitmap_matches_in_memory_index_through_checkpoints() -> Result<()> {
        use storage_types::testing::SweepRng;
        let path = temp_path("parity");
        let paged = PagedBitmapIndex::open(&path)?;
        let mem = BitmapTagIndex::new();
        let tags: Vec<InlineId> = ["red", "green", "greenish", "blue", "black"]
            .iter()
            .map(|t| InlineId::new(t).unwrap())
            .collect();
        let mut rng = SweepRng::new(9);
        let check = |paged: &PagedBitmapIndex, rng: &mut SweepRng| {
            for field in 1..=2u16 {
                for &t in &tags {
                    assert_eq!(paged.get_bitmap(field, t).to_vec(), mem.get_bitmap(field, t).to_vec());
                    assert!(paged.cardinality(field, t) >= mem.cardinality(field, t));
                    if mem.cardinality(field, t) > 0 {
                        assert!(paged.cardinality(field, t) > 0);
                    }
                }
                let a = tags[rng.below(tags.len())];
                let b = tags[rng.below(tags.len())];
                assert_eq!(paged.intersect_all(&[(field, a), (field, b)]).to_vec(), mem.intersect_all(&[(field, a), (field, b)]).to_vec());
                assert_eq!(paged.union_tags(field, &[a, b]).to_vec(), mem.union_tags(field, &[a, b]).to_vec());
                assert_eq!(paged.union_tag_prefix(field, "gre").to_vec(), mem.union_tag_prefix(field, "gre").to_vec());
                assert_eq!(paged.all_on_field_bitmap(field).to_vec(), mem.all_on_field_bitmap(field).to_vec());
                let mut t1 = mem.all_on_field_bitmap(field);
                let mut t2 = t1.clone();
                paged.exclude_from_bitmap(&[(field, a)], &mut t1);
                mem.exclude_from_bitmap(&[(field, a)], &mut t2);
                assert_eq!(t1.to_vec(), t2.to_vec());
                let mut c1: Vec<RecordOrdinal> = (0..9_000).step_by(7).map(RecordOrdinal).collect();
                let mut c2 = c1.clone();
                paged.filter_in_place(&[(field, b)], &mut c1);
                mem.filter_in_place(&[(field, b)], &mut c2);
                assert_eq!(c1, c2);
                let group = [a, b];
                for o in (0..9_000u32).step_by(13).map(RecordOrdinal) {
                    assert_eq!(
                        paged.matches_ordinal(o, &[(field, a)], &[(field, b)], &[(field, &group)]),
                        mem.matches_ordinal(o, &[(field, a)], &[(field, b)], &[(field, &group)])
                    );
                }
            }
        };
        for round in 0..6u64 {
            for _ in 0..3_000 {
                let field = 1 + rng.below(2) as u16;
                let tag = tags[rng.below(tags.len())];
                let ord = RecordOrdinal(rng.below(9_000) as u32);
                if rng.below(3) == 0 {
                    paged.remove(field, tag, ord);
                    mem.remove(field, tag, ord);
                } else {
                    paged.insert(field, tag, ord);
                    mem.insert(field, tag, ord);
                }
            }
            check(&paged, &mut rng);
            if round % 2 == 1 {
                paged.checkpoint_with_stamp(round)?;
                check(&paged, &mut rng);
            }
        }
        drop(paged);
        let reopened = PagedBitmapIndex::open(&path)?;
        check(&reopened, &mut rng);
        Ok(())
    }
}
