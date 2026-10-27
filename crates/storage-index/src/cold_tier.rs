//! Disk-tiered primary index storage (`TASK-092`, `F-PIDX-03`, `RULE-MEM-03`).
//!
//! A cold tier is one immutable generation file holding the primary index as of a checkpoint:
//!
//! - header page (4 KiB): `magic | version | entry_count u64 | page_count u32 | ordinal_slots u32 |
//!   ords_off u64 | sep_off u64 | bloom_off u64 | bloom_blocks u64 | ords_crc u64 | sep_crc u64 |
//!   bloom_crc u64 | header_crc u64`, plus the encryption section of `paged_files`;
//! - entry pages (4 KiB each): `count u16 | pad [6] | body_crc u64 | v3 entries... | slot offsets`,
//!   entries sorted by `(id.fast_hash(), id)` across all pages; the `u16` start offset of entry `i`
//!   sits at `PAGE - 2 * (i + 1)`, so a lookup binary-searches a page decoding only ~6 ids;
//! - the ordinal table: `ordinal_slots` x 24-byte raw ids (all-zero = free), padded to 64 bytes;
//! - the page separators (minimum hash per page), padded to 64 bytes;
//! - the split-block Bloom filter.
//!
//! RAM per tier: separators (`8 B` per page) + Bloom filter (~`10 bits` per entry). A lookup is
//! one Bloom cache line, a binary search over separators, and one page decode (decrypting the
//! page into a stack buffer when sealed). Everything after the header page is one sealed region
//! when encryption is on (`paged_files::GenerationSealer`), verified once at open.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use storage_codec::{checksum_payload, FileSeal};
use storage_mmap::MmapRegion;
use storage_types::wire::{read_u16_le, read_u32_le, read_u64_le};
use storage_types::{InlineId, Result, StorageError};

use crate::checkpoint::{decode_v3_entry, encode_v3_entry};
use crate::cold_primary::SplitBlockBloomFilter;
use crate::paged_files::{
    map_path_read_only, open_generation_cipher, publish_generation, GenerationSealer, PageCipher,
    ENC_SECTION_LEN, ENC_SECTION_OFFSET,
};
use crate::primary::PrimaryEntry;

const PAGE: usize = 4096;
const PAGE_HEADER: usize = 16;
const TIER_MAGIC: u32 = 0x5443_5031; // "1PCT"
const TIER_VERSION: u32 = 1;
const ORD_SLOT: usize = 24;

/// One immutable cold generation (see the module docs).
pub(crate) struct ColdTier {
    mmap: MmapRegion,
    path: PathBuf,
    cipher: Option<PageCipher>,
    /// Minimum `fast_hash` of each page, ascending.
    separators: Vec<u64>,
    bloom: SplitBlockBloomFilter,
    entry_count: u64,
    ordinal_slots: u32,
    ords_off: usize,
}

impl core::fmt::Debug for ColdTier {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ColdTier")
            .field("path", &self.path)
            .field("entries", &self.entry_count)
            .field("pages", &self.separators.len())
            .finish_non_exhaustive()
    }
}

#[inline]
fn order_key(id: InlineId) -> (u64, InlineId) {
    (id.fast_hash(), id)
}

/// Start offset of entry `i` in a page (from the slot-offset array at the page end).
#[inline]
fn slot_offset(page: &[u8; PAGE], i: usize) -> usize {
    read_u16_le(page, PAGE - 2 * (i + 1)) as usize
}

#[inline]
fn pad64(n: usize) -> usize {
    n.div_ceil(64) * 64
}

/// Writes one region piece (encrypting it first when the generation is sealed).
type Emit<'a> = dyn FnMut(&mut BufWriter<File>, &mut [u8]) -> std::io::Result<()> + 'a;

/// Finalizes (count, checksum) and emits a full or final entry page, then resets it.
fn flush_page(
    w: &mut BufWriter<File>,
    page: &mut [u8; PAGE],
    count: &mut u16,
    used: &mut usize,
    emit: &mut Emit<'_>,
) -> std::io::Result<()> {
    page[0..2].copy_from_slice(&count.to_le_bytes());
    let crc = checksum_payload(&page[PAGE_HEADER..]);
    page[8..16].copy_from_slice(&crc.to_le_bytes());
    emit(w, page)?;
    page.fill(0);
    *count = 0;
    *used = PAGE_HEADER;
    Ok(())
}

impl ColdTier {
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Entries stored in this generation.
    #[inline]
    pub(crate) fn entry_count(&self) -> u64 {
        self.entry_count
    }

    /// Ordinal slots stored in this generation.
    #[inline]
    pub(crate) fn ordinal_slots(&self) -> u32 {
        self.ordinal_slots
    }

    /// RAM held by this tier (separators + Bloom filter).
    pub(crate) fn ram_bytes(&self) -> usize {
        self.separators.len() * 8 + self.bloom.memory_bytes()
    }

    /// Copies (and, when sealed, decrypts) page `page` into `buf`.
    #[inline]
    fn page_into(&self, page: usize, buf: &mut [u8; PAGE]) -> Option<()> {
        let off = PAGE * (page + 1);
        buf.copy_from_slice(self.mmap.as_slice().get(off..off + PAGE)?);
        if let Some(ref cipher) = self.cipher {
            cipher.decrypt_at(off - PAGE, buf).ok()?;
        }
        Some(())
    }

    /// The stored entry of `id` (`disk_verified` is `false`: the engine re-validates frames).
    pub(crate) fn get(&self, id: InlineId) -> Option<PrimaryEntry> {
        if !self.bloom.may_contain(id) {
            return None;
        }
        let key = order_key(id);
        // Equal hashes may straddle a page boundary: start at the last page whose minimum is
        // strictly below the hash.
        let first = self.separators.partition_point(|&m| m < key.0).saturating_sub(1);
        let mut buf = [0u8; PAGE];
        for (page, &min) in self.separators.iter().enumerate().skip(first) {
            if min > key.0 {
                break;
            }
            self.page_into(page, &mut buf)?;
            let count = read_u16_le(&buf, 0) as usize;
            // Binary search (lower bound) over the slot offsets, decoding only ids.
            let (mut lo, mut hi) = (0usize, count);
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                let mut cursor = slot_offset(&buf, mid);
                let mid_id = InlineId::read_compact_from(&buf, &mut cursor).ok()?;
                if order_key(mid_id) < key {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            if lo == count {
                continue; // every entry of this page sorts before `id`: try the next page
            }
            let mut cursor = slot_offset(&buf, lo);
            let entry = decode_v3_entry(&buf, &mut cursor).ok()?;
            return (entry.id == id).then_some(entry);
        }
        None
    }

    /// `true` if `id` is stored in this generation.
    #[inline]
    pub(crate) fn contains(&self, id: InlineId) -> bool {
        self.get(id).is_some()
    }

    /// The id stored at ordinal `ord` (`None` for a free or out-of-range slot).
    #[inline]
    pub(crate) fn resolve(&self, ord: u32) -> Option<InlineId> {
        if ord >= self.ordinal_slots {
            return None;
        }
        let off = self.ords_off + ord as usize * ORD_SLOT;
        let mut raw = [0u8; ORD_SLOT];
        raw.copy_from_slice(self.mmap.as_slice().get(off..off + ORD_SLOT)?);
        if let Some(ref cipher) = self.cipher {
            cipher.decrypt_at(off - PAGE, &mut raw).ok()?;
        }
        if raw[0] == 0 {
            return None;
        }
        InlineId::from_raw_bytes(raw).ok()
    }

    /// Stored entries in `(hash, id)` order, decoded lazily page by page (`O(page)` memory).
    pub(crate) fn iter(&self) -> TierIter<'_> {
        TierIter { tier: self, next_page: 0, buf: [0u8; PAGE], cursor: 0, remaining: 0 }
    }

    /// Calls `f` for every stored entry in `(hash, id)` order (`O(page)` memory).
    pub(crate) fn for_each_entry(&self, mut f: impl FnMut(PrimaryEntry)) {
        let mut buf = [0u8; PAGE];
        for page in 0..self.separators.len() {
            if self.page_into(page, &mut buf).is_none() {
                return;
            }
            let count = read_u16_le(&buf, 0) as usize;
            let mut cursor = PAGE_HEADER;
            for _ in 0..count {
                match decode_v3_entry(&buf, &mut cursor) {
                    Ok(entry) => f(entry),
                    Err(_) => return,
                }
            }
        }
    }

    /// Opens and fully validates (`RULE-SAFE-04`) the generation at `data_path`: header and
    /// section checksums, every page checksum, entry decoding and global order, every ordinal
    /// slot, and, when sealed, the region's authentication tag.
    pub(crate) fn open(data_path: &Path, seal: &FileSeal<'_>) -> Result<Self> {
        let corrupt = StorageError::corrupted_header;
        let mmap = map_path_read_only(data_path)?;
        let slice = mmap.as_slice();
        if slice.len() < PAGE {
            return Err(corrupt("cold tier truncated"));
        }
        if read_u32_le(slice, 0) != TIER_MAGIC || read_u32_le(slice, 4) != TIER_VERSION {
            return Err(corrupt("cold tier invalid magic or version"));
        }
        if checksum_payload(&slice[0..88]) != read_u64_le(slice, 88) {
            return Err(corrupt("cold tier header checksum mismatch"));
        }
        let entry_count = read_u64_le(slice, 8);
        let page_count = read_u32_le(slice, 16) as usize;
        let ordinal_slots = read_u32_le(slice, 20);
        let to_usize = |v: u64| usize::try_from(v).map_err(|_| corrupt("cold tier offset overflow"));
        let ords_off = to_usize(read_u64_le(slice, 24))?;
        let sep_off = to_usize(read_u64_le(slice, 32))?;
        let bloom_off = to_usize(read_u64_le(slice, 40))?;
        let bloom_blocks = to_usize(read_u64_le(slice, 48))?;
        let ords_len = pad64(ordinal_slots as usize * ORD_SLOT);
        let sep_len = pad64(page_count * 8);
        let bloom_len = bloom_blocks.checked_mul(64).ok_or_else(|| corrupt("cold tier bloom size"))?;
        let layout_ok = page_count
            .checked_add(1)
            .and_then(|p| p.checked_mul(PAGE))
            .is_some_and(|end| end == ords_off)
            && ords_off.checked_add(ords_len) == Some(sep_off)
            && sep_off.checked_add(sep_len) == Some(bloom_off)
            && bloom_off.checked_add(bloom_len) == Some(slice.len())
            && bloom_blocks.is_power_of_two();
        if !layout_ok {
            return Err(corrupt("cold tier section layout invalid"));
        }
        let ords = &slice[ords_off..sep_off];
        let seps = &slice[sep_off..bloom_off];
        let bloom_bytes = &slice[bloom_off..];
        for (at, region) in [(56, ords), (64, seps), (72, bloom_bytes)] {
            let (expected, actual) = (read_u64_le(slice, at), checksum_payload(region));
            if expected != actual {
                return Err(StorageError::checksum_mismatch(expected, actual));
            }
        }

        let enc = open_generation_cipher(&slice[..PAGE], seal)?;
        if let Some((ref cipher, ref tag)) = enc {
            let mut verifier = cipher.verifier(data_path);
            verifier.absorb_ciphertext(&slice[PAGE..])?;
            verifier.verify(tag)?;
        }
        let cipher = enc.map(|(cipher, _)| cipher);
        let decrypt = |at: usize, bytes: &[u8]| -> Result<Vec<u8>> {
            let mut out = Vec::new();
            out.try_reserve_exact(bytes.len())
                .map_err(|_| StorageError::resource_exhausted("cold tier directory memory"))?;
            out.extend_from_slice(bytes);
            if let Some(ref c) = cipher {
                c.decrypt_at(at - PAGE, &mut out)?;
            }
            Ok(out)
        };
        let seps_plain = decrypt(sep_off, seps)?;
        let separators: Vec<u64> = (0..page_count).map(|i| read_u64_le(&seps_plain, i * 8)).collect();
        let bloom = SplitBlockBloomFilter::from_bytes(&decrypt(bloom_off, bloom_bytes)?)?;

        let tier = Self {
            mmap,
            path: data_path.to_path_buf(),
            cipher,
            separators,
            bloom,
            entry_count,
            ordinal_slots,
            ords_off,
        };
        tier.validate_contents()?;
        Ok(tier)
    }

    /// Checks every page and ordinal slot once at open.
    fn validate_contents(&self) -> Result<()> {
        let corrupt = StorageError::corrupted_header;
        let mut buf = [0u8; PAGE];
        let mut prev: Option<(u64, InlineId)> = None;
        let mut seen = 0u64;
        for page in 0..self.separators.len() {
            self.page_into(page, &mut buf).ok_or_else(|| corrupt("cold tier page unreadable"))?;
            if checksum_payload(&buf[PAGE_HEADER..]) != read_u64_le(&buf, 8) {
                return Err(corrupt("cold tier page checksum mismatch"));
            }
            let count = read_u16_le(&buf, 0) as usize;
            if count == 0 {
                return Err(corrupt("cold tier empty page"));
            }
            let mut cursor = PAGE_HEADER;
            for i in 0..count {
                if slot_offset(&buf, i) != cursor {
                    return Err(corrupt("cold tier slot offset does not match its entry"));
                }
                let entry = decode_v3_entry(&buf, &mut cursor)?;
                if cursor > PAGE - 2 * count {
                    return Err(corrupt("cold tier entries overlap the slot offsets"));
                }
                let key = order_key(entry.id);
                if i == 0 && key.0 != self.separators[page] {
                    return Err(corrupt("cold tier separator does not match its page"));
                }
                if prev.is_some_and(|p| p >= key) {
                    return Err(corrupt("cold tier entries out of order"));
                }
                if entry.ordinal.0 >= self.ordinal_slots || self.resolve(entry.ordinal.0) != Some(entry.id) {
                    return Err(corrupt("cold tier entry ordinal does not match the ordinal table"));
                }
                prev = Some(key);
                seen += 1;
            }
        }
        if seen != self.entry_count {
            return Err(corrupt("cold tier entry count mismatch"));
        }
        // Every non-free ordinal slot holds a valid id (and therefore a stored entry, by the
        // per-entry check above and the equal counts below).
        let mut live = 0u64;
        for ord in 0..self.ordinal_slots {
            let off = self.ords_off + ord as usize * ORD_SLOT;
            let raw = self.mmap.as_slice().get(off..off + ORD_SLOT).ok_or_else(|| corrupt("ordinal slot"))?;
            let mut slot = [0u8; ORD_SLOT];
            slot.copy_from_slice(raw);
            if let Some(ref cipher) = self.cipher {
                cipher.decrypt_at(off - PAGE, &mut slot)?;
            }
            if slot.iter().any(|&b| b != 0) {
                InlineId::from_raw_bytes(slot)?;
                live += 1;
            }
        }
        if live != self.entry_count {
            return Err(corrupt("cold tier ordinal table does not match its entries"));
        }
        Ok(())
    }

    /// Streams a new generation to `tmp_path`, publishes it at `data_path` and opens it.
    ///
    /// `entries` must yield entries in strictly increasing `(fast_hash, id)` order, each with a
    /// valid ordinal below `ordinal_slots`; `resolve(ord)` gives the id stored at each ordinal
    /// slot (it must agree with `entries`). Memory: `O(page + Bloom filter)`.
    pub(crate) fn write(
        data_path: &Path,
        tmp_path: &Path,
        seal: &FileSeal<'_>,
        entries: impl Iterator<Item = PrimaryEntry>,
        expected_entries: usize,
        ordinal_slots: u32,
        resolve: impl Fn(u32) -> Option<InlineId>,
    ) -> Result<Self> {
        let io = |what: &'static str| move |e: std::io::Error| StorageError::io(what, e);
        if let Some(parent) = data_path.parent() {
            std::fs::create_dir_all(parent).map_err(io("create cold tier dir"))?;
        }
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(tmp_path)
            .map_err(io("create cold tier generation"))?;
        let mut w = BufWriter::with_capacity(16 * PAGE, file);
        w.write_all(&[0u8; PAGE]).map_err(io("write cold tier header placeholder"))?;
        let mut sealer = GenerationSealer::begin(seal, data_path);
        let mut emit = |w: &mut BufWriter<File>, piece: &mut [u8]| -> std::io::Result<()> {
            if let Some(s) = sealer.as_mut() {
                s.seal(piece)?;
            }
            w.write_all(piece)
        };

        // Entry pages.
        let mut bloom = SplitBlockBloomFilter::with_capacity(expected_entries.max(64));
        let mut separators: Vec<u64> = Vec::new();
        let mut page = [0u8; PAGE];
        let mut used = PAGE_HEADER;
        let mut count = 0u16;
        let mut entry_count = 0u64;
        let mut scratch = Vec::with_capacity(64);
        let mut prev: Option<(u64, InlineId)> = None;
        for entry in entries {
            let key = order_key(entry.id);
            if prev.is_some_and(|p| p >= key) || entry.ordinal.0 >= ordinal_slots {
                return Err(StorageError::invalid_archive("cold tier input out of order or ordinal out of range"));
            }
            prev = Some(key);
            scratch.clear();
            encode_v3_entry(&mut scratch, &entry);
            if used + scratch.len() + 2 * (count as usize + 1) > PAGE {
                flush_page(&mut w, &mut page, &mut count, &mut used, &mut emit).map_err(io("write cold tier page"))?;
            }
            if count == 0 {
                separators.push(key.0);
            }
            page[used..used + scratch.len()].copy_from_slice(&scratch);
            let slot = PAGE - 2 * (count as usize + 1);
            page[slot..slot + 2].copy_from_slice(&(used as u16).to_le_bytes());
            used += scratch.len();
            count += 1;
            entry_count += 1;
            bloom.insert(entry.id);
        }
        if count > 0 {
            flush_page(&mut w, &mut page, &mut count, &mut used, &mut emit).map_err(io("write cold tier page"))?;
        }
        let page_count = u32::try_from(separators.len())
            .map_err(|_| StorageError::invalid_archive("cold tier has too many pages"))?;

        // Ordinal table, in 64-byte-multiple pieces.
        let ords_off = PAGE * (separators.len() + 1);
        let ords_len = pad64(ordinal_slots as usize * ORD_SLOT);
        let mut chunk = Vec::with_capacity(ORD_SLOT * 512);
        let mut live = 0u64;
        for ord in 0..ordinal_slots {
            match resolve(ord) {
                Some(id) => {
                    chunk.extend_from_slice(&id.to_raw_bytes());
                    live += 1;
                }
                None => chunk.extend_from_slice(&[0u8; ORD_SLOT]),
            }
            if chunk.len() == ORD_SLOT * 512 {
                emit(&mut w, &mut chunk).map_err(io("write cold tier ordinals"))?;
                chunk.clear();
            }
        }
        chunk.resize(pad64(chunk.len()), 0);
        if !chunk.is_empty() {
            emit(&mut w, &mut chunk).map_err(io("write cold tier ordinals"))?;
        }
        if live != entry_count {
            return Err(StorageError::invalid_archive("cold tier ordinal table does not match its entries"));
        }

        // Separators and Bloom filter.
        let sep_off = ords_off + ords_len;
        let mut seps: Vec<u8> = separators.iter().flat_map(|m| m.to_le_bytes()).collect();
        seps.resize(pad64(seps.len()), 0);
        if !seps.is_empty() {
            emit(&mut w, &mut seps).map_err(io("write cold tier separators"))?;
        }
        let bloom_off = sep_off + seps.len();
        let mut bloom_bytes = bloom.to_bytes();
        let bloom_blocks = bloom_bytes.len() / 64;
        emit(&mut w, &mut bloom_bytes).map_err(io("write cold tier bloom"))?;

        let mut file = w
            .into_inner()
            .map_err(|e| StorageError::io("flush cold tier generation", e.into_error()))?;
        file.sync_all().map_err(io("fsync cold tier generation"))?;

        // Section checksums over the bytes as written (ciphertext when sealed).
        let (ords_crc, sep_crc, bloom_crc) = {
            let mapped = map_path_read_only(tmp_path)?;
            let s = mapped.as_slice();
            let get = |a: usize, b: usize| {
                s.get(a..b)
                    .map(checksum_payload)
                    .ok_or_else(|| StorageError::corrupted_header("cold tier shorter than written"))
            };
            (get(ords_off, sep_off)?, get(sep_off, bloom_off)?, get(bloom_off, s.len())?)
        };
        let mut header = [0u8; ENC_SECTION_OFFSET + ENC_SECTION_LEN];
        header[0..4].copy_from_slice(&TIER_MAGIC.to_le_bytes());
        header[4..8].copy_from_slice(&TIER_VERSION.to_le_bytes());
        header[8..16].copy_from_slice(&entry_count.to_le_bytes());
        header[16..20].copy_from_slice(&page_count.to_le_bytes());
        header[20..24].copy_from_slice(&ordinal_slots.to_le_bytes());
        header[24..32].copy_from_slice(&(ords_off as u64).to_le_bytes());
        header[32..40].copy_from_slice(&(sep_off as u64).to_le_bytes());
        header[40..48].copy_from_slice(&(bloom_off as u64).to_le_bytes());
        header[48..56].copy_from_slice(&(bloom_blocks as u64).to_le_bytes());
        header[56..64].copy_from_slice(&ords_crc.to_le_bytes());
        header[64..72].copy_from_slice(&sep_crc.to_le_bytes());
        header[72..80].copy_from_slice(&bloom_crc.to_le_bytes());
        let header_crc = checksum_payload(&header[0..88]);
        header[88..96].copy_from_slice(&header_crc.to_le_bytes());
        if let Some(s) = sealer {
            let (section, _cipher) = s.finish();
            header[ENC_SECTION_OFFSET..].copy_from_slice(&section);
        }
        file.seek(SeekFrom::Start(0))
            .and_then(|_| file.write_all(&header))
            .and_then(|()| file.sync_all())
            .map_err(io("finalize cold tier generation"))?;
        drop(file);
        publish_generation(tmp_path, data_path)?;
        Self::open(data_path, seal)
    }
}

/// Lazy ordered iterator over a tier's entries (see [`ColdTier::iter`]).
pub(crate) struct TierIter<'a> {
    tier: &'a ColdTier,
    next_page: usize,
    buf: [u8; PAGE],
    cursor: usize,
    remaining: usize,
}

impl Iterator for TierIter<'_> {
    type Item = PrimaryEntry;

    fn next(&mut self) -> Option<PrimaryEntry> {
        while self.remaining == 0 {
            if self.next_page >= self.tier.separators.len() {
                return None;
            }
            self.tier.page_into(self.next_page, &mut self.buf)?;
            self.next_page += 1;
            self.remaining = read_u16_le(&self.buf, 0) as usize;
            self.cursor = PAGE_HEADER;
        }
        self.remaining -= 1;
        // The tier was validated at open; a decode failure ends the iteration.
        decode_v3_entry(&self.buf, &mut self.cursor).ok()
    }
}
