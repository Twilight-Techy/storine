//! Segment files: immutable, memory-mapped, one or more row groups of column chunks.
//!
//! ```text
//! header (64 B): magic "STGECOL1" | u32 version | u32 flags (1 = sealed) | u64 rows
//!                | u64 footer offset | u32 footer len | u32 row-group rows | u64 footer checksum
//!                | u64 stamp | u64 header checksum (of bytes 0..56)
//! chunks:        each starting on a 64-byte boundary
//! id index:      n x u64 id hash (ascending) | n x u32 check hash | n x u32 row
//!                (rows of an id, for deletes: the two hashes make a 96-bit fingerprint)
//! footer:        u32 columns | columns x (u32 len, name, u8 flags (1 = hidden))
//!                | u32 groups | groups x (u32 rows | columns x
//!                    (u64 offset | u32 len | u64 checksum | u32 nulls | zone))
//!                | u64 index offset | u32 index len | u64 index checksum
//! ```
//!
//! Columns 0 and 1 are hidden: `$key` (document key, text) and `$version` (u64). A chunk with
//! `len = 0` holds no value (the column does not occur in that row group).
//!
//! With encryption at rest every chunk, the id index and the footer are sealed separately
//! (`FileSeal`, context = the caller's segment name, position = file offset). Plaintext chunks
//! are read straight from the mapping; each unit's checksum is verified the first time it is
//! read and the result is cached (`RULE-SAFE-04`).

use std::borrow::Cow;
#[cfg(test)]
use std::fs;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

use storage_codec::sealed_file::FileSeal;
use storage_codec::checksum_payload;
use storage_mmap::MmapRegion;
use storage_types::{FastHashMap, Result, StorageError};

use crate::bits::{corrupt, put_u32, put_u64, Reader};
use crate::column::{encode_chunk, Chunk, ChunkMeta, Val, Zone};

pub const SEGMENT_MAGIC: &[u8; 8] = b"STGECOL1";
pub const SEGMENT_VERSION: u32 = 1;
const HEADER_LEN: usize = 64;
const FLAG_SEALED: u32 = 1;
/// Default rows per row group.
pub const ROW_GROUP_ROWS: usize = 16_384;
/// Index of the hidden key column.
pub const KEY_COLUMN: usize = 0;
/// Index of the hidden version column.
pub const VERSION_COLUMN: usize = 1;
pub const KEY_NAME: &str = "$key";
pub const VERSION_NAME: &str = "$version";
/// Largest row group accepted on read (a hostile footer cannot request huge buffers).
const MAX_ROW_GROUP_ROWS: usize = 1 << 20;
const MAX_COLUMNS: usize = 1 << 16;

/// A row rebuilt from a segment: key, version and its cells (column name, value).
pub type DecodedRow = (String, u64, Vec<(String, Val)>);

/// A buffered row: key, version and its cells by column index.
type PendingRow = (String, u64, Vec<(usize, Val)>);

/// One document to write: its id fingerprint (two independent hashes, for delete lookups),
/// key, version and flattened cells.
#[derive(Clone, Copy, Debug)]
pub struct SegmentRow<'a> {
    pub id_hash: u64,
    pub id_check: u32,
    pub key: &'a str,
    pub version: u64,
    pub cells: &'a [(String, Val)],
}

/// Streams rows into a new segment file; nothing is visible until [`SegmentWriter::finish`]
/// renames the completed file into place.
pub struct SegmentWriter<'s> {
    path: PathBuf,
    tmp: PathBuf,
    out: BufWriter<storage_types::vfs::VfsHandle>,
    pos: u64,
    seal: FileSeal<'s>,
    context: Vec<u8>,
    rg_rows: usize,
    columns: Vec<String>,
    column_of: FastHashMap<String, usize>,
    /// Buffered rows of the current row group: `(key, version, cells by column)`.
    pending: Vec<PendingRow>,
    groups: Vec<(u32, Vec<ChunkLoc>)>,
    ids: Vec<(u64, u32, u32)>,
    rows: u64,
    stamp: u64,
}

impl core::fmt::Debug for SegmentWriter<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SegmentWriter").field("path", &self.path).field("rows", &self.rows).finish_non_exhaustive()
    }
}

/// Where a chunk is and what it holds.
#[derive(Clone, Debug, PartialEq)]
pub struct ChunkLoc {
    pub offset: u64,
    pub len: u32,
    pub checksum: u64,
    pub meta: ChunkMeta,
}

impl ChunkLoc {
    const EMPTY: ChunkLoc = ChunkLoc { offset: 0, len: 0, checksum: 0, meta: ChunkMeta { null_count: 0, zone: Zone::None } };
}

fn io_err(what: &'static str) -> impl Fn(std::io::Error) -> StorageError {
    move |e| StorageError::io(what, e)
}

impl<'s> SegmentWriter<'s> {
    /// Starts `path` (written as `<path>.tmp` until finished). `context` names the segment in
    /// sealed units; `stamp` is stored for the caller (e.g. the version the data was read at).
    pub fn create(path: &Path, seal: FileSeal<'s>, context: &[u8], rg_rows: usize, stamp: u64) -> Result<Self> {
        let mut tmp = path.as_os_str().to_os_string();
        tmp.push(".tmp");
        let tmp = PathBuf::from(tmp);
        let file = storage_types::vfs::at(&tmp)
            .open(&tmp, storage_types::vfs::OpenOpts::create_truncate())
            .map_err(io_err("create column segment"))?;
        let mut out = BufWriter::with_capacity(1 << 20, file);
        out.write_all(&[0u8; HEADER_LEN]).map_err(io_err("write column segment"))?;
        let mut w = Self {
            path: path.to_owned(),
            tmp,
            out,
            pos: HEADER_LEN as u64,
            seal,
            context: context.to_vec(),
            rg_rows: rg_rows.clamp(1, MAX_ROW_GROUP_ROWS),
            columns: Vec::new(),
            column_of: FastHashMap::default(),
            pending: Vec::new(),
            groups: Vec::new(),
            ids: Vec::new(),
            rows: 0,
            stamp,
        };
        w.column(KEY_NAME);
        w.column(VERSION_NAME);
        Ok(w)
    }

    fn column(&mut self, name: &str) -> usize {
        if let Some(&i) = self.column_of.get(name) {
            return i;
        }
        let i = self.columns.len();
        self.columns.push(name.to_owned());
        self.column_of.insert(name.to_owned(), i);
        i
    }

    /// Appends one row (rows keep their order). Field paths starting with `$` are reserved.
    pub fn push(&mut self, row: SegmentRow<'_>) -> Result<()> {
        if self.rows >= u64::from(u32::MAX) {
            return Err(StorageError::invalid_value("a column segment holds at most 2^32 - 1 rows"));
        }
        let mut cells = Vec::with_capacity(row.cells.len());
        for (name, v) in row.cells {
            if name.starts_with('$') {
                continue;
            }
            if self.columns.len() >= MAX_COLUMNS && !self.column_of.contains_key(name) {
                return Err(StorageError::invalid_value("a column segment holds at most 65,536 columns"));
            }
            let c = self.column(name);
            cells.push((c, v.clone()));
        }
        self.ids.push((row.id_hash, row.id_check, self.rows as u32));
        self.pending.push((row.key.to_owned(), row.version, cells));
        self.rows += 1;
        if self.pending.len() >= self.rg_rows {
            self.flush_group()?;
        }
        Ok(())
    }

    fn write_unit(&mut self, mut body: Vec<u8>) -> Result<ChunkLoc> {
        // Units start on 64-byte boundaries (cache lines; aligned typed reads later).
        let pad = (64 - (self.pos % 64)) % 64;
        if pad > 0 {
            self.out.write_all(&[0u8; 64][..pad as usize]).map_err(io_err("write column segment"))?;
            self.pos += pad;
        }
        let offset = self.pos;
        if self.seal.sealing() {
            let mut sealed = vec![0u8; self.seal.header_len()];
            sealed.append(&mut body);
            self.seal.seal_in_place(&self.context, offset, &mut sealed)?;
            body = sealed;
        }
        let len = u32::try_from(body.len()).map_err(|_| StorageError::invalid_value("column chunk over 4 GiB"))?;
        self.out.write_all(&body).map_err(io_err("write column segment"))?;
        self.pos += body.len() as u64;
        Ok(ChunkLoc { offset, len, checksum: checksum_payload(&body), meta: ChunkMeta { null_count: 0, zone: Zone::None } })
    }

    fn flush_group(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let rows = core::mem::take(&mut self.pending);
        let n = rows.len();
        let ncols = self.columns.len();
        // Cells by column, then one chunk per column.
        let mut by_col: Vec<Vec<(u32, Val)>> = (0..ncols).map(|_| Vec::new()).collect();
        let mut keys = Vec::with_capacity(n);
        let mut versions = Vec::with_capacity(n);
        for (r, (key, version, cells)) in rows.into_iter().enumerate() {
            keys.push(Some(Val::Text(key)));
            versions.push(Some(Val::UInt(version)));
            for (c, v) in cells {
                by_col[c].push((r as u32, v));
            }
        }
        let mut locs = Vec::with_capacity(ncols);
        for (c, cells) in by_col.into_iter().enumerate() {
            let dense: Vec<Option<Val>> = match c {
                KEY_COLUMN => core::mem::take(&mut keys),
                VERSION_COLUMN => core::mem::take(&mut versions),
                _ if cells.is_empty() => {
                    locs.push(ChunkLoc::EMPTY);
                    continue;
                }
                _ => {
                    let mut d: Vec<Option<Val>> = vec![None; n];
                    for (r, v) in cells {
                        d[r as usize] = Some(v);
                    }
                    d
                }
            };
            let mut body = Vec::new();
            let meta = encode_chunk(&dense, &mut body);
            let mut loc = self.write_unit(body)?;
            loc.meta = meta;
            locs.push(loc);
        }
        self.groups.push((n as u32, locs));
        Ok(())
    }

    /// Writes the id index, footer and header, syncs, and renames the file into place.
    pub fn finish(mut self) -> Result<SegmentSummary> {
        self.flush_group()?;
        let ncols = self.columns.len();
        let mut ids = core::mem::take(&mut self.ids);
        ids.sort_unstable();
        let mut index = Vec::with_capacity(ids.len() * 16);
        for (h, _, _) in &ids {
            put_u64(&mut index, *h);
        }
        for (_, c, _) in &ids {
            put_u32(&mut index, *c);
        }
        for (_, _, r) in &ids {
            put_u32(&mut index, *r);
        }
        let index_loc = self.write_unit(index)?;

        let mut footer = Vec::new();
        put_u32(&mut footer, ncols as u32);
        for (i, name) in self.columns.iter().enumerate() {
            crate::bits::put_bytes(&mut footer, name.as_bytes());
            footer.push(u8::from(i < 2));
        }
        put_u32(&mut footer, self.groups.len() as u32);
        for (rows, locs) in &self.groups {
            put_u32(&mut footer, *rows);
            for c in 0..ncols {
                let loc = locs.get(c).unwrap_or(&ChunkLoc::EMPTY);
                put_u64(&mut footer, loc.offset);
                put_u32(&mut footer, loc.len);
                put_u64(&mut footer, loc.checksum);
                put_u32(&mut footer, loc.meta.null_count);
                footer.extend_from_slice(&loc.meta.zone.encode());
            }
        }
        put_u64(&mut footer, index_loc.offset);
        put_u32(&mut footer, index_loc.len);
        put_u64(&mut footer, index_loc.checksum);
        let footer_loc = self.write_unit(footer)?;

        let mut header = [0u8; HEADER_LEN];
        header[0..8].copy_from_slice(SEGMENT_MAGIC);
        header[8..12].copy_from_slice(&SEGMENT_VERSION.to_le_bytes());
        let flags = if self.seal.sealing() { FLAG_SEALED } else { 0 };
        header[12..16].copy_from_slice(&flags.to_le_bytes());
        header[16..24].copy_from_slice(&self.rows.to_le_bytes());
        header[24..32].copy_from_slice(&footer_loc.offset.to_le_bytes());
        header[32..36].copy_from_slice(&footer_loc.len.to_le_bytes());
        header[36..40].copy_from_slice(&(self.rg_rows as u32).to_le_bytes());
        header[40..48].copy_from_slice(&footer_loc.checksum.to_le_bytes());
        header[48..56].copy_from_slice(&self.stamp.to_le_bytes());
        let hc = checksum_payload(&header[0..56]);
        header[56..64].copy_from_slice(&hc.to_le_bytes());

        self.out.flush().map_err(io_err("write column segment"))?;
        let mut file = self.out.into_inner().map_err(|e| StorageError::io("write column segment", e.into_error()))?;
        use std::io::{Seek, SeekFrom};
        file.seek(SeekFrom::Start(0)).map_err(io_err("write column segment"))?;
        file.write_all(&header).map_err(io_err("write column segment"))?;
        file.sync_all().map_err(io_err("sync column segment"))?;
        drop(file);
        storage_types::vfs::at(&self.path).rename(&self.tmp, &self.path).map_err(io_err("publish column segment"))?;
        sync_parent(&self.path).map_err(io_err("sync column segment directory"))?;
        Ok(SegmentSummary { rows: self.rows, bytes: self.pos, columns: ncols - 2 })
    }
}

/// What a finished segment holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentSummary {
    pub rows: u64,
    pub bytes: u64,
    /// Field columns (hidden columns not counted).
    pub columns: usize,
}

fn sync_parent(path: &Path) -> std::io::Result<()> {
    match path.parent() {
        Some(dir) => {
            let dir = if dir.as_os_str().is_empty() { Path::new(".") } else { dir };
            storage_types::vfs::at(dir).sync_dir(dir)
        }
        None => Ok(()),
    }
}

// ── reading ──

/// A column's name and flags.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnMeta {
    pub name: String,
    pub hidden: bool,
}

/// One row group: its first row, row count and one chunk per column.
#[derive(Clone, Debug)]
pub struct RowGroup {
    pub first_row: u64,
    pub rows: u32,
    pub chunks: Vec<ChunkLoc>,
}

/// An open segment (immutable; shareable across threads).
pub struct Segment {
    region: MmapRegion,
    sealed: bool,
    context: Vec<u8>,
    rows: u64,
    stamp: u64,
    columns: Vec<ColumnMeta>,
    column_of: FastHashMap<String, usize>,
    groups: Vec<RowGroup>,
    index: ChunkLoc,
    footer: ChunkLoc,
    /// Checksum verified, per chunk (row group major) and, last, the id index.
    verified: Vec<AtomicBool>,
    /// Decrypted id index of a sealed segment.
    sealed_index: OnceLock<Vec<u8>>,
}

impl core::fmt::Debug for Segment {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Segment").field("rows", &self.rows).field("columns", &self.columns.len()).finish_non_exhaustive()
    }
}

impl Segment {
    /// Opens and validates a segment's header and footer. Chunks are verified on first read.
    pub fn open(path: &Path, seal: &FileSeal<'_>, context: &[u8]) -> Result<Self> {
        let file = File::open(path).map_err(io_err("open column segment"))?;
        let len = file.metadata().map_err(io_err("stat column segment"))?.len() as usize;
        if len < HEADER_LEN {
            return Err(corrupt("column segment too short"));
        }
        let region = MmapRegion::map_read_only(&file, len)?;
        let b = region.as_slice();
        let header = &b[..HEADER_LEN];
        let u32_at = |o: usize| u32::from_le_bytes([header[o], header[o + 1], header[o + 2], header[o + 3]]);
        let u64_at = |o: usize| {
            let mut a = [0u8; 8];
            a.copy_from_slice(&header[o..o + 8]);
            u64::from_le_bytes(a)
        };
        if &header[0..8] != SEGMENT_MAGIC || u32_at(8) != SEGMENT_VERSION {
            return Err(corrupt("not a column segment, or an unsupported version"));
        }
        if checksum_payload(&header[0..56]) != u64_at(56) {
            return Err(corrupt("column segment header checksum"));
        }
        let sealed = u32_at(12) & FLAG_SEALED != 0;
        if sealed && seal.keys.is_none() {
            return Err(StorageError::invalid_value("column segment is encrypted and no keys are configured"));
        }
        if !sealed && !seal.accept_plain {
            return Err(StorageError::invalid_value("plaintext column segment refused (encryption is required)"));
        }
        let rows = u64_at(16);
        let footer_loc = ChunkLoc { offset: u64_at(24), len: u32_at(32), checksum: u64_at(40), meta: ChunkMeta { null_count: 0, zone: Zone::None } };
        let stamp = u64_at(48);
        let mut seg = Self {
            region,
            sealed,
            context: context.to_vec(),
            rows,
            stamp,
            columns: Vec::new(),
            column_of: FastHashMap::default(),
            groups: Vec::new(),
            index: ChunkLoc::EMPTY,
            footer: footer_loc.clone(),
            verified: Vec::new(),
            sealed_index: OnceLock::new(),
        };
        let footer = seg.unit(&footer_loc, seal, None)?;
        let footer = footer.into_owned();
        seg.parse_footer(&footer)?;
        Ok(seg)
    }

    fn parse_footer(&mut self, footer: &[u8]) -> Result<()> {
        let mut r = Reader::new(footer);
        let ncols = r.u32()? as usize;
        if !(2..=MAX_COLUMNS).contains(&ncols) {
            return Err(corrupt("column count"));
        }
        for i in 0..ncols {
            let name = core::str::from_utf8(r.bytes()?).map_err(|_| corrupt("column name"))?.to_owned();
            let hidden = r.u8()? & 1 != 0;
            self.column_of.insert(name.clone(), i);
            self.columns.push(ColumnMeta { name, hidden });
        }
        if self.columns[KEY_COLUMN].name != KEY_NAME || self.columns[VERSION_COLUMN].name != VERSION_NAME {
            return Err(corrupt("hidden columns"));
        }
        let ngroups = r.u32()? as usize;
        let mut first_row = 0u64;
        let file_len = self.region.len() as u64;
        for _ in 0..ngroups {
            let rows = r.u32()?;
            if rows == 0 || rows as usize > MAX_ROW_GROUP_ROWS {
                return Err(corrupt("row group size"));
            }
            let mut chunks = Vec::with_capacity(ncols);
            for _ in 0..ncols {
                let offset = r.u64()?;
                let len = r.u32()?;
                let checksum = r.u64()?;
                let null_count = r.u32()?;
                let zone = Zone::read(&mut r)?;
                if len > 0 && offset.checked_add(u64::from(len)).is_none_or(|e| e > file_len) {
                    return Err(corrupt("chunk out of bounds"));
                }
                chunks.push(ChunkLoc { offset, len, checksum, meta: ChunkMeta { null_count, zone } });
            }
            self.groups.push(RowGroup { first_row, rows, chunks });
            first_row += u64::from(rows);
        }
        if first_row != self.rows {
            return Err(corrupt("row groups do not add up to the row count"));
        }
        let offset = r.u64()?;
        let len = r.u32()?;
        let checksum = r.u64()?;
        if offset.checked_add(u64::from(len)).is_none_or(|e| e > file_len) {
            return Err(corrupt("id index out of bounds"));
        }
        self.index = ChunkLoc { offset, len, checksum, meta: ChunkMeta { null_count: 0, zone: Zone::None } };
        if r.remaining() != 0 {
            return Err(corrupt("trailing footer bytes"));
        }
        self.verified = (0..ngroups * ncols + 1).map(|_| AtomicBool::new(false)).collect();
        Ok(())
    }

    /// The bytes of a unit: verified once, decrypted if sealed.
    fn unit(&self, loc: &ChunkLoc, seal: &FileSeal<'_>, slot: Option<usize>) -> Result<Cow<'_, [u8]>> {
        let start = usize::try_from(loc.offset).map_err(|_| corrupt("unit offset"))?;
        let bytes = self
            .region
            .as_slice()
            .get(start..start + loc.len as usize)
            .ok_or_else(|| corrupt("unit out of bounds"))?;
        let flag = slot.and_then(|s| self.verified.get(s));
        if !flag.is_some_and(|f| f.load(Ordering::Acquire)) {
            if checksum_payload(bytes) != loc.checksum {
                return Err(corrupt("column chunk checksum"));
            }
            if let Some(f) = flag {
                f.store(true, Ordering::Release);
            }
        }
        if self.sealed {
            let mut owned = bytes.to_vec();
            let range = seal.open_in_place(&self.context, loc.offset, &mut owned)?;
            owned.truncate(range.end);
            owned.drain(..range.start);
            return Ok(Cow::Owned(owned));
        }
        Ok(Cow::Borrowed(bytes))
    }

    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// Byte ranges covered by checksums: header, every chunk, the id index and the footer.
    pub fn checksummed_ranges(&self) -> Vec<(u64, u64)> {
        let mut v = vec![(0, HEADER_LEN as u64)];
        for g in &self.groups {
            for c in &g.chunks {
                if c.len > 0 {
                    v.push((c.offset, c.offset + u64::from(c.len)));
                }
            }
        }
        v.push((self.index.offset, self.index.offset + u64::from(self.index.len)));
        v.push((self.footer.offset, self.footer.offset + u64::from(self.footer.len)));
        v
    }

    pub fn stamp(&self) -> u64 {
        self.stamp
    }

    pub fn columns(&self) -> &[ColumnMeta] {
        &self.columns
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.column_of.get(name).copied()
    }

    pub fn row_groups(&self) -> &[RowGroup] {
        &self.groups
    }

    /// Raw (verified, decrypted) bytes of chunk `(group, column)`; `None` if the column has no
    /// value in that row group.
    pub fn chunk_bytes(&self, group: usize, column: usize, seal: &FileSeal<'_>) -> Result<Option<Cow<'_, [u8]>>> {
        let g = self.groups.get(group).ok_or_else(|| StorageError::invalid_value("row group out of range"))?;
        let loc = g.chunks.get(column).ok_or_else(|| StorageError::invalid_value("column out of range"))?;
        if loc.len == 0 {
            return Ok(None);
        }
        let slot = group * self.columns.len() + column;
        self.unit(loc, seal, Some(slot)).map(Some)
    }

    /// Every row of chunk `(group, column)` as owned values (slow path).
    pub fn decode_column(&self, group: usize, column: usize, seal: &FileSeal<'_>) -> Result<Vec<Option<Val>>> {
        let rows = self.groups.get(group).map_or(0, |g| g.rows as usize);
        match self.chunk_bytes(group, column, seal)? {
            None => Ok(vec![None; rows]),
            Some(bytes) => Chunk::parse(&bytes, rows)?.decode_all(),
        }
    }

    /// The rows of row group `group` rebuilt as `(key, version, cells)`, in row order (cells in
    /// column order). Used to rewrite live rows when segments are compacted.
    pub fn decode_rows(&self, group: usize, seal: &FileSeal<'_>) -> Result<Vec<DecodedRow>> {
        let rows = self.groups.get(group).map_or(0, |g| g.rows as usize);
        let mut out: Vec<DecodedRow> = (0..rows).map(|_| (String::new(), 0, Vec::new())).collect();
        for (c, meta) in self.columns.iter().enumerate() {
            let vals = self.decode_column(group, c, seal)?;
            for (r, v) in vals.into_iter().enumerate() {
                let Some(v) = v else { continue };
                let row = out.get_mut(r).ok_or_else(|| corrupt("row count"))?;
                match (c, v) {
                    (KEY_COLUMN, Val::Text(k)) => row.0 = k,
                    (VERSION_COLUMN, Val::UInt(ver)) => row.1 = ver,
                    (KEY_COLUMN | VERSION_COLUMN, _) => return Err(corrupt("hidden column kind")),
                    (_, v) => row.2.push((meta.name.clone(), v)),
                }
            }
        }
        Ok(out)
    }

    /// Versions of every row of row group `group`.
    pub fn versions(&self, group: usize, seal: &FileSeal<'_>) -> Result<Vec<u64>> {
        self.decode_column(group, VERSION_COLUMN, seal)?
            .into_iter()
            .map(|v| match v {
                Some(Val::UInt(x)) => Ok(x),
                _ => Err(corrupt("version column")),
            })
            .collect()
    }

    /// Rows (segment-wide numbers) of the id whose fingerprint is `(hash, check)`.
    pub fn rows_with_hash(&self, hash: u64, check: u32, seal: &FileSeal<'_>) -> Result<Vec<u64>> {
        let n = usize::try_from(self.rows).map_err(|_| corrupt("row count"))?;
        let index: &[u8] = if self.sealed {
            match self.sealed_index.get() {
                Some(v) => v,
                None => {
                    let v = self.unit(&self.index, seal, Some(self.verified.len() - 1))?.into_owned();
                    let _ = self.sealed_index.set(v);
                    self.sealed_index.get().map_or(&[][..], Vec::as_slice)
                }
            }
        } else {
            match self.unit(&self.index, seal, Some(self.verified.len() - 1))? {
                Cow::Borrowed(b) => b,
                Cow::Owned(_) => return Err(corrupt("id index")),
            }
        };
        if index.len() != n * 16 {
            return Err(corrupt("id index length"));
        }
        let hash_at = |i: usize| {
            let mut a = [0u8; 8];
            a.copy_from_slice(&index[i * 8..i * 8 + 8]);
            u64::from_le_bytes(a)
        };
        let u32_at = |o: usize| u32::from_le_bytes([index[o], index[o + 1], index[o + 2], index[o + 3]]);
        let check_at = |i: usize| u32_at(n * 8 + i * 4);
        let row_at = |i: usize| u64::from(u32_at(n * 12 + i * 4));
        let (mut lo, mut hi) = (0usize, n);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if hash_at(mid) < hash {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        let mut out = Vec::new();
        while lo < n && hash_at(lo) == hash {
            if check_at(lo) == check {
                out.push(row_at(lo));
            }
            lo += 1;
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::flatten_owned;
    use std::collections::BTreeMap;
    use storage_codec::types::PropertyValue as P;
    use storage_types::Decimal;

    fn dir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("stge_col_{name}_{}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        fs::create_dir_all(&d).unwrap();
        d
    }

    fn doc(i: i64) -> BTreeMap<String, P> {
        let mut m = BTreeMap::new();
        m.insert("n".into(), P::Int64(i));
        m.insert("price".into(), P::Decimal(Decimal::new(i128::from(i) * 101, 2).unwrap()));
        m.insert("city".into(), P::Text(["Lagos", "Berlin", "Tokyo"][i as usize % 3].into()));
        if i % 5 == 0 {
            m.insert("note".into(), P::Text(format!("note number {i}")));
        }
        // A field whose kind varies: an int, a float or text.
        m.insert(
            "mixed".into(),
            match i % 3 {
                0 => P::Int64(i),
                1 => P::Float64(i as f64 + 0.5),
                _ => P::Text(i.to_string()),
            },
        );
        let mut inner = BTreeMap::new();
        inner.insert("ok".into(), P::Bool(i % 2 == 0));
        inner.insert("at".into(), P::TimestampNs(1_700_000_000_000_000_000 + i));
        m.insert("meta".into(), P::Map(inner));
        m.insert("tags".into(), P::List(vec![P::Text("a".into()), P::Int64(i)]));
        m.insert("u".into(), P::UInt64(u64::MAX - i as u64));
        m.insert("big".into(), P::Int128(i128::MAX - i128::from(i)));
        m
    }

    fn write(path: &Path, n: i64, seal: FileSeal<'_>, rg: usize) -> SegmentSummary {
        let mut w = SegmentWriter::create(path, seal, b"seg-test", rg, 42).unwrap();
        for i in 0..n {
            let mut cells = Vec::new();
            flatten_owned(&doc(i), &mut cells);
            let key = format!("k{i:06}");
            w.push(SegmentRow { id_hash: (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15), id_check: i as u32, key: &key, version: i as u64 + 1, cells: &cells })
                .unwrap();
        }
        w.finish().unwrap()
    }

    #[allow(clippy::needless_range_loop)]
    fn check(seg: &Segment, n: i64, seal: &FileSeal<'_>) {
        assert_eq!(seg.rows(), n as u64);
        assert_eq!(seg.stamp(), 42);
        let mut row = 0i64;
        for g in 0..seg.row_groups().len() {
            let cols: Vec<Vec<Option<Val>>> = (0..seg.columns().len()).map(|c| seg.decode_column(g, c, seal).unwrap()).collect();
            for r in 0..seg.row_groups()[g].rows as usize {
                let i = row;
                let mut want = Vec::new();
                flatten_owned(&doc(i), &mut want);
                let mut got: Vec<(String, Val)> = Vec::new();
                for (c, meta) in seg.columns().iter().enumerate() {
                    if let Some(v) = &cols[c][r] {
                        if !meta.hidden {
                            got.push((meta.name.clone(), v.clone()));
                        }
                    }
                }
                got.sort_by(|a, b| a.0.cmp(&b.0));
                want.sort_by(|a, b| a.0.cmp(&b.0));
                assert_eq!(got, want, "row {i}");
                assert_eq!(cols[KEY_COLUMN][r], Some(Val::Text(format!("k{i:06}"))));
                assert_eq!(cols[VERSION_COLUMN][r], Some(Val::UInt(i as u64 + 1)));
                row += 1;
            }
        }
        assert_eq!(row, n);
        for i in [0i64, 1, n / 2, n - 1] {
            let h = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
            assert_eq!(seg.rows_with_hash(h, i as u32, seal).unwrap(), vec![i as u64]);
            assert!(seg.rows_with_hash(h, i as u32 ^ 1, seal).unwrap().is_empty());
        }
        assert!(seg.rows_with_hash(12345, 0, seal).unwrap().is_empty());
    }

    #[test]
    fn segments_roundtrip_every_kind_exactly() {
        let d = dir("roundtrip");
        let path = d.join("seg.col");
        let s = write(&path, 10_000, FileSeal::PLAIN, 4096);
        assert_eq!(s.rows, 10_000);
        let seg = Segment::open(&path, &FileSeal::PLAIN, b"seg-test").unwrap();
        check(&seg, 10_000, &FileSeal::PLAIN);
        // Zone maps describe the chunk.
        let g0 = &seg.row_groups()[0];
        let n = seg.column_index("n").unwrap();
        assert_eq!(g0.chunks[n].meta.zone, Zone::Int { min: 0, max: 4095 });
        let note = seg.column_index("note").unwrap();
        assert_eq!(g0.chunks[note].meta.null_count, 4096 - 820);
        // Compact: far below the size of the documents as text.
        assert!(s.bytes < 10_000 * 60, "{} bytes", s.bytes);
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn sealed_segments_roundtrip_and_refuse_downgrade() {
        use storage_codec::crypto::KeyRing;
        let d = dir("sealed");
        let path = d.join("seg.col");
        let ring = KeyRing::new().with_key(7, storage_codec::crypto::EncryptionKey::from_bytes([9u8; 32]));
        let seal = FileSeal { keys: Some(&ring), active_key: Some(7), accept_plain: false };
        write(&path, 3000, seal, 1000);
        let seg = Segment::open(&path, &seal, b"seg-test").unwrap();
        check(&seg, 3000, &seal);
        // Wrong context (a segment moved to another collection) fails.
        assert!(Segment::open(&path, &seal, b"other").is_err());
        // Plaintext refused when encryption is required.
        let plain = d.join("plain.col");
        write(&plain, 10, FileSeal::PLAIN, 1000);
        assert!(Segment::open(&plain, &seal, b"seg-test").is_err());
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn corrupted_segments_are_detected_never_trusted() {
        let d = dir("corrupt");
        let path = d.join("seg.col");
        write(&path, 2000, FileSeal::PLAIN, 512);
        let good = fs::read(&path).unwrap();
        let ranges = Segment::open(&path, &FileSeal::PLAIN, b"seg-test").unwrap().checksummed_ranges();
        let step = (good.len() / 400).max(1);
        for pos in (0..good.len()).step_by(step) {
            let mut bad = good.clone();
            bad[pos] ^= 0x5A;
            let p = d.join("bad.col");
            fs::write(&p, &bad).unwrap();
            // Either open fails, or every read of the damaged unit fails; nothing panics and
            // no damaged value is returned as data.
            if let Ok(seg) = Segment::open(&p, &FileSeal::PLAIN, b"seg-test") {
                let mut failed = false;
                for g in 0..seg.row_groups().len() {
                    for c in 0..seg.columns().len() {
                        failed |= seg.decode_column(g, c, &FileSeal::PLAIN).is_err();
                    }
                }
                failed |= seg.rows_with_hash(0, 0, &FileSeal::PLAIN).is_err();
                let covered = ranges.iter().any(|&(a, b)| (a..b).contains(&(pos as u64)));
                assert!(failed || !covered, "flip at {pos} inside a checksummed unit went unnoticed");
            }
        }
        for cut in [0, 10, 63, 64, good.len() / 2, good.len() - 1] {
            let p = d.join("cut.col");
            fs::write(&p, &good[..cut]).unwrap();
            assert!(Segment::open(&p, &FileSeal::PLAIN, b"seg-test").is_err(), "cut at {cut}");
        }
        let _ = fs::remove_dir_all(&d);
    }
}

#[cfg(test)]
mod golden {
    use super::*;
    use crate::column::flatten_owned;
    use std::collections::BTreeMap;
    use storage_codec::types::PropertyValue as P;
    use storage_types::Decimal;

    /// The on-disk format is pinned: the same rows always produce the same bytes, on every
    /// platform and build (`RULE-SAFE-05`). A change here is a format change.
    #[test]
    fn segment_bytes_are_pinned() {
        let d = std::env::temp_dir().join(format!("stge_col_golden_{}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        fs::create_dir_all(&d).unwrap();
        let path = d.join("g.col");
        let mut w = SegmentWriter::create(&path, FileSeal::PLAIN, b"g", 100, 7).unwrap();
        for i in 0..250i64 {
            let mut m = BTreeMap::new();
            m.insert("a".to_string(), P::Int64(i * 3));
            m.insert("b".to_string(), P::Text(["x", "yy", "zzz"][i as usize % 3].into()));
            m.insert("c".to_string(), P::Decimal(Decimal::new(i128::from(i), 2).unwrap()));
            if i % 7 == 0 {
                m.insert("d".to_string(), P::Float64(i as f64 / 4.0));
            }
            let mut cells = Vec::new();
            flatten_owned(&m, &mut cells);
            let key = format!("{i}");
            w.push(SegmentRow { id_hash: i as u64 * 31, id_check: i as u32, key: &key, version: 1, cells: &cells }).unwrap();
        }
        w.finish().unwrap();
        let bytes = fs::read(&path).unwrap();
        let _ = fs::remove_dir_all(&d);
        assert_eq!((bytes.len(), checksum_payload(&bytes)), (GOLDEN_LEN, GOLDEN_SUM), "segment format changed");
    }

    const GOLDEN_LEN: usize = 7678;
    const GOLDEN_SUM: u64 = 5_671_029_601_970_640_754;
}
