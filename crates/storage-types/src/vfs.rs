//! The one file-system seam (`F-SIM-01`, `RULE-TEST-03`).
//!
//! Every durable write path (WAL, record files, arena segments, index checkpoints, column
//! segments, Raft storage) opens, syncs, renames and removes files through a [`Vfs`].
//!
//! - [`RealVfs`] is `std::fs` with no extra work: a handle wraps the `File`, and mapped files
//!   are mapped from [`VfsFile::file`], so mapped hot reads have no indirection at all.
//! - [`SimVfs`] runs on a real directory, the way the OS page cache would. It remembers what
//!   each file held at its last sync and which directory changes have been synced. A simulated
//!   power cut ([`SimVfs::power_cut`]) then rewrites the directory to what a disk may hold
//!   afterwards:
//!   - Every 512-byte sector written since the last sync is kept or lost on its own (sectors
//!     are atomic; bytes past the old end that were never written read as zeros).
//!     A write can come back torn, and a later write can survive an earlier one (reordering).
//!   - The file length lands anywhere between the synced and the written length.
//!   - Creates, renames and removes not yet made durable by a directory sync are rolled back,
//!     newest first. They keep journal order: a later one never survives an earlier one.
//!
//! Code finds its file system with [`at`]: the simulator mounted over the path
//! ([`SimVfs::mount`]), or the real one.
//!
//! A crash point ([`SimVfs::fail_at`]) fails the `k`-th I/O call and every call after it,
//! as if the process died there. Writes through a mapping are captured as well, because the
//! sim compares file contents instead of trusting the write calls it saw.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, RwLock};

use crate::testing::SweepRng;

/// Shared handle to a file system.
pub type VfsRef = Arc<dyn Vfs>;
/// An open file.
pub type VfsHandle = Box<dyn VfsFile>;

/// The process-wide real file system.
pub fn real() -> VfsRef {
    static REAL: OnceLock<VfsRef> = OnceLock::new();
    REAL.get_or_init(|| Arc::new(RealVfs)).clone()
}

static MOUNTED: AtomicBool = AtomicBool::new(false);
static MOUNTS: RwLock<Vec<(PathBuf, VfsRef)>> = RwLock::new(Vec::new());

/// The file system that owns `path`: a [`SimVfs`] mounted over one of its ancestors, or the
/// real one. With nothing mounted (production) this is one relaxed atomic load.
#[inline]
pub fn at(path: &Path) -> VfsRef {
    if !MOUNTED.load(Ordering::Relaxed) {
        return real();
    }
    at_mounted(path)
}

#[cold]
fn at_mounted(path: &Path) -> VfsRef {
    let p = if path.is_absolute() { norm(path) } else { norm(&std::env::current_dir().unwrap_or_default().join(path)) };
    let mounts = MOUNTS.read().unwrap_or_else(|e| e.into_inner());
    mounts.iter().filter(|(root, _)| p.starts_with(root)).max_by_key(|(root, _)| root.as_os_str().len()).map_or_else(real, |(_, v)| v.clone())
}

/// Keeps a [`SimVfs`] mounted over its root until dropped.
#[derive(Debug)]
#[must_use = "the simulator is unmounted when the guard is dropped"]
pub struct MountGuard {
    root: PathBuf,
}

impl Drop for MountGuard {
    fn drop(&mut self) {
        let mut mounts = MOUNTS.write().unwrap_or_else(|e| e.into_inner());
        if let Some(i) = mounts.iter().position(|(r, _)| *r == self.root) {
            mounts.remove(i);
        }
        MOUNTED.store(!mounts.is_empty(), Ordering::Relaxed);
    }
}

/// How to open a file (the subset of `OpenOptions` the engine uses).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OpenOpts {
    pub read: bool,
    pub write: bool,
    pub append: bool,
    pub create: bool,
    pub truncate: bool,
}

impl OpenOpts {
    /// Read only.
    pub const fn read() -> Self {
        Self { read: true, write: false, append: false, create: false, truncate: false }
    }
    /// Read and write an existing file.
    pub const fn read_write() -> Self {
        Self { read: true, write: true, append: false, create: false, truncate: false }
    }
    /// Read and write, creating the file when missing.
    pub const fn create() -> Self {
        Self { read: true, write: true, append: false, create: true, truncate: false }
    }
    /// Write a new, empty file (an existing one is truncated).
    pub const fn create_truncate() -> Self {
        Self { read: true, write: true, append: false, create: true, truncate: true }
    }
    /// Append, creating the file when missing.
    pub const fn append() -> Self {
        Self { read: true, write: false, append: true, create: true, truncate: false }
    }

    fn std(self) -> OpenOptions {
        let mut o = OpenOptions::new();
        o.read(self.read).write(self.write).append(self.append).create(self.create).truncate(self.truncate);
        o
    }
}

/// An open file. `Read`, `Write` and `Seek` move the handle's cursor like a `File`.
pub trait VfsFile: Read + Write + Seek + Send + Sync + fmt::Debug {
    /// Makes the file's data durable (`fdatasync`).
    fn sync_data(&self) -> io::Result<()>;
    /// Makes the file's data and metadata durable (`fsync`).
    fn sync_all(&self) -> io::Result<()>;
    fn set_len(&self, len: u64) -> io::Result<()>;
    fn len(&self) -> io::Result<u64>;
    fn is_empty(&self) -> io::Result<bool> {
        Ok(self.len()? == 0)
    }
    /// The underlying file, for memory mapping and positional reads. Writes through it (or a
    /// mapping of it) are still seen by the simulator, which compares contents at each sync.
    fn file(&self) -> &File;
}

/// A file system: real, or simulated for power-loss and crash-point tests.
pub trait Vfs: Send + Sync + fmt::Debug {
    fn open(&self, path: &Path, opts: OpenOpts) -> io::Result<VfsHandle>;
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;
    fn remove_file(&self, path: &Path) -> io::Result<()>;
    fn remove_dir_all(&self, path: &Path) -> io::Result<()>;
    fn create_dir_all(&self, path: &Path) -> io::Result<()>;
    /// Makes the directory's entries (creates, renames, removes) durable.
    fn sync_dir(&self, path: &Path) -> io::Result<()>;

    /// Paths of the entries of a directory (files and directories), unordered.
    fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        fs::read_dir(path)?.map(|e| e.map(|e| e.path())).collect()
    }
    fn exists(&self, path: &Path) -> bool {
        path.exists()
    }
    fn file_len(&self, path: &Path) -> io::Result<u64> {
        Ok(fs::metadata(path)?.len())
    }
    /// The whole file (`fs::read`).
    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        let mut f = self.open(path, OpenOpts::read())?;
        let mut out = Vec::new();
        f.read_to_end(&mut out)?;
        Ok(out)
    }
    /// Replaces the file's contents without syncing (`fs::write`).
    fn write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        let mut f = self.open(path, OpenOpts::create_truncate())?;
        f.write_all(data)
    }
    /// Writes `data` to `path` durably: a temporary file is written and synced, renamed over
    /// `path`, and the directory synced. A crash leaves the old or the new contents.
    fn write_atomic(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        let mut tmp = path.as_os_str().to_owned();
        tmp.push(".tmp");
        let tmp = PathBuf::from(tmp);
        {
            let mut f = self.open(&tmp, OpenOpts::create_truncate())?;
            f.write_all(data)?;
            f.sync_all()?;
        }
        self.rename(&tmp, path)?;
        self.sync_dir(path.parent().unwrap_or(Path::new(".")))
    }
    /// Copies a file without syncing (`fs::copy`).
    fn copy(&self, from: &Path, to: &Path) -> io::Result<u64> {
        let data = self.read(from)?;
        self.write(to, &data)?;
        Ok(data.len() as u64)
    }
    /// `true` for the simulator (lets callers skip work that only matters on real disks).
    fn is_simulated(&self) -> bool {
        false
    }
}

// ---------------------------------------------------------------------------------------------
// Real file system

/// `std::fs`, unchanged.
#[derive(Debug, Default, Clone, Copy)]
pub struct RealVfs;

#[derive(Debug)]
struct RealFile(File);

impl Read for RealFile {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}
impl Write for RealFile {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }
    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.0.write_all(buf)
    }
    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}
impl Seek for RealFile {
    #[inline]
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.0.seek(pos)
    }
}
impl VfsFile for RealFile {
    fn sync_data(&self) -> io::Result<()> {
        self.0.sync_data()
    }
    fn sync_all(&self) -> io::Result<()> {
        self.0.sync_all()
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.0.set_len(len)
    }
    fn len(&self) -> io::Result<u64> {
        Ok(self.0.metadata()?.len())
    }
    fn file(&self) -> &File {
        &self.0
    }
}

impl Vfs for RealVfs {
    fn open(&self, path: &Path, opts: OpenOpts) -> io::Result<VfsHandle> {
        Ok(Box::new(RealFile(opts.std().open(path)?)))
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        fs::rename(from, to)
    }
    fn remove_file(&self, path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }
    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        fs::remove_dir_all(path)
    }
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        fs::create_dir_all(path)
    }
    fn sync_dir(&self, path: &Path) -> io::Result<()> {
        // Directories cannot be opened (or synced) as files on Windows; NTFS journals entries.
        #[cfg(unix)]
        {
            File::open(path)?.sync_all()
        }
        #[cfg(not(unix))]
        {
            let _ = path;
            Ok(())
        }
    }
    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        fs::read(path)
    }
    fn write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        fs::write(path, data)
    }
    fn copy(&self, from: &Path, to: &Path) -> io::Result<u64> {
        fs::copy(from, to)
    }
}

// ---------------------------------------------------------------------------------------------
// Simulated file system

/// Sector size of the simulated disk: the unit a power cut keeps or loses on its own.
pub const SIM_SECTOR: usize = 512;

/// What a power cut does to writes made since the last sync.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CutMode {
    /// Each unsynced sector is kept, lost or zeroed at random; unsynced directory changes roll
    /// back to a random point (the default).
    Random,
    /// Every unsynced write and directory change is lost.
    LoseAll,
    /// Everything written survives (a process crash: the OS still had the page cache).
    KeepAll,
}

/// What the last power cut did, for test reports.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CutReport {
    pub files: usize,
    pub sectors_kept: u64,
    pub sectors_lost: u64,
    pub sectors_zeroed: u64,
    pub dir_ops_kept: usize,
    pub dir_ops_lost: usize,
}

#[derive(Debug, Clone)]
enum DirOp {
    Create(PathBuf, u64),
    Rename(PathBuf, PathBuf),
    Remove(PathBuf),
}

impl DirOp {
    fn touches(&self, dir: &Path) -> bool {
        let parent = |p: &PathBuf| p.parent() == Some(dir);
        match self {
            DirOp::Create(p, _) | DirOp::Remove(p) => parent(p),
            DirOp::Rename(a, b) => parent(a) || parent(b),
        }
    }

    fn apply(&self, ns: &mut BTreeMap<PathBuf, u64>) {
        match self {
            DirOp::Create(p, ino) => {
                ns.insert(p.clone(), *ino);
            }
            DirOp::Rename(a, b) => {
                if let Some(ino) = ns.remove(a) {
                    ns.insert(b.clone(), ino);
                }
            }
            DirOp::Remove(p) => {
                ns.remove(p);
            }
        }
    }
}

#[derive(Debug, Default)]
struct Inode {
    /// Contents as of the last sync.
    durable: Vec<u8>,
    /// Contents when the process stopped (filled by `freeze`).
    frozen: Option<Vec<u8>>,
    /// Contents when the file was unlinked (its last directory entry removed).
    unlinked: Option<Vec<u8>>,
    /// Lowest offset written through a handle since the last sync (`u64::MAX`: none).
    dirty_from: u64,
    /// The file was handed out for mapping / positional writes: compare all of it.
    mapped: bool,
}

#[derive(Debug)]
struct SimState {
    inodes: HashMap<u64, Inode>,
    next_ino: u64,
    /// Namespace as the running process sees it.
    cur: BTreeMap<PathBuf, u64>,
    /// Namespace a power cut keeps without any pending directory change.
    dur: BTreeMap<PathBuf, u64>,
    pending: Vec<DirOp>,
    ops: u64,
    fail_at: Option<u64>,
    frozen: bool,
    /// Bumped by each power cut: handles from before it fail.
    epoch: u64,
    rng: SweepRng,
    mode: CutMode,
    last: CutReport,
}

/// A simulated disk over a real directory (see the module docs).
#[derive(Debug, Clone)]
pub struct SimVfs {
    root: PathBuf,
    st: Arc<Mutex<SimState>>,
}

fn crashed() -> io::Error {
    io::Error::other("simulated crash: the process stopped at this I/O call")
}

/// Lexical normalisation (no symlink resolution): drops `.` and folds `..`.
fn norm(p: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for c in p.components() {
        match c {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

fn sector_at(src: &[u8], at: usize) -> Option<&[u8]> {
    (at < src.len()).then(|| &src[at..(at + SIM_SECTOR).min(src.len())])
}

/// The file's bytes from `from` to its end, without moving the handle's cursor.
fn read_from(f: &File, from: u64) -> io::Result<Vec<u8>> {
    let len = (f.metadata()?.len().saturating_sub(from)) as usize;
    let mut buf = vec![0u8; len];
    let mut done = 0;
    while done < len {
        let at = from + done as u64;
        #[cfg(unix)]
        let n = std::os::unix::fs::FileExt::read_at(f, &mut buf[done..], at)?;
        #[cfg(windows)]
        let n = std::os::windows::fs::FileExt::seek_read(f, &mut buf[done..], at)?;
        if n == 0 {
            buf.truncate(done);
            break;
        }
        done += n;
    }
    Ok(buf)
}

impl SimVfs {
    /// A simulated disk rooted at `root` (created if missing). Files already there count as
    /// durable. Paths outside `root` pass through to the real file system, untracked.
    pub fn new(root: &Path, seed: u64) -> io::Result<Self> {
        fs::create_dir_all(root)?;
        let root = norm(&if root.is_absolute() { root.to_path_buf() } else { std::env::current_dir()?.join(root) });
        let mut st = SimState {
            inodes: HashMap::new(),
            next_ino: 1,
            cur: BTreeMap::new(),
            dur: BTreeMap::new(),
            pending: Vec::new(),
            ops: 0,
            fail_at: None,
            frozen: false,
            epoch: 0,
            rng: SweepRng::new(seed),
            mode: CutMode::Random,
            last: CutReport::default(),
        };
        let mut stack = vec![root.clone()];
        while let Some(dir) = stack.pop() {
            for e in fs::read_dir(&dir)? {
                let p = norm(&e?.path());
                if p.is_dir() {
                    stack.push(p);
                } else {
                    let ino = st.next_ino;
                    st.next_ino += 1;
                    st.inodes.insert(ino, Inode { durable: fs::read(&p)?, dirty_from: u64::MAX, ..Inode::default() });
                    st.cur.insert(p.clone(), ino);
                    st.dur.insert(p, ino);
                }
            }
        }
        Ok(Self { root, st: Arc::new(Mutex::new(st)) })
    }

    /// Routes every [`at`] lookup under the root to this simulator until the guard drops, so
    /// code that opens files through `vfs::at` runs on the simulated disk unchanged.
    pub fn mount(&self) -> MountGuard {
        let mut mounts = MOUNTS.write().unwrap_or_else(|e| e.into_inner());
        mounts.retain(|(r, _)| *r != self.root);
        mounts.push((self.root.clone(), self.as_ref()));
        MOUNTED.store(true, Ordering::Relaxed);
        MountGuard { root: self.root.clone() }
    }

    /// As an `Arc<dyn Vfs>`.
    pub fn as_ref(&self) -> VfsRef {
        Arc::new(self.clone())
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn lock(&self) -> MutexGuard<'_, SimState> {
        self.st.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn abs(&self, p: &Path) -> PathBuf {
        if p.is_absolute() {
            norm(p)
        } else {
            norm(&std::env::current_dir().unwrap_or_default().join(p))
        }
    }

    fn tracked(&self, p: &Path) -> bool {
        p.starts_with(&self.root)
    }

    /// Counts one I/O call; fails it (and freezes the disk) at the crash point.
    fn tick(st: &mut SimState) -> io::Result<()> {
        if st.frozen {
            return Err(crashed());
        }
        st.ops += 1;
        if st.fail_at.is_some_and(|k| st.ops >= k) {
            Self::freeze_locked(st);
            return Err(crashed());
        }
        Ok(())
    }

    /// I/O calls made so far.
    pub fn ops(&self) -> u64 {
        self.lock().ops
    }

    /// Fails the `k`-th I/O call from now (counting from 1) and every one after it.
    pub fn fail_at(&self, k: u64) {
        let mut st = self.lock();
        st.fail_at = Some(st.ops + k.max(1));
    }

    pub fn set_mode(&self, mode: CutMode) {
        self.lock().mode = mode;
    }

    /// `true` once the process has stopped (crash point reached or [`Self::freeze`]).
    pub fn is_frozen(&self) -> bool {
        self.lock().frozen
    }

    /// Stops the process: every later I/O call fails, and the contents written so far are
    /// recorded, so writes made after this (through mappings, by threads still running) don't
    /// count.
    pub fn freeze(&self) {
        Self::freeze_locked(&mut self.lock());
    }

    fn freeze_locked(st: &mut SimState) {
        if st.frozen {
            return;
        }
        st.frozen = true;
        let live: Vec<(PathBuf, u64)> = st.cur.iter().map(|(p, i)| (p.clone(), *i)).collect();
        for (p, ino) in live {
            let data = fs::read(&p).ok();
            if let Some(n) = st.inodes.get_mut(&ino) {
                n.frozen = data;
            }
        }
    }

    /// What the last power cut did.
    pub fn last_cut(&self) -> CutReport {
        self.lock().last
    }

    /// Files under the root the simulator never saw created (written around the seam).
    pub fn untracked(&self) -> Vec<PathBuf> {
        let st = self.lock();
        let mut out = Vec::new();
        let mut stack = vec![self.root.clone()];
        while let Some(dir) = stack.pop() {
            let Ok(rd) = fs::read_dir(&dir) else { continue };
            for e in rd.flatten() {
                let p = norm(&e.path());
                if p.is_dir() {
                    stack.push(p);
                } else if !st.cur.contains_key(&p) {
                    out.push(p);
                }
            }
        }
        out.sort();
        out
    }

    /// Cuts the power. Every handle opened before this fails afterwards; the directory holds
    /// what the disk kept, and the simulator starts over from it with all of it durable.
    /// The caller drops the stopped process's objects first (mappings included).
    pub fn power_cut(&self) -> io::Result<CutReport> {
        let mut st = self.lock();
        Self::freeze_locked(&mut st);
        let mode = st.mode;
        let mut report = CutReport::default();

        // Directory changes: a prefix of the journal survives.
        let n = st.pending.len();
        let keep = match mode {
            CutMode::Random => st.rng.below(n + 1),
            CutMode::LoseAll => 0,
            CutMode::KeepAll => n,
        };
        let mut ns = st.dur.clone();
        let pending = std::mem::take(&mut st.pending);
        for op in &pending[..keep] {
            op.apply(&mut ns);
        }
        report.dir_ops_kept = keep;
        report.dir_ops_lost = n - keep;

        // File contents: mix synced and written sectors.
        let mut files: Vec<(PathBuf, Vec<u8>)> = Vec::with_capacity(ns.len());
        for (path, ino) in &ns {
            let (durable, written) = {
                let node = st.inodes.get(ino);
                let durable = node.map(|n| n.durable.clone()).unwrap_or_default();
                let written = node.and_then(|n| n.frozen.clone().or_else(|| n.unlinked.clone())).unwrap_or_else(|| durable.clone());
                (durable, written)
            };
            let data = Self::mix(&mut st.rng, mode, &durable, &written, &mut report);
            files.push((path.clone(), data));
        }
        report.files = files.len();

        // Rewrite the directory.
        for p in st.cur.keys() {
            let _ = fs::remove_file(p);
        }
        for (p, data) in &files {
            if let Some(dir) = p.parent() {
                fs::create_dir_all(dir)?;
            }
            fs::write(p, data)?;
        }

        // Start over: what is on disk now is durable.
        st.inodes.clear();
        st.cur.clear();
        st.dur.clear();
        st.next_ino = 1;
        for (p, data) in files {
            let ino = st.next_ino;
            st.next_ino += 1;
            st.inodes.insert(ino, Inode { durable: data, dirty_from: u64::MAX, ..Inode::default() });
            st.cur.insert(p.clone(), ino);
            st.dur.insert(p, ino);
        }
        st.frozen = false;
        st.fail_at = None;
        st.ops = 0;
        st.epoch += 1;
        st.last = report;
        Ok(report)
    }

    /// Stops the process without a power cut: everything written survives.
    pub fn crash(&self) -> io::Result<CutReport> {
        let mode = {
            let mut st = self.lock();
            std::mem::replace(&mut st.mode, CutMode::KeepAll)
        };
        let r = self.power_cut();
        self.lock().mode = mode;
        r
    }

    fn mix(rng: &mut SweepRng, mode: CutMode, durable: &[u8], written: &[u8], report: &mut CutReport) -> Vec<u8> {
        match mode {
            CutMode::KeepAll => return written.to_vec(),
            CutMode::LoseAll => return durable.to_vec(),
            CutMode::Random => {}
        }
        if durable == written {
            return written.to_vec();
        }
        let (lo, hi) = (durable.len().min(written.len()), durable.len().max(written.len()));
        // The length lands on a sector boundary between the two lengths, or on either end.
        let len = if lo == hi {
            lo
        } else {
            match rng.below(4) {
                0 => durable.len(),
                1 => written.len(),
                _ => {
                    let first = lo.div_ceil(SIM_SECTOR);
                    let last = hi / SIM_SECTOR;
                    if last > first { ((first + rng.below(last - first + 1)) * SIM_SECTOR).min(hi) } else { hi }
                }
            }
        };
        let mut out = vec![0u8; len];
        for (i, sector) in out.chunks_mut(SIM_SECTOR).enumerate() {
            let at = i * SIM_SECTOR;
            let (old, new) = (sector_at(durable, at), sector_at(written, at));
            if old == new {
                if let Some(o) = old {
                    let n = o.len().min(sector.len());
                    sector[..n].copy_from_slice(&o[..n]);
                }
                continue;
            }
            // Sectors are atomic: one comes back as written or as it was. Bytes past the old
            // end that were never written come back as zeros.
            let src = if rng.below(2) == 0 {
                report.sectors_kept += 1;
                new
            } else {
                if old.map_or(0, <[u8]>::len) < sector.len() {
                    report.sectors_zeroed += 1;
                } else {
                    report.sectors_lost += 1;
                }
                old
            };
            if let Some(s) = src {
                let n = s.len().min(sector.len());
                sector[..n].copy_from_slice(&s[..n]);
            }
        }
        out
    }

    fn sync_inode(&self, ino: u64, file: &File, epoch: u64) -> io::Result<()> {
        let mut st = self.lock();
        if st.epoch != epoch {
            return Err(crashed());
        }
        Self::tick(&mut st)?;
        let Some(node) = st.inodes.get(&ino) else { return Ok(()) };
        let dlen = node.durable.len() as u64;
        // Only the bytes from the first unsynced write on need reading, unless the file was
        // mapped (any byte may have changed).
        let from = if node.mapped { 0 } else { node.dirty_from.min(dlen) };
        drop(st);
        let len = file.metadata()?.len();
        if from == dlen && len == dlen {
            return Ok(());
        }
        let tail = read_from(file, from)?;
        let mut st = self.lock();
        if let Some(node) = st.inodes.get_mut(&ino) {
            node.durable.truncate(from as usize);
            node.durable.extend_from_slice(&tail);
            node.dirty_from = u64::MAX;
        }
        Ok(())
    }
}

/// A file on the simulated disk.
#[derive(Debug)]
struct SimFile {
    f: File,
    /// `None` for files outside the simulated root.
    ino: Option<u64>,
    vfs: SimVfs,
    epoch: u64,
    append: bool,
}

impl SimFile {
    fn check(&self) -> io::Result<()> {
        let mut st = self.vfs.lock();
        if st.epoch != self.epoch {
            return Err(crashed());
        }
        SimVfs::tick(&mut st)
    }

    fn mark(&mut self, from: u64) {
        if let Some(ino) = self.ino {
            if let Some(n) = self.vfs.lock().inodes.get_mut(&ino) {
                n.dirty_from = n.dirty_from.min(from);
            }
        }
    }
}

impl Read for SimFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.vfs.lock().frozen {
            return Err(crashed());
        }
        self.f.read(buf)
    }
}

impl Write for SimFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.check()?;
        let at = if self.append { self.f.metadata()?.len() } else { self.f.stream_position()? };
        self.mark(at);
        self.f.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.f.flush()
    }
}

impl Seek for SimFile {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.f.seek(pos)
    }
}

impl VfsFile for SimFile {
    fn sync_data(&self) -> io::Result<()> {
        match self.ino {
            Some(ino) => self.vfs.sync_inode(ino, &self.f, self.epoch),
            None => self.check(),
        }
    }
    fn sync_all(&self) -> io::Result<()> {
        self.sync_data()
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.check()?;
        if let Some(ino) = self.ino {
            if let Some(n) = self.vfs.lock().inodes.get_mut(&ino) {
                n.dirty_from = n.dirty_from.min(len);
            }
        }
        self.f.set_len(len)
    }
    fn len(&self) -> io::Result<u64> {
        Ok(self.f.metadata()?.len())
    }
    fn file(&self) -> &File {
        if let Some(ino) = self.ino {
            if let Some(n) = self.vfs.lock().inodes.get_mut(&ino) {
                n.mapped = true;
            }
        }
        &self.f
    }
}

impl Vfs for SimVfs {
    fn open(&self, path: &Path, opts: OpenOpts) -> io::Result<VfsHandle> {
        let p = self.abs(path);
        let mut st = self.lock();
        Self::tick(&mut st)?;
        let f = opts.std().open(&p)?;
        let ino = if self.tracked(&p) {
            let ino = match st.cur.get(&p) {
                Some(&ino) => ino,
                None => {
                    let ino = st.next_ino;
                    st.next_ino += 1;
                    st.inodes.insert(ino, Inode { dirty_from: u64::MAX, ..Inode::default() });
                    st.cur.insert(p.clone(), ino);
                    st.pending.push(DirOp::Create(p.clone(), ino));
                    ino
                }
            };
            if opts.truncate {
                if let Some(n) = st.inodes.get_mut(&ino) {
                    n.dirty_from = 0;
                }
            }
            Some(ino)
        } else {
            None
        };
        let epoch = st.epoch;
        drop(st);
        Ok(Box::new(SimFile { f, ino, vfs: self.clone(), epoch, append: opts.append }))
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let (a, b) = (self.abs(from), self.abs(to));
        let mut st = self.lock();
        Self::tick(&mut st)?;
        let displaced = st.cur.get(&b).copied();
        let displaced_data = displaced.map(|_| fs::read(&b)).transpose()?;
        fs::rename(&a, &b)?;
        if let Some(ino) = st.cur.remove(&a) {
            if let (Some(old), Some(data)) = (displaced, displaced_data) {
                if let Some(n) = st.inodes.get_mut(&old) {
                    n.unlinked = Some(data);
                }
            }
            st.cur.insert(b.clone(), ino);
            st.pending.push(DirOp::Rename(a, b));
        } else if self.tracked(&b) {
            // Renamed in from outside the root: appears as a new (unsynced) file.
            let ino = st.next_ino;
            st.next_ino += 1;
            st.inodes.insert(ino, Inode { dirty_from: 0, ..Inode::default() });
            st.cur.insert(b.clone(), ino);
            st.pending.push(DirOp::Create(b, ino));
        }
        Ok(())
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        let p = self.abs(path);
        let mut st = self.lock();
        Self::tick(&mut st)?;
        let data = if st.cur.contains_key(&p) { fs::read(&p).ok() } else { None };
        fs::remove_file(&p)?;
        if let Some(ino) = st.cur.remove(&p) {
            if let Some(n) = st.inodes.get_mut(&ino) {
                n.unlinked = data;
            }
            st.pending.push(DirOp::Remove(p));
        }
        Ok(())
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        let p = self.abs(path);
        let mut st = self.lock();
        Self::tick(&mut st)?;
        let under: Vec<(PathBuf, u64)> = st.cur.range(p.clone()..).take_while(|(k, _)| k.starts_with(&p)).map(|(k, v)| (k.clone(), *v)).collect();
        for (f, ino) in &under {
            let data = fs::read(f).ok();
            if let Some(n) = st.inodes.get_mut(ino) {
                n.unlinked = data;
            }
        }
        fs::remove_dir_all(&p)?;
        for (f, _) in under {
            st.cur.remove(&f);
            st.pending.push(DirOp::Remove(f));
        }
        Ok(())
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        let mut st = self.lock();
        Self::tick(&mut st)?;
        // Directories themselves are always durable in this model; their entries are not.
        fs::create_dir_all(self.abs(path))
    }

    fn sync_dir(&self, path: &Path) -> io::Result<()> {
        let d = self.abs(path);
        let mut st = self.lock();
        Self::tick(&mut st)?;
        // The journal commits in order: everything up to the last change in `d` is durable.
        if let Some(last) = st.pending.iter().rposition(|op| op.touches(&d)) {
            let done: Vec<DirOp> = st.pending.drain(..=last).collect();
            let mut dur = std::mem::take(&mut st.dur);
            for op in &done {
                op.apply(&mut dur);
            }
            st.dur = dur;
        }
        Ok(())
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        if self.lock().frozen {
            return Err(crashed());
        }
        fs::read_dir(path)?.map(|e| e.map(|e| e.path())).collect()
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        if self.lock().frozen {
            return Err(crashed());
        }
        fs::read(path)
    }

    fn is_simulated(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("storine_vfs_{name}_{}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        d
    }

    #[test]
    fn synced_data_survives_every_cut() {
        let d = dir("synced");
        for seed in 1..200 {
            let _ = fs::remove_dir_all(&d);
            let sim = SimVfs::new(&d, seed).unwrap();
            let p = d.join("f");
            let mut f = sim.open(&p, OpenOpts::create()).unwrap();
            f.write_all(&[7u8; 3000]).unwrap();
            f.sync_all().unwrap();
            sim.sync_dir(&d).unwrap();
            f.write_all(&[9u8; 2000]).unwrap();
            drop(f);
            sim.power_cut().unwrap();
            let got = fs::read(&p).unwrap();
            assert!(got.len() >= 3000 && got.len() <= 5000, "seed {seed}: len {}", got.len());
            assert!(got[..3000].iter().all(|&b| b == 7), "seed {seed}: synced bytes changed");
            assert!(got[3000..].iter().all(|&b| b == 9 || b == 0), "seed {seed}");
        }
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn unsynced_writes_are_dropped_torn_and_reordered() {
        let d = dir("torn");
        let (mut torn, mut reordered, mut lost) = (0, 0, 0);
        for seed in 1..300 {
            let _ = fs::remove_dir_all(&d);
            let sim = SimVfs::new(&d, seed).unwrap();
            let p = d.join("f");
            let mut f = sim.open(&p, OpenOpts::create()).unwrap();
            f.sync_all().unwrap();
            sim.sync_dir(&d).unwrap();
            // Two 4-sector writes after the last sync.
            f.write_all(&[1u8; 2048]).unwrap();
            f.write_all(&[2u8; 2048]).unwrap();
            drop(f);
            sim.power_cut().unwrap();
            let got = fs::read(&p).unwrap();
            let first: Vec<u8> = got.iter().take(2048).copied().collect();
            let second: Vec<u8> = got.iter().skip(2048).copied().collect();
            let full = |s: &[u8], v| s.len() == 2048 && s.iter().all(|&b| b == v);
            if got.is_empty() {
                lost += 1;
            }
            if !first.is_empty() && !full(&first, 1) && first.contains(&1) {
                torn += 1;
            }
            if !full(&first, 1) && second.contains(&2) {
                reordered += 1;
            }
        }
        assert!(torn > 0 && reordered > 0 && lost > 0, "torn {torn} reordered {reordered} lost {lost}");
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn renames_need_a_directory_sync() {
        let d = dir("rename");
        let (mut old, mut new) = (0, 0);
        for seed in 1..100 {
            let _ = fs::remove_dir_all(&d);
            let sim = SimVfs::new(&d, seed).unwrap();
            let p = d.join("data");
            sim.write_atomic(&p, b"old").unwrap();
            // Atomic replace without the final directory sync.
            let tmp = d.join("data.tmp");
            let mut f = sim.open(&tmp, OpenOpts::create_truncate()).unwrap();
            f.write_all(b"new").unwrap();
            f.sync_all().unwrap();
            drop(f);
            sim.rename(&tmp, &p).unwrap();
            sim.power_cut().unwrap();
            match fs::read(&p).unwrap().as_slice() {
                b"old" => old += 1,
                b"new" => new += 1,
                other => panic!("seed {seed}: torn rename {other:?}"),
            }
        }
        assert!(old > 0 && new > 0, "old {old} new {new}");
        // With the directory sync, the new contents always survive.
        for seed in 1..50 {
            let _ = fs::remove_dir_all(&d);
            let sim = SimVfs::new(&d, seed).unwrap();
            let p = d.join("data");
            sim.write_atomic(&p, b"old").unwrap();
            sim.write_atomic(&p, b"new").unwrap();
            sim.power_cut().unwrap();
            assert_eq!(fs::read(&p).unwrap(), b"new");
        }
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn unsynced_creates_and_removes_roll_back() {
        let d = dir("ns");
        for seed in 1..100 {
            let _ = fs::remove_dir_all(&d);
            let sim = SimVfs::new(&d, seed).unwrap();
            sim.write_atomic(&d.join("kept"), b"k").unwrap();
            sim.write(&d.join("maybe"), b"m").unwrap();
            sim.remove_file(&d.join("kept")).unwrap();
            sim.set_mode(CutMode::LoseAll);
            sim.power_cut().unwrap();
            assert_eq!(fs::read(d.join("kept")).unwrap(), b"k", "seed {seed}");
            assert!(!d.join("maybe").exists());
        }
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn crash_points_fail_every_later_call_and_keep_written_data() {
        let d = dir("crash");
        let sim = SimVfs::new(&d, 1).unwrap();
        let p = d.join("f");
        sim.fail_at(3);
        let mut f = sim.open(&p, OpenOpts::create()).unwrap(); // call 1
        f.write_all(b"abc").unwrap(); // call 2
        assert!(f.write_all(b"def").is_err()); // call 3: the crash point
        assert!(f.sync_data().is_err());
        assert!(sim.open(&p, OpenOpts::read()).is_err());
        drop(f);
        sim.crash().unwrap();
        // A process crash keeps unsynced writes (the page cache survives).
        assert_eq!(fs::read(&p).unwrap(), b"abc");
        // The disk works again after the restart, and old handles stay dead.
        let mut f = sim.open(&p, OpenOpts::append()).unwrap();
        f.write_all(b"!").unwrap();
        f.sync_data().unwrap();
        assert_eq!(fs::read(&p).unwrap(), b"abc!");
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn mapped_and_positional_writes_are_seen_at_sync() {
        let d = dir("mapped");
        let sim = SimVfs::new(&d, 5).unwrap();
        let p = d.join("f");
        let f = sim.open(&p, OpenOpts::create()).unwrap();
        f.set_len(1024).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::FileExt::write_all_at(f.file(), b"xyz", 100).unwrap();
        #[cfg(windows)]
        std::os::windows::fs::FileExt::seek_write(f.file(), b"xyz", 100).unwrap();
        f.sync_data().unwrap();
        sim.sync_dir(&d).unwrap();
        drop(f);
        sim.set_mode(CutMode::LoseAll);
        sim.power_cut().unwrap();
        let got = fs::read(&p).unwrap();
        assert_eq!(&got[100..103], b"xyz");
        assert!(sim.untracked().is_empty());
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn mounting_routes_paths_under_the_root() {
        let d = dir("mount");
        let sim = SimVfs::new(&d, 3).unwrap();
        assert!(!at(&d.join("x")).is_simulated());
        {
            let _g = sim.mount();
            assert!(at(&d.join("a/b")).is_simulated());
            assert!(at(&d.join("a/../c")).is_simulated());
            assert!(!at(&std::env::temp_dir().join("elsewhere")).is_simulated());
        }
        assert!(!at(&d.join("x")).is_simulated());
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn same_seed_same_disk() {
        let d1 = dir("seed_a");
        let d2 = dir("seed_b");
        let run = |d: &Path| {
            let sim = SimVfs::new(d, 42).unwrap();
            let mut f = sim.open(&d.join("f"), OpenOpts::create()).unwrap();
            for i in 0..50u8 {
                f.write_all(&[i; 300]).unwrap();
                if i % 7 == 0 {
                    f.sync_data().unwrap();
                }
            }
            drop(f);
            let r = sim.power_cut().unwrap();
            (r, fs::read(d.join("f")).unwrap_or_default())
        };
        assert_eq!(run(&d1), run(&d2));
        let _ = fs::remove_dir_all(&d1);
        let _ = fs::remove_dir_all(&d2);
    }
}
