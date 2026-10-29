//! Spill runs of partial aggregates (`RULE-OLAP-04`): when group tables outgrow the memory
//! budget, workers write their partial groups to hash partitions on disk and continue empty;
//! each partition is merged and finished on its own afterwards.
//!
//! Runs are sealed with a key that exists only in this process's memory (never written
//! anywhere): spilled user data is unreadable on disk whatever the encryption configuration,
//! and a crash leaves only ciphertext, swept by the next query.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use storage_codec::crypto;
use storage_types::{Result, StorageError};

/// Hash partitions per query: a spilled query merges `1 / PARTITIONS` of its groups at a time.
pub(crate) const PARTITIONS: usize = 64;
/// Plaintext bytes per sealed frame.
const FRAME: usize = 1 << 20;
/// Default disk budget of one query's spill runs.
pub const DEFAULT_SPILL_BUDGET: u64 = 16 << 30;

static SEQ: AtomicU64 = AtomicU64::new(0);

struct Part {
    file: Option<File>,
    buf: Vec<u8>,
    frames: u64,
}

pub(crate) struct Spill {
    dir: PathBuf,
    prefix: String,
    key: [u8; 32],
    parts: Vec<Mutex<Part>>,
    written: AtomicU64,
    limit: u64,
}

fn io(e: std::io::Error) -> StorageError {
    StorageError::io("analytic query spill run", e)
}

impl Spill {
    pub(crate) fn new(dir: &Path, limit: u64) -> Result<Self> {
        std::fs::create_dir_all(dir).map_err(io)?;
        // Runs of queries that died with their process: their key is gone, delete them.
        let me = format!("agg-{}-", std::process::id());
        if let Ok(entries) = std::fs::read_dir(dir) {
            for e in entries.flatten() {
                let name = e.file_name().to_string_lossy().into_owned();
                if name.starts_with("agg-") && name.ends_with(".run") && !name.starts_with(&me) {
                    let _ = std::fs::remove_file(e.path());
                }
            }
        }
        let key = storage_codec::rand::os_random_array::<32>().map_err(io)?;
        let prefix = format!("{me}{}", SEQ.fetch_add(1, Ordering::Relaxed));
        let parts = (0..PARTITIONS).map(|_| Mutex::new(Part { file: None, buf: Vec::new(), frames: 0 })).collect();
        Ok(Self { dir: dir.to_owned(), prefix, key, parts, written: AtomicU64::new(0), limit })
    }

    fn path(&self, p: usize) -> PathBuf {
        self.dir.join(format!("{}-{p}.run", self.prefix))
    }

    fn aad(&self, p: usize, frame: u64) -> Vec<u8> {
        format!("{}/{p}/{frame}", self.prefix).into_bytes()
    }

    /// Appends one encoded record to partition `p`.
    pub(crate) fn append(&self, p: usize, record: &[u8]) -> Result<()> {
        let mut part = self.parts[p].lock().unwrap_or_else(|e| e.into_inner());
        part.buf.extend_from_slice(&(record.len() as u32).to_le_bytes());
        part.buf.extend_from_slice(record);
        if part.buf.len() >= FRAME {
            self.flush(p, &mut part)?;
        }
        Ok(())
    }

    fn flush(&self, p: usize, part: &mut Part) -> Result<()> {
        if part.buf.is_empty() {
            return Ok(());
        }
        let sealed = crypto::seal(&self.key, &self.aad(p, part.frames), &part.buf);
        let total = self.written.fetch_add(sealed.len() as u64 + 4, Ordering::Relaxed) + sealed.len() as u64 + 4;
        if total > self.limit {
            return Err(StorageError::invalid_value(
                "the query needs more memory and spill disk than its budgets: narrow it with a filter, group by fewer values, or raise memory_budget / spill_budget",
            ));
        }
        if part.file.is_none() {
            let f = OpenOptions::new().create_new(true).read(true).write(true).open(self.path(p)).map_err(io)?;
            part.file = Some(f);
        }
        let f = part.file.as_mut().ok_or_else(|| io(std::io::Error::other("no file")))?;
        f.write_all(&(sealed.len() as u32).to_le_bytes()).map_err(io)?;
        f.write_all(&sealed).map_err(io)?;
        part.frames += 1;
        part.buf.clear();
        Ok(())
    }

    /// `true` once anything was spilled.
    pub(crate) fn used(&self) -> bool {
        self.parts.iter().any(|p| {
            let p = p.lock().unwrap_or_else(|e| e.into_inner());
            p.frames > 0 || !p.buf.is_empty()
        })
    }

    /// Calls `f` with every record of partition `p`, in write order.
    pub(crate) fn read(&self, p: usize, mut f: impl FnMut(&[u8]) -> Result<()>) -> Result<()> {
        let mut part = self.parts[p].lock().unwrap_or_else(|e| e.into_inner());
        self.flush(p, &mut part)?;
        let frames = part.frames;
        let Some(file) = part.file.as_mut() else { return Ok(()) };
        file.seek(SeekFrom::Start(0)).map_err(io)?;
        let mut frame_buf = Vec::new();
        for frame in 0..frames {
            let mut len = [0u8; 4];
            file.read_exact(&mut len).map_err(io)?;
            frame_buf.resize(u32::from_le_bytes(len) as usize, 0);
            file.read_exact(&mut frame_buf).map_err(io)?;
            let plain = crypto::open(&self.key, &self.aad(p, frame), &frame_buf)?;
            let mut at = 0;
            while at < plain.len() {
                let n = plain.get(at..at + 4).map_or(0, |b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize);
                let rec = plain.get(at + 4..at + 4 + n).ok_or_else(|| StorageError::invalid_value("corrupt spill run: record length"))?;
                f(rec)?;
                at += 4 + n;
            }
        }
        // A partition is read once: its file goes as soon as it has been merged.
        part.file = None;
        let _ = std::fs::remove_file(self.path(p));
        Ok(())
    }

    pub(crate) fn bytes_written(&self) -> u64 {
        self.written.load(Ordering::Relaxed)
    }
}

impl Drop for Spill {
    fn drop(&mut self) {
        for p in 0..PARTITIONS {
            let _ = std::fs::remove_file(self.path(p));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runs_round_trip_hold_no_plaintext_and_are_removed() {
        let dir = std::env::temp_dir().join(format!("stge_spill_unit_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        // A leftover of a dead process is swept.
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("agg-1-0-3.run"), b"old").unwrap();
        let s = Spill::new(&dir, 1 << 30).unwrap();
        assert!(!dir.join("agg-1-0-3.run").exists());
        let records: Vec<Vec<u8>> = (0..5000u32).map(|i| format!("SECRET-LABEL-{i}").into_bytes()).collect();
        for (i, r) in records.iter().enumerate() {
            s.append(i % 3, r).unwrap();
        }
        assert!(s.used());
        // Force the buffered tail of partition 0 to disk and inspect every file.
        {
            let mut p = s.parts[0].lock().unwrap();
            s.flush(0, &mut p).unwrap();
        }
        let raw = std::fs::read(s.path(0)).unwrap();
        assert!(!raw.windows(6).any(|w| w == b"SECRET"), "spill run holds plaintext");
        for p in 0..3 {
            let mut got = Vec::new();
            s.read(p, |r| {
                got.push(r.to_vec());
                Ok(())
            })
            .unwrap();
            let want: Vec<Vec<u8>> = records.iter().enumerate().filter(|(i, _)| i % 3 == p).map(|(_, r)| r.clone()).collect();
            assert_eq!(got, want);
            assert!(!s.path(p).exists(), "a read partition is removed");
        }
        // A disk budget below the runs fails cleanly.
        let small = Spill::new(&dir, 1000).unwrap();
        let err = (0..20_000).try_for_each(|i| small.append(0, format!("{i:0100}").as_bytes())).unwrap_err();
        assert!(format!("{err:?}").contains("spill"), "{err:?}");
        drop(small);
        drop(s);
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
