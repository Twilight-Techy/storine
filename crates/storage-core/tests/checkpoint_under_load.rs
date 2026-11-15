//! Checkpoints under writers that never pause: each ends, and every write survives a reopen.
//!
//! `flush_all` (and `checkpoint`, which calls it) once drained the flush queue until it was
//! empty, which writers re-dirtying records faster than it flushed kept from ever happening
//! (`compaction-cost` ran for minutes and filled the disk). The deterministic regression test
//! is `engine::checkpoint_drain_tests`; this one runs the real flusher and writers, and
//! `storage-wal`'s `test_truncation_throttles_writers_it_cannot_gain_on` covers writers that
//! outpace the disk.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Duration;
use storage_core::document::DOCUMENT_LAYOUT;
use storage_core::{AgentObjectRecord, EngineConfig, StorageEngine, StorageLayoutMode};
use storage_types::InlineId;
use storage_wal::SyncPolicy;

const WRITERS: u64 = 3;
/// Far longer than a bounded checkpoint of a few thousand records takes on any runner.
const DEADLINE: Duration = Duration::from_secs(60);

fn checkpoints_end_while_writers_run(layout: StorageLayoutMode, name: &str, keys_per_writer: u64) {
    let dir = std::env::temp_dir().join(format!("stge_ckpt_load_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = EngineConfig { layout_mode: layout, wal_sync_policy: SyncPolicy::AsyncMemoryFirst, ..EngineConfig::default() };
    let engine = Arc::new(StorageEngine::<AgentObjectRecord>::open(&dir, cfg.clone()).unwrap());
    let stop = Arc::new(AtomicBool::new(false));
    let mut writers = Vec::new();
    for w in 0..WRITERS {
        let (engine, stop) = (Arc::clone(&engine), Arc::clone(&stop));
        writers.push(std::thread::spawn(move || {
            // Each writer owns its keys, so its last value for each is the final one.
            let mut last: HashMap<String, i64> = HashMap::new();
            let mut n = 0i64;
            while !stop.load(Ordering::Relaxed) || n < 4 * keys_per_writer as i64 {
                let key = format!("w{w}k{:04}", n as u64 % keys_per_writer);
                let id = InlineId::new(&key).unwrap();
                let mut rec = AgentObjectRecord::new(id, "t", n, n);
                rec.body = vec![b'x'; 64 + (n as usize % 4) * 64];
                engine.put(id, &rec).unwrap();
                last.insert(key, n);
                n += 1;
                // About 10,000 writes a second each: steady load the disk keeps up with. Writers
                // logging faster than the disk writes make every checkpoint copy gigabytes of
                // log (`checkpoint_truncate_before` then holds them back to finish).
                if n % 10 == 0 {
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
            last
        }));
    }
    for round in 0..3 {
        let (tx, rx) = mpsc::channel();
        let e = Arc::clone(&engine);
        let h = std::thread::spawn(move || {
            let _ = tx.send(e.checkpoint().map(|_| ()));
        });
        match rx.recv_timeout(DEADLINE) {
            Ok(r) => r.unwrap(),
            Err(_) => panic!("{name}: checkpoint {round} did not end within {DEADLINE:?} while writers ran"),
        }
        // Its engine handle must be gone before the reopen below.
        h.join().unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    let mut expected = HashMap::new();
    for h in writers {
        expected.extend(h.join().unwrap());
    }
    drop(engine);
    let engine = StorageEngine::<AgentObjectRecord>::open(&dir, cfg).unwrap();
    for (key, n) in &expected {
        let (_, rec) = engine.get_owned(InlineId::new(key).unwrap()).unwrap_or_else(|e| panic!("{name}: {key} lost: {e}"));
        assert_eq!(rec.timestamp, *n, "{name}: {key} reopened with an older value");
    }
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn checkpoints_end_while_writers_run_arena() {
    checkpoints_end_while_writers_run(DOCUMENT_LAYOUT, "arena", 500);
}

#[test]
fn checkpoints_end_while_writers_run_dedicated_files() {
    // One file, `fsync` and rename per record: fewer keys keep it to seconds on a slow disk.
    checkpoints_end_while_writers_run(StorageLayoutMode::DedicatedFiles, "files", 100);
}
