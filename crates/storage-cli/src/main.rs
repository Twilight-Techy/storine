//! Command-Line Interface (`storage-cli`) for inspecting, mutating, verifying, snapshotting,
//! and benchmarking the Zero-Copy Object Storage Engine (`TASK-028`, `F-OPS-07`, `RULE-SAFE-01`).

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations, rust_2018_idioms)]

use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::process::ExitCode;
use std::time::Instant;

use storage_core::{
    backup::{archive_wal_for_pitr, create_online_backup, restore_point_in_time},
    AgentObjectRecord, EngineConfig, StorageEngine, StorageLayoutMode,
};
use storage_server::StorageClient;
use storage_types::{InlineId, RecordHeader};

fn main() -> ExitCode {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        print_usage();
        return ExitCode::from(1);
    }

    match run_command(&args[1..]) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::from(1)
        }
    }
}

fn print_usage() {
    eprintln!(
        "Usage: storage-cli <command> [args...]\n\
         Commands:\n\
         \x20 stats <db_dir>                                    Show engine & mmap telemetry\n\
         \x20 put <db_dir> <id> <title> <score> [tags_csv]      Upsert an AgentObjectRecord\n\
         \x20 get <db_dir> <id>                                 Zero-copy read a record by ID\n\
         \x20 inspect-header <rec_file_path>                    Inspect & verify a 64B .rec header\n\
         \x20 verify <db_dir>                                   Verify checksums & relationship integrity\n\
         \x20 compact <db_dir> [min_ratio]                      Compact fragmented arena segments\n\
         \x20 snapshot <db_dir> <dest_dir>                      Create a point-in-time snapshot backup\n\
         \x20 bench <db_dir> [count]                            Run quick CLI throughput benchmark\n\
         \x20 backup <db_dir> <backup_dir>                      Online hot backup with WAL LSN manifest\n\
         \x20 archive-wal <db_dir> <backup_dir>                 Archive WAL segment for PITR replay\n\
         \x20 restore-pitr <backup_dir> <restore_dir> <ts_ns>   Restore database to a nanosecond timestamp\n\
         \x20 remote-health <addr>                              Health check a remote StorageServer\n\
         \x20 remote-compact <addr>                             Trigger remote arena compaction (admin)\n\
         \x20 remote-backup <addr> <dest_path>                  Trigger remote online backup (admin)\n\
         \x20 remote-checkpoint <addr>                          Trigger remote incremental checkpoint (admin)\n\
         \x20 remote-metrics <addr>                             Scrape Prometheus metrics from remote server"
    );
}

fn run_command(args: &[String]) -> storage_types::Result<()> {
    let cmd = args[0].as_str();
    match cmd {
        "stats" => {
            let db_dir = args.get(1).map(|s| s.as_str()).unwrap_or("./data");
            let engine = StorageEngine::<AgentObjectRecord>::open(db_dir, EngineConfig::default())?;
            let st = engine.stats();
            println!("=== StorageEngine Telemetry ({db_dir}) ===");
            println!("Live Records      : {}", st.live_records);
            println!("Hot Dirty Bytes   : {}", st.hot_dirty_bytes);
            println!("Total Reads       : {}", st.total_reads);
            println!("Total Writes      : {}", st.total_writes);
            println!("Coalesced Flushes : {}", st.coalesced_flushes);
            println!("WAL Batches/Frames: {}/{}", st.wal_batches, st.wal_frames);
            println!("Active Mmaps      : {}", st.active_mmaps);
            println!("Mapped Bytes      : {}", st.mapped_bytes);
            Ok(())
        }
        "put" => {
            if args.len() < 5 {
                print_usage();
                return Err(storage_types::StorageError::invalid_key(
                    "missing required arguments for 'put'",
                ));
            }
            let db_dir = &args[1];
            let id = InlineId::new(&args[2])?;
            let title = &args[3];
            let score: i64 = args[4].parse().map_err(|_| {
                storage_types::StorageError::invalid_key("score must be a valid 64-bit integer")
            })?;
            let mut rec = AgentObjectRecord::new(id, title.clone(), 1_700_000_000, score);
            if let Some(tags_csv) = args.get(5) {
                for t in tags_csv.split(',').filter(|s| !s.is_empty()) {
                    rec.tags.push(InlineId::new(t.trim())?);
                }
            }
            let engine = StorageEngine::<AgentObjectRecord>::open(db_dir, EngineConfig::default())?;
            let ver = engine.put(id, &rec)?;
            engine.flush_all()?;
            println!("OK: stored '{id}' at version v{}", ver.0);
            Ok(())
        }
        "get" => {
            if args.len() < 3 {
                print_usage();
                return Err(storage_types::StorageError::invalid_key(
                    "missing required arguments for 'get'",
                ));
            }
            let db_dir = &args[1];
            let id = InlineId::new(&args[2])?;
            let engine = StorageEngine::<AgentObjectRecord>::open(db_dir, EngineConfig::default())?;
            match engine.with_ref(id, |view| {
                println!("ID       : {}", view.id);
                println!("Version  : v{}", view.version().0);
                println!("Title    : {}", view.get_str(&view.title));
                println!("Score    : {}", view.score);
                println!("Timestamp: {}", view.timestamp);
                println!("Tags     : {:?}", view.get_slice(&view.tags));
            }) {
                Ok(()) => Ok(()),
                Err(storage_types::StorageError::InvalidArchiveLayout { .. }) => {
                    let (ver, rec) = engine.get_owned(id)?;
                    println!("ID       : {}", rec.id);
                    println!("Version  : v{} (cold-compressed)", ver.0);
                    println!("Title    : {}", rec.title);
                    println!("Score    : {}", rec.score);
                    println!("Timestamp: {}", rec.timestamp);
                    println!("Tags     : {:?}", rec.tags);
                    Ok(())
                }
                Err(e) => Err(e),
            }
        }
        "inspect-header" => {
            if args.len() < 2 {
                print_usage();
                return Err(storage_types::StorageError::invalid_key(
                    "missing required <rec_file_path> for 'inspect-header'",
                ));
            }
            let path = Path::new(&args[1]);
            let bytes =
                fs::read(path).map_err(|e| storage_types::StorageError::io("read .rec file", e))?;
            let hdr = RecordHeader::from_slice_verified(&bytes)?;
            println!("=== Verified 64-Byte RecordHeader ===");
            println!("Magic            : {:#010x} (\"STGE\")", hdr.magic);
            println!("Format Version   : {}", hdr.format_version);
            println!("Schema Version   : {}", hdr.schema_version);
            println!("Record Version   : v{}", hdr.record_version);
            println!("Updated At (ns)  : {}", hdr.updated_at_ns);
            println!("Payload Length   : {} bytes", hdr.payload_len);
            println!("Uncompressed Len : {} bytes", hdr.uncompressed_len);
            println!("Root Offset      : {} bytes", hdr.root_offset);
            println!("Payload Checksum : {:#018x}", hdr.payload_checksum);
            println!("Header Checksum  : {:#018x}", hdr.header_checksum);
            Ok(())
        }
        "verify" => {
            let db_dir = args.get(1).map(|s| s.as_str()).unwrap_or("./data");
            let engine = StorageEngine::<AgentObjectRecord>::open(db_dir, EngineConfig::default())?;
            let dangling = engine.verify_relationships(|r| r.all_related_ids());
            println!(
                "Verified {} live records; dangling relationship edges: {}",
                engine.stats().live_records,
                dangling.len()
            );
            Ok(())
        }
        "compact" => {
            let db_dir = args.get(1).map(|s| s.as_str()).unwrap_or("./data");
            let min_ratio: f64 = match args.get(2) {
                Some(s) => s.parse().map_err(|_| {
                    storage_types::StorageError::invalid_key(
                        "min_ratio must be a float between 0.0 and 1.0",
                    )
                })?,
                None => 0.5,
            };
            let cfg = EngineConfig {
                layout_mode: StorageLayoutMode::AdaptiveArena {
                    arena_threshold_bytes: 4096,
                    segment_capacity: 8 * 1024 * 1024,
                },
                ..EngineConfig::default()
            };
            let engine = StorageEngine::<AgentObjectRecord>::open(db_dir, cfg)?;
            let removed = engine.compact_arenas(min_ratio)?;
            println!("Compacted {removed} fragmented arena segment(s) in {db_dir}");
            Ok(())
        }
        "snapshot" => {
            if args.len() < 3 {
                print_usage();
                return Err(storage_types::StorageError::invalid_key(
                    "missing required <db_dir> and <dest_dir> for 'snapshot'",
                ));
            }
            let db_dir = &args[1];
            let dest_dir = Path::new(&args[2]);
            let engine = StorageEngine::<AgentObjectRecord>::open(db_dir, EngineConfig::default())?;
            let copied = engine.create_snapshot(dest_dir)?;
            println!("Snapshot created at {:?} ({copied} files copied)", dest_dir);
            Ok(())
        }
        "bench" => {
            let db_dir = args.get(1).map(|s| s.as_str()).unwrap_or("./bench_tmp_cli");
            let count: usize = match args.get(2) {
                Some(s) => s.parse().map_err(|_| {
                    storage_types::StorageError::invalid_key("count must be a positive integer")
                })?,
                None => 5_000,
            };
            let cfg = EngineConfig {
                layout_mode: StorageLayoutMode::AdaptiveArena {
                    arena_threshold_bytes: 4096,
                    segment_capacity: 8 * 1024 * 1024,
                },
                wal_sync_policy: storage_wal::SyncPolicy::AsyncMemoryFirst,
                ..EngineConfig::default()
            };
            let engine = StorageEngine::<AgentObjectRecord>::open(db_dir, cfg)?;
            let mut keys = Vec::with_capacity(count);
            for i in 0..count {
                keys.push(InlineId::new(&format!("key_{i:06}"))?);
            }

            let start = Instant::now();
            for (i, &id) in keys.iter().enumerate() {
                let rec = AgentObjectRecord::new(id, "Benchmark Record", i as i64, (i * 10) as i64);
                engine.put(id, &rec)?;
            }
            engine.flush_all()?;
            let write_elapsed = start.elapsed();

            let r_start = Instant::now();
            let mut sum = 0i64;
            for &id in &keys {
                engine.with_ref(id, |v| {
                    sum = sum.wrapping_add(v.score);
                })?;
            }
            let read_elapsed = r_start.elapsed();
            println!(
                "CLI Bench ({count} records, checksum={sum}): Write {:?} ({:.0} ops/s), Zero-Copy Read {:?} ({:.0} ops/s)",
                write_elapsed,
                (count as f64) / write_elapsed.as_secs_f64().max(1e-9),
                read_elapsed,
                (count as f64) / read_elapsed.as_secs_f64().max(1e-9)
            );
            Ok(())
        }
        "backup" => {
            if args.len() < 3 {
                print_usage();
                return Err(storage_types::StorageError::invalid_key(
                    "missing required <db_dir> and <backup_dir> for 'backup'",
                ));
            }
            let db_dir = &args[1];
            let backup_dir = Path::new(&args[2]);
            let engine = StorageEngine::<AgentObjectRecord>::open(db_dir, EngineConfig::default())?;
            let manifest = create_online_backup(&engine, backup_dir)?;
            println!(
                "Online backup created at {:?}\n  Collection : {}\n  Base LSN   : {}\n  Files      : {}",
                backup_dir, manifest.collection_name, manifest.base_lsn, manifest.files_copied,
            );
            Ok(())
        }
        "archive-wal" => {
            if args.len() < 3 {
                print_usage();
                return Err(storage_types::StorageError::invalid_key(
                    "missing required <db_dir> and <backup_dir> for 'archive-wal'",
                ));
            }
            let db_dir = &args[1];
            let backup_dir = Path::new(&args[2]);
            let engine = StorageEngine::<AgentObjectRecord>::open(db_dir, EngineConfig::default())?;
            let archived = archive_wal_for_pitr(&engine, backup_dir)?;
            println!("Archived {archived} WAL segment(s) to {:?}/wal_pitr", backup_dir);
            Ok(())
        }
        "restore-pitr" => {
            if args.len() < 4 {
                print_usage();
                return Err(storage_types::StorageError::invalid_key(
                    "missing required <backup_dir>, <restore_dir>, and <target_ts_ns> for 'restore-pitr'",
                ));
            }
            let backup_dir = Path::new(&args[1]);
            let restore_dir = Path::new(&args[2]);
            let target_ts_ns: u64 = args[3].parse().map_err(|_| {
                storage_types::StorageError::invalid_key(
                    "target_ts_ns must be a valid 64-bit unsigned integer (nanoseconds since epoch)",
                )
            })?;
            let engine = restore_point_in_time::<AgentObjectRecord>(
                backup_dir,
                restore_dir,
                EngineConfig::default(),
                target_ts_ns,
            )?;
            let st = engine.stats();
            println!(
                "PITR restored to {:?} (target_ts_ns={})\n  Live Records: {}",
                restore_dir, target_ts_ns, st.live_records,
            );
            Ok(())
        }
        "remote-health" => {
            let addr = parse_addr(args.get(1))?;
            let mut client = StorageClient::connect(addr)?;
            let health = client.health()?;
            println!("{health}");
            Ok(())
        }
        "remote-compact" => {
            let addr = parse_addr(args.get(1))?;
            let mut client = StorageClient::connect(addr)?;
            let msg = client.admin_compact()?;
            println!("{msg}");
            Ok(())
        }
        "remote-backup" => {
            if args.len() < 3 {
                print_usage();
                return Err(storage_types::StorageError::invalid_key(
                    "missing required <addr> and <dest_path> for 'remote-backup'",
                ));
            }
            let addr = parse_addr(Some(&args[1]))?;
            let dest_path = &args[2];
            let mut client = StorageClient::connect(addr)?;
            let msg = client.admin_backup(dest_path)?;
            println!("{msg}");
            Ok(())
        }
        "remote-checkpoint" => {
            let addr = parse_addr(args.get(1))?;
            let mut client = StorageClient::connect(addr)?;
            let msg = client.admin_checkpoint()?;
            println!("{msg}");
            Ok(())
        }
        "remote-metrics" => {
            let addr = parse_addr(args.get(1))?;
            let mut client = StorageClient::connect(addr)?;
            let metrics = client.scrape_metrics()?;
            print!("{metrics}");
            Ok(())
        }
        _ => {
            print_usage();
            Err(storage_types::StorageError::invalid_key("unknown CLI command"))
        }
    }
}

/// Parses a `host:port` string into a `SocketAddr` with user-friendly error messages.
fn parse_addr(arg: Option<&String>) -> storage_types::Result<SocketAddr> {
    let s = arg.ok_or_else(|| {
        storage_types::StorageError::invalid_key("missing required <addr> (host:port)")
    })?;
    s.parse::<SocketAddr>().map_err(|_| {
        storage_types::StorageError::invalid_key("addr must be a valid socket address (e.g. 127.0.0.1:9090)")
    })
}
