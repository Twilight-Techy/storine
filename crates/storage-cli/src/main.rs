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
         \x20 remote-backup <addr> <backup_name>                Trigger remote backup under the server's backup root (admin)\n\
         \x20 remote-checkpoint <addr>                          Trigger remote incremental checkpoint (admin)\n\
         \x20 remote-metrics <addr>                             Scrape Prometheus metrics from remote server\n\
         \x20 cluster <addr> members                            List cluster members, roles and the leader\n\
         \x20 cluster <addr> add <id> <host:port>               Add a voter (learner until caught up) (admin)\n\
         \x20 cluster <addr> add-learner <id> <host:port>       Add a non-voting learner (admin)\n\
         \x20 cluster <addr> promote <id>                       Promote a caught-up learner to voter (admin)\n\
         \x20 cluster <addr> remove <id>                        Remove a member, the leader included (admin)\n\
         \n\
         Remote commands authenticate with the STORAGE_TOKEN environment variable when set.\n\
         cluster changes can be sent to any member: a follower's redirect to the leader is followed.\n\
         backup / archive-wal / restore-pitr enable WAL archiving (required for PITR)."
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
            let engine = StorageEngine::<AgentObjectRecord>::open(db_dir, pitr_config())?;
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
            let engine = StorageEngine::<AgentObjectRecord>::open(db_dir, pitr_config())?;
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
                pitr_config(),
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
            let mut client = connect_remote(addr)?;
            let health = client.health()?;
            println!("{health}");
            Ok(())
        }
        "remote-compact" => {
            let addr = parse_addr(args.get(1))?;
            let mut client = connect_remote(addr)?;
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
            let mut client = connect_remote(addr)?;
            let msg = client.admin_backup(dest_path)?;
            println!("{msg}");
            Ok(())
        }
        "remote-checkpoint" => {
            let addr = parse_addr(args.get(1))?;
            let mut client = connect_remote(addr)?;
            let msg = client.admin_checkpoint()?;
            println!("{msg}");
            Ok(())
        }
        "remote-metrics" => {
            let addr = parse_addr(args.get(1))?;
            let mut client = connect_remote(addr)?;
            let metrics = client.scrape_metrics()?;
            print!("{metrics}");
            Ok(())
        }
        "cluster" => cluster_command(args),
        _ => {
            print_usage();
            Err(storage_types::StorageError::invalid_key("unknown CLI command"))
        }
    }
}

/// One cluster member as `OP_CLUSTER_INFO` reports it.
struct MemberInfo {
    id: u64,
    addr: String,
    voter: bool,
}

/// `(leader id, leader address, term, commit index, members)` from an `OP_CLUSTER_INFO` reply.
fn parse_cluster_info(b: &[u8]) -> Option<(u64, String, u64, u64, Vec<MemberInfo>)> {
    let u32_at = |i: usize| -> Option<usize> { Some(u32::from_le_bytes(b.get(i..i + 4)?.try_into().ok()?) as usize) };
    let u64_at = |i: usize| -> Option<u64> { Some(u64::from_le_bytes(b.get(i..i + 8)?.try_into().ok()?)) };
    let str_at = |i: usize| -> Option<(String, usize)> {
        let len = u32_at(i)?;
        Some((String::from_utf8_lossy(b.get(i + 4..i + 4 + len)?).into_owned(), i + 4 + len))
    };
    let leader = u64_at(9)?;
    let (leader_addr, mut at) = str_at(17)?;
    let (term, commit) = (u64_at(at)?, u64_at(at + 8)?);
    at += 24;
    let n = u32_at(at)?;
    at += 4;
    let mut members = Vec::with_capacity(n.min(256));
    for _ in 0..n {
        let id = u64_at(at)?;
        let (addr, next) = str_at(at + 8)?;
        members.push(MemberInfo { id, addr, voter: true });
        at = next;
    }
    // Roles follow the member list (servers before `TASK-121` omit them: all voters).
    for m in &mut members {
        if let Some(&role) = b.get(at) {
            m.voter = role == 1;
            at += 1;
        }
    }
    Some((leader, leader_addr, term, commit, members))
}

fn cluster_command(args: &[String]) -> storage_types::Result<()> {
    use storage_server::{CLUSTER_ADD_LEARNER, CLUSTER_ADD_VOTER, CLUSTER_PROMOTE, CLUSTER_REMOVE, OP_CLUSTER_INFO};
    use storage_types::StorageError;
    let addr = parse_addr(args.get(1))?;
    let sub = args.get(2).map(String::as_str).unwrap_or("members");
    let id = || -> storage_types::Result<u16> {
        args.get(3)
            .and_then(|v| v.parse::<u16>().ok())
            .filter(|&v| v != 0)
            .ok_or_else(|| StorageError::invalid_key("missing or invalid <id> (1-65535)"))
    };
    let member_addr = || -> storage_types::Result<&str> {
        args.get(4)
            .map(String::as_str)
            .ok_or_else(|| StorageError::invalid_key("missing <host:port> of the new member"))
    };
    let (action, node, new_addr) = match sub {
        "members" => {
            let mut client = connect_remote(addr)?;
            let (_, info) = client.doc_command(OP_CLUSTER_INFO, &[])?;
            let (leader, leader_addr, term, commit, members) = parse_cluster_info(&info)
                .ok_or_else(|| StorageError::corrupted_header("malformed cluster info"))?;
            println!("leader {leader} ({leader_addr}), term {term}, commit index {commit}");
            for m in members {
                let role = if m.voter { "voter" } else { "learner" };
                let mark = if m.id == leader { "  (leader)" } else { "" };
                println!("  {:>5}  {:<7}  {}{mark}", m.id, role, m.addr);
            }
            return Ok(());
        }
        "add" => (CLUSTER_ADD_VOTER, id()?, member_addr()?),
        "add-learner" => (CLUSTER_ADD_LEARNER, id()?, member_addr()?),
        "promote" => (CLUSTER_PROMOTE, id()?, ""),
        "remove" => (CLUSTER_REMOVE, id()?, ""),
        _ => {
            print_usage();
            return Err(StorageError::invalid_key("unknown cluster command"));
        }
    };
    let mut client = connect_remote(addr)?;
    let result = match client.cluster_change(action, node, new_addr) {
        Err(StorageError::NotLeader { .. }) => {
            // Ask the follower where the leader is and send the change there.
            let (_, info) = client.doc_command(OP_CLUSTER_INFO, &[])?;
            let leader_addr = parse_cluster_info(&info)
                .map(|i| i.1)
                .filter(|a| !a.is_empty())
                .ok_or_else(|| StorageError::invalid_key("the cluster has no leader right now; retry"))?;
            let leader = leader_addr
                .parse::<SocketAddr>()
                .map_err(|_| StorageError::invalid_key("the leader's address is not host:port"))?;
            connect_remote(leader)?.cluster_change(action, node, new_addr)
        }
        other => other,
    };
    result?;
    println!("OK: cluster {sub} {node}");
    Ok(())
}

/// Connects to a remote server, authenticating with `STORAGE_TOKEN` when it is set.
fn connect_remote(addr: SocketAddr) -> storage_types::Result<StorageClient> {
    let mut client = StorageClient::connect(addr)?;
    if let Ok(token) = env::var("STORAGE_TOKEN") {
        client.authenticate(&token)?;
    }
    Ok(client)
}

/// Engine configuration with WAL archiving enabled (needed by backup / PITR commands).
fn pitr_config() -> EngineConfig {
    EngineConfig {
        wal_archive: true,
        ..EngineConfig::default()
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
