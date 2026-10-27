//! End-to-End Benchmark & Zero-Allocation Gate Runner (`TASK-002`, `TASK-030`).

use std::fs;
use std::sync::Arc;
use storage_benches::{assert_zero_alloc, run_latency_bench, TrackingAllocator};
use storage_codec::{checksum_payload, RecordCodec};
use storage_core::{
    AgentObjectRecord, EngineConfig, StorageEngine, StorageLayoutMode, FIELD_PRIMARY_TAG,
    FIELD_TIMESTAMP,
};
use storage_index::cosine_similarity_f32;
use storage_server::{StorageClient, StorageServer};
use storage_types::{InlineId, RecordFlags, Version};
use storage_wal::SyncPolicy;

#[global_allocator]
static GLOBAL_ALLOC: TrackingAllocator = TrackingAllocator;

fn main() -> storage_types::Result<()> {
    let bench_dir = std::env::temp_dir().join("storage_engine_full_bench");
    let _ = fs::remove_dir_all(&bench_dir);

    println!("==========================================================================================");
    println!("  StorageEngine End-to-End Performance, Latency Percentile & Zero-Allocation Benchmark");
    println!("==========================================================================================\n");

    // 1. Verify TrackingAllocator catches a 1-byte heap allocation
    let (_, probe) = storage_benches::measure_allocations(|| {
        let v = vec![1u8];
        core::hint::black_box(v);
    });
    assert!(probe.allocations >= 1 && probe.bytes >= 1);

    let mut results = Vec::new();

    // 2. InlineId creation + 3-register fast_hash (0 allocs)
    let id_a = InlineId::new("user:123456789")?;
    let id_b = InlineId::new("user:123456789")?;
    assert_zero_alloc("InlineId equality & fast_hash", || {
        assert_eq!(id_a, id_b);
        core::hint::black_box(id_a.fast_hash());
    });
    results.push(run_latency_bench("InlineId (24B) Hash + Eq", 50_000, |_| {
        core::hint::black_box(id_a == id_b);
        core::hint::black_box(id_a.fast_hash());
    }));

    // 3. Hardware SIMD Checksum (XXH3-64 + SSE4.2 CRC32C) over 4KB payload
    let payload_4k = vec![0x5Au8; 4096];
    results.push(run_latency_bench(
        "SIMD Checksum (XXH3 + CRC32C, 4KB)",
        20_000,
        |_| {
            core::hint::black_box(checksum_payload(&payload_4k));
        },
    ));

    // 4. Zero-Copy RecordCodec encode (pooled TLS scratch) & unchecked read (0 allocs)
    let sample_id = InlineId::new("agent_core")?;
    let mut sample = AgentObjectRecord::new(sample_id, "Core Memory State", 1_700_000_000, 950);
    sample.tags.push(InlineId::new("ai")?);
    sample.tags.push(InlineId::new("memory")?);
    sample.embedding = vec![0.125f32; 64];

    let encoded_frame =
        RecordCodec::encode_aligned(&sample, Version(1), 1_700_000_000, RecordFlags::NONE, 0);
    let _ = RecordCodec::validate_frame::<AgentObjectRecord>(&encoded_frame)?;

    assert_zero_alloc("RecordCodec::decode_unchecked + field reads", || {
        // SAFETY: `encoded_frame` was just validated above.
        let view = unsafe { RecordCodec::decode_unchecked::<AgentObjectRecord>(&encoded_frame) };
        assert_eq!(view.score, 950);
        assert_eq!(view.get_str(&view.title), "Core Memory State");
        assert_eq!(view.get_slice(&view.tags).len(), 2);
    });

    results.push(run_latency_bench(
        "Zero-Copy RecordView Read (Unchecked)",
        50_000,
        |_| {
            // SAFETY: Validated above.
            let view =
                unsafe { RecordCodec::decode_unchecked::<AgentObjectRecord>(&encoded_frame) };
            core::hint::black_box(view.score);
            core::hint::black_box(view.get_str(&view.title));
        },
    ));

    // 5. StorageEngine Warm Zero-Copy Read (`with_ref` -> 0 heap allocations!) & Coalesced Writes
    let cfg = EngineConfig {
        layout_mode: StorageLayoutMode::AdaptiveArena {
            arena_threshold_bytes: 4096,
            segment_capacity: 16 * 1024 * 1024,
        },
        wal_sync_policy: SyncPolicy::AsyncMemoryFirst,
        background_flush: true,
        ..EngineConfig::default()
    };

    let engine = Arc::new(StorageEngine::<AgentObjectRecord>::open(&bench_dir, cfg.clone())?);

    // Pre-populate 2,000 records
    let mut keys = Vec::with_capacity(2_000);
    for i in 0..2_000 {
        let k = InlineId::new(&format!("rec_{i:06}"))?;
        let mut r = AgentObjectRecord::new(k, "Memory Node", 1_700_000_000 + i64::from(i), i64::from(i));
        r.tags.push(if i % 2 == 0 {
            InlineId::from_static("even")
        } else {
            InlineId::from_static("odd")
        });
        r.embedding = vec![(i as f32) * 0.001; 64];
        engine.put(k, &r)?;
        keys.push(k);
    }

    // Verify 0 heap allocations on warm `engine.with_ref` read (`RULE-MEM-01`)!
    let target_key = keys[42];
    assert_zero_alloc("StorageEngine::with_ref warm read", || {
        let score = engine
            .with_ref(target_key, |view| {
                core::hint::black_box(view.get_str(&view.title));
                view.score
            })
            .unwrap();
        assert_eq!(score, 42);
    });

    results.push(run_latency_bench(
        "StorageEngine::with_ref (0-Alloc Hot Read)",
        50_000,
        |i| {
            let k = keys[i % keys.len()];
            let _ = engine.with_ref(k, |view| {
                core::hint::black_box(view.score);
            });
        },
    ));

    // Flush to Arena Segment `.seg` and verify 0-alloc read directly from `MmapPool` / `Arena` too!
    engine.flush_all()?;

    assert_zero_alloc("StorageEngine::with_ref flushed Arena Mmap read", || {
        let score = engine
            .with_ref(target_key, |view| {
                core::hint::black_box(view.get_str(&view.title));
                view.score
            })
            .unwrap();
        assert_eq!(score, 42);
    });

    results.push(run_latency_bench(
        "StorageEngine::get_ref (Mmap Arena Segment)",
        20_000,
        |i| {
            let k = keys[i % keys.len()];
            let r = engine.get_ref(k).unwrap();
            core::hint::black_box(r.score);
        },
    ));

    // Encryption at rest (`F-SEC-02`): warm reads are served from the decrypted-frame cache with
    // zero allocations; with no cache budget every read decrypts (the cold-read cost).
    for (label, cache_bytes) in [
        ("StorageEngine::with_ref (Encrypted, warm cache)", 64 * 1024 * 1024),
        ("StorageEngine::with_ref (Encrypted, cold decrypt)", 0),
    ] {
        let enc_dir = std::env::temp_dir().join(format!("storage_engine_bench_enc_{cache_bytes}"));
        let _ = fs::remove_dir_all(&enc_dir);
        let enc_engine = StorageEngine::<AgentObjectRecord>::open(
            &enc_dir,
            EngineConfig {
                encryption: Some(storage_core::EncryptionConfig::strict(
                    1,
                    storage_codec::EncryptionKey::from_bytes([0x42; 32]),
                )),
                decrypted_cache_bytes: cache_bytes,
                ..cfg.clone()
            },
        )?;
        for (i, &k) in keys.iter().enumerate() {
            let r = AgentObjectRecord::new(k, "Memory Node", 1_700_000_000 + i as i64, i as i64);
            enc_engine.put(k, &r)?;
        }
        enc_engine.flush_all()?;
        if cache_bytes > 0 {
            for &k in &keys {
                let _ = enc_engine.with_ref(k, |v| v.score);
            }
            assert_zero_alloc("StorageEngine::with_ref encrypted warm read", || {
                let score = enc_engine.with_ref(target_key, |view| view.score).unwrap();
                assert_eq!(score, 42);
            });
        }
        results.push(run_latency_bench(label, 20_000, |i| {
            let k = keys[i % keys.len()];
            let _ = enc_engine.with_ref(k, |view| core::hint::black_box(view.score));
        }));
        drop(enc_engine);
        let _ = fs::remove_dir_all(&enc_dir);
    }

    // Cold primary tier (`TASK-092`): the record's index entry is read from the tier file (Bloom
    // filter + one page decode), not RAM; the frame is re-validated on each read.
    {
        let cold_dir = std::env::temp_dir().join("storage_engine_bench_cold_tier");
        let _ = fs::remove_dir_all(&cold_dir);
        let cold_engine = StorageEngine::<AgentObjectRecord>::open(
            &cold_dir,
            EngineConfig { primary_tier: storage_core::PrimaryTier::Cold, ..cfg.clone() },
        )?;
        for (i, &k) in keys.iter().enumerate() {
            let r = AgentObjectRecord::new(k, "Memory Node", 1_700_000_000 + i as i64, i as i64);
            cold_engine.put(k, &r)?;
        }
        cold_engine.flush_all()?;
        assert_eq!(cold_engine.primary_index().resident_entries(), 0, "entries live in the tier");
        results.push(run_latency_bench("StorageEngine::with_ref (Cold primary tier)", 20_000, |i| {
            let k = keys[i % keys.len()];
            let _ = cold_engine.with_ref(k, |view| core::hint::black_box(view.score));
        }));
        drop(cold_engine);
        let _ = fs::remove_dir_all(&cold_dir);
    }

    // Hot-key burst updates to measure Write Coalescing
    results.push(run_latency_bench(
        "StorageEngine::update (Coalesced Hot Write)",
        5_000,
        |i| {
            let k = keys[i % 32]; // 32 hot keys updated repeatedly
            let _ = engine.update(k, |draft| {
                draft.score += 1;
                Ok(())
            });
        },
    ));

    // Secondary Index Bitmap Tag + B+Tree Range Query
    let tag_even = InlineId::from_static("even");
    results.push(run_latency_bench(
        "Secondary Index (Tag Bitmap + BTree Range)",
        10_000,
        |_| {
            let hits = engine
                .query()
                .filter_tag(FIELD_PRIMARY_TAG, tag_even)
                .filter_range(FIELD_TIMESTAMP, 1_700_000_100..=1_700_000_500)
                .resolve_ordinals();
            core::hint::black_box(hits.len());
        },
    ));

    // SIMD 64-Dim Cosine Similarity
    let q_vec = vec![0.25f32; 64];
    let doc_vec = vec![0.50f32; 64];
    results.push(run_latency_bench(
        "SIMD Cosine Similarity (64-dim f32)",
        50_000,
        |_| {
            core::hint::black_box(cosine_similarity_f32(&q_vec, &doc_vec));
        },
    ));

    // TCP Binary Protocol Server Loopback RPC
    let mut srv = StorageServer::start("127.0.0.1:0", Arc::clone(&engine))?;
    let mut client = StorageClient::connect(srv.local_addr())?;
    results.push(run_latency_bench(
        "StorageServer TCP Binary GET Frame RPC",
        1_000,
        |i| {
            let k = keys[i % 100];
            let frame = client.get_frame(k).unwrap();
            core::hint::black_box(frame.len());
        },
    ));
    srv.stop();

    println!(
        "{:<44} | {:>12} | {:>8} | {:>8} | {:>8} | {:>9}",
        "Benchmark Name", "Throughput", "p50 (ns)", "p99 (ns)", "p99.9", "Allocs/Op"
    );
    println!("{}", "-".repeat(98));
    for m in &results {
        println!(
            "{:<44} | {:>9.0}/s | {:>8} | {:>8} | {:>8} | {:>9.2}",
            m.name, m.ops_per_sec, m.p50_ns, m.p99_ns, m.p999_ns, m.allocs_per_op
        );
    }

    let st = engine.stats();
    let idx_path = bench_dir.join("indexes").join("default_primary.idx");
    let idx_bytes = fs::metadata(&idx_path).map(|m| m.len()).unwrap_or(0);
    println!(
        "\nEngine Telemetry: live_records={}, total_writes={}, coalesced_flushes={} ({:.1}% write reduction!)",
        st.live_records,
        st.total_writes,
        st.coalesced_flushes,
        (st.coalesced_flushes as f64 / st.total_writes.max(1) as f64) * 100.0
    );
    println!(
        "Footprint Metrics: ArchivedAgentObjectRecord={}B, PrimaryEntry={}B, IndexSnapshot={}B, MmapGuard={}B, primary.idx={}B ({:.1}B/record)",
        core::mem::size_of::<storage_core::ArchivedAgentObjectRecord>(),
        core::mem::size_of::<storage_index::PrimaryEntry>(),
        core::mem::size_of::<storage_index::IndexSnapshot>(),
        core::mem::size_of::<storage_mmap::MmapGuard>(),
        idx_bytes,
        (idx_bytes as f64) / (st.live_records.max(1) as f64)
    );

    drop(engine);
    let _ = fs::remove_dir_all(&bench_dir);
    Ok(())
}
