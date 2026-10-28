//! Write-safety guarantees (`TASK-093` – `TASK-097`): monotonic versions across delete and
//! re-create (no ABA), conditional writes, atomic field operations and idempotent requests.

use std::path::PathBuf;

use storage_core::{AgentObjectRecord, EngineConfig, StorageEngine};
use storage_types::{InlineId, StorageError, Version};

fn fresh_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("stge_ws_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn rec(id: InlineId, score: i64) -> AgentObjectRecord {
    AgentObjectRecord::new(id, "r", 1, score)
}

#[test]
fn test_versions_are_never_reused_after_delete_and_recreate() {
    let dir = fresh_dir("aba");
    let id = InlineId::new("acct:1").unwrap();
    {
        let engine = StorageEngine::<AgentObjectRecord>::open(&dir, EngineConfig::default()).unwrap();
        let v1 = engine.put(id, &rec(id, 100)).unwrap();
        assert_eq!(v1, Version::INITIAL);
        assert!(engine.delete(id).unwrap());
        let v2 = engine.put(id, &rec(id, 0)).unwrap();
        assert!(v2 > v1, "a re-created record must not reuse version {v1:?} (got {v2:?})");
        // A CAS holding the pre-delete version must fail against the new record.
        let err = engine.put_cas(id, v1, &rec(id, 999)).unwrap_err();
        assert!(matches!(err, StorageError::VersionConflict { .. }), "{err:?}");
        // Same for `insert` after a delete.
        assert!(engine.delete(id).unwrap());
        let v3 = engine.insert(id, &rec(id, 1)).unwrap();
        assert!(v3 > v2);
        assert!(engine.delete(id).unwrap());
        engine.flush_all().unwrap();
    }
    // The floor survives a clean restart (checkpointed) ...
    let v_after_restart = {
        let engine = StorageEngine::<AgentObjectRecord>::open(&dir, EngineConfig::default()).unwrap();
        let v = engine.put(id, &rec(id, 5)).unwrap();
        assert!(v.0 > 3, "version reused after restart: {v:?}");
        assert!(engine.delete(id).unwrap());
        // No flush: the deletion only lives in the WAL.
        v
    };
    // ... and a WAL-only deletion replayed after a crash-style reopen.
    let engine = StorageEngine::<AgentObjectRecord>::open(&dir, EngineConfig::default()).unwrap();
    let v = engine.put(id, &rec(id, 6)).unwrap();
    assert!(v > v_after_restart, "{v:?} <= {v_after_restart:?}");

    // Transactions follow the same rule.
    let other = InlineId::new("acct:2").unwrap();
    engine.put(other, &rec(other, 1)).unwrap();
    engine.delete(other).unwrap();
    engine
        .transaction(|tx| {
            tx.put(other, rec(other, 2));
            Ok(())
        })
        .unwrap();
    assert!(engine.get_owned(other).unwrap().0 > Version::INITIAL);
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}
