//! Horizontal Consistent-Hash Virtual Sharding (`1,024` vshards) & Online Live Shard Migration
//! (`TASK-045`, `TASK-047`, `F-DIST-01`, `F-DIST-03`).
//!
//! - Maps every `InlineId` to one of `1,024` virtual shards in $O(1)$ time (`(id.fast_hash() & 1023) as u16`)
//!   with **zero heap allocations**.
//! - `ClusterMap` fits in a fixed 2 KiB `[NodeId; 1024]` array for $O(1)$ L1-cache-resident shard routing.
//! - Supports online live `vshard` migration with 64-bit checksum digest verification and atomic epoch cutover.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use storage_codec::{xxh3_64, RecordCodec, RecordView, ZeroCopyRecord};
use storage_index::Indexable;
use storage_types::{InlineId, Result, StorageError, Version};

use crate::engine::StorageEngine;

/// Number of virtual shards across the consistent hash ring (`2^10 = 1,024`).
pub const VIRTUAL_SHARD_COUNT: usize = 1024;
const VSHARD_MASK: u64 = (VIRTUAL_SHARD_COUNT as u64) - 1;

/// Logical node identifier in a distributed Storine cluster.
pub type NodeId = u16;

/// Computes the virtual shard (`0..1024`) for `id` in $O(1)$ time (1 bitwise AND, 0 allocations).
#[inline(always)]
pub const fn vshard_for_id(id: InlineId) -> u16 {
    (id.fast_hash() & VSHARD_MASK) as u16
}

/// Compact 2 KiB cluster routing table mapping each of the `1,024` virtual shards to its owner `NodeId`.
#[derive(Clone, Debug)]
pub struct ClusterMap {
    pub epoch: u64,
    owners: [NodeId; VIRTUAL_SHARD_COUNT],
}

impl ClusterMap {
    /// Initializes a `ClusterMap` distributing the `1,024` virtual shards evenly across `nodes`.
    pub fn new_balanced(nodes: &[NodeId]) -> Result<Self> {
        if nodes.is_empty() {
            return Err(StorageError::invalid_key(
                "ClusterMap requires at least 1 NodeId",
            ));
        }
        let mut owners = [0u16; VIRTUAL_SHARD_COUNT];
        for (vshard, slot) in owners.iter_mut().enumerate() {
            *slot = nodes[vshard % nodes.len()];
        }
        Ok(Self { epoch: 1, owners })
    }

    #[inline(always)]
    pub const fn owner_of_vshard(&self, vshard: u16) -> NodeId {
        self.owners[(vshard as usize) & (VIRTUAL_SHARD_COUNT - 1)]
    }

    #[inline(always)]
    pub const fn owner_of_key(&self, id: InlineId) -> NodeId {
        self.owner_of_vshard(vshard_for_id(id))
    }

    #[inline]
    pub fn assign_vshard(&mut self, vshard: u16, new_owner: NodeId) {
        let idx = (vshard as usize) & (VIRTUAL_SHARD_COUNT - 1);
        self.owners[idx] = new_owner;
        self.epoch = self.epoch.wrapping_add(1);
    }

    /// Returns the number of virtual shards currently owned by `node_id`.
    pub fn vshard_count_for_node(&self, node_id: NodeId) -> usize {
        self.owners.iter().filter(|&&n| n == node_id).count()
    }
}

/// Summary receipt returned after an online virtual shard migration (`TASK-047`, `F-DIST-03`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShardMigrationReceipt {
    pub vshard: u16,
    pub source_node: NodeId,
    pub target_node: NodeId,
    pub records_migrated: usize,
    pub shard_digest_xxh3: u64,
    pub new_cluster_epoch: u64,
}

/// Horizontal Sharded Cluster Router (`TASK-045`, `TASK-047`, `F-DIST-01`, `F-DIST-03`).
///
/// Routes single-key reads/writes in $O(1)$ to the owning node's `StorageEngine<T>`, executes
/// scatter-gather multi-shard queries, and performs zero-downtime live `vshard` migrations with
/// 64-bit `XXH3-64` digest verification.
pub struct ClusterRouter<T: ZeroCopyRecord + Indexable> {
    map: RwLock<ClusterMap>,
    nodes: RwLock<HashMap<NodeId, Arc<StorageEngine<T>>>>,
    routed_ops: AtomicU64,
}

impl<T: ZeroCopyRecord + Indexable> core::fmt::Debug for ClusterRouter<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let map = self.map.read().unwrap_or_else(|p| p.into_inner());
        let nodes = self.nodes.read().unwrap_or_else(|p| p.into_inner());
        f.debug_struct("ClusterRouter")
            .field("epoch", &map.epoch)
            .field("node_count", &nodes.len())
            .field("routed_ops", &self.routed_ops.load(Ordering::Relaxed))
            .finish()
    }
}

impl<T: ZeroCopyRecord + Indexable> ClusterRouter<T> {
    pub fn new(node_engines: Vec<(NodeId, Arc<StorageEngine<T>>)>) -> Result<Self> {
        let node_ids: Vec<NodeId> = node_engines.iter().map(|&(id, _)| id).collect();
        let map = ClusterMap::new_balanced(&node_ids)?;
        let mut nodes = HashMap::with_capacity(node_engines.len());
        for (id, eng) in node_engines {
            nodes.insert(id, eng);
        }
        Ok(Self {
            map: RwLock::new(map),
            nodes: RwLock::new(nodes),
            routed_ops: AtomicU64::new(0),
        })
    }

    /// Returns the current cluster topology epoch.
    #[inline]
    pub fn epoch(&self) -> u64 {
        self.map.read().unwrap_or_else(|p| p.into_inner()).epoch
    }

    /// Returns the `NodeId` and `Arc<StorageEngine<T>>` owning `id` in $O(1)$ time.
    #[inline]
    pub fn route_key(&self, id: InlineId) -> Result<(NodeId, Arc<StorageEngine<T>>)> {
        self.routed_ops.fetch_add(1, Ordering::Relaxed);
        let owner = self
            .map
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .owner_of_key(id);
        let nodes = self.nodes.read().unwrap_or_else(|p| p.into_inner());
        let eng = nodes
            .get(&owner)
            .cloned()
            .ok_or_else(|| StorageError::invalid_key("target shard node not registered"))?;
        Ok((owner, eng))
    }

    /// Routes `put(id, record)` to the owning shard node.
    #[inline]
    pub fn put(&self, id: InlineId, record: &T) -> Result<(NodeId, Version)> {
        let (node_id, eng) = self.route_key(id)?;
        let ver = eng.put(id, record)?;
        Ok((node_id, ver))
    }

    /// Routes zero-copy `with_ref` to the owning shard node.
    #[inline]
    pub fn with_ref<R>(&self, id: InlineId, f: impl FnOnce(RecordView<'_, T>) -> R) -> Result<R> {
        let (_, eng) = self.route_key(id)?;
        eng.with_ref(id, f)
    }

    /// Routes `get_owned(id)` to the owning shard node.
    #[inline]
    pub fn get_owned(&self, id: InlineId) -> Result<(NodeId, Version, T)> {
        let (node_id, eng) = self.route_key(id)?;
        let (ver, rec) = eng.get_owned(id)?;
        Ok((node_id, ver, rec))
    }

    /// Routes `delete(id)` to the owning shard node.
    #[inline]
    pub fn delete(&self, id: InlineId) -> Result<bool> {
        let (_, eng) = self.route_key(id)?;
        eng.delete(id)
    }

    /// Adds a new storage engine node to the cluster without moving shards yet.
    pub fn add_node(&self, node_id: NodeId, engine: Arc<StorageEngine<T>>) {
        self.nodes
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .insert(node_id, engine);
    }

    /// Executes a scatter-gather range query across all shard nodes and returns combined `InlineId`s.
    pub fn scatter_gather_range(
        &self,
        field_id: u16,
        min_val: i64,
        max_val: i64,
        limit_per_shard: usize,
    ) -> Vec<InlineId> {
        let nodes = self.nodes.read().unwrap_or_else(|p| p.into_inner());
        let mut results = Vec::new();
        for eng in nodes.values() {
            let mut part = eng
                .query()
                .filter_range(field_id, min_val..=max_val)
                .limit(limit_per_shard)
                .execute_ids();
            results.append(&mut part);
        }
        results
    }

    /// Online Live Virtual Shard Migration (`TASK-047`, `F-DIST-03`):
    ///
    /// 1. Scans `source_node` for all live keys belonging to `vshard` (`vshard_for_id(k) == vshard`).
    /// 2. Copies each record to `target_node` and accumulates an order-independent 64-bit `XXH3-64`
    ///    verification digest over `(id, serialized_frame)`.
    /// 3. Verifies that `target_node` holds every migrated record with an identical `XXH3-64` digest.
    /// 4. Atomically updates `ClusterMap` ownership of `vshard -> target_node` (incrementing `epoch`).
    /// 5. Prunes the migrated keys from `source_node`.
    pub fn migrate_vshard(
        &self,
        vshard: u16,
        target_node: NodeId,
    ) -> Result<ShardMigrationReceipt> {
        let vshard = vshard & ((VIRTUAL_SHARD_COUNT as u16) - 1);
        let source_node = self
            .map
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .owner_of_vshard(vshard);

        if source_node == target_node {
            let epoch = self.epoch();
            return Ok(ShardMigrationReceipt {
                vshard,
                source_node,
                target_node,
                records_migrated: 0,
                shard_digest_xxh3: 0,
                new_cluster_epoch: epoch,
            });
        }

        let (src_eng, dst_eng) = {
            let nodes = self.nodes.read().unwrap_or_else(|p| p.into_inner());
            let s = nodes
                .get(&source_node)
                .cloned()
                .ok_or_else(|| StorageError::invalid_key("source shard node not found"))?;
            let d = nodes
                .get(&target_node)
                .cloned()
                .ok_or_else(|| StorageError::invalid_key("target shard node not found"))?;
            (s, d)
        };

        // 1. Collect all keys belonging to `vshard` on `source_node`
        let mut keys_to_move = Vec::new();
        for id in src_eng.primary_index().ids_snapshot() {
            if vshard_for_id(id) == vshard {
                keys_to_move.push(id);
            }
        }
        keys_to_move.sort_unstable();

        // 2. Stream records to `target_node` and compute source shard digest
        let mut src_digest = 0u64;
        for &id in &keys_to_move {
            let (_ver, owned) = src_eng.get_owned(id)?;
            dst_eng.put(id, &owned)?;
            let frame = RecordCodec::encode_aligned(
                &owned,
                Version::INITIAL,
                0,
                storage_types::RecordFlags::NONE,
                0,
            );
            src_digest ^= xxh3_64(frame.as_slice()).rotate_left((id.fast_hash() & 63) as u32);
        }

        // 3. Verify target shard digest matches source shard digest (`F-DIST-03`)
        let mut dst_digest = 0u64;
        for &id in &keys_to_move {
            let (_ver, owned) = dst_eng.get_owned(id)?;
            let frame = RecordCodec::encode_aligned(
                &owned,
                Version::INITIAL,
                0,
                storage_types::RecordFlags::NONE,
                0,
            );
            dst_digest ^= xxh3_64(frame.as_slice()).rotate_left((id.fast_hash() & 63) as u32);
        }
        if src_digest != dst_digest {
            return Err(StorageError::checksum_mismatch(src_digest, dst_digest));
        }

        // 4. Atomic cutover in `ClusterMap`
        let new_epoch = {
            let mut map = self.map.write().unwrap_or_else(|p| p.into_inner());
            map.assign_vshard(vshard, target_node);
            map.epoch
        };

        // 5. Prune migrated records from `source_node`
        for id in &keys_to_move {
            let _ = src_eng.delete(*id);
        }

        Ok(ShardMigrationReceipt {
            vshard,
            source_node,
            target_node,
            records_migrated: keys_to_move.len(),
            shard_digest_xxh3: dst_digest,
            new_cluster_epoch: new_epoch,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::EngineConfig;
    use crate::model::{AgentObjectRecord, FIELD_SCORE};

    #[test]
    fn test_sharded_cluster_router_and_online_vshard_migration() {
        let base = std::env::temp_dir().join("stge_test_sharding_suite");
        let _ = std::fs::remove_dir_all(&base);

        let n1 = Arc::new(
            StorageEngine::<AgentObjectRecord>::open(&base.join("node1"), EngineConfig::default())
                .unwrap(),
        );
        let n2 = Arc::new(
            StorageEngine::<AgentObjectRecord>::open(&base.join("node2"), EngineConfig::default())
                .unwrap(),
        );
        let n3 = Arc::new(
            StorageEngine::<AgentObjectRecord>::open(&base.join("node3"), EngineConfig::default())
                .unwrap(),
        );

        let router = ClusterRouter::new(vec![(1, Arc::clone(&n1)), (2, Arc::clone(&n2))]).unwrap();

        // Insert 40 records across the 2-node cluster
        let mut target_vshard = None;
        for i in 0..40u64 {
            let id = InlineId::new(&format!("shard_key_{i:04}")).unwrap();
            let vs = vshard_for_id(id);
            if router.route_key(id).unwrap().0 == 1 && target_vshard.is_none() {
                target_vshard = Some(vs);
            }
            let rec =
                AgentObjectRecord::new(id, format!("Agent {i}"), 1_000 + i as i64, (i * 10) as i64);
            router.put(id, &rec).unwrap();
        }

        // Verify scatter-gather range query finds records across both nodes
        let hits = router.scatter_gather_range(FIELD_SCORE, 0, 100, 50);
        assert_eq!(hits.len(), 11); // scores 0, 10, ..., 100

        // Add Node 3 and live-migrate `target_vshard` from Node 1 -> Node 3!
        router.add_node(3, Arc::clone(&n3));
        let vs = target_vshard.unwrap();
        let receipt = router.migrate_vshard(vs, 3).unwrap();
        assert_eq!(receipt.source_node, 1);
        assert_eq!(receipt.target_node, 3);
        assert!(receipt.records_migrated >= 1);
        assert!(receipt.new_cluster_epoch >= 2);

        // Verify all 40 records are still readable via zero-copy `with_ref` after migration!
        for i in 0..40u64 {
            let id = InlineId::new(&format!("shard_key_{i:04}")).unwrap();
            let score = router.with_ref(id, |v| v.score).unwrap();
            assert_eq!(score, (i * 10) as i64);
        }

        drop(router);
        drop(n1);
        drop(n2);
        drop(n3);
        let _ = std::fs::remove_dir_all(&base);
    }
}
