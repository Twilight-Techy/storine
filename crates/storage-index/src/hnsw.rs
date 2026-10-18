//! Multi-Layer HNSW (Hierarchical Navigable Small World) Vector Index with `SQ8` Scalar Quantization
//! and Generation-Stamped Visited Tracking (`TASK-037`, `F-INDX-07`, `RULE-ALGO-01`, `RULE-MEM-02`).
//!
//! Achieves $O(\log N)$ sub-millisecond Approximate Nearest Neighbor (ANN) vector search with
//! `4x` reduced RAM footprint via `QuantizedVecSq8` codes and `hnsw.idx` disk persistence.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs;
use std::path::Path;
use std::sync::{Mutex, RwLock};

use storage_codec::checksum_payload;
use storage_mmap::atomic_write_replace;
use storage_types::{RecordOrdinal, Result, StorageError};

use crate::bitmap::CompressedBitmap;
use crate::quantize::{sq8_cosine_similarity, QuantizedVecSq8};
use crate::vector::VectorTopKHit;

const HNSW_MAGIC: u32 = 0x484E_5357; // "HNSW"

/// Tunable parameters for the multi-layer HNSW proximity graph (`F-INDX-07`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HnswConfig {
    /// Max bidirectional links per node on layers `> 0` (default: 16).
    pub m: usize,
    /// Max bidirectional links per node on layer `0` (default: 32).
    pub m_max0: usize,
    /// Beam width during graph construction (default: 64).
    pub ef_construction: usize,
    /// Default beam width during top-$K$ search (default: 48).
    pub ef_search: usize,
}

impl Default for HnswConfig {
    fn default() -> Self {
        Self {
            m: 16,
            m_max0: 32,
            ef_construction: 64,
            ef_search: 48,
        }
    }
}

#[derive(Clone, Debug)]
struct HnswNode {
    quantized: QuantizedVecSq8,
    /// Adjacency lists per layer (`layers[0]` is base layer `0`, up to `layers[max_node_layer]`).
    layers: Vec<Vec<u32>>,
    deleted: bool,
}

/// Generation-stamped visited array (`RULE-MEM-02`):
/// Advances `current_gen` by 1 on each search instead of allocating a `HashSet` or zeroing memory!
#[derive(Default)]
struct VisitedTable {
    stamp: Vec<u32>,
    current_gen: u32,
}

impl VisitedTable {
    fn prepare(&mut self, capacity: usize) -> u32 {
        if self.stamp.len() < capacity {
            self.stamp.resize(capacity.next_power_of_two().max(64), 0);
        }
        self.current_gen = self.current_gen.wrapping_add(1);
        if self.current_gen == 0 {
            self.stamp.fill(0);
            self.current_gen = 1;
        }
        self.current_gen
    }

    #[inline]
    fn mark_visited(&mut self, idx: u32, gen: u32) -> bool {
        let i = idx as usize;
        if self.stamp[i] == gen {
            false
        } else {
            self.stamp[i] = gen;
            true
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct ScoredNode {
    ord: u32,
    score: f32,
}

impl PartialEq for ScoredNode {
    fn eq(&self, other: &Self) -> bool {
        self.ord == other.ord && self.score.to_bits() == other.score.to_bits()
    }
}
impl Eq for ScoredNode {}

impl PartialOrd for ScoredNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Max-heap ordering (highest similarity score popped first).
impl Ord for ScoredNode {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| other.ord.cmp(&self.ord))
    }
}

/// Min-heap wrapper so we can evict the worst candidate in $O(\log \text{ef})$.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MinScoredNode(ScoredNode);

impl PartialOrd for MinScoredNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MinScoredNode {
    fn cmp(&self, other: &Self) -> Ordering {
        other.0.cmp(&self.0)
    }
}

#[derive(Default)]
struct HnswGraphState {
    nodes: Vec<Option<HnswNode>>,
    entry_point: Option<u32>,
    max_layer: usize,
    live_count: usize,
    dim: usize,
}

/// Multi-layer HNSW proximity graph index with `SQ8` quantized storage (`TASK-037`, `F-INDX-07`).
pub struct HnswVectorIndex {
    config: HnswConfig,
    graph: RwLock<HnswGraphState>,
    visited_pool: Mutex<VisitedTable>,
}

impl core::fmt::Debug for HnswVectorIndex {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("HnswVectorIndex")
            .field("config", &self.config)
            .field("live_vectors", &self.len())
            .finish()
    }
}

impl Default for HnswVectorIndex {
    fn default() -> Self {
        Self::new(HnswConfig::default())
    }
}

impl HnswVectorIndex {
    pub fn new(config: HnswConfig) -> Self {
        Self {
            config,
            graph: RwLock::new(HnswGraphState::default()),
            visited_pool: Mutex::new(VisitedTable::default()),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.graph
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .live_count
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Deterministic geometric level assignment for `ord` (`P(level >= l) = (1/m)^l`, max 6 layers).
    #[inline]
    fn assign_layer(ord: u32) -> usize {
        let mut h = u64::from(ord).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        h ^= h >> 30;
        h = h.wrapping_mul(0xBF58_476D_1CE4_E5B9);
        let tz = h.trailing_zeros() as usize;
        (tz / 4).min(6)
    }

    /// Inserts or updates a vector embedding for `ord` in the multi-layer HNSW graph (`O(log N)`).
    pub fn insert(&self, ord: RecordOrdinal, vec: &[f32]) {
        if vec.is_empty() {
            self.remove(ord);
            return;
        }
        let q_vec = QuantizedVecSq8::quantize(vec);
        let node_id = ord.0;
        let node_layer = Self::assign_layer(node_id);

        let mut g = self
            .graph
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if g.dim == 0 {
            g.dim = vec.len();
        } else if g.dim != vec.len() {
            return;
        }

        let idx = node_id as usize;
        if g.nodes.len() <= idx {
            g.nodes.resize_with(idx + 1, || None);
        }

        let was_live = g.nodes[idx].as_ref().is_some_and(|n| !n.deleted);
        let mut layers = Vec::with_capacity(node_layer + 1);
        for l in 0..=node_layer {
            let cap = if l == 0 {
                self.config.m_max0
            } else {
                self.config.m
            };
            layers.push(Vec::with_capacity(cap));
        }

        let Some(mut cur_ep) = g.entry_point else {
            g.nodes[idx] = Some(HnswNode {
                quantized: q_vec,
                layers,
                deleted: false,
            });
            g.entry_point = Some(node_id);
            g.max_layer = node_layer;
            if !was_live {
                g.live_count += 1;
            }
            return;
        };

        if cur_ep == node_id {
            if let Some(existing) = g.nodes[idx].as_mut() {
                existing.quantized = q_vec;
                existing.deleted = false;
            }
            return;
        }

        let mut cur_score = g.nodes[cur_ep as usize]
            .as_ref()
            .map_or(-1.0, |ep_node| sq8_cosine_similarity(&q_vec, &ep_node.quantized));

        // 1. Greedy descent from `g.max_layer` down to `node_layer + 1`
        if g.max_layer > node_layer {
            for l in ((node_layer + 1)..=g.max_layer).rev() {
                loop {
                    let mut improved = false;
                    if let Some(ep_node) = g.nodes[cur_ep as usize].as_ref() {
                        if let Some(neighbors) = ep_node.layers.get(l) {
                            for &nb in neighbors {
                                if let Some(nb_node) = g.nodes.get(nb as usize).and_then(|x| x.as_ref()) {
                                    let s = sq8_cosine_similarity(&q_vec, &nb_node.quantized);
                                    if s > cur_score {
                                        cur_score = s;
                                        cur_ep = nb;
                                        improved = true;
                                    }
                                }
                            }
                        }
                    }
                    if !improved {
                        break;
                    }
                }
            }
        }

        // 2. Beam search and bidirectional linking on layers `min(node_layer, max_layer)` down to `0`
        let top_connect_layer = node_layer.min(g.max_layer);
        let mut vis = self
            .visited_pool
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        for l in (0..=top_connect_layer).rev() {
            let max_links = if l == 0 {
                self.config.m_max0
            } else {
                self.config.m
            };
            let candidates = search_layer_locked(
                &g,
                &mut vis,
                &q_vec,
                cur_ep,
                l,
                self.config.ef_construction.max(max_links),
            );
            if let Some(best) = candidates.first() {
                cur_ep = best.ord;
            }

            let selected: Vec<u32> = candidates
                .into_iter()
                .filter(|c| c.ord != node_id)
                .take(max_links)
                .map(|c| c.ord)
                .collect();

            layers[l] = selected.clone();

            // Add reverse edges with degree-bound pruning
            for nb in selected {
                if let Some(Some(nb_node)) = g.nodes.get_mut(nb as usize) {
                    if nb_node.layers.len() <= l {
                        nb_node.layers.resize_with(l + 1, Vec::new);
                    }
                    if !nb_node.layers[l].contains(&node_id) {
                        if nb_node.layers[l].len() < max_links {
                            nb_node.layers[l].push(node_id);
                        } else {
                            // Replace the weakest neighbor if `node_id` is closer
                            let nb_q = nb_node.quantized.clone();
                            let new_sim = sq8_cosine_similarity(&nb_q, &q_vec);
                            let mut worst_idx = 0usize;
                            let mut worst_sim = f32::INFINITY;
                            let existing_nbs = nb_node.layers[l].clone();
                            for (i, &other_id) in existing_nbs.iter().enumerate() {
                                let sim = g
                                    .nodes
                                    .get(other_id as usize)
                                    .and_then(|x| x.as_ref())
                                    .map_or(-1.0, |o| sq8_cosine_similarity(&nb_q, &o.quantized));
                                if sim < worst_sim {
                                    worst_sim = sim;
                                    worst_idx = i;
                                }
                            }
                            if new_sim > worst_sim {
                                if let Some(Some(nb_node_mut)) = g.nodes.get_mut(nb as usize) {
                                    nb_node_mut.layers[l][worst_idx] = node_id;
                                }
                            }
                        }
                    }
                }
            }
        }
        drop(vis);

        g.nodes[idx] = Some(HnswNode {
            quantized: q_vec,
            layers,
            deleted: false,
        });
        if node_layer > g.max_layer {
            g.max_layer = node_layer;
            g.entry_point = Some(node_id);
        }
        if !was_live {
            g.live_count += 1;
        }
    }

    /// Marks `ord` as deleted in the HNSW graph.
    pub fn remove(&self, ord: RecordOrdinal) {
        let mut g = self
            .graph
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let idx = ord.0 as usize;
        if let Some(Some(node)) = g.nodes.get_mut(idx) {
            if !node.deleted {
                node.deleted = true;
                g.live_count = g.live_count.saturating_sub(1);
            }
        }
    }

    /// Searches the multi-layer HNSW graph for the top `k` approximate nearest neighbors of `query`
    /// using `SQ8` quantized similarity, optionally filtered by `filter_bitmap` (`TASK-037`, `TASK-038`).
    pub fn search_sq8_candidates(
        &self,
        query: &[f32],
        k: usize,
        filter_bitmap: Option<&CompressedBitmap>,
    ) -> Vec<VectorTopKHit> {
        if k == 0 || query.is_empty() {
            return Vec::new();
        }
        let g = self
            .graph
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(mut cur_ep) = g.entry_point else {
            return Vec::new();
        };
        if g.live_count == 0 || g.dim != query.len() {
            return Vec::new();
        }

        let q_query = QuantizedVecSq8::quantize(query);
        let mut cur_score = g.nodes[cur_ep as usize]
            .as_ref()
            .map_or(-1.0, |ep_node| sq8_cosine_similarity(&q_query, &ep_node.quantized));

        // 1. Greedy descent from `max_layer` down to layer `1`
        for l in (1..=g.max_layer).rev() {
            loop {
                let mut improved = false;
                if let Some(ep_node) = g.nodes[cur_ep as usize].as_ref() {
                    if let Some(neighbors) = ep_node.layers.get(l) {
                        for &nb in neighbors {
                            if let Some(nb_node) = g.nodes.get(nb as usize).and_then(|x| x.as_ref()) {
                                let s = sq8_cosine_similarity(&q_query, &nb_node.quantized);
                                if s > cur_score {
                                    cur_score = s;
                                    cur_ep = nb;
                                    improved = true;
                                }
                            }
                        }
                    }
                }
                if !improved {
                    break;
                }
            }
        }

        // 2. Beam search on base layer `0` with generation-stamped visited array
        let ef = self.config.ef_search.max(k * 2);
        let mut vis = self
            .visited_pool
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let results = search_layer_locked(&g, &mut vis, &q_query, cur_ep, 0, ef);
        drop(vis);

        let mut hits: Vec<VectorTopKHit> = results
            .into_iter()
            .filter(|c| {
                let is_live = g
                    .nodes
                    .get(c.ord as usize)
                    .and_then(|x| x.as_ref())
                    .is_some_and(|n| !n.deleted);
                let passes_filter =
                    filter_bitmap.is_none_or(|bm| bm.contains(RecordOrdinal(c.ord)));
                is_live && passes_filter
            })
            .take(k)
            .map(|c| VectorTopKHit {
                ordinal: RecordOrdinal(c.ord),
                score: c.score,
            })
            .collect();

        // Fallback if a highly selective filter pruned the HNSW neighborhood
        if hits.len() < k {
            if let Some(bm) = filter_bitmap {
                let mut seen: Vec<u32> = hits.iter().map(|h| h.ordinal.0).collect();
                for ord in bm.to_vec() {
                    if seen.contains(&ord.0) {
                        continue;
                    }
                    if let Some(Some(node)) = g.nodes.get(ord.0 as usize) {
                        if !node.deleted {
                            let s = sq8_cosine_similarity(&q_query, &node.quantized);
                            hits.push(VectorTopKHit {
                                ordinal: ord,
                                score: s,
                            });
                            seen.push(ord.0);
                        }
                    }
                }
                hits.sort_unstable_by(|a, b| b.score.total_cmp(&a.score));
                hits.truncate(k);
            }
        }

        hits
    }

    /// Persists the HNSW graph and `SQ8` quantized vectors to `path` (`hnsw.idx`) with CRC32C verification.
    pub fn save_to_disk(&self, path: &Path) -> Result<()> {
        let g = self
            .graph
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let mut payload = Vec::new();
        let entry_raw = g.entry_point.unwrap_or(u32::MAX);
        payload.extend_from_slice(&entry_raw.to_le_bytes());
        payload.extend_from_slice(&(g.max_layer as u32).to_le_bytes());
        payload.extend_from_slice(&(g.live_count as u32).to_le_bytes());
        payload.extend_from_slice(&(g.dim as u32).to_le_bytes());
        payload.extend_from_slice(&(g.nodes.len() as u32).to_le_bytes());

        for (ord, opt_node) in g.nodes.iter().enumerate() {
            if let Some(node) = opt_node {
                payload.push(if node.deleted { 2u8 } else { 1u8 });
                payload.extend_from_slice(&(ord as u32).to_le_bytes());
                node.quantized.write_to(&mut payload);
                payload.extend_from_slice(&(node.layers.len() as u32).to_le_bytes());
                for layer_links in &node.layers {
                    payload.extend_from_slice(&(layer_links.len() as u32).to_le_bytes());
                    for &nb in layer_links {
                        payload.extend_from_slice(&nb.to_le_bytes());
                    }
                }
            } else {
                payload.push(0u8);
            }
        }

        let crc = checksum_payload(&payload);
        let mut full = Vec::with_capacity(12 + payload.len());
        full.extend_from_slice(&HNSW_MAGIC.to_le_bytes());
        full.extend_from_slice(&crc.to_le_bytes());
        full.extend_from_slice(&payload);

        if let Some(parent) = path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent).map_err(|e| StorageError::io("create hnsw dir", e))?;
            }
        }
        atomic_write_replace(path, &full, true)
    }

    /// Loads a persisted `HnswVectorIndex` from `path` (`hnsw.idx`).
    pub fn load_from_disk(path: &Path, config: HnswConfig) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::new(config));
        }
        let bytes = fs::read(path).map_err(|e| StorageError::io("read hnsw.idx", e))?;
        if bytes.len() < 32 {
            return Err(StorageError::corrupted_header("hnsw.idx truncated"));
        }
        let magic = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        if magic != HNSW_MAGIC {
            return Err(StorageError::corrupted_header("hnsw.idx invalid magic"));
        }
        let expected_crc = u64::from_le_bytes([
            bytes[4], bytes[5], bytes[6], bytes[7], bytes[8], bytes[9], bytes[10], bytes[11],
        ]);
        let payload = &bytes[12..];
        let actual_crc = checksum_payload(payload);
        if actual_crc != expected_crc {
            return Err(StorageError::checksum_mismatch(expected_crc, actual_crc));
        }

        let entry_raw = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
        let max_layer =
            u32::from_le_bytes([payload[4], payload[5], payload[6], payload[7]]) as usize;
        let live_count =
            u32::from_le_bytes([payload[8], payload[9], payload[10], payload[11]]) as usize;
        let dim = u32::from_le_bytes([payload[12], payload[13], payload[14], payload[15]]) as usize;
        let nodes_len =
            u32::from_le_bytes([payload[16], payload[17], payload[18], payload[19]]) as usize;

        let mut off = 20usize;
        let mut nodes = Vec::with_capacity(nodes_len);

        for _ in 0..nodes_len {
            let status = *payload
                .get(off)
                .ok_or_else(|| StorageError::corrupted_header("hnsw node status EOF"))?;
            off += 1;
            if status == 0 {
                nodes.push(None);
                continue;
            }
            off += 4; // skip ordinal u32
            let quantized = QuantizedVecSq8::read_from(&payload[off..], dim)
                .ok_or_else(|| StorageError::corrupted_header("hnsw quantized vec EOF"))?;
            off += 16 + dim;
            let num_layers = u32::from_le_bytes([
                payload[off],
                payload[off + 1],
                payload[off + 2],
                payload[off + 3],
            ]) as usize;
            off += 4;
            let mut layers = Vec::with_capacity(num_layers);
            for _ in 0..num_layers {
                let link_count = u32::from_le_bytes([
                    payload[off],
                    payload[off + 1],
                    payload[off + 2],
                    payload[off + 3],
                ]) as usize;
                off += 4;
                let mut links = Vec::with_capacity(link_count);
                for _ in 0..link_count {
                    let nb = u32::from_le_bytes([
                        payload[off],
                        payload[off + 1],
                        payload[off + 2],
                        payload[off + 3],
                    ]);
                    off += 4;
                    links.push(nb);
                }
                layers.push(links);
            }
            nodes.push(Some(HnswNode {
                quantized,
                layers,
                deleted: status == 2,
            }));
        }

        let state = HnswGraphState {
            nodes,
            entry_point: (entry_raw != u32::MAX).then_some(entry_raw),
            max_layer,
            live_count,
            dim,
        };

        Ok(Self {
            config,
            graph: RwLock::new(state),
            visited_pool: Mutex::new(VisitedTable::default()),
        })
    }
}

fn search_layer_locked(
    g: &HnswGraphState,
    vis: &mut VisitedTable,
    q_query: &QuantizedVecSq8,
    entry_point: u32,
    layer: usize,
    ef: usize,
) -> Vec<ScoredNode> {
    let gen = vis.prepare(g.nodes.len().max(1));
    vis.mark_visited(entry_point, gen);

    let ep_score = g
        .nodes
        .get(entry_point as usize)
        .and_then(|x| x.as_ref())
        .map_or(-1.0, |n| sq8_cosine_similarity(q_query, &n.quantized));

    let ep_item = ScoredNode {
        ord: entry_point,
        score: ep_score,
    };

    let mut candidates = BinaryHeap::with_capacity(ef * 2);
    let mut top_ef = BinaryHeap::with_capacity(ef + 1);
    candidates.push(ep_item);
    top_ef.push(MinScoredNode(ep_item));

    while let Some(curr) = candidates.pop() {
        let worst_best = top_ef.peek().map_or(-1.0, |m| m.0.score);
        if curr.score < worst_best && top_ef.len() >= ef {
            break;
        }

        if let Some(Some(curr_node)) = g.nodes.get(curr.ord as usize) {
            if let Some(neighbors) = curr_node.layers.get(layer) {
                for &nb in neighbors {
                    if vis.mark_visited(nb, gen) {
                        if let Some(Some(nb_node)) = g.nodes.get(nb as usize) {
                            let s = sq8_cosine_similarity(q_query, &nb_node.quantized);
                            let worst = top_ef.peek().map_or(-1.0, |m| m.0.score);
                            if top_ef.len() < ef || s > worst {
                                let item = ScoredNode { ord: nb, score: s };
                                candidates.push(item);
                                top_ef.push(MinScoredNode(item));
                                if top_ef.len() > ef {
                                    top_ef.pop();
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let mut sorted: Vec<ScoredNode> = top_ef.into_iter().map(|m| m.0).collect();
    sorted.sort_unstable_by(|a, b| b.score.total_cmp(&a.score));
    sorted
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hnsw_vector_index_recall_and_persistence() -> Result<()> {
        let path = std::env::temp_dir().join("storine_test_hnsw.idx");
        let _ = fs::remove_file(&path);

        let hnsw = HnswVectorIndex::new(HnswConfig::default());
        for i in 0..500u32 {
            let angle = (i as f32) * 0.0125;
            let vec: Vec<f32> = (0..32)
                .map(|d| (angle + (d as f32) * 0.1).cos())
                .collect();
            hnsw.insert(RecordOrdinal(i), &vec);
        }

        // Query near ordinal 250
        let target_angle = 250.0f32 * 0.0125;
        let query: Vec<f32> = (0..32)
            .map(|d| (target_angle + (d as f32) * 0.1).cos())
            .collect();

        let hits = hnsw.search_sq8_candidates(&query, 5, None);
        assert_eq!(hits.len(), 5);
        assert!(
            hits.iter().any(|h| h.ordinal == RecordOrdinal(250)),
            "top-5 HNSW hits should include exact match ordinal 250: {:?}",
            hits
        );

        // Save and reload from disk
        hnsw.save_to_disk(&path)?;
        let loaded = HnswVectorIndex::load_from_disk(&path, HnswConfig::default())?;
        assert_eq!(loaded.len(), 500);
        let hits2 = loaded.search_sq8_candidates(&query, 5, None);
        assert!(hits2.iter().any(|h| h.ordinal == RecordOrdinal(250)));

        let _ = fs::remove_file(&path);
        Ok(())
    }
}
