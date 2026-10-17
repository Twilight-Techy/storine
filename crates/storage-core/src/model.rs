//! Built-in general-purpose zero-copy `AgentObjectRecord` (`ZeroCopyRecord + Indexable`)
//! for AI agent memory, knowledge graphs, hierarchical documents, and CLI/Server operations.

use storage_codec::{
    ArchiveBuilder, ArchivedBytes, ArchivedSlice, ArchivedString, ZeroCopyRecord,
};
use storage_graph::{
    ArchivedManyToMany, ArchivedOneToMany, ArchivedOneToOne, ManyToMany, OneToMany, OneToOne,
};
use storage_index::{IndexSnapshot, Indexable};
use storage_types::{InlineId, Result};

/// Secondary index field IDs for `AgentObjectRecord`.
pub const FIELD_TIMESTAMP: u16 = 1;
pub const FIELD_SCORE: u16 = 2;
pub const FIELD_PRIMARY_TAG: u16 = 1;

/// High-performance general-purpose object record supporting explicit `1:1`, `1:N`, and `M:N`
/// relationships, numeric/timestamp secondary indexing, tag bitmaps, and AI vector embeddings.
#[derive(Clone, Debug, PartialEq)]
pub struct AgentObjectRecord {
    pub id: InlineId,
    pub timestamp: i64,
    pub score: i64,
    pub title: String,
    pub body: Vec<u8>,
    pub tags: Vec<InlineId>,
    pub parent: OneToOne<AgentObjectRecord>,
    pub children: OneToMany<AgentObjectRecord>,
    pub peers: ManyToMany<AgentObjectRecord>,
    pub embedding: Vec<f32>,
}

/// Zero-copy `#[repr(C)]` archived layout mapped directly in virtual memory (`mmap`).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ArchivedAgentObjectRecord {
    pub id: InlineId,
    pub timestamp: i64,
    pub score: i64,
    pub title: ArchivedString,
    pub body: ArchivedBytes,
    pub tags: ArchivedSlice<InlineId>,
    pub parent: ArchivedOneToOne<AgentObjectRecord>,
    pub children: ArchivedOneToMany<AgentObjectRecord>,
    pub peers: ArchivedManyToMany<AgentObjectRecord>,
    pub embedding: ArchivedSlice<f32>,
}

impl AgentObjectRecord {
    pub fn new(id: InlineId, title: impl Into<String>, timestamp: i64, score: i64) -> Self {
        Self {
            id,
            timestamp,
            score,
            title: title.into(),
            body: Vec::new(),
            tags: Vec::new(),
            parent: OneToOne::NONE,
            children: OneToMany::new(),
            peers: ManyToMany::new(),
            embedding: Vec::new(),
        }
    }

    /// Extracts all related IDs (`parent`, `children`, `peers`) for referential integrity verification.
    pub fn all_related_ids(&self) -> Vec<InlineId> {
        let mut out =
            Vec::with_capacity(1 + self.children.len() + self.peers.len());
        if let Some(p) = self.parent.get() {
            out.push(p);
        }
        out.extend_from_slice(self.children.as_slice());
        out.extend_from_slice(self.peers.as_slice());
        out.sort_unstable();
        out.dedup();
        out
    }

    /// Removes `target_id` from `parent`, `children`, and `peers` (`OnDelete::Unlink`).
    pub fn unlink_all(&mut self, target_id: InlineId) {
        self.parent.clear_if_matches(target_id);
        self.children.unlink(target_id);
        self.peers.unlink(target_id);
    }
}

impl ZeroCopyRecord for AgentObjectRecord {
    type Archived = ArchivedAgentObjectRecord;
    const SCHEMA_VERSION: u16 = 1;
    const COLLECTION_ID: u16 = 1;

    fn archive(&self, builder: &mut ArchiveBuilder<'_>) -> Self::Archived {
        let title = builder.write_str(&self.title);
        let body = builder.write_bytes(&self.body);
        let tags = builder.write_slice(&self.tags);
        let parent = self.parent.archive();
        let children = self.children.archive(builder);
        let peers = self.peers.archive(builder);
        let embedding = builder.write_slice(&self.embedding);

        ArchivedAgentObjectRecord {
            id: self.id,
            timestamp: self.timestamp,
            score: self.score,
            title,
            body,
            tags,
            parent,
            children,
            peers,
            embedding,
        }
    }

    fn validate_archived(root: &Self::Archived, payload: &[u8]) -> Result<()> {
        InlineId::from_raw_bytes(root.id.to_raw_bytes())?;
        root.title.validate(payload)?;
        root.body.validate(payload)?;
        root.tags.validate(payload)?;
        for &tag in root.tags.as_slice(payload) {
            InlineId::from_raw_bytes(tag.to_raw_bytes())?;
        }
        root.parent.validate()?;
        root.children.validate(payload)?;
        root.peers.validate(payload)?;
        root.embedding.validate(payload)?;
        Ok(())
    }

    fn unarchive(root: &Self::Archived, payload: &[u8]) -> Result<Self> {
        Ok(Self {
            id: root.id,
            timestamp: root.timestamp,
            score: root.score,
            title: root.title.as_str(payload).to_owned(),
            body: root.body.as_slice(payload).to_vec(),
            tags: root.tags.as_slice(payload).to_vec(),
            parent: root.parent.unarchive(),
            children: root.children.unarchive(payload),
            peers: root.peers.unarchive(payload),
            embedding: root.embedding.as_slice(payload).to_vec(),
        })
    }
}

impl Indexable for AgentObjectRecord {
    fn extract_index_snapshot(&self) -> IndexSnapshot {
        let mut snap = IndexSnapshot::new();
        snap.push_numeric(FIELD_TIMESTAMP, self.timestamp);
        snap.push_numeric(FIELD_SCORE, self.score);
        for &tag in self.tags.iter().take(8) {
            snap.push_tag(FIELD_PRIMARY_TAG, tag);
        }
        if !self.embedding.is_empty() {
            snap.set_embedding_slice(&self.embedding);
        }
        snap
    }

    #[inline]
    fn extract_archived_snapshot(root: &Self::Archived, payload: &[u8]) -> Option<IndexSnapshot> {
        let mut snap = IndexSnapshot::new();
        snap.push_numeric(FIELD_TIMESTAMP, root.timestamp);
        snap.push_numeric(FIELD_SCORE, root.score);
        for &tag in root.tags.as_slice(payload).iter().take(8) {
            snap.push_tag(FIELD_PRIMARY_TAG, tag);
        }
        let emb = root.embedding.as_slice(payload);
        if !emb.is_empty() {
            snap.set_embedding_slice(emb);
        }
        Some(snap)
    }

    #[inline]
    fn embedding_slice(&self) -> Option<&[f32]> {
        if self.embedding.is_empty() {
            None
        } else {
            Some(&self.embedding)
        }
    }

    #[inline]
    fn archived_embedding_slice<'a>(
        root: &'a Self::Archived,
        payload: &'a [u8],
    ) -> Option<&'a [f32]> {
        let emb = root.embedding.as_slice(payload);
        if emb.is_empty() {
            None
        } else {
            Some(emb)
        }
    }
}
