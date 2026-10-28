//! Document protocol handlers (`TASK-098`, `protocol/PROTOCOL.md` §5–§8).
//!
//! Each handler takes an already size- and role-checked request body and returns
//! `(status, version, response body)`; the reactor sends it (inline) or an admin job does (index
//! builds, collection drops).

use std::collections::BTreeMap;

use storage_codec::value_wire::{encode_map_body, SveReader};
use storage_core::document::{document_id, CollectionInfo, DocumentStore};
use storage_core::document_wire::{
    decode_query, decode_write_request, encode_page, encode_write_response, put_str,
};
use storage_types::{InlineId, StorageError, Version};

use crate::{
    STATUS_COMMIT_UNKNOWN, STATUS_CONFLICT, STATUS_ERROR, STATUS_INVALID, STATUS_NOT_FOUND,
    STATUS_NOT_LEADER, STATUS_OK, STATUS_OVERLOADED,
};

/// `OP_DOC_ADMIN` / `OP_DOC_INFO` sub-commands.
pub const ADMIN_CREATE_INDEX: u8 = 1;
pub const ADMIN_DROP_INDEX: u8 = 2;
pub const INFO_LIST_COLLECTIONS: u8 = 3;
pub const ADMIN_DROP_COLLECTION: u8 = 4;
pub const ADMIN_SET_EXTRA: u8 = 5;
pub const INFO_COLLECTION: u8 = 6;

pub(crate) type Reply = (u8, u64, Vec<u8>);

/// Maps an engine error to a wire status (the body carries the readable message).
pub(crate) fn status_for(e: &StorageError) -> u8 {
    match e {
        StorageError::RecordNotFound { .. } => STATUS_NOT_FOUND,
        StorageError::VersionConflict { .. }
        | StorageError::ConditionFailed { .. }
        | StorageError::RecordAlreadyExists { .. }
        | StorageError::TransactionAborted { .. } => STATUS_CONFLICT,
        StorageError::InvalidValue { .. } | StorageError::InvalidKey { .. } => STATUS_INVALID,
        StorageError::NotLeader { .. } | StorageError::ReadOnlyReplica => STATUS_NOT_LEADER,
        StorageError::CommitUnknown { .. } => STATUS_COMMIT_UNKNOWN,
        StorageError::ResourceExhausted { .. } => STATUS_OVERLOADED,
        _ => STATUS_ERROR,
    }
}

pub(crate) fn error_reply(e: &StorageError) -> Reply {
    let mut body = Vec::new();
    if let StorageError::CommitUnknown { index } = e {
        return (STATUS_COMMIT_UNKNOWN, *index, e.to_string().into_bytes());
    }
    if let StorageError::NotLeader { leader } = e {
        // Version field carries the leader's node id (0 = unknown).
        return (STATUS_NOT_LEADER, leader.unwrap_or(0), e.to_string().into_bytes());
    }
    body.extend_from_slice(e.to_string().as_bytes());
    (status_for(e), 0, body)
}

/// Conflict body: `[u32 write index | u8 exists | u64 current version | UTF-8 message]`.
fn conflict_reply(index: usize, actual: Option<Version>, e: &StorageError) -> Reply {
    let mut body = Vec::with_capacity(13 + 64);
    body.extend_from_slice(&(index as u32).to_le_bytes());
    body.push(u8::from(actual.is_some()));
    body.extend_from_slice(&actual.map_or(0, |v| v.0).to_le_bytes());
    body.extend_from_slice(e.to_string().as_bytes());
    (STATUS_CONFLICT, 0, body)
}

/// Index of the write addressing `id`, for conflict reports.
fn index_of(ids: &[InlineId], id: InlineId) -> usize {
    ids.iter().position(|&x| x == id).unwrap_or(u32::MAX as usize)
}

/// `OP_DOC_GET`: `str collection | str key` -> `map` (version in the header).
pub(crate) fn doc_get(store: &DocumentStore, body: &[u8]) -> Reply {
    let mut r = SveReader::new(body);
    let parsed = (|| -> storage_types::Result<(String, String)> {
        let c = r.str()?.to_owned();
        let k = r.str()?.to_owned();
        if r.remaining() != 0 {
            return Err(StorageError::invalid_value("trailing bytes after get request"));
        }
        Ok((c, k))
    })();
    let (c, k) = match parsed {
        Ok(v) => v,
        Err(e) => return error_reply(&e),
    };
    match store.get(&c, &k) {
        Ok(Some(doc)) => {
            let mut out = Vec::new();
            encode_map_body(&doc.fields, &mut out);
            (STATUS_OK, doc.version.0, out)
        }
        Ok(None) => (STATUS_NOT_FOUND, 0, Vec::new()),
        Err(e) => error_reply(&e),
    }
}

/// `OP_DOC_WRITE`: atomic batch of writes with preconditions, optionally idempotent.
pub(crate) fn doc_write(store: &DocumentStore, body: &[u8]) -> Reply {
    let (writes, request_id) = match decode_write_request(body) {
        Ok(v) => v,
        Err(e) => return error_reply(&e),
    };
    let ids: Vec<InlineId> = writes
        .iter()
        .map(|w| document_id(&w.collection, &w.key).unwrap_or(InlineId::EMPTY))
        .collect();
    match store.write(writes, request_id) {
        Ok(summary) => {
            let mut out = Vec::with_capacity(5 + summary.versions.len() * 9);
            encode_write_response(&summary.versions, summary.duplicate, &mut out);
            (STATUS_OK, 0, out)
        }
        Err(e @ StorageError::VersionConflict { id, actual, .. }) => {
            let actual = (actual.0 != 0).then_some(actual);
            conflict_reply(index_of(&ids, id), actual, &e)
        }
        Err(e @ StorageError::ConditionFailed { id, actual, .. }) => {
            conflict_reply(index_of(&ids, id), actual, &e)
        }
        Err(e @ StorageError::RecordNotFound { id }) => {
            // A patch of a missing document: report which write.
            let mut body = Vec::new();
            body.extend_from_slice(&(index_of(&ids, id) as u32).to_le_bytes());
            body.extend_from_slice(e.to_string().as_bytes());
            (STATUS_NOT_FOUND, 0, body)
        }
        Err(e) => error_reply(&e),
    }
}

/// `OP_DOC_QUERY` -> page.
pub(crate) fn doc_query(store: &DocumentStore, body: &[u8]) -> Reply {
    let q = match decode_query(body) {
        Ok(q) => q,
        Err(e) => return error_reply(&e),
    };
    match store.query(&q) {
        Ok(page) => {
            let mut out = Vec::new();
            encode_page(&page, &mut out);
            if out.len() > storage_types::MAX_PAYLOAD_SIZE as usize {
                return (
                    STATUS_INVALID,
                    0,
                    b"query result page exceeds the maximum frame size; use a smaller limit".to_vec(),
                );
            }
            (STATUS_OK, page.total as u64, out)
        }
        Err(e) => error_reply(&e),
    }
}

fn encode_info(info: &CollectionInfo, out: &mut Vec<u8>) {
    put_str(out, &info.name);
    out.extend_from_slice(&(info.indexes.len() as u32).to_le_bytes());
    for f in &info.indexes {
        put_str(out, f);
    }
    out.extend_from_slice(&(info.building.len() as u32).to_le_bytes());
    for f in &info.building {
        put_str(out, f);
    }
    encode_map_body(&info.extra, out);
}

/// `true` if this `OP_DOC_ADMIN` request may take long (runs as an admin job).
pub(crate) fn admin_is_slow(body: &[u8]) -> bool {
    matches!(body.first(), Some(&(ADMIN_CREATE_INDEX | ADMIN_DROP_INDEX | ADMIN_DROP_COLLECTION)))
}

/// `OP_DOC_ADMIN` / `OP_DOC_INFO`: `u8 sub-command | args`.
pub(crate) fn doc_admin(store: &DocumentStore, body: &[u8], allow_writes: bool) -> Reply {
    let mut r = SveReader::new(body);
    let mut run = || -> storage_types::Result<Reply> {
        let sub = r.u8()?;
        let is_write = matches!(sub, ADMIN_CREATE_INDEX | ADMIN_DROP_INDEX | ADMIN_DROP_COLLECTION | ADMIN_SET_EXTRA);
        if is_write && !allow_writes {
            return Err(StorageError::invalid_value("use OP_DOC_ADMIN for schema changes"));
        }
        let reply = match sub {
            ADMIN_CREATE_INDEX | ADMIN_DROP_INDEX => {
                let c = r.str()?.to_owned();
                let f = r.str()?.to_owned();
                if sub == ADMIN_CREATE_INDEX {
                    store.create_index(&c, &f)?;
                } else {
                    store.drop_index(&c, &f)?;
                }
                (STATUS_OK, 0, Vec::new())
            }
            INFO_LIST_COLLECTIONS => {
                let all = store.list_collections();
                let mut out = Vec::new();
                out.extend_from_slice(&(all.len() as u32).to_le_bytes());
                for info in &all {
                    encode_info(info, &mut out);
                }
                (STATUS_OK, all.len() as u64, out)
            }
            ADMIN_DROP_COLLECTION => {
                let c = r.str()?.to_owned();
                let n = store.drop_collection(&c)?;
                (STATUS_OK, n as u64, Vec::new())
            }
            ADMIN_SET_EXTRA => {
                let c = r.str()?.to_owned();
                let extra: BTreeMap<_, _> = r.map_body()?;
                store.set_collection_extra(&c, extra)?;
                (STATUS_OK, 0, Vec::new())
            }
            INFO_COLLECTION => {
                let c = r.str()?.to_owned();
                let mut out = Vec::new();
                encode_info(&store.collection_info(&c), &mut out);
                (STATUS_OK, 0, out)
            }
            _ => return Err(StorageError::invalid_value("unknown collection command")),
        };
        if r.remaining() != 0 {
            return Err(StorageError::invalid_value("trailing bytes after collection command"));
        }
        Ok(reply)
    };
    run().unwrap_or_else(|e| error_reply(&e))
}

