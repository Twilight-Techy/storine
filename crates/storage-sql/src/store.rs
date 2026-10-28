//! Storage access through the Storine client: reads, scans and atomic tagged write batches.

use std::time::Duration;

use storine::{Client, Doc, Document, Error as StoreError, FieldOp, Filter, Query};

use crate::error::{SqlError, SqlResult};

/// What a write must find before it applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pre {
    None,
    Absent,
    Exists,
    Version(u64),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Op {
    Put(Doc),
    Delete,
    Patch(Vec<FieldOp>),
    /// Writes nothing; only checks the precondition.
    Check,
}

/// Why a write is in a batch, so a failed precondition becomes the right SQL error.
#[derive(Debug, Clone, PartialEq)]
pub enum Tag {
    /// A row of `table` (absent precondition failing: duplicate primary key).
    Row { table: String },
    /// A unique-constraint marker (absent precondition failing: duplicate value).
    Unique { table: String, constraint: String },
    /// A referenced parent row that must exist (foreign key).
    Parent { table: String, constraint: String },
    /// A row lock document.
    Lock,
    Internal,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Write {
    pub collection: String,
    pub key: String,
    pub op: Op,
    pub pre: Pre,
    pub tag: Tag,
}

/// Why an atomic batch did not apply.
#[derive(Debug)]
pub enum CommitError {
    /// Write `index`'s precondition failed (the document changed or exists / does not exist).
    Condition { index: usize },
    /// A patch targeted a missing document.
    Missing { index: usize },
    Other(SqlError),
}

impl CommitError {
    /// The SQL error for this failure, given the batch that failed.
    pub fn to_sql(&self, writes: &[Write]) -> SqlError {
        match self {
            CommitError::Condition { index } | CommitError::Missing { index } => match writes.get(*index) {
                Some(Write { tag: Tag::Row { table }, pre: Pre::Absent, .. }) => SqlError::unique(table, &format!("{table}_pkey")),
                Some(Write { tag: Tag::Unique { table, constraint }, pre: Pre::Absent, .. }) => SqlError::unique(table, constraint),
                Some(Write { tag: Tag::Parent { table, constraint }, .. }) => SqlError::foreign_key(format!(
                    "insert or update on table \"{table}\" violates foreign key constraint \"{constraint}\""
                )),
                _ => SqlError::serialization(),
            },
            CommitError::Other(e) => e.clone(),
        }
    }

    /// True when retrying the statement from scratch may succeed (a concurrent change).
    pub fn is_retryable(&self, writes: &[Write]) -> bool {
        self.to_sql(writes).code == "40001"
    }
}

/// The storage connection shared by every SQL session.
#[derive(Debug)]
pub struct Store {
    client: Client,
}

impl Store {
    pub fn new(client: Client) -> Store {
        Store { client }
    }

    /// Connects to `storine://...` (with the client's usual retries and failover).
    pub fn connect(url: &str, timeout: Duration) -> SqlResult<Store> {
        let client = storine::connect_with(url, storine::Options { timeout, ..storine::Options::default() })?;
        Ok(Store { client })
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    pub fn get(&self, collection: &str, key: &str) -> SqlResult<Option<Document>> {
        Ok(self.client.collection(collection).get(key)?)
    }

    /// Every document of `collection` matching `filter`, in key order.
    pub fn scan(&self, collection: &str, filter: Option<Filter>) -> SqlResult<Vec<Document>> {
        let mut out = Vec::new();
        for d in self.client.collection(collection).scan(filter) {
            out.push(d?);
        }
        Ok(out)
    }

    /// Visits matching documents in key order until `visit` returns false.
    pub fn scan_while(&self, collection: &str, filter: Option<Filter>, mut visit: impl FnMut(Document) -> SqlResult<bool>) -> SqlResult<()> {
        for d in self.client.collection(collection).scan(filter) {
            if !visit(d?)? {
                break;
            }
        }
        Ok(())
    }

    pub fn find(&self, collection: &str, query: Query) -> SqlResult<Vec<Document>> {
        Ok(self.client.collection(collection).find(query)?)
    }

    pub fn count(&self, collection: &str, filter: Option<Filter>) -> SqlResult<u64> {
        Ok(self.client.collection(collection).count(filter)?)
    }

    pub fn create_index(&self, collection: &str, field: &str) -> SqlResult<()> {
        Ok(self.client.collection(collection).create_index(field)?)
    }

    pub fn drop_index(&self, collection: &str, field: &str) -> SqlResult<()> {
        Ok(self.client.collection(collection).drop_index(field)?)
    }

    /// Storage indexes of a collection (ready and building).
    pub fn indexed_fields(&self, collection: &str) -> SqlResult<Vec<String>> {
        let info = self.client.collection(collection).info()?;
        Ok(info.indexes.into_iter().chain(info.building).collect())
    }

    pub fn drop_collection(&self, collection: &str) -> SqlResult<u64> {
        Ok(self.client.collection(collection).drop()?)
    }

    pub fn collections(&self) -> SqlResult<Vec<String>> {
        Ok(self.client.collections()?)
    }

    /// Applies `writes` atomically (all or none); returns the new versions.
    pub fn commit(&self, writes: &[Write]) -> Result<Vec<Option<u64>>, CommitError> {
        if writes.is_empty() {
            return Ok(Vec::new());
        }
        let r = self.client.transaction(|tx| {
            for w in writes {
                let (c, k) = (w.collection.as_str(), w.key.as_str());
                match (&w.op, w.pre) {
                    (Op::Put(d), Pre::None) => tx.put(c, k, d.clone()),
                    (Op::Put(d), Pre::Absent) => tx.insert(c, k, d.clone()),
                    (Op::Put(d), Pre::Exists) => tx.replace(c, k, d.clone()),
                    (Op::Put(d), Pre::Version(v)) => tx.replace_if_version(c, k, v, d.clone()),
                    (Op::Delete, Pre::Version(v)) => tx.delete_if_version(c, k, v),
                    (Op::Delete, _) => tx.delete(c, k),
                    (Op::Patch(ops), Pre::Version(v)) => tx.update_if_version(c, k, v, ops.clone()),
                    (Op::Patch(ops), _) => tx.update(c, k, ops.clone()),
                    (Op::Check, Pre::Version(v)) => tx.require_version(c, k, v),
                    (Op::Check, Pre::Absent) => tx.require_exists(c, k, false),
                    (Op::Check, _) => tx.require_exists(c, k, true),
                };
            }
            Ok(())
        });
        r.map_err(|e| match e {
            StoreError::Conflict { write_index: Some(i), .. } => CommitError::Condition { index: i as usize },
            StoreError::Conflict { .. } => CommitError::Condition { index: usize::MAX },
            StoreError::NotFound { write_index: Some(i), .. } => CommitError::Missing { index: i as usize },
            StoreError::NotFound { .. } => CommitError::Missing { index: usize::MAX },
            StoreError::Invalid(m) => CommitError::Other(SqlError::new("22023", m)),
            other => CommitError::Other(other.into()),
        })
    }
}
