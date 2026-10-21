//! Zero-allocation stack path resolver and filesystem-safe encoder (`TASK-009`, `F-CORE-04`, `F-CORE-05`, `RULE-MEM-02`).
//!
//! Security Guarantees:
//! - Prevents path traversal (`..`, `/`, `\`) and Windows NTFS Alternate Data Stream (`:`) injection
//!   by hex-encoding any character outside `[A-Za-z0-9_-]`.
//! - Formats relative record paths directly into a 256-byte stack buffer (`StackRelPath`) with
//!   **zero heap allocations**.

use std::fmt;
use std::path::{Path, PathBuf};
use storage_types::{InlineId, Result, StorageError};

/// Stack-allocated UTF-8 relative path buffer (`256 bytes`, zero heap allocation).
#[derive(Clone, Copy)]
pub struct StackRelPath {
    len: u16,
    buf: [u8; 254],
}

impl StackRelPath {
    pub const EMPTY: Self = Self {
        len: 0,
        buf: [0u8; 254],
    };

    #[inline]
    pub fn new() -> Self {
        Self::EMPTY
    }

    #[inline]
    pub fn push_str(&mut self, s: &str) -> Result<()> {
        self.push_bytes(s.as_bytes())
    }

    #[inline]
    pub fn push_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        let cur = self.len as usize;
        let next = cur
            .checked_add(bytes.len())
            .ok_or_else(|| StorageError::invalid_key("stack path overflow"))?;
        if next > self.buf.len() {
            return Err(StorageError::invalid_key(
                "relative path exceeds 254-byte stack buffer",
            ));
        }
        self.buf[cur..next].copy_from_slice(bytes);
        self.len = next as u16;
        Ok(())
    }

    #[inline]
    pub fn push_byte(&mut self, b: u8) -> Result<()> {
        self.push_bytes(&[b])
    }

    #[inline]
    pub fn push_hex_u8(&mut self, val: u8) -> Result<()> {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let pair = [HEX[(val >> 4) as usize], HEX[(val & 0x0F) as usize]];
        self.push_bytes(&pair)
    }

    /// Appends `raw` while percent-encoding any byte outside `[A-Za-z0-9_-]` so logical IDs
    /// like `"user:123"` or `"a/b"` never trigger NTFS ADS (`:`) or directory traversal.
    #[inline]
    pub fn push_safe_component(&mut self, raw: &str) -> Result<()> {
        const HEX: &[u8; 16] = b"0123456789ABCDEF";
        for &b in raw.as_bytes() {
            let is_safe = b.is_ascii_alphanumeric() || b == b'_' || b == b'-';
            if is_safe {
                self.push_byte(b)?;
            } else {
                let encoded = [b'%', HEX[(b >> 4) as usize], HEX[(b & 0x0F) as usize]];
                self.push_bytes(&encoded)?;
            }
        }
        Ok(())
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        // SAFETY: All bytes pushed into `self.buf` are valid ASCII/UTF-8.
        unsafe { core::str::from_utf8_unchecked(&self.buf[..self.len as usize]) }
    }

    #[inline]
    pub fn as_path(&self) -> &Path {
        Path::new(self.as_str())
    }
}

impl Default for StackRelPath {
    #[inline]
    fn default() -> Self {
        Self::EMPTY
    }
}

impl fmt::Debug for StackRelPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StackRelPath({:?})", self.as_str())
    }
}

impl fmt::Display for StackRelPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Resolves logical `(collection, optional parent_hierarchy, InlineId)` into sharded or
/// hierarchically grouped file paths (`F-CORE-04`, `F-CORE-05`).
#[derive(Clone, Debug)]
pub struct PathResolver {
    base_dir: PathBuf,
}

impl PathResolver {
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
        }
    }

    #[inline]
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    /// Formats the relative sharded record path on the stack with **zero heap allocations**:
    /// `records/<collection>/<shard_hi>/<shard_lo>/<safe_id>.rec`
    #[inline]
    pub fn relative_record_path(
        collection: &str,
        id: InlineId,
    ) -> Result<StackRelPath> {
        Self::relative_hierarchical_record_path(collection, None, id)
    }

    /// Formats a record path supporting optional parent-folder hierarchy grouping (`F-CORE-05`):
    /// - If `parent_group` is `None`: `records/<collection>/<shard_hi>/<shard_lo>/<safe_id>.rec`
    /// - If `parent_group` is `Some(parent_id)`: `groups/<collection>/<shard_hi>/<safe_parent>/<safe_id>.rec`
    #[inline]
    pub fn relative_hierarchical_record_path(
        collection: &str,
        parent_group: Option<InlineId>,
        id: InlineId,
    ) -> Result<StackRelPath> {
        if collection.is_empty() {
            return Err(StorageError::invalid_key("collection name cannot be empty"));
        }
        let mut out = StackRelPath::new();
        if let Some(parent) = parent_group {
            let (hi, _) = parent.shard_pair();
            out.push_str("groups/")?;
            out.push_safe_component(collection)?;
            out.push_byte(b'/')?;
            out.push_hex_u8(hi)?;
            out.push_byte(b'/')?;
            out.push_safe_component(parent.as_str())?;
            out.push_byte(b'/')?;
            out.push_safe_component(id.as_str())?;
            out.push_str(".rec")?;
        } else {
            let (hi, lo) = id.shard_pair();
            out.push_str("records/")?;
            out.push_safe_component(collection)?;
            out.push_byte(b'/')?;
            out.push_hex_u8(hi)?;
            out.push_byte(b'/')?;
            out.push_hex_u8(lo)?;
            out.push_byte(b'/')?;
            out.push_safe_component(id.as_str())?;
            out.push_str(".rec")?;
        }
        Ok(out)
    }

    /// Resolves the full on-disk `PathBuf` for a record.
    #[inline]
    pub fn full_record_path(
        &self,
        collection: &str,
        parent_group: Option<InlineId>,
        id: InlineId,
    ) -> Result<PathBuf> {
        let rel = Self::relative_hierarchical_record_path(collection, parent_group, id)?;
        Ok(self.base_dir.join(rel.as_path()))
    }

    /// Resolves the full on-disk `PathBuf` for a Hierarchy Bundle file (`<parent_id>.bundle`).
    #[inline]
    pub fn full_bundle_path(&self, collection: &str, parent_id: InlineId) -> Result<PathBuf> {
        let (hi, _) = parent_id.shard_pair();
        let mut rel = StackRelPath::new();
        rel.push_str("bundles/")?;
        rel.push_safe_component(collection)?;
        rel.push_byte(b'/')?;
        rel.push_hex_u8(hi)?;
        rel.push_byte(b'/')?;
        rel.push_safe_component(parent_id.as_str())?;
        rel.push_str(".bundle")?;
        Ok(self.base_dir.join(rel.as_path()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zero_alloc_stack_path_and_ntfs_safety() {
        let id = InlineId::new("user:123/admin").unwrap();
        let rel = PathResolver::relative_record_path("users", id).unwrap();
        let s = rel.as_str();
        assert!(s.starts_with("records/users/"));
        assert!(s.ends_with("/user%3A123%2Fadmin.rec"));
        assert!(!s.contains(':'));
        assert!(!s.contains(".."));
    }

    #[test]
    fn test_hierarchical_parent_group_path() {
        let parent = InlineId::new("agent_core").unwrap();
        let child = InlineId::new("memory_001").unwrap();
        let rel =
            PathResolver::relative_hierarchical_record_path("memories", Some(parent), child)
                .unwrap();
        assert!(rel.as_str().starts_with("groups/memories/"));
        assert!(rel.as_str().ends_with("/agent_core/memory_001.rec"));
    }
}
