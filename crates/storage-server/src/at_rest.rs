//! Encryption at rest for the server binary (`TASK-156`, `F-SEC-02`).
//!
//! Keys live in a file named by `STORAGE_ENCRYPTION_KEYS`, one per line: `<id> <64 hex digits>`
//! (blank lines and `#` comments allowed). On Unix the file must be readable by its owner only.
//! `storage-server gen-key <file>` appends a new random key.
//!
//! - `STORAGE_ENCRYPTION_ACTIVE_KEY`: the key that seals new data (default: the highest id), or
//!   `none` to decrypt everything (turning encryption off).
//! - `STORAGE_ENCRYPTION_ALLOW_PLAINTEXT=1`: accept plaintext while existing data is converted
//!   (turning encryption on for a database that has data). Once the conversion finishes the
//!   server refuses plaintext again on its own; remove the setting before the next restart.
//!
//! At every start the server **reseals**: it rewrites whatever is not in the form the settings
//! require (sealed with the active key, or plaintext when it is `none`), so turning encryption
//! on, rotating keys and turning it off are each one restart. Records, the WAL, index files,
//! Raft logs and snapshots are covered. Every node of a cluster needs the same keys, since
//! snapshots travel between nodes as they are stored.

use std::path::Path;
use std::sync::Arc;

use storage_codec::{EncryptionKey, KeyRing};
use storage_core::{EncryptionConfig, ResealReport};
use storage_types::{Result, StorageError};

/// Encryption settings from the environment.
#[derive(Clone, Debug)]
pub struct AtRest {
    /// What the engine opens with (accepts plaintext while converting).
    pub startup: EncryptionConfig,
    /// What it switches to once resealing has finished (`None`: keep `startup`).
    pub after_reseal: Option<EncryptionConfig>,
}

fn bad(msg: impl Into<String>) -> StorageError {
    StorageError::config(msg)
}

fn parse_hex32(text: &str) -> Option<[u8; 32]> {
    if text.len() != 64 || !text.is_ascii() {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&text[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

/// Refuses a key file others can read (Unix): the keys protect every byte of the database.
fn check_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(path).map_err(|e| StorageError::io("read encryption key file", e))?.permissions().mode();
        if mode & 0o077 != 0 {
            return Err(bad(format!(
                "encryption key file {} can be read by other users (mode {:o}): run chmod 600 on it",
                path.display(),
                mode & 0o777
            )));
        }
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

/// Reads a key file: `(id, key)` pairs in file order.
pub fn read_key_file(path: &Path) -> Result<Vec<(u32, EncryptionKey)>> {
    check_permissions(path)?;
    let text = std::fs::read_to_string(path).map_err(|e| StorageError::io("read encryption key file", e))?;
    let mut keys: Vec<(u32, EncryptionKey)> = Vec::new();
    for (n, line) in text.lines().enumerate() {
        let line = line.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        let mut parts = line.split_whitespace();
        let (Some(id), Some(hex), None) = (parts.next(), parts.next(), parts.next()) else {
            return Err(bad(format!("{} line {}: expected `<id> <64 hex digits>`", path.display(), n + 1)));
        };
        let id: u32 = id.parse().ok().filter(|&i| i > 0).ok_or_else(|| bad(format!("{} line {}: the key id must be a number from 1", path.display(), n + 1)))?;
        let bytes = parse_hex32(hex).ok_or_else(|| bad(format!("{} line {}: the key must be 64 hex digits (32 bytes)", path.display(), n + 1)))?;
        if keys.iter().any(|(k, _)| *k == id) {
            return Err(bad(format!("{} line {}: key id {id} appears twice", path.display(), n + 1)));
        }
        keys.push((id, EncryptionKey::from_bytes(bytes)));
    }
    if keys.is_empty() {
        return Err(bad(format!("{} holds no key", path.display())));
    }
    Ok(keys)
}

/// Appends a new random key to `path` (created readable by its owner only) and returns its id.
pub fn add_key(path: &Path) -> Result<u32> {
    let existing = if path.exists() { read_key_file(path)? } else { Vec::new() };
    let id = existing.iter().map(|(i, _)| *i).max().unwrap_or(0).checked_add(1).ok_or_else(|| bad("no key id left"))?;
    let key: [u8; 32] = storage_codec::rand::os_random_array().map_err(|e| StorageError::io("generate an encryption key", e))?;
    let hex: String = key.iter().map(|b| format!("{b:02x}")).collect();
    let mut text = if path.exists() { std::fs::read_to_string(path).map_err(|e| StorageError::io("read encryption key file", e))? } else { String::from("# Storine encryption keys: <id> <64 hex digits>. Keep a copy: data sealed with a lost key is lost.\n") };
    if !text.is_empty() && !text.ends_with('\n') {
        text.push('\n');
    }
    text.push_str(&format!("{id} {hex}\n"));
    write_private(path, text.as_bytes())?;
    Ok(id)
}

/// Writes `bytes` to `path` atomically, readable by its owner only.
fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    let tmp = path.with_extension("tmp");
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut f = opts.open(&tmp).map_err(|e| StorageError::io("write encryption key file", e))?;
    f.write_all(bytes).and_then(|()| f.sync_all()).map_err(|e| StorageError::io("write encryption key file", e))?;
    drop(f);
    std::fs::rename(&tmp, path).map_err(|e| StorageError::io("replace encryption key file", e))
}

/// The settings from `STORAGE_ENCRYPTION_KEYS`, `STORAGE_ENCRYPTION_ACTIVE_KEY` and
/// `STORAGE_ENCRYPTION_ALLOW_PLAINTEXT` (`None`: encryption at rest is off).
pub fn from_env() -> Result<Option<AtRest>> {
    let Some(file) = std::env::var_os("STORAGE_ENCRYPTION_KEYS").filter(|f| !f.is_empty()) else {
        if std::env::var_os("STORAGE_ENCRYPTION_ACTIVE_KEY").is_some() {
            return Err(bad("STORAGE_ENCRYPTION_ACTIVE_KEY needs STORAGE_ENCRYPTION_KEYS (the key file)"));
        }
        return Ok(None);
    };
    let active = std::env::var("STORAGE_ENCRYPTION_ACTIVE_KEY").ok();
    let allow_plain = std::env::var("STORAGE_ENCRYPTION_ALLOW_PLAINTEXT").is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"));
    settings(&read_key_file(Path::new(&file))?, active.as_deref(), allow_plain).map(Some)
}

/// The settings for `keys`, the active key (`None`: the highest id; `"none"`: decrypt) and
/// whether plaintext is accepted while converting.
pub fn settings(keys: &[(u32, EncryptionKey)], active: Option<&str>, allow_plain: bool) -> Result<AtRest> {
    let ring = keys.iter().fold(KeyRing::new(), |r, (id, k)| r.with_key(*id, k.clone()));
    let active_key = match active.map(str::trim) {
        Some(a) if a.eq_ignore_ascii_case("none") => None,
        Some(a) => {
            let id: u32 = a.parse().map_err(|_| bad("STORAGE_ENCRYPTION_ACTIVE_KEY must be a key id or `none`"))?;
            if !keys.iter().any(|(k, _)| *k == id) {
                return Err(bad(format!("STORAGE_ENCRYPTION_ACTIVE_KEY {id} is not in the key file")));
            }
            Some(id)
        }
        None => keys.iter().map(|(id, _)| *id).max(),
    };
    let strict = EncryptionConfig { keys: ring, active_key, accept_unencrypted: active_key.is_none() };
    Ok(if allow_plain && active_key.is_some() {
        AtRest { startup: EncryptionConfig { accept_unencrypted: true, ..strict.clone() }, after_reseal: Some(strict) }
    } else {
        AtRest { startup: strict, after_reseal: None }
    })
}

/// Brings everything stored by `engine` into the form its settings require, then applies
/// `after` (the strict settings once plaintext is gone). `raft` reseals Raft logs and
/// snapshots alongside (single-group or range replicas).
pub fn reseal<T: storage_codec::ZeroCopyRecord + storage_index::Indexable>(
    engine: &storage_core::StorageEngine<T>,
    raft: impl FnOnce() -> Result<()>,
    after: Option<&EncryptionConfig>,
) -> Result<ResealReport> {
    let report = engine.reseal_all()?;
    raft()?;
    if let Some(cfg) = after {
        engine.set_encryption(Some(cfg.clone()))?;
    }
    Ok(report)
}

/// `,"encryption":...` for a health report.
pub fn health_field(enc: Option<Arc<EncryptionConfig>>) -> String {
    match enc {
        None => ",\"encryption\":\"off\"".into(),
        Some(e) => format!(
            ",\"encryption\":{{\"active_key\":{},\"keys\":{:?},\"accept_plaintext\":{}}}",
            e.active_key.map_or("null".to_string(), |k| k.to_string()),
            e.keys.ids(),
            e.accept_unencrypted
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("stge_keys_{name}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d.join("keys")
    }

    #[test]
    fn keys_are_generated_read_and_chosen() {
        let f = tmp("gen");
        assert_eq!(add_key(&f).unwrap(), 1);
        assert_eq!(add_key(&f).unwrap(), 2);
        let keys = read_key_file(&f).unwrap();
        assert_eq!(keys.iter().map(|(i, _)| *i).collect::<Vec<_>>(), [1, 2]);
        let s = settings(&keys, None, false).unwrap();
        assert_eq!((s.startup.active_key, s.startup.accept_unencrypted, s.after_reseal.is_none()), (Some(2), false, true));
        let s = settings(&keys, Some("1"), true).unwrap();
        assert_eq!((s.startup.active_key, s.startup.accept_unencrypted), (Some(1), true));
        assert!(!s.after_reseal.unwrap().accept_unencrypted, "strict once converted");
        let s = settings(&keys, Some("none"), false).unwrap();
        assert_eq!((s.startup.active_key, s.startup.accept_unencrypted), (None, true));
        assert!(settings(&keys, Some("9"), false).is_err());
        assert!(health_field(Some(Arc::new(s.startup))).contains("\"active_key\":null"));
    }

    #[test]
    fn bad_key_files_are_refused_with_the_line() {
        let f = tmp("bad");
        for (text, says) in [
            ("1 abc\n", "64 hex"),
            ("0 0000000000000000000000000000000000000000000000000000000000000000\n", "number from 1"),
            ("# only a comment\n", "no key"),
            ("1 0000000000000000000000000000000000000000000000000000000000000000\n1 1111111111111111111111111111111111111111111111111111111111111111\n", "twice"),
        ] {
            write_private(&f, text.as_bytes()).unwrap();
            let e = read_key_file(&f).unwrap_err().to_string();
            assert!(e.contains(says), "{text:?}: {e}");
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            add_key(&f.with_extension("ok")).unwrap();
            std::fs::set_permissions(f.with_extension("ok"), std::fs::Permissions::from_mode(0o644)).unwrap();
            assert!(read_key_file(&f.with_extension("ok")).unwrap_err().to_string().contains("chmod 600"));
        }
    }
}
