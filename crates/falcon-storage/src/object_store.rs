//! Pluggable object storage: a backend that stores each value as an
//! independent blob addressed by key. This is the seam for third-party
//! storage — a local folder-of-files today, an object bucket (S3-compatible
//! or otherwise) tomorrow — without the engines needing to know which.
//!
//! File/object-per-key trades the fast batched WAL for simplicity and
//! maintainability: every key is a standalone object, trivially shardable,
//! inspectable, and portable to any blob store. Best for cold/remote data;
//! the warm WAL tier stays the fast local default.

use crate::engine::StorageError;
use async_trait::async_trait;
use std::path::{Path, PathBuf};

/// A key-addressed blob store. Implementations must be safe for concurrent
/// use (the engine serializes same-key writes via a lock table above this).
#[async_trait]
pub trait ObjectStore: Send + Sync {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError>;
    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError>;
    async fn delete(&self, key: &[u8]) -> Result<(), StorageError>;
    /// List all (key, value) pairs whose key starts with `prefix`.
    async fn list_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError>;
    /// Human-readable description for logs/health (e.g. "local:/data/ks").
    fn describe(&self) -> String;

    /// Approximate total bytes stored, for the durable-size gauge. Default 0
    /// for backends that can't answer cheaply (e.g. a remote bucket).
    fn approx_size_bytes(&self) -> u64 {
        0
    }
}

/// Local filesystem backend: one file per key under a directory. Keys are
/// percent-ish-encoded to safe filenames so arbitrary bytes (including '/')
/// map to a single flat file without escaping the directory.
pub struct LocalDirStore {
    root: PathBuf,
}

impl LocalDirStore {
    pub fn open(root: &Path) -> Result<Self, StorageError> {
        std::fs::create_dir_all(root)?;
        Ok(Self {
            root: root.to_path_buf(),
        })
    }

    fn path_for(&self, key: &[u8]) -> PathBuf {
        self.root.join(encode_key(key))
    }
}

/// Encode arbitrary key bytes to a single safe filename. Alphanumerics and
/// a few safe chars pass through; everything else becomes `%XX`. This keeps
/// keys human-readable when they're plain text and collision-free otherwise.
fn encode_key(key: &[u8]) -> String {
    let mut out = String::with_capacity(key.len() + 2);
    for &b in key {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.') {
            out.push(b as char);
        } else {
            out.push('%');
            out.push(hex_digit(b >> 4));
            out.push(hex_digit(b & 0xf));
        }
    }
    // A key encoding to empty (empty key) would be an invalid filename.
    if out.is_empty() {
        out.push_str("%00empty");
    }
    out
}

fn decode_key(name: &str) -> Option<Vec<u8>> {
    if name == "%00empty" {
        return Some(Vec::new());
    }
    let bytes = name.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hi = from_hex(*bytes.get(i + 1)?)?;
            let lo = from_hex(*bytes.get(i + 2)?)?;
            out.push((hi << 4) | lo);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    Some(out)
}

fn hex_digit(n: u8) -> char {
    match n {
        0..=9 => (b'0' + n) as char,
        _ => (b'a' + (n - 10)) as char,
    }
}

fn from_hex(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

#[async_trait]
impl ObjectStore for LocalDirStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        let path = self.path_for(key);
        let result = tokio::task::spawn_blocking(move || match std::fs::read(&path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StorageError::Io(e)),
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::other(e)))?;
        result
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        let path = self.path_for(key);
        let tmp = path.with_extension("tmp");
        let value = value.to_vec();
        tokio::task::spawn_blocking(move || {
            // Atomic per-key durability: write to a temp file, fsync, rename.
            // Rename is atomic on POSIX, so a reader never sees a half-write.
            use std::io::Write;
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(&value)?;
            f.sync_all()?;
            std::fs::rename(&tmp, &path)?;
            Ok::<(), std::io::Error>(())
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::other(e)))??;
        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> Result<(), StorageError> {
        let path = self.path_for(key);
        tokio::task::spawn_blocking(move || match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(StorageError::Io(e)),
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::other(e)))?
    }

    async fn list_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        let root = self.root.clone();
        let prefix = prefix.to_vec();
        tokio::task::spawn_blocking(move || {
            let mut out = Vec::new();
            let dir = match std::fs::read_dir(&root) {
                Ok(d) => d,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
                Err(e) => return Err(StorageError::Io(e)),
            };
            for entry in dir.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name.ends_with(".tmp") {
                    continue; // in-flight write
                }
                let Some(key) = decode_key(&name) else { continue };
                if key.starts_with(&prefix) {
                    if let Ok(value) = std::fs::read(entry.path()) {
                        out.push((key, value));
                    }
                }
            }
            Ok(out)
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::other(e)))?
    }

    fn describe(&self) -> String {
        format!("local-dir:{}", self.root.display())
    }

    fn approx_size_bytes(&self) -> u64 {
        std::fs::read_dir(&self.root)
            .map(|rd| {
                rd.flatten()
                    .filter_map(|e| e.metadata().ok())
                    .filter(|m| m.is_file())
                    .map(|m| m.len())
                    .sum()
            })
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_encoding_round_trips() {
        for k in [
            &b""[..],
            b"simple",
            b"with/slash",
            b"user:42",
            b"unicode-\xf0\x9f\x98\x80",
            &[0u8, 1, 2, 255],
        ] {
            let enc = encode_key(k);
            assert!(!enc.contains('/'), "encoded key must be a flat filename: {enc}");
            let dec = decode_key(&enc).expect("must decode");
            assert_eq!(dec, k, "round-trip failed for {k:?} (enc={enc})");
        }
    }
}
