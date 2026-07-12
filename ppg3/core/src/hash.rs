//! blake3 helpers (WP1/WP2-Rust, ppg3/CONTRACT.md "Deps allowed").
//!
//! All hashes are lowercase hex, no prefixes, 64 chars (256-bit blake3).

use std::fs::File;
use std::io::Read;
use std::path::Path;

use crate::error::Error;

/// Streaming chunk size for file hashing.
const CHUNK_SIZE: usize = 64 * 1024;

/// Hash arbitrary bytes, returning lowercase hex.
pub fn blake3_hex(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

/// Hash a file's contents by streaming it in 64KiB chunks, returning
/// lowercase hex. Does not follow symlinks specially — the caller is
/// responsible for rejecting symlinks before calling this (store.rs does).
pub fn blake3_file(path: &Path) -> Result<String, Error> {
    let mut file = File::open(path).map_err(|e| Error::io(path, e))?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = [0u8; CHUNK_SIZE];
    loop {
        let n = file.read(&mut buf).map_err(|e| Error::io(path, e))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn hex_of_empty_matches_known_blake3() {
        // blake3("") is a well-known test vector.
        assert_eq!(
            blake3_hex(b""),
            "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
        );
    }

    #[test]
    fn hex_is_64_lowercase_hex_chars() {
        let h = blake3_hex(b"hello world");
        assert_eq!(h.len(), 64);
        assert!(h
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
    }

    #[test]
    fn file_hash_matches_bytes_hash_small() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f.bin");
        let content = b"the quick brown fox jumps over the lazy dog";
        std::fs::write(&path, content).unwrap();
        assert_eq!(blake3_file(&path).unwrap(), blake3_hex(content));
    }

    #[test]
    fn file_hash_matches_bytes_hash_multi_chunk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f.bin");
        let mut content = vec![0u8; CHUNK_SIZE * 3 + 17];
        for (i, b) in content.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        let mut f = File::create(&path).unwrap();
        f.write_all(&content).unwrap();
        drop(f);
        assert_eq!(blake3_file(&path).unwrap(), blake3_hex(&content));
    }

    #[test]
    fn file_hash_missing_file_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("does-not-exist");
        assert!(blake3_file(&path).is_err());
    }
}
