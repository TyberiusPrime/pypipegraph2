//! Manifest types (WP1, PPG3_DESIGN.md §10.1 / CONTRACT.md "Manifest").
//!
//! `manifest.json` is the authoritative, immutable record stored inside
//! `entries/<oh>/manifest.json`. `output_hash(content)` is blake3 of the
//! canonical-JSON serialization of the `content` map — this IS `oh`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::canon;
use crate::error::Error;
use crate::hash::blake3_hex;

/// One file's record within a store entry's content manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentEntry {
    pub blake3: String,
    /// 4-digit octal string of the permission bits, e.g. `"0644"`.
    pub mode: String,
    pub size: u64,
}

/// relative path (posix separators, relative to the entry's `data/`) -> entry.
/// `BTreeMap` gives us byte-sorted key order for free, matching canonical
/// JSON object key order.
pub type ContentMap = BTreeMap<String, ContentEntry>;

/// Non-hashed build metadata (CONTRACT.md `BuiltInfo`). Extra field vs.
/// PPG3_DESIGN.md §10.1's `built` shape: `retain_evict` (used by GC, §11);
/// serialized as the manifest's `"built"` object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuiltInfo {
    pub start_ms: i64,
    pub end_ms: i64,
    pub host: String,
    pub sandboxed: bool,
    pub ppg3_version: String,
    #[serde(default)]
    pub retain_evict: bool,
}

/// `manifest.json`, per PPG3_DESIGN.md §10.1.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    pub ppg3_manifest_version: u64,
    pub input_key: String,
    pub key_document: serde_json::Value,
    pub content: ContentMap,
    pub output_hash: String,
    pub built: BuiltInfo,
    /// Informational only; not part of any hash.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_view_name: Option<String>,
}

/// `oh` = blake3 hex of the canonical-JSON serialization of `content`.
pub fn output_hash(content: &ContentMap) -> Result<String, Error> {
    let value = serde_json::to_value(content)
        .map_err(|e| Error::Canon(format!("serializing content map: {e}")))?;
    let canon_bytes = canon::canonicalize(&value)?;
    Ok(blake3_hex(&canon_bytes))
}

/// Canonical-JSON bytes of a content map, used both to compute `oh` and to
/// byte-compare two entries' content manifests for the publish dedup check.
pub fn canonical_content_bytes(content: &ContentMap) -> Result<Vec<u8>, Error> {
    let value = serde_json::to_value(content)
        .map_err(|e| Error::Canon(format!("serializing content map: {e}")))?;
    canon::canonicalize(&value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_content() -> ContentMap {
        let mut m = ContentMap::new();
        m.insert(
            "a.txt".to_string(),
            ContentEntry {
                blake3: "a".repeat(64),
                mode: "0644".to_string(),
                size: 3,
            },
        );
        m.insert(
            "b/c.txt".to_string(),
            ContentEntry {
                blake3: "b".repeat(64),
                mode: "0755".to_string(),
                size: 7,
            },
        );
        m
    }

    #[test]
    fn output_hash_is_deterministic() {
        let c = sample_content();
        let h1 = output_hash(&c).unwrap();
        let h2 = output_hash(&c).unwrap();
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 64);
    }

    #[test]
    fn output_hash_changes_with_content() {
        let c1 = sample_content();
        let mut c2 = sample_content();
        c2.get_mut("a.txt").unwrap().size = 4;
        assert_ne!(output_hash(&c1).unwrap(), output_hash(&c2).unwrap());
    }

    #[test]
    fn manifest_roundtrips_through_json() {
        let m = Manifest {
            ppg3_manifest_version: 1,
            input_key: "i".repeat(64),
            key_document: serde_json::json!({"ppg3_key_version": 1}),
            content: sample_content(),
            output_hash: output_hash(&sample_content()).unwrap(),
            built: BuiltInfo {
                start_ms: 1,
                end_ms: 2,
                host: "host".to_string(),
                sandboxed: true,
                ppg3_version: "0.1.0".to_string(),
                retain_evict: false,
            },
            job_view_name: Some("results/x.tsv".to_string()),
        };
        let s = serde_json::to_string(&m).unwrap();
        let back: Manifest = serde_json::from_str(&s).unwrap();
        assert_eq!(m, back);
    }

    #[test]
    fn canonical_content_bytes_is_canonical_json() {
        let c = sample_content();
        let bytes = canonical_content_bytes(&c).unwrap();
        canon::validate(&bytes).unwrap();
    }
}
