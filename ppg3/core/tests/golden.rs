//! Golden key-document fixtures (PPG3_DESIGN.md §12.2, CONTRACT.md "Golden
//! fixtures"). Every `tests/golden/keydoc_*.json` must round-trip through
//! `canon::canonicalize`/`canon::input_key`; every `keydoc_reject_*.json`
//! must fail `canonicalize`.

use std::fs;
use std::path::PathBuf;

use ppg3_core::canon;
use serde_json::Value;

fn golden_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("tests")
        .join("golden")
}

#[test]
fn golden_fixtures_canonicalize_and_hash_correctly() {
    let dir = golden_dir();
    assert!(dir.is_dir(), "golden fixture dir missing: {dir:?}");

    let mut valid_count = 0usize;
    let mut reject_count = 0usize;

    let mut entries: Vec<PathBuf> = fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("reading {dir:?}: {e}"))
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("json"))
        .collect();
    entries.sort();
    assert!(!entries.is_empty(), "no golden fixtures found in {dir:?}");

    for path in entries {
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        let bytes = fs::read(&path).unwrap_or_else(|e| panic!("reading {path:?}: {e}"));
        let fixture: Value =
            serde_json::from_slice(&bytes).unwrap_or_else(|e| panic!("{name}: invalid json: {e}"));

        if name.starts_with("keydoc_reject") {
            reject_count += 1;
            assert_eq!(
                fixture.get("error"),
                Some(&Value::Bool(true)),
                "{name}: missing `error: true`"
            );
            let doc = fixture
                .get("doc")
                .unwrap_or_else(|| panic!("{name}: missing `doc`"));
            assert!(
                canon::canonicalize(doc).is_err(),
                "{name}: expected canonicalize() to reject this document"
            );
        } else if name.starts_with("keydoc_") {
            valid_count += 1;
            let doc = fixture
                .get("doc")
                .unwrap_or_else(|| panic!("{name}: missing `doc`"));
            let expected_canonical = fixture
                .get("canonical")
                .and_then(Value::as_str)
                .unwrap_or_else(|| panic!("{name}: missing `canonical`"));
            let expected_ik = fixture
                .get("ik")
                .and_then(Value::as_str)
                .unwrap_or_else(|| panic!("{name}: missing `ik`"));

            let canonical_bytes = canon::canonicalize(doc)
                .unwrap_or_else(|e| panic!("{name}: canonicalize() failed: {e}"));
            let canonical = String::from_utf8(canonical_bytes.clone())
                .unwrap_or_else(|e| panic!("{name}: canonical bytes not utf-8: {e}"));
            assert_eq!(
                canonical, expected_canonical,
                "{name}: canonical string mismatch"
            );

            // The stored canonical string must itself validate (it's what
            // gets hashed / written to disk elsewhere).
            canon::validate(canonical_bytes.as_slice())
                .unwrap_or_else(|e| panic!("{name}: canonical output failed validate(): {e}"));

            let ik = canon::input_key(&canonical_bytes)
                .unwrap_or_else(|e| panic!("{name}: input_key() failed: {e}"));
            assert_eq!(ik, expected_ik, "{name}: input key mismatch");
            assert_eq!(ik.len(), 64, "{name}: input key must be 64 hex chars");
        }
    }

    assert!(
        valid_count >= 6,
        "expected >= 6 valid keydoc fixtures, found {valid_count}"
    );
    assert!(
        reject_count >= 2,
        "expected >= 2 reject keydoc fixtures, found {reject_count}"
    );
}
