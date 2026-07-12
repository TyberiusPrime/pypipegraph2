//! One-off generator for `tests/golden/*.json` fixtures. Computes
//! `canonical`/`ik` using ppg3-core's own canon::canonicalize/input_key so
//! the fixtures are self-consistent with the implementation under test.
//! Not part of the deliverable API; run with
//! `cargo run -p ppg3-core --example gen_golden` from `ppg3/`.

use std::path::Path;

use ppg3_core::canon;
use serde_json::{json, Value};

fn write_valid(dir: &Path, name: &str, doc: Value) {
    let canonical_bytes = canon::canonicalize(&doc).expect("canonicalize");
    let canonical = String::from_utf8(canonical_bytes.clone()).expect("utf8");
    let ik = canon::input_key(&canonical_bytes).expect("input_key");
    let fixture = json!({
        "doc": doc,
        "canonical": canonical,
        "ik": ik,
    });
    let path = dir.join(format!("{name}.json"));
    std::fs::write(
        &path,
        serde_json::to_string_pretty(&fixture).unwrap() + "\n",
    )
    .unwrap();
    println!("wrote {}", path.display());
}

fn write_reject(dir: &Path, name: &str, doc: Value) {
    // sanity: confirm it actually fails to canonicalize
    assert!(
        canon::canonicalize(&doc).is_err(),
        "{name} was expected to fail canonicalization"
    );
    let fixture = json!({ "doc": doc, "error": true });
    let path = dir.join(format!("{name}.json"));
    std::fs::write(
        &path,
        serde_json::to_string_pretty(&fixture).unwrap() + "\n",
    )
    .unwrap();
    println!("wrote {}", path.display());
}

fn main() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../tests/golden");
    std::fs::create_dir_all(&dir).unwrap();

    // 1. minimal, already-sorted document.
    write_valid(
        &dir,
        "keydoc_01_minimal",
        json!({
            "ppg3_key_version": 1,
            "job_recipe": "a".repeat(64),
            "inputs": {},
            "tools": {},
            "runtime": {"python_env": "b".repeat(64), "preload": [], "shim": "v1"},
            "env": {},
            "outputs_declared": ["out.txt"]
        }),
    );

    // 2. key sorting: keys deliberately given out of order, including a
    // case where ASCII byte order (uppercase < lowercase) matters.
    write_valid(
        &dir,
        "keydoc_02_key_sorting",
        json!({
            "outputs_declared": ["z.txt", "a.txt"],
            "ppg3_key_version": 1,
            "job_recipe": "c".repeat(64),
            "env": {"Zeta": "1", "alpha": "2", "Alpha": "3", "beta": "4"},
            "tools": {"zzz": "x", "aaa": "y"},
            "inputs": {"z_parent": "1".repeat(64), "a_parent": "2".repeat(64)},
            "runtime": {"shim": "v1", "preload": ["pandas", "numpy"], "python_env": "d".repeat(64)}
        }),
    );

    // 3. unicode strings: accented latin, CJK, emoji, in both keys and values.
    write_valid(
        &dir,
        "keydoc_03_unicode",
        json!({
            "ppg3_key_version": 1,
            "job_recipe": "e".repeat(64),
            "inputs": {"réference": "3".repeat(64), "データ": "4".repeat(64)},
            "tools": {},
            "runtime": {"python_env": "f".repeat(64), "preload": ["café☃"], "shim": "v1"},
            "env": {"GREETING": "héllo wörld ☃ 日本語 😀"},
            "outputs_declared": ["résultats/été.tsv"]
        }),
    );

    // 4. nested objects/arrays (structured params under inputs, ppg2-style
    // ParameterInvariant shapes: nested dict/list/bool/null/int).
    write_valid(
        &dir,
        "keydoc_04_nested_params",
        json!({
            "ppg3_key_version": 1,
            "job_recipe": "g".repeat(64),
            "inputs": {
                "params": {
                    "n_trees": 200,
                    "nested": {"z": [1, 2, 3], "a": {"deep": [true, false, null]}},
                    "labels": ["b", "a", "c"]
                }
            },
            "tools": {},
            "runtime": {"python_env": "h".repeat(64), "preload": [], "shim": "v1"},
            "env": {},
            "outputs_declared": ["model.pkl"]
        }),
    );

    // 5. subset-input hash: a parent job contributes only named files,
    // whose value is the hash of just those files (per §5, distinct from
    // a whole-parent output hash).
    write_valid(
        &dir,
        "keydoc_05_subset_inputs",
        json!({
            "ppg3_key_version": 1,
            "job_recipe": "i".repeat(64),
            "inputs": {
                "ref_subset": "j".repeat(64),
                "whole_parent": "k".repeat(64)
            },
            "tools": {"samtools": "l".repeat(64)},
            "runtime": {"python_env": "m".repeat(64), "preload": ["numpy"], "shim": "v1"},
            "env": {},
            "outputs_declared": ["counts.tsv", "counts.log"]
        }),
    );

    // 6. declared env vars only (no inheritance - §6 - just literal values,
    // including one that looks numeric but must stay a string).
    write_valid(
        &dir,
        "keydoc_06_env_vars",
        json!({
            "ppg3_key_version": 1,
            "job_recipe": "n".repeat(64),
            "inputs": {},
            "tools": {},
            "runtime": {"python_env": "o".repeat(64), "preload": [], "shim": "v1"},
            "env": {"OMP_NUM_THREADS": "4", "LC_ALL": "C.UTF-8", "SOURCE_DATE_EPOCH": "0"},
            "outputs_declared": ["log.txt"]
        }),
    );

    // 7. full §5 shape with every field populated, larger/more realistic.
    write_valid(
        &dir,
        "keydoc_07_full_shape",
        json!({
            "ppg3_key_version": 1,
            "job_recipe": "p".repeat(64),
            "inputs": {
                "reads": "q".repeat(64),
                "ref": "r".repeat(64)
            },
            "tools": {"samtools": "s".repeat(64), "bwa": "t".repeat(64)},
            "runtime": {
                "python_env": "u".repeat(64),
                "preload": ["numpy", "pandas"],
                "shim": "v1"
            },
            "env": {"OMP_NUM_THREADS": "4"},
            "outputs_declared": ["results/counts.tsv", "results/counts.log"]
        }),
    );

    // reject 1: a float parameter (decimal point).
    write_reject(
        &dir,
        "keydoc_reject_01_float_dot",
        json!({
            "ppg3_key_version": 1,
            "job_recipe": "v".repeat(64),
            "inputs": {"params": {"threshold": 0.5}},
            "tools": {},
            "runtime": {"python_env": "w".repeat(64), "preload": [], "shim": "v1"},
            "env": {},
            "outputs_declared": ["out.txt"]
        }),
    );

    // reject 2: a float via exponent notation (no decimal point, but still
    // not an integer literal - `1e3` parses to serde_json's f64 variant).
    write_reject(
        &dir,
        "keydoc_reject_02_float_exponent",
        json!({
            "ppg3_key_version": 1,
            "job_recipe": "x".repeat(64),
            "inputs": {"params": {"scale": 1e3}},
            "tools": {},
            "runtime": {"python_env": "y".repeat(64), "preload": [], "shim": "v1"},
            "env": {},
            "outputs_declared": ["out.txt"]
        }),
    );
}
