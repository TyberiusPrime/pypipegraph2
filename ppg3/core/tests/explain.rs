//! Integration tests for `explain.rs` (WP9, CONTRACT.md "explain/diff") that
//! exercise `explain_view_path`/`diff_entries` against real published
//! entries and real generations built through `views::write_generation`.

use ppg3_core::explain::{self, Explanation};
use ppg3_core::manifest::BuiltInfo;
use ppg3_core::store::Store;
use ppg3_core::storeset::StoreSet;
use ppg3_core::views::{self, ViewEntry, ViewSpec};

fn built() -> BuiltInfo {
    BuiltInfo {
        start_ms: 0,
        end_ms: 1,
        host: "test-host".to_string(),
        sandboxed: false,
        ppg3_version: "0.1.0".to_string(),
        retain_evict: false,
    }
}

fn publish(
    store: &Store,
    ik: &str,
    key_doc: &serde_json::Value,
    files: &[(&str, &[u8])],
) -> String {
    let staging = store.open_staging().unwrap();
    for (name, content) in files {
        std::fs::write(staging.path().join(name), content).unwrap();
    }
    let outcome = store.publish(staging, ik, key_doc, built(), None).unwrap();
    outcome.oh().to_string()
}

fn one_entry_spec(view_path: &str, oh: String, file_in_entry: &str) -> ViewSpec {
    ViewSpec {
        entries: vec![ViewEntry {
            view_rel_path: view_path.to_string(),
            oh,
            path_within_entry: file_in_entry.to_string(),
            store_index: 0,
        }],
    }
}

#[test]
fn explain_first_appearance_when_no_previous_generation() {
    let store_dir = tempfile::tempdir().unwrap();
    let store = Store::open("s", store_dir.path(), false).unwrap();
    let doc = serde_json::json!({
        "ppg3_key_version": 1, "job_recipe": "r1",
        "inputs": {}, "tools": {}, "env": {},
        "runtime": {"python_env": "x", "preload": [], "shim": "1"},
        "outputs_declared": ["out.txt"],
    });
    let oh = publish(&store, &"a".repeat(64), &doc, &[("out.txt", b"v1")]);

    let stores = StoreSet::new(vec![store]);
    let project = tempfile::tempdir().unwrap();
    let project_dir = project.path().join(".ppg3");
    let spec = one_entry_spec("out.txt", oh.clone(), "out.txt");
    views::write_generation(&project_dir, "proj", &stores, &spec, false).unwrap();

    let explanation = explain::explain_view_path(&project_dir, &stores, "out.txt").unwrap();
    match explanation {
        Explanation::FirstAppearance {
            view_path,
            oh: got_oh,
            ..
        } => {
            assert_eq!(view_path, "out.txt");
            assert_eq!(got_oh, oh);
        }
        other => panic!("expected FirstAppearance, got {other:?}"),
    }
}

#[test]
fn explain_diffs_current_against_previous_generation() {
    let store_dir = tempfile::tempdir().unwrap();
    let store = Store::open("s", store_dir.path(), false).unwrap();

    let doc_v1 = serde_json::json!({
        "ppg3_key_version": 1, "job_recipe": "recipe-v1",
        "inputs": {"x": "1".repeat(64)}, "tools": {}, "env": {"A": "1"},
        "runtime": {"python_env": "x", "preload": [], "shim": "1"},
        "outputs_declared": ["out.txt"],
    });
    let doc_v2 = serde_json::json!({
        "ppg3_key_version": 1, "job_recipe": "recipe-v2",
        "inputs": {"x": "2".repeat(64)}, "tools": {}, "env": {"A": "1", "B": "2"},
        "runtime": {"python_env": "x", "preload": [], "shim": "1"},
        "outputs_declared": ["out.txt"],
    });
    let oh1 = publish(&store, &"b".repeat(64), &doc_v1, &[("out.txt", b"v1")]);
    let oh2 = publish(&store, &"c".repeat(64), &doc_v2, &[("out.txt", b"v2")]);

    let stores = StoreSet::new(vec![store]);
    let project = tempfile::tempdir().unwrap();
    let project_dir = project.path().join(".ppg3");

    let n1 = views::write_generation(
        &project_dir,
        "proj",
        &stores,
        &one_entry_spec("out.txt", oh1.clone(), "out.txt"),
        false,
    )
    .unwrap();
    let n2 = views::write_generation(
        &project_dir,
        "proj",
        &stores,
        &one_entry_spec("out.txt", oh2.clone(), "out.txt"),
        false,
    )
    .unwrap();

    let explanation = explain::explain_view_path(&project_dir, &stores, "out.txt").unwrap();
    match explanation {
        Explanation::Diff {
            previous_generation,
            current_generation,
            oh_a,
            oh_b,
            diff,
            ..
        } => {
            assert_eq!(previous_generation, n1);
            assert_eq!(current_generation, n2);
            assert_eq!(oh_a, oh1);
            assert_eq!(oh_b, oh2);
            assert!(diff.recipe_changed);
            assert_eq!(diff.inputs.changed, vec!["x".to_string()]);
            assert_eq!(diff.env.added, vec!["B".to_string()]);
        }
        other => panic!("expected Diff, got {other:?}"),
    }
}

#[test]
fn explain_skips_dropped_intermediate_generations() {
    let store_dir = tempfile::tempdir().unwrap();
    let store = Store::open("s", store_dir.path(), false).unwrap();
    let mk_doc = |recipe: &str| {
        serde_json::json!({
            "ppg3_key_version": 1, "job_recipe": recipe,
            "inputs": {}, "tools": {}, "env": {},
            "runtime": {"python_env": "x", "preload": [], "shim": "1"},
            "outputs_declared": ["out.txt"],
        })
    };
    let oh1 = publish(
        &store,
        &"d".repeat(64),
        &mk_doc("r1"),
        &[("out.txt", b"v1")],
    );
    let oh2 = publish(
        &store,
        &"e".repeat(64),
        &mk_doc("r2"),
        &[("out.txt", b"v2")],
    );
    let oh3 = publish(
        &store,
        &"f".repeat(64),
        &mk_doc("r3"),
        &[("out.txt", b"v3")],
    );

    let stores = StoreSet::new(vec![store]);
    let project = tempfile::tempdir().unwrap();
    let project_dir = project.path().join(".ppg3");

    let n1 = views::write_generation(
        &project_dir,
        "proj",
        &stores,
        &one_entry_spec("out.txt", oh1.clone(), "out.txt"),
        false,
    )
    .unwrap();
    let n2 = views::write_generation(
        &project_dir,
        "proj",
        &stores,
        &one_entry_spec("out.txt", oh2, "out.txt"),
        false,
    )
    .unwrap();
    let n3 = views::write_generation(
        &project_dir,
        "proj",
        &stores,
        &one_entry_spec("out.txt", oh3.clone(), "out.txt"),
        false,
    )
    .unwrap();

    // Drop the middle generation (n2); explain must fall back to n1.
    views::drop_generation(&project_dir, &stores, n2).unwrap();

    let explanation = explain::explain_view_path(&project_dir, &stores, "out.txt").unwrap();
    match explanation {
        Explanation::Diff {
            previous_generation,
            current_generation,
            oh_a,
            oh_b,
            ..
        } => {
            assert_eq!(previous_generation, n1);
            assert_eq!(current_generation, n3);
            assert_eq!(oh_a, oh1);
            assert_eq!(oh_b, oh3);
        }
        other => panic!("expected Diff, got {other:?}"),
    }
}

#[test]
fn diff_entries_reports_added_removed_and_first_diff_offset() {
    let store_dir = tempfile::tempdir().unwrap();
    let store = Store::open("s", store_dir.path(), false).unwrap();
    let doc = serde_json::json!({"ppg3_key_version": 1});

    let oh_a = publish(
        &store,
        &"1".repeat(64),
        &doc,
        &[
            ("same.txt", b"unchanged"),
            ("gone.txt", b"bye"),
            ("changed.txt", b"AAAAhello"),
        ],
    );
    let oh_b = publish(
        &store,
        &"2".repeat(64),
        &doc,
        &[
            ("same.txt", b"unchanged"),
            ("new.txt", b"hi"),
            ("changed.txt", b"AAAAworld"),
        ],
    );

    let diff = explain::diff_entries(&store, &oh_a, &oh_b).unwrap();
    assert_eq!(diff.added, vec!["new.txt".to_string()]);
    assert_eq!(diff.removed, vec!["gone.txt".to_string()]);
    assert_eq!(diff.changed.len(), 1);
    let c = &diff.changed[0];
    assert_eq!(c.path, "changed.txt");
    assert_eq!(c.size_a, 9);
    assert_eq!(c.size_b, 9);
    // "AAAAhello" vs "AAAAworld" first differ at index 4.
    assert_eq!(c.first_diff_offset, Some(4));
    assert_ne!(c.blake3_a, c.blake3_b);
}

#[test]
fn diff_entries_identical_entries_have_no_changes() {
    let store_dir = tempfile::tempdir().unwrap();
    let store = Store::open("s", store_dir.path(), false).unwrap();
    let doc = serde_json::json!({"ppg3_key_version": 1});
    let oh = publish(&store, &"3".repeat(64), &doc, &[("a.txt", b"x")]);
    let diff = explain::diff_entries(&store, &oh, &oh).unwrap();
    assert!(diff.added.is_empty());
    assert!(diff.removed.is_empty());
    assert!(diff.changed.is_empty());
}

#[test]
fn list_entries_enumerates_published_entries() {
    let store_dir = tempfile::tempdir().unwrap();
    let store = Store::open("s", store_dir.path(), false).unwrap();
    let doc = serde_json::json!({"ppg3_key_version": 1});
    let oh1 = publish(&store, &"4".repeat(64), &doc, &[("a.txt", b"1")]);
    let oh2 = publish(&store, &"5".repeat(64), &doc, &[("a.txt", b"2")]);

    let mut entries = explain::list_entries(&store).unwrap();
    entries.sort();
    let mut expected = vec![oh1, oh2];
    expected.sort();
    assert_eq!(entries, expected);
}
