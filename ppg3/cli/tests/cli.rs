//! CLI integration tests (WP5, CONTRACT.md "CLI") — drives the built
//! `ppg3` binary via `assert_cmd`/`predicates` against real stores and
//! projects built directly through `ppg3-core` (no fakes: same publish
//! protocol, same `write_generation` the Python side will eventually call).

use std::path::{Path, PathBuf};

use assert_cmd::Command;
use predicates::prelude::*;

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

fn simple_doc(recipe: &str) -> serde_json::Value {
    serde_json::json!({
        "ppg3_key_version": 1, "job_recipe": recipe,
        "inputs": {}, "tools": {}, "env": {},
        "runtime": {"python_env": "x", "preload": [], "shim": "1"},
        "outputs_declared": ["out.txt"],
    })
}

/// Set up `<tmp>/proj/.ppg3/config.json` pointing at a single store rooted
/// at `<tmp>/store`. Returns (project root dir, .ppg3 dir, store).
fn setup_project(tmp: &Path) -> (PathBuf, PathBuf, Store) {
    let store_dir = tmp.join("store");
    let store = Store::open("main", &store_dir, false).unwrap();

    let project_root = tmp.join("proj");
    let project_dir = project_root.join(".ppg3");
    std::fs::create_dir_all(&project_dir).unwrap();

    let config = serde_json::json!({
        "stores": [
            {"name": "main", "path": store_dir.to_string_lossy(), "readonly": false}
        ]
    });
    std::fs::write(
        project_dir.join("config.json"),
        serde_json::to_vec_pretty(&config).unwrap(),
    )
    .unwrap();

    (project_root, project_dir, store)
}

fn one_entry_spec(view_path: &str, oh: String) -> ViewSpec {
    ViewSpec {
        entries: vec![ViewEntry {
            view_rel_path: view_path.to_string(),
            oh,
            path_within_entry: "out.txt".to_string(),
            store_index: 0,
        }],
    }
}

#[test]
fn generations_list_human_and_json() {
    let tmp = tempfile::tempdir().unwrap();
    let (project_root, project_dir, store) = setup_project(tmp.path());
    let oh = publish(
        &store,
        &"1".repeat(64),
        &simple_doc("r1"),
        &[("out.txt", b"v1")],
    );
    let stores = StoreSet::new(vec![store]);
    let n = views::write_generation(
        &project_dir,
        "proj",
        &stores,
        &one_entry_spec("out.txt", oh),
        false,
    )
    .unwrap();

    Command::cargo_bin("ppg3")
        .unwrap()
        .current_dir(&project_root)
        .args(["generations", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains(n.to_string()));

    let output = Command::cargo_bin("ppg3")
        .unwrap()
        .current_dir(&project_root)
        .args(["--json", "generations", "list"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let parsed: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let arr = parsed.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["n"].as_u64().unwrap(), n);
    assert!(arr[0]["current"].as_bool().unwrap());
    assert_eq!(arr[0]["n_entries"].as_u64().unwrap(), 1);
}

#[test]
fn generations_command_finds_ppg3_walking_up_from_subdirectory() {
    let tmp = tempfile::tempdir().unwrap();
    let (project_root, project_dir, store) = setup_project(tmp.path());
    let oh = publish(
        &store,
        &"2".repeat(64),
        &simple_doc("r1"),
        &[("out.txt", b"v1")],
    );
    let stores = StoreSet::new(vec![store]);
    views::write_generation(
        &project_dir,
        "proj",
        &stores,
        &one_entry_spec("out.txt", oh),
        false,
    )
    .unwrap();

    let subdir = project_root.join("nested/deeper");
    std::fs::create_dir_all(&subdir).unwrap();

    Command::cargo_bin("ppg3")
        .unwrap()
        .current_dir(&subdir)
        .args(["generations", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("1"));
}

#[test]
fn explain_reports_first_appearance_then_diff() {
    let tmp = tempfile::tempdir().unwrap();
    let (project_root, project_dir, store) = setup_project(tmp.path());
    let oh1 = publish(
        &store,
        &"3".repeat(64),
        &simple_doc("recipe-a"),
        &[("out.txt", b"v1")],
    );
    let stores = StoreSet::new(vec![store]);
    views::write_generation(
        &project_dir,
        "proj",
        &stores,
        &one_entry_spec("out.txt", oh1),
        false,
    )
    .unwrap();

    Command::cargo_bin("ppg3")
        .unwrap()
        .current_dir(&project_root)
        .args(["explain", "out.txt"])
        .assert()
        .success()
        .stdout(predicate::str::contains("first appearance"));

    // A second generation with a changed recipe should produce a real diff.
    let oh2 = publish(
        &stores.stores[0],
        &"4".repeat(64),
        &simple_doc("recipe-b"),
        &[("out.txt", b"v2")],
    );
    views::write_generation(
        &project_dir,
        "proj",
        &stores,
        &one_entry_spec("out.txt", oh2),
        false,
    )
    .unwrap();

    let output = Command::cargo_bin("ppg3")
        .unwrap()
        .current_dir(&project_root)
        .args(["--json", "explain", "outputs/out.txt"]) // exercise the outputs/ prefix stripping
        .output()
        .unwrap();
    assert!(output.status.success());
    let parsed: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(parsed["kind"], "Diff");
    assert_eq!(parsed["diff"]["recipe_changed"], true);
}

#[test]
fn rollback_default_goes_to_previous_generation() {
    let tmp = tempfile::tempdir().unwrap();
    let (project_root, project_dir, store) = setup_project(tmp.path());
    let oh1 = publish(
        &store,
        &"5".repeat(64),
        &simple_doc("r1"),
        &[("out.txt", b"one")],
    );
    let stores = StoreSet::new(vec![store]);
    let n1 = views::write_generation(
        &project_dir,
        "proj",
        &stores,
        &one_entry_spec("out.txt", oh1),
        false,
    )
    .unwrap();
    let oh2 = publish(
        &stores.stores[0],
        &"6".repeat(64),
        &simple_doc("r2"),
        &[("out.txt", b"two")],
    );
    views::write_generation(
        &project_dir,
        "proj",
        &stores,
        &one_entry_spec("out.txt", oh2),
        false,
    )
    .unwrap();

    Command::cargo_bin("ppg3")
        .unwrap()
        .current_dir(&project_root)
        .arg("rollback")
        .assert()
        .success()
        .stdout(predicate::str::contains(n1.to_string()));

    assert_eq!(
        std::fs::read_to_string(project_dir.join("views/current/out.txt")).unwrap(),
        "one"
    );

    // Explicit generation argument works too.
    let cur = views::current_generation_number(&project_dir)
        .unwrap()
        .unwrap();
    Command::cargo_bin("ppg3")
        .unwrap()
        .current_dir(&project_root)
        .args(["rollback", &cur.to_string()])
        .assert()
        .success();
}

#[test]
fn generations_rm_and_keep() {
    let tmp = tempfile::tempdir().unwrap();
    let (project_root, project_dir, store) = setup_project(tmp.path());
    let stores = StoreSet::new(vec![store]);
    let mut gens = Vec::new();
    for i in 0..4u8 {
        let oh = publish(
            &stores.stores[0],
            &format!("{i}").repeat(64),
            &simple_doc("r"),
            &[("out.txt", format!("v{i}").as_bytes())],
        );
        gens.push(
            views::write_generation(
                &project_dir,
                "proj",
                &stores,
                &one_entry_spec("out.txt", oh),
                false,
            )
            .unwrap(),
        );
    }

    // rm the oldest, non-current generation.
    Command::cargo_bin("ppg3")
        .unwrap()
        .current_dir(&project_root)
        .args(["generations", "rm", &gens[0].to_string()])
        .assert()
        .success();
    assert!(!project_dir.join("views").join(gens[0].to_string()).exists());

    // rm the current generation must fail with exit code 1.
    let cur = views::current_generation_number(&project_dir)
        .unwrap()
        .unwrap();
    Command::cargo_bin("ppg3")
        .unwrap()
        .current_dir(&project_root)
        .args(["generations", "rm", &cur.to_string()])
        .assert()
        .failure()
        .code(1);

    // keep 1 -> drops everything except current.
    Command::cargo_bin("ppg3")
        .unwrap()
        .current_dir(&project_root)
        .args(["generations", "keep", "1"])
        .assert()
        .success();
    let remaining = views::list_generations(&project_dir).unwrap();
    assert_eq!(remaining.len(), 1);
    assert!(remaining[0].current);
}

#[test]
fn diff_entries_cli_reports_changed_file() {
    let tmp = tempfile::tempdir().unwrap();
    let store_dir = tmp.path().join("store");
    let store = Store::open("main", &store_dir, false).unwrap();
    let doc = serde_json::json!({"ppg3_key_version": 1});
    let oh1 = publish(&store, &"7".repeat(64), &doc, &[("a.txt", b"hello world")]);
    let oh2 = publish(&store, &"8".repeat(64), &doc, &[("a.txt", b"hello WORLD")]);

    Command::cargo_bin("ppg3")
        .unwrap()
        .args(["diff-entries", &oh1, &oh2, "--store"])
        .arg(&store_dir)
        .assert()
        .success()
        .stdout(predicate::str::contains("a.txt"));

    let output = Command::cargo_bin("ppg3")
        .unwrap()
        .args(["--json", "diff-entries", &oh1, &oh2, "--store"])
        .arg(&store_dir)
        .output()
        .unwrap();
    assert!(output.status.success());
    let parsed: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(parsed["changed"][0]["path"], "a.txt");
    assert_eq!(parsed["changed"][0]["first_diff_offset"], 6);
}

#[test]
fn store_verify_succeeds_then_fails_after_corruption() {
    let tmp = tempfile::tempdir().unwrap();
    let store_dir = tmp.path().join("store");
    let store = Store::open("main", &store_dir, false).unwrap();
    let doc = serde_json::json!({"ppg3_key_version": 1});
    let oh = publish(
        &store,
        &"9".repeat(64),
        &doc,
        &[("a.txt", b"pristine content")],
    );

    Command::cargo_bin("ppg3")
        .unwrap()
        .args(["store", "verify", "--entry", &oh, "--store"])
        .arg(&store_dir)
        .assert()
        .success()
        .stdout(predicate::str::contains("OK"));

    // Corrupt the published (read-only) file: chmod +w, then edit it in
    // place, so verify_entry's rehash no longer matches the manifest.
    let data_file = store.data_dir(&oh).join("a.txt");
    let mut perms = std::fs::metadata(&data_file).unwrap().permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut perms, 0o644);
    std::fs::set_permissions(&data_file, perms).unwrap();
    std::fs::write(&data_file, b"TAMPERED CONTENT").unwrap();

    Command::cargo_bin("ppg3")
        .unwrap()
        .args(["store", "verify", "--entry", &oh, "--store"])
        .arg(&store_dir)
        .assert()
        .failure()
        .code(1)
        .stdout(predicate::str::contains("FAIL"));
}

#[test]
fn store_verify_sample_and_entry_are_mutually_exclusive_usage_error() {
    let tmp = tempfile::tempdir().unwrap();
    let store_dir = tmp.path().join("store");
    Store::open("main", &store_dir, false).unwrap();

    Command::cargo_bin("ppg3")
        .unwrap()
        .args([
            "store",
            "verify",
            "--sample",
            "10",
            "--entry",
            &"a".repeat(64),
            "--store",
        ])
        .arg(&store_dir)
        .assert()
        .failure()
        .code(2);
}

#[test]
fn store_gc_end_to_end_sweeps_unrooted_unpinned_entry() {
    let tmp = tempfile::tempdir().unwrap();
    let store_dir = tmp.path().join("store");
    let store = Store::open("main", &store_dir, false).unwrap();
    let doc = serde_json::json!({"ppg3_key_version": 1});

    let oh_rooted = publish(&store, &"a".repeat(64), &doc, &[("a.txt", b"rooted")]);
    let oh_pinned = publish(&store, &"b".repeat(64), &doc, &[("b.txt", b"pinned")]);
    let oh_orphan = publish(&store, &"c".repeat(64), &doc, &[("c.txt", b"orphan")]);

    // Root entry 1 via a real generation.
    let project_dir = tmp.path().join("proj/.ppg3");
    let stores = StoreSet::new(vec![store]);
    let spec = ViewSpec {
        entries: vec![ViewEntry {
            view_rel_path: "out.txt".to_string(),
            oh: oh_rooted.clone(),
            path_within_entry: "a.txt".to_string(),
            store_index: 0,
        }],
    };
    views::write_generation(&project_dir, "proj", &stores, &spec, false).unwrap();

    // Pin entry 2.
    stores.stores[0].pin("keepme", &oh_pinned).unwrap();

    // `gc()` only budget-sweeps unrooted/unpinned entries when `max_size`
    // is set and currently exceeded (WP1's gc.rs module doc / STATUS.md
    // deviation); pass a tiny budget so the lone orphan entry gets swept.
    // Rooted/pinned entries are excluded from the sweep candidate set
    // entirely, independent of budget, so they survive regardless.
    let output = Command::cargo_bin("ppg3")
        .unwrap()
        .args(["--json", "store", "gc", "--max-size", "1", "--store"])
        .arg(&store_dir)
        .output()
        .unwrap();
    assert!(output.status.success(), "gc failed: {output:?}");
    let parsed: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let removed: Vec<String> = parsed["removed_entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    assert_eq!(removed, vec![oh_orphan.clone()]);

    assert!(!store_dir.join("v1/entries").join(&oh_orphan).exists());
    assert!(store_dir.join("v1/entries").join(&oh_rooted).exists());
    assert!(store_dir.join("v1/entries").join(&oh_pinned).exists());
}

#[test]
fn unknown_subcommand_is_a_usage_error() {
    Command::cargo_bin("ppg3")
        .unwrap()
        .arg("not-a-real-command")
        .assert()
        .failure()
        .code(2);
}
