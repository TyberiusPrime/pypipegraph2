//! Crash injection between every pair of publish steps (PPG3_DESIGN.md
//! §12.1, CONTRACT.md "Publish must be resumable/crash-safe at every step
//! boundary"). `Store::publish` is decomposed into six `#[doc(hidden)] pub`
//! step functions (store.rs); this suite calls a prefix of them, drops
//! everything (simulating a crash — no further step runs, any lock held
//! is released by drop), and then performs one ordinary, fresh
//! `Store::publish` call with identical content. That fresh call must
//! succeed and `verify_entry` must pass afterwards.

use std::path::Path;

use ppg3_core::error::Error;
use ppg3_core::manifest::BuiltInfo;
use ppg3_core::store::{PublishOutcome, Staging, Store};

fn built() -> BuiltInfo {
    BuiltInfo {
        start_ms: 100,
        end_ms: 200,
        host: "crashtest".to_string(),
        sandboxed: false,
        ppg3_version: "0.1.0".to_string(),
        retain_evict: false,
    }
}

fn write_staging(store: &Store, content: &[(&str, &[u8])]) -> Staging {
    let staging = store.open_staging().expect("open_staging");
    for (name, bytes) in content {
        let p = staging.path().join(name);
        if let Some(parent) = p.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&p, bytes).unwrap();
    }
    staging
}

fn key_doc(ik: &str) -> serde_json::Value {
    serde_json::json!({"ppg3_key_version": 1, "job_recipe": ik})
}

/// Run the publish protocol through step `stop_after` (1..=5) and then stop
/// ("crash"): nothing further runs, and any RAII guard (the shared gc.lock
/// from step 4) is dropped/released at the end of this function.
fn crash_after_step(store: &Store, stop_after: u32, ik: &str, content: &[(&str, &[u8])]) {
    let staging = write_staging(store, content);
    let content_map = store.publish_step1_hash(&staging).expect("step1");
    if stop_after == 1 {
        return;
    }
    let (manifest, oh) = store
        .publish_step2_build_manifest(ik, content_map, &key_doc(ik), built(), None)
        .expect("step2");
    if stop_after == 2 {
        return;
    }
    store
        .publish_step3_write_staging_manifest(&staging, &manifest)
        .expect("step3");
    if stop_after == 3 {
        return;
    }
    let _lock = store.publish_step4_lock_shared().expect("step4");
    if stop_after == 4 {
        return; // lock released when _lock drops at end of fn
    }
    let tentative = store
        .publish_step5_resolve_entry(staging, &oh)
        .expect("step5");
    if stop_after == 5 {
        return; // entries/<oh> may now exist, but inputs/<ik> does not yet
    }
    let _ = store.publish_step6_resolve_input_symlink(ik, &oh, tentative);
}

fn assert_recovers(dir: &Path, ik: &str, content: &[(&str, &[u8])], stop_after: u32) {
    let store = Store::open("s", dir, false).expect("open store");
    crash_after_step(&store, stop_after, ik, content);

    // Fresh publish of identical content must succeed and leave a
    // consistent store, regardless of what partial state the "crash" left
    // behind.
    let staging2 = write_staging(&store, content);
    let outcome = store
        .publish(staging2, ik, &key_doc(ik), built(), None)
        .unwrap_or_else(|e| {
            panic!("recovery publish failed after crash at step {stop_after}: {e}")
        });

    let oh = match &outcome {
        PublishOutcome::Published { oh } | PublishOutcome::DedupHit { oh } => oh.clone(),
    };

    let manifest = store
        .lookup(ik)
        .unwrap_or_else(|e: Error| panic!("lookup failed after crash at step {stop_after}: {e}"))
        .unwrap_or_else(|| panic!("lookup returned None after crash at step {stop_after}"));
    assert_eq!(manifest.output_hash, oh);

    let report = store.verify_entry(&oh).expect("verify_entry");
    assert!(
        report.ok,
        "verify_entry failed after crash at step {stop_after}: {:?}",
        report.mismatches
    );
}

macro_rules! crash_test {
    ($name:ident, $stop_after:expr) => {
        #[test]
        fn $name() {
            let dir = tempfile::tempdir().unwrap();
            let ik = "a".repeat(64);
            assert_recovers(
                dir.path(),
                &ik,
                &[("out.txt", b"hello world"), ("sub/dir.txt", b"nested")],
                $stop_after,
            );
        }
    };
}

crash_test!(crash_between_step1_and_step2_hash_then_manifest, 1);
crash_test!(crash_between_step2_and_step3_manifest_then_write, 2);
crash_test!(crash_between_step3_and_step4_write_then_lock, 3);
crash_test!(crash_between_step4_and_step5_lock_then_resolve_entry, 4);
crash_test!(crash_between_step5_and_step6_entry_then_symlink, 5);

/// Same as the step5/6 boundary case, but explicitly checks the
/// intermediate state: after crashing right after step 5, the entry exists
/// on disk but `inputs/<ik>` does not yet - this is the specific
/// "published but not yet linked" window the shared gc.lock exists to
/// protect (PPG3_DESIGN.md §11).
#[test]
fn crash_after_step5_leaves_entry_without_input_symlink() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open("s", dir.path(), false).unwrap();
    let ik = "b".repeat(64);
    let content: &[(&str, &[u8])] = &[("out.txt", b"partial")];

    crash_after_step(&store, 5, &ik, content);

    assert!(
        store.lookup(&ik).unwrap().is_none(),
        "inputs/<ik> must not exist yet"
    );
    // But some entries/<oh> directory should already have been created.
    let entries = std::fs::read_dir(dir.path().join("v1").join("entries"))
        .unwrap()
        .count();
    assert_eq!(
        entries, 1,
        "the entry itself should have been published by step 5"
    );

    assert_recovers(dir.path(), &ik, content, 5);
}

/// Multiple sequential crashes at different points for the *same* ik must
/// all converge on one consistent entry once a full publish finally
/// completes.
#[test]
fn repeated_crashes_then_final_success_is_consistent() {
    let dir = tempfile::tempdir().unwrap();
    let ik = "c".repeat(64);
    let content: &[(&str, &[u8])] = &[("out.txt", b"converge")];
    for stop_after in 1..=5 {
        let store = Store::open("s", dir.path(), false).unwrap();
        crash_after_step(&store, stop_after, &ik, content);
    }
    assert_recovers(dir.path(), &ik, content, 5);

    let store = Store::open("s", dir.path(), true).unwrap();
    let m = store.lookup(&ik).unwrap().unwrap();
    let report = store.verify_entry(&m.output_hash).unwrap();
    assert!(report.ok, "{:?}", report.mismatches);
}
