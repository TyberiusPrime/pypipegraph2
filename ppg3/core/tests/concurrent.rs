//! Two-OS-process concurrency tests (PPG3_DESIGN.md §11.1, CONTRACT.md
//! acceptance bar: "concurrent publisher publishing the same ik+content
//! concurrently -> one Published + one DedupHit", "leased entry never
//! swept"). Spawns the real `store_helper` binary (`core/src/bin/`) as
//! separate OS processes against a shared store directory - this exercises
//! actual fcntl (`gc.lock`) contention and filesystem races, which
//! in-process threads sharing one `Store` handle would not.

use std::path::PathBuf;
use std::process::{Command, Output};
use std::time::Duration;

use ppg3_core::store::Store;

fn helper_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_store_helper"))
}

fn run_publish(root: &std::path::Path, ik: &str, content: &str) -> std::process::Child {
    Command::new(helper_bin())
        .arg(root)
        .arg("publish")
        .arg(ik)
        .arg(content)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn store_helper publish")
}

fn wait(child: std::process::Child) -> Output {
    child.wait_with_output().expect("wait for store_helper")
}

fn stdout_str(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// Same ik, same content, published concurrently from N separate
/// processes: exactly one must win (Published) and the rest must dedup
/// (DedupHit) - never a determinism violation, never more than one entry.
#[test]
fn concurrent_same_ik_same_content_one_published_rest_dedup() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    Store::open("s", &root, false).unwrap(); // pre-create layout once, sequentially

    let ik = "1".repeat(64);
    let n = 6;
    let children: Vec<_> = (0..n)
        .map(|_| run_publish(&root, &ik, "identical-content"))
        .collect();
    let outputs: Vec<Output> = children.into_iter().map(wait).collect();

    for out in &outputs {
        assert!(
            out.status.success(),
            "helper failed: stdout={} stderr={}",
            stdout_str(out),
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let lines: Vec<String> = outputs.iter().map(stdout_str).collect();
    let published = lines.iter().filter(|l| l.starts_with("PUBLISHED")).count();
    let dedup = lines.iter().filter(|l| l.starts_with("DEDUP")).count();
    assert_eq!(
        published, 1,
        "exactly one process should have created the entry; got: {lines:?}"
    );
    assert_eq!(dedup, n - 1, "the rest should have deduped; got: {lines:?}");

    // all processes must agree on the same oh
    let ohs: std::collections::HashSet<&str> =
        lines.iter().map(|l| l.split(' ').nth(1).unwrap()).collect();
    assert_eq!(
        ohs.len(),
        1,
        "all processes must agree on one output hash: {lines:?}"
    );

    let store = Store::open("s", &root, true).unwrap();
    let entries_dir = root.join("v1").join("entries");
    assert_eq!(
        std::fs::read_dir(&entries_dir).unwrap().count(),
        1,
        "exactly one entry must exist"
    );
    let m = store.lookup(&ik).unwrap().expect("ik should resolve");
    assert_eq!(m.output_hash, *ohs.iter().next().unwrap());
}

/// Same ik, *different* content published concurrently: at most one
/// process may win the `inputs/<ik>` symlink; every other process with
/// differing content must observe a determinism violation, never a silent
/// second winner (PPG3_DESIGN.md §11.1 "calc twice, throw one away", now
/// enforced).
#[test]
fn concurrent_same_ik_conflicting_content_exactly_one_winner() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    Store::open("s", &root, false).unwrap();

    let ik = "2".repeat(64);
    let n = 5;
    let children: Vec<_> = (0..n)
        .map(|i| run_publish(&root, &ik, &format!("distinct-content-{i}")))
        .collect();
    let outputs: Vec<Output> = children.into_iter().map(wait).collect();
    let lines: Vec<String> = outputs.iter().map(stdout_str).collect();

    let winners = outputs.iter().filter(|o| o.status.success()).count();
    let losers = outputs.iter().filter(|o| !o.status.success()).count();
    assert_eq!(
        winners, 1,
        "exactly one conflicting publish should win: {lines:?}"
    );
    assert_eq!(losers, n - 1);
    for out in &outputs {
        if !out.status.success() {
            let stdout = stdout_str(out);
            assert!(
                stdout.starts_with("ERROR"),
                "unexpected failure mode: {stdout}"
            );
            assert!(
                stdout.to_lowercase().contains("determinism"),
                "expected a determinism violation, got: {stdout}"
            );
        }
    }

    // inputs/<ik> must resolve to exactly the winner's oh.
    let winning_oh = lines
        .iter()
        .zip(outputs.iter())
        .find(|(_, o)| o.status.success())
        .map(|(l, _)| l.split(' ').nth(1).unwrap().to_string())
        .unwrap();
    let store = Store::open("s", &root, true).unwrap();
    let m = store
        .lookup(&ik)
        .unwrap()
        .expect("ik should resolve to the winner");
    assert_eq!(m.output_hash, winning_oh);
}

/// A leased (but otherwise unrooted) entry must survive a concurrent,
/// aggressive GC in another process; once the lease is released, the same
/// entry becomes sweepable again.
#[test]
fn leased_entry_survives_concurrent_gc() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let store = Store::open("s", &root, false).unwrap();

    let ik = "3".repeat(64);
    let staging = store.open_staging().unwrap();
    std::fs::write(staging.path().join("out.txt"), b"lease-me").unwrap();
    let outcome = store
        .publish(
            staging,
            &ik,
            &serde_json::json!({"ik": ik}),
            ppg3_core::manifest::BuiltInfo {
                start_ms: 0,
                end_ms: 1,
                host: "test".into(),
                sandboxed: false,
                ppg3_version: "0.1.0".into(),
                retain_evict: false,
            },
            None,
        )
        .unwrap();
    let oh = match outcome {
        ppg3_core::store::PublishOutcome::Published { oh } => oh,
        ppg3_core::store::PublishOutcome::DedupHit { oh } => oh,
    };
    let entry_dir = root.join("v1").join("entries").join(&oh);
    assert!(entry_dir.exists());

    // Hold a lease protecting `oh` in a separate process for 1.5s.
    let mut lease_child = Command::new(helper_bin())
        .arg(&root)
        .arg("lease-hold")
        .arg("run1")
        .arg(&oh)
        .arg("1500")
        .stdout(std::process::Stdio::piped())
        .spawn()
        .expect("spawn lease-hold");

    // Give the lease time to register on disk.
    std::thread::sleep(Duration::from_millis(300));

    // Aggressive GC from a second process while the lease is held.
    let gc_out = wait(
        Command::new(helper_bin())
            .arg(&root)
            .arg("gc")
            .arg("0")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("spawn gc"),
    );
    assert!(
        gc_out.status.success(),
        "gc failed: {}",
        String::from_utf8_lossy(&gc_out.stderr)
    );
    assert!(
        entry_dir.exists(),
        "leased entry must survive a concurrent gc"
    );

    // Let the lease finish and release.
    lease_child.wait().expect("wait lease-hold");

    // Now GC again: without the lease, the (still unrooted) entry must go.
    let gc_out2 = wait(
        Command::new(helper_bin())
            .arg(&root)
            .arg("gc")
            .arg("0")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("spawn gc"),
    );
    assert!(gc_out2.status.success());
    assert!(
        !entry_dir.exists(),
        "entry must be swept once the lease is released"
    );
}
