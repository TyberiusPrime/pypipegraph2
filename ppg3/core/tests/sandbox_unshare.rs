//! Runtime test for `sandbox::enter_sandbox` (the unshare/pivot_root
//! no-exec path, PPG3_DESIGN.md §6.4). Only built with `--features
//! linux-sandbox`; drives the `sandbox_helper` binary as a real OS
//! process (same pattern as `concurrent.rs`/`store_helper`), because
//! `unshare(CLONE_NEWUSER)` requires a single-threaded caller and a
//! successful `pivot_root` would hijack the test harness process.
#![cfg(all(target_os = "linux", feature = "linux-sandbox"))]

use std::process::Command;

#[test]
fn enter_sandbox_runtime_smoke() {
    let helper = env!("CARGO_BIN_EXE_sandbox_helper");
    let new_root = tempfile::tempdir().unwrap();
    let input = tempfile::tempdir().unwrap();
    std::fs::write(input.path().join("marker.txt"), "marker\n").unwrap();

    let out = Command::new(helper)
        .arg(new_root.path())
        .arg(input.path())
        .output()
        .expect("spawning sandbox_helper");

    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    match out.status.code() {
        Some(0) => {
            assert!(
                stdout.contains("SANDBOX-OK"),
                "helper exited 0 without SANDBOX-OK\nstdout: {stdout}\nstderr: {stderr}"
            );
        }
        Some(2) => {
            eprintln!("skipping: user namespaces unavailable on this host\n{stderr}");
        }
        code => panic!("sandbox_helper failed (exit {code:?})\nstdout: {stdout}\nstderr: {stderr}"),
    }
}
