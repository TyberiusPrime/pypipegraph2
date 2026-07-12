//! Test-only helper binary, spawned as a real OS process by
//! `core/tests/concurrent.rs` to exercise the store's concurrency
//! guarantees (CONTRACT.md acceptance bar: "concurrent publisher +
//! publisher" and "concurrent publisher + gc" via two processes).
//!
//! Not part of the public API surface; intentionally minimal argv parsing.
//!
//! Usage:
//!   store_helper <store_root> publish <ik> <content>
//!   store_helper <store_root> gc <max_size_bytes>
//!   store_helper <store_root> lease-hold <run_id> <oh> <hold_ms>

use std::env;
use std::path::PathBuf;

use ppg3_core::gc::GcPolicy;
use ppg3_core::manifest::BuiltInfo;
use ppg3_core::store::{PublishOutcome, Store};

fn built() -> BuiltInfo {
    BuiltInfo {
        start_ms: 0,
        end_ms: 1,
        host: "store_helper".to_string(),
        sandboxed: false,
        ppg3_version: "0.1.0".to_string(),
        retain_evict: false,
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("usage: store_helper <store_root> <publish|gc|lease-hold> ...");
        std::process::exit(2);
    }
    let root = PathBuf::from(&args[1]);
    let cmd = args[2].as_str();

    match cmd {
        "publish" => {
            let ik = &args[3];
            let content = &args[4];
            let store = Store::open("s", &root, false).expect("open store");
            let staging = store.open_staging().expect("open staging");
            std::fs::write(staging.path().join("out.txt"), content.as_bytes())
                .expect("write staged file");
            match store.publish(
                staging,
                ik,
                &serde_json::json!({"ik": ik, "c": content}),
                built(),
                None,
            ) {
                Ok(PublishOutcome::Published { oh }) => println!("PUBLISHED {oh}"),
                Ok(PublishOutcome::DedupHit { oh }) => println!("DEDUP {oh}"),
                Err(e) => {
                    println!("ERROR {e}");
                    std::process::exit(1);
                }
            }
        }
        "gc" => {
            let max_size: u64 = args[3].parse().expect("max_size");
            let store = Store::open("s", &root, false).expect("open store");
            let report = store
                .gc(&GcPolicy {
                    max_size: Some(max_size),
                    evict_logs: true,
                    dry_run: false,
                })
                .expect("gc");
            println!(
                "GC removed_entries={} removed_logs={} bytes_freed={}",
                report.removed_entries.len(),
                report.removed_logs.len(),
                report.bytes_freed
            );
        }
        "lease-hold" => {
            let run_id = &args[3];
            let oh = &args[4];
            let hold_ms: u64 = args[5].parse().expect("hold_ms");
            let store = Store::open("s", &root, false).expect("open store");
            let lease = store.lease(run_id).expect("lease");
            lease.protect(oh).expect("protect");
            println!("LEASED");
            std::thread::sleep(std::time::Duration::from_millis(hold_ms));
            drop(lease);
            println!("RELEASED");
        }
        other => {
            eprintln!("unknown command: {other}");
            std::process::exit(2);
        }
    }
}
