//! Mark/sweep GC (WP1, PPG3_DESIGN.md §11, CONTRACT.md "GC").
//!
//! Exclusive `gc.lock` for the whole run (publish only ever takes it
//! shared, so publish and GC can never interleave between an entry's
//! rename and its `inputs/<ik>` symlink creation — PPG3_DESIGN.md §11).
//!
//! Deviation from the terse CONTRACT.md description, documented here since
//! it is load-bearing for what "under budget" means:
//! - `retain=Evict`-marked (`.ppg3-evict-ok`) unrooted entries are always
//!   swept on every `gc()` call, independent of `max_size` — matching §7.3
//!   ("first-in-line for GC once no running lease references it", not
//!   "only once we're low on space").
//! - Log eviction (`evict_logs: bool`) is budget-driven: logs are only
//!   removed when `max_size` is set *and* currently exceeded, and are
//!   always exhausted before any ordinary (non-evict-marked) entry is
//!   touched, per §6.1a ("GC evicts logs/ before it ever touches
//!   entries/"). Without a `max_size`, `evict_logs` has no effect — we do
//!   not want a routine `gc()` call silently wiping build logs that are
//!   documented as valuable cross-machine cache-hit debugging data.

use std::collections::HashSet;
use std::path::PathBuf;
use std::time::SystemTime;

use serde::Serialize;

use crate::error::Error;
use crate::lease;
use crate::store::{self, GcLockGuard, Store};

#[derive(Debug, Clone, Default)]
pub struct GcPolicy {
    pub max_size: Option<u64>,
    pub evict_logs: bool,
    pub dry_run: bool,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct GcReport {
    pub removed_entries: Vec<String>,
    pub removed_logs: Vec<String>,
    pub removed_dangling_inputs: Vec<String>,
    pub bytes_freed: u64,
    pub remaining_size: u64,
    pub dry_run: bool,
}

pub fn run(store: &Store, policy: &GcPolicy) -> Result<GcReport, Error> {
    let _lock = GcLockGuard::lock_exclusive(&store.gc_lock_path())?;
    let mut report = GcReport {
        dry_run: policy.dry_run,
        ..Default::default()
    };

    // 1. Roots: view generations, pins, live leases.
    let mut rooted: HashSet<String> = store::collect_symlinked_ohs(&store.roots_dir())?;
    rooted.extend(store::collect_symlinked_ohs(&store.pins_dir())?);
    rooted.extend(lease::live_protected_ohs(&store.leases_dir())?);

    // 2. Dangling inputs/* symlinks (target entry no longer present).
    for name in store::list_dir_names(&store.inputs_dir())? {
        let link_path = store.inputs_dir().join(&name);
        let meta = match std::fs::symlink_metadata(&link_path) {
            Ok(m) => m,
            Err(_) => continue,
        };
        if !meta.file_type().is_symlink() {
            continue;
        }
        let target = std::fs::read_link(&link_path).map_err(|e| Error::io(&link_path, e))?;
        let resolved = link_path.parent().expect("inputs dir").join(&target);
        if !resolved.exists() {
            report.removed_dangling_inputs.push(name);
            if !policy.dry_run {
                std::fs::remove_file(&link_path).map_err(|e| Error::io(&link_path, e))?;
            }
        }
    }

    // 3. Sweep unrooted entries.
    let all_ohs = store::list_dir_names(&store.entries_dir())?;
    let mut candidates: Vec<String> = all_ohs
        .into_iter()
        .filter(|oh| !rooted.contains(oh))
        .collect();

    // 3a. `retain=Evict`-marked entries: always swept, independent of budget.
    let mut evict_marked: Vec<String> = candidates
        .iter()
        .filter(|oh| store::entry_has_evict_marker(&store.entry_dir(oh)))
        .cloned()
        .collect();
    evict_marked.sort();
    for oh in &evict_marked {
        let size = store::dir_size(&store.entry_dir(oh));
        report.bytes_freed += size;
        report.removed_entries.push(oh.clone());
        if !policy.dry_run {
            remove_entry(store, oh)?;
        }
    }
    let evict_marked_set: HashSet<&String> = evict_marked.iter().collect();
    candidates.retain(|oh| !evict_marked_set.contains(oh));

    let mut total_size = total_store_size(store).saturating_sub(report.bytes_freed);

    if let Some(budget) = policy.max_size {
        if total_size > budget && policy.evict_logs {
            let mut log_dirs = collect_log_leaf_dirs(store)?;
            log_dirs.sort_by_key(|(_, mtime)| *mtime);
            for (dir, _mtime) in log_dirs {
                if total_size <= budget {
                    break;
                }
                let size = store::dir_size(&dir);
                total_size = total_size.saturating_sub(size);
                report.bytes_freed += size;
                let rel = dir
                    .strip_prefix(store.logs_dir())
                    .unwrap_or(&dir)
                    .to_string_lossy()
                    .to_string();
                report.removed_logs.push(rel);
                if !policy.dry_run {
                    std::fs::remove_dir_all(&dir).map_err(|e| Error::io(&dir, e))?;
                }
            }
        }

        if total_size > budget {
            let mut lru: Vec<(String, i64)> = candidates
                .iter()
                .map(|oh| (oh.clone(), store::entry_atime_ms(&store.entry_dir(oh))))
                .collect();
            lru.sort_by_key(|(_, atime)| *atime);
            for (oh, _atime) in lru {
                if total_size <= budget {
                    break;
                }
                let size = store::dir_size(&store.entry_dir(&oh));
                total_size = total_size.saturating_sub(size);
                report.bytes_freed += size;
                report.removed_entries.push(oh.clone());
                if !policy.dry_run {
                    remove_entry(store, &oh)?;
                }
            }
        }
    }

    report.remaining_size = total_size;
    Ok(report)
}

fn remove_entry(store: &Store, oh: &str) -> Result<(), Error> {
    let entry_dir = store.entry_dir(oh);
    // Payload was made read-only at publish time (§4); chmod +w before
    // deleting (CONTRACT.md: "deleting read-only entries requires chmod +w
    // first").
    store::chmod_writable_recursive(&entry_dir)?;
    std::fs::remove_dir_all(&entry_dir).map_err(|e| Error::io(&entry_dir, e))
}

fn total_store_size(store: &Store) -> u64 {
    store::dir_size(&store.entries_dir()) + store::dir_size(&store.logs_dir())
}

fn collect_log_leaf_dirs(store: &Store) -> Result<Vec<(PathBuf, SystemTime)>, Error> {
    let mut out = Vec::new();
    let logs_dir = store.logs_dir();
    for ik in store::list_dir_names(&logs_dir)? {
        let ik_dir = logs_dir.join(&ik);
        for ts_host in store::list_dir_names(&ik_dir)? {
            let leaf = ik_dir.join(&ts_host);
            let mtime = std::fs::metadata(&leaf)
                .and_then(|m| m.modified())
                .unwrap_or(SystemTime::UNIX_EPOCH);
            out.push((leaf, mtime));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::BuiltInfo;
    use crate::store::PublishOutcome;

    fn sample_built(retain_evict: bool) -> BuiltInfo {
        BuiltInfo {
            start_ms: 0,
            end_ms: 1,
            host: "test".to_string(),
            sandboxed: false,
            ppg3_version: "0.1.0".to_string(),
            retain_evict,
        }
    }

    fn publish(store: &Store, ik: &str, bytes: &[u8], retain_evict: bool) -> PublishOutcome {
        let staging = store.open_staging().unwrap();
        std::fs::write(staging.path().join("out.txt"), bytes).unwrap();
        store
            .publish(
                staging,
                ik,
                &serde_json::json!({"k": ik}),
                sample_built(retain_evict),
                None,
            )
            .unwrap()
    }

    #[test]
    fn unrooted_entry_is_swept() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open("s", dir.path(), false).unwrap();
        let o = publish(&store, &"1".repeat(64), b"x", false);
        let report = store
            .gc(&GcPolicy {
                max_size: Some(0),
                ..Default::default()
            })
            .unwrap();
        assert!(report.removed_entries.contains(&o.oh().to_string()));
        assert!(!store.entry_dir(o.oh()).exists());
    }

    #[test]
    fn rooted_pinned_leased_entries_are_kept() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open("s", dir.path(), false).unwrap();
        let rooted = publish(&store, &"1".repeat(64), b"rooted", false);
        let pinned = publish(&store, &"2".repeat(64), b"pinned", false);
        let leased = publish(&store, &"3".repeat(64), b"leased", false);
        let unrooted = publish(&store, &"4".repeat(64), b"gone", false);

        store
            .add_root("proj", 1, &[rooted.oh().to_string()])
            .unwrap();
        store.pin("release", pinned.oh()).unwrap();
        let lease = store.lease("run1").unwrap();
        lease.protect(leased.oh()).unwrap();

        let report = store
            .gc(&GcPolicy {
                max_size: Some(0),
                ..Default::default()
            })
            .unwrap();

        assert!(store.entry_dir(rooted.oh()).exists());
        assert!(store.entry_dir(pinned.oh()).exists());
        assert!(store.entry_dir(leased.oh()).exists());
        assert!(!store.entry_dir(unrooted.oh()).exists());
        assert!(report.removed_entries.contains(&unrooted.oh().to_string()));

        drop(lease);
    }

    #[test]
    fn evict_marked_entries_swept_even_without_budget() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open("s", dir.path(), false).unwrap();
        let evictable = publish(&store, &"1".repeat(64), b"temp", true);
        let normal = publish(
            &store,
            &"2".repeat(64),
            b"keep-me-unrooted-but-not-evict-marked",
            false,
        );

        // No max_size budget at all - evict-marked should still go.
        let report = store.gc(&GcPolicy::default()).unwrap();
        assert!(!store.entry_dir(evictable.oh()).exists());
        assert!(report.removed_entries.contains(&evictable.oh().to_string()));
        // normal unrooted entry survives because there's no size pressure.
        assert!(store.entry_dir(normal.oh()).exists());
    }

    #[test]
    fn dangling_input_symlink_is_removed() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open("s", dir.path(), false).unwrap();
        let o = publish(&store, &"1".repeat(64), b"x", false);
        // Force-remove the entry out from under the symlink to simulate a
        // dangling link (normally GC removes entries itself, in order).
        store::chmod_writable_recursive(&store.entry_dir(o.oh())).unwrap();
        std::fs::remove_dir_all(store.entry_dir(o.oh())).unwrap();

        let report = store.gc(&GcPolicy::default()).unwrap();
        assert!(report.removed_dangling_inputs.contains(&"1".repeat(64)));
        assert!(!store.inputs_dir().join("1".repeat(64)).exists());
    }

    #[test]
    fn logs_evicted_before_entries_under_budget() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open("s", dir.path(), false).unwrap();
        let rooted = publish(&store, &"1".repeat(64), b"keep-this-content-rooted", false);
        store
            .add_root("proj", 1, &[rooted.oh().to_string()])
            .unwrap();

        let log_dir = store.log_dir_for(&"1".repeat(64)).unwrap();
        std::fs::write(log_dir.join("stdout.txt"), vec![0u8; 4096]).unwrap();

        let entries_size = store::dir_size(&store.entries_dir());
        let total = entries_size + store::dir_size(&store.logs_dir());
        // Budget allows the (rooted) entry but not the logs on top of it.
        let budget = entries_size;
        assert!(total > budget);

        let report = store
            .gc(&GcPolicy {
                max_size: Some(budget),
                evict_logs: true,
                dry_run: false,
            })
            .unwrap();

        assert!(
            store.entry_dir(rooted.oh()).exists(),
            "rooted entry must survive"
        );
        assert!(!log_dir.exists(), "logs must be evicted to make budget");
        assert!(!report.removed_logs.is_empty());
        assert!(
            report.removed_entries.is_empty(),
            "logs alone should have made budget"
        );
    }

    #[test]
    fn dry_run_reports_without_deleting() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open("s", dir.path(), false).unwrap();
        let o = publish(&store, &"1".repeat(64), b"x", false);
        let report = store
            .gc(&GcPolicy {
                max_size: Some(0),
                dry_run: true,
                ..Default::default()
            })
            .unwrap();
        assert!(report.removed_entries.contains(&o.oh().to_string()));
        assert!(
            store.entry_dir(o.oh()).exists(),
            "dry_run must not delete anything"
        );
    }

    #[test]
    fn lru_order_respected_under_budget() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open("s", dir.path(), false).unwrap();
        let old = publish(&store, &"1".repeat(64), b"old-entry-content", false);
        std::thread::sleep(std::time::Duration::from_millis(5));
        let newer = publish(&store, &"2".repeat(64), b"newer-entry-content", false);
        // touch `newer` via lookup so its atime is fresher than `old`'s.
        std::thread::sleep(std::time::Duration::from_millis(5));
        store.lookup(&"2".repeat(64)).unwrap();

        let one_entry_size = store::dir_size(&store.entry_dir(newer.oh()));
        let report = store
            .gc(&GcPolicy {
                max_size: Some(one_entry_size),
                ..Default::default()
            })
            .unwrap();

        assert!(
            !store.entry_dir(old.oh()).exists(),
            "older/never-hit entry should be evicted first"
        );
        assert!(
            store.entry_dir(newer.oh()).exists(),
            "recently-hit entry should survive"
        );
        assert!(report.removed_entries.contains(&old.oh().to_string()));
    }
}
