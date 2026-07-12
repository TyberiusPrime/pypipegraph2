//! Leases and intents (WP1, PPG3_DESIGN.md §11/§11.1, CONTRACT.md
//! `store.lease()`/`store.write_intent()`).
//!
//! * A **lease** (`leases/<runid>.json`) is a running coordinator's GC
//!   protection. `heartbeat()` bumps its timestamp/mtime; the scheduler is
//!   expected to call it periodically (design says every 5 min against a
//!   30 min staleness window). `Drop` removes the file — a live process
//!   holding the `Lease` value is the only thing keeping it alive.
//! * An **intent** (`intents/<ik>.json`) is advisory only ("I am building
//!   this ik"): host/pid/heartbeat, ignored once stale (>30 min).
//!   Correctness never depends on intents (§11.1) — they are a
//!   duplicate-work optimization hint, not a lock.
//!
//! Deviation from the one-line CONTRACT.md description: `Lease` gained
//! `protect`/`unprotect` beyond bare heartbeating. CONTRACT.md's GC rule
//! ("roots = ... entries referenced by leases <30min old", PPG3_DESIGN.md
//! §11) requires a lease to be *about* a concrete set of output hashes;
//! nothing else in the contract offers a place to record that set, so the
//! lease file's `protects` field carries it and `heartbeat()` preserves it
//! (read-modify-write of `ts_ms` only). See STATUS.md.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::error::Error;

/// Leases older than this (no heartbeat refresh) are ignored by GC.
pub const LEASE_STALE_AFTER: Duration = Duration::from_secs(30 * 60);
/// Intents older than this are ignored (advisory only).
pub const INTENT_STALE_AFTER: Duration = Duration::from_secs(30 * 60);

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("HOST"))
        .unwrap_or_else(|_| "unknown-host".to_string())
}

// ---------------------------------------------------------------- Lease ---

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LeaseFile {
    run_id: String,
    ts_ms: i64,
    #[serde(default)]
    protects: Vec<String>,
}

/// A live run's GC-protection handle. Remove the value (or let it drop) to
/// release the lease.
pub struct Lease {
    path: PathBuf,
    run_id: String,
}

impl Lease {
    /// Create (or take over) `leases/<run_id>.json`.
    pub fn create(leases_dir: &Path, run_id: &str) -> Result<Lease, Error> {
        std::fs::create_dir_all(leases_dir).map_err(|e| Error::io(leases_dir, e))?;
        let path = leases_dir.join(format!("{run_id}.json"));
        let f = LeaseFile {
            run_id: run_id.to_string(),
            ts_ms: now_ms(),
            protects: Vec::new(),
        };
        write_json(&path, &f)?;
        Ok(Lease {
            path,
            run_id: run_id.to_string(),
        })
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Bump the heartbeat timestamp (and mtime), keeping the protected set
    /// unchanged. Call periodically from a live run (design: every 5 min).
    pub fn heartbeat(&self) -> Result<(), Error> {
        let mut f = self.read()?;
        f.ts_ms = now_ms();
        write_json(&self.path, &f)
    }

    /// Add `oh` to the set of entries this lease protects from GC.
    pub fn protect(&self, oh: &str) -> Result<(), Error> {
        let mut f = self.read()?;
        if !f.protects.iter().any(|x| x == oh) {
            f.protects.push(oh.to_string());
        }
        f.ts_ms = now_ms();
        write_json(&self.path, &f)
    }

    /// Remove `oh` from the protected set (e.g. once it is durably rooted).
    pub fn unprotect(&self, oh: &str) -> Result<(), Error> {
        let mut f = self.read()?;
        f.protects.retain(|x| x != oh);
        f.ts_ms = now_ms();
        write_json(&self.path, &f)
    }

    fn read(&self) -> Result<LeaseFile, Error> {
        read_json(&self.path)
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Union of output hashes protected by all non-stale leases in `leases_dir`.
/// Missing/unreadable directory is treated as "no leases" (empty set).
pub fn live_protected_ohs(leases_dir: &Path) -> Result<HashSet<String>, Error> {
    let mut out = HashSet::new();
    let entries = match std::fs::read_dir(leases_dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(Error::io(leases_dir, e)),
    };
    for entry in entries {
        let entry = entry.map_err(|e| Error::io(leases_dir, e))?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        if is_stale(&path, LEASE_STALE_AFTER)? {
            continue;
        }
        let f: LeaseFile = match read_json(&path) {
            Ok(f) => f,
            Err(_) => continue, // corrupt/partial lease file: ignore, not a root
        };
        out.extend(f.protects);
    }
    Ok(out)
}

// --------------------------------------------------------------- Intent ---

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IntentFile {
    ik: String,
    host: String,
    pid: u32,
    ts_ms: i64,
}

/// Advisory "I am building this ik" marker. Never load-bearing for
/// correctness (§11.1); a stale (>30min) intent is simply ignored.
pub struct Intent {
    path: PathBuf,
    ik: String,
}

impl Intent {
    pub fn create(intents_dir: &Path, ik: &str) -> Result<Intent, Error> {
        std::fs::create_dir_all(intents_dir).map_err(|e| Error::io(intents_dir, e))?;
        let path = intents_dir.join(format!("{ik}.json"));
        let f = IntentFile {
            ik: ik.to_string(),
            host: hostname(),
            pid: std::process::id(),
            ts_ms: now_ms(),
        };
        write_json(&path, &f)?;
        Ok(Intent {
            path,
            ik: ik.to_string(),
        })
    }

    pub fn ik(&self) -> &str {
        &self.ik
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn heartbeat(&self) -> Result<(), Error> {
        let mut f: IntentFile = read_json(&self.path)?;
        f.ts_ms = now_ms();
        write_json(&self.path, &f)
    }

    /// Explicitly release (remove) the intent file. Not automatic on drop —
    /// intents are meant to expire by staleness, matching CONTRACT.md.
    pub fn release(self) -> Result<(), Error> {
        match std::fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::io(&self.path, e)),
        }
    }
}

/// True if a fresh (non-stale) foreign intent exists for `ik`.
pub fn has_fresh_intent(intents_dir: &Path, ik: &str) -> Result<bool, Error> {
    let path = intents_dir.join(format!("{ik}.json"));
    if !path.exists() {
        return Ok(false);
    }
    Ok(!is_stale(&path, INTENT_STALE_AFTER)?)
}

// --------------------------------------------------------------- shared ---

fn is_stale(path: &Path, stale_after: Duration) -> Result<bool, Error> {
    let meta = std::fs::metadata(path).map_err(|e| Error::io(path, e))?;
    let mtime = meta.modified().map_err(|e| Error::io(path, e))?;
    let age = SystemTime::now().duration_since(mtime).unwrap_or_default();
    Ok(age > stale_after)
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<(), Error> {
    let bytes = serde_json::to_vec_pretty(value)
        .map_err(|e| Error::Canon(format!("serializing {path:?}: {e}")))?;
    // Write via a temp file + rename so a concurrent reader never observes
    // a half-written lease/intent file.
    let tmp = path.with_extension(format!("tmp-{}", std::process::id()));
    std::fs::write(&tmp, &bytes).map_err(|e| Error::io(&tmp, e))?;
    std::fs::rename(&tmp, path).map_err(|e| Error::io(path, e))?;
    Ok(())
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, Error> {
    let bytes = std::fs::read(path).map_err(|e| Error::io(path, e))?;
    serde_json::from_slice(&bytes).map_err(|e| Error::Canon(format!("parsing {path:?}: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lease_create_and_drop_removes_file() {
        let dir = tempfile::tempdir().unwrap();
        let path;
        {
            let lease = Lease::create(dir.path(), "run1").unwrap();
            path = lease.path().to_path_buf();
            assert!(path.exists());
        }
        assert!(!path.exists());
    }

    #[test]
    fn lease_heartbeat_updates_ts_without_losing_protects() {
        let dir = tempfile::tempdir().unwrap();
        let lease = Lease::create(dir.path(), "run1").unwrap();
        lease.protect("oh1").unwrap();
        let before: LeaseFile = read_json(lease.path()).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        lease.heartbeat().unwrap();
        let after: LeaseFile = read_json(lease.path()).unwrap();
        assert_eq!(after.protects, vec!["oh1".to_string()]);
        assert!(after.ts_ms >= before.ts_ms);
    }

    #[test]
    fn lease_protect_unprotect() {
        let dir = tempfile::tempdir().unwrap();
        let lease = Lease::create(dir.path(), "run1").unwrap();
        lease.protect("oh1").unwrap();
        lease.protect("oh2").unwrap();
        lease.protect("oh1").unwrap(); // idempotent
        let f: LeaseFile = read_json(lease.path()).unwrap();
        assert_eq!(f.protects.len(), 2);
        lease.unprotect("oh1").unwrap();
        let f: LeaseFile = read_json(lease.path()).unwrap();
        assert_eq!(f.protects, vec!["oh2".to_string()]);
    }

    #[test]
    fn live_protected_ohs_collects_fresh_leases_only() {
        let dir = tempfile::tempdir().unwrap();
        let lease = Lease::create(dir.path(), "run1").unwrap();
        lease.protect("oh1").unwrap();

        // A stale lease file (old mtime) must be ignored.
        let stale_path = dir.path().join("stale.json");
        let f = LeaseFile {
            run_id: "stale".to_string(),
            ts_ms: 0,
            protects: vec!["oh2".to_string()],
        };
        write_json(&stale_path, &f).unwrap();
        let old = std::time::SystemTime::now() - Duration::from_secs(3600);
        set_mtime(&stale_path, old);

        let protected = live_protected_ohs(dir.path()).unwrap();
        assert!(protected.contains("oh1"));
        assert!(!protected.contains("oh2"));
    }

    #[test]
    fn live_protected_ohs_missing_dir_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("does-not-exist");
        let protected = live_protected_ohs(&missing).unwrap();
        assert!(protected.is_empty());
    }

    #[test]
    fn intent_create_heartbeat_release() {
        let dir = tempfile::tempdir().unwrap();
        let intent = Intent::create(dir.path(), "ik1").unwrap();
        assert!(intent.path().exists());
        intent.heartbeat().unwrap();
        assert!(!has_fresh_intent(dir.path(), "ik-other").unwrap());
        assert!(has_fresh_intent(dir.path(), "ik1").unwrap());
        intent.release().unwrap();
        assert!(!has_fresh_intent(dir.path(), "ik1").unwrap());
    }

    #[test]
    fn intent_stale_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let intent = Intent::create(dir.path(), "ik1").unwrap();
        let old = std::time::SystemTime::now() - Duration::from_secs(3600);
        set_mtime(intent.path(), old);
        assert!(!has_fresh_intent(dir.path(), "ik1").unwrap());
    }

    fn set_mtime(path: &Path, time: SystemTime) {
        let ft = filetime_from(time);
        filetime_set(path, ft);
    }

    // Minimal mtime setter without pulling in the `filetime` crate: uses
    // libc utimensat via std's unstable-free path is not available, so we
    // shell out to `touch -d` semantics via `utime`-free approach: we
    // reopen+rewrite the file which updates mtime to "now", then rely on
    // std::fs::File::set_times if available (Rust 1.75+).
    fn filetime_from(time: SystemTime) -> SystemTime {
        time
    }

    fn filetime_set(path: &Path, time: SystemTime) {
        let file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        let ft = std::fs::FileTimes::new().set_modified(time);
        file.set_times(ft).unwrap();
    }
}
