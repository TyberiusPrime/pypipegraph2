//! Store (WP1, PPG3_DESIGN.md §4/§9/§11, CONTRACT.md "Store").
//!
//! Layout under `<root>/v1/`: `inputs/<ik>` symlinks into `entries/<oh>/`;
//! `entries/<oh>/{data/,manifest.json}` is immutable after publish;
//! `staging/<host>-<pid>-<rand>/` holds in-progress builds; `leases/`,
//! `intents/`, `roots/`, `pins/` are the GC-protection mechanisms (§11);
//! `gc.lock` is the fcntl lock (shared for publish, exclusive for GC).
//!
//! ## Deviations from the literal one-paragraph CONTRACT.md description
//! (each is a considered interpretation of an underspecified corner; see
//! STATUS.md for the short version):
//!
//! 1. **`chmod -R a-w` scope.** Applied to `entries/<oh>/data/**` and
//!    `entries/<oh>/manifest.json`, *not* to the `entries/<oh>` directory
//!    entry itself. §11 requires touching `entries/<oh>/.atime` on every
//!    lookup hit and writing `entries/<oh>/.ppg3-evict-ok` at publish time;
//!    both are impossible after a run-of-the-mill recursive `chmod a-w`
//!    that includes the directory's own write bit. The directory keeps its
//!    own write bit so these sentinel files can be created; its payload
//!    becomes fully read-only.
//! 2. **`inputs/<ik>` creation.** Uses a direct `symlink()` syscall to the
//!    final name (POSIX-atomic; fails `EEXIST` if the name is taken)
//!    instead of "symlink to tmp name + rename". A tmp-name+rename would
//!    *unconditionally replace* an existing destination symlink on Linux,
//!    which is exactly the clobber a concurrent conflicting publisher must
//!    never be allowed to do silently; direct `symlink()` gives the same
//!    atomicity with an exclusivity check for free. `pin()`, where
//!    *replacing* an existing name is the desired behavior, does use
//!    tmp-name+rename.
//! 3. **"quarantine staging under `staging/violations/`" for a determinism
//!    violation.** By the time the violation is detected (step 6, after the
//!    `entries/<oh>` resolution in step 5), the staging directory has
//!    already been consumed — either renamed into a brand-new
//!    `entries/<oh>` or discarded because it deduped against a pre-existing
//!    entry. Deleting an already-published, potentially-shared
//!    `entries/<oh>` on a downstream mapping conflict would violate store
//!    immutability. So quarantine writes a forensic record
//!    (`staging/violations/<ik>-<ts>/report.txt` + `attempted_oh.txt`)
//!    rather than moving live entry content; the entry itself is left
//!    exactly where content-addressing put it.

use std::io::Read;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use serde_json::Value;

use crate::error::Error;
use crate::gc::{self, GcPolicy, GcReport};
use crate::lease::{Intent, Lease};
use crate::manifest::{self, BuiltInfo, ContentMap, Manifest};

const MANIFEST_FILE: &str = "manifest.json";
const DATA_DIR: &str = "data";
const ATIME_FILE: &str = ".atime";
const EVICT_OK_FILE: &str = ".ppg3-evict-ok";

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

static SUFFIX_COUNTER: AtomicU64 = AtomicU64::new(0);

fn random_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let c = SUFFIX_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{nanos:x}{c:x}")
}

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("HOST"))
        .unwrap_or_else(|_| "host".to_string())
}

fn read_manifest(path: &Path) -> Result<Manifest, Error> {
    let bytes = std::fs::read(path).map_err(|e| Error::io(path, e))?;
    serde_json::from_slice(&bytes)
        .map_err(|e| Error::CorruptStore(format!("unreadable manifest {path:?}: {e}")))
}

fn write_manifest(path: &Path, m: &Manifest) -> Result<(), Error> {
    let bytes = serde_json::to_vec_pretty(m)
        .map_err(|e| Error::Canon(format!("serializing manifest: {e}")))?;
    std::fs::write(path, bytes).map_err(|e| Error::io(path, e))
}

fn oh_from_symlink_target(target: &Path) -> Result<String, Error> {
    target
        .file_name()
        .and_then(|s| s.to_str())
        .map(|s| s.to_string())
        .ok_or_else(|| {
            Error::CorruptStore(format!("malformed symlink target: {}", target.display()))
        })
}

/// Recursively clear (all classes') write bits, or restore owner-write,
/// under `path` (which may itself be a file). Post-order (children before
/// parent) so directory write bits don't get cleared before we're done
/// writing into them.
fn chmod_recursive(path: &Path, writable: bool) -> Result<(), Error> {
    let meta = std::fs::symlink_metadata(path).map_err(|e| Error::io(path, e))?;
    if meta.file_type().is_symlink() {
        return Ok(());
    }
    if meta.is_dir() {
        let entries = std::fs::read_dir(path).map_err(|e| Error::io(path, e))?;
        for entry in entries {
            let entry = entry.map_err(|e| Error::io(path, e))?;
            chmod_recursive(&entry.path(), writable)?;
        }
    }
    let mut perm = meta.permissions();
    let mode = perm.mode();
    let new_mode = if writable {
        mode | 0o200
    } else {
        mode & !0o222
    };
    perm.set_mode(new_mode);
    std::fs::set_permissions(path, perm).map_err(|e| Error::io(path, e))
}

/// Walk `data_dir` and hash every regular file. Rejects symlinks (publish
/// error, §10.1: "Symlinks inside outputs are forbidden").
pub fn hash_staging_content(data_dir: &Path) -> Result<ContentMap, Error> {
    let mut out = ContentMap::new();
    if !data_dir.exists() {
        return Ok(out);
    }
    walk_and_hash(data_dir, Path::new(""), &mut out)?;
    Ok(out)
}

fn walk_and_hash(data_dir: &Path, rel: &Path, out: &mut ContentMap) -> Result<(), Error> {
    let dir = data_dir.join(rel);
    let entries = std::fs::read_dir(&dir).map_err(|e| Error::io(&dir, e))?;
    for entry in entries {
        let entry = entry.map_err(|e| Error::io(&dir, e))?;
        let path = entry.path();
        let rel_path = rel.join(entry.file_name());
        let meta = std::fs::symlink_metadata(&path).map_err(|e| Error::io(&path, e))?;
        let ft = meta.file_type();
        if ft.is_symlink() {
            return Err(Error::Other(format!(
                "symlink inside staged output not allowed: {}",
                rel_path.display()
            )));
        } else if ft.is_dir() {
            walk_and_hash(data_dir, &rel_path, out)?;
        } else if ft.is_file() {
            let hash = crate::hash::blake3_file(&path)?;
            // Publish always strips write bits (§4 "chmod -R a-w") from
            // data files, so the manifest records the mode the file will
            // actually have once published, not its transient
            // staging-time (umask-dependent) mode - otherwise `verify_entry`
            // would report a spurious mismatch on every published entry,
            // and identical outputs staged under different umasks would
            // spuriously fail to dedup.
            let mode_bits = meta.permissions().mode() & 0o777 & !0o222;
            let rel_str = rel_path
                .to_str()
                .ok_or_else(|| Error::Other(format!("non-utf8 path: {}", rel_path.display())))?
                .to_string();
            out.insert(
                rel_str,
                manifest::ContentEntry {
                    blake3: hash,
                    mode: format!("{mode_bits:04o}"),
                    size: meta.len(),
                },
            );
        } else {
            return Err(Error::Other(format!(
                "unsupported file type at {}",
                rel_path.display()
            )));
        }
    }
    Ok(())
}

/// Recursive sum of regular-file sizes under `path` (directories/symlinks
/// don't count). Missing path = 0.
pub(crate) fn dir_size(path: &Path) -> u64 {
    let mut total = 0u64;
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    for entry in entries.flatten() {
        let p = entry.path();
        if let Ok(meta) = std::fs::symlink_metadata(&p) {
            if meta.file_type().is_symlink() {
                continue;
            } else if meta.is_dir() {
                total += dir_size(&p);
            } else {
                total += meta.len();
            }
        }
    }
    total
}

// --------------------------------------------------------------- Store ---

pub struct Store {
    name: String,
    root: PathBuf,
    readonly: bool,
}

/// A shared or exclusive lock on `<root>/v1/gc.lock`. Shared for publish,
/// exclusive for GC (PPG3_DESIGN.md §11). Released on drop (fs2's flock
/// releases when the underlying fd closes; we also call `unlock()`
/// explicitly for clarity).
pub struct GcLockGuard {
    file: std::fs::File,
}

impl GcLockGuard {
    pub(crate) fn lock_shared(path: &Path) -> Result<Self, Error> {
        let file = Self::open(path)?;
        fs2::FileExt::lock_shared(&file).map_err(|e| Error::io(path, e))?;
        Ok(GcLockGuard { file })
    }

    pub(crate) fn lock_exclusive(path: &Path) -> Result<Self, Error> {
        let file = Self::open(path)?;
        fs2::FileExt::lock_exclusive(&file).map_err(|e| Error::io(path, e))?;
        Ok(GcLockGuard { file })
    }

    fn open(path: &Path) -> Result<std::fs::File, Error> {
        std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(path)
            .map_err(|e| Error::io(path, e))
    }
}

impl Drop for GcLockGuard {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.file);
    }
}

/// A build-in-progress. `path()` is the (already-created) directory that
/// becomes the entry's `data/` on publish.
pub struct Staging {
    root: PathBuf,
    data_dir: PathBuf,
}

impl Staging {
    pub fn path(&self) -> &Path {
        &self.data_dir
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn manifest_path(&self) -> PathBuf {
        self.root.join(MANIFEST_FILE)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum PublishOutcome {
    Published { oh: String },
    DedupHit { oh: String },
}

impl PublishOutcome {
    pub fn oh(&self) -> &str {
        match self {
            PublishOutcome::Published { oh } | PublishOutcome::DedupHit { oh } => oh,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct VerifyReport {
    pub oh: String,
    pub ok: bool,
    pub mismatches: Vec<String>,
}

impl Store {
    /// Open (and, if writable, create) a store at `root`. `root` is the
    /// store directory; the versioned layout lives under `root/v1/`.
    pub fn open(name: &str, root: &Path, readonly: bool) -> Result<Store, Error> {
        let store = Store {
            name: name.to_string(),
            root: root.to_path_buf(),
            readonly,
        };
        if readonly {
            if !store.v1_dir().is_dir() {
                return Err(Error::Other(format!(
                    "store {name:?} at {root:?} does not exist (readonly open)"
                )));
            }
        } else {
            for dir in [
                store.v1_dir(),
                store.inputs_dir(),
                store.entries_dir(),
                store.staging_dir(),
                store.logs_dir(),
                store.leases_dir(),
                store.roots_dir(),
                store.pins_dir(),
                store.intents_dir(),
            ] {
                std::fs::create_dir_all(&dir).map_err(|e| Error::io(&dir, e))?;
            }
            let lock_path = store.gc_lock_path();
            if !lock_path.exists() {
                std::fs::File::create(&lock_path).map_err(|e| Error::io(&lock_path, e))?;
            }
        }
        Ok(store)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn is_readonly(&self) -> bool {
        self.readonly
    }

    fn ensure_writable(&self) -> Result<(), Error> {
        if self.readonly {
            Err(Error::ReadOnlyStore(self.name.clone()))
        } else {
            Ok(())
        }
    }

    fn v1_dir(&self) -> PathBuf {
        self.root.join("v1")
    }

    pub(crate) fn inputs_dir(&self) -> PathBuf {
        self.v1_dir().join("inputs")
    }

    pub(crate) fn entries_dir(&self) -> PathBuf {
        self.v1_dir().join("entries")
    }

    pub(crate) fn staging_dir(&self) -> PathBuf {
        self.v1_dir().join("staging")
    }

    pub(crate) fn logs_dir(&self) -> PathBuf {
        self.v1_dir().join("logs")
    }

    pub(crate) fn leases_dir(&self) -> PathBuf {
        self.v1_dir().join("leases")
    }

    pub(crate) fn roots_dir(&self) -> PathBuf {
        self.v1_dir().join("roots")
    }

    pub(crate) fn pins_dir(&self) -> PathBuf {
        self.v1_dir().join("pins")
    }

    pub(crate) fn intents_dir(&self) -> PathBuf {
        self.v1_dir().join("intents")
    }

    pub(crate) fn gc_lock_path(&self) -> PathBuf {
        self.v1_dir().join("gc.lock")
    }

    pub fn entry_dir(&self, oh: &str) -> PathBuf {
        self.entries_dir().join(oh)
    }

    pub fn data_dir(&self, oh: &str) -> PathBuf {
        self.entry_dir(oh).join(DATA_DIR)
    }

    /// Follows `inputs/<ik>`, loads the entry's manifest, touches
    /// `entries/<oh>/.atime` (best effort — ignored if the store/entry
    /// directory is not writable).
    pub fn lookup(&self, ik: &str) -> Result<Option<Manifest>, Error> {
        let link_path = self.inputs_dir().join(ik);
        let target = match std::fs::read_link(&link_path) {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(Error::io(&link_path, e)),
        };
        let oh = oh_from_symlink_target(&target)?;
        let entry_dir = self.entry_dir(&oh);
        let manifest_path = entry_dir.join(MANIFEST_FILE);
        if !manifest_path.is_file() {
            return Err(Error::CorruptStore(format!(
                "inputs/{ik} points at missing entries/{oh} (dangling symlink / evicted entry)"
            )));
        }
        let manifest = read_manifest(&manifest_path)?;
        let atime_path = entry_dir.join(ATIME_FILE);
        let _ = std::fs::write(&atime_path, now_ms().to_string());
        Ok(Some(manifest))
    }

    /// Create a fresh staging directory: `v1/staging/<host>-<pid>-<rand>/`
    /// with its `data/` subdirectory already created.
    pub fn open_staging(&self) -> Result<Staging, Error> {
        self.ensure_writable()?;
        let base = self.staging_dir();
        std::fs::create_dir_all(&base).map_err(|e| Error::io(&base, e))?;
        for _ in 0..20 {
            let name = format!("{}-{}-{}", hostname(), std::process::id(), random_suffix());
            let root = base.join(name);
            match std::fs::create_dir(&root) {
                Ok(()) => {
                    let data_dir = root.join(DATA_DIR);
                    std::fs::create_dir(&data_dir).map_err(|e| Error::io(&data_dir, e))?;
                    return Ok(Staging { root, data_dir });
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(Error::io(&root, e)),
            }
        }
        Err(Error::Other(
            "could not allocate a unique staging directory".to_string(),
        ))
    }

    // ---- publish, exposed as individually callable, #[doc(hidden)] steps
    // ---- for crash-injection testing (CONTRACT.md: "expose step functions
    // ---- or a publish_steps test API").

    /// Step 1: hash every file under `staging.path()`.
    #[doc(hidden)]
    pub fn publish_step1_hash(&self, staging: &Staging) -> Result<ContentMap, Error> {
        hash_staging_content(staging.path())
    }

    /// Step 2: compute `oh` and assemble the in-memory manifest.
    #[doc(hidden)]
    pub fn publish_step2_build_manifest(
        &self,
        ik: &str,
        content: ContentMap,
        key_document: &Value,
        built: BuiltInfo,
        job_view_name: Option<&str>,
    ) -> Result<(Manifest, String), Error> {
        let oh = manifest::output_hash(&content)?;
        let m = Manifest {
            ppg3_manifest_version: crate::MANIFEST_VERSION,
            input_key: ik.to_string(),
            key_document: key_document.clone(),
            content,
            output_hash: oh.clone(),
            built,
            job_view_name: job_view_name.map(|s| s.to_string()),
        };
        Ok((m, oh))
    }

    /// Step 3: write `manifest.json` into the staging directory (sibling of
    /// `data/`).
    #[doc(hidden)]
    pub fn publish_step3_write_staging_manifest(
        &self,
        staging: &Staging,
        manifest: &Manifest,
    ) -> Result<(), Error> {
        write_manifest(&staging.manifest_path(), manifest)
    }

    /// Step 4: take `gc.lock` in shared mode; held until publish finishes.
    #[doc(hidden)]
    pub fn publish_step4_lock_shared(&self) -> Result<GcLockGuard, Error> {
        GcLockGuard::lock_shared(&self.gc_lock_path())
    }

    /// Step 5: resolve `entries/<oh>` — dedup against an existing entry
    /// (byte-identical content required, else `Error::CorruptStore`) or
    /// promote the staging directory into place and `chmod -R a-w` its
    /// payload. Consumes `staging`.
    #[doc(hidden)]
    pub fn publish_step5_resolve_entry(
        &self,
        staging: Staging,
        oh: &str,
    ) -> Result<PublishOutcome, Error> {
        let entry_dir = self.entry_dir(oh);
        let mut published_fresh = false;

        if !entry_dir.exists() {
            let parent = self.entries_dir();
            std::fs::create_dir_all(&parent).map_err(|e| Error::io(&parent, e))?;
            match std::fs::rename(staging.root(), &entry_dir) {
                Ok(()) => {
                    published_fresh = true;
                }
                Err(e)
                    if e.kind() == std::io::ErrorKind::AlreadyExists
                        || e.raw_os_error() == Some(libc::ENOTEMPTY) =>
                {
                    // Lost a race to a concurrent publisher of the same oh.
                    // Fall through to the dedup-compare path below.
                }
                Err(e) => return Err(Error::io(&entry_dir, e)),
            }
        }

        if published_fresh {
            let evict_ok = {
                let m = read_manifest(&entry_dir.join(MANIFEST_FILE))?;
                m.built.retain_evict
            };
            if evict_ok {
                let marker = entry_dir.join(EVICT_OK_FILE);
                std::fs::write(&marker, b"").map_err(|e| Error::io(&marker, e))?;
            }
            chmod_recursive(&entry_dir.join(DATA_DIR), false)?;
            chmod_recursive(&entry_dir.join(MANIFEST_FILE), false)?;
            return Ok(PublishOutcome::Published { oh: oh.to_string() });
        }

        // entries/<oh> already existed (either before we started, or a
        // concurrent publisher just won the race) - dedup-verify.
        let existing = read_manifest(&entry_dir.join(MANIFEST_FILE))?;
        let existing_bytes = manifest::canonical_content_bytes(&existing.content)?;
        let staged = read_manifest(&staging.manifest_path())?;
        let staged_bytes = manifest::canonical_content_bytes(&staged.content)?;
        let identical = existing_bytes == staged_bytes;

        // The staging copy is redundant either way now.
        std::fs::remove_dir_all(staging.root()).map_err(|e| Error::io(staging.root(), e))?;

        if !identical {
            return Err(Error::CorruptStore(format!(
                "entries/{oh} exists with content differing from a freshly built staging \
                 directory sharing the same output hash (store corruption or blake3 collision)"
            )));
        }
        // Idempotent safety net: a crash between rename and chmod in a
        // previous attempt could have left the entry only partially
        // read-only; make sure it is fully locked down before we return.
        chmod_recursive(&entry_dir.join(DATA_DIR), false)?;
        chmod_recursive(&entry_dir.join(MANIFEST_FILE), false)?;
        Ok(PublishOutcome::DedupHit { oh: oh.to_string() })
    }

    /// Step 6: resolve `inputs/<ik>` — create it if absent, no-op if it
    /// already points at `oh`, else `Error::DeterminismViolation`.
    #[doc(hidden)]
    pub fn publish_step6_resolve_input_symlink(
        &self,
        ik: &str,
        oh: &str,
        tentative: PublishOutcome,
    ) -> Result<PublishOutcome, Error> {
        let inputs_dir = self.inputs_dir();
        std::fs::create_dir_all(&inputs_dir).map_err(|e| Error::io(&inputs_dir, e))?;
        let link_path = inputs_dir.join(ik);
        let target_rel = format!("../entries/{oh}");
        match std::os::unix::fs::symlink(&target_rel, &link_path) {
            Ok(()) => Ok(tentative),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                let existing_target =
                    std::fs::read_link(&link_path).map_err(|e2| Error::io(&link_path, e2))?;
                let existing_oh = oh_from_symlink_target(&existing_target)?;
                if existing_oh == oh {
                    return Ok(tentative);
                }
                let report = self.diff_report(&existing_oh, oh)?;
                self.quarantine_violation_record(ik, oh, &report)?;
                Err(Error::DeterminismViolation {
                    ik: ik.to_string(),
                    report,
                })
            }
            Err(e) => Err(Error::io(&link_path, e)),
        }
    }

    /// Full publish protocol (PPG3_DESIGN.md §4): steps 1-6 above, in order.
    pub fn publish(
        &self,
        staging: Staging,
        ik: &str,
        key_document: &Value,
        built: BuiltInfo,
        job_view_name: Option<&str>,
    ) -> Result<PublishOutcome, Error> {
        self.ensure_writable()?;
        let content = self.publish_step1_hash(&staging)?;
        let (manifest, oh) =
            self.publish_step2_build_manifest(ik, content, key_document, built, job_view_name)?;
        self.publish_step3_write_staging_manifest(&staging, &manifest)?;
        let _lock = self.publish_step4_lock_shared()?;
        let tentative = self.publish_step5_resolve_entry(staging, &oh)?;
        self.publish_step6_resolve_input_symlink(ik, &oh, tentative)
    }

    fn diff_report(&self, old_oh: &str, new_oh: &str) -> Result<String, Error> {
        let old_m = read_manifest(&self.entry_dir(old_oh).join(MANIFEST_FILE))?;
        let new_m = read_manifest(&self.entry_dir(new_oh).join(MANIFEST_FILE))?;

        let mut added = Vec::new();
        let mut removed = Vec::new();
        let mut changed = Vec::new();

        for (path, new_entry) in &new_m.content {
            match old_m.content.get(path) {
                None => added.push(path.clone()),
                Some(old_entry) if old_entry != new_entry => {
                    let offset = if old_entry.blake3 != new_entry.blake3 {
                        first_diff_offset(
                            &self.data_dir(old_oh).join(path),
                            &self.data_dir(new_oh).join(path),
                        )
                        .ok()
                        .flatten()
                    } else {
                        None
                    };
                    let mut line = format!(
                        "{path}: size {}->{}, blake3 {}->{}, mode {}->{}",
                        old_entry.size,
                        new_entry.size,
                        old_entry.blake3,
                        new_entry.blake3,
                        old_entry.mode,
                        new_entry.mode
                    );
                    if let Some(off) = offset {
                        line += &format!(", first differing byte offset {off}");
                    }
                    changed.push(line);
                }
                Some(_) => {}
            }
        }
        for path in old_m.content.keys() {
            if !new_m.content.contains_key(path) {
                removed.push(path.clone());
            }
        }
        added.sort();
        removed.sort();
        changed.sort();

        let mut out = format!(
            "determinism violation: input key {ik} previously produced output hash {old_oh}, \
             this build produced {new_oh}\n",
            ik = new_m.input_key,
        );
        out += &format!("added files ({}): {added:?}\n", added.len());
        out += &format!("removed files ({}): {removed:?}\n", removed.len());
        out += &format!("changed files ({}):\n", changed.len());
        for c in &changed {
            out += &format!("  {c}\n");
        }
        Ok(out)
    }

    fn quarantine_violation_record(
        &self,
        ik: &str,
        attempted_oh: &str,
        report: &str,
    ) -> Result<(), Error> {
        let violations_dir = self.staging_dir().join("violations");
        std::fs::create_dir_all(&violations_dir).map_err(|e| Error::io(&violations_dir, e))?;
        let dir = violations_dir.join(format!("{ik}-{}", now_ms()));
        std::fs::create_dir_all(&dir).map_err(|e| Error::io(&dir, e))?;
        std::fs::write(dir.join("report.txt"), report).map_err(|e| Error::io(&dir, e))?;
        std::fs::write(dir.join("attempted_oh.txt"), attempted_oh)
            .map_err(|e| Error::io(&dir, e))?;
        Ok(())
    }

    /// Rehash `entries/<oh>/data` and compare against the stored manifest.
    pub fn verify_entry(&self, oh: &str) -> Result<VerifyReport, Error> {
        let entry_dir = self.entry_dir(oh);
        if !entry_dir.is_dir() {
            return Err(Error::Other(format!("no such entry: {oh}")));
        }
        let manifest = read_manifest(&entry_dir.join(MANIFEST_FILE))?;
        let actual = hash_staging_content(&self.data_dir(oh))?;
        let mut mismatches = Vec::new();
        for (path, expected) in &manifest.content {
            match actual.get(path) {
                None => mismatches.push(format!("missing file: {path}")),
                Some(a) if a != expected => mismatches.push(format!(
                    "changed file: {path} (manifest blake3={} size={} mode={}, actual blake3={} size={} mode={})",
                    expected.blake3, expected.size, expected.mode, a.blake3, a.size, a.mode
                )),
                Some(_) => {}
            }
        }
        for path in actual.keys() {
            if !manifest.content.contains_key(path) {
                mismatches.push(format!("unexpected file: {path}"));
            }
        }
        if manifest.output_hash != oh {
            mismatches.push(format!(
                "manifest output_hash {} does not match entry directory name {oh}",
                manifest.output_hash
            ));
        }
        Ok(VerifyReport {
            oh: oh.to_string(),
            ok: mismatches.is_empty(),
            mismatches,
        })
    }

    pub fn lease(&self, run_id: &str) -> Result<Lease, Error> {
        self.ensure_writable()?;
        Lease::create(&self.leases_dir(), run_id)
    }

    pub fn write_intent(&self, ik: &str) -> Result<Intent, Error> {
        self.ensure_writable()?;
        Intent::create(&self.intents_dir(), ik)
    }

    /// Register roots at `v1/roots/<project_id>/<generation>/<oh>` for
    /// every oh in `ohs` (one symlink per oh; re-registering is a no-op).
    pub fn add_root(&self, project_id: &str, generation: u64, ohs: &[String]) -> Result<(), Error> {
        self.ensure_writable()?;
        let gen_dir = self
            .roots_dir()
            .join(project_id)
            .join(generation.to_string());
        std::fs::create_dir_all(&gen_dir).map_err(|e| Error::io(&gen_dir, e))?;
        for oh in ohs {
            let link_path = gen_dir.join(oh);
            if link_path.exists() {
                continue;
            }
            let target = format!("../../../entries/{oh}");
            match std::os::unix::fs::symlink(&target, &link_path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(e) => return Err(Error::io(&link_path, e)),
            }
        }
        Ok(())
    }

    pub fn remove_root(&self, project_id: &str, generation: u64) -> Result<(), Error> {
        self.ensure_writable()?;
        let gen_dir = self
            .roots_dir()
            .join(project_id)
            .join(generation.to_string());
        match std::fs::remove_dir_all(&gen_dir) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::io(&gen_dir, e)),
        }
    }

    /// Create or replace the named pin `v1/pins/<name>` -> `entries/<oh>`.
    pub fn pin(&self, name: &str, oh: &str) -> Result<(), Error> {
        self.ensure_writable()?;
        let pins_dir = self.pins_dir();
        std::fs::create_dir_all(&pins_dir).map_err(|e| Error::io(&pins_dir, e))?;
        let link_path = pins_dir.join(name);
        let target = format!("../entries/{oh}");
        let tmp = pins_dir.join(format!(".tmp-{name}-{}", random_suffix()));
        std::os::unix::fs::symlink(&target, &tmp).map_err(|e| Error::io(&tmp, e))?;
        std::fs::rename(&tmp, &link_path).map_err(|e| Error::io(&link_path, e))
    }

    pub fn log_dir_for(&self, ik: &str) -> Result<PathBuf, Error> {
        self.ensure_writable()?;
        let dir = self
            .logs_dir()
            .join(ik)
            .join(format!("{}-{}", now_ms(), hostname()));
        std::fs::create_dir_all(&dir).map_err(|e| Error::io(&dir, e))?;
        Ok(dir)
    }

    pub fn gc(&self, policy: &GcPolicy) -> Result<GcReport, Error> {
        self.ensure_writable()?;
        gc::run(self, policy)
    }
}

fn first_diff_offset(a: &Path, b: &Path) -> Result<Option<u64>, Error> {
    let mut fa = std::fs::File::open(a).map_err(|e| Error::io(a, e))?;
    let mut fb = std::fs::File::open(b).map_err(|e| Error::io(b, e))?;
    let mut ba = [0u8; 8192];
    let mut bb = [0u8; 8192];
    let mut offset: u64 = 0;
    loop {
        let na = fa.read(&mut ba).map_err(|e| Error::io(a, e))?;
        let nb = fb.read(&mut bb).map_err(|e| Error::io(b, e))?;
        let n = na.min(nb);
        for i in 0..n {
            if ba[i] != bb[i] {
                return Ok(Some(offset + i as u64));
            }
        }
        if na != nb {
            return Ok(Some(offset + n as u64));
        }
        if na == 0 {
            return Ok(None);
        }
        offset += n as u64;
    }
}

/// Recursively collect the `oh` referenced by every symlink under `dir`
/// (used for `roots/` — two levels deep — and `pins/` — one level deep).
pub(crate) fn collect_symlinked_ohs(
    dir: &Path,
) -> Result<std::collections::HashSet<String>, Error> {
    let mut out = std::collections::HashSet::new();
    collect_symlinked_ohs_into(dir, &mut out)?;
    Ok(out)
}

fn collect_symlinked_ohs_into(
    dir: &Path,
    out: &mut std::collections::HashSet<String>,
) -> Result<(), Error> {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(Error::io(dir, e)),
    };
    for entry in entries {
        let entry = entry.map_err(|e| Error::io(dir, e))?;
        let path = entry.path();
        let meta = std::fs::symlink_metadata(&path).map_err(|e| Error::io(&path, e))?;
        if meta.file_type().is_symlink() {
            let target = std::fs::read_link(&path).map_err(|e| Error::io(&path, e))?;
            out.insert(oh_from_symlink_target(&target)?);
        } else if meta.is_dir() {
            collect_symlinked_ohs_into(&path, out)?;
        }
    }
    Ok(())
}

pub(crate) fn entry_atime_ms(entry_dir: &Path) -> i64 {
    let atime_path = entry_dir.join(ATIME_FILE);
    if let Ok(s) = std::fs::read_to_string(&atime_path) {
        if let Ok(v) = s.trim().parse::<i64>() {
            return v;
        }
    }
    std::fs::metadata(entry_dir)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub(crate) fn entry_has_evict_marker(entry_dir: &Path) -> bool {
    entry_dir.join(EVICT_OK_FILE).is_file()
}

pub(crate) fn chmod_writable_recursive(path: &Path) -> Result<(), Error> {
    chmod_recursive(path, true)
}

pub(crate) fn list_dir_names(dir: &Path) -> Result<Vec<String>, Error> {
    let mut out = Vec::new();
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(Error::io(dir, e)),
    };
    for entry in entries {
        let entry = entry.map_err(|e| Error::io(dir, e))?;
        if let Some(n) = entry.file_name().to_str() {
            out.push(n.to_string());
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::BuiltInfo;

    fn sample_built() -> BuiltInfo {
        BuiltInfo {
            start_ms: 0,
            end_ms: 1,
            host: "test".to_string(),
            sandboxed: false,
            ppg3_version: "0.1.0".to_string(),
            retain_evict: false,
        }
    }

    fn write_file(path: &Path, content: &[u8]) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    }

    fn publish_simple(
        store: &Store,
        ik: &str,
        content: &[(&str, &[u8])],
    ) -> Result<PublishOutcome, Error> {
        let staging = store.open_staging().unwrap();
        for (name, bytes) in content {
            write_file(&staging.path().join(name), bytes);
        }
        store.publish(
            staging,
            ik,
            &serde_json::json!({"k": ik}),
            sample_built(),
            None,
        )
    }

    #[test]
    fn open_creates_layout() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open("local", dir.path(), false).unwrap();
        assert!(store.inputs_dir().is_dir());
        assert!(store.entries_dir().is_dir());
        assert!(store.gc_lock_path().is_file());
    }

    #[test]
    fn readonly_open_of_missing_store_errors() {
        let dir = tempfile::tempdir().unwrap();
        assert!(Store::open("ro", dir.path(), true).is_err());
    }

    #[test]
    fn lookup_miss_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open("s", dir.path(), false).unwrap();
        assert!(store.lookup(&"a".repeat(64)).unwrap().is_none());
    }

    #[test]
    fn publish_then_lookup_roundtrips() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open("s", dir.path(), false).unwrap();
        let ik = "1".repeat(64);
        let outcome = publish_simple(&store, &ik, &[("out.txt", b"hello")]).unwrap();
        assert!(matches!(outcome, PublishOutcome::Published { .. }));
        let m = store.lookup(&ik).unwrap().expect("should hit");
        assert_eq!(m.input_key, ik);
        assert!(store.entry_dir(&m.output_hash).join(ATIME_FILE).is_file());
    }

    #[test]
    fn publish_makes_data_read_only() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open("s", dir.path(), false).unwrap();
        let ik = "2".repeat(64);
        let outcome = publish_simple(&store, &ik, &[("out.txt", b"hello")]).unwrap();
        let data_file = store.data_dir(outcome.oh()).join("out.txt");
        let mode = std::fs::metadata(&data_file).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode & 0o222, 0, "file should have no write bits");
        // entry_dir itself must remain writable so .atime can be touched.
        let entry_mode = std::fs::metadata(store.entry_dir(outcome.oh()))
            .unwrap()
            .permissions()
            .mode();
        assert_ne!(entry_mode & 0o200, 0, "entry dir must keep owner-write");
    }

    #[test]
    fn dedup_same_content_two_iks_one_entry() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open("s", dir.path(), false).unwrap();
        let ik1 = "3".repeat(64);
        let ik2 = "4".repeat(64);
        let o1 = publish_simple(&store, &ik1, &[("out.txt", b"same")]).unwrap();
        let o2 = publish_simple(&store, &ik2, &[("out.txt", b"same")]).unwrap();
        assert!(matches!(o1, PublishOutcome::Published { .. }));
        assert!(matches!(o2, PublishOutcome::DedupHit { .. }));
        assert_eq!(o1.oh(), o2.oh());
        // one entry, two input symlinks
        assert_eq!(list_dir_names(&store.entries_dir()).unwrap().len(), 1);
        assert!(store.inputs_dir().join(&ik1).exists());
        assert!(store.inputs_dir().join(&ik2).exists());
    }

    #[test]
    fn republish_same_ik_same_content_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open("s", dir.path(), false).unwrap();
        let ik = "5".repeat(64);
        let o1 = publish_simple(&store, &ik, &[("out.txt", b"x")]).unwrap();
        let o2 = publish_simple(&store, &ik, &[("out.txt", b"x")]).unwrap();
        assert_eq!(o1.oh(), o2.oh());
        assert!(matches!(o2, PublishOutcome::DedupHit { .. }));
    }

    #[test]
    fn determinism_violation_on_conflicting_republish() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open("s", dir.path(), false).unwrap();
        let ik = "6".repeat(64);
        let o1 = publish_simple(&store, &ik, &[("out.txt", b"first")]).unwrap();
        let err = publish_simple(&store, &ik, &[("out.txt", b"second")]).unwrap_err();
        match err {
            Error::DeterminismViolation { ik: eik, report } => {
                assert_eq!(eik, ik);
                assert!(report.contains("out.txt"));
            }
            other => panic!("expected DeterminismViolation, got {other:?}"),
        }
        // original mapping must be untouched
        let m = store.lookup(&ik).unwrap().unwrap();
        assert_eq!(m.output_hash, *o1.oh());
        // a forensic record was left behind
        let violations = store.staging_dir().join("violations");
        assert!(violations.is_dir());
        assert!(list_dir_names(&violations)
            .unwrap()
            .iter()
            .any(|n| n.starts_with(&ik)));
    }

    #[test]
    fn symlink_in_staged_output_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open("s", dir.path(), false).unwrap();
        let staging = store.open_staging().unwrap();
        write_file(&staging.path().join("real.txt"), b"x");
        std::os::unix::fs::symlink(
            staging.path().join("real.txt"),
            staging.path().join("link.txt"),
        )
        .unwrap();
        let err = store
            .publish(
                staging,
                &"7".repeat(64),
                &serde_json::json!({}),
                sample_built(),
                None,
            )
            .unwrap_err();
        assert!(matches!(err, Error::Other(_)));
    }

    #[test]
    fn verify_entry_passes_after_publish() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open("s", dir.path(), false).unwrap();
        let outcome = publish_simple(
            &store,
            &"8".repeat(64),
            &[("a.txt", b"one"), ("b/c.txt", b"two")],
        )
        .unwrap();
        let report = store.verify_entry(outcome.oh()).unwrap();
        assert!(report.ok, "mismatches: {:?}", report.mismatches);
    }

    #[test]
    fn verify_entry_detects_tampering() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open("s", dir.path(), false).unwrap();
        let outcome = publish_simple(&store, &"9".repeat(64), &[("a.txt", b"one")]).unwrap();
        let data_file = store.data_dir(outcome.oh()).join("a.txt");
        chmod_recursive(&data_file, true).unwrap();
        std::fs::write(&data_file, b"tampered").unwrap();
        let report = store.verify_entry(outcome.oh()).unwrap();
        assert!(!report.ok);
    }

    #[test]
    fn pin_creates_and_replaces() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open("s", dir.path(), false).unwrap();
        let o1 = publish_simple(&store, &"a".repeat(64), &[("x", b"1")]).unwrap();
        let o2 = publish_simple(&store, &"b".repeat(64), &[("x", b"2")]).unwrap();
        store.pin("release", o1.oh()).unwrap();
        let target = std::fs::read_link(store.pins_dir().join("release")).unwrap();
        assert_eq!(oh_from_symlink_target(&target).unwrap(), o1.oh());
        store.pin("release", o2.oh()).unwrap();
        let target = std::fs::read_link(store.pins_dir().join("release")).unwrap();
        assert_eq!(oh_from_symlink_target(&target).unwrap(), o2.oh());
    }

    #[test]
    fn add_root_and_remove_root() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open("s", dir.path(), false).unwrap();
        let o1 = publish_simple(&store, &"c".repeat(64), &[("x", b"1")]).unwrap();
        store.add_root("proj", 1, &[o1.oh().to_string()]).unwrap();
        let ohs = collect_symlinked_ohs(&store.roots_dir()).unwrap();
        assert!(ohs.contains(o1.oh()));
        store.remove_root("proj", 1).unwrap();
        let ohs = collect_symlinked_ohs(&store.roots_dir()).unwrap();
        assert!(!ohs.contains(o1.oh()));
    }

    #[test]
    fn log_dir_for_creates_directory() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open("s", dir.path(), false).unwrap();
        let log_dir = store.log_dir_for(&"d".repeat(64)).unwrap();
        assert!(log_dir.is_dir());
        assert!(log_dir.starts_with(store.logs_dir().join("d".repeat(64))));
    }

    #[test]
    fn lease_and_intent_via_store() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open("s", dir.path(), false).unwrap();
        let lease = store.lease("run1").unwrap();
        assert!(lease.path().is_file());
        let intent = store.write_intent(&"e".repeat(64)).unwrap();
        assert!(intent.path().is_file());
    }

    #[test]
    fn writes_rejected_on_readonly_store() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = Store::open("s", dir.path(), false).unwrap();
            publish_simple(&store, &"f".repeat(64), &[("x", b"1")]).unwrap();
        }
        let ro = Store::open("s", dir.path(), true).unwrap();
        assert!(ro.lookup(&"f".repeat(64)).unwrap().is_some());
        assert!(matches!(ro.open_staging(), Err(Error::ReadOnlyStore(_))));
        assert!(matches!(ro.pin("x", "y"), Err(Error::ReadOnlyStore(_))));
    }
}
