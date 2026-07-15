//! Views, generations, GC roots (WP5, PPG3_DESIGN.md §11, CONTRACT.md
//! "Views").
//!
//! `.ppg3/views/<n>/` is a tree of symlinks into one or more stores'
//! `entries/<oh>/data/` directories; `.ppg3/views/current` symlinks to the
//! active `<n>`; `<project_dir>/../outputs` symlinks to `.ppg3/views/current`
//! (kept one indirection away from `current` so `outputs/` never has to be
//! touched again after the first run — only `current` is repointed).
//!
//! ## Contract clarification (documented additively in CONTRACT.md too)
//!
//! CONTRACT.md sketches `ViewSpec { entries: Vec<(String, String, String)> }`
//! as `(view-rel path, oh, store-root path or index)`. That shape can't
//! actually address *which file inside a multi-file entry* a view path
//! should link to. This module implements the "SIMPLER and correct"
//! alternative named in the work-package brief: the caller (today: tests;
//! eventually: `ppg3.run()` on the Python side) already knows, for every
//! view path, exactly one `(oh, path-within-entry)` pair — there is no
//! directory-of-symlinks fan-out inside this module. `ViewSpec::entries` is
//! therefore `Vec<ViewEntry>` with an explicit `path_within_entry` (relative
//! path matching a key in that entry's content manifest) and `store_index`
//! (index into the `StoreSet` passed to `write_generation`).

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::error::Error;
use crate::storeset::StoreSet;

/// The generation directory's own meta-file name.
const META_FILE: &str = "meta.json";
const EPHEMERAL_MARKER: &str = ".ephemeral";
const VIEWS_DIRNAME: &str = "views";
const CURRENT_NAME: &str = "current";
const OUTPUTS_NAME: &str = "outputs";

/// One entry of a `ViewSpec`: a single view-relative path linking directly
/// to a single file inside a single store entry. See the module doc for why
/// this is a 4-field struct rather than CONTRACT.md's literal 3-tuple.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewEntry {
    /// Path of the symlink to create under `views/<n>/`, forward-slash
    /// separated, relative (no `..` components, not absolute).
    pub view_rel_path: String,
    /// Output hash of the store entry this view path resolves into.
    pub oh: String,
    /// Path of the target file *within* that entry's `data/` directory
    /// (matches a key in the entry's content manifest).
    pub path_within_entry: String,
    /// Index into `StoreSet::stores` identifying which store holds `oh`.
    pub store_index: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ViewSpec {
    pub entries: Vec<ViewEntry>,
}

/// VCS (jujutsu) snapshot recorded at generation-write time, when the
/// coordinator ran with jj support enabled (`ppg3.new(jj=True)` on the
/// Python side). `committed` distinguishes a generation whose job sources
/// were exactly a durable commit (the jj working-copy commit `@` was
/// empty, so the source tree equals its parent — a commit that survives
/// normal history rewriting) from an *op-log* generation: one built from a
/// dirty working copy whose state is recorded only as jj's automatic
/// working-copy snapshot, i.e. recoverable solely through the operation
/// log once the user amends onward. Op-log generations are reproducible
/// today but not durably so — `remove_old_generations` therefore prunes
/// them under a separate (typically smaller) budget.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VcsInfo {
    /// VCS backend name; `"jj"` is the only producer today.
    pub backend: String,
    /// Commit id of the jj working-copy commit `@` at run time (the commit
    /// that actually contained the job sources).
    pub commit_id: String,
    /// Change id of `@` (stable across jj rewrites, unlike `commit_id`).
    pub change_id: String,
    /// Operation-log id current when the generation was written — the
    /// durable handle for op-log generations (`jj op restore <op_id>`).
    pub op_id: String,
    /// `true` ⇔ the working copy was empty at run time: the sources are
    /// exactly `parent_commit_id`, a proper commit. `false` ⇔ op-log
    /// generation (sources only exist as the `@` auto-snapshot).
    pub committed: bool,
    /// Commit id of `@-` (the working-copy parent) — for `committed`
    /// generations this is the commit the sources correspond to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_commit_id: Option<String>,
}

/// Persisted per-generation metadata (`views/<n>/meta.json`). Store entries
/// are recorded by store *name* (not index) since indices are only
/// meaningful for the `StoreSet` a given `write_generation` call was made
/// with — later readers (a different CLI invocation, `explain`, GC-keep
/// policy) reconstruct their own `StoreSet` from `.ppg3/config.json` and
/// must look stores up by name.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GenMetaEntry {
    pub view_rel_path: String,
    pub oh: String,
    pub path_within_entry: String,
    pub store_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GenMeta {
    pub created_at: i64,
    pub project_id: String,
    #[serde(default)]
    pub ephemeral: bool,
    /// VCS snapshot at write time; `None` for runs made without jj support
    /// (including every generation written before this field existed —
    /// `serde(default)` keeps old `meta.json` files readable).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vcs: Option<VcsInfo>,
    pub entries: Vec<GenMetaEntry>,
}

/// Summary row for `ppg3 generations list`.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct GenInfo {
    pub n: u64,
    pub created_at: i64,
    pub ephemeral: bool,
    pub current: bool,
    pub n_entries: usize,
    /// VCS snapshot from `meta.json`, when the generation was written with
    /// jj support enabled.
    pub vcs: Option<VcsInfo>,
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

static SUFFIX_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A per-process-unique suffix for temp names (crash-safe: never reused
/// even across nanosecond-identical calls within one process).
fn random_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let c = SUFFIX_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{nanos:x}-{}-{c:x}", std::process::id())
}

fn views_dir(project_dir: &Path) -> PathBuf {
    project_dir.join(VIEWS_DIRNAME)
}

fn generation_dir(project_dir: &Path, n: u64) -> PathBuf {
    views_dir(project_dir).join(n.to_string())
}

/// Reject `..`/absolute components in a view-relative path — a malformed
/// `ViewSpec` must not be able to escape `views/<n>/`.
fn validate_view_rel_path(p: &str) -> Result<(), Error> {
    let path = Path::new(p);
    if path.is_absolute() || p.is_empty() {
        return Err(Error::Other(format!(
            "view_rel_path must be a non-empty relative path: {p:?}"
        )));
    }
    for comp in path.components() {
        if matches!(comp, std::path::Component::ParentDir) {
            return Err(Error::Other(format!(
                "view_rel_path must not contain `..`: {p:?}"
            )));
        }
    }
    Ok(())
}

/// Compute a relative symlink target from `link_parent` (the directory the
/// symlink itself lives in) to `target` — both must be absolute, existing
/// (canonicalizable) paths. Used so view trees, `current`, and `outputs`
/// all use relative symlinks (PPG3_DESIGN.md §11: "a tree of relative
/// symlinks into the write store").
fn relative_target(link_parent: &Path, target: &Path) -> PathBuf {
    let lp: Vec<_> = link_parent.components().collect();
    let tp: Vec<_> = target.components().collect();
    let common = lp.iter().zip(tp.iter()).take_while(|(a, b)| a == b).count();
    let mut out = PathBuf::new();
    for _ in common..lp.len() {
        out.push("..");
    }
    for comp in &tp[common..] {
        out.push(comp.as_os_str());
    }
    out
}

fn read_meta(gen_dir: &Path) -> Result<GenMeta, Error> {
    let path = gen_dir.join(META_FILE);
    let bytes = fs::read(&path).map_err(|e| Error::io(&path, e))?;
    serde_json::from_slice(&bytes).map_err(|e| Error::Other(format!("parsing {path:?}: {e}")))
}

/// Read `views/<n>/meta.json`. Public: `explain.rs` needs both the current
/// and previous generations' entry lists.
pub fn read_generation_meta(project_dir: &Path, n: u64) -> Result<GenMeta, Error> {
    read_meta(&generation_dir(project_dir, n))
}

fn next_generation_number(vdir: &Path) -> Result<u64, Error> {
    let mut max = 0u64;
    if vdir.is_dir() {
        let entries = fs::read_dir(vdir).map_err(|e| Error::io(vdir, e))?;
        for entry in entries {
            let entry = entry.map_err(|e| Error::io(vdir, e))?;
            if let Some(n) = entry
                .file_name()
                .to_str()
                .and_then(|s| s.parse::<u64>().ok())
            {
                max = max.max(n);
            }
        }
    }
    Ok(max + 1)
}

/// Read `views/current`'s symlink target and parse it as a generation
/// number. `Ok(None)` if there is no `current` yet.
pub fn current_generation_number(project_dir: &Path) -> Result<Option<u64>, Error> {
    let current_path = views_dir(project_dir).join(CURRENT_NAME);
    match fs::read_link(&current_path) {
        Ok(target) => {
            let name = target.file_name().and_then(|s| s.to_str()).ok_or_else(|| {
                Error::CorruptStore(format!("malformed current symlink target: {target:?}"))
            })?;
            name.parse::<u64>().map(Some).map_err(|_| {
                Error::CorruptStore(format!(
                    "current symlink target not a generation number: {name:?}"
                ))
            })
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(Error::io(&current_path, e)),
    }
}

/// The largest existing generation number strictly less than `n` (skipping
/// numbers whose directory was dropped) — this is `explain.rs`'s notion of
/// "the previous generation" for a rolling, possibly-sparse sequence.
pub fn previous_existing_generation(project_dir: &Path, n: u64) -> Result<Option<u64>, Error> {
    let vdir = views_dir(project_dir);
    if !vdir.is_dir() {
        return Ok(None);
    }
    let mut best: Option<u64> = None;
    for entry in fs::read_dir(&vdir).map_err(|e| Error::io(&vdir, e))? {
        let entry = entry.map_err(|e| Error::io(&vdir, e))?;
        if let Some(m) = entry
            .file_name()
            .to_str()
            .and_then(|s| s.parse::<u64>().ok())
        {
            if m < n && best.map(|b| m > b).unwrap_or(true) {
                best = Some(m);
            }
        }
    }
    Ok(best)
}

/// Ensure `<project_dir>/../outputs` exists and symlinks to
/// `.ppg3/views/current` — but only when `outputs` is currently missing or
/// is already a symlink itself. A pre-existing real directory/file named
/// `outputs` is never touched (CONTRACT.md: "never clobber a real dir").
fn ensure_outputs_symlink(project_dir: &Path) -> Result<(), Error> {
    let parent = project_dir.parent().ok_or_else(|| {
        Error::Other(format!(
            "project_dir {project_dir:?} has no parent to hold outputs/"
        ))
    })?;
    let outputs_path = parent.join(OUTPUTS_NAME);

    let safe_to_write = match fs::symlink_metadata(&outputs_path) {
        Ok(meta) => meta.file_type().is_symlink(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => true,
        Err(e) => return Err(Error::io(&outputs_path, e)),
    };
    if !safe_to_write {
        return Ok(());
    }

    let ppg3_name = project_dir
        .file_name()
        .ok_or_else(|| Error::Other(format!("project_dir {project_dir:?} has no file name")))?;
    let rel_target = PathBuf::from(ppg3_name)
        .join(VIEWS_DIRNAME)
        .join(CURRENT_NAME);

    let tmp = parent.join(format!(".tmp-outputs-{}", random_suffix()));
    std::os::unix::fs::symlink(&rel_target, &tmp).map_err(|e| Error::io(&tmp, e))?;
    fs::rename(&tmp, &outputs_path).map_err(|e| Error::io(&outputs_path, e))
}

/// Build `views/<n>/` for a new generation: register roots first (grouped
/// by store), then lay down the symlink tree, write `meta.json` (and
/// `.ephemeral` if requested), then atomically repoint `views/current` and
/// (best-effort, safety-checked) `outputs`.
pub fn write_generation(
    project_dir: &Path,
    project_id: &str,
    stores: &StoreSet,
    spec: &ViewSpec,
    ephemeral: bool,
) -> Result<u64, Error> {
    write_generation_with_vcs(project_dir, project_id, stores, spec, ephemeral, None)
}

/// [`write_generation`] plus an optional VCS snapshot persisted into the
/// generation's `meta.json` (additive over the CONTRACT.md sketch — the
/// original entry point above keeps its signature and delegates here).
pub fn write_generation_with_vcs(
    project_dir: &Path,
    project_id: &str,
    stores: &StoreSet,
    spec: &ViewSpec,
    ephemeral: bool,
    vcs: Option<VcsInfo>,
) -> Result<u64, Error> {
    for e in &spec.entries {
        validate_view_rel_path(&e.view_rel_path)?;
        if stores.stores.get(e.store_index).is_none() {
            return Err(Error::Other(format!(
                "ViewSpec entry {:?} references store_index {} but only {} stores configured",
                e.view_rel_path,
                e.store_index,
                stores.stores.len()
            )));
        }
    }

    fs::create_dir_all(project_dir).map_err(|e| Error::io(project_dir, e))?;
    let project_dir = fs::canonicalize(project_dir).map_err(|e| Error::io(project_dir, e))?;
    let vdir = views_dir(&project_dir);
    fs::create_dir_all(&vdir).map_err(|e| Error::io(&vdir, e))?;

    let n = next_generation_number(&vdir)?;

    // 1. Register roots FIRST, grouped by store, before any view symlink
    //    exists that could reference them (CONTRACT.md ordering).
    let mut by_store: BTreeMap<usize, Vec<String>> = BTreeMap::new();
    for e in &spec.entries {
        let ohs = by_store.entry(e.store_index).or_default();
        if !ohs.contains(&e.oh) {
            ohs.push(e.oh.clone());
        }
    }
    for (idx, ohs) in &by_store {
        stores.stores[*idx].add_root(project_id, n, ohs)?;
    }

    // 2. Build the generation directory under a hidden tmp name, then
    //    rename it into place as `views/<n>` (atomic wrt readers who only
    //    ever see either "not there yet" or "fully built").
    let tmp_gen_dir = vdir.join(format!(".tmp-gen-{n}-{}", random_suffix()));
    fs::create_dir_all(&tmp_gen_dir).map_err(|e| Error::io(&tmp_gen_dir, e))?;

    let mut meta_entries = Vec::with_capacity(spec.entries.len());
    for e in &spec.entries {
        let store = &stores.stores[e.store_index];
        let target = store.data_dir(&e.oh).join(&e.path_within_entry);
        let target = fs::canonicalize(&target).map_err(|e2| Error::io(&target, e2))?;
        let link_path = tmp_gen_dir.join(&e.view_rel_path);
        if let Some(parent) = link_path.parent() {
            fs::create_dir_all(parent).map_err(|e2| Error::io(parent, e2))?;
        }
        let link_parent = link_path.parent().unwrap_or(&tmp_gen_dir);
        let rel = relative_target(link_parent, &target);
        std::os::unix::fs::symlink(&rel, &link_path).map_err(|e2| Error::io(&link_path, e2))?;

        meta_entries.push(GenMetaEntry {
            view_rel_path: e.view_rel_path.clone(),
            oh: e.oh.clone(),
            path_within_entry: e.path_within_entry.clone(),
            store_name: store.name().to_string(),
        });
    }

    if ephemeral {
        let marker = tmp_gen_dir.join(EPHEMERAL_MARKER);
        fs::write(&marker, b"").map_err(|e| Error::io(&marker, e))?;
    }

    let meta = GenMeta {
        created_at: now_ms(),
        project_id: project_id.to_string(),
        ephemeral,
        vcs,
        entries: meta_entries,
    };
    let meta_path = tmp_gen_dir.join(META_FILE);
    let meta_bytes = serde_json::to_vec_pretty(&meta)
        .map_err(|e| Error::Other(format!("serializing generation meta: {e}")))?;
    fs::write(&meta_path, meta_bytes).map_err(|e| Error::io(&meta_path, e))?;

    let gen_dir = generation_dir(&project_dir, n);
    fs::rename(&tmp_gen_dir, &gen_dir).map_err(|e| Error::io(&gen_dir, e))?;

    // 3. Atomically repoint `views/current` (symlink-to-tmp-name + rename).
    let current_path = vdir.join(CURRENT_NAME);
    let tmp_current = vdir.join(format!(".tmp-current-{}", random_suffix()));
    std::os::unix::fs::symlink(n.to_string(), &tmp_current)
        .map_err(|e| Error::io(&tmp_current, e))?;
    fs::rename(&tmp_current, &current_path).map_err(|e| Error::io(&current_path, e))?;

    // 4. `outputs` -> `.ppg3/views/current`, created only if safe.
    ensure_outputs_symlink(&project_dir)?;

    Ok(n)
}

/// Repoint `views/current` at an already-existing generation.
pub fn rollback(project_dir: &Path, generation: u64) -> Result<(), Error> {
    let vdir = views_dir(project_dir);
    let gen_dir = generation_dir(project_dir, generation);
    if !gen_dir.is_dir() {
        return Err(Error::Other(format!(
            "generation {generation} does not exist (no {gen_dir:?})"
        )));
    }
    let current_path = vdir.join(CURRENT_NAME);
    let tmp_current = vdir.join(format!(".tmp-current-{}", random_suffix()));
    std::os::unix::fs::symlink(generation.to_string(), &tmp_current)
        .map_err(|e| Error::io(&tmp_current, e))?;
    fs::rename(&tmp_current, &current_path).map_err(|e| Error::io(&current_path, e))?;
    Ok(())
}

/// List all generations still on disk, with `current: bool` set from
/// `views/current`'s target. Returned sorted by generation number.
pub fn list_generations(project_dir: &Path) -> Result<Vec<GenInfo>, Error> {
    let vdir = views_dir(project_dir);
    if !vdir.is_dir() {
        return Ok(Vec::new());
    }
    let current = current_generation_number(project_dir)?;
    let mut out = Vec::new();
    for entry in fs::read_dir(&vdir).map_err(|e| Error::io(&vdir, e))? {
        let entry = entry.map_err(|e| Error::io(&vdir, e))?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let n: u64 = match name.parse() {
            Ok(n) => n,
            Err(_) => continue, // "current", ".tmp-*", stray files
        };
        let gen_dir = entry.path();
        if !gen_dir.is_dir() {
            continue;
        }
        let meta = read_meta(&gen_dir)?;
        let ephemeral = gen_dir.join(EPHEMERAL_MARKER).is_file();
        out.push(GenInfo {
            n,
            created_at: meta.created_at,
            ephemeral,
            current: current == Some(n),
            n_entries: meta.entries.len(),
            vcs: meta.vcs,
        });
    }
    out.sort_by_key(|g| g.n);
    Ok(out)
}

/// Unregister a generation's roots (in every store its own `meta.json`
/// references, looked up by name) and remove `views/<n>`. Refuses to drop
/// the current generation.
pub fn drop_generation(
    project_dir: &Path,
    stores: &StoreSet,
    generation: u64,
) -> Result<(), Error> {
    if current_generation_number(project_dir)? == Some(generation) {
        return Err(Error::Other(format!(
            "refusing to drop generation {generation}: it is the current generation"
        )));
    }
    let gen_dir = generation_dir(project_dir, generation);
    if !gen_dir.is_dir() {
        return Err(Error::Other(format!(
            "generation {generation} does not exist (no {gen_dir:?})"
        )));
    }
    let meta = read_meta(&gen_dir)?;

    let mut store_names: Vec<&str> = meta.entries.iter().map(|e| e.store_name.as_str()).collect();
    store_names.sort_unstable();
    store_names.dedup();
    for name in store_names {
        let store = stores
            .stores
            .iter()
            .find(|s| s.name() == name)
            .ok_or_else(|| {
                Error::Other(format!(
                    "generation {generation} references store {name:?} which is not in the \
                     provided StoreSet; cannot unregister its roots"
                ))
            })?;
        store.remove_root(&meta.project_id, generation)?;
    }

    fs::remove_dir_all(&gen_dir).map_err(|e| Error::io(&gen_dir, e))?;
    Ok(())
}

/// GC-policy helper (PPG3_DESIGN.md §6.7): drop all but the last `n`
/// generations. With `keep_explicit`, the budget `n` applies only to
/// *ephemeral* generations ("keep the last N ephemeral + all explicit
/// generations") — non-ephemeral (explicit) generations are never dropped
/// by this function. The current generation is never dropped either way.
/// Returns the generation numbers actually dropped.
pub fn keep_last(
    project_dir: &Path,
    stores: &StoreSet,
    n: u64,
    keep_explicit: bool,
) -> Result<Vec<u64>, Error> {
    let gens = list_generations(project_dir)?;
    let current = gens.iter().find(|g| g.current).map(|g| g.n);

    let mut candidates: Vec<u64> = if keep_explicit {
        gens.iter().filter(|g| g.ephemeral).map(|g| g.n).collect()
    } else {
        gens.iter().map(|g| g.n).collect()
    };
    candidates.sort_unstable();
    let keep_from = candidates.len().saturating_sub(n as usize);
    let to_drop: Vec<u64> = candidates.drain(..keep_from).collect();

    let mut dropped = Vec::new();
    for g in to_drop {
        if Some(g) == current {
            continue;
        }
        drop_generation(project_dir, stores, g)?;
        dropped.push(g);
    }
    Ok(dropped)
}

/// Report of [`remove_old_generations`] — the "old generations" half of the
/// split GC (`ppg3 gc` phase 1; phase 2 is the per-store mark/sweep in
/// `gc.rs`, which only ever sees the roots left over after this phase).
#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct RemoveOldReport {
    /// Dropped generations that were properly committed (or predate VCS
    /// tracking — see `classify_oplog`).
    pub dropped_committed: Vec<u64>,
    /// Dropped op-log/ephemeral generations.
    pub dropped_oplog: Vec<u64>,
    /// Generations still present after the (possibly dry) run.
    pub kept: Vec<u64>,
    pub dry_run: bool,
}

/// `true` ⇔ this generation belongs to the *op-log* bucket of
/// [`remove_old_generations`]: either its jj working copy was dirty at run
/// time (`vcs.committed == false` — the sources survive only in jj's op
/// log) or it is a watch-mode `ephemeral` generation (transient by §6.7's
/// own definition). Generations with no VCS info at all are classified as
/// committed — the conservative choice, since nothing is known about them
/// and the committed budget is the larger one.
fn classify_oplog(g: &GenInfo) -> bool {
    g.ephemeral || g.vcs.as_ref().map(|v| !v.committed).unwrap_or(false)
}

/// Phase 1 of the split GC: drop old generations, with separate retention
/// budgets for properly committed generations (`keep`) and op-log/ephemeral
/// generations (`keep_oplog` — typically smaller: their source state is not
/// durably recorded, so keeping many of them pins intermediates that can
/// never be re-derived from history anyway once jj's op log is abandoned).
/// The current generation is never dropped and does not count against
/// either budget. `dry_run` reports what would be dropped without touching
/// anything. This deliberately does NOT sweep store entries — run the
/// store-level mark/sweep (`Store::gc`) afterwards to actually reclaim the
/// space freed by the unregistered roots.
pub fn remove_old_generations(
    project_dir: &Path,
    stores: &StoreSet,
    keep: u64,
    keep_oplog: u64,
    dry_run: bool,
) -> Result<RemoveOldReport, Error> {
    let gens = list_generations(project_dir)?;
    let current = gens.iter().find(|g| g.current).map(|g| g.n);

    let mut committed: Vec<u64> = Vec::new();
    let mut oplog: Vec<u64> = Vec::new();
    for g in &gens {
        if Some(g.n) == current {
            continue;
        }
        if classify_oplog(g) {
            oplog.push(g.n);
        } else {
            committed.push(g.n);
        }
    }
    committed.sort_unstable();
    oplog.sort_unstable();

    let mut report = RemoveOldReport {
        dry_run,
        ..Default::default()
    };
    let drop_committed = committed.len().saturating_sub(keep as usize);
    let drop_oplog = oplog.len().saturating_sub(keep_oplog as usize);
    for &g in &committed[..drop_committed] {
        if !dry_run {
            drop_generation(project_dir, stores, g)?;
        }
        report.dropped_committed.push(g);
    }
    for &g in &oplog[..drop_oplog] {
        if !dry_run {
            drop_generation(project_dir, stores, g)?;
        }
        report.dropped_oplog.push(g);
    }

    report.kept = gens
        .iter()
        .map(|g| g.n)
        .filter(|n| !report.dropped_committed.contains(n) && !report.dropped_oplog.contains(n))
        .collect();
    report.kept.sort_unstable();
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_view_rel_path_rejects_traversal_and_absolute() {
        assert!(validate_view_rel_path("a/b.txt").is_ok());
        assert!(validate_view_rel_path("../escape").is_err());
        assert!(validate_view_rel_path("/abs").is_err());
        assert!(validate_view_rel_path("").is_err());
    }

    #[test]
    fn relative_target_computes_correct_updirs() {
        let from = Path::new("/a/b/c");
        let to = Path::new("/a/x/y");
        assert_eq!(relative_target(from, to), PathBuf::from("../../x/y"));
    }

    #[test]
    fn relative_target_same_dir() {
        let from = Path::new("/a/b");
        let to = Path::new("/a/b/file.txt");
        assert_eq!(relative_target(from, to), PathBuf::from("file.txt"));
    }
}
