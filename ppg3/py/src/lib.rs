//! PyO3 bindings for ppg3-core (WP7, ppg3/CONTRACT.md "PyO3 boundary").
//! Module name: `ppg3._core`.
//!
//! Boundary rule (§8.1 / CONTRACT.md): every function here is str/bytes JSON
//! in, JSON out — no rich objects cross the boundary except the opaque
//! [`StoreSetHandle`] pyclass and the caller-supplied `callbacks` object
//! (which itself is only ever called with/returning JSON strings).
//!
//! ## Additive extensions over the CONTRACT.md sketch (see STATUS.md)
//!
//! - `run(...)` takes an extra `work_dir: &str` fifth argument (the
//!   [`NoneExecutor`] staging root) — not in CONTRACT.md's one-line
//!   function list, needed to construct the executor.
//! - `run(...)` takes a further additive, optional (`#[pyo3(signature =
//!   (..., template_argv = Vec::new()))]`) sixth argument, `template_argv:
//!   Vec<String>` (forkserver work package, see STATUS.md) — the template
//!   start command (e.g. `[sys.executable, "-I", "-m", "ppg3._template"]`)
//!   handed to [`TemplateManager::new`]. `run()` now always constructs a
//!   `ForkserverExecutor` (wrapping a plain `NoneExecutor` as its
//!   `fallback`) instead of a bare `NoneExecutor`; an empty `template_argv`
//!   (the default, and what `run.py` passes for `forkserver=False`) makes
//!   `ForkserverExecutor` behave exactly like the old bare `NoneExecutor`
//!   for every job — this is a behavior-preserving superset, not a
//!   breaking change, for any caller that omits the new argument.
//! - `run(...)` takes a further additive, optional seventh argument,
//!   `session: Option<Session>` (§6.7 cross-run template persistence, see
//!   STATUS.md "session mode"). `Session` (a new `#[pyclass]`) wraps an
//!   `Arc<TemplateManager>` that outlives any one `run()` call —
//!   `python/ppg3/run.py` keeps a module-level `Session` alive across
//!   `ppg3.run()` calls so warm templates survive between runs inside one
//!   coordinator process. When `session` is given, `run()` builds its
//!   `ForkserverExecutor` from `session`'s manager (a cloned `Arc`, cheap)
//!   and **ignores** `template_argv` entirely — the session was already
//!   configured with its own `template_argv` at `open_session()` time, and
//!   letting a later `run()` call silently override it would defeat the
//!   whole point of a session having one stable template pool. Without a
//!   `session` (the default, `None`), `run()` builds a private
//!   `TemplateManager` exactly as before — that manager (and any templates
//!   it spawned) is torn down at the end of the call when the `Arc` drops,
//!   reproducing the pre-§6.7 "kill at run end" behavior for callers that
//!   never opted into a session.
//! - `lookup(...)` returns the manifest JSON with an extra top-level
//!   `store_index` field spliced in (`#[serde(flatten)]` of the `Manifest`
//!   plus `"store_index"`). CONTRACT.md's `ViewSpec`/`write_generation`
//!   (see its "Additive clarification (WP5)") needs a `store_index` per
//!   view entry, and "Python never sees store entry paths except via
//!   manifest/report JSON" — an index is not a path, so this stays within
//!   that rule. Without it, `ppg3.run()` on the Python side would have no
//!   way to learn which configured store a given `oh` actually landed in
//!   (`StoreSet::lookup` walks stores in order and the winning index is
//!   otherwise Rust-internal).
//! - `open_stores`'s `config_json` is the bare JSON array CONTRACT.md's
//!   PyO3 boundary paragraph describes (`[{"name","path","readonly"}, ...]`)
//!   — distinct from `.ppg3/config.json`'s `{"stores": [...]}` wrapper
//!   object (a CLI-side, WP5 concept; see `cli/src/config.rs`). `run.py`
//!   builds both shapes from the same `Store` list.
//! - No abort-flag wiring in v1: `run()` constructs its own private
//!   `AtomicBool` (always `false`) per call; there is no way to request an
//!   abort from Python yet. Noted in STATUS.md.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use ppg3_core::executor::NoneExecutor;
use ppg3_core::forkserver::{ForkserverExecutor, TemplateManager};
use ppg3_core::manifest::Manifest;
use ppg3_core::scheduler::{self, HostCallbacks, JobDef};
use ppg3_core::store::Store;
use ppg3_core::storeset::StoreSet;
use ppg3_core::views::{self, ViewEntry, ViewSpec};
use ppg3_core::{canon, hash};

fn to_pyerr(e: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

// =============================================================== hashing

#[pyfunction]
fn input_key(canonical: &[u8]) -> PyResult<String> {
    canon::input_key(canonical).map_err(to_pyerr)
}

#[pyfunction]
fn canonicalize(json: &str) -> PyResult<String> {
    let v: Value = serde_json::from_str(json).map_err(to_pyerr)?;
    let bytes = canon::canonicalize(&v).map_err(to_pyerr)?;
    String::from_utf8(bytes).map_err(to_pyerr)
}

#[pyfunction]
fn blake3_file(path: &str) -> PyResult<String> {
    hash::blake3_file(Path::new(path)).map_err(to_pyerr)
}

#[pyfunction]
fn blake3_hex(data: &[u8]) -> PyResult<String> {
    Ok(hash::blake3_hex(data))
}

// ================================================================ stores

#[derive(Debug, Deserialize)]
struct StoreConfigEntry {
    name: String,
    path: PathBuf,
    #[serde(default)]
    readonly: bool,
}

/// Opaque handle wrapping an open `StoreSet` (CONTRACT.md: "Python never
/// sees store entry paths except via manifest/report JSON").
#[pyclass]
struct StoreSetHandle {
    inner: StoreSet,
}

#[pyfunction]
fn open_stores(config_json: &str) -> PyResult<StoreSetHandle> {
    let entries: Vec<StoreConfigEntry> = serde_json::from_str(config_json).map_err(to_pyerr)?;
    let mut stores = Vec::with_capacity(entries.len());
    for e in entries {
        let store = Store::open(&e.name, &e.path, e.readonly).map_err(to_pyerr)?;
        stores.push(store);
    }
    Ok(StoreSetHandle {
        inner: StoreSet::new(stores),
    })
}

#[derive(Serialize)]
struct LookupResult<'a> {
    store_index: usize,
    #[serde(flatten)]
    manifest: &'a Manifest,
}

#[pyfunction]
fn lookup(handle: &StoreSetHandle, ik: &str) -> PyResult<Option<String>> {
    match handle.inner.lookup(ik).map_err(to_pyerr)? {
        Some((store_index, manifest)) => {
            let wrapped = LookupResult {
                store_index,
                manifest: &manifest,
            };
            let json = serde_json::to_string(&wrapped).map_err(to_pyerr)?;
            Ok(Some(json))
        }
        None => Ok(None),
    }
}

// ============================================================= session

/// A coordinator-session handle (§6.7 "templates outlive runs inside a
/// coordinator session"): wraps an `Arc<TemplateManager>` so warm templates
/// survive across multiple [`run`] calls sharing this `Session`, instead of
/// being killed at the end of each individual run. Python holds one of
/// these at module scope (`python/ppg3/run.py`); when the last reference to
/// a `Session` (and thus to its `Arc<TemplateManager>`) is dropped —
/// explicit `ppg3.session_stop()`, or ordinary Python GC at process exit —
/// every template it owns is killed (`TemplateManager`'s `Drop` impl).
#[pyclass]
struct Session {
    manager: Arc<TemplateManager>,
}

/// Opens a new coordinator session: a `TemplateManager` whose templates
/// live until `session_shutdown()` is called (or this `Session` — and every
/// clone of it — is dropped). `work_dir`: the staging root passed through
/// to `TemplateManager::new` (same role as `run()`'s own `work_dir`
/// argument — see that function's doc comment on why a *stable* directory
/// matters for session mode). `template_argv`: the template start command
/// (e.g. `[sys.executable, "-I", "-m", "ppg3._template"]`); empty disables
/// the forkserver for every `run()` call that uses this session (the
/// `forkserver=False` opt-out, session-scoped).
#[pyfunction]
fn open_session(work_dir: &str, template_argv: Vec<String>) -> Session {
    Session {
        manager: Arc::new(TemplateManager::new(PathBuf::from(work_dir), template_argv)),
    }
}

/// Kills and reaps every template `session` currently owns, then clears its
/// pool (`TemplateManager::shutdown` — idempotent, safe to call more than
/// once on the same session). Does not invalidate `session` itself: a
/// further `run(..., session=session)` call simply respawns templates on
/// demand into the same (now-empty) pool.
#[pyfunction]
fn session_shutdown(session: &Session) {
    session.manager.shutdown();
}

/// Number of distinct template keys `session` has ever spawned a template
/// for (§6.7: idle templates — e.g. from a since-superseded `PyEnv`
/// resolution — are not proactively reaped, so this only grows until
/// `session_shutdown()` resets it to 0). For tests/UX.
#[pyfunction]
fn session_template_count(session: &Session) -> usize {
    session.manager.template_count()
}

// =============================================================== run()

/// Wraps a Python `callbacks` object (§8.1 rule 1: the two coarse
/// Rust->Python calls) as a `HostCallbacks` implementation. Both methods
/// re-acquire the GIL themselves — `run()` below calls into
/// `scheduler::run` via `py.allow_threads`, so no worker thread holds the
/// GIL except for the brief window inside these two methods.
struct PyHostCallbacks {
    callbacks: PyObject,
}

impl HostCallbacks for PyHostCallbacks {
    fn expand_graph_job(&self, job_id: &str) -> ppg3_core::Result<Vec<JobDef>> {
        Python::with_gil(|py| {
            let bound = self.callbacks.bind(py);
            let result = bound
                .call_method1("expand_graph_job", (job_id,))
                .map_err(|e| {
                    ppg3_core::Error::Other(format!(
                        "expand_graph_job({job_id:?}) raised in Python: {e}"
                    ))
                })?;
            let json_str: String = result.extract().map_err(|e| {
                ppg3_core::Error::Other(format!(
                    "expand_graph_job({job_id:?}) must return a JSON str: {e}"
                ))
            })?;
            serde_json::from_str(&json_str).map_err(|e| {
                ppg3_core::Error::Other(format!(
                    "expand_graph_job({job_id:?}) returned invalid JobDef-list JSON: {e}"
                ))
            })
        })
    }

    fn run_in_process(&self, job_id: &str, key_doc: &Value) -> ppg3_core::Result<()> {
        Python::with_gil(|py| {
            let key_doc_json = serde_json::to_string(key_doc).map_err(|e| {
                ppg3_core::Error::Other(format!("serializing key_doc for {job_id:?}: {e}"))
            })?;
            let bound = self.callbacks.bind(py);
            bound
                .call_method1("run_in_process", (job_id, key_doc_json))
                .map_err(|e| {
                    ppg3_core::Error::Other(format!(
                        "run_in_process({job_id:?}) raised in Python: {e}"
                    ))
                })?;
            Ok(())
        })
    }
}

// `session` (§6.7) is the 8th additive-but-optional argument on this PyO3
// boundary function; splitting it into a builder/options struct would ripple
// through `run.py`'s call site and CONTRACT.md's PyO3 boundary sketch for no
// real clarity gain at this arity — every argument here is a plain JSON
// str/bool/Vec/handle, not several booleans that are easy to transpose.
#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (handle, jobs_json, parallelism_json, callbacks, work_dir, template_argv = Vec::new(), session = None))]
fn run(
    py: Python<'_>,
    handle: &StoreSetHandle,
    jobs_json: &str,
    parallelism_json: &str,
    callbacks: PyObject,
    work_dir: &str,
    template_argv: Vec<String>,
    session: Option<PyRef<'_, Session>>,
) -> PyResult<String> {
    let jobs: Vec<JobDef> = serde_json::from_str(jobs_json).map_err(to_pyerr)?;
    let parallelism: BTreeMap<String, u64> =
        serde_json::from_str(parallelism_json).map_err(to_pyerr)?;
    let fallback = NoneExecutor::new(PathBuf::from(work_dir));
    // Additive 6th argument (forkserver work package, see STATUS.md):
    // `template_argv` — empty (the default, and what `run.py` passes when
    // the caller opts out via `forkserver=False`) disables the forkserver
    // entirely, so every job takes exactly the pre-existing `NoneExecutor`
    // path; non-empty routes shim-shaped python jobs through warm template
    // processes (`ppg3_core::forkserver::ForkserverExecutor`) while
    // `CommandJob`s and non-shim argv still fall back to `NoneExecutor`
    // unchanged.
    //
    // Additive 7th argument (§6.7 session mode, see STATUS.md): `session`,
    // when given, supplies an already-open `TemplateManager` (a cloned
    // `Arc`, so its templates outlive this call) and `template_argv` above
    // is ignored — see this module's doc comment "session" bullet for why.
    // Without a session, a fresh `TemplateManager` is built right here and
    // torn down (killing any templates it spawned) when it drops at the
    // end of this function, exactly like the pre-§6.7 `ForkserverExecutor`.
    let manager = match &session {
        Some(s) => s.manager.clone(),
        None => Arc::new(TemplateManager::new(PathBuf::from(work_dir), template_argv)),
    };
    let executor = ForkserverExecutor::new(manager, fallback);
    let host_callbacks = PyHostCallbacks { callbacks };
    let abort = AtomicBool::new(false);

    let report = py
        .allow_threads(|| {
            scheduler::run(
                &handle.inner,
                &executor,
                jobs,
                &host_callbacks,
                &parallelism,
                &abort,
            )
        })
        .map_err(to_pyerr)?;

    serde_json::to_string(&report).map_err(to_pyerr)
}

// ======================================================== write_generation

#[derive(Deserialize)]
struct ViewEntryWire {
    view_rel_path: String,
    oh: String,
    path_within_entry: String,
    store_index: usize,
}

#[derive(Deserialize)]
struct ViewSpecWire {
    entries: Vec<ViewEntryWire>,
}

#[pyfunction]
fn write_generation(
    handle: &StoreSetHandle,
    project_dir: &str,
    project_id: &str,
    view_spec_json: &str,
    ephemeral: bool,
) -> PyResult<u64> {
    let wire: ViewSpecWire = serde_json::from_str(view_spec_json).map_err(to_pyerr)?;
    let spec = ViewSpec {
        entries: wire
            .entries
            .into_iter()
            .map(|e| ViewEntry {
                view_rel_path: e.view_rel_path,
                oh: e.oh,
                path_within_entry: e.path_within_entry,
                store_index: e.store_index,
            })
            .collect(),
    };
    views::write_generation(
        Path::new(project_dir),
        project_id,
        &handle.inner,
        &spec,
        ephemeral,
    )
    .map_err(to_pyerr)
}

// ================================================================ module

#[pymodule]
#[pyo3(name = "_core")]
fn ppg3_core_ext(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<StoreSetHandle>()?;
    m.add_class::<Session>()?;
    m.add_function(wrap_pyfunction!(input_key, m)?)?;
    m.add_function(wrap_pyfunction!(canonicalize, m)?)?;
    m.add_function(wrap_pyfunction!(blake3_file, m)?)?;
    m.add_function(wrap_pyfunction!(blake3_hex, m)?)?;
    m.add_function(wrap_pyfunction!(open_stores, m)?)?;
    m.add_function(wrap_pyfunction!(lookup, m)?)?;
    m.add_function(wrap_pyfunction!(run, m)?)?;
    m.add_function(wrap_pyfunction!(write_generation, m)?)?;
    m.add_function(wrap_pyfunction!(open_session, m)?)?;
    m.add_function(wrap_pyfunction!(session_shutdown, m)?)?;
    m.add_function(wrap_pyfunction!(session_template_count, m)?)?;
    Ok(())
}
