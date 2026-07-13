//! Forkserver executor (warm template processes, PPG3_DESIGN.md §6.4,
//! §6.7, CONTRACT.md "Executor").
//!
//! **Scope constraint (agreed, see STATUS.md)**: the forkserver itself needs
//! *no* user namespaces. Templates fork and their children run in the same
//! `sandbox="none"` staged layout `NoneExecutor` uses today
//! ([`crate::executor::stage`]/[`crate::executor::finish`], reused
//! verbatim). The unshare/pivot_root no-exec entry (§6.4's "riskiest
//! implementation item") stays the existing feature-gated stub in
//! `sandbox.rs` — this module does not touch it.
//!
//! ## Ownership split (§6.7 cross-run persistence)
//!
//! [`TemplateManager`] owns the template pool (the keyed map of warm
//! processes) independently of any one run: it is constructed once per
//! coordinator *session* (`py/src/lib.rs`'s `Session` pyclass wraps an
//! `Arc<TemplateManager>`) and handed, by reference-counted clone, to a
//! fresh [`ForkserverExecutor`] on every `run()` call. `ForkserverExecutor`
//! itself is now a thin per-run façade — dropping it (at the end of a run)
//! does **not** kill any templates; only [`TemplateManager::shutdown`]
//! (explicit `ppg3.session_stop()`) or dropping the last `Arc` to the
//! manager (session GC'd / process exit) does. Callers that want the old
//! "kill at run end" behavior (no session) simply construct a fresh
//! `TemplateManager` per run and let it drop with the run — its `Drop`
//! impl calls `shutdown()`, matching the pre-§6.7 semantics exactly.
//!
//! Idle-template reaping (a template whose key is no longer used by any
//! job, e.g. after a `PyEnv` fingerprint changes) is **not implemented**:
//! per §6.7, idle templates simply live until session end (`shutdown`) —
//! "there is nothing to go stale" but nothing proactively evicts them
//! either. `TemplateManager::template_count()` exposes the live count for
//! tests/UX to observe this.
//!
//! ## Eligibility
//!
//! [`ForkserverExecutor::run`] dispatches a job through a template only when
//! *both* hold: `job.runtime` is `Some` (the §5 `runtime` object) **and**
//! its `argv` has the python-shim shape (`argv[1..]` contains `"-m"`
//! immediately followed by `"ppg3._shim"` — [`is_shim_job`]). Everything
//! else (a `CommandJob`'s plain argv, or a manager constructed with an
//! empty `template_argv` — the opt-out, see `py/src/lib.rs`) falls straight
//! through to `fallback` (a plain [`NoneExecutor`]).
//!
//! ## Template key
//!
//! One template per `(interpreter, preload, python_env)` triple — §6.4's
//! "one template per `(PyEnv, preload-list)` pair" plus §6.4's fork-time
//! purity requirement ("must be a pure function of `(PyEnv, preload, shim
//! version)`") folded in explicitly by including `runtime.python_env` (the
//! job's resolved `PyEnv` tool hash, §5) in the key. This is what makes
//! §6.7's "a template is discarded only when its `PyEnv` resolution
//! changes" automatic: a changed fingerprint/nix path is simply a new key —
//! the old template is never touched, just never dispatched to again (see
//! "Idle-template reaping" above). `interpreter` is `argv[0]` of the *job*
//! (not of the `template_argv` constructor argument, which only supplies
//! the fixed tail after the interpreter — see [`TemplateManager::new`]'s
//! doc comment for why).
//!
//! ## Wire protocol (JSON lines, one object per line)
//!
//! - template -> rust on start: `{"ready": true, "pid": N, "preloaded": [...]}`
//!   (or `{"ready": false, "error": "..."}` on a preload import failure).
//! - rust -> template: `{"run": {"id", "argv", "env", "cwd", "stdout_path",
//!   "stderr_path"}}`
//! - template -> rust: `{"started": {"id", "pid"}}`
//! - template -> rust (async, any order): `{"exited": {"id", "pid",
//!   "exit_code"}}` (signal death: `exit_code = 128 + signal`)
//!
//! A template's stdout carries only this protocol; its own stderr (never
//! the *job's* stderr, which the forked child redirects straight to
//! `stderr_path`) is captured to a per-template log file under
//! `work_parent` so a crashing template leaves evidence.

use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, BufReader, Write as _};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Mutex, Once};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;

use crate::error::Error;
use crate::executor::{self, Executor, ExecResult, NoneExecutor, PreparedJob};
use crate::Result;

// ============================================================ eligibility

/// True if `job` should be dispatched through a template rather than
/// `fallback`: `job.runtime` is present *and* `argv[1..]` contains `"-m"`
/// immediately followed by `"ppg3._shim"` (the shape `jobs.py`'s
/// `_shim_argv` always builds for `FileJob`/`DataJob`/`FetchJob`). A plain
/// `CommandJob` argv (arbitrary argv, no `runtime.python_env`) never
/// matches; this is a structural check on the argv itself (robust to extra
/// flags before/after `-m ppg3._shim`), not a hardcoded fixed position.
pub(crate) fn is_shim_job(job: &PreparedJob) -> bool {
    if job.runtime.is_none() {
        return false;
    }
    job.argv
        .windows(2)
        .any(|w| w[0] == "-m" && w[1] == "ppg3._shim")
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct TemplateKey {
    interpreter: String,
    preload_json: String,
    /// `runtime.python_env` (the §5 `PyEnv` tool hash), stringified. Part of
    /// the key per §6.4/§6.7 (see module doc "Template key") — folded in
    /// as its own field (not merged into `preload_json`) so
    /// [`ForkserverExecutor`]'s doc/tests can reason about "same
    /// interpreter+preload, different `PyEnv`" independently.
    python_env: String,
}

/// `None` only if `job.argv` is empty (never true for a job that passed
/// `is_shim_job`, since that requires at least two argv tokens — defensive
/// anyway, since this is also unit-tested standalone).
pub(crate) fn template_key_for(job: &PreparedJob) -> Option<TemplateKey> {
    let interpreter = job.argv.first()?.clone();
    let runtime = job.runtime.as_ref();
    let preload = runtime
        .and_then(|r| r.get("preload"))
        .cloned()
        .unwrap_or_else(|| Value::Array(vec![]));
    let preload_json = serde_json::to_string(&preload).ok()?;
    // `runtime.python_env` is a JSON string in the well-formed §5 document
    // (or `null` for a non-python job, which never reaches here since
    // `is_shim_job` already requires the shim argv shape) — stringify
    // whatever is there defensively rather than assuming the exact shape.
    let python_env = match runtime.and_then(|r| r.get("python_env")) {
        Some(Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
        None => "null".to_string(),
    };
    Some(TemplateKey {
        interpreter,
        preload_json,
        python_env,
    })
}

// ================================================================= misc

static WARN_ONCE: Once = Once::new();

fn warn_once() {
    WARN_ONCE.call_once(|| {
        eprintln!("forkserver: sandbox=none children (no enforcement)");
    });
}

static ID_COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let c = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("job-{nanos:x}-{c:x}")
}

/// The template's *own* process env (not the job's — that travels inside
/// the "run" message and is applied by the template's forked child). A
/// scrubbed baseline (§6.1a: `HOME`/`TMPDIR`/`TZ`/`LC_ALL`/
/// `SOURCE_DATE_EPOCH`) plus a pragmatic, **weakly-hermetic** pass-through
/// of `PATH`/`PYTHONPATH`/`VIRTUAL_ENV` from the coordinator's own process
/// env so the template can actually import the `ppg3` package (and, for a
/// nix-resolved `PyEnv`, find its own interpreter's shared libs). This is a
/// deliberate wart, not an oversight — see STATUS.md "forkserver template
/// env wart": hermeticity here is enforced by review, not the kernel;
/// that's the `linux-sandbox`/userns future, out of scope per this work
/// package's scope constraint.
fn template_spawn_env() -> BTreeMap<String, String> {
    let mut env = BTreeMap::new();
    env.insert("HOME".to_string(), "/tmp".to_string());
    env.insert("TMPDIR".to_string(), "/tmp".to_string());
    env.insert("TZ".to_string(), "UTC".to_string());
    env.insert("LC_ALL".to_string(), "C.UTF-8".to_string());
    env.insert("SOURCE_DATE_EPOCH".to_string(), "0".to_string());
    for var in ["PATH", "PYTHONPATH", "VIRTUAL_ENV"] {
        if let Ok(v) = std::env::var(var) {
            env.insert(var.to_string(), v);
        }
    }
    env
}

// =========================================================== template

enum TemplateEvent {
    Exited(i32),
    /// The template process died (EOF/read-error on its stdout) while this
    /// dispatcher was still waiting.
    Died,
}

type Waiters = Arc<Mutex<HashMap<String, SyncSender<TemplateEvent>>>>;

struct Template {
    child: Mutex<Child>,
    stdin: Mutex<ChildStdin>,
    waiters: Waiters,
}

/// One reader thread per template, spawned at template start. Routes
/// `{"exited": {...}}` lines to the matching waiter; on EOF/read-error
/// (template died), wakes *every* still-registered waiter with
/// [`TemplateEvent::Died`] so no dispatcher hangs forever.
fn reader_loop(mut reader: BufReader<std::process::ChildStdout>, waiters: Waiters) {
    let mut line = String::new();
    loop {
        line.clear();
        let n = match reader.read_line(&mut line) {
            Ok(n) => n,
            Err(_) => break,
        };
        if n == 0 {
            break; // EOF
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let v: Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(_) => continue, // ignore malformed/unexpected lines
        };
        if let Some(exited) = v.get("exited") {
            let id = exited.get("id").and_then(|x| x.as_str());
            let exit_code = exited.get("exit_code").and_then(|x| x.as_i64()).unwrap_or(-1) as i32;
            if let Some(id) = id {
                let mut w = waiters.lock().unwrap();
                if let Some(sender) = w.remove(id) {
                    let _ = sender.send(TemplateEvent::Exited(exit_code));
                }
            }
        }
        // "started" lines: nothing else needs to consume them.
    }
    let mut w = waiters.lock().unwrap();
    for (_, sender) in w.drain() {
        let _ = sender.send(TemplateEvent::Died);
    }
}

struct TemplateSlot {
    template: Option<Arc<Template>>,
}

enum DispatchOutcome {
    Result(ExecResult),
    TemplateDied,
}

// ========================================================= TemplateManager

/// Owns the keyed pool of warm template processes (§6.4/§6.7), independent
/// of any one run. See the module doc "Ownership split" for the lifecycle
/// story: a `TemplateManager` is normally wrapped in an `Arc` and shared —
/// by a coordinator session across many `run()` calls (§6.7), or, for a
/// caller with no session concept, constructed fresh per run so its `Drop`
/// (which calls [`shutdown`](TemplateManager::shutdown)) reproduces the
/// pre-§6.7 "kill at run end" behavior exactly.
pub struct TemplateManager {
    work_parent: PathBuf,
    /// The fixed *tail* of the template start command (flags/module after
    /// the interpreter, e.g. `["-I", "-m", "ppg3._template"]`) — see
    /// `new`'s doc comment for why element 0 is not used verbatim.
    /// Empty ⇒ forkserver dispatch is globally disabled (every job goes to
    /// `fallback`) — the `forkserver=False` opt-out (`py/src/lib.rs`).
    template_argv: Vec<String>,
    templates: Mutex<HashMap<TemplateKey, Arc<Mutex<TemplateSlot>>>>,
}

impl TemplateManager {
    /// `template_argv`: the command used to start a template, e.g.
    /// `["/path/to/python", "-I", "-m", "ppg3._template"]` (the py side
    /// passes the venv interpreter; core tests pass a custom script — see
    /// `core/tests/forkserver.rs`).
    ///
    /// **Deviation from a literal reading of the work brief** (documented
    /// per CONTRACT.md rule 4, see STATUS.md "template interpreter
    /// substitution"): element 0 of `template_argv` is treated as a
    /// *default*, not as the interpreter every template is forced to use.
    /// At spawn time this manager substitutes the *job's own* interpreter
    /// (`job.argv[0]`, i.e. the resolved `PyEnv.executable_hint()` a
    /// `FileJob`/`DataJob`/`FetchJob` already carries) for `template_argv[0]`
    /// and keeps `template_argv[1..]` as the fixed tail — this is what
    /// makes "Template key: (interpreter path = argv[0] of the job, ...)"
    /// (§6.4) actually spawn the interpreter its own key names, and is what
    /// lets a single `TemplateManager` serve jobs declared under
    /// *different* `PyEnv`s (§6.4's "gains something ppg2 never had") in
    /// one run — or across many runs in one session (§6.7). For every graph
    /// in this codebase's test suite (all jobs share one `PyEnv.current()`)
    /// `job.argv[0] == template_argv[0]` anyway, so this substitution is
    /// behaviorally invisible there; it only matters once multi-PyEnv
    /// graphs exist.
    ///
    /// Empty `template_argv` disables the forkserver entirely: every job
    /// (shim-shaped or not) goes straight to a caller's `fallback`.
    pub fn new(work_parent: PathBuf, template_argv: Vec<String>) -> Self {
        TemplateManager {
            work_parent,
            template_argv,
            templates: Mutex::new(HashMap::new()),
        }
    }

    /// Whether this manager is configured to ever spawn a template (a
    /// non-empty `template_argv`) — the Rust-side mirror of the
    /// `forkserver=False` opt-out.
    pub(crate) fn is_enabled(&self) -> bool {
        !self.template_argv.is_empty()
    }

    /// Number of template keys with a live-or-formerly-live slot registered
    /// (i.e. distinct `(interpreter, preload, python_env)` triples ever
    /// dispatched to) — for tests/UX (§6.7: idle templates are not reaped,
    /// so this only grows, or resets to 0 after [`shutdown`](Self::shutdown)).
    pub fn template_count(&self) -> usize {
        self.templates.lock().unwrap().len()
    }

    /// Kill and reap every live template, then clear the pool. Idempotent —
    /// safe to call on an already-empty/shutdown manager (a no-op), and
    /// also runs automatically on `Drop`. This is the Rust side of
    /// `ppg3.session_stop()` (§6.7 "session end ... kills templates").
    pub fn shutdown(&self) {
        let slots: Vec<Arc<Mutex<TemplateSlot>>> = {
            let mut map = self.templates.lock().unwrap();
            let slots = map.values().cloned().collect();
            map.clear();
            slots
        };
        for slot_arc in slots {
            let mut slot = slot_arc.lock().unwrap();
            if let Some(t) = slot.template.take() {
                let mut child = t.child.lock().unwrap();
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }

    fn key_hash(key: &TemplateKey) -> String {
        let bytes = format!(
            "{}\u{0}{}\u{0}{}",
            key.interpreter, key.preload_json, key.python_env
        );
        crate::hash::blake3_hex(bytes.as_bytes())[..16].to_string()
    }

    fn spawn_template(&self, key: &TemplateKey) -> Result<Arc<Template>> {
        std::fs::create_dir_all(&self.work_parent).map_err(|e| Error::io(&self.work_parent, e))?;

        let mut argv = vec![key.interpreter.clone()];
        argv.extend(self.template_argv.iter().skip(1).cloned());
        argv.push("--preload".to_string());
        argv.push(key.preload_json.clone());

        let stderr_path = self
            .work_parent
            .join(format!("template-{}.stderr.log", Self::key_hash(key)));
        let stderr_file =
            std::fs::File::create(&stderr_path).map_err(|e| Error::io(&stderr_path, e))?;

        let mut cmd = Command::new(&argv[0]);
        cmd.args(&argv[1..])
            .env_clear()
            .envs(template_spawn_env())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::from(stderr_file));

        let mut child = cmd
            .spawn()
            .map_err(|e| Error::Other(format!("spawning template {:?} failed: {e}", argv[0])))?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| Error::Other("template: stdout was not piped".to_string()))?;
        let mut reader = BufReader::new(stdout);

        let mut first_line = String::new();
        let read = reader.read_line(&mut first_line).map_err(|e| {
            Error::Other(format!("template: reading ready line failed: {e}"))
        })?;
        if read == 0 || first_line.trim().is_empty() {
            let _ = child.kill();
            let _ = child.wait();
            return Err(Error::Other(format!(
                "template {:?} exited before printing a ready line (see {})",
                argv[0],
                stderr_path.display()
            )));
        }
        let ready: Value = serde_json::from_str(first_line.trim()).map_err(|e| {
            Error::Other(format!(
                "template {:?} printed a non-JSON ready line ({e}): {first_line:?}",
                argv[0]
            ))
        })?;
        let ok = ready.get("ready").and_then(Value::as_bool).unwrap_or(false);
        if !ok {
            let err_msg = ready
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("unknown error");
            let _ = child.kill();
            let _ = child.wait();
            return Err(Error::Other(format!(
                "template {:?} failed to start: {err_msg}",
                argv[0]
            )));
        }

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| Error::Other("template: stdin was not piped".to_string()))?;

        let waiters: Waiters = Arc::new(Mutex::new(HashMap::new()));
        let reader_waiters = waiters.clone();
        thread::spawn(move || reader_loop(reader, reader_waiters));

        Ok(Arc::new(Template {
            child: Mutex::new(child),
            stdin: Mutex::new(stdin),
            waiters,
        }))
    }

    /// Get a live template for `key`, (re)spawning if none exists or the
    /// existing one has died. Serializes creation per-key (via the slot's
    /// own `Mutex`, held across the blocking `spawn_template` call
    /// deliberately — this is a *named*, intentionally-held guard, not the
    /// accidental-temporary-lifetime-extension hazard: it only blocks other
    /// callers wanting the *same* key, never `self.templates` itself, which
    /// is released immediately after the per-key slot is looked up/inserted).
    fn get_or_spawn(&self, key: &TemplateKey) -> Result<Arc<Template>> {
        let slot_arc = {
            let mut templates = self.templates.lock().unwrap();
            templates
                .entry(key.clone())
                .or_insert_with(|| Arc::new(Mutex::new(TemplateSlot { template: None })))
                .clone()
        };

        let mut slot = slot_arc.lock().unwrap();
        if let Some(t) = &slot.template {
            let alive = {
                let mut child = t.child.lock().unwrap();
                matches!(child.try_wait(), Ok(None))
            };
            if alive {
                return Ok(t.clone());
            }
        }
        let fresh = self.spawn_template(key)?;
        slot.template = Some(fresh.clone());
        Ok(fresh)
    }

    /// Remove `dead` from `key`'s slot (if it is still the current
    /// occupant) and best-effort kill+wait it.
    fn drop_template(&self, key: &TemplateKey, dead: &Arc<Template>) {
        let slot_arc = {
            let templates = self.templates.lock().unwrap();
            templates.get(key).cloned()
        };
        if let Some(slot_arc) = slot_arc {
            let mut slot = slot_arc.lock().unwrap();
            let matches_dead = matches!(&slot.template, Some(current) if Arc::ptr_eq(current, dead));
            if matches_dead {
                slot.template = None;
            }
        }
        let mut child = dead.child.lock().unwrap();
        let _ = child.kill();
        let _ = child.wait();
    }

    fn dispatch(
        &self,
        template: &Template,
        staged: &executor::StagedJob,
        stdout_path: &Path,
        stderr_path: &Path,
    ) -> Result<DispatchOutcome> {
        let alive = {
            let mut child = template.child.lock().unwrap();
            matches!(child.try_wait(), Ok(None))
        };
        if !alive {
            return Ok(DispatchOutcome::TemplateDied);
        }

        let id = unique_id();
        let (tx, rx) = std::sync::mpsc::sync_channel::<TemplateEvent>(1);
        {
            let mut w = template.waiters.lock().unwrap();
            w.insert(id.clone(), tx);
        }

        let msg = serde_json::json!({
            "run": {
                "id": id,
                "argv": staged.argv,
                "env": staged.env,
                "cwd": staged.cwd.to_string_lossy(),
                "stdout_path": stdout_path.to_string_lossy(),
                "stderr_path": stderr_path.to_string_lossy(),
            }
        });
        let mut line = serde_json::to_string(&msg)
            .map_err(|e| Error::Other(format!("serializing run message: {e}")))?;
        line.push('\n');

        let write_ok = {
            let mut stdin = template.stdin.lock().unwrap();
            stdin.write_all(line.as_bytes()).and_then(|_| stdin.flush())
        };
        if write_ok.is_err() {
            let mut w = template.waiters.lock().unwrap();
            w.remove(&id);
            return Ok(DispatchOutcome::TemplateDied);
        }

        // No timeout by design (§6.4/work brief: "jobs can be long"); a
        // dead template is guaranteed to eventually produce *some* event
        // for every registered id — either `Exited` or, from the reader
        // thread's EOF handler, `Died` — so this cannot hang forever
        // beyond the template's own lifetime.
        match rx.recv() {
            Ok(TemplateEvent::Exited(exit_code)) => {
                let stdout = std::fs::read(stdout_path).unwrap_or_default();
                let stderr = std::fs::read(stderr_path).unwrap_or_default();
                Ok(DispatchOutcome::Result(ExecResult {
                    exit_code,
                    stdout,
                    stderr,
                }))
            }
            Ok(TemplateEvent::Died) | Err(_) => {
                let mut w = template.waiters.lock().unwrap();
                w.remove(&id);
                Ok(DispatchOutcome::TemplateDied)
            }
        }
    }

    fn run_via_template(&self, job: &PreparedJob) -> Result<ExecResult> {
        let key = template_key_for(job).ok_or_else(|| {
            Error::Other("forkserver: could not derive a template key (empty argv?)".to_string())
        })?;

        let staged = executor::stage(job, &self.work_parent)?;
        let stdout_path = staged.log_dir.join("stdout.txt");
        let stderr_path = staged.log_dir.join("stderr.txt");

        let mut respawned_once = false;
        loop {
            let template = self.get_or_spawn(&key)?;
            match self.dispatch(&template, &staged, &stdout_path, &stderr_path)? {
                DispatchOutcome::Result(result) => {
                    executor::finish(&staged, result.exit_code == 0);
                    return Ok(result);
                }
                DispatchOutcome::TemplateDied => {
                    self.drop_template(&key, &template);
                    if respawned_once {
                        executor::finish(&staged, false);
                        return Err(Error::Other(format!(
                            "forkserver: template for interpreter {:?} died twice while \
                             dispatching job {:?} (respawn did not help)",
                            key.interpreter, job.ik
                        )));
                    }
                    respawned_once = true;
                    continue;
                }
            }
        }
    }
}

/// §6.7 "session end ... kills templates": dropping the last `Arc` to a
/// `TemplateManager` (session GC'd, or — for a caller with no session
/// concept — a per-run manager going out of scope at the end of `run()`)
/// kills every live template. Calls the same [`shutdown`](TemplateManager::shutdown)
/// an explicit `ppg3.session_stop()` does, so both paths are one code path.
impl Drop for TemplateManager {
    fn drop(&mut self) {
        self.shutdown();
    }
}

// ======================================================== ForkserverExecutor

/// Warm-template executor (§6.4) — a thin per-run façade over a shared
/// [`TemplateManager`] (§6.7: the manager, not the executor, owns the
/// template pool's lifetime). Falls back to a plain [`NoneExecutor`] for
/// anything not eligible for template dispatch (see [`is_shim_job`]).
/// Dropping a `ForkserverExecutor` does **not** kill any templates — only
/// dropping (or explicitly shutting down) the underlying `TemplateManager`
/// does; see the module doc "Ownership split".
pub struct ForkserverExecutor {
    manager: Arc<TemplateManager>,
    fallback: NoneExecutor,
}

impl ForkserverExecutor {
    pub fn new(manager: Arc<TemplateManager>, fallback: NoneExecutor) -> Self {
        ForkserverExecutor { manager, fallback }
    }
}

impl Executor for ForkserverExecutor {
    fn run(&self, job: &PreparedJob) -> Result<ExecResult> {
        if !self.manager.is_enabled() || !is_shim_job(job) {
            return self.fallback.run(job);
        }
        warn_once();
        self.manager.run_via_template(job)
    }

    fn is_sandboxed(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap as Map;

    fn shim_job(argv0: &str, preload: Vec<&str>) -> PreparedJob {
        shim_job_with_env(argv0, preload, "abc123")
    }

    fn shim_job_with_env(argv0: &str, preload: Vec<&str>, python_env: &str) -> PreparedJob {
        PreparedJob {
            ik: "a".repeat(64),
            argv: vec![
                argv0.to_string(),
                "-I".to_string(),
                "-m".to_string(),
                "ppg3._shim".to_string(),
                "--spec-b64".to_string(),
                "AAAA".to_string(),
            ],
            env: Map::new(),
            inputs: vec![],
            tools: vec![],
            out_dir: PathBuf::from("/tmp/out"),
            log_dir: PathBuf::from("/tmp/log"),
            allow_network: false,
            cwd_out: true,
            runtime: Some(serde_json::json!({
                "python_env": python_env,
                "preload": preload,
                "shim": "1",
            })),
        }
    }

    fn command_job() -> PreparedJob {
        PreparedJob {
            ik: "b".repeat(64),
            argv: vec!["/bin/sh".to_string(), "-c".to_string(), "true".to_string()],
            env: Map::new(),
            inputs: vec![],
            tools: vec![],
            out_dir: PathBuf::from("/tmp/out"),
            log_dir: PathBuf::from("/tmp/log"),
            allow_network: false,
            cwd_out: true,
            runtime: Some(serde_json::json!({"python_env": null, "preload": [], "shim": "0"})),
        }
    }

    #[test]
    fn is_shim_job_true_for_shim_argv_shape() {
        assert!(is_shim_job(&shim_job("/usr/bin/python3", vec!["json"])));
    }

    #[test]
    fn is_shim_job_false_for_plain_command_argv() {
        assert!(!is_shim_job(&command_job()));
    }

    #[test]
    fn is_shim_job_false_when_runtime_is_none() {
        let mut job = shim_job("/usr/bin/python3", vec![]);
        job.runtime = None;
        assert!(!is_shim_job(&job));
    }

    #[test]
    fn is_shim_job_false_for_empty_argv() {
        let mut job = shim_job("/usr/bin/python3", vec![]);
        job.argv = vec![];
        assert!(!is_shim_job(&job));
    }

    #[test]
    fn is_shim_job_robust_to_extra_leading_flags() {
        let mut job = shim_job("/usr/bin/python3", vec![]);
        job.argv = vec![
            "/usr/bin/python3".to_string(),
            "-I".to_string(),
            "-B".to_string(),
            "-m".to_string(),
            "ppg3._shim".to_string(),
        ];
        assert!(is_shim_job(&job));
    }

    #[test]
    fn template_key_uses_job_argv0_as_interpreter() {
        let job = shim_job("/some/venv/bin/python3", vec!["numpy", "pandas"]);
        let key = template_key_for(&job).unwrap();
        assert_eq!(key.interpreter, "/some/venv/bin/python3");
        assert_eq!(key.preload_json, "[\"numpy\",\"pandas\"]");
    }

    #[test]
    fn template_key_differs_by_preload() {
        let a = template_key_for(&shim_job("/py", vec!["numpy"])).unwrap();
        let b = template_key_for(&shim_job("/py", vec!["pandas"])).unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn template_key_differs_by_interpreter() {
        let a = template_key_for(&shim_job("/py310", vec!["numpy"])).unwrap();
        let b = template_key_for(&shim_job("/py311", vec!["numpy"])).unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn template_key_same_for_identical_interpreter_and_preload() {
        let a = template_key_for(&shim_job("/py", vec!["numpy", "pandas"])).unwrap();
        let b = template_key_for(&shim_job("/py", vec!["numpy", "pandas"])).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn template_key_none_for_empty_argv() {
        let mut job = shim_job("/py", vec![]);
        job.argv = vec![];
        assert!(template_key_for(&job).is_none());
    }

    #[test]
    fn template_key_differs_by_python_env() {
        let a = template_key_for(&shim_job_with_env("/py", vec!["numpy"], "env-a")).unwrap();
        let b = template_key_for(&shim_job_with_env("/py", vec!["numpy"], "env-b")).unwrap();
        assert_ne!(a, b, "same interpreter+preload but different python_env must differ");
    }

    #[test]
    fn empty_template_argv_always_falls_back() {
        let parent = tempfile::tempdir().unwrap();
        let fallback = NoneExecutor::new(parent.path());
        let manager = Arc::new(TemplateManager::new(parent.path().to_path_buf(), vec![]));
        let exec = ForkserverExecutor::new(manager, fallback);
        // A shim-shaped job with a real interpreter would normally route
        // through the template, but empty template_argv disables that
        // globally — falls back to NoneExecutor, which will fail to exec
        // "/usr/bin/python3 -I -m ppg3._shim ..." for real (no such
        // behavior here) but must NOT attempt any template spawn (no
        // panics, no hangs) — using /bin/true as argv[0] keeps this a pure
        // "did it route to fallback" check without needing python.

        let bin_true = {
            // no /bin/true on nixos.
            std::str::from_utf8(
                &(
            std::process::Command::new("which")
                .arg("true")
                .output()
                .expect("failed to run `which true`")
                .stdout)).expect("which true did not return utf-8").trim().to_string()
        };
        let job = shim_job(&bin_true, vec![]);
        let result = exec.run(&job).unwrap();
        assert_eq!(result.exit_code, 0);
    }

    #[test]
    fn manager_shutdown_is_idempotent_and_clears_count() {
        let parent = tempfile::tempdir().unwrap();
        let manager = TemplateManager::new(parent.path().to_path_buf(), vec![]);
        assert_eq!(manager.template_count(), 0);
        manager.shutdown();
        manager.shutdown();
        assert_eq!(manager.template_count(), 0);
    }
}
