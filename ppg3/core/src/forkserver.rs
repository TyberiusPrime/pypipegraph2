//! Forkserver executor (warm template processes, PPG3_DESIGN.md §6.4,
//! CONTRACT.md "Executor").
//!
//! **Scope constraint (agreed, see STATUS.md)**: the forkserver itself needs
//! *no* user namespaces. Templates fork and their children run in the same
//! `sandbox="none"` staged layout `NoneExecutor` uses today
//! ([`crate::executor::stage`]/[`crate::executor::finish`], reused
//! verbatim). The unshare/pivot_root no-exec entry (§6.4's "riskiest
//! implementation item") stays the existing feature-gated stub in
//! `sandbox.rs` — this module does not touch it.
//!
//! ## Eligibility
//!
//! [`ForkserverExecutor::run`] dispatches a job through a template only when
//! *both* hold: `job.runtime` is `Some` (the §5 `runtime` object) **and**
//! its `argv` has the python-shim shape (`argv[1..]` contains `"-m"`
//! immediately followed by `"ppg3._shim"` — [`is_shim_job`]). Everything
//! else (a `CommandJob`'s plain argv, or a `ForkserverExecutor` constructed
//! with an empty `template_argv` — the opt-out, see `py/src/lib.rs`) falls
//! straight through to `fallback` (a plain [`NoneExecutor`]).
//!
//! ## Template key
//!
//! One template per `(interpreter, preload)` pair, matching §6.4's "one
//! template per `(PyEnv, preload-list)` pair" — `interpreter` is `argv[0]`
//! of the *job* (not of the `template_argv` constructor argument, which
//! only supplies the fixed tail after the interpreter — see
//! [`ForkserverExecutor::new`]'s doc comment for why).
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
}

/// `None` only if `job.argv` is empty (never true for a job that passed
/// `is_shim_job`, since that requires at least two argv tokens — defensive
/// anyway, since this is also unit-tested standalone).
pub(crate) fn template_key_for(job: &PreparedJob) -> Option<TemplateKey> {
    let interpreter = job.argv.first()?.clone();
    let preload = job
        .runtime
        .as_ref()
        .and_then(|r| r.get("preload"))
        .cloned()
        .unwrap_or_else(|| Value::Array(vec![]));
    let preload_json = serde_json::to_string(&preload).ok()?;
    Some(TemplateKey {
        interpreter,
        preload_json,
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

// ======================================================== ForkserverExecutor

/// Warm-template executor (§6.4). Falls back to a plain [`NoneExecutor`]
/// for anything not eligible for template dispatch (see [`is_shim_job`]).
pub struct ForkserverExecutor {
    work_parent: PathBuf,
    /// The fixed *tail* of the template start command (flags/module after
    /// the interpreter, e.g. `["-I", "-m", "ppg3._template"]`) — see
    /// `new`'s doc comment for why element 0 is not used verbatim.
    /// Empty ⇒ forkserver dispatch is globally disabled (every job goes to
    /// `fallback`) — the `forkserver=False` opt-out (`py/src/lib.rs`).
    template_argv: Vec<String>,
    fallback: NoneExecutor,
    templates: Mutex<HashMap<TemplateKey, Arc<Mutex<TemplateSlot>>>>,
}

impl ForkserverExecutor {
    /// `template_argv`: the command used to start a template, e.g.
    /// `["/path/to/python", "-I", "-m", "ppg3._template"]` (the py side
    /// passes the venv interpreter; core tests pass a custom script — see
    /// `core/tests/forkserver.rs`).
    ///
    /// **Deviation from a literal reading of the work brief** (documented
    /// per CONTRACT.md rule 4, see STATUS.md "template interpreter
    /// substitution"): element 0 of `template_argv` is treated as a
    /// *default*, not as the interpreter every template is forced to use.
    /// At spawn time this executor substitutes the *job's own* interpreter
    /// (`job.argv[0]`, i.e. the resolved `PyEnv.executable_hint()` a
    /// `FileJob`/`DataJob`/`FetchJob` already carries) for `template_argv[0]`
    /// and keeps `template_argv[1..]` as the fixed tail — this is what
    /// makes "Template key: (interpreter path = argv[0] of the job, ...)"
    /// (§6.4) actually spawn the interpreter its own key names, and is what
    /// lets a single `ForkserverExecutor` serve jobs declared under
    /// *different* `PyEnv`s (§6.4's "gains something ppg2 never had") in
    /// one run. For every graph in this codebase's test suite (all jobs
    /// share one `PyEnv.current()`) `job.argv[0] == template_argv[0]`
    /// anyway, so this substitution is behaviorally invisible there; it
    /// only matters once multi-PyEnv graphs exist.
    ///
    /// Empty `template_argv` disables the forkserver entirely: every job
    /// (shim-shaped or not) goes straight to `fallback`.
    pub fn new(work_parent: PathBuf, template_argv: Vec<String>, fallback: NoneExecutor) -> Self {
        ForkserverExecutor {
            work_parent,
            template_argv,
            fallback,
            templates: Mutex::new(HashMap::new()),
        }
    }

    fn key_hash(key: &TemplateKey) -> String {
        let bytes = format!("{}\u{0}{}", key.interpreter, key.preload_json);
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

impl Executor for ForkserverExecutor {
    fn run(&self, job: &PreparedJob) -> Result<ExecResult> {
        if self.template_argv.is_empty() || !is_shim_job(job) {
            return self.fallback.run(job);
        }
        warn_once();
        self.run_via_template(job)
    }

    fn is_sandboxed(&self) -> bool {
        false
    }
}

impl Drop for ForkserverExecutor {
    fn drop(&mut self) {
        let slots: Vec<Arc<Mutex<TemplateSlot>>> = {
            let map = self.templates.lock().unwrap();
            map.values().cloned().collect()
        };
        for slot_arc in slots {
            let slot = slot_arc.lock().unwrap();
            if let Some(t) = &slot.template {
                let mut child = t.child.lock().unwrap();
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap as Map;

    fn shim_job(argv0: &str, preload: Vec<&str>) -> PreparedJob {
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
                "python_env": "abc123",
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
    fn empty_template_argv_always_falls_back() {
        let parent = tempfile::tempdir().unwrap();
        let fallback = NoneExecutor::new(parent.path());
        let exec = ForkserverExecutor::new(parent.path().to_path_buf(), vec![], fallback);
        // A shim-shaped job with a real interpreter would normally route
        // through the template, but empty template_argv disables that
        // globally — falls back to NoneExecutor, which will fail to exec
        // "/usr/bin/python3 -I -m ppg3._shim ..." for real (no such
        // behavior here) but must NOT attempt any template spawn (no
        // panics, no hangs) — using /bin/true as argv[0] keeps this a pure
        // "did it route to fallback" check without needing python.
        let job = shim_job("/bin/true", vec![]);
        let result = exec.run(&job).unwrap();
        assert_eq!(result.exit_code, 0);
    }
}
