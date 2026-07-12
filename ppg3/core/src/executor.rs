//! Executors (WP3, PPG3_DESIGN.md §6, CONTRACT.md "Executor").
//!
//! `Executor::run` turns a fully-lowered `PreparedJob` (virtual `/ppg/...`
//! paths only, §6.2) into an `ExecResult`. Two implementations live here:
//!
//! - [`NoneExecutor`]: the tested path in this dev container (no userns).
//!   Builds a staged directory tree with symlinks standing in for bind
//!   mounts and rewrites `/ppg/...` occurrences in argv/env to the real
//!   staged path before exec-ing. No enforcement — a job *can* escape this
//!   sandbox if it tries. Prints a one-time warning and the caller is
//!   expected to mark the resulting manifest `"sandboxed": false`
//!   (`Executor::is_sandboxed` reports this).
//! - [`bwrap_argv`] / [`BwrapExecutor`]: real enforcement via bubblewrap.
//!   `bwrap_argv` is a pure function (no process spawned) so it is fully
//!   unit-testable without `bwrap` installed; `BwrapExecutor` spawns it and
//!   is only exercised by an integration test that skips itself when
//!   `bwrap` is not on `PATH` (not installed in this container).
//!
//! ## Deviation from the CONTRACT.md doc-comment for `NoneExecutor`
//!
//! The one-line CONTRACT.md sketch for `NoneExecutor` mentions a `PPG_ROOT`
//! env var pointing at the staged root. The work-package brief for this
//! agent is explicit and more specific: "sets env exactly to job.env plus
//! TMPDIR/HOME ... don't invent extra vars beyond TMPDIR/HOME defaults" —
//! precisely so that an "env scrub" integration test (run `env` inside the
//! job, snapshot it, assert it is *exactly* the declared set) is
//! meaningful. Adding `PPG_ROOT` would break that invariant for no
//! contract-mandated reason, so it is omitted here. See STATUS.md.

use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Once;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::Error;
use crate::Result;

/// A single read-only bind mount (or, for `NoneExecutor`, symlink) from a
/// real filesystem path to a virtual `/ppg/...` path the job sees.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mount {
    /// e.g. `/ppg/in/<name>` or `/ppg/tools/<name>`.
    pub virtual_path: String,
    pub source: PathBuf,
}

/// A job lowered to argv/env/mounts, ready to execute. All paths inside
/// `argv`/`env` use the virtual `/ppg/...` form (§6.2); executors translate.
#[derive(Debug, Clone)]
pub struct PreparedJob {
    pub ik: String,
    pub argv: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub inputs: Vec<Mount>,
    pub tools: Vec<Mount>,
    /// Staging data dir (becomes the store entry's `data/` on publish).
    pub out_dir: PathBuf,
    pub log_dir: PathBuf,
    /// Only meaningful for fixed-output jobs (§7.6).
    pub allow_network: bool,
    /// cwd = `/ppg/out` when true.
    pub cwd_out: bool,
    /// The §5 `runtime` object (`{"python_env":..., "preload":[...],
    /// "shim":...}`), copied verbatim from `JobDef.runtime` by the
    /// scheduler. Additive field (forkserver work package, see
    /// STATUS.md): `NoneExecutor`/`BwrapExecutor` ignore it entirely;
    /// `ForkserverExecutor` (`forkserver.rs`) uses it (together with an
    /// argv shape check) to decide whether a job is eligible for template
    /// dispatch. `None` for any caller that predates this field (e.g. the
    /// unit tests in this module) — always `Some` for jobs built by
    /// `scheduler::dispatch_argv_job`.
    pub runtime: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct ExecResult {
    pub exit_code: i32,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

pub trait Executor: Send + Sync {
    fn run(&self, job: &PreparedJob) -> Result<ExecResult>;

    /// Whether this executor provides real sandbox enforcement (bwrap /
    /// unshare) vs. best-effort staging only (`NoneExecutor`). Additive
    /// default (not in the original one-line CONTRACT.md trait sketch) so
    /// callers — the scheduler, when filling in `BuiltInfo.sandboxed` — can
    /// ask without a downcast. Default `false` matches `NoneExecutor`.
    fn is_sandboxed(&self) -> bool {
        false
    }
}

fn ppg_name(virtual_path: &str) -> Result<&str> {
    Path::new(virtual_path)
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| Error::Other(format!("malformed virtual mount path: {virtual_path:?}")))
}

// ============================================================ NoneExecutor

static WARN_ONCE: Once = Once::new();

fn warn_once() {
    WARN_ONCE.call_once(|| {
        eprintln!("sandbox=none: running without enforcement");
    });
}

static WORKDIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_name(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let c = WORKDIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{nanos:x}-{c:x}")
}

/// The result of staging a `PreparedJob` into a work directory (shared by
/// `NoneExecutor` and `ForkserverExecutor` — see `stage`/`finish` below).
pub(crate) struct StagedJob {
    /// The per-job staged root (`<work_parent>/<ik>-...`); removed by
    /// `finish` on success.
    pub work_dir: PathBuf,
    /// `job.argv` with every `/ppg/` occurrence rewritten to
    /// `<work_dir>/ppg/`.
    pub argv: Vec<String>,
    /// `job.env` plus `TMPDIR`/`HOME` defaults, same rewrite applied.
    pub env: BTreeMap<String, String>,
    /// `<work_dir>/ppg/out` if `job.cwd_out`, else `work_dir`.
    pub cwd: PathBuf,
    /// Unchanged from `job.log_dir` (never staged/rewritten — §6.1a, the
    /// non-reproducible log channel already lives at its real path).
    pub log_dir: PathBuf,
}

fn build_layout(job: &PreparedJob, work: &Path) -> Result<()> {
    let ppg = work.join("ppg");
    for sub in ["in", "tools", "tmp"] {
        let dir = ppg.join(sub);
        std::fs::create_dir_all(&dir).map_err(|e| Error::io(&dir, e))?;
    }
    for m in &job.inputs {
        let name = ppg_name(&m.virtual_path)?;
        let link = ppg.join("in").join(name);
        std::os::unix::fs::symlink(&m.source, &link).map_err(|e| Error::io(&link, e))?;
    }
    for m in &job.tools {
        let name = ppg_name(&m.virtual_path)?;
        let link = ppg.join("tools").join(name);
        std::os::unix::fs::symlink(&m.source, &link).map_err(|e| Error::io(&link, e))?;
    }
    std::fs::create_dir_all(&job.out_dir).map_err(|e| Error::io(&job.out_dir, e))?;
    std::fs::create_dir_all(&job.log_dir).map_err(|e| Error::io(&job.log_dir, e))?;
    let out_link = ppg.join("out");
    std::os::unix::fs::symlink(&job.out_dir, &out_link).map_err(|e| Error::io(&out_link, e))?;
    let log_link = ppg.join("log");
    std::os::unix::fs::symlink(&job.log_dir, &log_link).map_err(|e| Error::io(&log_link, e))?;
    Ok(())
}

fn rewrite_ppg_path(s: &str, work: &Path) -> String {
    let replacement = format!("{}/ppg/", work.display());
    s.replace("/ppg/", &replacement)
}

/// Stage `job` into a fresh directory under `work_parent`: builds the
/// `<work>/ppg/{in,tools,out,log,tmp}` symlink layout (standing in for bind
/// mounts, §6.1 — no enforcement), then rewrites every `/ppg/...`
/// occurrence in `argv`/`env` to the real staged path. Shared by
/// `NoneExecutor` and `forkserver::ForkserverExecutor` (both run children
/// in the same unenforced staged layout, per the forkserver work package's
/// scope constraint — see STATUS.md). Does not spawn anything.
pub(crate) fn stage(job: &PreparedJob, work_parent: &Path) -> Result<StagedJob> {
    let work = work_parent.join(unique_name(&job.ik));
    build_layout(job, &work)?;

    let argv: Vec<String> = job
        .argv
        .iter()
        .map(|s| rewrite_ppg_path(s, &work))
        .collect();

    let mut env = job.env.clone();
    env.entry("TMPDIR".to_string())
        .or_insert_with(|| "/ppg/tmp".to_string());
    env.entry("HOME".to_string())
        .or_insert_with(|| "/ppg/tmp".to_string());
    let env: BTreeMap<String, String> = env
        .into_iter()
        .map(|(k, v)| (k, rewrite_ppg_path(&v, &work)))
        .collect();

    let cwd = if job.cwd_out {
        work.join("ppg").join("out")
    } else {
        work.clone()
    };

    Ok(StagedJob {
        work_dir: work,
        argv,
        env,
        cwd,
        log_dir: job.log_dir.clone(),
    })
}

/// Post-run cleanup: remove the staged work dir on success, keep it (for
/// postmortem) on failure. Mirrors `NoneExecutor`'s original behavior
/// exactly.
pub(crate) fn finish(staged: &StagedJob, success: bool) {
    if success {
        let _ = std::fs::remove_dir_all(&staged.work_dir);
    }
}

/// Staged-directory fallback executor (no user namespaces). Tested path in
/// this dev container per CONTRACT.md's "Scope deviations".
pub struct NoneExecutor {
    work_parent: PathBuf,
}

impl NoneExecutor {
    /// `work_parent`: directory under which per-job staged work dirs are
    /// created (and normally cleaned up on success). Created if missing.
    pub fn new(work_parent: impl Into<PathBuf>) -> Self {
        NoneExecutor {
            work_parent: work_parent.into(),
        }
    }
}

impl Executor for NoneExecutor {
    fn run(&self, job: &PreparedJob) -> Result<ExecResult> {
        warn_once();
        let staged = stage(job, &self.work_parent)?;

        if staged.argv.is_empty() {
            return Err(Error::Other("PreparedJob.argv is empty".to_string()));
        }

        let mut cmd = Command::new(&staged.argv[0]);
        cmd.args(&staged.argv[1..])
            .current_dir(&staged.cwd)
            .env_clear()
            .envs(&staged.env)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let output = cmd
            .output()
            .map_err(|e| Error::Other(format!("spawning {:?} failed: {e}", staged.argv[0])))?;

        let stdout_path = staged.log_dir.join("stdout.txt");
        let stderr_path = staged.log_dir.join("stderr.txt");
        write_log_file(&stdout_path, &output.stdout)?;
        write_log_file(&stderr_path, &output.stderr)?;

        let exit_code = output.status.code().unwrap_or_else(|| {
            #[cfg(unix)]
            {
                use std::os::unix::process::ExitStatusExt;
                output
                    .status
                    .signal()
                    .map(|sig| 128 + sig)
                    .unwrap_or(-1)
            }
            #[cfg(not(unix))]
            {
                -1
            }
        });

        finish(&staged, exit_code == 0);

        Ok(ExecResult {
            exit_code,
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }
}

fn write_log_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut f = std::fs::File::create(path).map_err(|e| Error::io(path, e))?;
    f.write_all(bytes).map_err(|e| Error::io(path, e))?;
    Ok(())
}

// =============================================================== bwrap

/// Pure function: the full `bwrap` invocation for `job` (argv[0] is
/// `bwrap`'s own path). No process is spawned — this makes the
/// (security-relevant) argv construction fully unit-testable without
/// `bwrap` installed (not available in this dev container).
pub fn bwrap_argv(job: &PreparedJob, bwrap: &Path) -> Vec<String> {
    let mut argv = Vec::new();
    argv.push(bwrap.to_string_lossy().into_owned());
    argv.push("--unshare-all".to_string());
    if job.allow_network {
        argv.push("--share-net".to_string());
    }
    for m in &job.inputs {
        argv.push("--ro-bind".to_string());
        argv.push(m.source.to_string_lossy().into_owned());
        argv.push(m.virtual_path.clone());
    }
    for m in &job.tools {
        argv.push("--ro-bind".to_string());
        argv.push(m.source.to_string_lossy().into_owned());
        argv.push(m.virtual_path.clone());
        // Nix binaries hardcode /nix/store paths, so tools sourced from the
        // nix store are additionally bound at their real path (§6.1).
        let source_str = m.source.to_string_lossy();
        if source_str.starts_with("/nix/store") {
            argv.push("--ro-bind".to_string());
            argv.push(source_str.into_owned());
            argv.push(m.source.to_string_lossy().into_owned());
        }
    }
    argv.push("--bind".to_string());
    argv.push(job.out_dir.to_string_lossy().into_owned());
    argv.push("/ppg/out".to_string());
    argv.push("--bind".to_string());
    argv.push(job.log_dir.to_string_lossy().into_owned());
    argv.push("/ppg/log".to_string());
    argv.push("--tmpfs".to_string());
    argv.push("/tmp".to_string());
    for dev in ["/dev/null", "/dev/zero", "/dev/urandom"] {
        argv.push("--dev-bind-try".to_string());
        argv.push(dev.to_string());
        argv.push(dev.to_string());
    }
    argv.push("--chdir".to_string());
    argv.push(if job.cwd_out { "/ppg/out" } else { "/" }.to_string());
    argv.push("--clearenv".to_string());
    for (k, v) in &job.env {
        argv.push("--setenv".to_string());
        argv.push(k.clone());
        argv.push(v.clone());
    }
    argv.push("--".to_string());
    argv.extend(job.argv.iter().cloned());
    argv
}

/// Real bubblewrap executor. Not runtime-tested in this container (no
/// `bwrap` binary); its integration test skips itself when `bwrap` is not
/// on `PATH`.
pub struct BwrapExecutor {
    pub bwrap_path: PathBuf,
}

impl BwrapExecutor {
    pub fn new(bwrap_path: impl Into<PathBuf>) -> Self {
        BwrapExecutor {
            bwrap_path: bwrap_path.into(),
        }
    }
}

impl Executor for BwrapExecutor {
    fn run(&self, job: &PreparedJob) -> Result<ExecResult> {
        std::fs::create_dir_all(&job.out_dir).map_err(|e| Error::io(&job.out_dir, e))?;
        std::fs::create_dir_all(&job.log_dir).map_err(|e| Error::io(&job.log_dir, e))?;
        let full_argv = bwrap_argv(job, &self.bwrap_path);
        let mut cmd = Command::new(&full_argv[0]);
        cmd.args(&full_argv[1..])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let output = cmd
            .output()
            .map_err(|e| Error::Other(format!("spawning bwrap failed: {e}")))?;
        let stdout_path = job.log_dir.join("stdout.txt");
        let stderr_path = job.log_dir.join("stderr.txt");
        write_log_file(&stdout_path, &output.stdout)?;
        write_log_file(&stderr_path, &output.stderr)?;
        Ok(ExecResult {
            exit_code: output.status.code().unwrap_or(-1),
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }

    fn is_sandboxed(&self) -> bool {
        true
    }
}

/// True if `bwrap` is discoverable on `PATH` — integration tests use this
/// to skip (not fail) when the sandbox binary isn't installed, matching
/// CONTRACT.md's "no bwrap, no nix — skip, don't fail" testing bar.
pub fn bwrap_available() -> bool {
    std::env::var_os("PATH")
        .map(|paths| {
            std::env::split_paths(&paths).any(|dir| dir.join("bwrap").is_file())
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_job() -> PreparedJob {
        let mut env = BTreeMap::new();
        env.insert("PATH".to_string(), "/ppg/tools/py/bin".to_string());
        env.insert("HOME".to_string(), "/tmp".to_string());
        PreparedJob {
            ik: "i".repeat(64),
            argv: vec!["/ppg/tools/py/bin/python3".to_string(), "-c".to_string(), "pass".to_string()],
            env,
            inputs: vec![Mount {
                virtual_path: "/ppg/in/data".to_string(),
                source: PathBuf::from("/store/entries/aaa/data"),
            }],
            tools: vec![
                Mount {
                    virtual_path: "/ppg/tools/py".to_string(),
                    source: PathBuf::from("/usr/bin"),
                },
                Mount {
                    virtual_path: "/ppg/tools/nixtool".to_string(),
                    source: PathBuf::from("/nix/store/abc123-tool"),
                },
            ],
            out_dir: PathBuf::from("/store/staging/xyz/data"),
            log_dir: PathBuf::from("/store/logs/ik1/ts-host"),
            allow_network: false,
            cwd_out: true,
            runtime: None,
        }
    }

    #[test]
    fn bwrap_argv_starts_with_bwrap_path_and_unshare_all() {
        let job = sample_job();
        let argv = bwrap_argv(&job, Path::new("/usr/bin/bwrap"));
        assert_eq!(argv[0], "/usr/bin/bwrap");
        assert_eq!(argv[1], "--unshare-all");
    }

    #[test]
    fn bwrap_argv_network_flag_off_by_default() {
        let job = sample_job();
        let argv = bwrap_argv(&job, Path::new("/usr/bin/bwrap"));
        assert!(!argv.contains(&"--share-net".to_string()));
    }

    #[test]
    fn bwrap_argv_network_flag_present_when_allowed() {
        let mut job = sample_job();
        job.allow_network = true;
        let argv = bwrap_argv(&job, Path::new("/usr/bin/bwrap"));
        // must appear right after --unshare-all, before any binds
        let idx = argv.iter().position(|s| s == "--share-net").unwrap();
        assert_eq!(argv[idx - 1], "--unshare-all");
        let first_bind = argv.iter().position(|s| s == "--ro-bind").unwrap();
        assert!(idx < first_bind);
    }

    #[test]
    fn bwrap_argv_binds_inputs_and_tools() {
        let job = sample_job();
        let argv = bwrap_argv(&job, Path::new("/usr/bin/bwrap"));
        let joined = argv.join("\u{1}");
        assert!(joined.contains(&"--ro-bind\u{1}/store/entries/aaa/data\u{1}/ppg/in/data".to_string()));
        assert!(joined.contains(&"--ro-bind\u{1}/usr/bin\u{1}/ppg/tools/py".to_string()));
    }

    #[test]
    fn bwrap_argv_double_mounts_nix_store_tools() {
        let job = sample_job();
        let argv = bwrap_argv(&job, Path::new("/usr/bin/bwrap"));
        // both the virtual mount and the real /nix/store path must appear
        let virt_idx = argv
            .windows(3)
            .position(|w| {
                w[0] == "--ro-bind"
                    && w[1] == "/nix/store/abc123-tool"
                    && w[2] == "/ppg/tools/nixtool"
            })
            .expect("virtual nix mount present");
        let real_idx = argv
            .windows(3)
            .position(|w| {
                w[0] == "--ro-bind"
                    && w[1] == "/nix/store/abc123-tool"
                    && w[2] == "/nix/store/abc123-tool"
            })
            .expect("real /nix/store double-mount present");
        assert!(real_idx > virt_idx);
        // the non-nix tool must NOT be double-mounted
        let count_usr_bin = argv.iter().filter(|s| s.as_str() == "/usr/bin").count();
        assert_eq!(count_usr_bin, 1);
    }

    #[test]
    fn bwrap_argv_out_and_log_and_tmpfs_and_dev() {
        let job = sample_job();
        let argv = bwrap_argv(&job, Path::new("/usr/bin/bwrap"));
        let joined = argv.join("\u{1}");
        assert!(joined.contains("--bind\u{1}/store/staging/xyz/data\u{1}/ppg/out"));
        assert!(joined.contains("--bind\u{1}/store/logs/ik1/ts-host\u{1}/ppg/log"));
        assert!(joined.contains("--tmpfs\u{1}/tmp"));
        assert!(joined.contains("--dev-bind-try\u{1}/dev/null\u{1}/dev/null"));
        assert!(joined.contains("--dev-bind-try\u{1}/dev/zero\u{1}/dev/zero"));
        assert!(joined.contains("--dev-bind-try\u{1}/dev/urandom\u{1}/dev/urandom"));
    }

    #[test]
    fn bwrap_argv_chdir_respects_cwd_out() {
        let mut job = sample_job();
        job.cwd_out = true;
        let argv = bwrap_argv(&job, Path::new("/usr/bin/bwrap"));
        let idx = argv.iter().position(|s| s == "--chdir").unwrap();
        assert_eq!(argv[idx + 1], "/ppg/out");

        job.cwd_out = false;
        let argv = bwrap_argv(&job, Path::new("/usr/bin/bwrap"));
        let idx = argv.iter().position(|s| s == "--chdir").unwrap();
        assert_eq!(argv[idx + 1], "/");
    }

    #[test]
    fn bwrap_argv_clearenv_then_setenv_pairs_then_double_dash_then_job_argv() {
        let job = sample_job();
        let argv = bwrap_argv(&job, Path::new("/usr/bin/bwrap"));
        let clearenv_idx = argv.iter().position(|s| s == "--clearenv").unwrap();
        let dashdash_idx = argv.iter().position(|s| s == "--").unwrap();
        assert!(clearenv_idx < dashdash_idx);
        // every declared env var appears as a --setenv pair between clearenv and --
        for (k, v) in &job.env {
            let idx = argv.iter().position(|s| s == "--setenv").map(|i| {
                // find the specific pair (there may be several --setenv)
                argv.windows(3)
                    .position(|w| w[0] == "--setenv" && &w[1] == k && &w[2] == v)
                    .unwrap_or(i)
            });
            let idx = idx.unwrap();
            assert!(idx > clearenv_idx && idx < dashdash_idx);
        }
        assert_eq!(&argv[dashdash_idx + 1..], job.argv.as_slice());
    }

    #[test]
    fn bwrap_argv_setenv_sorted_by_key() {
        let job = sample_job();
        let argv = bwrap_argv(&job, Path::new("/usr/bin/bwrap"));
        let keys: Vec<&String> = argv
            .windows(3)
            .filter(|w| w[0] == "--setenv")
            .map(|w| &w[1])
            .collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted);
    }

    // ---------------------------------------------------- NoneExecutor ---

    fn none_job(work_out: &Path, work_log: &Path, argv: Vec<String>, extra_env: &[(&str, &str)]) -> PreparedJob {
        let mut env = BTreeMap::new();
        for (k, v) in extra_env {
            env.insert(k.to_string(), v.to_string());
        }
        PreparedJob {
            ik: "j".repeat(64),
            argv,
            env,
            inputs: vec![],
            tools: vec![],
            out_dir: work_out.to_path_buf(),
            log_dir: work_log.to_path_buf(),
            allow_network: false,
            cwd_out: true,
            runtime: None,
        }
    }

    #[test]
    fn none_executor_runs_real_sh_and_writes_output_file() {
        let parent = tempfile::tempdir().unwrap();
        let out = tempfile::tempdir().unwrap();
        let log = tempfile::tempdir().unwrap();
        let exec = NoneExecutor::new(parent.path());
        let job = none_job(
            out.path(),
            log.path(),
            vec![
                "/bin/sh".to_string(),
                "-c".to_string(),
                "echo hello > {out}/greeting.txt".to_string(),
            ],
            &[],
        );
        // substitute {out} the way the scheduler would before calling the executor
        let mut job = job;
        job.argv = job
            .argv
            .iter()
            .map(|s| s.replace("{out}", "/ppg/out"))
            .collect();
        let result = exec.run(&job).unwrap();
        assert_eq!(result.exit_code, 0, "stderr: {}", String::from_utf8_lossy(&result.stderr));
        let content = std::fs::read_to_string(out.path().join("greeting.txt")).unwrap();
        assert_eq!(content, "hello\n");
        assert!(log.path().join("stdout.txt").is_file());
        assert!(log.path().join("stderr.txt").is_file());
    }

    #[test]
    fn none_executor_scrubs_env_to_declared_set_plus_defaults() {
        let parent = tempfile::tempdir().unwrap();
        let out = tempfile::tempdir().unwrap();
        let log = tempfile::tempdir().unwrap();
        let exec = NoneExecutor::new(parent.path());
        let job = none_job(
            out.path(),
            log.path(),
            vec!["/usr/bin/env".to_string()],
            &[("MY_VAR", "hello")],
        );
        let result = exec.run(&job).unwrap();
        assert_eq!(result.exit_code, 0);
        let out_str = String::from_utf8_lossy(&result.stdout);
        let mut seen: BTreeMap<String, String> = BTreeMap::new();
        for line in out_str.lines() {
            if let Some((k, v)) = line.split_once('=') {
                seen.insert(k.to_string(), v.to_string());
            }
        }
        assert_eq!(seen.get("MY_VAR").map(String::as_str), Some("hello"));
        assert!(seen.contains_key("TMPDIR"));
        assert!(seen.contains_key("HOME"));
        // exactly declared + TMPDIR + HOME - nothing leaked from the host
        // process' own environment (e.g. no inherited PATH unless declared).
        assert_eq!(seen.len(), 3, "unexpected leaked env vars: {seen:?}");
    }

    #[test]
    fn none_executor_rewrites_ppg_paths_in_argv_and_env() {
        let parent = tempfile::tempdir().unwrap();
        let out = tempfile::tempdir().unwrap();
        let log = tempfile::tempdir().unwrap();
        let exec = NoneExecutor::new(parent.path());
        let job = none_job(
            out.path(),
            log.path(),
            vec![
                "/bin/sh".to_string(),
                "-c".to_string(),
                "echo \"$MARK\" > /ppg/out/marker.txt".to_string(),
            ],
            &[("MARK", "from-env")],
        );
        let result = exec.run(&job).unwrap();
        assert_eq!(result.exit_code, 0, "stderr: {}", String::from_utf8_lossy(&result.stderr));
        let content = std::fs::read_to_string(out.path().join("marker.txt")).unwrap();
        assert_eq!(content, "from-env\n");
    }

    #[test]
    fn none_executor_cleans_up_workdir_on_success_keeps_on_failure() {
        let parent = tempfile::tempdir().unwrap();
        let out_ok = tempfile::tempdir().unwrap();
        let log_ok = tempfile::tempdir().unwrap();
        let exec = NoneExecutor::new(parent.path());
        let ok_job = none_job(out_ok.path(), log_ok.path(), vec!["/bin/true".to_string()], &[]);
        exec.run(&ok_job).unwrap();
        let before_fail: Vec<_> = std::fs::read_dir(parent.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        assert!(before_fail.is_empty(), "success must clean up its work dir");

        let out_fail = tempfile::tempdir().unwrap();
        let log_fail = tempfile::tempdir().unwrap();
        let fail_job = none_job(out_fail.path(), log_fail.path(), vec!["/bin/false".to_string()], &[]);
        let result = exec.run(&fail_job).unwrap();
        assert_ne!(result.exit_code, 0);
        let after_fail: Vec<_> = std::fs::read_dir(parent.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        assert_eq!(after_fail.len(), 1, "failure must keep its work dir for postmortem");
    }
}

// ================================================== bwrap integration ---
// (kept out of #[cfg(test)] mod tests above so it can live in the crate's
// normal test target alongside the unit tests, but gated so it never runs
// without bwrap.)
#[cfg(test)]
mod bwrap_integration {
    use super::*;

    #[test]
    fn bwrap_executor_smoke_if_available() {
        if !bwrap_available() {
            eprintln!("skipping: bwrap not installed in this environment");
            return;
        }
        let out = tempfile::tempdir().unwrap();
        let log = tempfile::tempdir().unwrap();
        let job = PreparedJob {
            ik: "k".repeat(64),
            argv: vec!["/bin/true".to_string()],
            env: BTreeMap::new(),
            inputs: vec![],
            tools: vec![],
            out_dir: out.path().to_path_buf(),
            log_dir: log.path().to_path_buf(),
            allow_network: false,
            cwd_out: true,
            runtime: None,
        };
        let exec = BwrapExecutor::new("bwrap");
        let result = exec.run(&job).unwrap();
        assert_eq!(result.exit_code, 0);
    }
}
