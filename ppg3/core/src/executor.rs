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
//!   is exercised by integration tests that skip themselves when `bwrap`
//!   is not on `PATH` or user namespaces are unavailable at runtime.
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

    fn build_layout(&self, job: &PreparedJob) -> Result<PathBuf> {
        let work = self.work_parent.join(unique_name(&job.ik));
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
        Ok(work)
    }

    fn rewrite(s: &str, work: &Path) -> String {
        let replacement = format!("{}/ppg/", work.display());
        s.replace("/ppg/", &replacement)
    }
}

impl Executor for NoneExecutor {
    fn run(&self, job: &PreparedJob) -> Result<ExecResult> {
        warn_once();
        let work = self.build_layout(job)?;

        if job.argv.is_empty() {
            return Err(Error::Other("PreparedJob.argv is empty".to_string()));
        }
        let argv: Vec<String> = job.argv.iter().map(|s| Self::rewrite(s, &work)).collect();

        let mut env = job.env.clone();
        env.entry("TMPDIR".to_string())
            .or_insert_with(|| "/ppg/tmp".to_string());
        env.entry("HOME".to_string())
            .or_insert_with(|| "/ppg/tmp".to_string());
        let env: BTreeMap<String, String> = env
            .into_iter()
            .map(|(k, v)| (k, Self::rewrite(&v, &work)))
            .collect();

        let cwd = if job.cwd_out {
            work.join("ppg").join("out")
        } else {
            work.clone()
        };

        let mut cmd = Command::new(&argv[0]);
        cmd.args(&argv[1..])
            .current_dir(&cwd)
            .env_clear()
            .envs(&env)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let output = cmd
            .output()
            .map_err(|e| Error::Other(format!("spawning {:?} failed: {e}", argv[0])))?;

        let stdout_path = job.log_dir.join("stdout.txt");
        let stderr_path = job.log_dir.join("stderr.txt");
        write_log_file(&stdout_path, &output.stdout)?;
        write_log_file(&stderr_path, &output.stderr)?;

        let exit_code = exit_code_of(&output.status);

        if exit_code == 0 {
            let _ = std::fs::remove_dir_all(&work);
        }

        Ok(ExecResult {
            exit_code,
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }
}

fn exit_code_of(status: &std::process::ExitStatus) -> i32 {
    status.code().unwrap_or_else(|| {
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            status.signal().map(|sig| 128 + sig).unwrap_or(-1)
        }
        #[cfg(not(unix))]
        {
            -1
        }
    })
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
/// `bwrap` installed.
pub fn bwrap_argv(job: &PreparedJob, bwrap: &Path) -> Vec<String> {
    let mut argv = Vec::new();
    argv.push(bwrap.to_string_lossy().into_owned());
    argv.push("--unshare-all".to_string());
    if job.allow_network {
        argv.push("--share-net".to_string());
    }
    argv.push("--die-with-parent".to_string());
    for m in &job.inputs {
        argv.push("--ro-bind".to_string());
        argv.push(m.source.to_string_lossy().into_owned());
        argv.push(m.virtual_path.clone());
    }
    let mut any_nix_tool = false;
    for m in &job.tools {
        argv.push("--ro-bind".to_string());
        argv.push(m.source.to_string_lossy().into_owned());
        argv.push(m.virtual_path.clone());
        any_nix_tool |= m.source.starts_with("/nix/store");
    }
    // Nix binaries hardcode /nix/store paths (§6.1) — not just their own
    // store path but their whole dependency closure (ld-linux, libc, ...),
    // which is not enumerable from the Mount alone. Binding only the
    // tool's own path fails at execvp (verified: the ELF interpreter
    // lives in a different store path). Bind the whole store read-only.
    if any_nix_tool {
        argv.push("--ro-bind".to_string());
        argv.push("/nix/store".to_string());
        argv.push("/nix/store".to_string());
    }
    // /ppg/{in,tools} must exist even for jobs with no inputs/tools so the
    // virtual layout is uniform across executors (NoneExecutor always
    // creates them).
    argv.push("--dir".to_string());
    argv.push("/ppg/in".to_string());
    argv.push("--dir".to_string());
    argv.push("/ppg/tools".to_string());
    argv.push("--bind".to_string());
    argv.push(job.out_dir.to_string_lossy().into_owned());
    argv.push("/ppg/out".to_string());
    argv.push("--bind".to_string());
    argv.push(job.log_dir.to_string_lossy().into_owned());
    argv.push("/ppg/log".to_string());
    argv.push("--tmpfs".to_string());
    argv.push("/tmp".to_string());
    argv.push("--tmpfs".to_string());
    argv.push("/ppg/tmp".to_string());
    argv.push("--proc".to_string());
    argv.push("/proc".to_string());
    for dev in ["/dev/null", "/dev/zero", "/dev/urandom"] {
        argv.push("--dev-bind-try".to_string());
        argv.push(dev.to_string());
        argv.push(dev.to_string());
    }
    argv.push("--chdir".to_string());
    argv.push(if job.cwd_out { "/ppg/out" } else { "/" }.to_string());
    argv.push("--clearenv".to_string());
    let mut env = job.env.clone();
    // Same TMPDIR/HOME defaulting as NoneExecutor, pointing at the
    // sandbox-side tmpfs.
    env.entry("TMPDIR".to_string())
        .or_insert_with(|| "/ppg/tmp".to_string());
    env.entry("HOME".to_string())
        .or_insert_with(|| "/ppg/tmp".to_string());
    for (k, v) in &env {
        argv.push("--setenv".to_string());
        argv.push(k.clone());
        argv.push(v.clone());
    }
    argv.push("--".to_string());
    argv.extend(job.argv.iter().cloned());
    argv
}

/// Real bubblewrap executor. Its integration tests skip themselves when
/// `bwrap` is not on `PATH` or user namespaces are unavailable.
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
            exit_code: exit_code_of(&output.status),
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
        .map(|paths| std::env::split_paths(&paths).any(|dir| dir.join("bwrap").is_file()))
        .unwrap_or(false)
}

/// True if `bwrap` is installed *and* can actually create its namespaces
/// here — the binary being present does not imply unprivileged user
/// namespaces are enabled (seccomp/sysctl can block them), so this probes
/// with a trivial sandboxed command.
pub fn bwrap_runtime_available() -> bool {
    if !bwrap_available() {
        return false;
    }
    Command::new("bwrap")
        .args([
            "--unshare-all",
            "--ro-bind",
            "/",
            "/",
            "/bin/sh",
            "-c",
            "true",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
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
            argv: vec![
                "/ppg/tools/py/bin/python3".to_string(),
                "-c".to_string(),
                "pass".to_string(),
            ],
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
        assert!(
            joined.contains(&"--ro-bind\u{1}/store/entries/aaa/data\u{1}/ppg/in/data".to_string())
        );
        assert!(joined.contains(&"--ro-bind\u{1}/usr/bin\u{1}/ppg/tools/py".to_string()));
    }

    #[test]
    fn bwrap_argv_binds_whole_nix_store_for_nix_tools() {
        let job = sample_job();
        let argv = bwrap_argv(&job, Path::new("/usr/bin/bwrap"));
        // the virtual mount and a single whole-store bind must appear (the
        // tool's dependency closure — ld-linux, libc, ... — lives in other
        // store paths, so binding only the tool's own path cannot work)
        let virt_idx = argv
            .windows(3)
            .position(|w| {
                w[0] == "--ro-bind"
                    && w[1] == "/nix/store/abc123-tool"
                    && w[2] == "/ppg/tools/nixtool"
            })
            .expect("virtual nix mount present");
        let store_idx = argv
            .windows(3)
            .position(|w| w[0] == "--ro-bind" && w[1] == "/nix/store" && w[2] == "/nix/store")
            .expect("whole /nix/store bind present");
        assert!(store_idx > virt_idx);
        // exactly one store bind, and the non-nix tool is not double-mounted
        let store_binds = argv.iter().filter(|s| s.as_str() == "/nix/store").count();
        assert_eq!(store_binds, 2); // one --ro-bind pair
        let count_usr_bin = argv.iter().filter(|s| s.as_str() == "/usr/bin").count();
        assert_eq!(count_usr_bin, 1);
    }

    #[test]
    fn bwrap_argv_no_nix_store_bind_without_nix_tools() {
        let mut job = sample_job();
        job.tools.retain(|m| !m.source.starts_with("/nix/store"));
        let argv = bwrap_argv(&job, Path::new("/usr/bin/bwrap"));
        assert!(!argv.iter().any(|s| s == "/nix/store"));
    }

    #[test]
    fn bwrap_argv_defaults_tmpdir_and_home_without_overriding_declared() {
        let job = sample_job(); // declares HOME=/tmp, no TMPDIR
        let argv = bwrap_argv(&job, Path::new("/usr/bin/bwrap"));
        let pair = |k: &str| {
            argv.windows(3)
                .find(|w| w[0] == "--setenv" && w[1] == k)
                .map(|w| w[2].clone())
        };
        assert_eq!(pair("TMPDIR").as_deref(), Some("/ppg/tmp"));
        assert_eq!(
            pair("HOME").as_deref(),
            Some("/tmp"),
            "declared HOME must win"
        );
    }

    #[test]
    fn bwrap_argv_out_and_log_and_tmpfs_and_dev() {
        let job = sample_job();
        let argv = bwrap_argv(&job, Path::new("/usr/bin/bwrap"));
        let joined = argv.join("\u{1}");
        assert!(joined.contains("--bind\u{1}/store/staging/xyz/data\u{1}/ppg/out"));
        assert!(joined.contains("--bind\u{1}/store/logs/ik1/ts-host\u{1}/ppg/log"));
        assert!(joined.contains("--tmpfs\u{1}/tmp"));
        assert!(joined.contains("--tmpfs\u{1}/ppg/tmp"));
        assert!(joined.contains("--proc\u{1}/proc"));
        assert!(joined.contains("--dir\u{1}/ppg/in"));
        assert!(joined.contains("--dir\u{1}/ppg/tools"));
        assert!(joined.contains("--die-with-parent"));
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

    fn none_job(
        work_out: &Path,
        work_log: &Path,
        argv: Vec<String>,
        extra_env: &[(&str, &str)],
    ) -> PreparedJob {
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
        assert_eq!(
            result.exit_code,
            0,
            "stderr: {}",
            String::from_utf8_lossy(&result.stderr)
        );
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
        assert_eq!(
            result.exit_code,
            0,
            "stderr: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        let content = std::fs::read_to_string(out.path().join("marker.txt")).unwrap();
        assert_eq!(content, "from-env\n");
    }

    #[test]
    fn none_executor_cleans_up_workdir_on_success_keeps_on_failure() {
        let parent = tempfile::tempdir().unwrap();
        let out_ok = tempfile::tempdir().unwrap();
        let log_ok = tempfile::tempdir().unwrap();
        let exec = NoneExecutor::new(parent.path());
        // /bin/sh is the only POSIX-guaranteed binary path (this host has
        // no /bin/true — NixOS ships only /bin/sh).
        let ok_job = none_job(
            out_ok.path(),
            log_ok.path(),
            vec!["/bin/sh".to_string(), "-c".to_string(), "true".to_string()],
            &[],
        );
        exec.run(&ok_job).unwrap();
        let before_fail: Vec<_> = std::fs::read_dir(parent.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        assert!(before_fail.is_empty(), "success must clean up its work dir");

        let out_fail = tempfile::tempdir().unwrap();
        let log_fail = tempfile::tempdir().unwrap();
        let fail_job = none_job(
            out_fail.path(),
            log_fail.path(),
            vec!["/bin/sh".to_string(), "-c".to_string(), "false".to_string()],
            &[],
        );
        let result = exec.run(&fail_job).unwrap();
        assert_ne!(result.exit_code, 0);
        let after_fail: Vec<_> = std::fs::read_dir(parent.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        assert_eq!(
            after_fail.len(),
            1,
            "failure must keep its work dir for postmortem"
        );
    }
}

// ================================================== bwrap integration ---
// (kept out of #[cfg(test)] mod tests above so it can live in the crate's
// normal test target alongside the unit tests, but gated so it never runs
// without a working bwrap.)
#[cfg(test)]
mod bwrap_integration {
    use super::*;

    /// The nix package dir of the host's `/bin/sh` (e.g.
    /// `/nix/store/...-bash-5.x`), used as the job's tool mount so the
    /// sandboxed argv is `/ppg/tools/sh/bin/sh`. `None` (⇒ skip) when
    /// bwrap can't run here or `/bin/sh` is not nix-sourced — running a
    /// non-nix shell would need its FHS library dirs mounted at their
    /// real paths, which `PreparedJob` deliberately has no vocabulary for.
    fn setup() -> Option<PathBuf> {
        if !bwrap_runtime_available() {
            eprintln!("skipping: bwrap not installed or user namespaces unavailable");
            return None;
        }
        let real = std::fs::canonicalize("/bin/sh").ok()?;
        if !real.starts_with("/nix/store") {
            eprintln!("skipping: /bin/sh is not from /nix/store; no hermetic tool to mount");
            return None;
        }
        Some(real.parent()?.parent()?.to_path_buf())
    }

    fn sh_job(sh_pkg: &Path, out: &Path, log: &Path, script: &str) -> PreparedJob {
        PreparedJob {
            ik: "k".repeat(64),
            argv: vec![
                "/ppg/tools/sh/bin/sh".to_string(),
                "-c".to_string(),
                script.to_string(),
            ],
            env: BTreeMap::new(),
            inputs: vec![],
            tools: vec![Mount {
                virtual_path: "/ppg/tools/sh".to_string(),
                source: sh_pkg.to_path_buf(),
            }],
            out_dir: out.to_path_buf(),
            log_dir: log.to_path_buf(),
            allow_network: false,
            cwd_out: true,
        }
    }

    #[test]
    fn bwrap_executor_runs_nix_shell_and_writes_output() {
        let Some(sh_pkg) = setup() else { return };
        let out = tempfile::tempdir().unwrap();
        let log = tempfile::tempdir().unwrap();
        let job = sh_job(
            &sh_pkg,
            out.path(),
            log.path(),
            "echo hello > /ppg/out/greeting.txt && pwd",
        );
        let result = BwrapExecutor::new("bwrap").run(&job).unwrap();
        assert_eq!(
            result.exit_code,
            0,
            "stderr: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        let content = std::fs::read_to_string(out.path().join("greeting.txt")).unwrap();
        assert_eq!(content, "hello\n");
        // cwd_out=true ⇒ the job saw /ppg/out as cwd
        assert_eq!(String::from_utf8_lossy(&result.stdout).trim(), "/ppg/out");
        assert!(log.path().join("stdout.txt").is_file());
        assert!(log.path().join("stderr.txt").is_file());
    }

    #[test]
    fn bwrap_executor_input_mounts_are_readable_but_not_writable() {
        let Some(sh_pkg) = setup() else { return };
        let out = tempfile::tempdir().unwrap();
        let log = tempfile::tempdir().unwrap();
        let input = tempfile::tempdir().unwrap();
        std::fs::write(input.path().join("x.txt"), "input-content\n").unwrap();
        let mut job = sh_job(
            &sh_pkg,
            out.path(),
            log.path(),
            // read must succeed, write must fail (read-only bind)
            "read line < /ppg/in/data/x.txt && echo \"$line\" && echo boom > /ppg/in/data/x.txt",
        );
        job.inputs.push(Mount {
            virtual_path: "/ppg/in/data".to_string(),
            source: input.path().to_path_buf(),
        });
        let result = BwrapExecutor::new("bwrap").run(&job).unwrap();
        assert_ne!(result.exit_code, 0, "write to ro input must fail");
        assert_eq!(
            String::from_utf8_lossy(&result.stdout).trim(),
            "input-content",
            "read from input must have succeeded first"
        );
        let on_host = std::fs::read_to_string(input.path().join("x.txt")).unwrap();
        assert_eq!(on_host, "input-content\n", "host file must be untouched");
    }

    #[test]
    fn bwrap_executor_env_is_scrubbed_and_host_fs_invisible() {
        let Some(sh_pkg) = setup() else { return };
        let out = tempfile::tempdir().unwrap();
        let log = tempfile::tempdir().unwrap();
        let mut job = sh_job(
            &sh_pkg,
            out.path(),
            log.path(),
            concat!(
                "echo \"MY=$MY_VAR PATH=${PATH:-unset} TMPDIR=$TMPDIR HOME=$HOME\"; ",
                "if [ -e /etc/passwd ]; then echo etc-visible; else echo etc-hidden; fi; ",
                "echo probe > \"$TMPDIR/w\" && echo tmp-writable"
            ),
        );
        job.env.insert("MY_VAR".to_string(), "hello".to_string());
        let result = BwrapExecutor::new("bwrap").run(&job).unwrap();
        assert_eq!(
            result.exit_code,
            0,
            "stderr: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        let stdout = String::from_utf8_lossy(&result.stdout);
        // PATH is not asserted literally: with PATH absent from the env,
        // nixpkgs bash reports its compiled-in default ("/no-such-path").
        // What matters is that the *host's* PATH value did not leak.
        assert!(
            stdout.contains("MY=hello PATH="),
            "declared env must be present: {stdout}"
        );
        assert!(
            stdout.contains("TMPDIR=/ppg/tmp HOME=/ppg/tmp"),
            "TMPDIR/HOME defaults must point at the sandbox tmpfs: {stdout}"
        );
        let host_path = std::env::var("PATH").unwrap_or_default();
        assert!(
            host_path.is_empty() || !stdout.contains(&host_path),
            "host PATH leaked into the sandbox: {stdout}"
        );
        assert!(
            stdout.contains("etc-hidden"),
            "host fs must be invisible: {stdout}"
        );
        assert!(
            stdout.contains("tmp-writable"),
            "/ppg/tmp must be writable: {stdout}"
        );
    }

    #[test]
    fn bwrap_executor_network_namespace_has_only_loopback() {
        let Some(sh_pkg) = setup() else { return };
        let out = tempfile::tempdir().unwrap();
        let log = tempfile::tempdir().unwrap();
        // /proc/net/dev lists one `name:` line per interface; a fresh net
        // namespace contains exactly `lo`. Pure-shell parse — no coreutils
        // exist inside the sandbox, only the mounted bash package.
        let script = "while read -r l; do case $l in *:*) echo \"$l\";; esac; done < /proc/net/dev";
        let job = sh_job(&sh_pkg, out.path(), log.path(), script);
        let result = BwrapExecutor::new("bwrap").run(&job).unwrap();
        assert_eq!(result.exit_code, 0);
        let stdout = String::from_utf8_lossy(&result.stdout);
        let ifaces: Vec<&str> = stdout
            .lines()
            .filter_map(|l| l.split(':').next())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect();
        assert_eq!(ifaces, vec!["lo"], "expected only loopback, got: {stdout}");
    }
}
