//! `ForkserverExecutor` integration tests (forkserver work package,
//! PPG3_DESIGN.md §6.4, CONTRACT.md "Executor").
//!
//! Drives a real `ForkserverExecutor` against a **minimal, self-contained
//! fake template** written in plain `python3` (no `ppg3` import at all —
//! keeps this crate's tests independent of the `python/` package, which may
//! not even be built when `cargo test -p ppg3-core` runs). The fake
//! template implements exactly the wire protocol `core/src/forkserver.rs`
//! speaks (ready/run/started/exited JSON lines over stdin/stdout) but,
//! instead of actually importing/execing the job's `argv`, drives its
//! forked children entirely off `TEST_*` env vars carried in the job's
//! (staged) `env` — see `FAKE_TEMPLATE_PY` below. That's enough to exercise
//! every piece of executor-side protocol/lifecycle logic (concurrent
//! dispatch to one template, template death mid-job + respawn, the
//! non-shim-argv fallback path) without needing the real Python shim.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ppg3_core::executor::{Executor, NoneExecutor, PreparedJob};
use ppg3_core::forkserver::{ForkserverExecutor, TemplateManager};

const FAKE_TEMPLATE_PY: &str = r#"
import sys, os, json, select, signal, time

def emit(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()

# Deliberately NOT `select.select([sys.stdin], ...)` + `sys.stdin.readline()`:
# TextIOWrapper's internal buffered reader can pull more than one line's
# worth of bytes from the fd in a single underlying read() (e.g. when two
# "run" messages are written back-to-back), after which the *fd itself* has
# no more bytes to offer select() even though a second, complete line is
# still sitting unread in Python's userspace buffer -- select() then times
# out forever and the second message is silently never processed. Verified
# empirically while building this test. Read the raw fd directly instead and
# do our own line buffering, exactly like the real `ppg3._template` (which
# has the identical multi-message-pipelining hazard for real).
_fd = sys.stdin.fileno()
_buf = b""

def try_read_line(timeout):
    global _buf
    nl = _buf.find(b"\n")
    if nl != -1:
        line = _buf[:nl]
        _buf = _buf[nl + 1:]
        return ("line", line.decode("utf-8"))
    r, _, _ = select.select([_fd], [], [], timeout)
    if _fd not in r:
        return ("timeout", None)
    chunk = os.read(_fd, 65536)
    if chunk == b"":
        return ("eof", None)
    _buf += chunk
    nl = _buf.find(b"\n")
    if nl != -1:
        line = _buf[:nl]
        _buf = _buf[nl + 1:]
        return ("line", line.decode("utf-8"))
    return ("timeout", None)

def main():
    args = sys.argv[1:]
    preload = []
    if "--preload" in args:
        idx = args.index("--preload")
        preload = json.loads(args[idx + 1])

    emit({"ready": True, "pid": os.getpid(), "preloaded": preload})

    children = {}
    while True:
        status, line = try_read_line(0.2)
        if status == "eof":
            for pid in list(children):
                try:
                    os.kill(pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
            sys.exit(0)
        if status == "line":
            line = line.strip()
            if line:
                msg = json.loads(line)
                run = msg.get("run")
                if run is not None:
                    rid = run["id"]
                    env = run.get("env", {})
                    cwd = run.get("cwd", ".")
                    stdout_path = run["stdout_path"]
                    stderr_path = run["stderr_path"]
                    if env.get("TEST_SELFDESTRUCT") == "1":
                        os._exit(1)
                    pid = os.fork()
                    if pid == 0:
                        try:
                            os.setsid()
                        except OSError:
                            pass
                        try:
                            out_fd = os.open(stdout_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
                            os.dup2(out_fd, 1)
                            os.close(out_fd)
                            err_fd = os.open(stderr_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
                            os.dup2(err_fd, 2)
                            os.close(err_fd)
                            try:
                                os.chdir(cwd)
                            except OSError:
                                pass
                            sleep_s = float(env.get("TEST_SLEEP", "0"))
                            if sleep_s:
                                time.sleep(sleep_s)
                            marker = env.get("TEST_STDOUT", "")
                            if marker:
                                sys.stdout.write(marker)
                            if env.get("TEST_WRITE_PPID") == "1":
                                # No exec happened between the template's
                                # `fork()` and here, so the forked child's
                                # parent pid *is* the template process' own
                                # pid -- lets cross-run tests confirm "same
                                # template served both dispatches" (or, after
                                # `TemplateManager::shutdown()`, "a fresh
                                # template was spawned") without needing any
                                # extra wire-protocol surface.
                                sys.stdout.write(str(os.getppid()))
                            sys.stdout.flush()
                            code = int(env.get("TEST_EXIT_CODE", "0"))
                        except Exception:
                            os._exit(1)
                        os._exit(code)
                    else:
                        children[pid] = rid
                        emit({"started": {"id": rid, "pid": pid}})
        while True:
            try:
                wpid, status = os.waitpid(-1, os.WNOHANG)
            except ChildProcessError:
                break
            if wpid == 0:
                break
            rid = children.pop(wpid, None)
            if rid is None:
                continue
            if os.WIFEXITED(status):
                code = os.WEXITSTATUS(status)
            elif os.WIFSIGNALED(status):
                code = 128 + os.WTERMSIG(status)
            else:
                code = -1
            emit({"exited": {"id": rid, "pid": wpid, "exit_code": code}})

if __name__ == "__main__":
    main()
"#;

fn python3_available() -> bool {
    std::process::Command::new("python3")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Writes the fake template script into `dir` and returns its path.
fn write_fake_template(dir: &Path) -> PathBuf {
    let path = dir.join("fake_template.py");
    std::fs::write(&path, FAKE_TEMPLATE_PY).unwrap();
    path
}

fn shim_job(ik: &str, out_dir: &Path, log_dir: &Path, extra_env: &[(&str, &str)]) -> PreparedJob {
    shim_job_with_env(ik, out_dir, log_dir, extra_env, "test-py-env")
}

/// Like [`shim_job`] but with a caller-chosen `runtime.python_env` (the §5
/// `PyEnv` tool hash) — used by the template-key tests to prove two jobs
/// that are otherwise identical (same interpreter, same preload list) still
/// get distinct templates when their `python_env` differs.
fn shim_job_with_env(
    ik: &str,
    out_dir: &Path,
    log_dir: &Path,
    extra_env: &[(&str, &str)],
    python_env: &str,
) -> PreparedJob {
    std::fs::create_dir_all(out_dir).unwrap();
    std::fs::create_dir_all(log_dir).unwrap();
    let mut env = BTreeMap::new();
    for (k, v) in extra_env {
        env.insert(k.to_string(), v.to_string());
    }
    PreparedJob {
        ik: ik.to_string(),
        // Must look like a real `python -I -m ppg3._shim ...` invocation
        // for `ForkserverExecutor`'s eligibility check
        // (`forkserver::is_shim_job`) — the fake template itself never
        // execs this argv, it only reacts to `env` (see FAKE_TEMPLATE_PY).
        argv: vec![
            "python3".to_string(),
            "-I".to_string(),
            "-m".to_string(),
            "ppg3._shim".to_string(),
            "--spec-b64".to_string(),
            "AAAA".to_string(),
        ],
        env,
        inputs: vec![],
        tools: vec![],
        out_dir: out_dir.to_path_buf(),
        log_dir: log_dir.to_path_buf(),
        allow_network: false,
        cwd_out: true,
        runtime: Some(serde_json::json!({
            "python_env": python_env,
            "preload": ["decimal"],
            "shim": "1",
        })),
    }
}

fn command_job(out_dir: &Path, log_dir: &Path) -> PreparedJob {
    std::fs::create_dir_all(out_dir).unwrap();
    std::fs::create_dir_all(log_dir).unwrap();
    PreparedJob {
        ik: "c".repeat(64),
        argv: vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            "echo fallback-ran > /ppg/out/marker.txt".to_string(),
        ],
        env: BTreeMap::new(),
        inputs: vec![],
        tools: vec![],
        out_dir: out_dir.to_path_buf(),
        log_dir: log_dir.to_path_buf(),
        allow_network: false,
        cwd_out: true,
        runtime: Some(serde_json::json!({"python_env": null, "preload": [], "shim": "0"})),
    }
}

/// Builds a `TemplateManager` pointed at the fake template script, wrapped
/// in an `Arc` so callers can share it across several `ForkserverExecutor`s
/// the way a coordinator session does (§6.7).
fn make_manager(work_parent: &Path, template_script: &Path) -> Arc<TemplateManager> {
    Arc::new(TemplateManager::new(
        work_parent.to_path_buf(),
        vec!["python3".to_string(), template_script.to_string_lossy().into_owned()],
    ))
}

#[test]
fn two_jobs_dispatch_concurrently_through_one_template() {
    if !python3_available() {
        eprintln!("skipping: python3 not available");
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let template_script = write_fake_template(root.path());
    let work_parent = root.path().join("work");
    let fallback = NoneExecutor::new(&work_parent);
    let manager = make_manager(&work_parent, &template_script);
    let exec = ForkserverExecutor::new(manager, fallback);

    let job_a = shim_job(
        &"a".repeat(64),
        &root.path().join("out-a"),
        &root.path().join("log-a"),
        &[("TEST_STDOUT", "marker-a"), ("TEST_SLEEP", "0.4")],
    );
    let job_b = shim_job(
        &"b".repeat(64),
        &root.path().join("out-b"),
        &root.path().join("log-b"),
        &[("TEST_STDOUT", "marker-b"), ("TEST_SLEEP", "0.4")],
    );

    let start = Instant::now();
    let result = std::thread::scope(|scope| {
        let exec_ref = &exec;
        let job_a_ref = &job_a;
        let job_b_ref = &job_b;
        let ha = scope.spawn(move || exec_ref.run(job_a_ref));
        let hb = scope.spawn(move || exec_ref.run(job_b_ref));
        (ha.join().unwrap(), hb.join().unwrap())
    });
    let elapsed = start.elapsed();

    let ra = result.0.expect("job a dispatch");
    let rb = result.1.expect("job b dispatch");
    assert_eq!(ra.exit_code, 0, "stderr: {}", String::from_utf8_lossy(&ra.stderr));
    assert_eq!(rb.exit_code, 0, "stderr: {}", String::from_utf8_lossy(&rb.stderr));

    let stdout_a = std::fs::read_to_string(root.path().join("log-a").join("stdout.txt")).unwrap();
    let stdout_b = std::fs::read_to_string(root.path().join("log-b").join("stdout.txt")).unwrap();
    assert_eq!(stdout_a, "marker-a");
    assert_eq!(stdout_b, "marker-b");

    // Both jobs sleep 0.4s each; if the template serialized them the wall
    // time would be >= 0.8s. Genuine concurrency (one fork per job, no
    // blocking of the template's own accept loop) keeps it well under
    // that — generous margin to avoid flaking under CI/container jitter.
    assert!(
        elapsed < Duration::from_millis(700),
        "jobs did not appear to run concurrently: took {elapsed:?}"
    );

    // Exactly one template process was ever spawned for this shared
    // (interpreter, preload) key — exactly one stderr log file.
    let template_logs: Vec<_> = std::fs::read_dir(&work_parent)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("template-"))
        .collect();
    assert_eq!(template_logs.len(), 1, "expected exactly one template spawned: {template_logs:?}");
}

#[test]
fn template_death_mid_job_errors_then_respawn_serves_next_job() {
    if !python3_available() {
        eprintln!("skipping: python3 not available");
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let template_script = write_fake_template(root.path());
    let work_parent = root.path().join("work");
    let fallback = NoneExecutor::new(&work_parent);
    let manager = make_manager(&work_parent, &template_script);
    let exec = ForkserverExecutor::new(manager, fallback);

    // This job's template dies the instant it receives the "run" message
    // (before even forking) — both the first attempt AND its one
    // automatic respawn hit the same self-destructing env, so this must
    // surface as a real error (not hang, not silently succeed).
    let dying_job = shim_job(
        &"d".repeat(64),
        &root.path().join("out-d"),
        &root.path().join("log-d"),
        &[("TEST_SELFDESTRUCT", "1")],
    );
    let err = exec.run(&dying_job).expect_err("dying job must surface an error");
    let msg = err.to_string();
    assert!(msg.contains("died twice"), "unexpected error message: {msg}");

    // A normal job with the *same* (interpreter, preload) template key
    // dispatched afterwards must still work — proves get_or_spawn respawns
    // a fresh, healthy template rather than staying wedged on the dead one.
    let ok_job = shim_job(
        &"e".repeat(64),
        &root.path().join("out-e"),
        &root.path().join("log-e"),
        &[("TEST_STDOUT", "alive-again"), ("TEST_EXIT_CODE", "0")],
    );
    let result = exec.run(&ok_job).expect("job after respawn must succeed");
    assert_eq!(result.exit_code, 0, "stderr: {}", String::from_utf8_lossy(&result.stderr));
    let stdout = std::fs::read_to_string(root.path().join("log-e").join("stdout.txt")).unwrap();
    assert_eq!(stdout, "alive-again");
}

#[test]
fn nonshim_argv_falls_back_to_none_executor() {
    let root = tempfile::tempdir().unwrap();
    // Deliberately point template_argv at a nonexistent interpreter: if
    // the fallback routing were broken and this job were mistakenly sent
    // to a template, spawning would fail loudly (there is no
    // "/definitely/does/not/exist" binary) rather than silently passing.
    let work_parent = root.path().join("work");
    let fallback = NoneExecutor::new(&work_parent);
    let manager = Arc::new(TemplateManager::new(
        work_parent,
        vec![
            "/definitely/does/not/exist/python".to_string(),
            "-I".to_string(),
            "-m".to_string(),
            "ppg3._template".to_string(),
        ],
    ));
    let exec = ForkserverExecutor::new(manager, fallback);

    let job = command_job(&root.path().join("out-c"), &root.path().join("log-c"));
    let result = exec.run(&job).expect("plain CommandJob argv must fall back to NoneExecutor");
    assert_eq!(result.exit_code, 0, "stderr: {}", String::from_utf8_lossy(&result.stderr));
    let content = std::fs::read_to_string(root.path().join("out-c").join("marker.txt")).unwrap();
    assert_eq!(content, "fallback-ran\n");
}

// --------------------------------------------------------------------------
// §6.7 cross-run persistence: a `TemplateManager` outlives any one
// `ForkserverExecutor` — this is what lets a coordinator session keep
// templates warm across separate `ppg3.run()` calls.
// --------------------------------------------------------------------------

#[test]
fn manager_outlives_executors_same_template_reused_then_shutdown_respawns() {
    if !python3_available() {
        eprintln!("skipping: python3 not available");
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let template_script = write_fake_template(root.path());
    let work_parent = root.path().join("work");
    let manager = make_manager(&work_parent, &template_script);

    // --- "run" 1: a fresh executor A built on the shared manager --------
    let exec_a = ForkserverExecutor::new(manager.clone(), NoneExecutor::new(&work_parent));
    let job1 = shim_job(
        &"1".repeat(64),
        &root.path().join("out-1"),
        &root.path().join("log-1"),
        &[("TEST_WRITE_PPID", "1")],
    );
    let r1 = exec_a.run(&job1).expect("job1 dispatch");
    assert_eq!(r1.exit_code, 0, "stderr: {}", String::from_utf8_lossy(&r1.stderr));
    let ppid1 =
        std::fs::read_to_string(root.path().join("log-1").join("stdout.txt")).unwrap();
    // Dropping executor A (as `ppg3.run()` would at the end of a run) must
    // NOT kill the manager's template — that is the entire point of the
    // ownership split (§6.7).
    drop(exec_a);
    assert_eq!(manager.template_count(), 1);

    // --- "run" 2: a brand-new executor B, same manager -------------------
    let exec_b = ForkserverExecutor::new(manager.clone(), NoneExecutor::new(&work_parent));
    let job2 = shim_job(
        &"2".repeat(64),
        &root.path().join("out-2"),
        &root.path().join("log-2"),
        &[("TEST_WRITE_PPID", "1")],
    );
    let r2 = exec_b.run(&job2).expect("job2 dispatch");
    assert_eq!(r2.exit_code, 0, "stderr: {}", String::from_utf8_lossy(&r2.stderr));
    let ppid2 =
        std::fs::read_to_string(root.path().join("log-2").join("stdout.txt")).unwrap();
    assert_eq!(
        ppid1, ppid2,
        "executor B must reuse the same warm template executor A used (same template pid)"
    );
    drop(exec_b);

    // --- explicit shutdown: the next dispatch must spawn a fresh template
    manager.shutdown();
    assert_eq!(manager.template_count(), 0);

    let exec_c = ForkserverExecutor::new(manager.clone(), NoneExecutor::new(&work_parent));
    let job3 = shim_job(
        &"3".repeat(64),
        &root.path().join("out-3"),
        &root.path().join("log-3"),
        &[("TEST_WRITE_PPID", "1")],
    );
    let r3 = exec_c.run(&job3).expect("job3 dispatch");
    assert_eq!(r3.exit_code, 0, "stderr: {}", String::from_utf8_lossy(&r3.stderr));
    let ppid3 =
        std::fs::read_to_string(root.path().join("log-3").join("stdout.txt")).unwrap();
    assert_ne!(
        ppid1, ppid3,
        "after shutdown() the next dispatch must spawn a fresh template process"
    );
    assert_eq!(manager.template_count(), 1);
}

#[test]
fn template_key_includes_python_env_two_distinct_templates() {
    if !python3_available() {
        eprintln!("skipping: python3 not available");
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let template_script = write_fake_template(root.path());
    let work_parent = root.path().join("work");
    let manager = make_manager(&work_parent, &template_script);
    let exec = ForkserverExecutor::new(manager.clone(), NoneExecutor::new(&work_parent));

    // Two jobs, identical interpreter + preload, but different
    // `runtime.python_env` — §6.4/§6.7 require these to land in distinct
    // templates (a changed `PyEnv` resolution must never reuse an old
    // template's fork-time state).
    let job_a = shim_job_with_env(
        &"4".repeat(64),
        &root.path().join("out-4"),
        &root.path().join("log-4"),
        &[("TEST_STDOUT", "a")],
        "python-env-a",
    );
    let job_b = shim_job_with_env(
        &"5".repeat(64),
        &root.path().join("out-5"),
        &root.path().join("log-5"),
        &[("TEST_STDOUT", "b")],
        "python-env-b",
    );

    let ra = exec.run(&job_a).expect("job a dispatch");
    let rb = exec.run(&job_b).expect("job b dispatch");
    assert_eq!(ra.exit_code, 0, "stderr: {}", String::from_utf8_lossy(&ra.stderr));
    assert_eq!(rb.exit_code, 0, "stderr: {}", String::from_utf8_lossy(&rb.stderr));

    assert_eq!(
        manager.template_count(),
        2,
        "distinct python_env must produce distinct templates even with identical interpreter+preload"
    );
}
