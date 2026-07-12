//! Test-only helper for `core/tests/sandbox_unshare.rs` (same pattern as
//! `store_helper.rs`): a real, freshly-exec'd, **single-threaded** OS
//! process that calls `sandbox::enter_sandbox` — which cannot be called
//! from the multi-threaded cargo test harness itself
//! (`unshare(CLONE_NEWUSER)` fails with EINVAL in a multi-threaded
//! process, and a successful `pivot_root` would hijack the harness).
//!
//! Usage: `sandbox_helper <new_root> <ro_source_dir>`
//!
//! `ro_source_dir` (containing `marker.txt`) is bind-mounted read-only at
//! `<new_root>/in/data`; after `enter_sandbox` the process asserts the
//! §6.1-shaped world from the inside. Exit codes:
//! - 0: entered the sandbox, all assertions held (prints `SANDBOX-OK`)
//! - 2: user namespaces unavailable on this host (caller should skip)
//! - 1: any real failure (diagnostics on stderr)

use std::path::{Path, PathBuf};

use ppg3_core::sandbox::{enter_sandbox, SandboxLayout, SandboxMount};

fn fail(msg: &str) -> ! {
    eprintln!("FAIL: {msg}");
    std::process::exit(1);
}

fn main() {
    let mut args = std::env::args().skip(1);
    let new_root = PathBuf::from(args.next().unwrap_or_else(|| fail("missing <new_root>")));
    let ro_source = PathBuf::from(
        args.next()
            .unwrap_or_else(|| fail("missing <ro_source_dir>")),
    );

    let layout = SandboxLayout {
        new_root,
        mounts: vec![SandboxMount {
            source: ro_source,
            target_rel: PathBuf::from("in/data"),
            read_only: true,
        }],
        chdir: PathBuf::from("/in/data"),
        allow_network: false,
    };

    if let Err(e) = enter_sandbox(&layout) {
        let msg = e.to_string();
        // EPERM (1), EACCES (13), ENOSYS (38): unprivileged user
        // namespaces are disabled/unsupported here — a skip, not a bug.
        if msg.contains("unshare failed")
            && ["(os error 1)", "(os error 13)", "(os error 38)"]
                .iter()
                .any(|c| msg.contains(c))
        {
            eprintln!("SKIP: {msg}");
            std::process::exit(2);
        }
        fail(&format!("enter_sandbox: {msg}"));
    }

    // -- Inside the sandbox now. Assert the world looks right. --

    let cwd = std::env::current_dir().unwrap_or_else(|e| fail(&format!("getcwd: {e}")));
    if cwd != Path::new("/in/data") {
        fail(&format!("cwd is {cwd:?}, expected /in/data"));
    }

    match std::fs::read_to_string("/in/data/marker.txt") {
        Ok(s) if s == "marker\n" => {}
        Ok(s) => fail(&format!("marker.txt content wrong: {s:?}")),
        Err(e) => fail(&format!("reading /in/data/marker.txt: {e}")),
    }

    match std::fs::write("/in/data/should-fail.txt", b"x") {
        Err(e) if e.raw_os_error() == Some(libc::EROFS) => {}
        Err(e) => fail(&format!(
            "write to ro mount failed with {e}, expected EROFS"
        )),
        Ok(_) => fail("write to read-only /in/data unexpectedly succeeded"),
    }

    if std::fs::metadata("/etc/passwd").is_ok() {
        fail("/etc/passwd is visible — host fs leaked into the sandbox");
    }

    if std::fs::metadata("/.ppg3-old-root").is_ok() {
        fail("/.ppg3-old-root still present — old root not detached");
    }

    if let Err(e) = std::fs::write("/tmp/probe.txt", b"w") {
        fail(&format!("writing to sandbox /tmp tmpfs: {e}"));
    }

    println!("SANDBOX-OK");
}
