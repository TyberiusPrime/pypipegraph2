//! No-exec sandbox entry (WP3, PPG3_DESIGN.md §6.4/§8.1).
//!
//! This is the forkserver's self-sandboxing entry point: a template
//! process `fork()`s (cheap, COW of warmed import state), and the child
//! calls [`enter_sandbox`] *without exec-ing* to drop into the same
//! `/ppg/...` hygiene as a `CommandJob` under bwrap, before running the
//! user callback in-process. PPG3_DESIGN.md and CONTRACT.md both flag this
//! as the highest-risk item in the whole design ("build it as a standalone
//! prototype ... before integrating").
//!
//! ## Scope of this implementation
//!
//! This container has neither user namespaces enabled in a way we can
//! verify at build time, nor any way to *run* this code (no CI step here
//! exercises unshare/pivot_root). Per the work-package brief: "a compiling,
//! documented implementation is required but runtime testing is impossible
//! in this container; correctness review will come later." Accordingly:
//!
//! - The real implementation lives behind `#[cfg(all(target_os = "linux",
//!   feature = "linux-sandbox"))]` and is never compiled by the default
//!   `cargo test -p ppg3-core` / `cargo clippy` run (feature off by
//!   default, CONTRACT.md `core/Cargo.toml`). `cargo check -p ppg3-core
//!   --features linux-sandbox` must still compile it.
//! - **Not implemented here**: the PID namespace + second `fork()` that
//!   PPG3_DESIGN.md §6.4 describes ("then a second fork for the PID
//!   namespace"). `enter_sandbox` only performs the user+mount(+net)
//!   namespace unshare, bind-mount layout, and `pivot_root` — the part
//!   expressible as a single non-forking function. Wiring the PID
//!   namespace requires owning process reaping across an actual `fork()`,
//!   which is forkserver-protocol shaped and out of this module's
//!   "layout in, sandboxed cwd out" contract. Left as a documented TODO
//!   for whichever WP wires the real forkserver (§6.4 is explicitly
//!   deferred for v1-in-this-repo per CONTRACT.md's "Scope deviations").
//! - Without the `linux-sandbox` feature (or off Linux), `enter_sandbox`
//!   is a stub returning `Error::Other("linux-sandbox feature not
//!   enabled")` — callers always get a `Result`, never a missing symbol.

use std::path::PathBuf;

use crate::error::Error;
use crate::Result;

/// One bind mount to set up under the new root, relative to it (e.g.
/// `in/data`, `tools/py`, `out`, `log`, mirroring `/ppg/...` §6.1).
#[derive(Debug, Clone)]
pub struct SandboxMount {
    pub source: PathBuf,
    /// Path *relative to* [`SandboxLayout::new_root`], e.g. `PathBuf::from("in/data")`.
    pub target_rel: PathBuf,
    pub read_only: bool,
}

/// Everything [`enter_sandbox`] needs to build the job's `/ppg/...` world
/// (PPG3_DESIGN.md §6.1) and pivot into it.
#[derive(Debug, Clone)]
pub struct SandboxLayout {
    /// A pre-existing, writable directory that becomes the new `/`. Must
    /// live on a filesystem that supports bind mounts (i.e. not itself a
    /// tmpfs-in-a-container edge case — see kernel `pivot_root(2)` caveats).
    pub new_root: PathBuf,
    pub mounts: Vec<SandboxMount>,
    /// Absolute path *inside* the new root to `chdir` into once pivoted
    /// (e.g. `/ppg/out`).
    pub chdir: PathBuf,
    /// If true, skip `CLONE_NEWNET` (fixed-output jobs only, §7.6).
    pub allow_network: bool,
}

#[cfg(all(target_os = "linux", feature = "linux-sandbox"))]
mod imp {
    use super::{SandboxLayout, SandboxMount};
    use crate::error::Error;
    use crate::Result;
    use std::ffi::CString;
    use std::path::Path;
    use std::ptr;

    fn errno_err(op: &str) -> Error {
        Error::Other(format!(
            "{op} failed: {}",
            std::io::Error::last_os_error()
        ))
    }

    fn path_cstring(p: &Path) -> Result<CString> {
        CString::new(p.as_os_str().as_encoded_bytes())
            .map_err(|_| Error::Other(format!("path contains NUL: {p:?}")))
    }

    /// Write the uid/gid maps that make the calling (real host) user appear
    /// as uid/gid 0 inside the new user namespace — required before any
    /// further namespace operation that needs privilege inside the ns
    /// (mount, pivot_root). `setgroups` must be denied first: the kernel
    /// refuses an unprivileged write to `gid_map` otherwise.
    unsafe fn write_uid_gid_maps() -> Result<()> {
        let uid = libc::geteuid();
        let gid = libc::getegid();
        std::fs::write("/proc/self/setgroups", b"deny")
            .map_err(|e| Error::io("/proc/self/setgroups", e))?;
        std::fs::write("/proc/self/uid_map", format!("0 {uid} 1\n"))
            .map_err(|e| Error::io("/proc/self/uid_map", e))?;
        std::fs::write("/proc/self/gid_map", format!("0 {gid} 1\n"))
            .map_err(|e| Error::io("/proc/self/gid_map", e))?;
        Ok(())
    }

    unsafe fn bind_mount(source: &Path, target: &Path, read_only: bool) -> Result<()> {
        let src = path_cstring(source)?;
        let dst = path_cstring(target)?;
        if libc::mount(src.as_ptr(), dst.as_ptr(), ptr::null(), libc::MS_BIND, ptr::null()) != 0 {
            return Err(errno_err(&format!("bind mount {source:?} -> {target:?}")));
        }
        if read_only {
            // A read-only bind mount needs a remount pass: MS_BIND alone
            // ignores MS_RDONLY on the initial call (long-standing Linux
            // mount(2) quirk).
            if libc::mount(
                ptr::null(),
                dst.as_ptr(),
                ptr::null(),
                libc::MS_BIND | libc::MS_REMOUNT | libc::MS_RDONLY,
                ptr::null(),
            ) != 0
            {
                return Err(errno_err(&format!("ro-remount {target:?}")));
            }
        }
        Ok(())
    }

    unsafe fn mount_tmpfs(target: &Path) -> Result<()> {
        let dst = path_cstring(target)?;
        let fstype = CString::new("tmpfs").unwrap();
        if libc::mount(ptr::null(), dst.as_ptr(), fstype.as_ptr(), 0, ptr::null()) != 0 {
            return Err(errno_err(&format!("mount tmpfs at {target:?}")));
        }
        Ok(())
    }

    /// Enter the sandbox: unshare user(+mount+net) namespaces, remap
    /// uid/gid to 0-in-namespace, bind-mount the declared layout, and
    /// `pivot_root` into it. Does **not** fork or set up a PID namespace
    /// (see module docs) — caller is expected to already be the dedicated
    /// child process for this one job.
    pub fn enter_sandbox(layout: &SandboxLayout) -> Result<()> {
        unsafe {
            let mut flags = libc::CLONE_NEWUSER | libc::CLONE_NEWNS;
            if !layout.allow_network {
                flags |= libc::CLONE_NEWNET;
            }
            if libc::unshare(flags) != 0 {
                return Err(errno_err("unshare"));
            }
            write_uid_gid_maps()?;

            // Stop mount events propagating back to the host mount ns
            // before we start bind-mounting things.
            let root = CString::new("/").unwrap();
            if libc::mount(
                ptr::null(),
                root.as_ptr(),
                ptr::null(),
                libc::MS_REC | libc::MS_PRIVATE,
                ptr::null(),
            ) != 0
            {
                return Err(errno_err("mount MS_PRIVATE /"));
            }

            // pivot_root requires the new root to itself be a mount point.
            bind_mount(&layout.new_root, &layout.new_root, false)?;

            for m in &layout.mounts {
                let target = layout.new_root.join(&m.target_rel);
                std::fs::create_dir_all(&target).map_err(|e| Error::io(&target, e))?;
                bind_mount(&m.source, &target, m.read_only)?;
            }

            let tmp = layout.new_root.join("tmp");
            std::fs::create_dir_all(&tmp).map_err(|e| Error::io(&tmp, e))?;
            mount_tmpfs(&tmp)?;

            let old_root = layout.new_root.join(".ppg3-old-root");
            std::fs::create_dir_all(&old_root).map_err(|e| Error::io(&old_root, e))?;
            let new_root_c = path_cstring(&layout.new_root)?;
            let old_root_c = path_cstring(&old_root)?;
            let ret = libc::syscall(libc::SYS_pivot_root, new_root_c.as_ptr(), old_root_c.as_ptr());
            if ret != 0 {
                return Err(errno_err("pivot_root"));
            }

            let slash = CString::new("/").unwrap();
            if libc::chdir(slash.as_ptr()) != 0 {
                return Err(errno_err("chdir /"));
            }

            let old_root_after_pivot = CString::new("/.ppg3-old-root").unwrap();
            if libc::umount2(old_root_after_pivot.as_ptr(), libc::MNT_DETACH) != 0 {
                return Err(errno_err("umount2 old root"));
            }
            let _ = std::fs::remove_dir("/.ppg3-old-root");

            let chdir_c = path_cstring(&layout.chdir)?;
            if libc::chdir(chdir_c.as_ptr()) != 0 {
                return Err(errno_err("chdir to job cwd"));
            }
        }
        Ok(())
    }

    #[allow(dead_code)]
    fn _assert_send(_: &SandboxMount) {}
}

#[cfg(all(target_os = "linux", feature = "linux-sandbox"))]
pub use imp::enter_sandbox;

#[cfg(not(all(target_os = "linux", feature = "linux-sandbox")))]
pub fn enter_sandbox(_layout: &SandboxLayout) -> Result<()> {
    Err(Error::Other(
        "linux-sandbox feature not enabled".to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stub_without_feature_returns_documented_error() {
        // This test runs under the default (no linux-sandbox feature)
        // build, which is what `cargo test -p ppg3-core` exercises.
        #[cfg(not(all(target_os = "linux", feature = "linux-sandbox")))]
        {
            let layout = SandboxLayout {
                new_root: PathBuf::from("/nonexistent"),
                mounts: vec![],
                chdir: PathBuf::from("/"),
                allow_network: false,
            };
            let err = enter_sandbox(&layout).unwrap_err();
            match err {
                Error::Other(msg) => assert!(msg.contains("linux-sandbox feature not enabled")),
                other => panic!("unexpected error variant: {other:?}"),
            }
        }
    }

    /// Documents how to actually exercise `enter_sandbox` — impossible in
    /// this container (no privilege to create user namespaces reliably /
    /// no way to assert on the result of a `pivot_root`'d child from a
    /// test harness). To run manually on a Linux box with unprivileged
    /// user namespaces enabled (`sysctl kernel.unprivileged_userns_clone`
    /// or equivalent distro knob):
    ///
    /// 1. `cargo test -p ppg3-core --features linux-sandbox
    ///    sandbox::tests::manual_enter_sandbox_smoke -- --ignored --nocapture`
    /// 2. Populate a scratch dir with `in/`, `tools/`, `out/`, `log/`
    ///    subdirectories containing marker files.
    /// 3. Fork (e.g. via `libc::fork` in the test, or run this as a
    ///    subprocess) *before* calling `enter_sandbox` — it mutates the
    ///    calling process' namespaces irreversibly.
    /// 4. After `enter_sandbox` returns `Ok`, assert: `/ppg/in/...` marker
    ///    files are readable; a write to `/ppg/in/...` fails with EROFS;
    ///    a path outside the declared mounts (e.g. `/etc/passwd` outside
    ///    any bind) is ENOENT; `ip link` (if available) shows only `lo`
    ///    when `allow_network: false`.
    #[test]
    #[ignore = "requires unprivileged user namespaces and a forked child; see doc comment"]
    fn manual_enter_sandbox_smoke() {
        #[cfg(all(target_os = "linux", feature = "linux-sandbox"))]
        {
            let dir = tempfile::tempdir().unwrap();
            let layout = SandboxLayout {
                new_root: dir.path().to_path_buf(),
                mounts: vec![],
                chdir: PathBuf::from("/"),
                allow_network: false,
            };
            // Deliberately not forked: running this for real in-process
            // would pivot_root the test harness itself. Left as a
            // documented manual step (see doc comment above).
            let _ = enter_sandbox(&layout);
        }
    }
}
