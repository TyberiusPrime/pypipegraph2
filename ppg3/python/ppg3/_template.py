"""``python -I -m ppg3._template`` — the forkserver warm-template process
(PPG3_DESIGN.md §6.4, CONTRACT.md "Executor", ``core/src/forkserver.rs``'s
wire protocol).

This module MUST stay single-threaded and MUST NOT run user code at import
time or start any threads — that is precisely what makes ``os.fork()`` here
safe (§6.4: "The template is guaranteed single-threaded ... it never runs
user code directly and never starts threads. All forking in ppg3 happens
here — the multi-threaded coordinator never forks."). Only the *preload*
modules (declared, non-user infrastructure/library code named in a
``PyEnv(preload=[...])``) are imported before the fork/control loop starts;
the actual job callback only ever runs inside a freshly forked child, after
``ppg3._shim.main()`` reconstructs it there.

Wire protocol (JSON lines, one object per line — see the ``forkserver.rs``
module doc comment for the authoritative spec this mirrors):

- template -> rust on start: ``{"ready": true, "pid": N, "preloaded": [...]}``
  (or ``{"ready": false, "error": "..."}`` then exit 3, on a preload import
  failure).
- rust -> template: ``{"run": {"id", "argv", "env", "cwd", "stdout_path",
  "stderr_path"}}``
- template -> rust: ``{"started": {"id", "pid"}}``
- template -> rust (async, any order): ``{"exited": {"id", "pid",
  "exit_code"}}`` (signal death: ``exit_code = 128 + signal``)

Reading control messages: deliberately **not**
``select.select([sys.stdin], ...)`` + ``sys.stdin.readline()`` — verified by
hand while building this module that the combination is unsafe here:
``TextIOWrapper``'s internal buffered reader can pull more than one queued
"run" line into its own userspace buffer on a single underlying ``read()``
syscall (this happens routinely — several scheduler worker threads can each
write one "run" line to this process' stdin in close succession). After
that, the *file descriptor* itself has no more bytes for ``select()`` to see
even though a second, complete line is still sitting unread in the
``TextIOWrapper``'s buffer — ``select()`` then times out forever and that
second message is silently never processed (a real, observed hang, not a
theoretical one). Fixed by reading the raw fd directly (``os.read``) and
doing manual `\\n`-delimited line buffering ourselves (`_try_read_line`).
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import select
import signal
import sys
import traceback
from typing import Any, Dict, List, Optional, Tuple

_STDIN_FD = 0
_read_buf = b""


def _try_read_line(timeout: float) -> Tuple[str, Optional[str]]:
    """Returns ``("line", text)``, ``("timeout", None)``, or ``("eof",
    None)``. See the module docstring for why this is not
    ``select`` + ``TextIOWrapper.readline()``."""
    global _read_buf
    nl = _read_buf.find(b"\n")
    if nl != -1:
        line = _read_buf[:nl]
        _read_buf = _read_buf[nl + 1 :]
        return ("line", line.decode("utf-8"))
    r, _, _ = select.select([_STDIN_FD], [], [], timeout)
    if _STDIN_FD not in r:
        return ("timeout", None)
    chunk = os.read(_STDIN_FD, 1 << 16)
    if chunk == b"":
        return ("eof", None)
    _read_buf += chunk
    nl = _read_buf.find(b"\n")
    if nl != -1:
        line = _read_buf[:nl]
        _read_buf = _read_buf[nl + 1 :]
        return ("line", line.decode("utf-8"))
    return ("timeout", None)


def _emit(obj: Dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


def _preload(names: List[str]) -> None:
    for name in names:
        importlib.import_module(name)


def _shim_args_from_argv(argv: List[str]) -> List[str]:
    """``argv`` is the job's full staged command (e.g. ``["<venv>/bin/
    python3", "-I", "-m", "ppg3._shim", "--spec-b64", ..., "--in", ...]``,
    already `/ppg/`-rewritten by `executor::stage` on the Rust side). This
    process never execs it (that would defeat the whole point of forking a
    warm interpreter) — it locates the ``ppg3._shim`` module marker and
    hands everything after it to ``ppg3._shim.main()`` directly, in-process,
    inside the freshly forked child."""
    try:
        idx = argv.index("ppg3._shim")
    except ValueError:
        raise RuntimeError(
            f"ppg3._template: job argv does not contain 'ppg3._shim': {argv!r}"
        ) from None
    return argv[idx + 1 :]


def _run_child(run: Dict[str, Any]) -> None:
    """Runs entirely inside the forked child (post-fork, pre-exit). Never
    returns — always ``os._exit()``s."""
    stdout_path = run["stdout_path"]
    stderr_path = run["stderr_path"]
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
    except OSError:
        # No usable stdout/stderr redirection target — nothing sane to do
        # but exit non-zero; can't even write a traceback anywhere useful.
        os._exit(1)

    try:
        os.chdir(run.get("cwd", "."))
        os.environ.clear()
        os.environ.update(run.get("env", {}))
        shim_args = _shim_args_from_argv(run["argv"])
        sys.argv = ["ppg3._shim"] + shim_args

        from ppg3 import _shim

        rc = _shim.main(shim_args)
    except SystemExit as e:
        rc = e.code if isinstance(e.code, int) else (0 if e.code is None else 1)
    except Exception:
        traceback.print_exc(file=sys.stderr)
        rc = 1
    os._exit(rc)


def _reap(children: Dict[int, str]) -> None:
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
            exit_code = os.WEXITSTATUS(status)
        elif os.WIFSIGNALED(status):
            exit_code = 128 + os.WTERMSIG(status)
        else:
            exit_code = -1
        _emit({"exited": {"id": rid, "pid": wpid, "exit_code": exit_code}})


def _handle_run(msg: Dict[str, Any], children: Dict[int, str]) -> None:
    run = msg["run"]
    rid = run["id"]
    pid = os.fork()
    if pid == 0:
        _run_child(run)
        # unreachable: _run_child always os._exit()s
        os._exit(70)
    children[pid] = rid
    _emit({"started": {"id": rid, "pid": pid}})


def main_loop() -> None:
    children: Dict[int, str] = {}
    while True:
        status, line = _try_read_line(0.2)
        if status == "eof":
            for pid in list(children):
                try:
                    os.killpg(pid, signal.SIGTERM)
                except (ProcessLookupError, PermissionError, OSError):
                    pass
            _reap(children)
            return
        if status == "line" and line:
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue
            if "run" in msg:
                _handle_run(msg, children)
        _reap(children)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="ppg3._template")
    parser.add_argument("--preload", default="[]")
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    try:
        preload_names = json.loads(args.preload)
        _preload(preload_names)
    except Exception as e:
        _emit({"ready": False, "error": f"{type(e).__name__}: {e}"})
        return 3

    _emit({"ready": True, "pid": os.getpid(), "preloaded": preload_names})
    main_loop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
