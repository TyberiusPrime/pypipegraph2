"""Polling watcher + definition-pass loop for ``python -m ppg3 watch``
(PPG3_DESIGN.md §6.7, CONTRACT.md "Python package").

**Deviation from §6.7** (also recorded in ``STATUS.md``): the design says
the coordinator watches leaf inputs / the pipeline script / ``Source``
files "via inotify". This dev container has no inotify Python binding and
the project takes no third-party dependencies (CONTRACT.md), so this
implementation polls ``mtime_ns``+``size`` instead (§10.3's stat-cache
already uses the same non-authoritative stat signal for the same reason).
Same semantics — a definition pass re-runs exactly when a watched path's
content or existence changes — strictly worse latency/efficiency (bounded
by ``--interval``, default 0.5s, instead of kernel-immediate).
:class:`PollingWatcher` exposes only the narrow ``poll() -> List[str]``
interface documented below so a future inotify-backed watcher can be
dropped in without touching :func:`run_watch`.

Loop semantics (§6.7, "watch" bullet — session-mode template persistence is
out of scope here):

1. Each iteration executes the pipeline script as a fresh definition pass
   (``runpy.run_path(script, run_name="__main__")``); the script itself
   calls ``ppg3.new(...)`` / ``ppg3.run()``.
2. Runs made while :func:`ppg3.run.watch_mode` is active are forced
   ephemeral (``_core.write_generation(..., ephemeral=True)``), regardless
   of what the script itself passed to ``ppg3.run(ephemeral=...)``.
3. After each pass, the watch set is the pipeline script path plus
   whatever :meth:`ppg3.jobs.Graph.watched_paths` collected (leaf ``File``
   inputs + ``Source`` callback files/includes) — read back via
   ``ppg3.run.get_last_run_info()`` since the script's own local ``graph``
   variable does not survive ``runpy.run_path()`` returning.
4. Polls until something in the watch set changes (content, appear, or
   disappear), debouncing: after the first detected change, sleep one more
   interval and re-scan once so multi-file saves batch into one re-run.
5. A definition-pass exception (including :class:`ppg3.run.PPGRunError`)
   prints a traceback/report and keeps the loop alive, watching the
   last-known watch set (or, on the very first iteration, just the script).
   ``KeyboardInterrupt`` exits cleanly (exit code 0) with a summary.
"""

from __future__ import annotations

import os
import runpy
import sys
import time
import traceback
from datetime import datetime, timezone
from typing import Dict, IO, Iterable, List, Optional, Sequence, Tuple

from . import jobs as _jobs_module

# NOT `from . import run as _run_module`: `ppg3/__init__.py` does `from
# .run import run`, which rebinds the *package* attribute `ppg3.run` to the
# function (shadowing the submodule of the same name) — `from . import run`
# here would silently resolve to that function via attribute lookup rather
# than the module. Importing the specific names we need from `ppg3.run`
# sidesteps the clash (it resolves against the submodule's own namespace).
from .run import PPGRunError, get_last_run_info, watch_mode

# `(mtime_ns, size)` of a tracked path, or `None` if it does not exist.
StatT = Optional[Tuple[int, int]]


def _stat(path: str) -> StatT:
    try:
        st = os.stat(path)
    except OSError:
        return None
    return (st.st_mtime_ns, st.st_size)


class PollingWatcher:
    """Polls ``(mtime_ns, size)`` for a fixed set of paths and reports which
    ones changed since the last :meth:`poll` call — including a path
    appearing (``None`` -> a stat) or disappearing (a stat -> ``None``).

    Deliberately a class with only this narrow interface (see module
    docstring): a future inotify-backed implementation only needs to expose
    the same ``poll() -> List[str]`` method to drop into :func:`run_watch`.
    """

    def __init__(self, paths: Iterable[str] = ()):
        self._state: Dict[str, StatT] = {}
        self.set_paths(paths)

    def set_paths(self, paths: Iterable[str]) -> None:
        """Replace the tracked path set. A path that was already tracked
        keeps its last-known state (so switching watch sets between
        iterations never manufactures a spurious "changed" result); a
        brand-new path gets a fresh baseline stat instead (it only reports
        as changed on a *later* poll, once it actually changes)."""
        new_paths = {str(p) for p in paths}
        self._state = {
            p: (self._state[p] if p in self._state else _stat(p)) for p in new_paths
        }

    def poll(self) -> List[str]:
        """Re-stat every tracked path; return the sorted list of paths
        whose state changed (content/mtime, appeared, or disappeared) since
        the last call, and update the internal baseline to match."""
        changed = []
        for path, prev in self._state.items():
            cur = _stat(path)
            if cur != prev:
                changed.append(path)
        for path in changed:
            self._state[path] = _stat(path)
        return sorted(changed)


def wait_for_change(
    watcher: PollingWatcher,
    interval: float,
    debounce: bool = True,
) -> List[str]:
    """Block, polling ``watcher`` every ``interval`` seconds, until
    something changes; then (if ``debounce``) sleep one more interval and
    poll once more, merging in anything that changed during that settle
    window, so multi-file saves batch into a single trigger (§6.7).
    Returns the sorted, de-duplicated union of changed paths.
    ``KeyboardInterrupt`` (SIGINT during ``time.sleep``) propagates
    straight through — that is how the caller stops the loop."""
    changed: List[str] = []
    while not changed:
        time.sleep(interval)
        changed = watcher.poll()
    if debounce:
        time.sleep(interval)
        changed = sorted(set(changed) | set(watcher.poll()))
    return changed


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _print(stream: IO[str], msg: str) -> None:
    stream.write(msg + "\n")
    stream.flush()


def _run_definition_pass(script: str, script_args: Sequence[str]) -> None:
    """One definition pass: defensively reset the module-level
    "current graph" global (``ppg3.jobs._current_graph`` — regression guard
    so two consecutive passes never accidentally accumulate jobs from a
    stale graph even if a script somehow skips its own ``ppg3.new(...)``
    call), then run the pipeline script fresh via ``runpy.run_path`` under
    ``ppg3.run.watch_mode()`` (forces ephemeral generations, §6.7).
    ``KeyboardInterrupt`` is never caught here — it must propagate all the
    way out of :func:`run_watch`'s loop."""
    _jobs_module._current_graph = None
    saved_argv = sys.argv
    sys.argv = [script, *script_args]
    try:
        with watch_mode():
            runpy.run_path(script, run_name="__main__")
    finally:
        sys.argv = saved_argv


def _report_counts(report: Optional[dict]) -> str:
    if not report:
        return "no report"
    built = len(report.get("built", []))
    hits = len(report.get("hits", []))
    failed = len(report.get("failed", {}))
    return f"built={built} hits={hits} failed={failed}"


def run_watch(
    script: str,
    script_args: Sequence[str] = (),
    interval: float = 0.5,
    stream: Optional[IO[str]] = None,
) -> int:
    """The ``python -m ppg3 watch <script> [args...]`` loop (§6.7).

    Runs the pipeline script's definition pass in-process, once
    immediately and then again every time a watched path changes, forever,
    until ``KeyboardInterrupt`` (SIGINT). A definition-pass exception
    (including :class:`ppg3.run.PPGRunError`) is reported to ``stream`` and
    the loop keeps going, watching the last-known watch set (just the
    script itself before any pass has ever produced one). Returns 0 (the
    process exit code for a clean Ctrl-C exit); this function only returns
    via ``KeyboardInterrupt``, never by falling off the end.
    """
    stream = stream or sys.stdout
    script = os.path.abspath(script)
    watcher = PollingWatcher([script])
    n_runs = 0
    n_failures = 0
    reason = "initial run"

    try:
        while True:
            _print(stream, f"[{_timestamp()}] {reason}")
            n_runs += 1
            try:
                _run_definition_pass(script, script_args)
            except PPGRunError as exc:
                n_failures += 1
                result = exc.result
                _print(
                    stream,
                    "  run failed: "
                    f"{_report_counts(get_last_run_info().get('report'))} "
                    "(view left unchanged)",
                )
                for job_id, err in sorted(result.failed.items())[:5]:
                    _print(stream, f"    {job_id}: {err}")
            except KeyboardInterrupt:
                raise
            except BaseException:
                n_failures += 1
                _print(stream, "  definition pass raised (see stderr for traceback):")
                # Deliberately real stderr (not `stream`, which defaults to
                # stdout): tracebacks are diagnostic noise, not the
                # iteration-status console output described in the module
                # docstring, and the E2E test suite asserts on stderr here.
                traceback.print_exc(file=sys.stderr)
                sys.stderr.flush()
            else:
                info = get_last_run_info()
                _print(
                    stream,
                    f"  ok: {_report_counts(info.get('report'))} "
                    f"generation={info.get('generation')}",
                )

            info = get_last_run_info()
            watched = set(info.get("watched_paths") or [])
            watched.add(script)
            watcher.set_paths(watched)

            changed = wait_for_change(watcher, interval)
            reason = f"change detected: {changed}"
    except KeyboardInterrupt:
        _print(
            stream,
            f"[{_timestamp()}] stopped (SIGINT): {n_runs} run(s), "
            f"{n_failures} failure(s)",
        )
        return 0
