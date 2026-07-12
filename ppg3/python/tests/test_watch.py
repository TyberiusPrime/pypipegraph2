"""Tests for ``python -m ppg3 watch`` (PPG3_DESIGN.md §6.7, CONTRACT.md
"Python package" watch addendum, ``python/ppg3/{watch.py,__main__.py}``).

Layout:

- Unit tests for :class:`ppg3.watch.PollingWatcher` (mtime/size/deletion/
  appearance detection) and :func:`ppg3.watch.wait_for_change`'s debounce
  batching — no ``_core`` needed.
- Unit tests for watch-set collection (``ppg3.jobs.Graph.watched_paths``)
  covering leaf ``File`` inputs (recorded at lowering) and ``Source``
  callback files/includes (recorded at definition) — need blake3 (via
  ``_core`` or the pip package) because both trigger hashing.
- A graph-state-reset regression test for two consecutive
  ``runpy``-driven definition passes (``ppg3.watch._run_definition_pass``)
  — no ``_core`` needed.
- CLI argv parsing tests for ``ppg3.__main__._parse_watch_argv``.
- E2E subprocess tests (``requires_core``): a real ``python -m ppg3 watch``
  process against a small pipeline script, asserting on real generations
  written to disk (including the ``.ephemeral`` marker, per
  ``core/src/views.rs``) and on clean-SIGINT / survive-a-broken-script
  behavior. Every wait in this section has an explicit deadline and the
  subprocess is always killed in a ``finally`` so a bug here fails fast
  rather than hanging the suite.
"""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
import sys
import time

import pytest

import ppg3
from ppg3.tools import PyEnv
from ppg3.watch import PollingWatcher, _run_definition_pass, wait_for_change

from conftest import requires_blake3, requires_core

# --------------------------------------------------------------------------
# PollingWatcher — the polling deviation from §6.7's "inotify" (STATUS.md).
# --------------------------------------------------------------------------


def test_polling_watcher_detects_mtime_change(tmp_path):
    f = tmp_path / "a.txt"
    f.write_text("x")
    watcher = PollingWatcher([str(f)])
    assert watcher.poll() == []  # baseline captured at construction

    time.sleep(0.01)
    os.utime(f, None)  # touch: new mtime_ns, same size/content
    assert watcher.poll() == [str(f)]
    assert watcher.poll() == []  # baseline updated; no further change


def test_polling_watcher_detects_size_only_change_same_mtime(tmp_path):
    f = tmp_path / "b.txt"
    f.write_text("x")
    orig = os.stat(f)
    watcher = PollingWatcher([str(f)])

    f.write_text("x" * 10)
    # Force the mtime back to the original value so this is *purely* a
    # size change from the watcher's point of view.
    os.utime(f, ns=(orig.st_atime_ns, orig.st_mtime_ns))
    after = os.stat(f)
    assert after.st_mtime_ns == orig.st_mtime_ns
    assert after.st_size != orig.st_size

    assert watcher.poll() == [str(f)]


def test_polling_watcher_detects_deletion(tmp_path):
    f = tmp_path / "c.txt"
    f.write_text("x")
    watcher = PollingWatcher([str(f)])

    f.unlink()
    assert watcher.poll() == [str(f)]
    assert watcher.poll() == []  # already-missing baseline; no repeat trigger


def test_polling_watcher_detects_new_file_appearing(tmp_path):
    f = tmp_path / "d.txt"  # does not exist yet
    watcher = PollingWatcher([str(f)])
    assert watcher.poll() == []  # missing is the baseline, not a change

    f.write_text("hello")
    assert watcher.poll() == [str(f)]


def test_polling_watcher_set_paths_preserves_baseline_no_spurious_change(tmp_path):
    f = tmp_path / "e.txt"
    f.write_text("x")
    watcher = PollingWatcher([])
    # Adding a path that already exists must not itself count as "changed"
    # on the next poll — only an actual subsequent modification should.
    watcher.set_paths([str(f)])
    assert watcher.poll() == []

    time.sleep(0.01)
    os.utime(f, None)
    assert watcher.poll() == [str(f)]


def test_polling_watcher_set_paths_drops_untracked_paths(tmp_path):
    f = tmp_path / "f.txt"
    f.write_text("x")
    watcher = PollingWatcher([str(f)])
    watcher.set_paths([])  # stop tracking f

    time.sleep(0.01)
    os.utime(f, None)
    assert watcher.poll() == []  # no longer tracked


# --------------------------------------------------------------------------
# Debounce (§6.7: "after first detected change, sleep one interval and
# re-scan once so multi-file saves batch").
# --------------------------------------------------------------------------


class _ScriptedWatcher:
    """A fake watcher whose ``poll()`` returns a pre-scripted sequence of
    results, one per call — lets the debounce *logic* in
    :func:`wait_for_change` be tested deterministically, independent of
    real filesystem timing."""

    def __init__(self, sequence):
        self._seq = list(sequence)

    def poll(self):
        return self._seq.pop(0) if self._seq else []


def test_wait_for_change_debounce_merges_second_poll():
    # First poll: nothing yet. Second poll: "a" changed (triggers the
    # debounce settle sleep). Third poll (post-settle re-scan): "b" also
    # changed. Both must come back merged from one wait_for_change() call.
    watcher = _ScriptedWatcher([[], ["a"], ["b"]])
    changed = wait_for_change(watcher, interval=0.01)
    assert changed == ["a", "b"]


def test_wait_for_change_no_debounce_returns_immediately_on_first_hit():
    watcher = _ScriptedWatcher([["a"]])
    changed = wait_for_change(watcher, interval=0.01, debounce=False)
    assert changed == ["a"]


def test_wait_for_change_real_files_batches_two_rapid_writes(tmp_path):
    """End-to-end (real filesystem, real sleeps, still fast/deterministic
    via a background thread) version of the debounce test: two files
    written a few milliseconds apart, well inside one polling interval,
    must show up as a single trigger carrying both paths."""
    import threading

    f1 = tmp_path / "one.txt"
    f2 = tmp_path / "two.txt"
    f1.write_text("1")
    f2.write_text("1")
    watcher = PollingWatcher([str(f1), str(f2)])
    assert watcher.poll() == []

    def _writer():
        time.sleep(0.05)
        f1.write_text("11")
        time.sleep(0.02)
        f2.write_text("11")

    t = threading.Thread(target=_writer)
    t.start()
    try:
        changed = wait_for_change(watcher, interval=0.1)
    finally:
        t.join(timeout=5)
    assert changed == sorted([str(f1), str(f2)])


# --------------------------------------------------------------------------
# Watch-set collection (§6.7: leaf File inputs, Source files+includes, the
# script itself — the last of which is watch.py's own job, not Graph's).
# --------------------------------------------------------------------------


@requires_blake3
def test_watched_paths_records_file_leaf_input_at_lowering(tmp_path):
    leaf = tmp_path / "leaf.txt"
    leaf.write_text("hi")
    g = ppg3.new(stores=[], project_dir=str(tmp_path / ".ppg3"), frozen=False)
    ppg3.CommandJob(
        view={"out": "out.txt"},
        argv=["/bin/true"],
        inputs={"leaf": ppg3.File(str(leaf))},
    )
    # Not yet lowered: `_lower_input` (where File leaf paths get recorded)
    # only runs from `job_defs()`/`job_def()`.
    assert g.watched_paths() == []
    g.job_defs()
    assert g.watched_paths() == [str(leaf)]


@requires_blake3
def test_watched_paths_records_source_file_and_includes_at_definition(tmp_path):
    cb_file = tmp_path / "cb.py"
    cb_file.write_text("def cb(io):\n    pass\n")
    include_file = tmp_path / "helper.py"
    include_file.write_text("X = 1\n")

    g = ppg3.new(
        stores=[],
        default_python=PyEnv.current(),
        project_dir=str(tmp_path / ".ppg3"),
        frozen=False,
        paranoid=True,
    )
    # Recorded at __init__ time (definition), *before* job_defs() is ever
    # called — unlike the File-leaf case above.
    ppg3.FileJob(
        view={"out": "out.bin"},
        run=ppg3.Source(f"{cb_file}::cb", includes=[str(include_file)]),
    )
    assert set(g.watched_paths()) == {str(cb_file), str(include_file)}


@requires_blake3
def test_watched_paths_deduplicates_and_sorts(tmp_path):
    leaf = tmp_path / "leaf.txt"
    leaf.write_text("hi")
    g = ppg3.new(stores=[], project_dir=str(tmp_path / ".ppg3"), frozen=False)
    ppg3.CommandJob(
        view={"a": "a.txt"},
        argv=["/bin/true"],
        inputs={"leaf": ppg3.File(str(leaf))},
    )
    ppg3.CommandJob(
        view={"b": "b.txt"},
        argv=["/bin/true"],
        inputs={"leaf": ppg3.File(str(leaf))},
    )
    g.job_defs()
    assert g.watched_paths() == [str(leaf)]  # one entry, not two


def test_graph_record_watched_path_direct(tmp_path):
    g = ppg3.new(stores=[], project_dir=str(tmp_path / ".ppg3"), frozen=False)
    g.record_watched_path("/some/path")
    g.record_watched_path("/some/path")  # duplicate, ignored
    g.record_watched_path("/other/path")
    assert g.watched_paths() == ["/other/path", "/some/path"]


# --------------------------------------------------------------------------
# Graph-state reset (regression guard for the module-level current-graph
# global, `ppg3.jobs._current_graph`).
# --------------------------------------------------------------------------


def test_two_definition_passes_do_not_accumulate_jobs(tmp_path):
    script = tmp_path / "pipeline.py"
    script.write_text(
        "import ppg3\n"
        f"g = ppg3.new(stores=[], project_dir={str(tmp_path / '.ppg3')!r}, frozen=False)\n"
        "ppg3.CommandJob(view={'out': 'out.txt'}, argv=['/bin/true'])\n"
    )

    _run_definition_pass(str(script), [])
    first_graph = ppg3.jobs._current_graph
    assert first_graph is not None
    assert len(first_graph.jobs) == 1

    _run_definition_pass(str(script), [])
    second_graph = ppg3.jobs._current_graph
    assert second_graph is not None
    assert len(second_graph.jobs) == 1
    assert second_graph is not first_graph  # a genuinely fresh Graph


def test_definition_pass_forces_ephemeral_context_flag(tmp_path):
    """``_run_definition_pass`` wraps the script in ``ppg3.run.watch_mode()``
    — verify the module-level flag it toggles is set during the pass and
    cleared after, without needing a real `_core`-backed `run()` call.

    Reached via ``sys.modules["ppg3.run"]`` rather than ``ppg3.run`` or
    ``import ppg3.run as x``: ``ppg3/__init__.py`` does ``from .run import
    run``, which rebinds the *package* attribute ``ppg3.run`` to that
    function, shadowing the submodule of the same name — any of the
    obvious spellings resolve to the function, not the module (see
    ``ppg3/watch.py``'s own import-time comment about this)."""
    import sys as _sys

    run_module = _sys.modules["ppg3.run"]
    assert run_module._watch_active is False

    script = tmp_path / "pipeline.py"
    flag_file = tmp_path / "flag_seen.txt"
    script.write_text(
        "import sys\n"
        "run_mod = sys.modules['ppg3.run']\n"
        f"open({str(flag_file)!r}, 'w').write(str(run_mod._watch_active))\n"
        "import ppg3\n"
        f"ppg3.new(stores=[], project_dir={str(tmp_path / '.ppg3')!r}, frozen=False)\n"
    )
    _run_definition_pass(str(script), [])
    assert flag_file.read_text() == "True"
    assert run_module._watch_active is False


# --------------------------------------------------------------------------
# CLI argv parsing (python/ppg3/__main__.py).
# --------------------------------------------------------------------------


def test_parse_watch_argv_interval_after_script():
    from ppg3.__main__ import _parse_watch_argv

    script, script_args, interval = _parse_watch_argv(["script.py", "--interval", "0.1"])
    assert script == "script.py"
    assert script_args == []
    assert interval == 0.1


def test_parse_watch_argv_interval_before_script_with_extra_args():
    from ppg3.__main__ import _parse_watch_argv

    script, script_args, interval = _parse_watch_argv(
        ["--interval", "0.3", "script.py", "foo", "bar"]
    )
    assert script == "script.py"
    assert script_args == ["foo", "bar"]
    assert interval == 0.3


def test_parse_watch_argv_default_interval():
    from ppg3.__main__ import _parse_watch_argv

    script, script_args, interval = _parse_watch_argv(["script.py", "a", "b"])
    assert script == "script.py"
    assert script_args == ["a", "b"]
    assert interval == 0.5


def test_parse_watch_argv_missing_script_raises():
    from ppg3.__main__ import _parse_watch_argv

    with pytest.raises(SystemExit):
        _parse_watch_argv(["--interval", "0.1"])


# --------------------------------------------------------------------------
# E2E: a real `python -m ppg3 watch` subprocess.
# --------------------------------------------------------------------------

_DEADLINE = 30.0
_POLL = 0.05


def _wait_until(predicate, deadline=_DEADLINE, poll=_POLL, description="condition"):
    start = time.monotonic()
    while time.monotonic() - start < deadline:
        if predicate():
            return
        time.sleep(poll)
    raise AssertionError(f"timed out after {deadline}s waiting for: {description}")


def _write_pipeline_script(path):
    path.write_text(
        "import sys\n"
        "import ppg3\n"
        "from ppg3.tools import PyEnv\n"
        "\n"
        "leaf_path, store_dir, project_dir, cat_bin = sys.argv[1:5]\n"
        "g = ppg3.new(\n"
        "    stores=[ppg3.Store('main', store_dir)],\n"
        "    default_python=PyEnv.current(),\n"
        "    project_dir=project_dir,\n"
        "    frozen=False,\n"
        "    paranoid=True,\n"
        ")\n"
        "ppg3.CommandJob(\n"
        "    view={'out': 'out.txt'},\n"
        "    argv=['/bin/sh', '-c', cat_bin + ' ' + leaf_path + ' > {out:out}'],\n"
        "    inputs={'leaf': ppg3.File(leaf_path)},\n"
        ")\n"
        "ppg3.run(g, project_id='watch-e2e')\n"
    )


@requires_core
def test_e2e_watch_detects_change_ephemeral_generation_and_clean_sigint(tmp_path):
    cat_bin = shutil.which("cat") or "/bin/cat"
    script = tmp_path / "pipeline.py"
    _write_pipeline_script(script)
    leaf = tmp_path / "leaf.txt"
    leaf.write_text("v1\n")
    store_dir = tmp_path / "store"
    store_dir.mkdir()
    project_dir = tmp_path / ".ppg3"
    views_dir = project_dir / "views"

    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "ppg3",
            "watch",
            str(script),
            "--interval",
            "0.1",
            str(leaf),
            str(store_dir),
            str(project_dir),
            cat_bin,
        ],
        cwd=str(tmp_path),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        _wait_until(
            lambda: (views_dir / "1").is_dir(),
            description="generation 1 to appear",
        )
        outputs = tmp_path / "outputs"
        _wait_until(
            lambda: outputs.is_symlink() or outputs.is_dir(),
            description="outputs/ view symlink to appear",
        )
        _wait_until(
            lambda: (outputs / "out.txt").exists()
            and (outputs / "out.txt").read_text() == "v1\n",
            description="outputs/out.txt == 'v1\\n'",
        )
        assert (views_dir / "1" / ".ephemeral").is_file()

        # --- modify the leaf input: expect a second, ephemeral generation
        leaf.write_text("v2\n")
        _wait_until(
            lambda: (views_dir / "2").is_dir(),
            description="generation 2 to appear",
        )
        assert (views_dir / "2" / ".ephemeral").is_file()
        _wait_until(
            lambda: (outputs / "out.txt").read_text() == "v2\n",
            description="outputs/out.txt updated to 'v2\\n'",
        )

        # --- clean SIGINT exit
        proc.send_signal(signal.SIGINT)
        try:
            returncode = proc.wait(timeout=_DEADLINE)
        except subprocess.TimeoutExpired:
            proc.kill()
            raise AssertionError("process did not exit within deadline after SIGINT")
        assert returncode == 0
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)


@requires_core
def test_e2e_watch_survives_syntax_error_and_resumes(tmp_path):
    cat_bin = shutil.which("cat") or "/bin/cat"
    script = tmp_path / "pipeline.py"
    _write_pipeline_script(script)
    good_source = script.read_text()
    leaf = tmp_path / "leaf.txt"
    leaf.write_text("v1\n")
    store_dir = tmp_path / "store"
    store_dir.mkdir()
    project_dir = tmp_path / ".ppg3"
    views_dir = project_dir / "views"

    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "ppg3",
            "watch",
            str(script),
            "--interval",
            "0.1",
            str(leaf),
            str(store_dir),
            str(project_dir),
            cat_bin,
        ],
        cwd=str(tmp_path),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        _wait_until(
            lambda: (views_dir / "1").is_dir(),
            description="generation 1 to appear",
        )

        # --- break the script; the loop must survive ---------------------
        script.write_text(good_source + "\nthis is not python (((\n")
        # Give the watcher a couple of poll cycles to notice + fail + loop.
        time.sleep(0.6)
        assert proc.poll() is None, "subprocess died on a broken pipeline script"

        # --- fix it again; a fresh generation must eventually appear -----
        script.write_text(good_source)
        _wait_until(
            lambda: (views_dir / "2").is_dir(),
            description="generation 2 to appear after fixing the script",
        )
        assert proc.poll() is None

        proc.send_signal(signal.SIGINT)
        try:
            returncode = proc.wait(timeout=_DEADLINE)
        except subprocess.TimeoutExpired:
            proc.kill()
            raise AssertionError("process did not exit within deadline after SIGINT")
        assert returncode == 0

        stderr_output = proc.stderr.read()
        assert "SyntaxError" in stderr_output
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)
