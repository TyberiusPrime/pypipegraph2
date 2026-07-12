"""End-to-end smoke test (CONTRACT.md "Testing bar"): a real ``ppg3._core``
extension, real subprocess/template-forked jobs, real store/view filesystem
state. Skipped entirely if the extension isn't built (`requires_core`).

Scenario per CONTRACT.md: "two-job pipeline (CommandJob producing a file,
FileJob python callback consuming it) runs, produces a view; second run =
all hits, zero builds; parameter flip back = hits (§12.3 oracle)." Plus a
couple of adjacent scenarios (`GraphJob` expansion, partial-failure view
isolation) that exercise the rest of the PyO3 boundary this WP owns, and
(forkserver work package, PPG3_DESIGN.md §6.4) a handful of scenarios
specifically covering warm-template dispatch: template reuse across jobs,
preload imports actually landing in the callback's `sys.modules`, the
template's env baseline, the `forkserver=False` opt-out, and a determinism
check that forkserver-on and forkserver-off runs of the same pipeline hash
identically (the template must not poison keys).

`ppg3.new(...)` defaults to `forkserver=True`, so every test in this file
*except* the explicit off-path below already exercises real warm-template
dispatch for its `FileJob`s — that's not a special mode, it's just how
`ppg3.run()` behaves now.
"""

import json
import os

import pytest

import ppg3
from ppg3.run import PPGRunError
from ppg3.tools import PyEnv

from conftest import requires_core


def _make_summary(io):
    """Module-level (not a closure) so it is also valid under source-mode
    transport (localscope-clean: only `io` + builtins), even though the
    dev venv has cloudpickle installed and will prefer that path."""
    with open(io.input("greeting")) as fh:
        content = fh.read()
    with open(io.path("summary"), "w") as fh:
        fh.write(content.strip().upper() + str(io.params["cfg"]["n"]))


def _make_graph(tmp_path, n=1):
    store_dir = tmp_path / "store"
    store_dir.mkdir(exist_ok=True)
    g = ppg3.new(
        stores=[ppg3.Store("main", str(store_dir))],
        default_python=PyEnv.current(),
        project_dir=str(tmp_path / ".ppg3"),
        frozen=False,
        # Force source-mode transport (see STATUS.md "cloudpickle
        # by-reference gap"): cloudpickle pickles a module-level function
        # *by reference* when it believes the defining module is
        # importable, which `test_e2e` is in the pytest parent process but
        # not in the `python -I` shim subprocess (isolated mode does not
        # inherit pytest's rootdir sys.path insertion). `paranoid=True`
        # sidesteps this by always using the localscope-checked,
        # ship-the-source transport instead.
        paranoid=True,
    )
    cmd_job = ppg3.CommandJob(
        view={"greeting": "greeting.txt"},
        argv=["/bin/sh", "-c", "echo hello > {out:greeting}"],
    )
    ppg3.FileJob(
        view={"summary": "summary.txt"},
        run=_make_summary,
        inputs={"greeting": cmd_job, "cfg": ppg3.Params({"n": n})},
    )
    return g


@requires_core
def test_e2e_two_job_pipeline_build_then_hit_then_param_flip(tmp_path):
    # --- first run: everything builds -------------------------------
    g1 = _make_graph(tmp_path, n=1)
    r1 = ppg3.run(g1, project_id="e2e")
    assert sorted(r1.built) == ["greeting.txt", "summary.txt"]
    assert r1.hits == []
    assert r1.failed == {}
    assert r1.generation == 1

    outputs = tmp_path / "outputs"
    assert outputs.is_symlink() or outputs.is_dir()
    assert (outputs / "greeting.txt").read_text() == "hello\n"
    assert (outputs / "summary.txt").read_text() == "HELLO1"

    # --- second run, fresh Graph object, same project/store: all hits -
    g2 = _make_graph(tmp_path, n=1)
    r2 = ppg3.run(g2, project_id="e2e")
    assert r2.built == []
    assert sorted(r2.hits) == ["greeting.txt", "summary.txt"]
    assert r2.failed == {}
    assert r2.generation == 2

    # --- param flip: only the downstream job rebuilds ------------------
    g3 = _make_graph(tmp_path, n=2)
    r3 = ppg3.run(g3, project_id="e2e")
    assert r3.built == ["summary.txt"]
    assert r3.hits == ["greeting.txt"]
    assert (outputs / "summary.txt").read_text() == "HELLO2"

    # --- flip back: all hits again (§12.3 oracle) -----------------------
    g4 = _make_graph(tmp_path, n=1)
    r4 = ppg3.run(g4, project_id="e2e")
    assert r4.built == []
    assert sorted(r4.hits) == ["greeting.txt", "summary.txt"]
    assert (outputs / "summary.txt").read_text() == "HELLO1"


@requires_core
def test_e2e_graphjob_expansion_runs_and_publishes(tmp_path):
    store_dir = tmp_path / "store"
    store_dir.mkdir()
    g = ppg3.new(
        stores=[ppg3.Store("main", str(store_dir))],
        default_python=PyEnv.current(),
        project_dir=str(tmp_path / ".ppg3"),
        frozen=False,
    )

    def expand():
        # A closure is fine here: GraphJob callbacks run in-process via
        # `RunCallbacks.expand_graph_job` (no (de)serialization at all).
        ppg3.CommandJob(
            view={"out": "generated.txt"},
            argv=["/bin/sh", "-c", "echo generated > {out:out}"],
        )

    ppg3.GraphJob(expand, name="expand-job")
    r = ppg3.run(g, project_id="graphjob-e2e")
    assert r.failed == {}
    assert "generated.txt" in r.built
    assert (tmp_path / "outputs" / "generated.txt").read_text() == "generated\n"


@requires_core
def test_e2e_partial_failure_raises_and_leaves_view_untouched(tmp_path):
    store_dir = tmp_path / "store"
    store_dir.mkdir()
    g = ppg3.new(
        stores=[ppg3.Store("main", str(store_dir))],
        default_python=PyEnv.current(),
        project_dir=str(tmp_path / ".ppg3"),
        frozen=False,
    )
    ppg3.CommandJob(view={"ok": "ok.txt"}, argv=["/bin/sh", "-c", "echo ok > {out:ok}"])
    ppg3.CommandJob(view={"bad": "bad.txt"}, argv=["/bin/sh", "-c", "exit 7"])

    with pytest.raises(PPGRunError) as exc_info:
        ppg3.run(g, project_id="fail-e2e")

    result = exc_info.value.result
    assert "bad.txt" in result.failed
    assert result.generation is None
    assert not (tmp_path / "outputs").exists()


# --------------------------------------------------------------------------
# Forkserver (§6.4) — warm-template dispatch scenarios.
#
# All callbacks below are module-level with body-local imports only (no
# free variables beyond their `io` parameter + builtins) so they are valid
# under *both* transports (`paranoid=True` forces source-mode, which is
# strictly localscope-checked — see the "cloudpickle by-reference gap" wart
# noted on `_make_summary` above).
# --------------------------------------------------------------------------


def _write_ppid(io):
    import os

    with open(io.path(), "w") as fh:
        fh.write(str(os.getppid()))


def _write_preload_check(io):
    import sys

    with open(io.path(), "w") as fh:
        fh.write(str("decimal" in sys.modules))


def _write_env_dump(io):
    import json
    import os

    with open(io.path(), "w") as fh:
        json.dump(dict(os.environ), fh)


def _make_env_pipeline(base_dir, forkserver):
    """Same two-job pipeline as `_make_graph` above (CommandJob ->
    FileJob(_make_summary)), parameterized by `forkserver=` and rooted
    under its own `base_dir` (so an on/off pair never shares a store or
    project — a store hit would let the "off" run skip the executor
    entirely and prove nothing)."""
    store_dir = base_dir / "store"
    store_dir.mkdir()
    g = ppg3.new(
        stores=[ppg3.Store("main", str(store_dir))],
        default_python=PyEnv.current(),
        project_dir=str(base_dir / ".ppg3"),
        frozen=False,
        paranoid=True,
        forkserver=forkserver,
    )
    cmd_job = ppg3.CommandJob(
        view={"greeting": "greeting.txt"},
        argv=["/bin/sh", "-c", "echo hello > {out:greeting}"],
    )
    ppg3.FileJob(
        view={"summary": "summary.txt"},
        run=_make_summary,
        inputs={"greeting": cmd_job, "cfg": ppg3.Params({"n": 1})},
    )
    return g


@requires_core
def test_e2e_warm_template_reuse(tmp_path):
    """Two independent `FileJob`s under the same `PyEnv` (same interpreter,
    same preload list) must be dispatched through the *same* template
    process: each callback's `os.getppid()` (its direct parent — the
    template that forked it, since there is no intervening `exec`) must be
    identical across both jobs, and must not be the test driver's own pid
    (the coordinator never forks — §6.4)."""
    store_dir = tmp_path / "store"
    store_dir.mkdir()
    g = ppg3.new(
        stores=[ppg3.Store("main", str(store_dir))],
        default_python=PyEnv.current(preload=["json"]),
        project_dir=str(tmp_path / ".ppg3"),
        frozen=False,
        paranoid=True,
    )
    ppg3.FileJob(view={"a": "a.txt"}, run=_write_ppid)
    ppg3.FileJob(view={"b": "b.txt"}, run=_write_ppid)
    r = ppg3.run(g, project_id="warm-template")
    assert r.failed == {}

    outputs = tmp_path / "outputs"
    ppid_a = (outputs / "a.txt").read_text()
    ppid_b = (outputs / "b.txt").read_text()
    assert ppid_a == ppid_b
    assert ppid_a != str(os.getpid())


@requires_core
def test_e2e_preload_actually_imported(tmp_path):
    """`PyEnv.current(preload=["decimal"])` must make `decimal` already
    present in `sys.modules` *before* the callback runs — proves the
    template really imported the preload list at startup rather than this
    just being a no-op flag. `decimal` (not `json`, which `_shim.py` itself
    imports) is picked specifically so a positive result can only come from
    the preload mechanism, never an incidental import elsewhere on the
    path to the callback."""
    store_dir = tmp_path / "store"
    store_dir.mkdir()
    g = ppg3.new(
        stores=[ppg3.Store("main", str(store_dir))],
        default_python=PyEnv.current(preload=["decimal"]),
        project_dir=str(tmp_path / ".ppg3"),
        frozen=False,
        paranoid=True,
    )
    ppg3.FileJob(view={"out": "check.txt"}, run=_write_preload_check)
    r = ppg3.run(g, project_id="preload-check")
    assert r.failed == {}
    content = (tmp_path / "outputs" / "check.txt").read_text()
    assert content == "True"


@requires_core
def test_e2e_template_env_scrubbed(tmp_path, monkeypatch):
    """A job dispatched through a template must see exactly the §6.1a
    baseline env (`HOME`/`TMPDIR`/`TZ`/`LC_ALL`) and nothing the *template
    process' own* (weakly-hermetic, host-inheriting — see STATUS.md)
    environment carried — in particular a var set on the test driver's own
    process must never leak into the callback."""
    monkeypatch.setenv("PPG3_TEST_SECRET", "leak-me-not")
    store_dir = tmp_path / "store"
    store_dir.mkdir()
    g = ppg3.new(
        stores=[ppg3.Store("main", str(store_dir))],
        default_python=PyEnv.current(),
        project_dir=str(tmp_path / ".ppg3"),
        frozen=False,
        paranoid=True,
    )
    ppg3.FileJob(view={"out": "env.json"}, run=_write_env_dump)
    r = ppg3.run(g, project_id="env-scrub")
    assert r.failed == {}
    env = json.loads((tmp_path / "outputs" / "env.json").read_text())
    assert env.get("HOME") == "/tmp"
    assert env.get("TMPDIR") == "/tmp"
    assert env.get("TZ") == "UTC"
    assert env.get("LC_ALL") == "C.UTF-8"
    assert "PPG3_TEST_SECRET" not in env


@requires_core
def test_e2e_forkserver_off_fallback_matches_forkserver_on(tmp_path):
    """`forkserver=False` must still produce a working, correct run (the
    old cold-exec-per-job `NoneExecutor` path) — and, dispatched against a
    *separate* store/project so no store hit can short-circuit either run,
    its `(ik, oh)` for the shared job must match the `forkserver=True` run
    bit-for-bit. Same recipe/inputs/runtime in, only the executor differs;
    if the template's fork-time state leaked into what gets hashed or
    produced, these would diverge (§9 determinism enforcement, and this
    work package's requirement that a template "does not poison keys")."""
    on_dir = tmp_path / "on"
    on_dir.mkdir()
    off_dir = tmp_path / "off"
    off_dir.mkdir()

    g_on = _make_env_pipeline(on_dir, forkserver=True)
    r_on = ppg3.run(g_on, project_id="fs-on")
    assert r_on.failed == {}
    assert sorted(r_on.built) == ["greeting.txt", "summary.txt"]

    g_off = _make_env_pipeline(off_dir, forkserver=False)
    r_off = ppg3.run(g_off, project_id="fs-off")
    assert r_off.failed == {}
    assert sorted(r_off.built) == ["greeting.txt", "summary.txt"]

    on_summary = (on_dir / "outputs" / "summary.txt").read_text()
    off_summary = (off_dir / "outputs" / "summary.txt").read_text()
    assert on_summary == off_summary == "HELLO1"

    on_ik, on_oh = r_on.job_entries["summary.txt"]
    off_ik, off_oh = r_off.job_entries["summary.txt"]
    assert on_ik == off_ik, "forkserver on/off must derive the same input key"
    assert on_oh == off_oh, "forkserver on/off must produce the same output hash"
