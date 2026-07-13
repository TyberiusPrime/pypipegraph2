"""§6.7 session mode: forkserver templates and loader memos outlive
individual ppg3.run() calls inside one coordinator process, until
ppg3.session_stop()."""

import os

import pytest

import importlib

import ppg3
from ppg3.tools import PyEnv

# `ppg3.run` the attribute is the run() *function* (see watch.py's note on
# the submodule shadowing); import_module returns the actual module.
run_mod = importlib.import_module("ppg3.run")

from conftest import requires_core


def _write_ppid(io):
    import os  # body-local: resolves inside the job env (§6.6 idiom)

    with open(io.path("out"), "w") as fh:
        fh.write(str(os.getppid()))


def _fresh_graph(tmp_path, tag, n):
    """Independent store+project per call so every run really builds."""
    store_dir = tmp_path / f"store-{tag}"
    store_dir.mkdir(exist_ok=True)
    g = ppg3.new(
        stores=[ppg3.Store("main", str(store_dir))],
        default_python=PyEnv.current(),
        project_dir=str(tmp_path / f".ppg3-{tag}"),
        frozen=False,
        paranoid=True,
    )
    ppg3.FileJob(
        view={"out": "out.txt"},
        run=_write_ppid,
        inputs={"cfg": ppg3.Params({"n": n})},
    )
    return g, tmp_path / f"outputs"


def _run_and_read_ppid(tmp_path, tag, n):
    g, _ = _fresh_graph(tmp_path, tag, n)
    r = ppg3.run(g, project_id=f"session-{tag}")
    assert r.failed == {}
    # outputs symlink lives next to the project dir (sibling of .ppg3-*),
    # which is tmp_path for every tag here — read via the generation the
    # run just wrote instead, to avoid tag collisions on the shared
    # `outputs` symlink.
    view = tmp_path / f".ppg3-{tag}" / "views" / "current" / "out.txt"
    return view.read_text()


@requires_core
def test_session_templates_survive_across_runs(tmp_path):
    """Two separate ppg3.run() calls (distinct graphs, stores, projects)
    must fork their jobs from the SAME warm template process (§6.7)."""
    ppg3.session_stop()  # clean slate regardless of test order
    ppid_1 = _run_and_read_ppid(tmp_path, "one", 1)
    ppid_2 = _run_and_read_ppid(tmp_path, "two", 2)
    assert ppid_1 == ppid_2
    assert ppid_1 != str(os.getpid())
    core = run_mod.get_core()
    assert core.session_template_count(run_mod._session) >= 1
    ppg3.session_stop()


@requires_core
def test_session_stop_kills_templates_and_next_run_respawns(tmp_path):
    ppg3.session_stop()
    ppid_1 = _run_and_read_ppid(tmp_path, "a", 1)
    core = run_mod.get_core()
    session = run_mod._session
    assert core.session_template_count(session) >= 1

    ppg3.session_stop()
    # the old session object's templates are shut down and the module slot
    # is cleared — a fresh run must spawn a NEW template
    assert run_mod._session is None
    assert core.session_template_count(session) == 0

    ppid_2 = _run_and_read_ppid(tmp_path, "b", 2)
    assert ppid_2 != ppid_1
    ppg3.session_stop()


_LOADER_CALLS = []


def _counting_loader(io):
    _LOADER_CALLS.append(1)


@requires_core
def test_loader_memo_persists_across_runs_until_session_stop(tmp_path):
    """§6.7: loader-layer memos persist across runs in the session, keyed
    by ik — an InProcess job with an unchanged key document runs once per
    session, not once per run."""
    ppg3.session_stop()
    del _LOADER_CALLS[:]

    def build_and_run(tag):
        store_dir = tmp_path / "store"
        store_dir.mkdir(exist_ok=True)
        g = ppg3.new(
            stores=[ppg3.Store("main", str(store_dir))],
            default_python=PyEnv.current(),
            project_dir=str(tmp_path / f".ppg3-{tag}"),
            frozen=False,
            paranoid=True,
        )
        with pytest.warns(UserWarning):
            ppg3.UnsandboxedJob(
                run=_counting_loader,
                name="loader",
                inputs={"cfg": ppg3.Params({"n": 1})},
            )
        r = ppg3.run(g, project_id="loader-memo")
        assert r.failed == {}

    build_and_run("r1")
    assert len(_LOADER_CALLS) == 1
    build_and_run("r2")  # same key document -> same ik -> memo hit
    assert len(_LOADER_CALLS) == 1

    ppg3.session_stop()  # clears loader memos
    build_and_run("r3")
    assert len(_LOADER_CALLS) == 2
    ppg3.session_stop()


@requires_core
def test_session_stop_idempotent():
    ppg3.session_stop()
    ppg3.session_stop()  # must not raise
