"""End-to-end smoke test (CONTRACT.md "Testing bar"): a real ``ppg3._core``
extension, real ``NoneExecutor`` subprocesses, real store/view filesystem
state. Skipped entirely if the extension isn't built (`requires_core`).

Scenario per CONTRACT.md: "two-job pipeline (CommandJob producing a file,
FileJob python callback consuming it) runs, produces a view; second run =
all hits, zero builds; parameter flip back = hits (§12.3 oracle)." Plus a
couple of adjacent scenarios (`GraphJob` expansion, partial-failure view
isolation) that exercise the rest of the PyO3 boundary this WP owns.
"""

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
