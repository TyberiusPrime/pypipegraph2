"""Tests for jj (jujutsu) support (``ppg3.new(jj=True)``, ``ppg3/jj.py``).

Neither this container nor every CI box has a real ``jj`` binary, so most
tests drive :mod:`ppg3.jj` through a scripted fake substituted via the
``PPG3_JJ`` environment variable (the module's documented override point).
The fake reads its canned answers from a JSON state file (``FAKE_JJ_STATE``)
and dispatches on the same argv shapes the real binary would see, so the
module under test runs its real subprocess path end to end. A final
integration test runs against a real ``jj`` when one is on PATH.
"""

import json
import os
import shutil
import stat
from pathlib import Path

import pytest

import ppg3
from ppg3 import jj
from ppg3.tools import PyEnv

from conftest import requires_core

requires_real_jj = pytest.mark.skipif(
    shutil.which("jj") is None, reason="real jj binary not on PATH"
)

FAKE_JJ_SCRIPT = """\
#!/usr/bin/env python3
import json, os, sys

state = json.load(open(os.environ["FAKE_JJ_STATE"]))
args = sys.argv[1:]

if args[:2] == ["workspace", "root"]:
    cwd = os.path.realpath(os.getcwd())
    root = os.path.realpath(state["root"])
    if cwd == root or cwd.startswith(root + os.sep) or root == os.sep:
        print(state["root"])
        sys.exit(0)
    sys.stderr.write('Error: There is no jj repo in "."\\n')
    sys.exit(1)
if args[:2] == ["file", "list"]:
    for f in state["tracked"]:
        print(f)
    sys.exit(0)
if args and args[0] == "log":
    r = args[args.index("-r") + 1]
    if r == "@":
        print(state["commit_id"])
        print(state["change_id"])
        print("true" if state["empty"] else "false")
    elif r == "@-":
        for p in state["parents"]:
            print(p)
    else:
        sys.stderr.write("fake jj: unhandled revset %r\\n" % r)
        sys.exit(1)
    sys.exit(0)
if args[:2] == ["op", "log"]:
    print(state["op_id"])
    sys.exit(0)
sys.stderr.write("fake jj: unhandled args %r\\n" % (args,))
sys.exit(1)
"""


class FakeJJ:
    """Install the fake jj binary + a mutable state file."""

    def __init__(self, tmp_path: Path, monkeypatch):
        self.script = tmp_path / "fake-jj"
        self.script.write_text(FAKE_JJ_SCRIPT)
        self.script.chmod(self.script.stat().st_mode | stat.S_IEXEC)
        self.state_path = tmp_path / "fake-jj-state.json"
        monkeypatch.setenv("PPG3_JJ", str(self.script))
        monkeypatch.setenv("FAKE_JJ_STATE", str(self.state_path))

    def set_state(
        self,
        root,
        tracked=(),
        commit_id="c" * 40,
        change_id="z" * 32,
        op_id="0" * 64,
        empty=True,
        parents=("p" * 40,),
    ):
        self.state_path.write_text(
            json.dumps(
                {
                    "root": str(root),
                    "tracked": list(tracked),
                    "commit_id": commit_id,
                    "change_id": change_id,
                    "op_id": op_id,
                    "empty": empty,
                    "parents": list(parents),
                }
            )
        )


@pytest.fixture
def fake_jj(tmp_path, monkeypatch):
    return FakeJJ(tmp_path, monkeypatch)


# ---------------------------------------------------------------- jj module


def test_find_workspace_root_inside_and_outside(fake_jj, tmp_path):
    root = tmp_path / "ws"
    (root / "sub").mkdir(parents=True)
    fake_jj.set_state(root)
    assert jj.find_workspace_root(str(root / "sub")) == str(root)

    outside = tmp_path / "elsewhere"
    outside.mkdir()
    assert jj.find_workspace_root(str(outside)) is None


def test_missing_binary_is_a_hard_error(tmp_path, monkeypatch):
    monkeypatch.setenv("PPG3_JJ", str(tmp_path / "does-not-exist"))
    with pytest.raises(jj.JJError, match="not found"):
        jj.find_workspace_root(str(tmp_path))
    with pytest.raises(jj.JJError, match="not found"):
        jj.capture_state(str(tmp_path))


def test_capture_state_committed(fake_jj, tmp_path):
    fake_jj.set_state(tmp_path, empty=True)
    state = jj.capture_state(str(tmp_path))
    assert state == {
        "backend": "jj",
        "commit_id": "c" * 40,
        "change_id": "z" * 32,
        "op_id": "0" * 64,
        "committed": True,
        "parent_commit_id": "p" * 40,
    }


def test_capture_state_dirty_working_copy_is_oplog(fake_jj, tmp_path):
    fake_jj.set_state(tmp_path, empty=False)
    state = jj.capture_state(str(tmp_path))
    assert state["committed"] is False


def test_capture_state_merge_working_copy_has_no_single_parent(fake_jj, tmp_path):
    fake_jj.set_state(tmp_path, parents=["a" * 40, "b" * 40])
    state = jj.capture_state(str(tmp_path))
    assert "parent_commit_id" not in state


def test_assert_sources_tracked_passes_for_tracked_files(fake_jj, tmp_path):
    root = tmp_path / "ws"
    (root / "sub").mkdir(parents=True)
    pipeline = root / "sub" / "pipeline.py"
    pipeline.write_text("# code\n")
    fake_jj.set_state(root, tracked=["sub/pipeline.py"])
    jj.assert_sources_tracked(str(root), [str(pipeline)])


def test_assert_sources_tracked_names_untracked_files(fake_jj, tmp_path):
    root = tmp_path / "ws"
    root.mkdir()
    rogue = root / "rogue.py"
    rogue.write_text("# code\n")
    fake_jj.set_state(root, tracked=["other.py"])
    with pytest.raises(jj.JJError, match="not tracked by jj") as excinfo:
        jj.assert_sources_tracked(str(root), [str(rogue)])
    assert "rogue.py" in str(excinfo.value)


def test_assert_sources_tracked_names_files_outside_workspace(fake_jj, tmp_path):
    root = tmp_path / "ws"
    root.mkdir()
    stranger = tmp_path / "outside.py"
    stranger.write_text("# code\n")
    fake_jj.set_state(root, tracked=[])
    with pytest.raises(jj.JJError, match="outside the jj workspace") as excinfo:
        jj.assert_sources_tracked(str(root), [str(stranger)])
    assert "outside.py" in str(excinfo.value)


# ------------------------------------------------------- source recording


def _summary_callback(io):
    with open(io.input("greeting")) as fh:
        content = fh.read()
    with open(io.path("summary"), "w") as fh:
        fh.write(content.strip().upper())


def test_graph_records_job_source_paths(tmp_path):
    g = ppg3.new(
        stores=[ppg3.Store("main", str(tmp_path / "store"))],
        default_python=PyEnv.current(),
        project_dir=str(tmp_path / ".ppg3"),
        frozen=False,
        paranoid=True,
        jj=True,
    )
    assert g.jj

    this_file = os.path.abspath(__file__)

    # Constructor call site (this file), even for a callback-less job kind.
    ppg3.CommandJob(
        view={"greeting": "greeting.txt"},
        argv=["/bin/sh", "-c", "echo hi > {out:greeting}"],
    )
    assert this_file in g.source_paths()

    # Callable callbacks record their defining file (here: also this file).
    ppg3.FileJob(
        view={"summary": "summary.txt"},
        run=_summary_callback,
        inputs={"greeting": g.jobs["greeting.txt"]},
    )
    assert g.source_paths().count(this_file) == 1  # deduplicated

    # Source refs record path + includes.
    src_file = tmp_path / "callbacks.py"
    src_file.write_text("def cb(io):\n    pass\n")
    inc_file = tmp_path / "helpers.py"
    inc_file.write_text("# helpers\n")
    ppg3.FileJob(
        view={"extra": "extra.txt"},
        run=ppg3.Source(f"{src_file}::cb", includes=[str(inc_file)]),
    )
    sources = g.source_paths()
    assert str(src_file) in sources
    assert str(inc_file) in sources

    # Leaf *data* files are watched, but are NOT job sources.
    data = tmp_path / "data.csv"
    data.write_text("1,2\n")
    ppg3.CommandJob(
        view={"copy": "copy.csv"},
        argv=["/bin/cp", ppg3.In("d"), ppg3.Out("copy")],
        inputs={"d": ppg3.File(str(data))},
    )
    g.job_defs()  # File paths are recorded at lowering time
    assert str(data) in g.watched_paths()
    assert str(data) not in g.source_paths()


# ------------------------------------------------------------- run() wiring


def _tracked_for_root(paths, root="/"):
    return [os.path.relpath(os.path.realpath(p), root) for p in paths]


def _make_jj_graph(tmp_path):
    store_dir = tmp_path / "store"
    store_dir.mkdir(exist_ok=True)
    g = ppg3.new(
        stores=[ppg3.Store("main", str(store_dir))],
        default_python=PyEnv.current(),
        project_dir=str(tmp_path / ".ppg3"),
        frozen=False,
        paranoid=True,
        jj=True,
    )
    ppg3.CommandJob(
        view={"greeting": "greeting.txt"},
        argv=["/bin/sh", "-c", "echo hello > {out:greeting}"],
    )
    return g


@requires_core
def test_run_with_jj_records_vcs_in_generation_meta(fake_jj, tmp_path):
    g = _make_jj_graph(tmp_path)
    # Fake workspace rooted at "/" so this test file (the recorded call
    # site) is inside it; mark every known source as tracked.
    fake_jj.set_state(
        "/",
        tracked=_tracked_for_root(g.source_paths()),
        empty=False,
        commit_id="e" * 40,
        change_id="y" * 32,
        op_id="7" * 64,
    )
    r = ppg3.run(g, project_id="jj-e2e")
    assert r.failed == {}
    assert r.generation == 1

    meta = json.loads(
        (tmp_path / ".ppg3" / "views" / "1" / "meta.json").read_text()
    )
    assert meta["vcs"] == {
        "backend": "jj",
        "commit_id": "e" * 40,
        "change_id": "y" * 32,
        "op_id": "7" * 64,
        "committed": False,
        "parent_commit_id": "p" * 40,
    }


@requires_core
def test_run_with_jj_untracked_source_is_a_hard_error(fake_jj, tmp_path):
    g = _make_jj_graph(tmp_path)
    fake_jj.set_state("/", tracked=[])  # nothing tracked at all
    with pytest.raises(jj.JJError, match="not tracked by jj"):
        ppg3.run(g, project_id="jj-e2e-fail")
    # Hard error means: no generation was written.
    assert not (tmp_path / ".ppg3" / "views").exists()


@requires_core
def test_run_with_jj_outside_any_workspace_is_a_hard_error(fake_jj, tmp_path):
    g = _make_jj_graph(tmp_path)
    somewhere_else = tmp_path / "not-a-workspace-marker"
    fake_jj.set_state(str(somewhere_else))  # cwd is never inside this root
    with pytest.raises(jj.JJError, match="no jj workspace"):
        ppg3.run(g, project_id="jj-e2e-nows")


@requires_core
def test_run_without_jj_flag_still_writes_vcs_less_meta(tmp_path):
    store_dir = tmp_path / "store"
    store_dir.mkdir()
    g = ppg3.new(
        stores=[ppg3.Store("main", str(store_dir))],
        default_python=PyEnv.current(),
        project_dir=str(tmp_path / ".ppg3"),
        frozen=False,
        paranoid=True,
    )
    ppg3.CommandJob(
        view={"greeting": "greeting.txt"},
        argv=["/bin/sh", "-c", "echo hello > {out:greeting}"],
    )
    r = ppg3.run(g, project_id="no-jj")
    assert r.generation == 1
    meta = json.loads(
        (tmp_path / ".ppg3" / "views" / "1" / "meta.json").read_text()
    )
    assert "vcs" not in meta


# ------------------------------------------------------------ real jj (opt)


@requires_real_jj
def test_real_jj_capture_and_tracking(tmp_path, monkeypatch):
    monkeypatch.delenv("PPG3_JJ", raising=False)
    monkeypatch.setenv("JJ_USER", "ppg3-test")
    monkeypatch.setenv("JJ_EMAIL", "ppg3-test@example.invalid")
    root = tmp_path / "ws"
    root.mkdir()
    import subprocess

    subprocess.run(["jj", "git", "init"], cwd=root, check=True, capture_output=True)
    tracked = root / "pipeline.py"
    tracked.write_text("# pipeline\n")

    assert jj.find_workspace_root(str(root)) == str(root)
    assert jj.find_workspace_root(str(tmp_path)) is None

    state = jj.capture_state(str(root))
    assert state["backend"] == "jj"
    assert len(state["commit_id"]) == 40
    assert state["op_id"]
    # the new file makes the working copy non-empty -> op-log state
    assert state["committed"] is False

    # jj auto-tracks new files by default, so `tracked` passes...
    jj.assert_sources_tracked(str(root), [str(tracked)])
    # ...while a file outside the workspace still fails.
    outside = tmp_path / "outside.py"
    outside.write_text("# outside\n")
    with pytest.raises(jj.JJError, match="outside the jj workspace"):
        jj.assert_sources_tracked(str(root), [str(outside)])
