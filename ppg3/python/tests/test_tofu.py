"""Tests for ``ppg3.tofu`` (PPG3_DESIGN.md §7.6 TOFU source patcher).

Three tiers, per CONTRACT.md's testing conventions:

- Unit tests on the libcst-backed patcher (``tofu._patch_file``) against
  fixture source strings — no ``_core``, no real ``FetchJob``, a small
  ``_FakeJob`` stand-in that only needs ``.view``/``.url``/``._call_site``.
- Call-site recording tests (``FetchJob._call_site``, set by ``jobs.py``'s
  ``_record_call_site``) — needs a real ``Graph``/``FetchJob`` but no
  ``_core`` (call-site recording happens at ``__init__`` time, before any
  hashing).
- End-to-end (``requires_core``): a real pipeline script run twice via
  ``runpy``, first run TOFU-pins the hash into the script on disk, second
  run (fresh ``Graph``, same script) is an all-hits re-run with the patch
  a no-op (blake3 already pinned).
"""

from __future__ import annotations

import inspect
import json
import os
import runpy

import pytest

import ppg3
from ppg3 import tofu
from ppg3.tools import PyEnv

from conftest import requires_core

# A stand-in for a real `FetchJob` in the unit tests below: `tofu._patch_file`
# and `tofu.run_tofu_pass` only ever touch `.view["file"]` (the view path),
# `.url`, and `._call_site` on a job — never `.job_def()`/`.blake3`/anything
# that would need `graph.default_python` or hashing.
class _FakeJob:
    def __init__(self, view_path, url, call_site):
        self.view = {"file": view_path}
        self.url = url
        self._call_site = call_site
        self.id = view_path


_DIGEST = "ab" * 32


def _patch(tmp_path, source, lineno, view="x", url="y", digest=_DIGEST):
    path = tmp_path / "pipeline.py"
    path.write_text(source)
    job = _FakeJob(view, url, (str(path), lineno))
    unresolved = tofu._patch_file(str(path), [(lineno, job, digest)])
    return path.read_text(), unresolved


# --------------------------------------------------------------------------
# Unit: patcher on fixture source strings
# --------------------------------------------------------------------------


def test_patch_inserts_missing_kwarg_exact_text(tmp_path, capsys):
    src = 'import ppg3\njob = ppg3.FetchJob(view="x", url="y")\n'
    out, unresolved = _patch(tmp_path, src, 2)
    assert unresolved == []
    assert out == f'import ppg3\njob = ppg3.FetchJob(view="x", url="y", blake3="{_DIGEST}")\n'
    captured = capsys.readouterr()
    assert "pinned x (abababab…) in" in captured.out
    assert captured.out.rstrip().endswith(":2")


def test_patch_replaces_none_value(tmp_path):
    src = 'job = ppg3.FetchJob(view="x", url="y", blake3=None)\n'
    out, unresolved = _patch(tmp_path, src, 1)
    assert unresolved == []
    assert out == f'job = ppg3.FetchJob(view="x", url="y", blake3="{_DIGEST}")\n'


def test_patch_replaces_none_value_regardless_of_arg_order(tmp_path):
    src = 'job = ppg3.FetchJob(blake3=None, view="x", url="y")\n'
    out, unresolved = _patch(tmp_path, src, 1)
    assert unresolved == []
    assert out == f'job = ppg3.FetchJob(blake3="{_DIGEST}", view="x", url="y")\n'


def test_patch_aliased_bare_import(tmp_path):
    """``from ppg3 import FetchJob as FJ`` — the call site is a bare `FJ(...)`
    with no attribute access, so matching needs the import-alias scan."""
    src = 'from ppg3 import FetchJob as FJ\njob = FJ(view="x", url="y")\n'
    out, unresolved = _patch(tmp_path, src, 2)
    assert unresolved == []
    assert out == (
        'from ppg3 import FetchJob as FJ\n'
        f'job = FJ(view="x", url="y", blake3="{_DIGEST}")\n'
    )


def test_patch_aliased_module_import(tmp_path):
    """``import ppg3 as p`` — attribute-form calls resolve via the last
    attribute segment regardless of what the module itself is aliased to."""
    src = 'import ppg3 as p\njob = p.FetchJob(view="x", url="y")\n'
    out, unresolved = _patch(tmp_path, src, 2)
    assert unresolved == []
    assert out == (
        'import ppg3 as p\n'
        f'job = p.FetchJob(view="x", url="y", blake3="{_DIGEST}")\n'
    )


def test_patch_two_calls_one_line_falls_back(tmp_path, capsys):
    src = 'a = ppg3.FetchJob(view="x", url="y"); b = ppg3.FetchJob(view="z", url="w")\n'
    out, unresolved = _patch(tmp_path, src, 1)
    assert out == src  # completely untouched
    assert len(unresolved) == 1
    assert "found 2" in capsys.readouterr().err


def test_patch_zero_matching_calls_falls_back(tmp_path, capsys):
    src = "x = 1\n"
    out, unresolved = _patch(tmp_path, src, 1)
    assert out == src
    assert len(unresolved) == 1
    assert "found 0" in capsys.readouterr().err


def test_patch_multiline_call_starting_at_recorded_line(tmp_path):
    """The call STARTS at the recorded line (kwargs on following lines) —
    the design's own example of the trickiest call-site match. Doesn't need
    to be byte-perfect (unlike the single-line case above), just valid and
    correctly pinned."""
    src = 'job = ppg3.FetchJob(\n    view="x",\n    url="y",\n)\n'
    out, unresolved = _patch(tmp_path, src, 1)
    assert unresolved == []
    assert f'blake3="{_DIGEST}"' in out
    # Still valid, parseable Python with the original args intact.
    import ast

    tree = ast.parse(out)
    call = tree.body[0].value
    kwargs = {kw.arg: kw.value.value for kw in call.keywords}
    assert kwargs == {"view": "x", "url": "y", "blake3": _DIGEST}


def test_patch_multiline_no_trailing_comma(tmp_path):
    src = 'job = ppg3.FetchJob(\n    view="x",\n    url="y"\n)\n'
    out, unresolved = _patch(tmp_path, src, 1)
    assert unresolved == []
    import ast

    tree = ast.parse(out)
    call = tree.body[0].value
    kwargs = {kw.arg: kw.value.value for kw in call.keywords}
    assert kwargs == {"view": "x", "url": "y", "blake3": _DIGEST}


def test_patch_tabs_and_comments_untouched_elsewhere(tmp_path):
    """Byte-diff only at the inserted kwarg — every other line (including a
    tab-indented function body and a trailing comment on the patched line
    itself) must come back identical."""
    src = (
        "import ppg3\n"
        "\n"
        "# a comment with a literal tab:\tafter it\n"
        "def f():\n"
        "\tpass\n"
        "\n"
        'job = ppg3.FetchJob(view="x", url="y")  # trailing comment\n'
    )
    out, unresolved = _patch(tmp_path, src, 7)
    assert unresolved == []
    before_lines = src.splitlines()
    after_lines = out.splitlines()
    assert len(before_lines) == len(after_lines)
    for i, (before, after) in enumerate(zip(before_lines, after_lines)):
        if i == 6:  # 0-indexed line 6 == source line 7, the patched line
            assert after != before
            assert after.startswith(f'job = ppg3.FetchJob(view="x", url="y", blake3="{_DIGEST}"')
            assert after.endswith(")  # trailing comment")
        else:
            assert after == before, f"line {i + 1} changed unexpectedly"


def test_patch_unreadable_file_falls_back(tmp_path, capsys):
    missing = tmp_path / "does-not-exist.py"
    job = _FakeJob("x", "y", (str(missing), 1))
    unresolved = tofu._patch_file(str(missing), [(1, job, _DIGEST)])
    assert len(unresolved) == 1
    assert "cannot read" in capsys.readouterr().err


def test_patch_unparseable_file_falls_back(tmp_path, capsys):
    path = tmp_path / "broken.py"
    path.write_text("def (:\n")
    job = _FakeJob("x", "y", (str(path), 1))
    unresolved = tofu._patch_file(str(path), [(1, job, _DIGEST)])
    assert len(unresolved) == 1
    assert "cannot parse" in capsys.readouterr().err


# --------------------------------------------------------------------------
# run_tofu_pass: grouping + graceful libcst-missing degradation
# --------------------------------------------------------------------------


class _FakeCore:
    def __init__(self, manifests):
        self._manifests = manifests

    def lookup(self, handle, ik):
        m = self._manifests.get(ik)
        return None if m is None else json.dumps(m)


def _fake_manifest(view_path, digest):
    return {"content": {view_path: {"blake3": digest, "mode": "0644", "size": 3}}}


@pytest.fixture
def graph(tmp_path):
    return ppg3.new(
        default_python=PyEnv.current(),
        project_dir=str(tmp_path / ".ppg3"),
        frozen=False,
    )


def test_run_tofu_pass_no_unpinned_jobs_is_a_no_op(graph, capsys):
    ppg3.FetchJob(view="a", url="http://example.invalid/a", blake3="cd" * 32, name="a")
    tofu.run_tofu_pass(graph, {"job_entries": {}}, core=None, handle=None)
    assert capsys.readouterr().out == ""


def test_run_tofu_pass_libcst_missing_prints_table(graph, tmp_path, capsys, monkeypatch):
    job = ppg3.FetchJob(view="inputs/x.bin", url="http://example.invalid/x", name="x")
    assert job.blake3 is None
    job._call_site = (str(tmp_path / "unused.py"), 1)
    report = {"job_entries": {"x": ("ik-x", "oh-x")}}
    core = _FakeCore({"ik-x": _fake_manifest("inputs/x.bin", "cd" * 32)})

    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "libcst":
            raise ImportError("simulated: libcst not installed")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    tofu.run_tofu_pass(graph, report, core, handle=None)
    captured = capsys.readouterr()
    assert "libcst is not installed" in captured.err
    assert "ppg3[tofu]" in captured.err
    assert "could not auto-patch" in captured.out
    assert "cd" * 32 in captured.out
    assert "http://example.invalid/x" in captured.out


def test_run_tofu_pass_no_call_site_falls_back_to_table(graph, capsys):
    job = ppg3.FetchJob(view="inputs/x.bin", url="http://example.invalid/x", name="x")
    job._call_site = None
    report = {"job_entries": {"x": ("ik-x", "oh-x")}}
    core = _FakeCore({"ik-x": _fake_manifest("inputs/x.bin", "cd" * 32)})
    tofu.run_tofu_pass(graph, report, core, handle=None)
    captured = capsys.readouterr()
    assert "could not auto-patch" in captured.out
    assert "cd" * 32 in captured.out


def test_run_tofu_pass_two_jobs_same_site_prints_table(graph, tmp_path, capsys):
    """Two `FetchJob`s sharing one call site (a loop) are never patched,
    even with libcst available — grouping happens before any patch
    attempt."""
    site = (str(tmp_path / "loop_pipeline.py"), 5)
    job_a = ppg3.FetchJob(view="a.bin", url="http://example.invalid/a", name="a")
    job_b = ppg3.FetchJob(view="b.bin", url="http://example.invalid/b", name="b")
    job_a._call_site = site
    job_b._call_site = site
    report = {"job_entries": {"a": ("ik-a", "oh-a"), "b": ("ik-b", "oh-b")}}
    core = _FakeCore(
        {
            "ik-a": _fake_manifest("a.bin", "11" * 32),
            "ik-b": _fake_manifest("b.bin", "22" * 32),
        }
    )
    tofu.run_tofu_pass(graph, report, core, handle=None)
    captured = capsys.readouterr()
    assert "could not auto-patch" in captured.out
    assert "11" * 32 in captured.out
    assert "22" * 32 in captured.out
    assert "pinned" not in captured.out


def test_run_tofu_pass_patches_real_file(graph, tmp_path, capsys):
    script = tmp_path / "one_job.py"
    script.write_text('job = ppg3.FetchJob(view="a.bin", url="http://example.invalid/a")\n')
    job = ppg3.FetchJob(view="a.bin", url="http://example.invalid/a", name="a")
    job._call_site = (str(script), 1)
    report = {"job_entries": {"a": ("ik-a", "oh-a")}}
    core = _FakeCore({"ik-a": _fake_manifest("a.bin", "33" * 32)})
    tofu.run_tofu_pass(graph, report, core, handle=None)
    captured = capsys.readouterr()
    assert f'pinned a.bin (33333333…) in {script}:1' in captured.out
    assert f'blake3="{"33" * 32}"' in script.read_text()


# --------------------------------------------------------------------------
# Call-site recording (jobs.py's FetchJob._call_site)
# --------------------------------------------------------------------------


def test_call_site_recorded_at_definition(graph):
    lineno = inspect.currentframe().f_lineno + 1
    job = ppg3.FetchJob(view="a", url="http://example.invalid/a", name="a")
    assert job._call_site == (os.path.abspath(__file__), lineno)


def test_call_site_shared_across_loop_iterations(graph):
    jobs = []
    for i in range(3):
        jobs.append(
            ppg3.FetchJob(view=f"a{i}", url=f"http://example.invalid/{i}", name=f"a{i}")
        )
    sites = {j._call_site for j in jobs}
    assert len(sites) == 1
    assert all(site is not None for site in sites)


def test_call_site_distinct_for_distinct_lines(graph):
    job_a = ppg3.FetchJob(view="a", url="http://example.invalid/a", name="a")
    job_b = ppg3.FetchJob(view="b", url="http://example.invalid/b", name="b")
    assert job_a._call_site != job_b._call_site
    assert job_a._call_site[0] == job_b._call_site[0]


def test_call_site_recorded_even_when_blake3_given(graph):
    job = ppg3.FetchJob(
        view="a", url="http://example.invalid/a", blake3="ab" * 32, name="a"
    )
    assert job._call_site is not None


# --------------------------------------------------------------------------
# E2E (requires_core): real run, real store, real source-patching
# --------------------------------------------------------------------------


def _write_pipeline_script(path, store_dir, project_dir, url):
    # Deliberately flat/single-line so the FetchJob(...) call's line number
    # is easy to reason about: line 5 (1-indexed) below.
    text = (
        "import ppg3\n"
        "from ppg3.tools import PyEnv\n"
        "\n"
        f"g = ppg3.new(stores=[ppg3.Store('main', {str(store_dir)!r})], "
        f"default_python=PyEnv.current(), project_dir={str(project_dir)!r}, "
        "frozen=False)\n"
        f"job = ppg3.FetchJob(view='inputs/data.bin', url={url!r})\n"
        "r = ppg3.run(g, project_id='tofu-e2e')\n"
    )
    path.write_text(text)
    return 5  # the "job = ppg3.FetchJob(...)" line


@requires_core
def test_e2e_tofu_pins_hash_then_second_run_is_all_hits(tmp_path, capsys):
    from ppg3 import _core

    payload = b"tofu e2e payload, pinned on first use"
    src_file = tmp_path / "source_data.bin"
    src_file.write_bytes(payload)
    url = src_file.resolve().as_uri()
    real_digest = _core.blake3_file(str(src_file))

    store_dir = tmp_path / "store"
    store_dir.mkdir()
    project_dir = tmp_path / ".ppg3"
    script = tmp_path / "pipeline.py"
    fetchjob_lineno = _write_pipeline_script(script, store_dir, project_dir, url)

    # --- first run: blake3=None is accepted (non-frozen), TOFU pins it ---
    ns1 = runpy.run_path(str(script), run_name="__main__")
    out1 = capsys.readouterr().out
    assert ns1["r"].failed == {}
    assert ns1["r"].built == ["inputs/data.bin"]
    assert ns1["r"].hits == []
    assert f"pinned inputs/data.bin ({real_digest[:8]}…) in {script}:{fetchjob_lineno}" in out1

    outputs = tmp_path / "outputs"
    assert (outputs / "inputs" / "data.bin").read_bytes() == payload

    patched_source = script.read_text()
    assert f'blake3=\'{real_digest}\'' in patched_source or f'blake3="{real_digest}"' in patched_source
    assert f"url={url!r}" in patched_source  # untouched elsewhere

    # --- second run: fresh Graph, same (now-patched) script: all hits, ---
    # --- zero table output, zero further patching -----------------------
    ns2 = runpy.run_path(str(script), run_name="__main__")
    out2 = capsys.readouterr().out
    assert ns2["r"].failed == {}
    assert ns2["r"].built == []
    assert ns2["r"].hits == ["inputs/data.bin"]
    assert "pinned" not in out2
    assert "ppg3 tofu" not in out2
    assert script.read_text() == patched_source  # byte-identical, no further edit


@requires_core
def test_e2e_tofu_frozen_mode_still_rejects_blake3_none(tmp_path):
    from ppg3.jobs import DefinitionError

    ppg3.new(
        default_python=PyEnv.current(),
        project_dir=str(tmp_path / ".ppg3"),
        frozen=True,
    )
    with pytest.raises(DefinitionError):
        ppg3.FetchJob(view="inputs/x.bin", url="http://example.invalid/x")
