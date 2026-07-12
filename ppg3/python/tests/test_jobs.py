import pytest

import ppg3
from ppg3.jobs import DefinitionError, In, Out, Tool, serialize_argv
from ppg3.tools import PyEnv

from conftest import requires_blake3


@pytest.fixture
def graph(tmp_path):
    g = ppg3.new(
        default_python=PyEnv.current(),
        project_dir=str(tmp_path / ".ppg3"),
        frozen=False,
    )
    yield g


def dummy_run(io):
    return None


@requires_blake3
def test_filejob_stable_id_from_view(graph):
    job = ppg3.FileJob(view={"counts": "results/counts.tsv"}, run=dummy_run)
    assert job.id == "results/counts.tsv"


@requires_blake3
def test_filejob_explicit_name_overrides(graph):
    job = ppg3.FileJob(view={"counts": "results/counts.tsv"}, run=dummy_run, name="my-job")
    assert job.id == "my-job"


@requires_blake3
def test_duplicate_job_id_rejected(graph):
    ppg3.FileJob(view={"a": "out.txt"}, run=dummy_run, name="dupe")
    with pytest.raises(DefinitionError):
        ppg3.FileJob(view={"a": "out2.txt"}, run=dummy_run, name="dupe")


def test_serialize_argv_placeholders():
    argv = [Tool("samtools"), "view", In("bam"), "-o", Out("sorted")]
    assert serialize_argv(argv) == [
        "{tool:samtools}",
        "view",
        "{in:bam}",
        "-o",
        "{out:sorted}",
    ]


def test_serialize_argv_bare_out():
    assert serialize_argv(["x", Out()]) == ["x", "{out}"]


def test_serialize_argv_rejects_bad_type():
    with pytest.raises(DefinitionError):
        serialize_argv(["ok", 5])


@requires_blake3
def test_commandjob_job_def_shape(graph):
    job = ppg3.CommandJob(
        view={"sorted.bam": "out/sorted.bam"},
        argv=[Tool("samtools"), "sort", In("bam"), "-o", Out()],
    )
    jd = job.job_def(graph)
    assert jd["id"] == "out/sorted.bam"
    assert jd["exec_template"] == {
        "Argv": {
            "argv": ["{tool:samtools}", "sort", "{in:bam}", "-o", "{out}"],
            "allow_network": False,
        }
    }
    assert jd["outputs_declared"] == ["sorted.bam"]
    assert jd["view"] == {"sorted.bam": "out/sorted.bam"}
    assert jd["fixed_output"] is None
    assert jd["retain"] == "Default"


@requires_blake3
def test_retain_json_variants(graph):
    j1 = ppg3.CommandJob(view={"a": "a.txt"}, argv=["true"], retain=ppg3.Retain.Evict)
    j2 = ppg3.CommandJob(
        view={"b": "b.txt"}, argv=["true"], retain=ppg3.Retain.Pin("keep-me")
    )
    assert j1.job_def(graph)["retain"] == "Evict"
    assert j2.job_def(graph)["retain"] == {"Pin": "keep-me"}


@requires_blake3
def test_fetchjob_frozen_mode_requires_blake3(tmp_path):
    ppg3.new(
        default_python=PyEnv.current(),
        project_dir=str(tmp_path / ".ppg3"),
        frozen=True,
    )
    with pytest.raises(DefinitionError):
        ppg3.FetchJob(view="inputs/genome.fa.gz", url="https://example.invalid/genome.fa.gz")


@requires_blake3
def test_fetchjob_non_frozen_allows_none_hash(tmp_path):
    ppg3.new(
        default_python=PyEnv.current(),
        project_dir=str(tmp_path / ".ppg3"),
        frozen=False,
    )
    job = ppg3.FetchJob(view="inputs/genome.fa.gz", url="https://example.invalid/genome.fa.gz")
    assert job.job_def()["fixed_output"] is None


@requires_blake3
def test_fetchjob_with_hash_ok_even_frozen(tmp_path):
    ppg3.new(
        default_python=PyEnv.current(),
        project_dir=str(tmp_path / ".ppg3"),
        frozen=True,
    )
    job = ppg3.FetchJob(
        view="inputs/genome.fa.gz",
        url="https://example.invalid/genome.fa.gz",
        blake3="a" * 64,
    )
    jd = job.job_def()
    assert jd["fixed_output"] == "a" * 64
    assert jd["exec_template"]["Argv"]["allow_network"] is True


@requires_blake3
def test_unsandboxedjob_warns(graph):
    with pytest.warns(UserWarning, match="unsandboxed"):
        job = ppg3.UnsandboxedJob(run=dummy_run, name="loader-thing")
    assert job.job_def()["exec_template"] == "InProcess"


@requires_blake3
def test_datajob_output_name_fixed(graph):
    def train(io):
        return 42

    job = ppg3.DataJob(view="models/foo", run=train)
    jd = job.job_def(graph)
    assert jd["outputs_declared"] == ["data.pickle"]
    assert jd["view"] == {"data.pickle": "models/foo"}
    assert job.id in graph.data_job_ids


@requires_blake3
def test_graphjob_job_def_shape(graph):
    def expand():
        pass

    job = ppg3.GraphJob(expand)
    jd = job.job_def()
    assert jd["exec_template"] == "InProcess"
    assert jd["id"] == expand.__qualname__


@requires_blake3
def test_input_lowering_file_and_params(graph, tmp_path):
    data_file = tmp_path / "raw.txt"
    data_file.write_text("hello")
    job = ppg3.FileJob(
        view={"out": "out.txt"},
        run=dummy_run,
        inputs={
            "raw": ppg3.File(str(data_file)),
            "cfg": ppg3.Params({"n": 1}),
        },
    )
    jd = job.job_def(graph)
    assert set(jd["inputs"].keys()) == {"raw", "cfg"}
    assert "Leaf" in jd["inputs"]["raw"]
    assert "Leaf" in jd["inputs"]["cfg"]
    assert len(jd["inputs"]["raw"]["Leaf"]["hash"]) == 64


@requires_blake3
def test_input_lowering_job_subset(graph):
    upstream = ppg3.FileJob(view={"fasta": "ref/fasta.fa", "log": "ref/log"}, run=dummy_run)
    downstream = ppg3.FileJob(
        view={"out": "out.txt"}, run=dummy_run, inputs={"ref": upstream["fasta"]}
    )
    jd = downstream.job_def(graph)
    assert jd["inputs"]["ref"] == {"JobSubset": {"id": upstream.id, "names": ["fasta"]}}


@requires_blake3
def test_input_lowering_whole_job(graph):
    upstream = ppg3.FileJob(view={"fasta": "ref/fasta.fa"}, run=dummy_run)
    downstream = ppg3.FileJob(
        view={"out": "out.txt"}, run=dummy_run, inputs={"reads": upstream}
    )
    jd = downstream.job_def(graph)
    assert jd["inputs"]["reads"] == {"Job": {"id": upstream.id}}


def test_no_active_graph_raises():
    import ppg3.jobs as jobs_mod

    jobs_mod._current_graph = None
    with pytest.raises(DefinitionError):
        ppg3.CommandJob(view={"a": "a.txt"}, argv=["true"])
