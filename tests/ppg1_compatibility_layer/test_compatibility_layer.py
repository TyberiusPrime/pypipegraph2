import pytest
from pathlib import Path
from .shared import write, force_load
import pypipegraph2 as ppg2
import pypipegraph as ppg1

# to push test coverage...


class TestcompatibilityLayer:
    def test_repeated_apply_unapply(self):
        fg1 = ppg1.FileGeneratingJob
        fg2 = ppg2.FileGeneratingJob
        ppg2.replace_ppg1()
        assert ppg1.FileGeneratingJob is not fg1
        assert ppg1.FileGeneratingJob is not fg2  # actually it is wrapped...
        fg1_to_two = ppg1.FileGeneratingJob
        ppg2.unreplace_ppg1()
        assert ppg1.FileGeneratingJob is fg1
        ppg2.replace_ppg1()
        assert ppg1.FileGeneratingJob is fg1_to_two
        ppg2.replace_ppg1()
        assert ppg1.FileGeneratingJob is fg1_to_two
        ppg2.replace_ppg1()
        assert ppg1.FileGeneratingJob is fg1_to_two
        ppg2.unreplace_ppg1()
        assert ppg1.FileGeneratingJob is fg1
        ppg2.unreplace_ppg1()
        assert ppg1.FileGeneratingJob is fg1


@pytest.mark.usefixtures("ppg1_compatibility_test")
class TestcompatibilityLayerMore:
    def test_use_cores_exclusive(self):
        j = ppg1.FileGeneratingJob("shu", lambda of: of.write_text("j"))
        j.cores_needed = -2
        assert j.resources == ppg2.Resources.Exclusive
        assert j.cores_needed == -2

    def test_fg_did_not_create_its_file(self):
        j = ppg1.FileGeneratingJob("test_file_gen", lambda: 55)  # old school callback
        ppg1.FileGeneratingJob(
            "test_file_gen_does", lambda: Path("test_file_gen_does").write_text("A")
        )  # old school callback
        with pytest.raises(ppg2.JobsFailed):
            ppg2.run()
        assert "did not create" in str(j.exception)

    def test_multifilegenerating_without_arguments(self):
        ppg1.MultiFileGeneratingJob(
            ["out/A", "out/B"], lambda: write("out/A", "A") or write("out/B", "B")
        )
        j2 = ppg1.MultiFileGeneratingJob(["out/C", "out/D"], lambda: 55)
        j3 = ppg1.MultiFileGeneratingJob(["out/G", "out/F"], lambda of: 55)
        with pytest.raises(ppg2.JobsFailed):
            ppg2.run()
        assert "did not create" in str(j2.exception)
        assert "did not create" in str(j3.exception)
        assert Path("out/A").read_text() == "A"
        assert Path("out/B").read_text() == "B"

    def test_temp_file_with_and_without(self):
        a = ppg1.TempFileGeneratingJob("A", lambda: 55)
        b = ppg1.TempFileGeneratingJob("b", lambda of: 55)
        c = ppg1.MultiTempFileGeneratingJob(["C"], lambda: 55)
        d = ppg1.MultiTempFileGeneratingJob(["D"], lambda of: 55)
        force_load(a)
        force_load(b)
        force_load(c)
        force_load(d)
        with pytest.raises(ppg2.JobsFailed):
            ppg2.run()
        assert "did not create" in str(a.exception)
        assert "did not create" in str(b.exception)
        assert "did not create" in str(c.exception)
        assert "did not create" in str(d.exception)

    def test_unsupported(self):
        with pytest.raises(NotImplementedError):
            ppg1.MemMappedDataLoadingJob()

    def test_predecessors(self):
        a = ppg1.TempFileGeneratingJob("A", lambda: 55)
        b = ppg1.TempFileGeneratingJob("b", lambda of: 55)
        b.ignore_code_changes()
        b.depends_on(a)
        assert list(b.prerequisites) == [a]

    def test_depends_on_file_param_returns_wrapped(self):
        a = ppg1.FileGeneratingJob("a", lambda of: write(of))
        Path("input").write_text("hello")
        b = a.depends_on_file("input").invariant
        assert isinstance(b, ppg2.ppg1_compatibility.FileInvariant)
        c = a.depends_on_params("shu").invariant
        assert isinstance(c, ppg2.ppg1_compatibility.ParameterInvariant)

    def test_callback_adaption_with_default_parameters(self):
        def no_args():
            pass

        def all_default_args(a=123, b=234):
            pass

        def new_style(of, a=123, b=234):
            pass

        def new_style2(of):
            pass

        a = ppg1.FileGeneratingJob("a", no_args)
        assert hasattr(
            a.generating_function, "wrapped_function"
        )  # is a wrapped function
        b = ppg1.FileGeneratingJob("b", new_style)
        assert not hasattr(
            b.generating_function, "wrapped_function"
        )  # is a wrapped function
        b1 = ppg1.FileGeneratingJob("b1", new_style)
        assert not hasattr(
            b1.generating_function, "wrapped_function"
        )  # is a wrapped function

        with pytest.raises(TypeError):
            ppg1.FileGeneratingJob("c", all_default_args)
        ppg1.new_pipegraph()
        a = ppg1.MultiFileGeneratingJob(["a"], no_args)
        assert hasattr(
            a.generating_function, "wrapped_function"
        )  # is a wrapped function
        b = ppg1.MultiFileGeneratingJob(["b"], new_style)
        assert not hasattr(
            b.generating_function, "wrapped_function"
        )  # is a wrapped function
        ppg1.MultiFileGeneratingJob(
            ["c"], all_default_args
        )  # mfg never passed [output_files]
        assert hasattr(
            a.generating_function, "wrapped_function"
        )  # is a wrapped function

    def test_rc_cores_available(self):
        assert (
            ppg1.util.global_pipegraph.rc.cores_available == ppg2.global_pipegraph.cores
        )

    def test_ignore_code_changes_changes_both_dag_and_jobs(self):
        a = ppg1.FileGeneratingJob("a", lambda of: of.write_text("a"))
        assert len(ppg2.global_pipegraph.jobs) == 2  # the fg, and teh FI
        assert len(ppg2.global_pipegraph.job_dag) == 2  # the fg, and teh FI
        a.ignore_code_changes()
        assert len(ppg2.global_pipegraph.jobs) == 1  # the fg, and teh FI
        assert len(ppg2.global_pipegraph.job_dag) == 1  # the fg, and teh FI
