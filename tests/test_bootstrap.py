import pypipegraph2 as ppg
from .shared import counter, read, write
from pathlib import Path
import pytest


@pytest.mark.usefixtures("ppg2_per_test", "job_trace_log")
class TestBootstrap:
    """Step by step, establish a working executing workflow for the ppg2"""

    def test_smallest(self):
        assert not Path("A").exists()
        ppg.FileGeneratingJob(
            "A", lambda of: of.write_text("Done"), depend_on_function=False
        )
        ppg.run()
        assert Path("A").read_text() == "Done"

    def test_smallest_graph(self):
        assert not Path("A").exists()

        def func(of):
            counter("a")
            of.write_text("A")

        ppg.FileGeneratingJob("A", func, depend_on_function=False)
        ppg.run()
        assert Path("A").read_text() == "A"
        assert read("a") == "1"
        ppg.run()
        assert read("a") == "1"
        Path("A").unlink()
        ppg.run()
        assert read("a") == "2"
        assert Path("A").read_text() == "A"

    def test_smallest_with_invariant(self):
        def func(of):
            counter("a")
            of.write_text("A")

        job = ppg.FileGeneratingJob("A", func, depend_on_function=False)
        job.depends_on_params(1)
        ppg.run()
        assert read("a") == "1"
        assert Path("A").read_text() == "A"
        ppg.run()
        assert read("a") == "1"
        assert Path("A").read_text() == "A"
        ppg.new()
        job = ppg.FileGeneratingJob("A", func, depend_on_function=False)
        job.depends_on_params(2)
        ppg.run()
        assert read("a") == "2"
        assert Path("A").read_text() == "A"

    def test_chain(self):
        jobA = ppg.FileGeneratingJob(
            "a", lambda of: counter("A") and write("a", "a"), depend_on_function=False
        )
        jobB = ppg.FileGeneratingJob(
            "b",
            lambda of: counter("B") and write("b", read("a") + "b"),
            depend_on_function=False,
        )
        jobC = ppg.FileGeneratingJob(
            "c",
            lambda of: counter("C") and write("c", read("b") + "c"),
            depend_on_function=False,
        )
        jobC.depends_on(jobB)
        jobB.depends_on(jobA)
        ppg.run()
        assert read("A") == "1"
        assert read("B") == "1"
        assert read("C") == "1"
        assert read("a") == "a"
        assert read("b") == "ab"
        assert read("c") == "abc"
        ppg.run()
        assert read("A") == "1"
        assert read("B") == "1"
        assert read("C") == "1"
        assert read("a") == "a"
        assert read("b") == "ab"
        assert read("c") == "abc"

        Path("a").unlink()
        ppg.run()
        assert read("a") == "a"
        assert read("b") == "ab"
        assert read("c") == "abc"
        assert read("A") == "2"
        assert read("B") == "1"
        assert read("C") == "1"

        Path("b").unlink()
        ppg.run()
        assert read("a") == "a"
        assert read("b") == "ab"
        assert read("c") == "abc"
        assert read("A") == "2"
        assert read("B") == "2"
        assert read("C") == "1"

        Path("c").unlink()
        ppg.run()
        assert read("a") == "a"
        assert read("b") == "ab"
        assert read("c") == "abc"
        assert read("A") == "2"
        assert read("B") == "2"
        assert read("C") == "2"

        Path("a").unlink()
        Path("b").unlink()
        ppg.run()
        assert read("a") == "a"
        assert read("b") == "ab"
        assert read("c") == "abc"
        assert read("A") == "3"
        assert read("B") == "3"
        assert read("C") == "2"

    def test_two_inputs(self):
        jobA = ppg.FileGeneratingJob(
            "a", lambda of: counter("A") and write(of, "a"), depend_on_function=False
        )
        jobB = ppg.FileGeneratingJob(
            "b", lambda of: counter("B") and write(of, "b"), depend_on_function=False
        )
        jobC = ppg.FileGeneratingJob(
            "c",
            lambda of: counter("C") and write(of, read("a") + read("b") + "c"),
            depend_on_function=False,
        )
        jobC.depends_on(jobA, jobB)
        ppg.run()
        assert read("A") == "1"
        assert read("B") == "1"
        assert read("C") == "1"
        assert read("c") == "abc"
        ppg.run()
        assert read("A") == "1"
        assert read("B") == "1"
        assert read("C") == "1"
        assert read("c") == "abc"
        Path("c").unlink()
        ppg.run()
        assert read("A") == "1"
        assert read("B") == "1"
        assert read("C") == "2"
        Path("a").unlink()
        ppg.run()
        assert read("A") == "2"  # rebuild
        assert read("B") == "1"
        assert read("C") == "2"  # not run, since the upstream did not invalidate.

    def test_data_loading_without_func_invariant(self):
        out = []
        a = ppg.DataLoadingJob(
            "a", lambda: counter("A") and out.append("a") or ppg.UseInputHashesForOutput(), depend_on_function=False
        )
        b = ppg.FileGeneratingJob(
            "b",
            lambda of, out=out: counter("B") and of.write_text(out[0] + "b"),
            depend_on_function=False,
        )
        b.depends_on(a)
        ppg.run()
        assert read("A") == "1"
        assert read("B") == "1"
        assert read("b") == "ab"
        ppg.run()
        assert read("A") == "1"
        assert read("B") == "1"
        assert read("b") == "ab"
        ppg.run()
        Path("b").unlink()
        ppg.run()
        assert read("A") == "2"
        assert read("B") == "2"
        assert read("b") == "ab"

    def test_data_loading(self):
        out = []
        a = ppg.DataLoadingJob(
            "a", lambda: counter("A") and out.append("a") or ppg.UseInputHashesForOutput(), depend_on_function=False
        )
        b = ppg.FileGeneratingJob(
            "b",
            lambda of, out=out: counter("B") and of.write_text(out[0] + "b"),
            depend_on_function=True,
        )
        b.depends_on(a)
        ppg.run()
        assert read("A") == "1"
        assert read("B") == "1"
        assert read("b") == "ab"
        ppg.run()
        assert read("A") == "1"
        assert read("B") == "1"
        assert read("b") == "ab"
        ppg.run()
        Path("b").unlink()
        ppg.run()
        assert read("A") == "2"
        assert read("B") == "2"
        assert read("b") == "ab"

    def test_two_data_loading_chain(self):
        out = []
        a = ppg.DataLoadingJob(
            "a", lambda: counter("A") and out.append("a") or ppg.UseInputHashesForOutput(), depend_on_function=False
        )
        b = ppg.DataLoadingJob(
            "b",
            lambda: counter("B") and out.append(out[0] + "b") or ppg.UseInputHashesForOutput(),
            depend_on_function=False,
        )
        c = ppg.FileGeneratingJob(
            "c",
            lambda of: counter("C") and of.write_text(out[1] + "c"),
            depend_on_function=False,
        )
        c.depends_on(b)
        b.depends_on(a)
        ppg.run()
        assert read("c") == "abc"
        assert read("A") == "1"
        assert read("B") == "1"
        assert read("C") == "1"
        ppg.run()
        assert read("c") == "abc"
        assert read("A") == "1"
        assert read("B") == "1"
        assert read("C") == "1"
        Path("c").unlink()
        ppg.run()
        assert read("c") == "abc"
        assert read("A") == "2"
        assert read("B") == "2"
        assert read("C") == "2"
