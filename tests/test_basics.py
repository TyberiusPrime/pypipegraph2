from pathlib import Path
from loguru import logger
import pytest
import pypipegraph2 as ppg
from pypipegraph2.runner import JobState


def counter(filename):
    """Helper for counting invocations in a sideeffect file"""
    try:
        res = int(Path(filename).read_text())
    except:
        res = 0
    Path(filename).write_text(str(res + 1))
    return str(res)


class TestPypipegraph2:
    def test_very_simple(self, ppg_per_test):
        assert not Path("A").exists()
        job = ppg.FileGeneratingJob("A", lambda of: of.write_text("Done"))
        ppg.run()
        assert Path("A").read_text() == "Done"

    def test_very_simple_chain(self, ppg_per_test):
        assert not Path("A").exists()
        assert not Path("B").exists()
        jobA = ppg.FileGeneratingJob("A", lambda of: of.write_text("AAA"))
        jobB = ppg.FileGeneratingJob(
            "B", lambda of: of.write_text("BBB" + Path("A").read_text())
        )
        jobB.depends_on(jobA)
        ppg.run()
        assert Path("A").read_text() == "AAA"
        assert Path("B").read_text() == "BBBAAA"

    def test_very_simple_chain_reverse(self, ppg_per_test):
        assert not Path("A").exists()
        assert not Path("B").exists()
        jobB = ppg.FileGeneratingJob(
            "B", lambda of: of.write_text("BBB" + Path("A").read_text())
        )
        jobA = ppg.FileGeneratingJob("A", lambda of: of.write_text("AAA"))
        jobB.depends_on(jobA)
        ppg.run()
        assert Path("A").read_text() == "AAA"
        assert Path("B").read_text() == "BBBAAA"

    def test_very_simple_chain_rerun(self, ppg_per_test, job_trace_log):
        assert not Path("A").exists()
        assert not Path("B").exists()
        counter = 0
        jobA = ppg.FileGeneratingJob("A", lambda of: of.write_text(f"{counter}"))
        jobB = ppg.FileGeneratingJob(
            "B", lambda of: of.write_text("BBB" + Path("A").read_text())
        )
        jobB.depends_on(jobA)
        ppg.run()
        assert Path("B").read_text() == "BBB0"
        ppg.run()
        assert Path("B").read_text() == "BBB0"
        Path("A").unlink()
        counter = 1
        ppg.run()
        assert Path("B").read_text() == "BBB1"

    def test_isolation(self, ppg_per_test):
        assert not Path("B").exists()
        assert not Path("C").exists()

        def b(of):
            of.write_text("BBB")
            count = Path("counter").read_text()
            Path("outcount").write_text(count)

        jobB = ppg.FileGeneratingJob("B", b)
        jobC = ppg.FileGeneratingJob(
            "C", lambda of: of.write_text("CCC" + Path("outcount").read_text())
        )
        jobC.depends_on(jobB)
        Path("counter").write_text("0")
        ppg.run()
        assert Path("B").read_text() == "BBB"
        assert Path("C").read_text() == "CCC0"
        assert Path("outcount").read_text() == "0"
        ppg.run()
        assert Path("B").read_text() == "BBB"
        assert Path("C").read_text() == "CCC0"
        assert Path("outcount").read_text() == "0"

        Path("counter").write_text("1")
        ppg.run()  # since the counter is *not* a dependency...
        assert Path("B").read_text() == "BBB"
        assert Path("C").read_text() == "CCC0"
        assert Path("outcount").read_text() == "0"

        Path("B").unlink()  # will make it rerun.
        ppg.run()
        assert Path("outcount").read_text() == "1"
        # but C was not rerun, since the B output did not change.
        assert Path("C").read_text() == "CCC0"

    def test_changing_inputs(self, ppg_per_test, job_trace_log):
        jobA = ppg.FileGeneratingJob("A", lambda of: of.write_text("A"))
        jobB = ppg.FileGeneratingJob(
            "B", lambda of: of.write_text("B" + Path("A").read_text())
        )
        assert not Path("A").exists()
        assert not Path("B").exists()
        jobB.depends_on(jobA)
        logger.warning("first  run")
        ppg.run()
        assert Path("A").read_text() == "A"
        assert Path("B").read_text() == "BA"
        jobA = ppg.FileGeneratingJob("A", lambda of: of.write_text("c"))
        logger.warning("Change run")
        ppg.run()
        assert Path("A").read_text() == "c"
        assert Path("B").read_text() == "Bc"

    def test_changing_inputs_when_job_was_temporarily_missing(self, ppg_per_test, job_trace_log):
        jobA = ppg.FileGeneratingJob("A", lambda of: counter('a') and of.write_text("AAA"))
        jobB = ppg.FileGeneratingJob( "B", lambda of: of.write_text("BBB" + Path("A").read_text()))
        jobB.depends_on(jobA)
        ppg.run()
        assert Path("A").read_text() == "AAA"
        assert Path("B").read_text() == "BBBAAA"
        assert Path('a').read_text() == '1'
        ppg.new()
        jobA = ppg.FileGeneratingJob('A', lambda of: counter('a') and of.write_text("AAAA"))
        ppg.run()
        assert Path("A").read_text() == "AAAA"
        assert Path("B").read_text() == "BBBAAA" # not rerun
        assert Path('a').read_text() == '2'
        ppg.new()
        jobA = ppg.FileGeneratingJob('A', lambda of: counter('a') and of.write_text("AAAA"))
        jobB = ppg.FileGeneratingJob( "B", lambda of: of.write_text("BBB" + Path("A").read_text()))
        ppg.run()
        assert Path('a').read_text() == '2'
        assert Path("B").read_text() == "BBBAAAA" # correctly rerun





    def test_changing_bound_variables(self, ppg_per_test):
        varA = "hello"
        jobA = ppg.FileGeneratingJob("A", lambda of, varA=varA: of.write_text(varA))
        ppg.run()
        assert Path("A").read_text() == "hello"

        varA = "world"
        jobA = ppg.FileGeneratingJob("A", lambda of, varA=varA: of.write_text(varA))
        ppg.run()
        assert Path("A").read_text() == "hello"

    def test_capturing_closures(self, ppg_per_test):
        varA = ["hello"]
        jobA = ppg.FileGeneratingJob("A", lambda of: of.write_text(str(varA)))
        ppg.run()
        assert Path("A").read_text() == str(["hello"])

        varA.append("world")
        jobA = ppg.FileGeneratingJob("A", lambda of: of.write_text(str(varA)))
        ppg.run()
        assert Path("A").read_text() == str(["hello", "world"])

    def test_failed_pruning(self, ppg_per_test, job_trace_log):
        def a(of):
            raise ValueError()

        jobA = ppg.FileGeneratingJob("A", a)
        jobB = ppg.FileGeneratingJob("B", lambda of: of.write_text("B"))
        jobC = ppg.FileGeneratingJob("C", lambda of: of.write_text("C"))
        jobB.depends_on(jobA)
        with pytest.raises(ppg.RunFailed):
            ppg.run()
        assert Path("C").read_text() == "C"
        last = ppg.global_pipegraph.last_run_result
        assert last["A"].state == JobState.Failed
        assert last["B"].state == JobState.UpstreamFailed
        assert last["C"].state == JobState.Executed
        assert "ValueError" in str(last["A"].error)

    def test_multi_file_generating_job(self, ppg_per_test, job_trace_log):
        import collections

        assert counter("X") == "0"
        # make sure the counter function does what it's supposed to
        assert counter("X") == "1"

        def a(files):
            files[0].write_text("A1")
            files[1].write_text("A2")

        jobA = ppg.MultiFileGeneratingJob(["A1", "A2"], a)
        jobB = ppg.FileGeneratingJob(
            "B", lambda of: of.write_text(f"B{counter('cB')}"), depend_on_function=False
        )
        jobC = ppg.FileGeneratingJob(
            "C", lambda of: of.write_text(f"C{counter('cC')}"), depend_on_function=False
        )
        jobD = ppg.FileGeneratingJob(
            "D", lambda of: of.write_text(f"D{counter('cD')}"), depend_on_function=False
        )
        jobB.depends_on("A2")  # todo output must exist!
        jobC.depends_on(jobA)
        ppg.run()
        assert Path("A1").read_text() == "A1"
        assert Path("A2").read_text() == "A2"
        assert Path("B").read_text() == "B0"
        assert Path("C").read_text() == "C0"
        assert Path("D").read_text() == "D0"

        def a(files):
            files[0].write_text("A1a")
            files[1].write_text("A2")

        jobA = ppg.MultiFileGeneratingJob(["A1", "A2"], a)
        ppg.run()
        assert Path("A1").read_text() == "A1a"
        assert Path("A2").read_text() == "A2"
        assert Path("B").read_text() == "B0"  # does not get rewritten. It depends on A
        assert Path("C").read_text() == "C1"  # c get's rewritte
        assert Path("D").read_text() == "D0"

    def test_tempfile(self, ppg_per_test, trace_log):
        jobA = ppg.TempFileGeneratingJob(
            "A",
            lambda of: of.write_text("A" + counter("a")),
            depend_on_function=False,
        )
        jobB = ppg.FileGeneratingJob(
            "B",
            lambda of: of.write_text("B" + counter("c") + Path("A").read_text()),
            depend_on_function=False,
        )
        jobB.depends_on(jobA)
        ppg.run()
        assert not Path("A").exists()
        assert Path("B").exists()
        assert Path("B").read_text() == "B0A0"
        logger.error("Second run")
        ppg.run()
        assert not Path("A").exists()
        assert Path("B").exists()
        assert Path("B").read_text() == "B0A0"

    def test_tempfile_chained(self, ppg_per_test, trace_log):
        jobA = ppg.TempFileGeneratingJob(
            "A", lambda of: of.write_text("A" + counter("a"))
        )
        jobB = ppg.TempFileGeneratingJob(
            "B", lambda of: of.write_text("B" + counter("b") + Path("A").read_text())
        )
        jobC = ppg.FileGeneratingJob(
            "C", lambda of: of.write_text("C" + counter("c") + Path("B").read_text())
        )
        jobC.depends_on(jobB)
        jobB.depends_on(jobA)
        logger.error("First run")
        ppg.run()
        assert not Path("A").exists()
        assert not Path("B").exists()
        assert Path("C").read_text() == "C0B0A0"
        logger.error("Second No op run.")
        ppg.run()
        assert Path("C").read_text() == "C0B0A0"
        assert not Path("A").exists()
        assert not Path("B").exists()

        jobB.depends_on(ppg.FunctionInvariant(lambda: 53, "lambda_52"))
        logger.error("Third run")
        ppg.run()
        assert Path("C").read_text() == "C1B1A1"
        assert not Path("A").exists()
        assert not Path("B").exists()

    def test_just_tempfiles(self, ppg_per_test ,trace_log):
        jobA = ppg.TempFileGeneratingJob(
            "A", lambda of: of.write_text("A" + counter("a"))
        )
        ppg.run()
        assert not Path('A').exists()
        assert not Path('a').exists()

    def test_just_chained_tempfile(self, ppg_per_test, trace_log):
        jobA = ppg.TempFileGeneratingJob(
            "A", lambda of: of.write_text("A" + counter("a"))
        )
        jobB = ppg.TempFileGeneratingJob(
            "B", lambda of: of.write_text("B" + counter("b") + Path("A").read_text())
        )
        ppg.run()
        assert not Path('A').exists()
        assert not Path('a').exists()
        assert not Path('B').exists()
        assert not Path('b').exists()

    def test_just_chained_tempfile3(self, ppg_per_test, trace_log):
        jobA = ppg.TempFileGeneratingJob(
            "A", lambda of: of.write_text("A" + counter("a"))
        )
        jobB = ppg.TempFileGeneratingJob(
            "B", lambda of: of.write_text("B" + counter("b") + Path("A").read_text())
        )
        jobC = ppg.TempFileGeneratingJob(
            "C", lambda of: of.write_text("C" + counter("c") + Path("B").read_text())
        )

        ppg.run()
        assert not Path('A').exists()
        assert not Path('a').exists()
        assert not Path('B').exists()
        assert not Path('b').exists()
        assert not Path('C').exists()
        assert not Path('c').exists()



    def test_last_invalidated_tempfile(self, ppg_per_test ,trace_log):
        jobA = ppg.TempFileGeneratingJob(
            "A", lambda of: of.write_text("A" + counter("a"))
        )
        jobB = ppg.TempFileGeneratingJob(
            "B", lambda of: of.write_text("B" + counter("b") + Path("A").read_text())
        )
        jobC = ppg.FileGeneratingJob(
            "C", lambda of: of.write_text("C" + counter("c") + Path("B").read_text())
        )
        jobC.depends_on(jobB)
        jobB.depends_on(jobA)
        ppg.run()
        assert Path("C").read_text() == "C0B0A0"
        jobC.depends_on(ppg.FunctionInvariant(lambda: 52, "lambda_52"))
        ppg.run()
        assert Path("C").read_text() == "C1B1A1"


    def test_cycles(self):
        jobA = ppg.FileGeneratingJob("A", lambda of: of.write_text("Done"))
        jobB = ppg.FileGeneratingJob("B", lambda of: of.write_text("Done"))
        jobA.depends_on(jobB)
        jobB.depends_on(jobA)
        with pytest.raises(ppg.exceptions.NotADag):
            ppg.run()

    def test_jobs_run_in_different_pids(self):
        raise NotImplementedError()

    def test_job_redefinition(self):
        raise NotImplementedError()

    def test_changing_mtime_triggers_recalc_of_hash(self):
        raise NotImplementedError()

    def test_same_mtime_same_size_leads_to_false_negative(self):
        raise NotImplementedError()
