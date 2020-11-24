# noqa: F841
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
    def test_very_simple(self, ppg_per_test, job_trace_log):
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

    def test_isolation(self, ppg_per_test, trace_log):
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
        logger.error("Run 1")
        ppg.run()
        assert Path("B").read_text() == "BBB"
        assert Path("C").read_text() == "CCC0"
        assert Path("outcount").read_text() == "0"
        logger.error("Run 2 no rerun")
        ppg.run()
        assert Path("B").read_text() == "BBB"
        assert Path("C").read_text() == "CCC0"
        assert Path("outcount").read_text() == "0"

        Path("counter").write_text("1")
        logger.error("Run 3 - no rerun")
        ppg.run()  # since the counter is *not* a dependency...
        assert Path("B").read_text() == "BBB"
        assert Path("C").read_text() == "CCC0"
        assert Path("outcount").read_text() == "0"

        Path("B").unlink()  # will make it rerun.
        logger.error("Run 4 - return B but not C")
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

    def test_changing_inputs_when_job_was_temporarily_missing(
        self, ppg_per_test, job_trace_log
    ):
        jobA = ppg.FileGeneratingJob(
            "A", lambda of: counter("a") and of.write_text("AAA")
        )
        jobB = ppg.FileGeneratingJob(
            "B", lambda of: of.write_text("BBB" + Path("A").read_text())
        )
        jobB.depends_on(jobA)
        ppg.run()
        assert Path("A").read_text() == "AAA"
        assert Path("B").read_text() == "BBBAAA"
        assert Path("a").read_text() == "1"
        ppg.new()
        jobA = ppg.FileGeneratingJob(
            "A", lambda of: counter("a") and of.write_text("AAAA")
        )
        ppg.run()
        assert Path("A").read_text() == "AAAA"
        assert Path("B").read_text() == "BBBAAA"  # not rerun
        assert Path("a").read_text() == "2"
        ppg.new()
        jobA = ppg.FileGeneratingJob(
            "A", lambda of: counter("a") and of.write_text("AAAA")
        )
        jobB = ppg.FileGeneratingJob(
            "B", lambda of: of.write_text("BBB" + Path("A").read_text())
        )
        ppg.run()
        assert Path("a").read_text() == "2"
        assert Path("B").read_text() == "BBBAAAA"  # correctly rerun

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

        logger.error("2nd no op run")
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
        logger.error("3rd run - run a, run c")
        ppg.run()
        assert Path("A1").read_text() == "A1a"
        assert Path("A2").read_text() == "A2"
        assert Path("B").read_text() == "B0"  # does not get rewritten. It depends on A2
        assert (
            Path("C").read_text() == "C1"
        )  # c get's rewritten, it depended on all of A
        assert Path("D").read_text() == "D0"

    def test_tempfile(self, ppg_per_test, trace_log):
        jobA = ppg.TempFileGeneratingJob(
            "TA",
            lambda of: of.write_text("A" + counter("a")),
            depend_on_function=False,
        )
        jobB = ppg.FileGeneratingJob(
            "B",
            lambda of: of.write_text("B" + counter("c") + Path("TA").read_text()),
            depend_on_function=False,
        )
        jobB.depends_on(jobA)
        ppg.run()
        assert not Path("TA").exists()
        assert Path("B").exists()
        assert Path("B").read_text() == "B0A0"
        logger.error("Second run - no rerun")
        ppg.run()
        assert not Path("TA").exists()
        assert Path("B").exists()
        assert Path("B").read_text() == "B0A0"

        Path("B").unlink()
        logger.error("Third run - B output missing")
        ppg.run()
        assert not Path("TA").exists()
        assert Path("B").exists()
        assert Path("B").read_text() == "B1A1"

    def test_tempfile_chained_invalidate_leaf(self, ppg_per_test, trace_log):
        jobA = ppg.TempFileGeneratingJob(
            "TA", lambda of: of.write_text("A" + counter("a")), depend_on_function=False
        )
        jobB = ppg.TempFileGeneratingJob(
            "TB",
            lambda of: of.write_text("B" + counter("b") + Path("TA").read_text()),
            depend_on_function=False,
        )
        jobC = ppg.FileGeneratingJob(
            "C",
            lambda of: of.write_text("C" + counter("c") + Path("TB").read_text()),
            depend_on_function=False,
        )
        jobC.depends_on(jobB)
        jobB.depends_on(jobA)
        logger.error("First run")
        ppg.run()
        assert not Path("TA").exists()
        assert not Path("TB").exists()
        assert Path("C").read_text() == "C0B0A0"
        logger.error("Second No op run.")
        ppg.run()
        assert Path("C").read_text() == "C0B0A0"
        assert not Path("TA").exists()
        assert not Path("TB").exists()

        jobC.depends_on(ppg.FunctionInvariant(lambda: 53, "lambda_52"))
        logger.error("Third run - rerun because of FI")
        ppg.run()
        assert Path("C").read_text() == "C1B1A1"
        assert not Path("TA").exists()
        assert not Path("TB").exists()

    def test_tempfile_chained_invalidate_intermediate(self, ppg_per_test, trace_log):
        jobA = ppg.TempFileGeneratingJob(
            "TA", lambda of: of.write_text("A" + counter("a")), depend_on_function=False
        )
        jobB = ppg.TempFileGeneratingJob(
            "TB",
            lambda of: of.write_text("B" + counter("b") + Path("TA").read_text()),
            depend_on_function=False,
        )
        jobC = ppg.FileGeneratingJob(
            "C",
            lambda of: of.write_text("C" + counter("c") + Path("TB").read_text()),
            depend_on_function=False,
        )
        jobC.depends_on(jobB)
        jobB.depends_on(jobA)
        logger.error("First run")
        ppg.run()
        assert not Path("TA").exists()
        assert not Path("TB").exists()
        assert Path("C").read_text() == "C0B0A0"
        logger.error("Second No op run.")
        ppg.run()
        assert Path("C").read_text() == "C0B0A0"
        assert not Path("TA").exists()
        assert not Path("TB").exists()

        jobB.depends_on(ppg.FunctionInvariant(lambda: 53, "lambda_52"))
        logger.error("Third run - rerun because of FI")
        ppg.run()
        assert Path("C").read_text() == "C1B1A1"
        assert not Path("TA").exists()
        assert not Path("TB").exists()

    def test_just_a_tempfile(self, ppg_per_test, trace_log):
        jobA = ppg.TempFileGeneratingJob(
            "TA", lambda of: of.write_text("A" + counter("a"))
        )
        ppg.run()
        assert not Path("TA").exists()
        assert not Path("a").exists()

    def test_just_chained_tempfile(self, ppg_per_test, trace_log):
        jobA = ppg.TempFileGeneratingJob(
            "TA", lambda of: of.write_text("A" + counter("a"))
        )
        jobB = ppg.TempFileGeneratingJob(
            "B", lambda of: of.write_text("B" + counter("b") + Path("A").read_text())
        )
        ppg.run()
        assert not Path("TA").exists()
        assert not Path("a").exists()
        assert not Path("B").exists()
        assert not Path("b").exists()

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
        assert not Path("A").exists()
        assert not Path("a").exists()
        assert not Path("B").exists()
        assert not Path("b").exists()
        assert not Path("C").exists()
        assert not Path("c").exists()

    def test_tempfile_triggered_by_invalidating_final_job(
        self, ppg_per_test, trace_log
    ):
        jobA = ppg.TempFileGeneratingJob(
            "TA",
            lambda of: of.write_text("A" + counter("a")),
            depend_on_function=False,
        )
        jobB = ppg.TempFileGeneratingJob(
            "TB",
            lambda of: of.write_text("B" + counter("b") + Path("TA").read_text()),
            depend_on_function=False,
        )
        jobC = ppg.FileGeneratingJob(
            "C",
            lambda of: of.write_text("C" + counter("c") + Path("TB").read_text()),
            depend_on_function=False,
        )
        jobC.depends_on(jobB)
        jobB.depends_on(jobA)
        ppg.run()
        assert Path("C").read_text() == "C0B0A0"
        jobC.depends_on(ppg.FunctionInvariant(lambda: 52, "lambda_52"))
        ppg.run()
        assert Path("C").read_text() == "C1B1A1"

    def test_tempfile_triggered_by_invalidating_tempfile(self, ppg_per_test, trace_log):
        jobA = ppg.TempFileGeneratingJob(
            "A",
            lambda of: of.write_text("A" + counter("a")),
            depend_on_function=False,
        )
        jobB = ppg.TempFileGeneratingJob(
            "B",
            lambda of: of.write_text("B" + counter("b") + Path("A").read_text()),
            depend_on_function=False,
        )
        jobC = ppg.FileGeneratingJob(
            "C",
            lambda of: of.write_text("C" + counter("c") + Path("B").read_text()),
            depend_on_function=False,
        )
        jobC.depends_on(jobB)
        jobB.depends_on(jobA)
        ppg.run()
        assert Path("C").read_text() == "C0B0A0"
        jobB.depends_on(ppg.FunctionInvariant(lambda: 52, "lambda_52"))
        ppg.run()
        assert Path("C").read_text() == "C1B1A1"

    def test_last_invalidated_tempfile_isolation(self, ppg_per_test, trace_log):
        jobA = ppg.TempFileGeneratingJob(
            "A",
            lambda of: of.write_text("A" + counter("a")),
            depend_on_function=False,
        )
        jobB = ppg.TempFileGeneratingJob(
            "B",
            lambda of: counter("b") and of.write_text("B"),
            depend_on_function=False,
        )
        jobC = ppg.FileGeneratingJob(
            "C",
            lambda of: of.write_text("C" + counter("c") + Path("B").read_text()),
            depend_on_function=False,
        )
        jobC.depends_on(jobB)
        jobB.depends_on(jobA)
        ppg.run()
        assert Path("C").read_text() == "C0B"
        assert Path("a").read_text() == "1"
        jobB.depends_on(ppg.FunctionInvariant(lambda: 52, "lambda_52"))
        ppg.run()
        assert Path("C").read_text() == "C0B"
        assert Path("a").read_text() == "2"

    def test_depending_on_two_temp_jobs_but_only_one_invalidated(self, ppg_per_test):
        jobA = ppg.TempFileGeneratingJob(
            "A",
            lambda of: of.write_text("A" + counter("a")),
            depend_on_function=False,
        )
        jobB = ppg.TempFileGeneratingJob(
            "B",
            lambda of: counter("b") and of.write_text("B"),
            depend_on_function=False,
        )
        jobC = ppg.FileGeneratingJob(
            "C",
            lambda of: of.write_text(
                "C" + counter("c") + Path("B").read_text() + Path("A").read_text()
            ),
            depend_on_function=False,
        )
        jobC.depends_on(jobB)
        jobC.depends_on(jobA)
        ppg.run()
        assert Path("C").read_text() == "C0BA0"
        assert Path("a").read_text() == "1"

        jobB = ppg.TempFileGeneratingJob(
            "B",
            lambda of: counter("b") and of.write_text("BB"),
            depend_on_function=False,
        )  # not changing the function does not trigger a change

        ppg.run()
        assert Path("C").read_text() == "C0BA0"
        assert Path("a").read_text() == "1"

        jobB = ppg.TempFileGeneratingJob(
            "B",
            lambda of: counter("b") and of.write_text("BB"),
            depend_on_function=True,
        )  # but if you have a function invariant!
        ppg.run()

        assert Path("C").read_text() == "C1BBA1"
        assert Path("a").read_text() == "2"

    def test_tempjob_serving_two(self, ppg_per_test):
        jobA = ppg.TempFileGeneratingJob(
            "TA",
            lambda of: of.write_text("TA" + counter("a")),
            depend_on_function=False,
        )
        jobB = ppg.FileGeneratingJob(
            "B",
            lambda of: counter("b") and of.write_text("B" + Path("TA").read_text()),
            depend_on_function=False,
        )
        jobC = ppg.FileGeneratingJob(
            "C",
            lambda of: of.write_text("C" + counter("c") + Path("TA").read_text()),
            depend_on_function=False,
        )
        jobB.depends_on(jobA)
        jobC.depends_on(jobA)
        ppg.run()
        assert Path("B").read_text() == "BTA0"
        assert Path("C").read_text() == "C0TA0"
        assert Path("a").read_text() == "1"
        ppg.run()
        assert Path("B").read_text() == "BTA0"
        assert Path("C").read_text() == "C0TA0"
        assert Path("a").read_text() == "1"
        Path("B").unlink()
        ppg.run()
        assert Path("B").read_text() == "BTA1"
        assert Path("C").read_text() == "C1TA1"
        assert Path("a").read_text() == "2"
        ppg.run()
        assert Path("B").read_text() == "BTA1"
        assert Path("C").read_text() == "C1TA1"
        assert Path("a").read_text() == "2"
        Path("B").unlink()
        Path("C").unlink()
        ppg.run()
        assert Path("B").read_text() == "BTA2"
        assert Path("C").read_text() == "C2TA2"
        assert Path("a").read_text() == "3"

    def test_two_temp_jobs(self, ppg_per_test, trace_log):
        """test_two_temp_jobs
        This tests one of the 'unnecessary' temp job reruns.
        We have these jobs
        Fi:TA -> TA -> C
                       ^
        Fi:TB -> TB    ->  D

        which means, after the graph rewriting,
        TA and TB depend on each other's FunctionInvariants
        (TempJobs steal the invariants from their downstreams,
        so that whenever the downstream is triggered,
        they are as well, and before hand.)

        If now Fi:TB triggers, we must recalculate TB,
        and we also recalculate TA.
        But if TB does not not lead to C and D's invalidation,
        we have recalculated TA unnecessarily.

        But I can't figure out a better way to do it.
        Handling TempJobs by anything other than graph rewriting has
        proven to be an absolute mess of a conditional event loop that
        I'm not capable of cutting through.

        The graph rewriting is elegant and makes the do-the-jobs event loop
        almost trivial. It fails on this particular task though.
        Not that it is given that a back-and-forth graph walking approach
        (ie. when C is triggered, go back and (re)do TA) would be able to
        actually avoid the issue.
        """

        jobA = ppg.TempFileGeneratingJob(
            "TA", lambda of: counter("a") and of.write_text("A")
        )
        jobB = ppg.TempFileGeneratingJob(
            "TB", lambda of: counter("b") and of.write_text("B")
        )
        jobC = ppg.FileGeneratingJob(
            "C",
            lambda of: counter("c")
            and of.write_text("C" + Path("TA").read_text() + Path("TB").read_text()),
        )
        jobD = ppg.FileGeneratingJob(
            "D", lambda of: counter("d") and of.write_text("D" + Path("TB").read_text())
        )
        jobC.depends_on(jobA, jobB)
        jobD.depends_on(jobB)
        ppg.run()
        assert Path("D").read_text() == "DB"
        assert Path("C").read_text() == "CAB"
        assert Path("a").read_text() == "1"
        assert Path("b").read_text() == "1"
        ppg.run()
        assert Path("a").read_text() == "1"
        assert Path("b").read_text() == "1"
        assert Path("c").read_text() == "1"
        assert Path("d").read_text() == "1"

        # now trigger TB invalidation, but not C (or D) invalidation
        logger.info("now change FunctionInvariant:TB")
        jobB = ppg.TempFileGeneratingJob(
            "TB", lambda of: counter("b") and True and of.write_text("B")
        )
        ppg.run()
        assert Path("b").read_text() == "2"  # we trigger that one
        assert (
            Path("a").read_text() == "2"
        )  # the FunctionInvariant:TB was pulled into TA's upstream by the rewrite
        assert Path("c").read_text() == "1"  # but this one was isolated
        assert Path("d").read_text() == "1"  # as was this one was isolated

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

    def test_file_invariant(self, ppg_per_test, job_trace_log):
        Path("A").write_text("A")
        jobA = ppg.FileInvariant("A")
        jobB = ppg.FileGeneratingJob(
            "B", lambda of: counter("b") and of.write_text(Path("A").read_text())
        )
        jobB.depends_on(jobA)
        ppg.run()
        assert Path("B").read_text() == "A"
        assert Path("b").read_text() == "1"
        ppg.run()
        assert Path("b").read_text() == "1"
        Path("A").write_text("AA")
        ppg.run()
        assert Path("b").read_text() == "2"
        assert Path("B").read_text() == "AA"

    def test_adding_and_removing_variants(self, ppg_per_test, job_trace_log):
        Path("A").write_text("A")
        jobA = ppg.FileInvariant("A")
        jobB = ppg.FileGeneratingJob(
            "B", lambda of: counter("b") and of.write_text(Path("A").read_text())
        )
        jobB.depends_on(jobA)
        ppg.run()
        assert Path("B").read_text() == "A"
        assert Path("b").read_text() == "1"
        ppg.run()
        assert Path("b").read_text() == "1"
        Path("C").write_text("C")
        jobC = ppg.FileInvariant("C")
        jobB.depends_on(jobC)
        ppg.run()
        assert Path("b").read_text() == "2"
        ppg.new()
        jobA.readd()
        jobB.readd()
        jobC.readd()
        jobB.depends_on(jobA, jobC)
        ppg.run()
        assert Path("b").read_text() == "2"
        ppg.new()
        jobA.readd()
        jobB.readd()
        jobC.readd()
        jobB.depends_on(jobA)
        ppg.run()
        assert Path("b").read_text() == "3"  # hey, we lost one!

    def test_function_invariant_binding_parameter(self, ppg_per_test):
        params = ["a"]
        jobB = ppg.FileGeneratingJob(
            "B", lambda of: counter("b") and of.write_text(params[0])
        )
        ppg.run()
        assert Path("B").read_text() == "a"
        assert Path("b").read_text() == "1"
        ppg.run()
        assert Path("B").read_text() == "a"
        assert Path("b").read_text() == "1"

        params[0] = "b"
        ppg.run()
        assert Path("B").read_text() == "b"
        assert Path("b").read_text() == "2"
        ppg.run()
        assert Path("B").read_text() == "b"
        assert Path("b").read_text() == "2"

    def test_parameter_invariant(self, ppg_per_test, job_trace_log):
        params = ["a"]
        jobA = ppg.ParameterInvariant('A', params)
        def shu(): # so the functionInvariant does not bind params itself!
            return params[0]
        jobB = ppg.FileGeneratingJob(
            "B", lambda of: counter("b") and of.write_text(shu())
        )
        jobB.depends_on(jobA)
        ppg.run()

        assert Path("B").read_text() == "a"
        assert Path("b").read_text() == "1"
        ppg.run()
        assert Path("B").read_text() == "a"
        assert Path("b").read_text() == "1"

        params[0] = "b"
        jobA = ppg.ParameterInvariant('A', params) # the parameters get frozen when teh job is defined!
        ppg.run()
        assert Path("B").read_text() == "b"
        assert Path("b").read_text() == "2"
        ppg.run()
        assert Path("B").read_text() == "b"
        assert Path("b").read_text() == "2"
