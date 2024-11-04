from pathlib import Path
import sys
import time
from loguru import logger
import pytest
import pypipegraph2 as ppg
from pypipegraph2.enums import JobOutcome
from .shared import counter, write, read


@pytest.mark.usefixtures("ppg2_per_test")
class TestPypipegraph2:
    def test_very_simple(self):
        assert not Path("A").exists()
        job = ppg.FileGeneratingJob("A", lambda of: of.write_text("Done"))
        ppg.run()
        assert Path("A").read_text() == "Done"

    def test_very_simple_chain(self):
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

    def test_very_simple_chain_reverse(self):
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

    def test_very_simple_chain_rerun(self, job_trace_log):
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
        assert Path("A").read_text() == "0"
        Path("A").unlink()
        counter = 1
        ppg.run()
        assert Path("A").read_text() == "1"
        assert Path("B").read_text() == "BBB1"

    def test_isolation(self, trace_log):
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
        logger.error("Run 4 - rerun B but not C")
        ppg.run()
        assert Path("outcount").read_text() == "1"
        # but C was not rerun, since the B output did not change.
        assert Path("C").read_text() == "CCC0"

    def test_changing_inputs(self):
        ppg.new(run_mode=ppg.RunMode.NOTEBOOK)
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

    def test_changing_inputs_when_job_was_temporarily_missing(self):
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
        assert not "A" in ppg.global_pipegraph.jobs
        assert not "B" in ppg.global_pipegraph.jobs
        jobA = ppg.FileGeneratingJob(
            "A", lambda of: counter("a") and of.write_text("AAAA")
        )
        assert not "B" in ppg.global_pipegraph.jobs
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
        jobB.depends_on(jobA)  # so that the changed A output actually get's registered
        ppg.run()
        assert Path("a").read_text() == "2"
        assert Path("A").read_text() == "AAAA"  # correctly rerun
        assert Path("B").read_text() == "BBBAAAA"  # correctly rerun

    def test_changing_bound_variables(self):
        ppg.new(run_mode=ppg.RunMode.NOTEBOOK)
        varA = "hello"
        jobA = ppg.FileGeneratingJob("A", lambda of, varA=varA: of.write_text(varA))
        ppg.run()
        assert Path("A").read_text() == "hello"

        varA = "world"
        jobA = ppg.FileGeneratingJob("A", lambda of, varA=varA: of.write_text(varA))
        ppg.run()
        assert Path("A").read_text() == "hello"

    def test_capturing_closures(self):
        ppg.new(run_mode=ppg.RunMode.NOTEBOOK)
        varA = ["hello"]
        jobA = ppg.FileGeneratingJob("A", lambda of: of.write_text(str(varA)))
        ppg.run()
        assert Path("A").read_text() == str(["hello"])

        varA.append("world")
        jobA = ppg.FileGeneratingJob("A", lambda of: of.write_text(str(varA)))
        ppg.run()
        assert Path("A").read_text() == str(["hello", "world"])

    def test_failed_pruning(self):
        def a(of):
            raise ValueError()

        jobA = ppg.FileGeneratingJob("A", a)
        jobB = ppg.FileGeneratingJob("B", lambda of: of.write_text("B"))
        jobC = ppg.FileGeneratingJob("C", lambda of: of.write_text("C"))
        jobB.depends_on(jobA)
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert Path("C").read_text() == "C"
        last = ppg.global_pipegraph.last_run_result
        print(last.keys())
        assert last["A"].outcome == JobOutcome.Failed
        assert last["B"].outcome == JobOutcome.UpstreamFailed
        assert last["C"].outcome == JobOutcome.Success
        assert "ValueError" in str(last["A"].error)

    def test_multi_file_generating_job(self):
        ppg.new(run_mode=ppg.RunMode.NOTEBOOK)

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

    def test_tempfile(self, job_trace_log):
        jobA = ppg.TempFileGeneratingJob(
            "TA",
            lambda of: counter("a") and of.write_text("A"),
            depend_on_function=False,
        )
        jobB = ppg.FileGeneratingJob(
            "B",
            lambda of: counter("b") and of.write_text("B" + Path("TA").read_text()),
            depend_on_function=False,
        )
        jobB.depends_on(jobA)
        ppg.run()
        assert not Path("TA").exists()
        assert Path("B").exists()
        assert Path("B").read_text() == "BA"
        assert Path("a").read_text() == "1"
        assert Path("b").read_text() == "1"
        logger.error("Second run - no rerun")
        ppg.run()
        assert not Path("TA").exists()
        assert Path("B").exists()
        assert Path("a").read_text() == "1"
        assert Path("b").read_text() == "1"

        Path("B").unlink()
        logger.error("Third run - B output missing")
        ppg.run()
        assert not Path("TA").exists()
        assert Path("B").exists()
        assert Path("B").read_text() == "BA"
        assert Path("a").read_text() == "2"
        assert Path("b").read_text() == "2"

    def test_tempfile_chained_invalidate_leaf(self):
        ppg.new(cores=1, log_level=6)
        jobA = ppg.TempFileGeneratingJob(
            "TA",
            lambda of: counter("a") and of.write_text("A"),
            depend_on_function=False,
        )
        jobB = ppg.TempFileGeneratingJob(
            "TB",
            lambda of: counter("b") and of.write_text("B" + Path("TA").read_text()),
            depend_on_function=False,
        )
        jobC = ppg.FileGeneratingJob(
            "C",
            lambda of: counter("c") and of.write_text("C" + Path("TB").read_text()),
            depend_on_function=False,
        )
        jobC.depends_on(jobB)
        jobB.depends_on(jobA)
        ppg.util.log_error("First run")
        ppg.run()
        assert not Path("TA").exists()
        assert not Path("TB").exists()
        assert Path("C").read_text() == "CBA"
        assert Path("a").read_text() == "1"
        assert Path("b").read_text() == "1"
        assert Path("c").read_text() == "1"
        ppg.util.log_error("Second No op run.")
        ppg.run()
        assert Path("C").read_text() == "CBA"
        assert Path("a").read_text() == "1"
        assert Path("b").read_text() == "1"
        assert Path("c").read_text() == "1"
        assert not Path("TA").exists()
        assert not Path("TB").exists()

        jobC.depends_on(ppg.FunctionInvariant(lambda: 53, "lambda_52"))
        ppg.util.log_error("Third run - rerun because of FI")
        ppg.run()
        assert Path("C").read_text() == "CBA"
        assert Path("a").read_text() == "2"
        assert Path("b").read_text() == "2"
        assert Path("c").read_text() == "2"

        assert not Path("TA").exists()
        assert not Path("TB").exists()

    def test_tempfile_chained_invalidate_intermediate(self, trace_log):
        jobA = ppg.TempFileGeneratingJob(
            "TA",
            lambda of: counter("a") and of.write_text("A"),
            depend_on_function=False,
        )
        jobB = ppg.TempFileGeneratingJob(
            "TB",
            lambda of: counter("b") and of.write_text("B" + Path("TA").read_text()),
            depend_on_function=False,
        )
        jobC = ppg.FileGeneratingJob(
            "C",
            lambda of: counter("c") and of.write_text("C" + Path("TB").read_text()),
            depend_on_function=False,
        )
        jobC.depends_on(jobB)
        jobB.depends_on(jobA)
        logger.error("First run")
        ppg.run()
        assert not Path("TA").exists()
        assert not Path("TB").exists()
        assert Path("C").read_text() == "CBA"
        assert Path("a").read_text() == "1"
        assert Path("b").read_text() == "1"
        assert Path("c").read_text() == "1"

        logger.error("Second No op run.")
        ppg.run()
        assert Path("C").read_text() == "CBA"
        assert not Path("TA").exists()
        assert not Path("TB").exists()
        assert Path("a").read_text() == "1"
        assert Path("b").read_text() == "1"
        assert Path("c").read_text() == "1"

        jobB.depends_on(ppg.FunctionInvariant(lambda: 53, "lambda_52"))
        logger.error("Third run - rerun because of FI")
        ppg.run()
        assert Path("C").read_text() == "CBA"
        assert not Path("TA").exists()
        assert not Path("TB").exists()
        assert Path("a").read_text() == "2"
        assert Path("b").read_text() == "2"
        assert (
            Path("c").read_text() == "1"
        )  # B does not change output -> c does not get rerun

    def test_just_a_tempfile(self, trace_log):
        jobA = ppg.TempFileGeneratingJob(
            "TA",
            lambda of: +counter("a") and of.write_text("A"),
            depend_on_function=False,
        )
        ppg.run()
        assert not Path("TA").exists()
        assert not Path("a").exists()

    def test_just_chained_tempfile(self, trace_log):
        jobA = ppg.TempFileGeneratingJob(
            "TA", lambda of: counter("a") and of.write_text("A")
        )
        jobB = ppg.TempFileGeneratingJob(
            "B", lambda of: counter("b") and of.write_text("B" + Path("TA").read_text())
        )
        jobB.depends_on(jobA)
        ppg.run()
        assert not Path("TA").exists()
        assert not Path("B").exists()
        assert not Path(
            "a"
        ).exists()  # changed with the smarter hull stuff - they don't run for sideeffects-and-giggles
        assert not Path("b").exists()

    def test_just_chained_tempfile_no_invariant(self, trace_log):
        jobA = ppg.TempFileGeneratingJob(
            "TA", lambda of: of.write_text("A" + counter("a")), depend_on_function=False
        )
        ppg.run()
        assert not Path("TA").exists()
        assert not Path("a").exists()

    def test_just_chained_tempfile3(self, trace_log):
        jobA = ppg.TempFileGeneratingJob(
            "A", lambda of: of.write_text("A" + counter("a"))
        )
        jobB = ppg.TempFileGeneratingJob(
            "B", lambda of: of.write_text("B" + counter("b") + Path("A").read_text())
        )
        jobC = ppg.TempFileGeneratingJob(
            "C", lambda of: of.write_text("C" + counter("c") + Path("B").read_text())
        )
        jobB.depends_on(jobA)
        jobC.depends_on(jobB)

        ppg.run()
        assert not Path("A").exists()
        assert not Path("a").exists()
        assert not Path("B").exists()
        assert not Path("b").exists()
        assert not Path("C").exists()
        assert not Path("c").exists()

    def test_tempfile_triggered_by_invalidating_final_job(self, trace_log):
        jobA = ppg.TempFileGeneratingJob(
            "TA",
            lambda of: counter("a") and of.write_text("A"),
            depend_on_function=False,
        )
        jobB = ppg.TempFileGeneratingJob(
            "TB",
            lambda of: counter("b") and of.write_text("B" + Path("TA").read_text()),
            depend_on_function=False,
        )
        jobC = ppg.FileGeneratingJob(
            "C",
            lambda of: counter("c") and of.write_text("C" + Path("TB").read_text()),
            depend_on_function=False,
        )
        jobC.depends_on(jobB)
        jobB.depends_on(jobA)
        ppg.run()
        assert Path("C").read_text() == "CBA"
        assert Path("a").read_text() == "1"
        assert Path("b").read_text() == "1"
        assert Path("c").read_text() == "1"

        jobC.depends_on(ppg.FunctionInvariant(lambda: 52, "lambda_52"))
        ppg.run()
        assert Path("C").read_text() == "CBA"
        assert Path("a").read_text() == "2"
        assert Path("b").read_text() == "2"
        assert Path("c").read_text() == "2"

    def test_tempfile_triggered_by_invalidating_tempfile(self, trace_log):
        jobA = ppg.TempFileGeneratingJob(
            "A",
            lambda of: counter("a") and of.write_text("A"),
            depend_on_function=False,
        )
        trigger = [True]
        jobB = ppg.TempFileGeneratingJob(
            "B",
            lambda of: counter("b")
            and of.write_text(("B" if trigger[0] else "x") + Path("A").read_text()),
            depend_on_function=False,
        )
        jobC = ppg.FileGeneratingJob(
            "C",
            lambda of: counter("c") and of.write_text("C" + Path("B").read_text()),
            depend_on_function=False,
        )
        jobC.depends_on(jobB)
        jobB.depends_on(jobA)
        ppg.run()
        assert Path("C").read_text() == "CBA"
        assert Path("a").read_text() == "1"
        assert Path("b").read_text() == "1"
        assert Path("c").read_text() == "1"

        jobB.depends_on(ppg.FunctionInvariant(lambda: 52, "lambda_52"))
        trigger[0] = False
        ppg.run()
        assert Path("C").read_text() == "CxA"
        assert Path("a").read_text() == "2"
        assert Path("b").read_text() == "2"
        assert Path("c").read_text() == "2"

    def test_last_invalidated_tempfile_isolation(self, trace_log):
        jobA = ppg.TempFileGeneratingJob(
            "A",
            lambda of: counter("a") and of.write_text("A"),
            depend_on_function=False,
        )
        jobB = ppg.TempFileGeneratingJob(
            "B",
            lambda of: counter("b") and of.write_text("B"),
            depend_on_function=False,
        )
        jobC = ppg.FileGeneratingJob(
            "C",
            lambda of: counter("c") and of.write_text("C" + Path("B").read_text()),
            depend_on_function=False,
        )
        jobC.depends_on(jobB)
        jobB.depends_on(jobA)
        ppg.run()
        assert Path("C").read_text() == "CB"
        assert Path("a").read_text() == "1"
        assert Path("b").read_text() == "1"
        assert Path("c").read_text() == "1"
        jobB.depends_on(ppg.FunctionInvariant(lambda: 52, "lambda_52"))
        ppg.run()
        assert Path("C").read_text() == "CB"
        assert Path("c").read_text() == "1"
        assert Path("b").read_text() == "2"
        assert Path("a").read_text() == "2"

    def test_depending_on_two_temp_jobs_but_only_one_invalidated(self):
        jobA = ppg.TempFileGeneratingJob(
            "A",
            lambda of: counter("a") and of.write_text("A"),
            depend_on_function=False,
        )
        jobB = ppg.TempFileGeneratingJob(
            "B",
            lambda of: counter("b") and of.write_text("B"),
            depend_on_function=False,
        )
        jobC = ppg.FileGeneratingJob(
            "C",
            lambda of: counter("c")
            and of.write_text("C" + Path("B").read_text() + Path("A").read_text()),
            depend_on_function=False,
        )
        jobC.depends_on(jobB)
        jobC.depends_on(jobA)
        ppg.run()
        assert Path("C").read_text() == "CBA"
        assert Path("a").read_text() == "1"
        assert Path("b").read_text() == "1"
        assert Path("c").read_text() == "1"

        jobB = ppg.TempFileGeneratingJob(
            "B",
            lambda of: counter("b") and of.write_text("BB"),
            depend_on_function=False,
        )  # not changing the function does not trigger a change

        ppg.run()
        assert Path("C").read_text() == "CBA"
        assert Path("a").read_text() == "1"
        assert Path("b").read_text() == "1"
        assert Path("c").read_text() == "1"

        jobB = ppg.TempFileGeneratingJob(
            "B",
            lambda of: counter("b") and of.write_text("BB"),
            depend_on_function=True,
        )  # but if you have a function invariant!
        ppg.run()

        assert Path("C").read_text() == "CBBA"
        assert Path("a").read_text() == "2"
        assert Path("b").read_text() == "2"
        assert Path("c").read_text() == "2"

    def test_tempjob_serving_two(self, trace_log):
        jobA = ppg.TempFileGeneratingJob(
            "TA",
            lambda of: counter("a") and of.write_text("TA"),
            depend_on_function=False,
        )
        jobB = ppg.FileGeneratingJob(
            "B",
            lambda of: counter("b") and of.write_text("B" + Path("TA").read_text()),
            depend_on_function=False,
        )
        jobC = ppg.FileGeneratingJob(
            "C",
            lambda of: counter("c") and of.write_text("C" + Path("TA").read_text()),
            depend_on_function=False,
        )
        jobB.depends_on(jobA)
        jobC.depends_on(jobA)
        ppg.run()
        assert Path("B").read_text() == "BTA"
        assert Path("C").read_text() == "CTA"
        assert Path("a").read_text() == "1"
        assert Path("b").read_text() == "1"
        assert Path("c").read_text() == "1"
        ppg.run()
        assert Path("B").read_text() == "BTA"
        assert Path("C").read_text() == "CTA"
        assert Path("a").read_text() == "1"
        assert Path("b").read_text() == "1"
        assert Path("c").read_text() == "1"
        Path("B").unlink()
        ppg.run()
        assert Path("B").read_text() == "BTA"
        assert Path("C").read_text() == "CTA"  # TA1 invalidates C when it runs.
        assert Path("a").read_text() == "2"
        assert Path("b").read_text() == "2"
        assert (
            Path("c").read_text() == "1"
        )  # input to C is A, A's output did not chaneg.

        ppg.run()
        assert Path("B").read_text() == "BTA"
        assert Path("C").read_text() == "CTA"
        assert Path("a").read_text() == "2"
        assert Path("b").read_text() == "2"
        assert Path("c").read_text() == "1"
        Path("B").unlink()
        Path("C").unlink()
        ppg.run()
        assert Path("B").read_text() == "BTA"
        assert Path("C").read_text() == "CTA"
        assert Path("a").read_text() == "3"
        assert Path("b").read_text() == "3"
        assert Path("c").read_text() == "2"

    def test_two_temp_jobs(self, trace_log):
        """test_two_temp_jobs
        This tests one of the 'unnecessary' temp job reruns.
        We have these jobs
        Fi:TA -> TA -> C
                       ^
        Fi:TB -> TB    ->  D

        (todo: this full argument might have been invalidated
        by the non-transitive-hull changes?)

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
        ppg.new(run_mode=ppg.RunMode.NOTEBOOK, log_level=5)

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
        def is_true():  # funilly enough, 'and True' or some similar constant
            # will lead to the same byte code as when leaving 'and True' off,
            # thereby not invalidating the FunctionInvariant
            return True

        jobB = ppg.TempFileGeneratingJob(
            "TB", lambda of: counter("b") and is_true and of.write_text("B")
        )
        ppg.run()
        assert Path("b").read_text() == "2"  # we trigger that one
        assert (
            Path("a").read_text() == "1"
        )  # the FunctionInvariant:TB was pulled into TA's upstream by the rewrite
        assert Path("c").read_text() == "1"  # but this one was isolated
        assert Path("d").read_text() == "1"  # as was this one was isolated

    def test_cycles(self):
        jobA = ppg.FileGeneratingJob("A", lambda of: of.write_text("Done"))
        jobB = ppg.FileGeneratingJob("B", lambda of: of.write_text("Done"))
        jobC = ppg.FileGeneratingJob("C", lambda of: of.write_text("Done"))
        jobA.depends_on(jobB)
        with pytest.raises(ppg.exceptions.NotADag):
            jobB.depends_on(jobA)  # simple one-step cycles: early !
        jobB.depends_on(jobC.depends_on(jobA))
        # bigger cycles: later
        with pytest.raises(ppg.exceptions.NotADag):
            ppg.run()

    def test_jobs_run_in_different_pids(self):
        import os

        pid_here = os.getpid()
        a = ppg.FileGeneratingJob("A", lambda of: of.write_text(str(os.getpid())))
        b = ppg.FileGeneratingJob("B", lambda of: of.write_text(str(os.getpid())))
        ppg.run()
        pid_a = Path("A").read_text()
        pid_b = Path("B").read_text()
        assert pid_a != pid_b
        assert pid_a != pid_here

    def test_temp_jobs_run_in_different_pids(self):
        import os

        pid_here = os.getpid()
        a = ppg.TempFileGeneratingJob(
            "A", lambda of: counter("A") and Path("a").write_text(str(os.getpid()))
        )
        b = ppg.TempFileGeneratingJob(
            "B", lambda of: counter("B") and Path("b").write_text(str(os.getpid()))
        )
        c = ppg.FileGeneratingJob("C", lambda of: counter("C"))
        c.depends_on(a, b)
        ppg.run()
        pid_a = Path("a").read_text()
        pid_b = Path("b").read_text()
        assert pid_a != pid_b
        assert pid_a != pid_here

    def test_temp_job_not_writing_its_file(self):
        import os

        pid_here = os.getpid()
        a = ppg.TempFileGeneratingJob(
            "A", lambda of: counter("A") and Path("a").write_text(str(os.getpid()))
        )
        b = ppg.TempFileGeneratingJob(
            "B", lambda of: counter("b") and Path("b").write_text(str(os.getpid()))
        )  # yes, it's planned that it doesn't write B, this exposed a bug
        c = ppg.FileGeneratingJob("C", lambda of: counter("C"))
        c.depends_on(a, b)
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        last = ppg.global_pipegraph.last_run_result
        assert last["A"].outcome == JobOutcome.Success
        assert last["B"].outcome == JobOutcome.Failed
        assert last["C"].outcome == JobOutcome.UpstreamFailed
        assert isinstance(last["B"].error.args[0], ppg.JobContractError)

    def test_file_gen_when_file_existed_outside_of_graph_depending_on_cached_data_load(
        self, job_trace_log
    ):
        # this exposed a bug when the file was existing
        # the graph would never return.
        o = []

        def load(x):
            o.append(x)

        load_job, cache_job = ppg.CachedDataLoadingJob(
            "b", lambda: "52", load, depend_on_function=False
        )
        a = ppg.FileGeneratingJob("A", lambda of: of.write_text("a" + o[0]))
        a.depends_on(load_job)
        Path("b").write_text("b")
        ppg.run()
        assert Path("A").read_text() == "a52"

    def test_event_timeout_handling(self):
        def doit(of):
            import time

            time.sleep(2)
            of.write_text("a")

        job = ppg.FileGeneratingJob("a", doit)
        ppg.run(event_timeout=0.1)
        assert Path("a").exists()

    @pytest.mark.skip  # ppg2_rust obsoletes this. Maybe
    def test_catching_catastrophic_execution_message_passing_failures(self):
        import pickle

        class BadFileGeneratingJob(ppg.FileGeneratingJob):
            """A file generating job that does not output_needed()= True
            if it has no history
            """

            def output_needed(self, runner):
                for fn in self.files:
                    if not fn.exists():
                        return True
                    # other wise we have no history, and the skipping will
                    # break the graph execution
                    # if str(fn) not in runner.job_outcome[self.job_id].historical_output:
                    #    return True
                return False

        # this exposed a bug when the file was existing
        # the graph would never return.
        o = []

        def load(x):
            o.append(x)

        old_fg = ppg.FileGeneratingJob
        try:
            ppg.jobs.FileGeneratingJob = BadFileGeneratingJob

            load_job, cache_job = ppg.CachedDataLoadingJob(
                "b", lambda: "52", load, depend_on_function=False
            )
            a = ppg.FileGeneratingJob("A", lambda of: of.write_text("a" + o[0]))
            a.depends_on(load_job)
            load_job2, cache_job2 = ppg.CachedDataLoadingJob(
                "c", lambda: "52", load, depend_on_function=False
            )
            d = ppg.FileGeneratingJob("D", lambda of: of.write_text("d" + o[-1]))
            d.depends_on(load_job2)
            Path("c").write_text("c")
            # write something sensible
            with open("b", "wb") as op:
                pickle.dump("153", op)
            # now this does not get rewritten
            # because of the BadFileGeneratingJob
            assert type(cache_job) is BadFileGeneratingJob
            # with the new execution engine (JobOutcome based)
            # this is no longer an issue
            # at worst, you'll get a pickle failed error if the job dies
            with pytest.raises(ppg.JobsFailed):
                ppg.run()
            assert read("A") == "a153"
            assert "UnpicklingError" in str(load_job2.exception)
        finally:
            ppg.jobs.FileGeneratingJob = old_fg

    def test_changing_mtime_triggers_recalc_of_hash(self):
        import datetime
        import time
        import os

        write("A", "hello")
        fi = ppg.FileInvariant("A")
        of = ppg.FileGeneratingJob("B", lambda of: write(of, read("A")))
        of.depends_on(fi)
        info = ppg.run()

        assert fi.did_hash_last_run
        del fi.did_hash_last_run  # so we detect if it's not run() at all
        ppg.run()
        assert fi.did_hash_last_run is False
        date = datetime.datetime(
            year=2020, month=12, day=12, hour=12, minute=12, second=12
        )
        modTime = time.mktime(date.timetuple())
        os.utime("A", (modTime, modTime))
        info = ppg.run()
        assert fi.did_hash_last_run
        # and for good measure, check that B wasn't run
        assert info["B"].outcome is JobOutcome.Skipped
        assert read("B") == "hello"

    def test_same_mtime_same_size_leads_to_false_negative(self):
        import datetime
        import time
        import os

        write("A", "hello")
        date = datetime.datetime(
            year=2020, month=12, day=12, hour=12, minute=12, second=12
        )
        modTime = time.mktime(date.timetuple())
        os.utime("A", (modTime, modTime))

        fi = ppg.FileInvariant("A")
        of = ppg.FileGeneratingJob("B", lambda of: write(of, read("A")))
        of.depends_on(fi)
        info = ppg.run()
        assert read("B") == "hello"
        write("A", "world")
        os.utime("A", (modTime, modTime))  # evily
        ppg.run()
        assert not fi.did_hash_last_run
        assert read("B") == "hello"
        ppg.run()
        assert not fi.did_hash_last_run
        assert read("B") == "hello"
        write("A", "world")
        ppg.run()
        assert fi.did_hash_last_run
        assert read("B") == "world"

    def test_file_invariant(self):
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

    def test_adding_and_removing_variants(self):
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

    def test_function_invariant_binding_parameter(self):
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

    def test_parameter_invariant(self):
        ppg.new(run_mode=ppg.RunMode.NOTEBOOK)
        params = ["a"]
        jobA = ppg.ParameterInvariant("A", params)

        def shu():  # so the functionInvariant does not bind params itself!
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
        jobA = ppg.ParameterInvariant(
            "A", params
        )  # the parameters get frozen when the job is defined!
        ppg.run()
        assert Path("B").read_text() == "b"
        assert Path("b").read_text() == "2"
        ppg.run()
        assert Path("B").read_text() == "b"
        assert Path("b").read_text() == "2"

    @pytest.mark.skip  # no longer relevant onnce we switched to deephash
    def test_parameter_invariant_needs_hash(self, create_out_dir):
        class NoHash:
            def __hash__(self):
                raise TypeError("can't hash this")

        with pytest.raises(TypeError):
            ppg.ParameterInvariant("C", (NoHash(),))

    def test_data_loading_job_returning_none(self):
        self.store = []  # use attribute to avoid closure binding
        jobA = ppg.DataLoadingJob("A", lambda: self.store.append("A"))
        jobB = ppg.FileGeneratingJob(
                "B", lambda of: counter("b") and of.write_text(self.store[0])
            )
        jobB.depends_on(jobA)
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert ppg.global_pipegraph.last_run_result["A"].outcome is JobOutcome.Failed

    def test_data_loading_job(self):
        ppg.new(run_mode=ppg.RunMode.NOTEBOOK)
        self.store = []  # use attribute to avoid closure binding
        try:
            jobA = ppg.DataLoadingJob("A", lambda: self.store.append("A") or 5)
            jobB = ppg.FileGeneratingJob(
                "B", lambda of: counter("b") and of.write_text(self.store[0])
            )
            jobB.depends_on(jobA)
            assert len(self.store) == 0
            ppg.run()
            assert len(self.store) == 1
            assert Path("B").read_text() == "A"
            assert Path("b").read_text() == "1"
            ppg.run()
            assert len(self.store) == 1
            assert Path("b").read_text() == "1"
            jobB.depends_on(ppg.ParameterInvariant("C", "C"))
            self.store.clear()  # so we can be sure the DataLoadingJob ran agin.
            ppg.run()
            assert len(self.store) == 1
            assert Path("b").read_text() == "2"
            assert Path("B").read_text() == "A"
            ppg.run()

            assert len(self.store) == 1
            assert Path("b").read_text() == "2"
            assert Path("B").read_text() == "A"
            self.store.clear()
            jobA = ppg.DataLoadingJob("A", lambda: self.store.append("B") or 5)
            ppg.util.log_error("final run")
            ppg.run()
            assert len(self.store) == 1
            # assert Path("b").read_text() == "3"
            # assert Path("B").read_text() == "B"
            # with the rust based engine, we do not rerun
            # because ephemeral jobs must not change their output
            # if their input was unchanged (=Validated)
            assert Path("b").read_text() == "2"
            assert Path("B").read_text() == "A"

        finally:
            del self.store

    def test_attribute_loading_job(self):
        ppg.new(run_mode=ppg.RunMode.NOTEBOOK)

        class TestRecv:
            def __init__(self):
                self.job = ppg.AttributeLoadingJob(
                    "A", self, "a_", lambda: counter("a") and "A"
                )

        a = TestRecv()
        jobB = ppg.FileGeneratingJob(
            "B", lambda of: counter("b") and of.write_text(a.a_)
        )
        jobB.depends_on(a.job)
        ppg.run()
        assert not hasattr(a, "a_")
        assert Path("B").read_text() == "A"
        assert Path("b").read_text() == "1"
        assert Path("a").read_text() == "1"
        ppg.run()
        assert not hasattr(a, "a_")
        assert Path("B").read_text() == "A"
        assert Path("b").read_text() == "1"
        assert Path("a").read_text() == "1"

        a.job = ppg.AttributeLoadingJob("A", a, "a_", lambda: counter("a") and "B")
        ppg.run()
        assert Path("B").read_text() == "B"
        assert Path("b").read_text() == "2"
        assert Path("a").read_text() == "2"
        assert not hasattr(a, "a_")

    def test_dict_loading_job(self):
        ppg.new(run_mode=ppg.RunMode.NOTEBOOK)

        class TestRecv:
            def __init__(self):
                self.store = {}
                self.job = ppg.DictEntryLoadingJob(
                    "A", self.store, "a_", lambda: counter("a") and "A"
                )

        a = TestRecv()
        jobB = ppg.FileGeneratingJob(
            "B", lambda of: counter("b") and of.write_text(a.store["a_"])
        )
        jobB.depends_on(a.job)
        ppg.run()
        assert not hasattr(a, "a_")
        assert Path("B").read_text() == "A"
        assert Path("b").read_text() == "1"
        assert Path("a").read_text() == "1"
        ppg.run()
        assert not hasattr(a, "a_")
        assert Path("B").read_text() == "A"
        assert Path("b").read_text() == "1"
        assert Path("a").read_text() == "1"

        a.job = ppg.DictEntryLoadingJob("A", a.store, "a_", lambda: counter("a") and "B")
        ppg.run()
        assert Path("B").read_text() == "B"
        assert Path("b").read_text() == "2"
        assert Path("a").read_text() == "2"
        assert not hasattr(a, "a_")

    def test_cached_attribute_loading_job(self):
        ppg.new(run_mode=ppg.RunMode.NOTEBOOK)

        class TestRecv:
            def __init__(self):
                self.job = ppg.CachedAttributeLoadingJob(
                    "A", self, "a_", lambda: counter("a") and "A"
                )

        a = TestRecv()
        jobB = ppg.FileGeneratingJob(
            "B", lambda of: counter("b") and of.write_text(a.a_)
        )
        jobB.depends_on(a.job[0])
        assert not Path("A").exists()
        ppg.run()
        assert not hasattr(a, "a_")
        assert Path("B").read_text() == "A"
        assert Path("b").read_text() == "1"
        assert Path("a").read_text() == "1"
        assert Path("A").exists()
        ppg.run()
        assert not hasattr(a, "a_")
        assert Path("B").read_text() == "A"
        assert Path("b").read_text() == "1"
        assert Path("a").read_text() == "1"
        assert Path("A").exists()

        a.job = ppg.CachedAttributeLoadingJob(
            "A", a, "a_", lambda: counter("a") and "B"
        )
        logger.info("Run leads to recalc of A, B")
        ppg.run()
        assert Path("B").read_text() == "B"
        assert Path("b").read_text() == "2"
        assert Path("a").read_text() == "2"
        assert not hasattr(a, "a_")
        assert Path("A").exists()

    def test_cached_dictentry_loading_job(self):
        ppg.new(run_mode=ppg.RunMode.NOTEBOOK)

        class TestRecv:
            def __init__(self):
                self.store = {}
                self.job = ppg.CachedDictEntryLoadingJob(
                    "A", self.store, "a_", lambda: counter("a") and "A"
                )

        a = TestRecv()
        jobB = ppg.FileGeneratingJob(
            "B", lambda of: counter("b") and of.write_text(a.store["a_"])
        )
        jobB.depends_on(a.job[0])
        assert not Path("A").exists()
        ppg.run()
        assert not hasattr(a, "a_")
        assert Path("B").read_text() == "A"
        assert Path("b").read_text() == "1"
        assert Path("a").read_text() == "1"
        assert Path("A").exists()
        ppg.run()
        assert not hasattr(a, "a_")
        assert Path("B").read_text() == "A"
        assert Path("b").read_text() == "1"
        assert Path("a").read_text() == "1"
        assert Path("A").exists()

        a.job = ppg.CachedDictEntryLoadingJob(
            "A", a.store, "a_", lambda: counter("a") and "B"
        )
        logger.info("Run leads to recalc of A, B")
        ppg.run()
        assert Path("B").read_text() == "B"
        assert Path("b").read_text() == "2"
        assert Path("a").read_text() == "2"
        assert not hasattr(a, "a_")
        assert Path("A").exists()

    def test_job_generating(self):
        def inner():  # don't keep it inside, or the FunctionInvariant will trigger each time.
            counter("a")
            b = ppg.FileGeneratingJob(
                "B", lambda of: counter("b") and of.write_text("B")
            )
            c = ppg.FileGeneratingJob(
                "C", lambda of: counter("c") and of.write_text("C" + read("B"))
            )
            c.depends_on(b)

        def gen():
            return ppg.JobGeneratingJob("A", inner)

        gen()
        ppg.run()
        assert ppg.global_pipegraph.has_edge("B", "C")
        assert Path("a").read_text() == "1"
        assert Path("b").read_text() == "1"
        assert Path("c").read_text() == "1"
        assert Path("B").read_text() == "B"
        assert Path("C").read_text() == "CB"

        # no rerun
        ppg.new()
        gen()
        ppg.run()
        assert Path("a").read_text() == "2"
        assert Path("b").read_text() == "1"
        assert Path("c").read_text() == "1"
        assert Path("B").read_text() == "B"
        assert Path("C").read_text() == "CB"
        ppg.new()
        jobA = gen()
        jobA.depends_on(ppg.ParameterInvariant("PA", "a"))
        ppg.run()
        assert Path("a").read_text() == "3"
        assert Path("b").read_text() == "1"  # this does not mean that B get's rerun.
        assert Path("c").read_text() == "1"
        assert Path("B").read_text() == "B"
        assert Path("C").read_text() == "CB"

        ppg.new()
        Path("B").unlink()
        gen()
        jobA.depends_on(ppg.ParameterInvariant("PA", "a"))
        ppg.run()
        assert Path("a").read_text() == "4"  # a runs once per ppg.run()
        assert Path("b").read_text() == "2"  # must rerun b, since file B is missing
        assert Path("c").read_text() == "1"  # but this one is insulated
        assert Path("B").exists()
        assert Path("C").read_text() == "CB"

        ppg.new()
        gen()
        ppg.run()  # missing ParameterInvariant triggers A to run
        assert (
            Path("a").read_text() == "5"
        )  # still no rerun - input to A didn't change!
        assert Path("b").read_text() == "2"  # this does not mean that B get's rerun.
        assert Path("c").read_text() == "1"  # this does not mean that B get's rerun.
        assert Path("B").read_text() == "B"
        assert Path("C").read_text() == "CB"

    def test_job_generating_generated_fails_rerun(self):
        # local_counter = [0]

        def inner():
            counter("a")

            def fg(of):
                if counter("b") in ("0", "1"):
                    raise ValueError()
                of.write_text("B")

            ppg.FileGeneratingJob("B", fg)

        def gen():
            ppg.JobGeneratingJob("A", inner)

        gen()
        assert not Path("a").exists()
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert Path("a").read_text() == "1"
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert Path("a").read_text() == "2"  # new .run means rerun this thing
        assert not Path("B").exists()
        ppg.new()
        gen()
        ppg.run()
        assert Path("B").read_text() == "B"
        assert (
            Path("a").read_text() == "3"
        )  # new pipegraph means we need to rerun the jobgenerating job

    def test_filegen_not_creating_files_throws_job_contract(self):
        jobA = ppg.FileGeneratingJob("A", lambda of: 55)
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert isinstance(
            ppg.global_pipegraph.last_run_result[jobA.job_id].error.args[0],
            ppg.JobContractError,
        )

    @pytest.mark.xfail
    def test_undeclared_output_leads_to_job_and_ppg_failure(self):
        raise NotImplementedError()

    def test_working_failing_filegen(self):
        ppg.new(run_mode=ppg.RunMode.NOTEBOOK)
        a = ppg.FileGeneratingJob(
            "A", lambda of: write(of, "A"), depend_on_function=True
        )
        ppg.run()
        write("do_raise", "True")

        def raiser(of):
            if read("do_raise") == "True":
                write(of, "B")
                raise ValueError()
            else:
                write(of, "C")

        a = ppg.FileGeneratingJob("A", raiser)
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        write("do_raise", "False")
        ppg.run()
        assert read("A") == "C"

    def test_failing_working_no_function_dep(self):
        write("do_raise", "True")

        def raiser(of):
            if read("do_raise") == "True":
                write(of, "B")
                raise ValueError()
            else:
                write(of, "C")

        ppg.FileGeneratingJob("A", raiser, depend_on_function=False)
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert read("A") == "B"
        write("do_raise", "False")
        ppg.run()
        assert read("A") == "C"

    def test_failing_plus_job_gen_runs_failing_only_once(self):
        def a(of):
            counter(of)
            counter("a")
            raise ValueError()

        def b():
            counter("B")
            ppg.FileGeneratingJob("C", lambda of: counter(of))

        jobA = ppg.FileGeneratingJob("A", a)
        ppg.FileGeneratingJob("E", lambda of: counter(of)).depends_on(jobA)
        ppg.JobGeneratingJob("B", b)
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert read("B") == "1"
        assert read("C") == "1"
        assert read("A") == "1"
        assert read("a") == "1"
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert read("A") == "1"  # get's unlinked prior to run
        assert read("a") == "2"  # the real 'run' counter'

    def test_actually_multithreading(self):
        # use the Barrier  primivite to force it to actually wait for all three threads
        from threading import Barrier

        barrier = Barrier(3, timeout=1)

        def inner(of):
            def inner2():
                c = barrier.wait()
                write(of, str(c))
                return ppg.UseInputHashesForOutput()

            return inner2

        ppg.new(cores=3)
        a = ppg.DataLoadingJob("A", inner("A") or ppg.UseInputHashesForOutput(), depend_on_function=False)
        b = ppg.DataLoadingJob("B", inner("B") or ppg.UseInputHashesForOutput(), depend_on_function=False)
        c = ppg.DataLoadingJob("C", inner("C") or ppg.UseInputHashesForOutput(), depend_on_function=False)
        d = ppg.JobGeneratingJob("D", lambda: None)
        d.depends_on(a, b, c)

        ppg.run()
        seen = set()
        seen.add(read("A"))
        seen.add(read("B"))
        seen.add(read("C"))
        assert seen == set(["0", "1", "2"])

    def test_failing_jobs_and_downstreams(self):
        def do_a(of):
            raise ValueError()

        a = ppg.FileGeneratingJob("A", do_a)
        b = ppg.FileGeneratingJob("B", lambda of: of.write_text(Path("A").read_text()))
        b.depends_on(a)
        c = ppg.FileGeneratingJob("C", lambda of: write(of, "C"))
        c.depends_on(b)
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert not Path("A").exists()
        assert not Path("B").exists()
        assert not Path("C").exists()

    def test_no_log_dir(self):
        ppg.new(dir_config=ppg.DirConfig(".ppg", log_dir=None))
        c = ppg.FileGeneratingJob("C", lambda of: write(of, "C"))
        ppg.run()
        assert read("C") == "C"

    def test_job_lying_about_its_outputs(self, job_trace_log):
        # tests the job returned the wrong set of outputs detection
        class LyingJob(ppg.FileGeneratingJob):
            def run(self, runner, historical_output):
                result = super().run(runner, historical_output)
                result["shu"] = "sha"
                return result

        a = LyingJob("A", lambda of: counter("shu") and write(of, "shu"))
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        print(ppg.global_pipegraph.last_run_result)
        error = ppg.global_pipegraph.last_run_result["A"].error
        assert isinstance(error, ppg.JobContractError)

    def test_failing_temp_does_not_get_run_twice(self):
        def a(of):
            counter("a")
            raise ValueError()

        jobA = ppg.TempFileGeneratingJob("A", a)
        jobB = ppg.FileGeneratingJob("B", lambda of: write(of, "B"))
        jobB.depends_on(jobA)
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert read("a") == "1"
        assert not Path("B").exists()

    def test_new_default_params(self):
        ppg.new(cores=50)
        assert ppg.global_pipegraph.cores == 50
        ppg.new()
        assert ppg.global_pipegraph.cores == 50
        ppg.new(cores=ppg.default)
        assert ppg.global_pipegraph.cores == ppg.util.CPUs()

    def test_two_job_failing(self):
        def err(of):
            raise ValueError()

        ppg.FileGeneratingJob("A", err)
        ppg.FileGeneratingJob("B", err)
        ppg.FileGeneratingJob("C", lambda of: write(of, str(of)))
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert len(ppg.global_pipegraph.do_raise) == 2  # both exceptions
        assert ppg.global_pipegraph.last_run_result["A"].error
        assert ppg.global_pipegraph.last_run_result["B"].error
        with pytest.raises(AttributeError):
            ppg.global_pipegraph.last_run_result["C"].error

    def test_getting_source_after_chdir(self):
        def inner(something):
            return 552341512412

        import os

        old = os.getcwd()
        try:
            f = ppg.FunctionInvariant("shu", inner)
            os.chdir("/tmp")
            assert f.get_source_file_name().is_absolute()
            assert "552341512412" in f.get_source()[0]
        finally:
            os.chdir(old)

    def test_massive_exception(self):
        should_len = 1024 * 1024

        def inner(_):
            raise ValueError("x " * (should_len // 2))

        ppg.FileGeneratingJob("A", inner)
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        # make sure we captured it all
        assert (
            len(ppg.global_pipegraph.last_run_result["A"].error.args[0].args[0])
            == should_len
        )

    def test_depends_on_func(self):
        a = ppg.FileGeneratingJob("A", lambda of: of.write_text("A"))

        def inner():
            return 55

        f1 = a.depends_on_func("mylambda1", lambda: 55)
        f2 = a.depends_on_func("inner", inner)
        f3 = a.depends_on_func(inner)
        f4 = a.depends_on_func(open)  # built in
        assert isinstance(f1.invariant, ppg._FunctionInvariant)
        assert isinstance(f2.invariant, ppg._FunctionInvariant)
        assert isinstance(f3.invariant, ppg._FunctionInvariant)
        assert isinstance(f4.invariant, ppg._FunctionInvariant)
        assert f1.self is a
        assert f2.self is a
        assert f3.self is a
        assert f4.self is a
        assert ppg.global_pipegraph.has_edge(f1.invariant, a)
        assert ppg.global_pipegraph.has_edge(f2.invariant, a)
        assert ppg.global_pipegraph.has_edge(f3.invariant, a)
        assert ppg.global_pipegraph.has_edge(f4.invariant, a)
        with pytest.raises(ValueError):
            a.depends_on_func("open")

    def test_partial_running_job(self):
        def a(of):
            counter("A")
            with open(of, "w") as op:
                op.write("one\n")
                raise ValueError()
                op.write("two\n")  # pragma: no cover

        job = ppg.FileGeneratingJob("a", a)
        ppg.run(raise_on_job_error=False)
        assert read("a") == "one\n"
        assert read("A") == "1"
        ppg.run(raise_on_job_error=False)
        assert read("A") == "2"

    def test_declaring_filegen_with_function_without_parameter_raises_immediatly(self):
        with pytest.raises(TypeError):
            ppg.FileGeneratingJob("A", lambda: None)

    def test_multi_file_generating_job_with_dict_file_definition(self):
        def ab(files):
            files["a"].write_text("A")
            files["b"].write_text("A")

        a = ppg.MultiFileGeneratingJob({"a": "A", "b": "B"}, ab)
        b = ppg.FileGeneratingJob("c", lambda of: of.write_text(a["a"].read_text()))
        b.depends_on(a["a"])
        ppg.run()
        with pytest.raises(ValueError):
            d = ppg.MultiFileGeneratingJob(["d"], lambda of: None)
            d["shu"]

    def test_no_error_dir(self):
        ppg.new(dir_config=ppg.DirConfig(error_dir=None))
        try:
            ppg.FileGeneratingJob("A", lambda of: of.write_text("A"))

            def b(of):
                raise ValueError()

            ppg.FileGeneratingJob("B", b)
            with pytest.raises(ppg.JobsFailed):
                ppg.run()
        finally:
            del ppg._last_new_arguments["dir_config"]  # reset to default

    def test_no_logs(self):
        ppg.new(dir_config=ppg.DirConfig(log_dir=None))
        try:
            ppg.FileGeneratingJob("A", lambda of: of.write_text("A"))

            def b(of):
                raise ValueError()

            ppg.FileGeneratingJob("B", b)
            with pytest.raises(ppg.JobsFailed):
                ppg.run()
        finally:
            del ppg._last_new_arguments["dir_config"]  # reset to default

    def test_dir_config(self):
        import shutil

        try:
            default_paths = [
                Path(".ppg/per_script/.pytest-wrapped/history"),
                Path(".ppg/per_script/pytest/history"),
            ]
            for d in default_paths:
                if d.exists():
                    default_path = d
                    break
            else:
                raise ValueError(
                    f"default path not found. We have {list(Path('.ppg/per_script').glob('*'))} available"
                )

            shutil.rmtree(default_path)  # from the default created ppg
            ppg.new(dir_config=ppg.DirConfig(".ppg2"))
            ppg.run()
            assert not default_path.exists()
            assert Path(".ppg2/history").exists()
            ppg.new(dir_config="ppg3")
            ppg.run()
            assert Path("ppg3/history").exists()
        finally:
            del ppg._last_new_arguments["dir_config"]  # reset to default

    def test_cache_dir(self):
        ppg.new(dir_config=ppg.DirConfig(cache_dir="shu"))
        assert Path("shu").exists()
        ppg.new(dir_config=ppg.DirConfig(cache_dir=None))
        a = ppg.FileGeneratingJob("a", lambda of: of.write_text("A"))
        ppg.run()
        assert Path("a").read_text() == "A"

    def test_log_retention(self):
        old_timeformat = ppg.graph.time_format
        try:
            ppg.graph.time_format = (
                "%Y-%m-%d_%H-%M-%S-%f"  # we need subsecond resolution for this test.
            )
            ppg.new(log_retention=1)  # so keep 2
            ppg.FileGeneratingJob("A", lambda of: of.write_text("A"))
            assert len(list(ppg.global_pipegraph.dir_config.log_dir.glob("*.log"))) == 0
            ppg.run()
            assert (
                len(list(ppg.global_pipegraph.dir_config.log_dir.glob("*messages*")))
                == 1 + 1  # for latest
            )  # runtimes
            ppg.run()
            assert (
                len(list(ppg.global_pipegraph.dir_config.log_dir.glob("*messages*")))
                == 2 + 1  # for latest
            )  # runtimes
            ppg.new(log_retention=2)
            ppg.run()
            prior = list(ppg.global_pipegraph.dir_config.log_dir.glob("*messages*"))
            assert (
                len(list(ppg.global_pipegraph.dir_config.log_dir.glob("*messages*")))
                == 3 + 1  # for latest
            )  # runtimes
            # no new.. still new log file please
            ppg.run()
            after = list(ppg.global_pipegraph.dir_config.log_dir.glob("*messages*"))
            assert (
                len(list(ppg.global_pipegraph.dir_config.log_dir.glob("*messages*")))
                == 3 + 1  # for latest
            )  # runtimes
            assert set([x.name for x in prior]) != set([x.name for x in after])

        finally:
            del ppg._last_new_arguments["log_retention"]
            ppg.graph.time_format = old_timeformat

    def test_nested_exception_traceback(self):
        def a():
            try:
                raise ValueError()
            except ValueError as e:
                raise KeyError() from e

        jobA = ppg.DataLoadingJob("a", a)
        b = ppg.FileGeneratingJob("b", lambda of: write("b", "b"))
        b.depends_on(jobA)
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        e = (
            ppg.global_pipegraph.dir_config.error_dir
            / ppg.global_pipegraph.time_str
            / "0_exception.txt"
        ).read_text()
        assert "KeyError" in e
        assert "ValueError" in e
        assert e.index("ValueError") < e.index("KeyError")
        assert "cause" in e

    @pytest.mark.skip  # TODO: Renaming support?
    def test_renaming_input_while_invalidating_other(self):
        a = ppg.FileGeneratingJob("A", lambda of: of.write_text("A"))
        b = ppg.FileGeneratingJob("B", lambda of: of.write_text("B"))

        def C(of):
            counter("C")
            of.write_text(a.files[0].read_text() + b.files[0].read_text())

        c = ppg.FileGeneratingJob(
            "c", C, depend_on_function=False
        )  # otherwise renaming the input job will trigger already.
        c.depends_on(a, b)
        ppg.run()
        assert read("C") == "1"
        assert read("c") == "AB"
        ppg.new()
        a = ppg.FileGeneratingJob("A1", lambda of: of.write_text("A"))
        b = ppg.FileGeneratingJob("B", lambda of: of.write_text("B"))
        c = ppg.FileGeneratingJob("c", C, depend_on_function=False)
        c.depends_on(a, b)
        time.sleep(1)  # always change mtime - it is being rewritten, right
        ppg.run()  # no rerun, changed name detection.
        assert read("C") == "1"
        assert read("c") == "AB"
        ppg.new()
        a = ppg.FileGeneratingJob("A2", lambda of: of.write_text("A"))
        b = ppg.FileGeneratingJob("B", lambda of: of.write_text("B2"))
        c = ppg.FileGeneratingJob("c", C, depend_on_function=False)
        c.depends_on(a, b)
        ppg.run()  # rerun, not because of the name change, but because b2 changed
        assert read("C") == "2"
        assert read("c") == "AB2"

    @pytest.mark.skip  # TODO: Renaming support?
    def test_renaming_maps_to_muliple(self):
        a = ppg.FileGeneratingJob("A", lambda of: of.write_text("A"))
        b = ppg.FileGeneratingJob("B", lambda of: of.write_text("A"))
        d = ppg.FileGeneratingJob("C", lambda of: counter("c") and of.write_text("c"))
        d.depends_on(a, b)
        ppg.run()
        assert read("c") == "1"
        ppg.new()
        a = ppg.FileGeneratingJob("A1", lambda of: of.write_text("A"))
        b = ppg.FileGeneratingJob("B", lambda of: of.write_text("A"))
        d = ppg.FileGeneratingJob("C", lambda of: counter("c") and of.write_text("c"))
        d.depends_on(a, b)

        ppg.run()
        assert read("c") == "2"

    def test_chained_failing_temps(self):
        def a(of):
            of.write_text("A")

        def b(of):
            of.write_text(Path("A").read_text())
            raise ValueError()

        def c(of):
            of.write_text(Path("B").read_text())

        a = ppg.TempFileGeneratingJob("A", a)
        b = ppg.TempFileGeneratingJob("B", b)
        c = ppg.FileGeneratingJob("C", c)
        d = ppg.JobGeneratingJob(
            "D",
            lambda: counter("D")
            and ppg.FileGeneratingJob("E", lambda of: of.write_text("E")),
        )
        b.depends_on(a)
        c.depends_on(b)
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert read("D") == "1"

    def test_chained_failing_temps_no_downstream(self):
        def a(of):
            raise ValueError()
            of.write_text("A")

        def b(of):
            of.write_text(Path("A").read_text())

        def c(of):
            of.write_text(Path("B").read_text())

        a = ppg.TempFileGeneratingJob("A", a)
        b = ppg.TempFileGeneratingJob("B", b)
        c = ppg.TempFileGeneratingJob("C", c)
        d = ppg.JobGeneratingJob(
            "D",
            lambda: counter("D")
            and ppg.FileGeneratingJob("E", lambda of: of.write_text("E")),
        )
        b.depends_on(a)
        c.depends_on(b)
        # with pytest.raises(ppg.JobsFailed):
        ppg.run()  # won't raise since the tfs never get run.
        # they don't get run, because we create the 'clone jobs'
        # backwards - and c disappears
        # since it had no downstreams of its own.
        # and then the 2nd level (b) disappears, because
        # it no longer has a downstream
        # and the same happens to a
        assert read("D") == "1"

    # this test is fairly unreliable with shifting pandas
    # and python versions - and it's a nieche case anyway.
    # def test_no_source_traceback(self):
    #     def a(of):
    #         import pandas

    #         df = pandas.DataFrame()
    #         df["shu"]  # which should raise

    #     ppg.FileGeneratingJob("a", a)
    #     with pytest.raises(ppg.JobsFailed):
    #         ppg.run()
    #     e = (
    #         ppg.global_pipegraph.dir_config.error_dir
    #         / ppg.global_pipegraph.time_str
    #         / "0_exception.txt"
    #     ).read_text()
    #     if (3, 8, 0) < sys.version_info[:3] < (3, 10, 11):
    #         # other versions have the source.
    #         assert "# no source available" in e
    #     assert "KeyError" in e

    def test_redefining_with_different_type(self):
        a = ppg.FileGeneratingJob("a", lambda of: of.write_text(str(of)))
        with pytest.raises(ValueError):
            ppg.JobGeneratingJob("a", lambda: None)

    def test_file_gen_job_running_here(self):
        tracker = []

        def a(of):
            of.write_text("a")
            tracker.append("a")

        ppg.FileGeneratingJob("a", a, resources=ppg.Resources.RunsHere)
        ppg.run()
        assert read("a") == "a"
        assert len(tracker) == 1

    def test_unlink_on_invalidation(self):
        Path("a").write_text("shu")

        def inner(of):
            assert not of.exists()
            of.write_text("a")

        a = ppg.FileGeneratingJob("a", inner)
        ppg.run()
        ppg.new()

        def inner(of):
            assert not of.exists()
            of.write_text("b")

        a = ppg.FileGeneratingJob("a", inner)
        ppg.run()

    def test_func_equals_none(self):
        a = ppg.FileGeneratingJob(
            "a", lambda of: of.write_text(str(of)), depend_on_function=False
        )
        a.depends_on(ppg.FunctionInvariant("myFunc", None))
        ppg.run()
        ppg.run()
        ppg.FunctionInvariant("myFunc", None)  # that one is ok, not a redef
        with pytest.raises(ppg.JobRedefinitionError):
            ppg.FunctionInvariant("myFunc", lambda: 5)
        with pytest.raises(ppg.JobRedefinitionError):
            ppg.FunctionInvariant("myFunc", open)
        ppg.FunctionInvariant("build", open)
        with pytest.raises(ppg.JobRedefinitionError):
            ppg.FunctionInvariant("build", lambda: 5)
        ll = lambda: 5  # noqa:E731
        ppg.FunctionInvariant("lamb", ll)
        ppg.FunctionInvariant("lamb", ll)
        with pytest.raises(ppg.JobRedefinitionError):
            ppg.FunctionInvariant("build", open)

    def test_funcinvariant_mixing_function_types_none(self):
        a = ppg.FileGeneratingJob(
            "A",
            lambda of: counter("a") and of.write_text(str(of)),
            depend_on_function=False,
        )
        a.depends_on(ppg.FunctionInvariant("myFunc", None))
        ppg.run()
        ppg.run()
        assert read("a") == "1"

        ppg.new()
        a = ppg.FileGeneratingJob(
            "A",
            lambda of: counter("a") and of.write_text(str(of)),
            depend_on_function=False,
        )
        a.depends_on(ppg.FunctionInvariant("myFunc", open))
        ppg.run()
        assert read("a") == "2"
        ppg.run()
        assert read("a") == "2"

        ppg.new()
        a = ppg.FileGeneratingJob(
            "A",
            lambda of: counter("a") and of.write_text(str(of)),
            depend_on_function=False,
        )
        a.depends_on(ppg.FunctionInvariant("myFunc", lambda: 55))
        ppg.run()
        ppg.run()
        assert read("a") == "3"

        ppg.new()
        a = ppg.FileGeneratingJob(
            "A",
            lambda of: counter("a") and of.write_text(str(of)),
            depend_on_function=False,
        )
        a.depends_on(ppg.FunctionInvariant("myFunc", open))
        ppg.run()
        assert read("a") == "4"
        ppg.run()
        assert read("a") == "4"

    def test_focus_on_these_jobs_and_generating(self, job_trace_log):
        """What happens when you focus() on a JobGeneratingJob?"""

        def inner():
            counter("b")
            ppg.FileGeneratingJob("A", lambda of: counter("a") and of.write_text("A"))

        c = ppg.FileGeneratingJob("c", lambda of: of.write_text("c"))
        ppg.JobGeneratingJob("B", inner)()  # here is the call
        assert read("b") == "1"
        assert read("a") == "1"
        assert read("A") == "A"
        assert not Path("c").exists()
        assert not hasattr(c, "prune_reason")
        ppg.run()
        assert read("b") == "2"
        assert read("a") == "1"
        assert read("c") == "c"

    def test_focus_on_multiple(self):
        a = ppg.FileGeneratingJob("a", lambda of: of.write_text(of.name))
        b = ppg.FileGeneratingJob("b", lambda of: of.write_text(of.name))
        c = ppg.FileGeneratingJob("c", lambda of: of.write_text(of.name))
        ppg.global_pipegraph.run_for_these([a, b])
        assert read("a") == "a"
        assert read("b") == "b"
        assert not Path("c").exists()

    def test_fail_but_write(self):
        def fail(of):
            of.write_text("A")
            raise ValueError()

        ppg.FileGeneratingJob("a", fail)
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert read("a") == "A"

    def test_failing_job_but_required_again_after_job_generating_job(
        self, job_trace_log
    ):
        def fail(of):
            counter("a")
            raise ValueError()

        a = ppg.FileGeneratingJob("A", fail)
        b = ppg.FileGeneratingJob(
            "B", lambda of: counter("b") and of.write_text(read("a"))
        )
        b.depends_on(a)

        def c():
            counter("c")
            d = ppg.FileGeneratingJob(
                "d", lambda of: counter("d") and of.write_text(read("a"))
            )
            d.depends_on(a)

        c = ppg.JobGeneratingJob("c", c)
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert read("a") == "1"
        assert read("c") == "1"
        assert not Path("b").exists()
        assert not Path("d").exists()

    def test_going_from_file_generating_to_file_invariant_no_retrigger(self):
        a = ppg.FileGeneratingJob("a", lambda of: of.write_text("a"))
        b = ppg.FileGeneratingJob(
            "b", lambda of: counter("B") and of.write_text("b" + read("a"))
        )
        b.depends_on(a)
        ppg.run()
        assert read("b") == "ba"
        assert read("B") == "1"
        ppg.new()
        a = ppg.FileInvariant("a")
        b = ppg.FileGeneratingJob(
            "b", lambda of: counter("B") and of.write_text("b" + read("a"))
        )
        b.depends_on(a)
        ppg.run()
        assert read("b") == "ba"
        assert read("B") == "1"
        ppg.new()
        a = ppg.FileGeneratingJob("a", lambda of: of.write_text("a"))
        b = ppg.FileGeneratingJob(
            "b", lambda of: counter("B") and of.write_text("b" + read("a"))
        )
        b.depends_on(a)
        ppg.run()
        assert read("b") == "ba"
        assert read("B") == "1"

    @pytest.mark.skip  # TODO: Renaming support?
    def test_going_from_multi_file_generating_to_file_invariant_no_retrigger(self):
        # this one depends on all files
        a = ppg.MultiFileGeneratingJob(
            ["a", "a1"], lambda of: of[0].write_text("a") and of[1].write_text("a1")
        )
        b = ppg.FileGeneratingJob(
            "b", lambda of: counter("B") and of.write_text("b" + read("a"))
        )
        b.depends_on(a)
        ppg.run()
        assert read("b") == "ba"
        assert read("B") == "1"

        ppg.new()
        b = ppg.FileGeneratingJob(
            "b", lambda of: counter("B") and of.write_text("b" + read("a"))
        )
        b.depends_on(ppg.FileInvariant("a"))
        b.depends_on(ppg.FileInvariant("a1"))
        ppg.run()
        assert read("b") == "ba"
        assert read("B") == "1"
        ppg.new()
        a = ppg.MultiFileGeneratingJob(
            ["a", "a1"], lambda of: of[0].write_text("a") and of[1].write_text("a1")
        )
        b = ppg.FileGeneratingJob(
            "b", lambda of: counter("B") and of.write_text("b" + read("a"))
        )
        b.depends_on(a)
        ppg.run()
        assert read("b") == "ba"
        assert read("B") == "1"

    def test_empty_depends_on_ok(self):
        a = ppg.FileGeneratingJob("shu", lambda of: of.write_text(of.name))
        a.depends_on()

    def test_job_name_must_not_contain_three_colons(self):
        with pytest.raises(ValueError):
            ppg.FileGeneratingJob("A:::b", lambda of: of.write_text("A"))
