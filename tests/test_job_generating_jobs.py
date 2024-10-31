from pathlib import Path
import pytest
import pypipegraph2 as ppg
from .shared import write, read, append, writeappend, Dummy, counter

shu = None


@pytest.mark.usefixtures("create_out_dir")
@pytest.mark.usefixtures("ppg2_per_test")
class TestJobGeneratingJob:
    def test_basic(self):
        def gen():
            ppg.FileGeneratingJob("out/A", lambda of: write("out/A", "A"))
            ppg.FileGeneratingJob("out/B", lambda of: write("out/B", "B"))
            ppg.FileGeneratingJob("out/C", lambda of: write("out/C", "C"))

        ppg.JobGeneratingJob("genjob", gen)
        ppg.run()
        assert read("out/A") == "A"
        assert read("out/B") == "B"
        assert read("out/C") == "C"

    def test_injecting_multiple_stages(self):
        def gen():
            def genB():
                def genC():
                    ppg.FileGeneratingJob("out/D", lambda of: write("out/D", "D"))

                ppg.JobGeneratingJob("C", genC)

            ppg.JobGeneratingJob("B", genB)

        ppg.JobGeneratingJob("A", gen)
        ppg.run()
        assert read("out/D") == "D"

    def test_generated_job_depending_on_each_other_one_of_them_is_Invariant(self):
        # basic idea. You have jobgen A,
        # it not only creates filegenB, but also ParameterDependencyC that A depends on
        # does that work
        def gen():
            jobB = ppg.FileGeneratingJob(
                "out/B", lambda of: write("out/B", "B"), depend_on_function=False
            )
            jobC = ppg.ParameterInvariant("C", ("ccc",))
            jobB.depends_on(jobC)

        ppg.JobGeneratingJob("A", gen)
        ppg.run()
        assert read("out/B") == "B"

        ppg.new()

        def gen2():
            jobB = ppg.FileGeneratingJob(
                "out/B", lambda of: write("out/B", "C"), depend_on_function=False
            )
            jobC = ppg.ParameterInvariant("C", ("ccc",))
            jobB.depends_on(jobC)

        ppg.JobGeneratingJob("A", gen2)
        ppg.run()
        assert read("out/B") == "B"  # no rerun

        ppg.new()

        def gen3():
            counter("3")
            jobB = ppg.FileGeneratingJob(
                "out/B", lambda of: write("out/B", "C"), depend_on_function=False
            )
            # jobB.ignore_code_changes()
            jobCX = ppg.ParameterInvariant("C", ("DDD",))
            jobB.depends_on(jobCX)

        ppg.JobGeneratingJob("A", gen3)
        ppg.run()
        assert read("out/B") == "C"  # did get rerun
        assert read("3") == "1"  # check that gen3 really ran...

    def test_generated_job_depending_on_job_that_cant_have_finished(self):
        # basic idea. You have jobgen A, and filegen B.
        # filegenB depends on jobgenA.
        # jobGenA created C depends on filegenB
        # Perhaps add a filegen D that's independand of jobGenA, but C also deps on D
        def a():
            jobB = ppg.FileGeneratingJob("out/B", lambda of: write("out/B", "B"))

            def genA():
                jobC = ppg.FileGeneratingJob("out/C", lambda of: write("out/C", "C"))
                jobC.depends_on(jobB)

            jobA = ppg.JobGeneratingJob("A", genA)
            jobB.depends_on(jobA)
            ppg.run()
            assert read("out/B") == "B"
            assert read("out/C") == "C"

        def b():
            jobB = ppg.FileGeneratingJob("out/B", lambda of: write("out/B", "B"))
            jobD = ppg.FileGeneratingJob("out/D", lambda of: write("out/D", "D"))

            def genA():
                jobC = ppg.FileGeneratingJob("out/C", lambda of: write("out/C", "C"))
                jobC.depends_on(jobB)
                jobC.depends_on(jobD)

            jobA = ppg.JobGeneratingJob("A", genA)
            jobB.depends_on(jobA)
            ppg.run()
            assert read("out/B") == "B"
            assert read("out/C") == "C"

        a()
        ppg.new()
        b()

    def test_generated_job_depending_on_each_other(self):
        # basic idea. You have jobgen A,
        # it not only creates filegenB, but also filegenC that depends on B
        # does that work
        def gen():
            jobB = ppg.FileGeneratingJob("out/B", lambda of: write("out/B", "B"))
            jobC = ppg.FileGeneratingJob(
                "out/C", lambda of: write("out/C", read("out/B"))
            )
            jobC.depends_on(jobB)

        ppg.JobGeneratingJob("A", gen)
        ppg.run()
        assert read("out/B") == "B"
        assert read("out/C") == "B"

    def test_generated_job_depending_on_each_other_one_of_them_is_loading(self):
        # basic idea. You have jobgen A,
        # it not only creates filegenB, but also DataloadingC that depends on B
        # does that work
        def gen():
            def load():
                global shu
                shu = "123"
                return ppg.UseInputHashesForOutput()

            def do_write(of):
                global shu
                write(of, shu)

            dl = ppg.DataLoadingJob("dl", load)
            jobB = ppg.FileGeneratingJob("out/A", do_write)
            jobB.depends_on(dl)

        ppg.JobGeneratingJob("gen", gen)
        ppg.run()
        assert read("out/A") == "123"

    def test_passing_non_function(self):
        with pytest.raises(TypeError):
            ppg.JobGeneratingJob("out/a", "shu")

    def test_passing_non_string_as_jobid(self):
        with pytest.raises(TypeError):
            ppg.JobGeneratingJob(5, lambda: 1)

    def test_generated_jobs_that_can_not_run_right_away_because_of_dataloading_do_not_crash(
        self,
    ):
        o = Dummy()
        existing_dl = ppg.AttributeLoadingJob("a", o, "a", lambda: "Ashu")

        def gen():
            new_dl = ppg.AttributeLoadingJob("b", o, "b", lambda: "Bshu")
            fg_a = ppg.FileGeneratingJob("out/C", lambda of: write("out/C", o.a))
            fg_b = ppg.FileGeneratingJob("out/D", lambda of: write("out/D", o.b))
            fg_a.depends_on(existing_dl)
            fg_b.depends_on(new_dl)

        ppg.JobGeneratingJob("E", gen)
        ppg.run()
        assert read("out/C") == "Ashu"
        assert read("out/D") == "Bshu"

    def test_filegen_invalidated_jobgen_created_filegen_later_also_invalidated(self):
        a = ppg.FileGeneratingJob(
            "out/A", lambda of: writeappend("out/A", "out/Ac", "A")
        )
        p = ppg.ParameterInvariant("p", "p")
        a.depends_on(p)

        def gen():
            append("out/g", "g")
            c = ppg.FileGeneratingJob(
                "out/C", lambda of: writeappend("out/C", "out/Cx", "C")
            )
            c.depends_on(a)

        # difference to ppg1 - gen jobs will not rerun if their inputs did not change!
        ppg.JobGeneratingJob("b", gen).depends_on(a)
        ppg.run()
        assert read("out/g") == "g"
        assert read("out/A") == "A"
        assert read("out/Ac") == "A"
        assert read("out/C") == "C"
        assert read("out/Cx") == "C"

        ppg.new()
        a = ppg.FileGeneratingJob(
            "out/A", lambda of: writeappend("out/A", "out/Ac", "A")
        )
        p = ppg.ParameterInvariant("p", "p2")
        a.depends_on(p)
        ppg.JobGeneratingJob("b", gen).depends_on(a)
        ppg.run()
        assert read("out/g") == "gg"
        assert read("out/Ac") == "AA"
        assert (
            read("out/Cx") == "C"
        )  # this is a difference to the pipegraph. depending on A
        # does not retrigger just because a's upstream changed.
        # only if a truly changed

        ppg.new()
        a = ppg.FileGeneratingJob(
            "out/A", lambda of: writeappend("out/A", "out/Ac", "B")
        )
        p = ppg.ParameterInvariant("p", "p3")
        a.depends_on(p)
        ppg.JobGeneratingJob("b", gen).depends_on(a)
        ppg.run()
        assert read("out/g") == "ggg"
        assert read("out/Ac") == "AAB"
        assert read("out/Cx") == "CC"

    def test_creating_within_dataload(self):
        """This used to be forbidden in ppg1.
        I don't see a reason to forbid it now, the only
        substantial difference is that DataLoadingJobs run when their downstream needs them,
        and JobGeneratingJobs always run"""
        write_job = ppg.FileGeneratingJob("out/A", lambda of: write("out/A", "aa"))

        def load():
            ppg.FileGeneratingJob("out/B", lambda of: write("out/B", "aa"))
            return ppg.UseInputHashesForOutput()

        dl = ppg.DataLoadingJob("load_data", load)
        write_job.depends_on(dl)
        # with pytest.raises(ppg.JobsFailed):
        ppg.run()
        assert Path("out/B").exists()

    def test_ignored_if_generating_within_filegenerating(self):
        write_job = ppg.FileGeneratingJob("out/A", lambda of: write("out/A", "aa"))

        def load(_of):
            ppg.FileGeneratingJob("out/B", lambda of: write("out/B", "aa"))
            write("out/C", "c")

        dl = ppg.FileGeneratingJob("out/C", load)
        write_job.depends_on(dl)
        ppg.run()
        assert read("out/C") == "c"
        assert not Path("out/B").exists()

    def test_invalidation(self):
        def gen():
            ppg.FileGeneratingJob("out/D", lambda of: write("out/D", "D"))

        ppg.JobGeneratingJob("A", gen)
        ppg.run()
        assert read("out/D") == "D"
        ppg.new()

        def gen():
            ppg.FileGeneratingJob("out/D", lambda of: write("out/D", "E"))

        ppg.JobGeneratingJob("A", gen)
        ppg.run()
        assert read("out/D") == "E"

    def test_invalidation_multiple_stages(self):
        counter = [0]

        def count():
            counter[0] += 1
            return str(counter[0])

        def gen():
            def genB():
                def genC():
                    count()
                    ppg.FileGeneratingJob("out/D", lambda of: write(of, "D"))

                ppg.JobGeneratingJob("C", genC)

            ppg.JobGeneratingJob("B", genB)

        ppg.JobGeneratingJob("A", gen)
        ppg.run()
        assert read("out/D") == "D"
        assert counter[0] == 1

        ppg.new()
        ppg.JobGeneratingJob("A", gen)
        ppg.run()
        assert read("out/D") == "D"
        assert counter[0] == 2

        ppg.new()

        def gen():
            def genB():
                def genC():
                    count()
                    ppg.FileGeneratingJob("out/D", lambda of: write("out/D", "E"))

                ppg.JobGeneratingJob("C", genC)

            ppg.JobGeneratingJob("B", genB)

        ppg.JobGeneratingJob("A", gen)
        ppg.run()
        assert read("out/D") == "E"
        assert counter[0] == 3

    # this test only works if you'd remove the locking around next_job_number
    # *and* add in a small delay to actually trigger it
    # def test_massive_trying_to_hit_jobnumber_conflict(self):
    #     ppg.new(cores=4)
    #     def fail(of):
    #         raise ValueError()
    #     def jj(prefix):
    #         def inner():
    #             Path(prefix).mkdir(exist_ok=True)
    #             for i in range(0,100):
    #                 ppg.FileGeneratingJob(prefix + '/' + str(i), lambda of: of.write_text(prefix))
    #         return inner
    #     a = ppg.JobGeneratingJob('a', jj('a'))
    #     b = ppg.JobGeneratingJob('b', jj('b'))
    #     c = ppg.JobGeneratingJob('c', jj('c'))
    #     d = ppg.JobGeneratingJob('d', jj('d'))
    #     ppg.run()

    def test_job_gen_leading_to_missing_history(self):
        """test_job_gen_leading_to_missing_history

        A case where our previous 'trash history when MFG is renamed'
        functionality lead to throwing away of history.
        """

        # I think I need a job that's new, so the MFG get's pulled back in
        # and I need a job that's was already done in the first round and is pruned
        # so that the 2nd's history is first removed by the 'rename paranoia code'
        # and then not restored, because it did not run.
        # also a regular mFG should trigger in.

        def do_a(ofs, prefix=None):
            for of in ofs:
                of.write_text("a")

        def do_f(of):
            of.write_text("f")

        a = ppg.MultiFileGeneratingJob(["a", "a1"], do_a)
        b = ppg.FileGeneratingJob("b", do_f)
        b.depends_on(a)

        def gen():
            c = ppg.FileGeneratingJob("c", lambda of: of.write_text("c"))
            c.depends_on(a)  # pull it back in.

        ppg.JobGeneratingJob("gen", gen)

        ppg.run()

        history = ppg.global_pipegraph._load_history()
        import pprint

        pprint.pprint(sorted(history.keys()))
        key_of_interest = a.job_id + "!!!" + "b"  # that's the one that goes missing
        assert key_of_interest in history
        key_of_interest = a.job_id + "!!!" + "c"
        assert key_of_interest in history

    def test_job_gen_leading_to_missing_history_part2(self):
        """test_job_gen_leading_to_missing_history_part2(

        A case where our previous 'trash history when MFG is renamed'
        functionality lead to throwing away of history.

        This does the same, but verifies we keep the history when the gen job is not using the MFG
        """

        # similar

        def do_a(ofs, prefix=None):
            for of in ofs:
                of.write_text("a")

        def do_f(of):
            of.write_text("f")

        a = ppg.MultiFileGeneratingJob(["a", "a1"], do_a)
        b = ppg.FileGeneratingJob("b", do_f)
        b.depends_on(a)

        def gen():
            c = ppg.FileGeneratingJob("c", lambda of: of.write_text("c"))

        ppg.JobGeneratingJob("gen", gen)

        ppg.run()

        history = ppg.global_pipegraph._load_history()
        import pprint

        pprint.pprint(sorted(history.keys()))
        key_of_interest = a.job_id + "!!!" + "b"  # that's the one that goes missing
        assert key_of_interest in history
