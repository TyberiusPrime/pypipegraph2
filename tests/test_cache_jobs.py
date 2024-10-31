import pytest
import pickle
from pathlib import Path
import pypipegraph2 as ppg

from .shared import read, write, append, Dummy, counter


@pytest.mark.usefixtures("ppg2_per_test")
class TestCachedDataLoadingJob:
    def test_simple(self):
        o = Dummy()

        def calc():
            return ", ".join(str(x) for x in range(0, 100))

        def store(value):
            o.a = value

        job, cache_job = ppg.CachedDataLoadingJob("out/mycalc", calc, store)
        of = "out/A"

        def do_write(of):
            write(of, o.a)

        ppg.FileGeneratingJob(of, do_write).depends_on(job)
        ppg.run()
        assert read(of) == ", ".join(str(x) for x in range(0, 100))

    def test_no_downstream_still_calc(self):
        o = Dummy()

        def calc():
            return ", ".join(str(x) for x in range(0, 100))

        def store(value):
            o.a = value

        ppg.CachedDataLoadingJob("out/mycalc", calc, store)
        # job.ignore_code_changes() #or it would run anyway... hm.
        assert not (Path("out/mycalc").exists())
        ppg.run()
        assert Path("out/mycalc").exists()

    def test_passing_non_function_to_calc(self):
        with pytest.raises(TypeError):
            ppg.CachedDataLoadingJob("out/a", "shu", lambda value: 55)

    def test_passing_non_function_to_store(self):
        with pytest.raises(TypeError):
            ppg.CachedDataLoadingJob("out/a", lambda value: 55, "shu")

    def test_passing_non_string_as_jobid(self):
        with pytest.raises(TypeError):
            ppg.CachedDataLoadingJob(5, lambda: 1, lambda value: 55)

    def test_being_generated(self):
        o = Dummy()

        def calc():
            return 55

        def store(value):
            o.a = value

        def dump(of):
            write("out/c", "c")
            write("out/A", str(o.a))

        def gen():
            load_job, cache_job = ppg.CachedDataLoadingJob("out/B", calc, store)
            dump_job = ppg.FileGeneratingJob("out/A", dump)
            dump_job.depends_on(load_job)

        ppg.JobGeneratingJob("out/C", gen)
        ppg.run()
        assert read("out/A") == "55"
        assert read("out/c") == "c"

    def test_being_generated_nested(self):
        o = Dummy()

        def calc():
            return 55

        def store(value):
            o.a = value

        def dump(of):
            write(of, str(o.a))

        def gen():
            write("out/c", "c")
            calc_job, cache_job = ppg.CachedDataLoadingJob("out/B", calc, store)

            def gen2():
                write("out/d", "d")
                dump_job = ppg.FileGeneratingJob("out/A", dump)
                dump_job.depends_on(calc_job)

            ppg.JobGeneratingJob("out/D", gen2)

        ppg.JobGeneratingJob("out/C", gen)
        ppg.run()
        assert read("out/c") == "c"
        assert read("out/d") == "d"
        assert read("out/A") == "55"

    def test_cached_dataloading_job_does_not_load_its_preqs_on_cached(self):
        o = Dummy()

        def a():
            o.a = "A"
            append("out/A", "A")
            return ppg.UseInputHashesForOutput()

        def calc():
            append("out/B", "B")
            return o.a * 2

        def load(value):
            o.c = value
            append("out/Cx", "C")  # not C, that's the cached file, you know...

        def output(of):
            write(of, o.c)

        dl = ppg.DataLoadingJob("out/A", a)
        ca, cca = ppg.CachedDataLoadingJob("out/C", calc, load)
        fg = ppg.FileGeneratingJob("out/D", output)
        fg.depends_on(ca)
        cca.depends_on(dl)
        ppg.run()
        assert read("out/D") == "AA"  # we did write the final result
        assert read("out/A") == "A"  # ran the dl job
        assert read("out/B") == "B"  # ran the calc job...
        assert read("out/Cx") == "C"  # ran the load jobo
        Path(
            "out/D"
        ).unlink()  # so the filegen and the loadjob of cached should rerun...
        ppg.new()
        dl = ppg.DataLoadingJob("out/A", a)
        ca, cca = ppg.CachedDataLoadingJob("out/C", calc, load)
        fg = ppg.FileGeneratingJob("out/D", output)
        fg.depends_on(ca)
        cca.depends_on(dl)
        ppg.run()
        assert read("out/D") == "AA"  # we did write the final result
        assert read("out/A") == "A"  # did not run the dl job
        assert read("out/B") == "B"  # did not run the calc job again
        assert read("out/Cx") == "CC"  # did run the load job again

    def test_name_must_be_str(self):
        with pytest.raises(TypeError):
            ppg.CachedDataLoadingJob(123, lambda: 123, lambda: 5)
        with pytest.raises(TypeError):
            ppg.CachedDataLoadingJob("123", 123, lambda: 5)
        with pytest.raises(TypeError):
            ppg.CachedDataLoadingJob("123", lambda: 5, 123)
        with pytest.raises(TypeError):
            ppg.CachedDataLoadingJob(Path("123"), lambda: 5, 123)

    def test_cant_unpickle(self):
        o = Dummy()

        def calc():
            return ", ".join(str(x) for x in range(0, 100))

        def store(value):
            o.a = value

        job, cache_job = ppg.CachedDataLoadingJob(
            "out/mycalc", calc, store, depend_on_function=False
        )
        write("out/mycalc", "no unpickling this")
        of = "out/A"

        def do_write(of):
            write(of, o.a)

        ppg.FileGeneratingJob(of, do_write).depends_on(job)
        ppg.run()  # this does not raise. The file must be generated by the graph!
        # overwriting it again
        write("out/mycalc", "no unpickling this")
        Path(of).unlink()
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        error = ppg.global_pipegraph.last_run_result[job.job_id].error
        assert isinstance(error, ppg.JobError)
        assert isinstance(error.args[0], pickle.UnpicklingError)
        assert "out/mycalc" in str(error)

    def test_use_cores(self):
        ca, cca = ppg.CachedDataLoadingJob("out/C", lambda: 55, lambda x: None)
        assert cca.resources is ppg.Resources.SingleCore
        assert cca.use_resources(ppg.Resources.AllCores) is cca
        assert cca.resources is ppg.Resources.AllCores
        assert ca.resources is ppg.Resources.SingleCore
        assert ca.use_resources(ppg.Resources.AllCores) is ca
        assert ca.resources is ppg.Resources.AllCores


@pytest.mark.usefixtures("ppg2_per_test")
class TestCachedAttributeJob:
    def test_simple(self):
        o = Dummy()

        def calc():
            return ", ".join(str(x) for x in range(0, 100))

        job, _cache_job = ppg.CachedAttributeLoadingJob("mycalc", o, "a", calc)
        of = "A"

        def do_write(output_filename):
            write(output_filename, o.a)

        ppg.FileGeneratingJob(of, do_write).depends_on(job)
        ppg.run()
        assert read(of) == ", ".join(str(x) for x in range(0, 100))

    def test_no_downstream_still_calc(self):
        o = Dummy()

        def calc():
            return ", ".join(str(x) for x in range(0, 100))

        ppg.CachedAttributeLoadingJob("mycalc", o, "a", calc)
        assert not (Path("mycalc").exists())
        ppg.run()
        assert Path("mycalc").exists()

    def test_invalidation_redoes_output(self):
        o = Dummy()

        def calc():
            return ", ".join(str(x) for x in range(0, 100))

        job, cache_job = ppg.CachedAttributeLoadingJob("mycalc", o, "a", calc)
        of = "A"

        def do_write(of):
            write(of, o.a)

        ppg.FileGeneratingJob(of, do_write).depends_on(job)
        ppg.run()
        assert read(of) == ", ".join(str(x) for x in range(0, 100))

        ppg.new()

        def calc2():
            return ", ".join(str(x) for x in range(0, 200))

        job, cache_job = ppg.CachedAttributeLoadingJob(
            "mycalc", o, "a", calc2
        )  # now, jobB should be deleted...
        ppg.FileGeneratingJob(of, do_write).depends_on(job)
        ppg.run()
        assert read(of) == ", ".join(str(x) for x in range(0, 200))

    def test_invalidation_ignored_does_not_redo_output(self):
        # actually, this test has evolved away from it's original behaviour.
        # adding/remoning a function dependency will always trigger!
        o = Dummy()

        def calc():
            counter("1")
            return ", ".join(str(x) for x in range(0, 100))

        job, cache_job = ppg.CachedAttributeLoadingJob("mycalc", o, "a", calc)
        of = "A"

        def do_write(of):
            write(of, o.a)

        ppg.FileGeneratingJob(of, do_write).depends_on(job)
        ppg.run()
        assert read(of) == ", ".join(str(x) for x in range(0, 100))
        assert read("1") == "1"

        ppg.new()

        def calc2():
            counter("2")
            return ", ".join(str(x) for x in range(0, 200))

        job, cache_job = ppg.CachedAttributeLoadingJob(
            "mycalc", o, "a", calc2, depend_on_function=False
        )
        ppg.FileGeneratingJob(of, do_write).depends_on(job)
        ppg.run()
        assert read(of) == ", ".join(
            str(x) for x in range(0, 200)
        )  # removing the dependency triggers
        assert read("2") == "1"

        ppg.new()
        job, cache_job = ppg.CachedAttributeLoadingJob("mycalc", o, "a", calc2)
        ppg.FileGeneratingJob(of, do_write).depends_on(job)
        ppg.run()
        assert read(of) == ", ".join(
            str(x) for x in range(0, 200)
        )  # The new stuff - you either have an explicit ignore_code_changes in our codebase, or we enforce consistency between code and result
        assert read("2") == "2"  # rerun, we regained the func dependency

        ppg.run()
        assert read(of) == ", ".join(
            str(x) for x in range(0, 200)
        )  # The new stuff - you either have an explicit ignore_code_changes in our codebase, or we enforce consistency between code and result
        assert read("2") == "2"  # no rerun

    def test_throws_on_non_function_func(self):
        o = Dummy()

        with pytest.raises(TypeError):
            ppg.CachedAttributeLoadingJob(
                "mycalc", lambda: 5, o, "a"
            )  # wrong argument order

    def test_calc_depends_on_added_dependencies(self):
        o = Dummy()
        load_attr = ppg.AttributeLoadingJob("load_attr", o, "o", lambda: 55)

        def calc():
            return o.o

        def out(output_filename):
            write(output_filename, str(o.o2))

        lj, cj = ppg.CachedAttributeLoadingJob("cached_job", o, "o2", calc)
        fg = ppg.FileGeneratingJob("A", out)
        fg.depends_on(lj)
        cj.depends_on(load_attr)
        ppg.run()
        assert read("A") == "55"

    def test_depends_on_returns_self(self):
        o = Dummy()
        jobA, cache_job = ppg.CachedAttributeLoadingJob(
            "A", o, "shu", lambda: write("out/A", "shu")
        )
        jobB = ppg.FileGeneratingJob("B", lambda of: write("out/B", "shu"))
        assert jobA.depends_on(jobB) is jobA

    def test_passing_non_function(self):
        o = Dummy()

        with pytest.raises(TypeError):
            ppg.CachedAttributeLoadingJob("a", o, "a", 55)

    def test_passing_non_string_as_jobid(self):
        o = Dummy()

        with pytest.raises(TypeError):
            ppg.CachedAttributeLoadingJob(5, o, "a", lambda: 55)

    def test_no_swapping_attributes_for_one_job(self):
        def cache():
            return list(range(0, 100))

        o = Dummy()
        ppg.CachedAttributeLoadingJob("A", o, "a", cache)

        with pytest.raises(ppg.JobRedefinitionError):
            ppg.CachedAttributeLoadingJob("A", o, "b", cache)
        ppg.new(run_mode=ppg.RunMode.NOTEBOOK)
        ppg.CachedAttributeLoadingJob("A", o, "a", cache)
        ppg.CachedAttributeLoadingJob("A", o, "b", cache)

    def test_no_swapping_objects_for_one_job(self):
        def cache():
            return list(range(0, 100))

        o = Dummy()
        o2 = Dummy()
        ppg.CachedAttributeLoadingJob("A", o, "a", cache)
        with pytest.raises(ppg.JobRedefinitionError):
            ppg.CachedAttributeLoadingJob("A", o2, "a", cache)
        ppg.new(run_mode=ppg.RunMode.NOTEBOOK)
        ppg.CachedAttributeLoadingJob("A", o, "a", cache)
        ppg.CachedAttributeLoadingJob("A", o2, "a", cache)

    def test_cached_attribute_job_does_not_load_its_preqs_on_cached(self):
        o = Dummy()

        def a():
            o.a = "A"
            append("A", "A")
            return ppg.UseInputHashesForOutput()

        def calc():
            append("B", "B")
            return o.a * 2

        def output(output_filename):
            write("D", o.c)

        dl = ppg.DataLoadingJob("A", a)
        ca, cache_job = ppg.CachedAttributeLoadingJob("C", o, "c", calc)
        fg = ppg.FileGeneratingJob("D", output)
        fg.depends_on(ca)
        cache_job.depends_on(dl)
        ppg.run()
        assert read("D") == "AA"  # we did write the final result
        assert read("A") == "A"  # ran the dl job
        assert read("B") == "B"  # ran the calc job...
        Path("D").unlink()  # so the filegen and the loadjob of cached should rerun...
        ppg.new()

        dl = ppg.DataLoadingJob("A", a)
        ca, cache_job = ppg.CachedAttributeLoadingJob("C", o, "c", calc)
        fg = ppg.FileGeneratingJob("D", output)
        fg.depends_on(ca)
        cache_job.depends_on(dl)
        ppg.run()
        assert read("D") == "AA"  # we did write the final result
        assert read("B") == "B"  # did not run the calc job again
        assert read("A") == "A"  # did not run the dl job

    def test_raises_on_non_string_filename(self):
        o = Dummy()
        with pytest.raises(TypeError):
            ppg.CachedAttributeLoadingJob(55, o, "c", lambda: 55)

    def test_raises_on_non_string_attribute(self):
        o = Dummy()
        with pytest.raises(ValueError):
            ppg.CachedAttributeLoadingJob("C", o, 354, lambda: 55)

    def test_callback_must_be_callable(self):
        o = Dummy()
        with pytest.raises(TypeError):
            ppg.CachedAttributeLoadingJob("x", o, "a", "shu")

    def test_name_must_be_str(self):
        o = Dummy()
        with pytest.raises(TypeError):
            ppg.CachedAttributeLoadingJob(123, o, "a", lambda: 123)
