import pytest
from pathlib import Path
import pypipegraph2 as ppg

from .shared import read, write, append, Dummy




@pytest.mark.usefixtures("ppg_per_test")
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

    def test_invalidation_redoes_output(self, ppg_per_test):
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

        ppg_per_test.new()

        def calc2():
            return ", ".join(str(x) for x in range(0, 200))

        job, cache_job = ppg.CachedAttributeLoadingJob(
            "mycalc", o, "a", calc2
        )  # now, jobB should be deleted...
        ppg.FileGeneratingJob(of, do_write).depends_on(job)
        ppg.run()
        assert read(of) == ", ".join(str(x) for x in range(0, 200))

    def test_invalidation_ignored_does_not_redo_output(self, ppg_per_test):
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

        ppg_per_test.new()

        def calc2():
            return ", ".join(str(x) for x in range(0, 200))

        job, cache_job = ppg.CachedAttributeLoadingJob(
            "mycalc", o, "a", calc2, depend_on_function=False
        )
        ppg.FileGeneratingJob(of, do_write).depends_on(job)
        ppg.run()
        assert read(of) == ", ".join(str(x) for x in range(0, 100))

        ppg_per_test.new()
        job, cache_job = ppg.CachedAttributeLoadingJob("mycalc", o, "a", calc2)
        ppg.FileGeneratingJob(of, do_write).depends_on(job)
        ppg.run()
        assert read(of) == ", ".join(
            str(x) for x in range(0, 200)
        )  # The new stuff - you either have an explicit ignore_code_changes in our codebase, or we enforce consistency between code and result

    def test_throws_on_non_function_func(self):
        o = Dummy()

        with pytest.raises(ValueError):
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
        jobB = ppg.FileGeneratingJob("B", lambda: write("out/B", "shu"))
        assert jobA.depends_on(jobB) is jobA

    def test_passing_non_function(self):
        o = Dummy()

        def inner():
            ppg.CachedAttributeLoadingJob("a", o, "a", 55)

        with pytest.raises(ValueError):
            inner()

    def test_passing_non_string_as_jobid(self):
        o = Dummy()

        def inner():
            ppg.CachedAttributeLoadingJob(5, o, "a", lambda: 55)

        with pytest.raises(TypeError):
            inner()

    @pytest.mark.xfail
    def test_no_swapping_attributes_for_one_job(self):
        def cache():
            return list(range(0, 100))

        o = Dummy()
        ppg.CachedAttributeLoadingJob("A", o, "a", cache)

        def inner():
            ppg.CachedAttributeLoadingJob("A", o, "b", cache)

        with pytest.raises(ppg.JobContractError):
            inner()

    @pytest.mark.xfail
    def test_no_swapping_objects_for_one_job(self):
        def cache():
            return list(range(0, 100))

        o = Dummy()
        o2 = Dummy()
        ppg.CachedAttributeLoadingJob("A", o, "a", cache)

        def inner():
            ppg.CachedAttributeLoadingJob("A", o2, "a", cache)

        with pytest.raises(ppg.JobContractError):
            inner()

    def test_cached_attribute_job_does_not_load_its_preqs_on_cached(self, ppg_per_test):
        o = Dummy()

        def a():
            o.a = "A"
            append("A", "A")

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
        ppg_per_test.new()

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
        def inner():
            o = Dummy()
            ppg.CachedAttributeLoadingJob(55, o, "c", lambda: 55)

        with pytest.raises(TypeError):
            inner()

    def test_raises_on_non_string_attribute(self):
        def inner():
            o = Dummy()
            ppg.CachedAttributeLoadingJob("C", o, 354, lambda: 55)

        with pytest.raises(ValueError):
            inner()

    def test_callback_must_be_callable(self):
        def inner():
            o = Dummy()
            ppg.CachedAttributeLoadingJob("x", o, "a", "shu")

        with pytest.raises(ValueError):
            inner()

    def test_name_must_be_str(self):
        def inner():
            o = Dummy()
            ppg.CachedAttributeLoadingJob(123, o, "a", lambda: 123)

        with pytest.raises(TypeError):
            inner()
