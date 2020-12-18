import os
from pathlib import Path
from loguru import logger
import stat
import time
import hashlib
import shutil
import pytest
import pypipegraph2 as ppg
from .shared import write, read, append, Dummy, counter


class Undepickable(object):
    def __getstate__(self):
        return {"shu": 123}  # must not return falsey value

    def __setstate__(self, state):
        self.sha = state["shu"]
        import pickle

        raise pickle.UnpicklingError("SHU")


@pytest.mark.usefixtures("create_out_dir")
@pytest.mark.usefixtures("ppg_per_test")
class TestInvariant:
    def sentinel_count(self):
        sentinel = "out/sentinel"
        try:
            op = open(sentinel, "r")
            count = int(op.read())
            op.close()
        except IOError:
            count = 1
        op = open(sentinel, "w")
        op.write("%i" % (count + 1))
        op.close()
        return count

    def test_filegen_jobs_detect_code_change(self):
        of = "out/a"

        def do_write(of):
            append(of, "shu" * self.sentinel_count())

        ppg.FileGeneratingJob(of, do_write)
        ppg.run()
        assert read(of) == "shu"
        ppg.new()
        ppg.FileGeneratingJob(of, do_write)
        ppg.run()
        assert read(of) == "shu"  # has not been run again...

        def do_write2(of):
            append(of, "sha")

        ppg.new()
        ppg.FileGeneratingJob(of, do_write2)
        ppg.run()
        assert read(of) == "sha"  # has been run again ;).

    def test_filegen_jobs_ignores_code_change(self):
        of = "out/a"

        def do_write(of):
            append(of, "shu" * self.sentinel_count())

        job = ppg.FileGeneratingJob(of, do_write)
        ppg.run()

        assert read(of) == "shu"
        ppg.new()
        job = ppg.FileGeneratingJob(of, do_write)
        ppg.run()
        assert read(of) == "shu"  # has not been run again, for no change

        ppg.new()

        def do_write2(of):
            append(of, "sha")

        job = ppg.FileGeneratingJob(of, do_write2, depend_on_function=False)
        ppg.run()
        assert read(of) == "shu"  # has not been run again, since we ignored the changes

        ppg.new()
        job = ppg.FileGeneratingJob(of, do_write2)
        ppg.run()
        assert (
            read(of) == "sha"
        )  # But the new code had not been stored, not ignoring => redoing.

    def test_parameter_dependency(self):
        of = "out/a"

        def do_write(of):
            append(of, "shu" * self.sentinel_count())

        job = ppg.FileGeneratingJob(of, do_write)
        param_dep = ppg.ParameterInvariant("myparam", (1, 2, 3))
        job.depends_on(param_dep)
        ppg.run()
        assert read(of) == "shu"
        ppg.new()
        job = ppg.FileGeneratingJob(of, do_write)
        param_dep = ppg.ParameterInvariant("myparam", (1, 2, 3))
        job.depends_on(param_dep)
        ppg.run()
        assert read(of) == "shu"  # has not been run again...
        ppg.new()
        job = ppg.FileGeneratingJob(of, do_write)
        param_dep = ppg.ParameterInvariant("myparam", (1, 2, 3, 4))
        job.depends_on(param_dep)
        ppg.run()
        assert read(of) == "shushu"  # has been run again ;).

    def test_parameter_invariant_adds_hidden_job_id_prefix(self):
        param = "A"
        jobA = ppg.FileGeneratingJob("out/A", lambda of: write("out/A", param))
        jobB = ppg.ParameterInvariant("out/A", param)
        jobA.depends_on(jobB)
        ppg.run()
        assert read("out/A") == param

    def test_depends_on_func(self):
        a = ppg.FileGeneratingJob("out/A", lambda of: write("a"))
        b, a_again = a.depends_on_func("a123", lambda: 123)
        assert b.job_id.startswith("FunctionInvariant:" + a.job_id + "_")
        assert ppg.global_pipegraph.has_edge(b, a)
        assert a_again is a

    def test_depends_on_file(self):
        a = ppg.FileGeneratingJob("out/A", lambda of: write("a"))
        write("shu", "hello")
        b = a.depends_on_file("shu")
        assert b.self is a
        assert ppg.global_pipegraph.has_edge(b.invariant, a)

    def test_depends_on_params(self):
        a = ppg.FileGeneratingJob("out/A", lambda of: write("a"))
        b = a.depends_on_params(23)
        assert b.invariant.job_id == "PIout/A"
        assert b.invariant.parameters == 23
        assert ppg.global_pipegraph.has_edge(b.invariant, a)
        assert b.self is a

    @pytest.mark.xfail()  # todo: interactive
    def test_parameter_invariant_twice_different_values(self):
        ppg.ParameterInvariant("a", (1, 2, 3))
        with pytest.raises(ValueError):
            ppg.ParameterInvariant("a", (1, 2, 4))

    def test_filetime_dependency(self):
        of = "out/a"

        def do_write(of):
            append(of, "shu" * self.sentinel_count())

        ftfn = "out/ftdep"
        write(ftfn, "hello")
        write(of, "hello")
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileInvariant(ftfn)
        job.depends_on(dep)
        ppg.run()
        assert (
            read(of) == "shu"
        )  # job get's run though there is a file, because the FileInvariant was not stored before...
        ppg.new()
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileInvariant(ftfn)
        job.depends_on(dep)
        ppg.run()
        assert read(of) == "shu"  # job does not get rerun...

        time.sleep(1)  # so linux actually advances the file time in the next line
        write(ftfn, "hello")  # same content, different time

        ppg.new()
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileInvariant(ftfn)
        job.depends_on(dep)
        ppg.run()
        assert (
            read(of) == "shu"
        )  # job does not get rerun - filetime invariant is now filechecksum invariant...

    def test_filechecksum_dependency_raises_on_too_short_a_filename(self):
        ppg.global_pipegraph.allow_short_filenames = False
        with pytest.raises(ValueError):
            ppg.FileInvariant("a")

        with pytest.raises(ValueError):
            ppg.FileInvariant("sh")
        ppg.FileInvariant("shu")

    def test_filechecksum_dependency(self):
        of = "out/a"

        def do_write(of):
            append(of, "shu" * self.sentinel_count())

        ftfn = "out/ftdep"
        write(ftfn, "hello")
        # import stat
        # logging.info('file time after creating %s'% os.stat(ftfn)[stat.ST_MTIME])

        write(of, "hello")

        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileInvariant(ftfn)
        job.depends_on(dep)
        ppg.run()
        assert (
            read(of) == "shu"
        )  # job get's run though there is a file, because the FileInvariant was not stored before...
        ppg.new()
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileInvariant(ftfn)
        job.depends_on(dep)
        ppg.run()
        assert read(of) == "shu"  # job does not get rerun...

        time.sleep(1)  # so linux actually advances the file time in the next line
        # logging.info("NOW REWRITE")
        write(ftfn, "hello")  # same content, different time

        ppg.new()
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileInvariant(ftfn)
        job.depends_on(dep)
        ppg.run()
        assert read(of) == "shu"  # job does not get rerun...

        # time.sleep(1) #we don't care about the time, size should be enough...
        write(ftfn, "hello world!!")  # different time
        time.sleep(1)  # give the file system a second to realize the change.

        ppg.new()
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileInvariant(ftfn)
        job.depends_on(dep)
        ppg.run()
        assert read(of) == "shushu"  # job does get rerun

    @pytest.mark.xfail()  # todo: job input renaming
    def test_robust_filechecksum_invariant(self):
        of = "out/B"

        def do_write(of):
            append(of, "shu" * self.sentinel_count())

        ftfn = "out/ftdep"
        write(ftfn, "hello")
        # import stat
        # logging.info('file time after creating %s'% os.stat(ftfn)[stat.ST_MTIME])

        write(of, "hello")

        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileInvariant(ftfn)
        job.depends_on(dep)
        ppg.run()
        assert (
            read(of) == "shu"
        )  # job get's run though there is a file, because the FileInvariant was not stored before...

        ppg.new()
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileInvariant(ftfn)
        job.depends_on(dep)
        ppg.run()
        assert read(of) == "shu"  # job does not get rerun...

        os.mkdir("out/moved_here")
        shutil.move(ftfn, os.path.join("out/moved_here", "ftdep"))
        ppg.new()
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileInvariant(os.path.join("out/moved_here", "ftdep"))
        job.depends_on(dep)
        assert read(of) == "shu"  # job does not get rerun...
        ppg.run()
        assert read(of) == "shu"  # job does not get rerun...

    @pytest.mark.skip()  # I have no idea why this was useful. Possibly the PrebuildJobs support?
    def test_file_invariant_with_md5sum(self):
        of = "out/a"

        def do_write(of):
            append(of, "shu" * self.sentinel_count())

        ftfn = "out/ftdep"
        write(ftfn, "hello")
        # import stat
        # logging.info('file time after creating %s'% os.stat(ftfn)[stat.ST_MTIME])

        write(of, "hello")

        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileInvariant(ftfn)
        job.depends_on(dep)
        ppg.run()
        assert (
            read(of) == "shu"
        )  # job get's run though there is a file, because the FileInvariant was not stored before...

        with open(ftfn + ".md5sum", "wb") as op:
            op.write(hashlib.md5(b"hello world").hexdigest().encode("utf-8"))
        write(ftfn, "hello world")  # different content
        t = time.time()
        # now make
        os.utime(ftfn, (t, t))
        os.utime(ftfn + ".md5sum", (t, t))
        time.sleep(1)  # give the file system a second to realize the change.

        ppg.new()
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileInvariant(ftfn)
        job.depends_on(dep)
        ppg.run()
        assert (
            read(of) == "shushu"
        )  # job get's run though there is a file, because the md5sum changed.

        with open(ftfn + ".md5sum", "wb") as op:
            op.write(hashlib.md5(b"hello world").hexdigest().encode("utf-8"))
        write(ftfn, "hello")  # different content, but the md5sum is stil the same!
        t = time.time()
        # now make
        os.utime(ftfn, (t, t))
        os.utime(ftfn + ".md5sum", (t, t))
        time.sleep(1)  # give the file system a second to realize the change.

        ppg.new()
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileInvariant(ftfn)
        job.depends_on(dep)
        ppg.run()
        assert read(of) == "shushu"  # job does not get rerun, md5sum did not change...

        t = time.time() - 100  # force a file time mismatch
        os.utime(
            ftfn, (t, t)
        )  # I must change the one on the actual file, otherwise the 'size+filetime is the same' optimization bytes me

        ppg.new()
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.FileInvariant(ftfn)
        job.depends_on(dep)
        ppg.run()
        assert (
            read(of) == "shushushu"
        )  # job does get rerun, md5sum and file time mismatch
        assert os.stat(ftfn)[stat.ST_MTIME] == os.stat(ftfn + ".md5sum")[stat.ST_MTIME]

    def test_invariant_dumping_on_job_failure(self):
        def w(of):
            write("out/A", "A")
            append("out/B", "B")

        def func_c(of):
            append("out/C", "C")

        func_dep = ppg.FunctionInvariant("func_c", func_c)
        fg = ppg.FileGeneratingJob("out/A", w, depend_on_function=False)
        fg.depends_on(func_dep)
        ppg.run()
        assert read("out/A") == "A"
        assert read("out/B") == "B"
        ppg.new()

        def func_c1(of):
            append("out/C", "D")

        def w2(of):
            raise ValueError()  # so there is an error in a job...

        func_dep = ppg.FunctionInvariant("func_c", func_c1)  # so this invariant changes
        fg = ppg.FileGeneratingJob(
            "out/A", w2, depend_on_function=False
        )  # and this job crashes
        fg.depends_on(func_dep)
        with pytest.raises(ppg.RunFailed):
            ppg.run()
        assert not (os.path.exists("out/A"))  # since it was removed, and not recreated
        assert read("out/B") == "B"
        ppg.new()
        func_dep = ppg.FunctionInvariant(
            "func_c", func_c1
        )  # no invariant change this time
        fg = ppg.FileGeneratingJob(
            "out/A", w, depend_on_function=False
        )  # but this was not done the last time...
        fg.depends_on(func_dep)
        ppg.run()
        assert read("out/A") == "A"
        assert read("out/B") == "BB"

    def test_invariant_dumping_on_graph_exception(self, mocker):
        # when an exception occurs not within a job
        # but within the pipegraph itself (e.g. when the user hit's CTRL-C
        # during history dumping
        # which we simulate here
        import pickle

        def w(of):
            write("out/A", "A")
            append("out/B", "B")

        def func_c(of):
            append("out/C", "C")

        func_dep = ppg.FunctionInvariant("func_c", func_c)
        fg = ppg.FileGeneratingJob("out/A", w, depend_on_function=False)
        fg.depends_on(func_dep)
        ppg.run()
        assert read("out/A") == "A"
        assert read("out/B") == "B"
        ppg.new()

        def func_c1(of):
            append("out/C", "D")

        def w2(of):
            write("out/A", "A2")
            raise ValueError()  # so there is an error in a job...

        func_dep = ppg.FunctionInvariant("func_c", func_c1)  # so this invariant changes
        fg = ppg.FileGeneratingJob(
            "out/A", w2, depend_on_function=False
        )  # and this job crashes
        fg.depends_on(func_dep)  # so a get's deleted, and rebuild
        fg2 = ppg.FileGeneratingJob(
            "out/C", lambda of: counter("out/c") and append(of, "C")
        )
        old_pickle_dumps = pickle.dumps
        raised_ki = [False]

        def new_pickle_dump(obj, protocol=None):
            if obj == "out/A" and not raised_ki[0]:
                raised_ki[0] = True
                raise KeyboardInterrupt("simulated")
            else:
                return old_pickle_dumps(obj, protocol)

        mocker.patch("pickle.dumps", new_pickle_dump)
        ki_raised = False
        try:
            ppg.run()
        except ppg.RunFailed:
            pass
        except KeyboardInterrupt:  # we expect this to be raised
            ki_raised = True
            pass
        if not ki_raised:
            raise ValueError("KeyboardInterrupt was not raised")
        assert os.path.exists("out/A")  # The file get's written.
        assert read("out/B") == "B"
        assert read("out/C") == "C"
        assert read("out/c") == "1"  #
        mocker.stopall()

        ppg.new()
        func_dep = ppg.FunctionInvariant(
            "func_c", func_c1
        )  # no invariant change this time
        # but we had no stored input/output for A, right?
        # so it get's rerun
        fg = ppg.FileGeneratingJob(
            "out/A", w, depend_on_function=False
        )  # but this was not done the last time...
        fg.depends_on(func_dep)
        fg2 = ppg.FileGeneratingJob(
            "out/C", lambda of: counter("out/c") and append(of, "C")
        )
        ppg.run()
        assert read("out/A") == "A"
        assert read("out/B") == "BB"
        assert read("out/C") == "C"  #
        assert read("out/c") == "1"  # c did not get rerun

    def test_input_output_dumping_dies_for_some_reason(self, ppg_per_test, mocker):
        import pickle

        raised_ki = [False]
        old_pickle_dumps = pickle.dumps

        def new_pickle_dump(obj, protocol=None):
            if obj == "A" and not raised_ki[0]:
                raised_ki[0] = True
                raise ValueError("simulated")
            else:
                return old_pickle_dumps(obj, protocol)

        mocker.patch("pickle.dumps", new_pickle_dump)

        ppg.FileGeneratingJob("A", lambda of: counter("a") and write(of, "A"))
        ppg.FileGeneratingJob("B", lambda of: counter("b") and write(of, "B"))
        with pytest.raises(ppg.RunFailedInternally):
            ppg.run()
        assert read("A") == "A"
        assert read("B") == "B"
        assert read("a") == "1"
        assert read("b") == "1"
        ppg.run()
        assert read("A") == "A"
        assert read("B") == "B"
        assert read("a") == "2"
        assert read("b") == "1"  # we had captured B so it's all good

    def test_FileInvariant_cant_have_dependencies(self):
        # invariants are always roots of the DAG - they can't have any dependencies themselves
        write("out/shu", "shu")
        job = ppg.FileInvariant("out/shu")
        jobB = ppg.FileGeneratingJob("out/A", lambda of: write("out/A", "a"))

        with pytest.raises(ppg.JobContractError):
            job.depends_on(jobB)

    def test_FunctionInvariant_cant_have_dependencies(self):
        # invariants are always roots of the DAG - they can't have any dependencies themselves
        job = ppg.FunctionInvariant("shu", lambda: 55)
        jobB = ppg.FileGeneratingJob("out/A", lambda of: write("out/A", "a"))

        with pytest.raises(ppg.JobContractError):
            job.depends_on(jobB)

    def test_ParameterInvariant_cant_have_dependencies(self):
        # invariants are always roots of the DAG - they can't have any dependencies themselves
        job = ppg.ParameterInvariant("out/shu", ("123",))
        jobB = ppg.FileGeneratingJob("out/A", lambda of: write("out/A", "a"))

        with pytest.raises(ppg.JobContractError):
            job.depends_on(jobB)

    def test_invariant_loading_issues_on_value_catastrophic(self):
        a = ppg.DataLoadingJob("a", lambda: 5)
        b = ppg.FileGeneratingJob(
            "out/b", lambda of: write("out/b", "b"), depend_on_function=False
        )
        b.depends_on(a)
        write("out/b", "a")
        import pickle

        Path(ppg.global_pipegraph._get_history_fn()).parent.mkdir(parents=True)
        with open(ppg.global_pipegraph._get_history_fn(), "wb") as op:
            pickle.dump(a.job_id, op, pickle.HIGHEST_PROTOCOL)
            op.write(b"This breaks")
        with pytest.raises(ppg.RunFailed):
            ppg.run()
        assert read("out/b") == "a"  # job was not run

    def test_invariant_loading_issues_on_value_undepickableclass(self):
        import tempfile
        import pickle

        # make sure Undepickable is Undepickable
        with tempfile.TemporaryFile("wb+") as tf:
            o = Undepickable()
            pickle.dump(o, tf, pickle.HIGHEST_PROTOCOL)
            with pytest.raises(pickle.UnpicklingError):
                tf.seek(0, 0)
                pickle.load(tf)

        a = ppg.ParameterInvariant("a", 5)
        b = ppg.FileGeneratingJob(
            "out/b", lambda of: write("out/b", "b"), depend_on_function=False
        )
        c = ppg.ParameterInvariant("c", 23)
        b.depends_on(a)
        write("out/b", "a")

        Path(ppg.global_pipegraph._get_history_fn()).parent.mkdir(parents=True)
        with open(ppg.global_pipegraph._get_history_fn(), "wb") as op:
            pickle.dump(a.job_id, op, pickle.HIGHEST_PROTOCOL)
            pickle.dump(Undepickable(), op, pickle.HIGHEST_PROTOCOL)
            pickle.dump(c.job_id, op, pickle.HIGHEST_PROTOCOL)
            pickle.dump(({}, {"c": str(c.parameters)}), op, pickle.HIGHEST_PROTOCOL)
        with pytest.raises(ppg.RunFailed):
            ppg.run()
        assert read("out/b") == "b"  # job was run
        # assert a.job_id in ppg.global_pipegraph.invariant_loading_issues
        # assert ppg.global_pipegraph.invariant_status["PIc"] == 23

    def test_invariant_loading_issues_on_key(self):
        a = ppg.DataLoadingJob("a", lambda: 5)
        b = ppg.FileGeneratingJob(
            "out/b", lambda of: write("out/b", "b"), depend_on_function=False
        )
        b.depends_on(a)
        write("out/b", "a")

        Path(ppg.global_pipegraph._get_history_fn()).parent.mkdir(parents=True)
        with open(ppg.global_pipegraph._get_history_fn(), "wb") as op:
            op.write(b"key breaks already")
            op.write(b"This breaks")
        with pytest.raises(ppg.RunFailed):
            ppg.run()
        assert read("out/b") == "a"  # job was not run


def first_value(d):
    return list(d.values())[0]


@pytest.mark.usefixtures("ppg_per_test")
class TestFunctionInvariant:
    # most of the function invariant testing is handled by other test classes.
    # but these are more specialized.

    def test_generator_expressions(self):
        def get_func(r):
            def shu():
                return sum(i + 0 for i in r)

            return shu

        def get_func2(r):
            def shu():
                return sum(i + 0 for i in r)

            return shu

        def get_func3(r):
            def shu():
                return sum(i + 1 for i in r)

            return shu

        a = ppg.FunctionInvariant("a", get_func(100))
        b = ppg.FunctionInvariant(
            "b", get_func2(100)
        )  # that invariant should be the same
        c = ppg.FunctionInvariant(
            "c", get_func3(100)
        )  # and this invariant should be different
        av = a.run(None, None)
        bv = b.run(None, None)
        cv = c.run(None, None)
        assert a.run(None, None)
        assert ppg.FunctionInvariant.compare_hashes(first_value(av), first_value(bv))
        assert not ppg.FunctionInvariant.compare_hashes(
            first_value(av), first_value(cv)
        )

    def test_lambdas(self):
        def get_func(x):
            def inner():
                arg = lambda y: x + x + x  # noqa:E731
                return arg(1)

            return inner

        def get_func2(x):
            def inner():
                arg = lambda y: x + x + x  # noqa:E731
                return arg(1)

            return inner

        def get_func3(x):
            def inner():
                arg = lambda y: x + x  # noqa:E731
                return arg(1)

            return inner

        a = ppg.FunctionInvariant("a", get_func(100))
        b = ppg.FunctionInvariant(
            "b", get_func2(100)
        )  # that invariant should be the same
        c = ppg.FunctionInvariant(
            "c", get_func3(100)
        )  # and this invariant should be different
        av = a.run(None, None)
        bv = b.run(None, None)
        cv = c.run(None, None)
        self.maxDiff = 20000
        assert a.run(None, None)
        assert ppg.FunctionInvariant.compare_hashes(first_value(av), first_value(bv))
        assert not ppg.FunctionInvariant.compare_hashes(
            first_value(av), first_value(cv)
        )

    def test_inner_functions(self):
        def get_func(x):
            def inner():
                return 23

            return inner

        def get_func2(x):
            def inner():
                return 23

            return inner

        def get_func3(x):
            def inner():
                return 23 + 5

            return inner

        a = ppg.FunctionInvariant("a", get_func(100))
        b = ppg.FunctionInvariant(
            "b", get_func2(100)
        )  # that invariant should be the same
        c = ppg.FunctionInvariant(
            "c", get_func3(100)
        )  # and this invariant should be different
        av = a.run(None, None)
        bv = b.run(None, None)
        cv = c.run(None, None)
        assert a.run(None, None)
        assert ppg.FunctionInvariant.compare_hashes(first_value(av), first_value(bv))
        assert not ppg.FunctionInvariant.compare_hashes(
            first_value(av), first_value(cv)
        )

    def test_nested_inner_functions(self):
        def get_func(x):
            def inner():
                def shu():
                    return 23

                return shu

            return inner

        def get_func2(x):
            def inner():
                def shu():
                    return 23

                return shu

            return inner

        def get_func3(x):
            def inner():
                def shu():
                    return 23 + 5

                return shu

            return inner

        a = ppg.FunctionInvariant("a", get_func(100))
        b = ppg.FunctionInvariant(
            "b", get_func2(100)
        )  # that invariant should be the same
        c = ppg.FunctionInvariant(
            "c", get_func3(100)
        )  # and this invariant should be different
        av = a.run(None, None)
        bv = b.run(None, None)
        cv = c.run(None, None)
        assert a.run(None, None)
        assert ppg.FunctionInvariant.compare_hashes(first_value(av), first_value(bv))
        assert not ppg.FunctionInvariant.compare_hashes(
            first_value(av), first_value(cv)
        )

    def test_inner_functions_with_parameters(self):
        def get_func(x):
            def inner():
                return x

            return inner

        a = ppg.FunctionInvariant("a", get_func(100))
        b = ppg.FunctionInvariant(
            "b", get_func(100)
        )  # that invariant should be the same
        c = ppg.FunctionInvariant(
            "c", get_func(2000)
        )  # and this invariant should be different
        av = a.run(None, None)
        bv = b.run(None, None)
        cv = c.run(None, None)
        assert a.run(None, None)
        assert ppg.FunctionInvariant.compare_hashes(first_value(av), first_value(bv))
        assert not ppg.FunctionInvariant.compare_hashes(
            first_value(av), first_value(cv)
        )

    def test_passing_non_function_raises(self):
        with pytest.raises(TypeError):
            ppg.FunctionInvariant("out/a", "shu")

    def test_passing_none_as_function_is_ok(self, create_out_dir):
        job = ppg.FunctionInvariant("out/a", None)
        jobB = ppg.FileGeneratingJob("out/A", lambda of: write("out/A", "A"))
        jobB.depends_on(job)
        ppg.run()
        assert read("out/A") == "A"

    def test_passing_non_string_as_jobid(self):
        with pytest.raises(TypeError):
            ppg.FunctionInvariant(5, lambda: 1)

    def test_cant_have_dependencies(self):
        # invariants are always roots of the DAG - they can't have any dependencies themselves
        def shu():
            pass

        job = ppg.FunctionInvariant("shu", shu)
        jobB = ppg.FileGeneratingJob("out/A", lambda of: write("out/A", "a"))

        with pytest.raises(ppg.JobContractError):
            job.depends_on(jobB)

    @pytest.mark.xfail()  # todo: interactive/notebook differences
    def test_raises_on_duplicate_with_different_functions(self):
        def shu():
            return "a"

        ppg.FunctionInvariant("A", shu)
        ppg.FunctionInvariant("A", shu)  # ok.
        with pytest.raises(ppg.JobContractError):
            ppg.FunctionInvariant("A", lambda: "b")  # raises ValueError

        def sha():
            def shu():
                return "b"

            return shu

        ppg.FunctionInvariant("B", sha())
        ppg.FunctionInvariant("B", sha())

    def test_instance_functions_ok(self, create_out_dir):
        class shu:
            def __init__(self, letter):
                self.letter = letter

            def get_job(self):
                job = ppg.FileGeneratingJob(
                    "out/" + self.letter, lambda of: append(of, "A")
                )
                job.depends_on(ppg.FunctionInvariant("shu.sha", self.sha))
                return job

            def sha(self):
                return 55 * 23

        x = shu("A")
        x.get_job()
        ppg.run()
        assert read("out/A") == "A"
        append("out/A", "A")

        ppg.new()
        x.get_job()
        y = shu("B")
        j1 = y.get_job()
        j2 = y.get_job()
        assert ppg.FunctionInvariant.functions_equal(
            j1.generating_function, j2.generating_function
        )
        # assert j1 is j2 # todo: interactive/notebook differences

    def test_buildin_function(self):
        a = ppg.FunctionInvariant("a", open)
        assert "<built-in" in str(a)

    def test_function_invariant_non_function(self):
        class CallMe:
            def __call__(self):
                raise ValueError()

        a = ppg.FunctionInvariant("a", CallMe)
        with pytest.raises(
            ValueError
        ):  # todo: is this the right behaviour? can't we just forward to __call__ as the invariant?
            a.run(None, None)

    def test_closure_capturing(self):
        def func(da_list):
            def f():
                return da_list

            return f

        a = ppg.FunctionInvariant("a", func([1, 2, 3]))
        b = ppg.FunctionInvariant(
            "b", func([1, 2, 3])
        )  # that invariant should be the same
        c = ppg.FunctionInvariant(
            "c", func([1, 2, 3, 4])
        )  # and this invariant should be different
        av = a.run(None, None)
        bv = b.run(None, None)
        cv = c.run(None, None)
        assert a.run(None, None)
        assert ppg.FunctionInvariant.compare_hashes(first_value(av), first_value(bv))
        assert not ppg.FunctionInvariant.compare_hashes(
            first_value(av), first_value(cv)
        )

    def test_function_to_str_builtin(self):
        assert ppg.FunctionInvariant.function_to_str(open) == "<built-in function open>"

    def test_closure_capturing_dict(self):
        def func(da_list):
            def f():
                return da_list

            return f

        a = ppg.FunctionInvariant("a", func({"1": "a", "3": "b", "2": "c"}))
        b = ppg.FunctionInvariant(
            "b", func({"1": "a", "3": "b", "2": "c"})
        )  # that invariant should be the same
        c = ppg.FunctionInvariant(
            "c", func({"1": "a", "3": "b", "2": "d"})
        )  # and this invariant should be different
        av = a.run(None, None)
        bv = b.run(None, None)
        cv = c.run(None, None)
        assert a.run(None, None)
        assert ppg.FunctionInvariant.compare_hashes(first_value(av), first_value(bv))
        assert not ppg.FunctionInvariant.compare_hashes(
            first_value(av), first_value(cv)
        )

    def test_closure_capturing_set(self):
        def func(da_list):
            def f():
                return da_list

            return f

        import random

        x = set(["1", "2", "3", "4", "5", "6", "7", "8"])
        a = ppg.FunctionInvariant("a", func(x))
        x2 = list(x)
        random.shuffle(x2)
        x2 = set(x2)
        b = ppg.FunctionInvariant("b", func(x2))  # that invariant should be the same
        c = ppg.FunctionInvariant(
            "c", func({"3", "2"})
        )  # and this invariant should be different
        av = a.run(None, None)
        bv = b.run(None, None)
        cv = c.run(None, None)
        assert a.run(None, None)
        assert ppg.FunctionInvariant.compare_hashes(first_value(av), first_value(bv))
        assert not ppg.FunctionInvariant.compare_hashes(
            first_value(av), first_value(cv)
        )

    def test_closure_capturing_frozen_set(self):
        def func(da_list):
            def f():
                return da_list

            return f

        import random

        x = frozenset(["1", "2", "3", "4", "5", "6", "7", "8"])
        a = ppg.FunctionInvariant("a", func(x))
        x2 = list(x)
        random.shuffle(x2)
        x2 = frozenset(x2)
        b = ppg.FunctionInvariant("b", func(x2))  # that invariant should be the same
        c = ppg.FunctionInvariant(
            "c", func(frozenset({"3", "2"}))
        )  # and this invariant should be different
        av = a.run(None, None)
        bv = b.run(None, None)
        cv = c.run(None, None)
        assert a.run(None, None)
        assert ppg.FunctionInvariant.compare_hashes(first_value(av), first_value(bv))
        assert not ppg.FunctionInvariant.compare_hashes(
            first_value(av), first_value(cv)
        )

    @pytest.mark.xfail
    def test_invariant_caching(self):

        a = ppg.FunctionInvariant("a", ppg.inside_ppg)
        old_dis = a.dis_code
        counter = [0]

        def new_dis(*args, **kwargs):
            counter[0] += 1
            return old_dis(*args, **kwargs)

        a.dis_code = new_dis
        # round 0 - everything needs to be calculated
        assert len(ppg.util.global_pipegraph.func_hashes) == 0
        iv1 = a._get_invariant(False, [])
        assert counter[0] == 1
        assert len(ppg.util.global_pipegraph.func_hashes) == 1
        assert len(ppg.util.global_pipegraph.file_hashes) == 0

        # same function again - no new calc
        iv2 = a._get_invariant(False, [])
        assert iv1 == iv2
        assert counter[0] == 1

        # we lost the function hash, and were passed false:
        ppg.util.global_pipegraph.func_hashes.clear()
        iv3 = a._get_invariant(False, [])
        assert iv3 == iv2
        assert counter[0] == 2
        assert len(ppg.util.global_pipegraph.func_hashes) == 1

        # we lost the function hash - but were passed an old tuple
        # with matching file hash
        ppg.util.global_pipegraph.func_hashes.clear()
        iv3b = a._get_invariant(iv3, [])
        assert iv3 is iv3b
        assert counter[0] == 2
        assert len(ppg.util.global_pipegraph.func_hashes) == 0
        ppg.util.global_pipegraph.func_hashes.clear()

        # now let's simulate the file changing
        faked_iv3 = ("aa",) + iv3[1:]
        ppg.util.global_pipegraph.func_hashes.clear()
        with pytest.raises(ppg.NothingChanged) as e:
            a._get_invariant(faked_iv3, [])
        iv4 = e.value.new_value
        assert iv4[2:] == iv3[2:]
        assert len(ppg.util.global_pipegraph.func_hashes) == 1
        assert counter[0] == 3
        assert (
            len(ppg.util.global_pipegraph.file_hashes) == 0
        )  # we still used the the function.__code__.co_filename

        # now let's simulate the line no changing.
        faked_iv3 = (iv3[0],) + (1,) + iv3[2:]
        ppg.util.global_pipegraph.func_hashes.clear()
        with pytest.raises(ppg.NothingChanged) as e:
            a._get_invariant(faked_iv3, [])
        iv5 = e.value.new_value
        assert iv5[2:] == iv3[2:]
        assert len(ppg.util.global_pipegraph.func_hashes) == 1
        assert counter[0] == 4
        assert (
            len(ppg.util.global_pipegraph.file_hashes) == 0
        )  # we still used the the function.__code__.co_filename

        # and now, going from the old to the new...
        old = iv1[2] + iv1[3]
        with pytest.raises(ppg.NothingChanged) as e:
            a._get_invariant(old, [])
        assert e.value.new_value == iv1

        # and last but not least let's test the closure based seperation
        ppg.util.global_pipegraph.func_hashes.clear()
        ppg.util.global_pipegraph.file_hashes.clear()

        def capture(x):
            def inner():
                return 5 + x

            return inner

        b = ppg.FunctionInvariant("x5", capture(5))
        c = ppg.FunctionInvariant("x10", capture(10))
        ivb = b._get_invariant(False, [])
        assert len(ppg.util.global_pipegraph.func_hashes) == 1
        ivc = c._get_invariant(False, [])
        # no recalc - reuse the one from the previous function
        assert len(ppg.util.global_pipegraph.func_hashes) == 1
        assert ivb[:3] == ivc[:3]

    def test_function_invariants_are_equal_if_dis_identical_or_source_identical(self):
        python_version = (3, 99)
        a = {"source": "hello", python_version: ("dis", "closure")}
        b = a.copy()
        b["source"] = "hello_world"
        c = a.copy()
        c[python_version] = ("disB", "closure")
        assert ppg.FunctionInvariant.compare_hashes(a, a, python_version)
        assert ppg.FunctionInvariant.compare_hashes(
            a, b, python_version
        )  # same dis ,different source
        assert not ppg.FunctionInvariant.compare_hashes(
            a, c, python_version
        )  # different dis, same source


@pytest.mark.usefixtures("create_out_dir")
@pytest.mark.usefixtures("ppg_per_test")
class TestDependency:
    def test_simple_chain(self):
        o = Dummy()

        def load_a():
            return "shu"

        jobA = ppg.AttributeLoadingJob("a", o, "myattr", load_a)
        ofB = "out/B"

        def do_write_b(ofB):
            write(ofB, o.myattr)

        jobB = ppg.FileGeneratingJob(ofB, do_write_b).depends_on(jobA)
        ofC = "out/C"

        def do_write_C(ofC):
            write(ofC, o.myattr)

        ppg.FileGeneratingJob(ofC, do_write_C).depends_on(jobA)

        ofD = "out/D"

        def do_write_d(ofD):
            write(ofD, read(ofC) + read(ofB))

        ppg.FileGeneratingJob(ofD, do_write_d).depends_on([jobA, jobB])

    def test_failed_job_kills_those_after(self):
        ofA = "out/A"

        def write_a(ofA):
            append(ofA, "hello")

        jobA = ppg.FileGeneratingJob(ofA, write_a)

        ofB = "out/B"

        def write_b(ofB):
            raise ValueError("shu")

        jobB = ppg.FileGeneratingJob(ofB, write_b)
        jobB.depends_on(jobA)

        ofC = "out/C"

        def write_c(ofC):
            write(ofC, "hello")

        jobC = ppg.FileGeneratingJob(ofC, write_c)
        jobC.depends_on(jobB)
        with pytest.raises(ppg.RunFailed):
            ppg.run()
        assert os.path.exists(ofA)  # which was before the error
        assert not (os.path.exists(ofB))  # which was on the error
        assert not (os.path.exists(ofC))  # which was after the error
        ppg.new()
        jobA = ppg.FileGeneratingJob(ofA, write_a)
        jobC = ppg.FileGeneratingJob(ofC, write_c)

        def write_b_ok(ofB):
            write(ofB, "BB")

        jobB = ppg.FileGeneratingJob(ofB, write_b_ok)
        jobB.depends_on(jobA)
        jobC.depends_on(jobB)
        ppg.run()

        assert os.path.exists(ofA)
        assert read(ofA) == "hello"  # run only once!
        assert os.path.exists(ofB)
        assert os.path.exists(ofC)

    def test_done_filejob_does_not_gum_up_execution(self):
        ofA = "out/A"
        write(ofA, "1111")

        def write_a(ofA):
            append(ofA, "hello")

        jobA = ppg.FileGeneratingJob(ofA, write_a, depend_on_function=False)

        ofB = "out/B"

        def write_b(ofB):
            append(ofB, "hello")

        jobB = ppg.FileGeneratingJob(ofB, write_b)
        jobB.depends_on(jobA)

        ofC = "out/C"

        def write_c(ofC):
            write(ofC, "hello")

        jobC = ppg.FileGeneratingJob(ofC, write_c)
        jobC.depends_on(jobB)
        assert os.path.exists(ofA)

        ppg.run()

        assert os.path.exists(ofB)
        assert os.path.exists(ofC)
        assert (
            read(ofA) == "hello"
        )  # change from ppgA, if it's not our file (we have recorded not output), rerun

    def test_invariant_violation_redoes_deps_but_not_nondeps(self):
        def get_job(name):
            fn = "out/" + name

            def do_write(of):
                if os.path.exists(fn + ".sentinel"):
                    d = read(fn + ".sentinel")
                else:
                    d = ""
                append(fn + ".sentinel", name)  # get's longer all the time...
                write(fn, d + name)  # get's deleted anyhow...

            return ppg.FileGeneratingJob(fn, do_write)

        jobA = get_job("A")
        jobB = get_job("B")
        jobC = get_job("C")
        get_job("D")
        jobC.depends_on(jobB)
        jobB.depends_on(jobA)
        dep = ppg.ParameterInvariant("myparam", ("hello",))
        jobA.depends_on(dep)
        ppg.run()
        assert read("out/A") == "A"
        assert read("out/B") == "B"
        assert read("out/C") == "C"

        ppg.new()
        jobA = get_job("A")
        jobB = get_job("B")
        jobC = get_job("C")
        get_job("D")
        jobC.depends_on(jobB)
        jobB.depends_on(jobA)
        dep = ppg.ParameterInvariant("myparam", ("hello stranger",))
        jobA.depends_on(dep)  # now, the invariant has been changed, all jobs rerun...
        ppg.run()
        assert read("out/A") == "AA"  # thanks to our smart rerun aware job definition..
        assert read("out/B") == "BB"
        assert read("out/C") == "CC"
        assert read("out/D") == "D"  # since that one does not to be rerun...

    def test_depends_on_accepts_a_list(self):
        jobA = ppg.FileGeneratingJob("out/A", lambda of: write("out/A", "A"))
        jobB = ppg.FileGeneratingJob("out/B", lambda of: write("out/B", "B"))
        jobC = ppg.FileGeneratingJob("out/C", lambda of: write("out/C", "C"))
        jobC.depends_on([jobA, jobB])
        ppg.run()
        assert read("out/A") == "A"
        assert read("out/B") == "B"
        assert read("out/C") == "C"

    def test_depends_on_accepts_multiple_values(self):
        jobA = ppg.FileGeneratingJob("out/A", lambda of: write("out/A", "A"))
        jobB = ppg.FileGeneratingJob("out/B", lambda of: write("out/B", "B"))
        jobC = ppg.FileGeneratingJob("out/C", lambda of: write("out/C", "C"))
        jobC.depends_on(jobA, jobB)
        ppg.run()
        assert read("out/A") == "A"
        assert read("out/B") == "B"
        assert read("out/C") == "C"

    def test_depends_on_accepts_multiple_values_mixed(self):
        jobA = ppg.FileGeneratingJob("out/A", lambda of: write("out/A", "A"))
        jobB = ppg.FileGeneratingJob("out/B", lambda of: write("out/B", "B"))
        jobC = ppg.FileGeneratingJob("out/C", lambda of: write("out/C", "C"))
        jobC.depends_on(jobA, [jobB])
        ppg.run()
        assert read("out/A") == "A"
        assert read("out/B") == "B"
        assert read("out/C") == "C"

    def test_depends_on_none_ignored(self):
        jobA = ppg.FileGeneratingJob("out/A", lambda of: write("out/A", "A"))
        jobB = ppg.FileGeneratingJob("out/B", lambda of: write("out/B", "B"))
        jobC = ppg.FileGeneratingJob("out/C", lambda of: write("out/C", "C"))
        jobC.depends_on(jobA, [jobB], None, [None])
        jobC.depends_on(None)
        jobC.depends_on()
        ppg.run()
        assert read("out/A") == "A"
        assert read("out/B") == "B"
        assert read("out/C") == "C"

    def test_depends_on_excludes_on_non_jobs(self):
        jobA = ppg.FileGeneratingJob("out/A", lambda of: write("out/A", "A"))

        with pytest.raises(KeyError):
            jobA.depends_on("SHU")

    def test_depends_on_instant_cycle_check(self):
        jobA = ppg.FileGeneratingJob("out/A", lambda of: write("out/A", "A"))
        jobB = ppg.FileGeneratingJob("out/b", lambda of: write("out/B", "b"))
        jobB.depends_on(jobA)

        with pytest.raises(ppg.NotADag):
            jobA.depends_on(jobA)

        with pytest.raises(ppg.NotADag):
            jobA.depends_on(jobB)

    def test_depends_on_accepts_a_list_of_lists(self):
        jobA = ppg.FileGeneratingJob("out/A", lambda of: write("out/A", "A"))
        jobB = ppg.FileGeneratingJob("out/B", lambda of: write("out/B", "B"))
        jobC = ppg.FileGeneratingJob(
            "out/C",
            lambda of: write("out/C", read("out/A") + read("out/B") + read("out/D")),
        )
        jobD = ppg.FileGeneratingJob("out/D", lambda of: write("out/D", "D"))
        jobC.depends_on([jobA, [jobB, jobD]])
        assert ppg.global_pipegraph.has_edge(jobD, jobC)
        assert ppg.global_pipegraph.has_edge(jobA, jobC)
        assert ppg.global_pipegraph.has_edge(jobB, jobC)
        ppg.run()
        assert read("out/A") == "A"
        assert read("out/B") == "B"
        assert read("out/C") == "ABD"
        assert read("out/D") == "D"

    def test_invariant_job_depends_on_raises(self):

        with pytest.raises(ppg.JobContractError):
            ppg.jobs._InvariantMixin().depends_on(ppg.Job("B"))

    def test_cached_job_depends_on(self):
        class Dummy:
            pass

        o = Dummy()
        jobA = ppg.CachedAttributeLoadingJob("cache/A", o, "a", lambda: 23)
        jobB = ppg.Job("B")
        jobC = ppg.Job("C")
        jobD = ppg.Job("D")
        jobA.calc.depends_on([jobB], jobC, jobD)
        assert not ppg.global_pipegraph.has_edge(jobB, jobA.load)
        assert not ppg.global_pipegraph.has_edge(jobC, jobA.load)
        assert not ppg.global_pipegraph.has_edge(jobD, jobA.load)
        assert ppg.global_pipegraph.has_edge(jobB, jobA.calc)
        assert ppg.global_pipegraph.has_edge(jobC, jobA.calc)
        assert ppg.global_pipegraph.has_edge(jobD, jobA.calc)

    def test_dependency_placeholder(self):
        jobA = ppg.FileGeneratingJob(
            "out/A", lambda of: write("out/A", "A" + read("out/B"))
        )
        jobB = ppg.FileGeneratingJob("out/B", lambda of: write("out/B", "B"))

        def gen_deps():
            logger.info("gen deps called")
            return [jobB]

        jobA.depends_on(gen_deps)
        ppg.run()
        assert read("out/A") == "AB"

    def test_dependency_placeholder2(self):
        jobA = ppg.FileGeneratingJob(
            "out/A", lambda of: write("out/A", "A" + read("out/B"))
        )

        def gen_deps():
            return ppg.FileGeneratingJob("out/B", lambda of: write("out/B", "B"))

        jobA.depends_on(gen_deps)
        ppg.run()
        assert read("out/A") == "AB"

    def test_dependency_placeholder_nested(self):
        jobA = ppg.FileGeneratingJob(
            "out/A", lambda of: write("out/A", "A" + read("out/B") + read("out/C"))
        )

        def gen_deps2():
            return ppg.FileGeneratingJob("out/C", lambda of: write("out/C", "C"))

        def gen_deps():
            return ppg.FileGeneratingJob(
                "out/B", lambda of: write("out/B", "B")
            ).depends_on(gen_deps2)

        jobA.depends_on(gen_deps)
        ppg.run()
        assert read("out/A") == "ABC"

    def test_dependency_placeholder_dynamic_auto_invariants(self):
        jobA = ppg.FileGeneratingJob(
            "out/A", lambda of: write("out/A", "A" + read("out/B"))
        )

        def check_function_invariant(of):
            write("out/B", "B")
            assert "FunctionInvariant:out/B" in ppg.global_pipegraph.jobs

        def gen_deps():
            jobB = ppg.FileGeneratingJob("out/B", check_function_invariant)
            print("gen deps called")
            return [jobB]

        jobA.depends_on(gen_deps)
        assert "FunctionInvariant:out/B" not in ppg.global_pipegraph.jobs
        ppg.run()
        assert read("out/A") == "AB"


@pytest.mark.usefixtures("ppg_per_test")
class TestDefinitionErrors:
    @pytest.mark.xfail()  # wait for interactive/notebook decision
    def test_defining_function_invariant_twice(self):
        a = lambda: 55  # noqa:E731
        b = lambda: 66  # noqa:E731
        ppg.FunctionInvariant("a", a)

        with pytest.raises(ppg.JobContractError):
            ppg.FunctionInvariant("a", b)

    @pytest.mark.xfail()  # wait for interactive/notebook decision
    def test_defining_function_and_parameter_invariant_with_same_name(self):
        a = lambda: 55  # noqa:E731
        b = lambda: 66  # noqa:E731
        ppg.FunctionInvariant("PIa", a)

        with pytest.raises(ppg.JobContractError):
            ppg.ParameterInvariant("a", b)

    @pytest.mark.xfail()  # wait for interactive/notebook decision
    def test_defining_function_and_parameter_invariant_with_same_name_reversed(self):
        a = lambda: 55  # noqa:E731
        b = lambda: 66  # noqa:E731
        ppg.ParameterInvariant("a", b)

        with pytest.raises(ppg.JobContractError):
            ppg.FunctionInvariant("PIa", a)


@pytest.mark.usefixtures("ppg_per_test")
class TestFunctionInvariantDisChanges_BetweenVersions:
    def test_function_name_is_irrelevant(self):
        def test_a():
            return 55

        def test_b():
            return 55

        def test_c():
            return 56

        a = ppg.FunctionInvariant("a", test_a)
        b = ppg.FunctionInvariant("b", test_b)
        c = ppg.FunctionInvariant("c", test_c)
        av = a.run(None, None)
        bv = b.run(None, None)
        cv = c.run(None, None)
        assert ppg.FunctionInvariant.compare_hashes(first_value(av), first_value(bv))
        assert not ppg.FunctionInvariant.compare_hashes(
            first_value(av), first_value(cv)
        )

    def test_docstring_is_irrelevant(self):
        def test():
            """A"""
            return 55

        a = ppg.FunctionInvariant("a", test)

        # fmt: off
        def test():
            '''B'''
            return 55
        # fmt: on
        b = ppg.FunctionInvariant("b", test)

        def test():
            "c"
            return 56

        c = ppg.FunctionInvariant("c", test)

        def test():
            "c"
            return 56

        d = ppg.FunctionInvariant("d", test)
        av = first_value(a.run(None, None))
        bv = first_value(b.run(None, None))
        cv = first_value(c.run(None, None))
        dv = first_value(d.run(None, None))
        assert ppg.FunctionInvariant.compare_hashes((av), (bv))
        assert ppg.FunctionInvariant.compare_hashes((cv), (dv))
        assert not ppg.FunctionInvariant.compare_hashes((av), (cv))
