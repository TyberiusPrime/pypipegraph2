from pathlib import Path
import sys
import pytest
import pypipegraph2 as ppg
from .shared import write, read, Dummy, append, counter

global_test = 0


@pytest.mark.usefixtures("ppg2_per_test")
class TestJobs:
    def test_assert_singletonicity_of_jobs(self):
        ppg.new()
        of = "out/a"
        data_to_write = "hello"

        def do_write(of):
            write(of, data_to_write)

        job = ppg.FileGeneratingJob(of, do_write)
        job2 = ppg.FileGeneratingJob(of, do_write)
        assert job is job2  # change from ppg1

    def test_add_job_twice_is_harmless(self):
        job = ppg.FileGeneratingJob("A", lambda of: 5)
        # implicit add
        assert job.job_id in ppg.global_pipegraph.jobs
        assert ppg.global_pipegraph.jobs[job.job_id] is job
        ppg.global_pipegraph.add(job)
        assert job.job_id in ppg.global_pipegraph.jobs
        assert ppg.global_pipegraph.jobs[job.job_id] is job

    def test_redefining_a_jobid_with_different_class_raises(self):
        ppg.new()
        of = "out/a"
        data_to_write = "hello"

        def do_write(of):
            write(of, data_to_write)

        ppg.FileGeneratingJob(of, do_write)

        def load():
            return "shu"

        with pytest.raises(ValueError):
            ppg.DataLoadingJob(of, load)

    def test_raises_on_non_str_job_id(self):
        with pytest.raises(TypeError):
            ppg.FileGeneratingJob(1234, lambda of: None)

    def test_auto_name(self):
        def inner():
            pass

        a = ppg.FunctionInvariant(inner)
        assert a.job_id == "FITestJobs.test_auto_name.<locals>.inner"
        with pytest.raises(TypeError):
            ppg.FunctionInvariant(lambda: 55)
        with pytest.raises(TypeError):
            ppg.FunctionInvariant(None)

    def test_equality_is_identity(self):
        def write_func(of):
            def do_write(of):
                write(of, "do_write done")

            return of, do_write

        ppg.new()
        jobA = ppg.FileGeneratingJob(*write_func("out/a"))
        jobA1 = ppg.FileGeneratingJob(*write_func("out/a"))
        jobB = ppg.FileGeneratingJob(*write_func("out/b"))
        assert jobA is jobA1
        assert jobA == jobA1
        assert not (jobA == jobB)

    def test_has_hash(self):
        ppg.new()
        jobA = ppg.FileGeneratingJob("out/", lambda of: None)
        assert hasattr(jobA, "__hash__")

    def test_repeated_job_definition_and_dependency_callbacks(self, ppg2_per_test):
        def func(of):
            of.write_text("a")

        a = ppg.FileGeneratingJob("a", func)
        a.depends_on(lambda: counter("ac") and None)
        assert len(a.dependency_callbacks) == 1
        assert not Path("ac").exists()
        a = ppg.FileGeneratingJob("a", func)
        a.depends_on(lambda: counter("ac") and counter("bc") and None)  # 2nd dependency
        assert len(a.dependency_callbacks) == 2
        assert not Path("ac").exists()
        ppg.run()
        assert read("ac") == "2"
        assert read("bc") == "1"

    def test_dependency_callback_plus_job(self):
        data = []

        def load_a():
            data.append("a")
            return ppg.UseInputHashesForOutput()

        a = ppg.DataLoadingJob("A", load_a)
        b = lambda: ppg.FileGeneratingJob(  # noqa: E731
            "B", lambda of: of.write_text("b")
        )
        c = ppg.FileGeneratingJob(
            "C", lambda of: of.write_text(Path("B").read_text() + data[0])
        )
        c.depends_on(b, a)
        ppg.run()

    def test_data_loading_MultiFile_dowstream(self, job_trace_log):
        # todo :rename this test?
        def tf(ofs):
            counter("A")
            ofs[0].write_text("a1")
            ofs[1].write_text("a1")

        a = ppg.MultiTempFileGeneratingJob(["a1", "a2"], tf)

        def write(ofs):
            counter("B")
            ofs[0].write_text("b")
            ofs[1].write_text("c" + read("a1"))

        bc = ppg.MultiFileGeneratingJob(["b", "c"], write)
        assert bc[0] == Path("b")
        assert bc[1] == Path("c")
        bc.depends_on(a)
        bc()
        assert read("c") == "ca1"
        assert read("b") == "b"
        assert read("b") == "b"
        assert read("A") == "1"
        assert read("B") == "1"
        ppg.run()
        assert read("c") == "ca1"
        assert read("b") == "b"
        assert read("B") == "1"
        assert read("A") == "1"

    def test_returning_none(self):
        ppg.new(run_mode=ppg.RunMode.NOTEBOOK)
        a = ppg.FileGeneratingJob("out/a", lambda of: counter("a") and write(of, "A"))
        b = ppg.FileGeneratingJob("out/b", lambda of: counter("b") and write(of, "B"))
        c = ppg.DataLoadingJob("o", lambda: counter("c") and ppg.UseInputHashesForOutput())
        b.depends_on(a)
        a.depends_on_params("x")
        a.depends_on(c)
        b.depends_on(c)
        ppg.run()
        assert read("a") == "1"
        assert read("b") == "1"
        assert read("c") == "1"
        a.depends_on_params("y")
        ppg.run()
        assert read("a") == "2"
        assert read("c") == "2"
        assert read("b") == "1"  # ppg2_rust change


@pytest.mark.usefixtures("ppg2_per_test")
class TestJobs2:
    def test_str(self):
        a = ppg.FileGeneratingJob("out/A", lambda of: write("out/A", "hello"))
        assert isinstance(str(a), str)

        a = ppg.ParameterInvariant("out/A", "hello")
        assert isinstance(str(a), str)

        a = ppg.JobGeneratingJob("out/Ax", lambda: "hello")
        assert isinstance(str(a), str)


@pytest.mark.usefixtures("create_out_dir")
@pytest.mark.usefixtures("ppg2_per_test")
class TestFileGeneratingJob:
    def test_basic(self):
        of = "out/a"
        data_to_write = "hello"

        def do_write(ofof):
            print("do_write was called")
            write(of, data_to_write)

        job = ppg.FileGeneratingJob(of, do_write, depend_on_function=False)
        res = ppg.run()
        assert res[job.job_id].outcome == ppg.enums.JobOutcome.Success
        assert Path(of).exists()
        assert read(of) == data_to_write

    def test_cores_needed(self):
        ppg.new(cores=5)
        of = "out/a"
        data_to_write = "hello"

        def do_write(ofof):
            print("do_write was called")
            write(of, data_to_write)

        job = ppg.FileGeneratingJob(of, do_write, depend_on_function=False)
        with pytest.raises(TypeError):
            job.use_resources(0)
        job.cores_needed = ppg.Resources.SingleCore
        job.cores_needed = ppg.Resources.AllCores
        job.cores_needed = ppg.Resources.Exclusive

        for i in range(10):
            j = ppg.FileGeneratingJob("out/%i" % i, lambda of, i=i: write(of, f"b{i}"))
            if i % 2 == 0:
                j.cores_needed = ppg.Resources.Exclusive

        ppg.run()
        assert read(of) == data_to_write
        for i in range(10):
            assert read(f"out/{i}") == f"b{i}"

    def test_cores_needed2(self):
        ppg.new(cores=3)
        # this is meant to trigger the
        # "this job needed to much resources, or was not runnable"
        # case of graph.start_jobs

        for i in range(20):
            j = ppg.FileGeneratingJob("out/%i" % i, lambda of, i=i: write(of, f"b{i}"))
            if i % 2 == 0:
                j.use_resources(ppg.Resources.AllCores)
            else:
                j.use_resources(ppg.Resources.Exclusive)

        ppg.run()
        for i in range(20):
            assert read(f"out/{i}") == f"b{i}"

    def test_misspecified_job_does_not_hang_graph(self):
        # this occurred during development
        ppg.new(cores=3)

        for i in range(1):
            j = ppg.FileGeneratingJob(
                "out/%i" % i,
                lambda i=i: write("out/%i" % i, "b"),
                depend_on_function=False,
            )

        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        print(
            "this is it'", str(ppg.global_pipegraph.last_run_result["out/0"].error), "'"
        )

        assert "number is required, not PosixPath" in str(
            ppg.global_pipegraph.last_run_result["out/0"].error
        )

    def test_job_not_creating_its_file(self):
        # this occurred during development
        ppg.new(cores=3)

        for i in range(1):
            j = ppg.FileGeneratingJob(
                "out/%i" % i,
                lambda of, i=i: write("shu", "b"),
                depend_on_function=False,
            )

        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert "did not create the following files: ['out/0']" in str(
            ppg.global_pipegraph.last_run_result["out/0"].error
        )

    def test_basic_with_parameter(self):
        data_to_write = "hello"

        def do_write(filename):
            print("do_write was called")
            write(filename, data_to_write)

        job = ppg.FileGeneratingJob("out/a", do_write, depend_on_function=False)
        ppg.run()
        assert Path("out/a").exists()
        op = open("out/a", "r")
        data = op.read()
        op.close()
        assert data == data_to_write

    def test_simple_filegeneration_with_function_dependency(self):
        of = "out/a"
        data_to_write = "hello"

        def do_write(ofof):
            print("do_write was called")
            write(of, data_to_write)

        job = ppg.FileGeneratingJob(of, do_write)
        ppg.run()
        assert Path(of).exists()
        op = open(of, "r")
        data = op.read()
        op.close()
        assert data == data_to_write

    def test_filejob_raises_if_no_data_is_written(self):
        of = "out/a"

        def do_write(of):
            write("out/A", "")

        job = ppg.FileGeneratingJob(of, do_write)

        with pytest.raises(ppg.JobsFailed):
            ppg.run()

        assert isinstance(
            ppg.global_pipegraph.last_run_result[job.job_id].error.args[0],
            ppg.JobContractError,
        )
        assert not (Path(of).exists())

    def test_filejob_empty_allowed(self):
        ppg.FileGeneratingJob("out/A", lambda of: write("out/A", ""), empty_ok=True)
        ppg.run()
        assert read("out/A") == ""

    def test_filejob_raising_exception_bubbles(self):
        def do(of):
            raise ValueError("Hello Exception")

        job = ppg.FileGeneratingJob("out/A", do)
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert "Hello Exception" in str(
            ppg.global_pipegraph.last_run_result[job.job_id].error
        )

    def test_simple_filegeneration_keeps_file_on_exception(self):
        of = "out/a"
        data_to_write = "hello"

        def do_write(of):
            write(of, data_to_write)
            raise ValueError("shu")

        ppg.FileGeneratingJob(of, do_write)
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert Path(
            of
        ).exists()  # unlike ppg1, we know by the (lack of) output records that we need to redo this
        assert isinstance(
            ppg.global_pipegraph.last_run_result[of].error.args[0], ValueError
        )

    def test_filegenerating_ok_change_fail_ok(self):
        ppg.new(run_mode=ppg.RunMode.NOTEBOOK)
        of = "A"
        func1 = lambda of: counter("a") and write(of, "A")  # noqa: E731
        job = ppg.FileGeneratingJob(of, func1)
        ppg.run()
        assert read("a") == "1"
        ppg.run()
        assert read("a") == "1"
        assert read("A") == "A"

        def do(of):
            counter("a")
            write(of, "B")
            raise ValueError()

        job = ppg.FileGeneratingJob(of, do)
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert read("a") == "2"
        assert read("A") == "B"
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert read("a") == "3"
        assert read("A") == "B"

        ppg.FileGeneratingJob(of, func1)  # so we get the input we had previously!
        ppg.run()
        assert read("a") == "4"
        assert read("A") == "A"
        ppg.run()
        assert read("a") == "4"
        assert read("A") == "A"

    def test_simple_filegeneration_captures_stdout_stderr(self):
        of = "out/a"
        data_to_write = "hello"

        def do_write(of):
            op = open(of, "w")
            op.write(data_to_write)
            op.close()
            print("stdout is cool")
            sys.stderr.write("I am stderr")

        job = ppg.FileGeneratingJob(of, do_write)
        ppg.run()
        assert Path(of).exists()
        op = open(of, "r")
        data = op.read()
        op.close()
        assert data == data_to_write
        assert job.stdout == "stdout is cool\n"
        assert job.stderr == "I am stderr"  # no \n here

    def test_simple_filegeneration_captures_stdout_stderr_of_spawned_processe(self):
        of = "out/a"
        data_to_write = "hello"

        def do_write(of):
            import subprocess

            subprocess.check_call("echo stdout is cool", shell=True)
            subprocess.check_call("echo I am stderr>&2", shell=True)
            of.write_text("hello")

        job = ppg.FileGeneratingJob(of, do_write)
        ppg.run()
        assert Path(of).exists()
        op = open(of, "r")
        data = op.read()
        op.close()
        assert data == data_to_write
        assert job.stdout == "stdout is cool\n"
        assert job.stderr == "I am stderr\n"  # no \n here

    def test_simple_filegeneration_disabled_stdout_capture(self):
        of = "out/a"
        data_to_write = "hello"

        def do_write(of):
            write(of, data_to_write)
            print("stdout is cool")
            sys.stderr.write("I am stderr")

        job = ppg.FileGeneratingJob(of, do_write, always_capture_output=False)
        ppg.run()
        assert Path(of).exists()
        op = open(of, "r")
        data = op.read()
        op.close()
        assert data == data_to_write
        assert job.stdout == "not captured"
        assert job.stderr == "not captured"  # no \n here

    def test_simple_filegeneration_captures_stdout_stderr_giant_response(self, capsys):
        of = "out/a"

        def do_write(of):
            write(of, "hello")
            print("s" * (16 * 1024**2))
            sys.stderr.write("I" * (256 * 1024**2))

        job = ppg.FileGeneratingJob(of, do_write)
        with capsys.disabled():
            ppg.run()
        assert Path(of).exists()
        assert read(of) == "hello"
        assert job.stdout == "s" * (16 * 1024**2) + "\n"
        assert job.stderr == "I" * (256 * 1024**2)  # no \n here

    def test_simple_filegeneration_captures_stdout_stderr_failure(self):
        of = "out/a"
        data_to_write = "hello"

        def do_write(of):
            op = open(of, "w")
            op.write(data_to_write)
            op.close()
            print("stdout is cool")
            sys.stderr.write("I am stderr")
            raise ValueError()

        job = ppg.FileGeneratingJob(of, do_write)
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert Path(of).exists()
        op = open(of, "r")
        data = op.read()
        op.close()
        assert data == data_to_write
        assert job.stdout == "stdout is cool\n"
        assert job.stderr == "I am stderr"  # no \n here

    def test_filegeneration_does_not_change_mcp(self):
        global global_test
        global_test = 1
        of = "out/a"
        data_to_write = "hello"

        def do_write(of):
            write(of, data_to_write)
            global global_test
            global_test = 2

        ppg.FileGeneratingJob(of, do_write)
        ppg.run()
        assert global_test == 1

    def test_file_generation_chaining_simple(self):
        ofA = "out/a"

        def writeA(of):
            write(ofA, "Hello")

        jobA = ppg.FileGeneratingJob(ofA, writeA)
        ofB = "out/b"

        def writeB(of):
            op = open(ofB, "w")
            ip = open(ofA, "r")
            op.write(ip.read()[::-1])
            op.close()
            ip.close()

        jobB = ppg.FileGeneratingJob(ofB, writeB)
        jobB.depends_on(jobA)
        ppg.run()
        assert read(ofA) == read(ofB)[::-1]

    def test_file_generation_multicore(self):
        # one fork per FileGeneratingJob...
        import os

        ofA = "out/a"

        def writeA(of):
            write(ofA, "%i" % os.getpid())

        ofB = "out/b"

        def writeB(of):
            write(ofB, "%i" % os.getpid())

        ppg.FileGeneratingJob(ofA, writeA)
        ppg.FileGeneratingJob(ofB, writeB)
        ppg.run()
        assert read(ofA) != read(ofB)

    def test_filegenerating_two_jobs_same_file(self):
        ppg.new(run_mode=ppg.RunMode.NOTEBOOK)
        ppg.MultiFileGeneratingJob(
            ["out/A", "out/B"], lambda of: write("out/A", "hello")
        )
        with pytest.raises(ppg.JobOutputConflict):
            ppg.MultiFileGeneratingJob(
                ["out/A", "out/C"], lambda of: write("out/A", "world")
            )
        ppg.MultiFileGeneratingJob(
            ["out/C"], lambda of: write("out/A", "world")
        )  # that's ok, we are replacing out/C
        ppg.FileGeneratingJob(
            "out/C", lambda of: write("out/C", "C")
        )  # that's ok, we are replacing out/C (at least in relaxed /notebook mode)
        with pytest.raises(ValueError):  # just checking the inheritance
            ppg.MultiFileGeneratingJob(
                ["out/C", "out/D"], lambda of: write("out/A", "world")
            )  # note that this one does not replace out/C
        ppg.MultiFileGeneratingJob(
            ["out/D", "out/E"], lambda of: write("out/A", "world")
        )
        with pytest.raises(ppg.JobOutputConflict):
            ppg.FileGeneratingJob("out/D", lambda of: write("out/C", "C"))

    def test_multi_file_with_exing_files_rerun_to_capture_hashes(self):
        def callback(filenames):
            counter("counter")
            for f in filenames:
                f.write_text("hello")

        ppg.MultiFileGeneratingJob(["a", "b"], callback)
        Path("a").write_text("shu")
        Path("b").write_text("shu")
        ppg.run()
        assert read("a") == "hello"
        assert read("b") == "hello"
        assert read("counter") == "1"
        ppg.run()
        assert read("counter") == "1"

    def test_invaliding_removes_file(self):
        of = "out/a"
        sentinel = "out/b"

        def do_write(of):
            if Path(sentinel).exists():
                raise ValueError("second run")
            write(of, "shu")
            write(sentinel, "done")

        job = ppg.FileGeneratingJob(of, do_write, empty_ok=True)
        dep = ppg.ParameterInvariant(
            "my_params",
            {
                1,
            },
        )
        job.depends_on(dep)
        ppg.run()
        assert Path(of).exists()
        assert Path(sentinel).exists()

        ppg.new()
        job = ppg.FileGeneratingJob(of, do_write)
        dep = ppg.ParameterInvariant(
            "my_params", (2,)
        )  # same name ,changed params, job needs to rerun, but explodes...
        job.depends_on(dep)  # on average, half the mistakes are in the tests...
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert not (Path(of).exists())

    def test_passing_non_function(self):
        with pytest.raises(TypeError):
            ppg.FileGeneratingJob("out/a", "shu")

    def test_passing_non_string_as_jobid(self):
        with pytest.raises(TypeError):
            ppg.FileGeneratingJob(5, lambda of: 1)

    def test_exceptions_are_preserved(self):
        def shu(of):
            write("out/A", "A")
            write("out/Ay", "ax")
            raise IndexError("twenty-five")  # just some exception

        jobA = ppg.FileGeneratingJob("out/A", shu)

        with pytest.raises(ppg.JobsFailed):
            ppg.run()

        assert isinstance(
            ppg.global_pipegraph.last_run_result[jobA.job_id].error.args[0], IndexError
        )
        assert Path("out/A").exists()  #
        assert read("out/Ay") == "ax"  # but the job did run, right?

    def test_exceptions_are_preserved2(self):
        def shu(of):
            write("out/A", "A")
            write("out/Ay", "ax")
            raise TypeError("twenty-five")  # just some exception

        jobA = ppg.FileGeneratingJob("out/A", shu)

        with pytest.raises(ppg.JobsFailed):
            ppg.run()

        print(ppg.global_pipegraph.last_run_result[jobA.job_id].error)
        assert isinstance(
            ppg.global_pipegraph.last_run_result[jobA.job_id].error.args[0], TypeError
        )
        assert "twenty-five" in str(
            ppg.global_pipegraph.last_run_result[jobA.job_id].error
        )
        assert Path(
            "out/A"
        ).exists()  # should clobber the resulting files in this case - just a double check to test_invaliding_removes_file
        assert read("out/Ay") == "ax"  # but the job did run, right?


@pytest.mark.usefixtures("create_out_dir")
@pytest.mark.usefixtures("ppg2_per_test")
class TestMultiFileGeneratingJob:
    def test_basic(self):
        of = ["out/a", "out/b"]

        def do_write(of):
            for f in of:
                append(f, "shu")

        ppg.MultiFileGeneratingJob(of, do_write)
        ppg.run()
        for f in of:
            assert read(f) == "shu"
        ppg.new()
        ppg.MultiFileGeneratingJob(of, do_write)
        ppg.run()
        for f in of:
            assert read(f) == "shu"  # ie. job has net been rerun...
        # but if I now delete one...
        Path(of[0]).unlink()
        ppg.new()
        ppg.MultiFileGeneratingJob(of, do_write)
        ppg.run()
        assert read(of[0]) == "shu"
        assert (
            read(of[1]) == "shu"
        )  # Since that file was also deleted when MultiFileGeneratingJob was invalidated...

    def test_empty_raises(self):
        of = ["out/a", "out/b"]

        def do_write(of):
            for f in of:
                append(f, "shu")
            write(f, "")  # one is empty

        job = ppg.MultiFileGeneratingJob(of, do_write, empty_ok=False)
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert isinstance(
            ppg.global_pipegraph.last_run_result[job.job_id].error.args[0],
            ppg.JobContractError,
        )

    def test_empty_ok(self):
        of = ["out/a", "out/b"]

        def do_write(of):
            for f in of:
                append(f, "shu")
            write(f, "")  # one is empty

        ppg.MultiFileGeneratingJob(of, do_write, empty_ok=True)
        ppg.run()
        for f in of[:-1]:
            assert read(f) == "shu"
        assert read(of[-1]) == ""

    def test_exception_destroys_no_files(self):
        of = ["out/a", "out/b"]

        def do_write(of):
            for f in of:
                append(f, "shu")
            raise ValueError("explode")

        ppg.MultiFileGeneratingJob(of, do_write)
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        for f in of:
            assert Path(f).exists()

    def test_invalidation_removes_all_files(self):
        of = ["out/a", "out/b"]
        sentinel = "out/sentinel"  # hack so this one does something different the second time around...

        def do_write(of):
            if Path(sentinel).exists():
                raise ValueError("explode")
            write(sentinel, "shu")
            for f in of:
                append(f, "shu")

        ppg.MultiFileGeneratingJob(of, do_write).depends_on(
            ppg.ParameterInvariant("myparam", (1,))
        )
        ppg.run()
        for f in of:
            assert Path(f).exists()
        ppg.new()
        ppg.MultiFileGeneratingJob(of, do_write).depends_on(
            ppg.ParameterInvariant("myparam", (2,))
        )
        with pytest.raises(ppg.JobsFailed):
            ppg.run()  # since this should blow up
        for f in of:
            assert not (Path(f).exists())

    def test_passing_not_a_list_of_str(self):
        with pytest.raises(TypeError):
            ppg.MultiFileGeneratingJob("out/a", lambda of: 1)

    def test_passing_non_function(self):
        with pytest.raises(TypeError):
            ppg.MultiFileGeneratingJob(["out/a"], "shu")

    def test_exceptions_are_preserved(self):
        def shu(of):
            write("out/A", "A")
            write("out/B", "B")
            write("out/Az", "ax")
            raise IndexError("twenty-five")  # just some exception

        jobA = ppg.MultiFileGeneratingJob(["out/A", "out/B"], shu)

        with pytest.raises(ppg.JobsFailed):
            ppg.run()

        assert isinstance(
            ppg.global_pipegraph.last_run_result[jobA.job_id].error.args[0], IndexError
        )
        assert Path("out/A").exists()  # ppg1 difference
        assert Path("out/B").exists()  # ppg1 difference
        assert read("out/Az") == "ax"  # but the job did run, right?

    def raises_on_non_string_filnames(self):
        with pytest.raises(ValueError):
            ppg.MultiFileGeneratingJob(["one", 2], lambda of: write("out/A"))

    def test_raises_on_collision(self):
        with pytest.raises(ValueError):
            ppg.MultiFileGeneratingJob(["test1", "test2"], lambda of: 5)
            ppg.MultiFileGeneratingJob(["test2", "test3"], lambda of: 5)

    def test_duplicate_prevention(self):
        param = "A"
        ppg.FileGeneratingJob("out/A", lambda of: write("out/A", param))

        with pytest.raises(ValueError):
            ppg.MultiFileGeneratingJob(["out/A"], lambda of: write("out/A", param))

        ppg.new(run_mode=ppg.RunMode.NOTEBOOK)
        assert len(ppg.global_pipegraph.jobs) == 0
        ppg.FileGeneratingJob("out/A", lambda of: write("out/A", param))
        ppg.MultiFileGeneratingJob(["out/A"], lambda of: write("out/A", param))

    def test_non_str(self):
        param = "A"

        with pytest.raises(TypeError):
            ppg.MultiFileGeneratingJob([25], lambda of: write("out/A", param))

    def test_non_iterable(self):
        param = "A"
        try:
            ppg.MultiFileGeneratingJob(25, lambda of: write("out/A", param))
            assert not ("Exception not raised")
        except TypeError as e:
            print(e)
            assert "files was not iterable" in str(e)

    def test_single_stre(self):
        param = "A"

        with pytest.raises(TypeError):
            ppg.MultiFileGeneratingJob("A", lambda of: write("out/A", param))

    def test_order_of_files_is_kept_for_callback(self):
        def do_b(ofs):
            assert ofs[0].name == "b"
            assert ofs[1].name == "B1"
            ofs[0].write_text("B")
            ofs[1].write_text("B")

        ppg.MultiFileGeneratingJob(["b", "B1"], do_b)
        ppg.run()


test_modifies_shared_global = []
shared_value = ""


@pytest.mark.usefixtures("create_out_dir")
@pytest.mark.usefixtures("ppg2_per_test")
class TestDataLoadingJob:
    def test_modifies_worker(self):
        # global shared
        # shared = "I was the the global in the mcp"
        def load():
            test_modifies_shared_global.append("shared data")
            return ppg.UseInputHashesForOutput()

        of = "out/a"

        def do_write(of):
            write(
                of, "\n".join(test_modifies_shared_global)
            )  # this might actually be a problem when defining this?

        dlJo = ppg.DataLoadingJob("myjob", load)
        writejob = ppg.FileGeneratingJob(of, do_write)
        writejob.depends_on(dlJo)

        writejob2 = ppg.FileGeneratingJob(
            "out/b",
            lambda of: write("out/b", "b" + "\n".join(test_modifies_shared_global)),
        )
        writejob2.depends_on(dlJo)
        ppg.run()
        assert read(of) == "shared data"
        assert read("out/b") == "bshared data"

    def test_global_statement_works(self):
        # this currently does not work in the cloudpickle transmitted jobs -
        # two jobs refereing to global have different globals afterwards
        # or the 'global shared' does not work as expected after loading
        global shared_value
        shared_value = "I was the the global in the mcp"

        def load():
            global shared_value
            shared_value = "shared data"
            return ppg.UseInputHashesForOutput()

        of = "out/a"

        def do_write(of):
            write(of, shared_value)

        dlJo = ppg.DataLoadingJob("myjob", load)
        writejob = ppg.FileGeneratingJob(of, do_write)
        writejob.depends_on(dlJo)
        ppg.run()
        assert read(of) == "shared data"

    def test_does_not_get_run_without_dep_job(self):
        of = "out/shu"

        def load():
            counter(of)

        ppg.DataLoadingJob("myjob", load)
        ppg.run()
        assert not Path(of).exists()
        ppg.run()
        assert not Path(of).exists()
        ppg.new()
        ppg.DataLoadingJob("myjob", load)
        ppg.run()
        assert not Path(of).exists()

    def test_does_not_get_run_in_chain_without_final_dep(self):
        of = "out/shu"

        def load():
            counter(of)
            return ppg.UseInputHashesForOutput()

        job = ppg.DataLoadingJob("myjob", load)
        ofB = "out/sha"

        def loadB():
            counter(ofB)

        ppg.DataLoadingJob("myjobB", loadB).depends_on(job)
        ppg.run()
        assert not Path(of).exists()
        assert not Path(ofB).exists()
        ppg.run()
        assert not Path(of).exists()
        assert not Path(ofB).exists()

    def test_does_get_run_in_chain_all(self):
        of = "out/shu"

        def load():
            write(
                of, "shu"
            )  # not the fine english way, but we need a sideeffect that's checkable
            return ppg.UseInputHashesForOutput()

        job = ppg.DataLoadingJob("myjob", load)
        ofB = "out/sha"

        def loadB():
            write(ofB, "sha")
            return ppg.UseInputHashesForOutput()

        jobB = ppg.DataLoadingJob("myjobB", loadB).depends_on(job)
        ofC = "out/c"

        def do_write(of):
            write(ofC, ofC)

        ppg.FileGeneratingJob(ofC, do_write).depends_on(jobB)
        ppg.run()
        assert Path(of).exists()
        assert Path(ofB).exists()
        assert Path(ofC).exists()

    def test_chain_with_filegenerating_works(self):
        of = "out/a"

        def do_write(of):
            write(of, str(of))

        jobA = ppg.FileGeneratingJob(of, do_write)
        o = Dummy()

        def do_load():
            o.a = read(of)
            return ppg.UseInputHashesForOutput()

        jobB = ppg.DataLoadingJob("loadme", do_load).depends_on(jobA)
        ofB = "out/b"

        def write2(of):
            write(ofB, o.a)

        ppg.FileGeneratingJob(ofB, write2).depends_on(jobB)
        ppg.run()
        assert read(of) == of
        assert read(ofB) == of

    def test_does_get_run_depending_on_jobgenjob(self):
        of = "out/shu"

        def load():
            write(
                of, "shu"
            )  # not the fine english way, but we need a sideeffect that's checkable
            return ppg.UseInputHashesForOutput()

        job = ppg.DataLoadingJob("myjob", load)

        def gen():
            ofB = "out/b"

            def do_write(of):
                write(ofB, "hello")

            ppg.FileGeneratingJob(ofB, do_write)

        ppg.JobGeneratingJob("mygen", gen).depends_on(job)
        ppg.run()
        assert Path(of).exists()  # so the data loading job was run
        assert read("out/b") == "hello"  # and so was the jobgen and filegen job.

    def test_passing_non_function(self):
        with pytest.raises(TypeError):
            ppg.DataLoadingJob("out/a", "shu")

    def test_passing_non_string_as_jobid(self):
        with pytest.raises(TypeError):
            ppg.DataLoadingJob(5, lambda: 1)

    def test_failing_dataloading_jobs(self):
        o = Dummy()
        of = "out/A"

        def write(of):
            write(of, o.a)

        def load():
            o.a = "shu"
            raise ValueError()

        job_fg = ppg.FileGeneratingJob(of, write)
        job_dl = ppg.DataLoadingJob("doload", load)
        job_fg.depends_on(job_dl)
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert not (Path(of).exists())
        assert isinstance(
            ppg.global_pipegraph.last_run_result[job_dl.job_id].error.args[0],
            ValueError,
        )

    def test_prev_dataloading_jobs_not_done_if_there_is_a_non_dataloading_job_inbetween_that_is_done(
        self,
    ):
        # so, A = DataLoadingJob, B = FileGeneratingJob, C = DataLoadingJob, D = FileGeneratingJob
        # D.depends_on(C)
        # C.depends_on(B)
        # B.depends_on(A)
        # B is done.
        # D is not
        # since a would be loaded, and then cleaned up right away (because B is Done)
        # it should not be loaded again
        o = Dummy()

        def a():
            o.a = "A"
            append("out/A", "A")
            return ppg.UseInputHashesForOutput()

        def b(of):
            append("out/B", "B")

        def c():
            o.c = "C"
            append("out/C", "C")
            return ppg.UseInputHashesForOutput()

        def d(of):
            append("out/D", "D")

        jobA = ppg.DataLoadingJob("out/A", a, depend_on_function=False)
        jobB = ppg.FileGeneratingJob("out/B", b, depend_on_function=False)
        jobB.depends_on(jobA)
        ppg.run()
        assert read("out/A") == "A"
        assert read("out/B") == "B"

        jobC = ppg.DataLoadingJob("out/C", c, depend_on_function=False)
        jobD = ppg.FileGeneratingJob("out/D", d, depend_on_function=False)
        jobD.depends_on(jobC)
        jobC.depends_on(jobB)
        ppg.run()
        assert read("out/A") == "A"
        assert read("out/B") == "B"
        assert read("out/C") == "C"
        assert read("out/D") == "D"

    def test_sending_a_non_pickable_exception_data_loading(self):
        class UnpickableException(Exception):
            def __getstate__(self):
                raise ValueError("Can't pickle me")

        def load():
            raise UnpickableException()

        jobA = ppg.DataLoadingJob("out/A", load)
        jobB = ppg.FileGeneratingJob("out/B", lambda of: True)
        jobB.depends_on(jobA)

        with pytest.raises(ppg.JobsFailed):
            ppg.run()

        assert isinstance(
            ppg.global_pipegraph.last_run_result[jobA.job_id].error.args[0],
            UnpickableException,
        )

    def test_sending_a_non_pickable_exception_file_generating(self):
        class UnpickableException(Exception):
            def __getstate__(self):
                raise ValueError("Can't pickle me")

        def load(of):
            raise UnpickableException()

        jobB = ppg.FileGeneratingJob("out/B", load)

        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert isinstance(
            ppg.global_pipegraph.last_run_result[jobB.job_id].error.args[0],
            ppg.exceptions.JobDied,
        )

        assert isinstance(
            ppg.global_pipegraph.last_run_result[jobB.job_id].error.args[0].args[0], str
        )

        assert (
            "UnpickableException"
            in ppg.global_pipegraph.last_run_result[jobB.job_id].error.args[0].args[0]
        )

    def test_creating_jobs_in_file_generating_are_ignored(self):
        def load(of):
            ppg.global_pipegraph.new_jobs = (
                {}
            )  # just to see if we can reach the check in the resource coordinator!
            c = ppg.FileGeneratingJob("out/C", lambda of: write("out/C", "C"))
            write("out/A", "A")
            return [c]

        ppg.FileGeneratingJob("out/A", load)
        ppg.run()
        assert read("out/A") == "A"
        assert not Path("out/C").exists()

    def test_creating_jobs_in_data_loading(self):
        def load():
            from loguru import logger  # noqa:F401

            logger.info("in load")
            ppg.FileGeneratingJob(
                "out/C", lambda of: write("out/C", "C"), depend_on_function=False
            )
            return ppg.UseInputHashesForOutput()

        a = ppg.FileGeneratingJob(
            "out/A", lambda of: write("out/A", "A"), depend_on_function=False
        )
        b = ppg.DataLoadingJob("out/B", load, depend_on_function=False)
        a.depends_on(b)
        ppg.run()
        assert Path("out/C").exists()
        # it is ok to create jobs in dataloading .
        # ppg1 guarded against this, but it had to special case everything
        # around on-the-fly-jobs
        # the drawback here is that the DataLoadingJob might not run,
        # but perhaps that's just what you want.

    def test_accept_path_as_job_id(self):
        ppg.DataLoadingJob(Path("shu"), lambda: 55)

    def test_job_gen_does_not_clobber_history_of_input_jobs(self):
        a = ppg.FileGeneratingJob("a", lambda of: counter("A") and of.write_text("a"))
        b = ppg.JobGeneratingJob(
            "b",
            lambda: counter("B")
            and ppg.FileGeneratingJob(
                "c", lambda of: counter("C") and of.write_text("c")
            ),
        )
        b.depends_on(a)
        ppg.run()
        assert Path("a").read_text() == "a"
        assert Path("A").read_text() == "1"
        assert Path("B").read_text() == "1"
        assert Path("C").read_text() == "1"
        ppg.run()
        assert Path("a").read_text() == "a"
        assert Path("A").read_text() == "1"
        assert Path("B").read_text() == "2"
        assert Path("C").read_text() == "1"

    def test_dict_return(self):
        collector = []

        def gen():
            collector.append("a")
            return {"hello": 123, "world": "world"}

        a = ppg.DataLoadingJob("gen", gen)
        b = ppg.FileGeneratingJob("b", lambda of: of.write_text(collector[0]))
        b.depends_on(a)
        ppg.run()
        assert read("b") == "a"

    def test_upstream_leads_to_invalidation_if_dl_returns_none(self):
        store = {}

        ppg.new(run_mode=ppg.RunMode.NOTEBOOK) # something non-strict

        a = ppg.ParameterInvariant("A",'a')
        # note that the default parameter is necessary, 
        # otherwise teh 2nd invokation will have bound a content-different
        # object 'store'.
        def do_b(store=store):
            store['b'] = store.get('b', 0) + 1
            return ppg.UseInputHashesForOutput()
        b = ppg.DataLoadingJob('B', do_b)

        c = ppg.FileGeneratingJob('C', lambda of, store=store: of.write_text('c' + str(store['b'])))

        c.depends_on(b)
        b.depends_on(a)
        ppg.run()
        assert Path('C').read_text() == 'c1'
        ppg.run()
        assert Path('C').read_text() == 'c1'
        ppg.ParameterInvariant('A', 'a1')
        ppg.run()
        assert Path('C').read_text() == 'c2'



@pytest.mark.usefixtures("create_out_dir")
@pytest.mark.usefixtures("ppg2_per_test")
class TestAttributeJob:
    def test_basic_attribute_loading(self):
        o = Dummy()

        def load():
            return "shu"

        job = ppg.AttributeLoadingJob("load_dummy_shu", o, "a", load)
        of = "out/a"

        def do_write(of):
            write(of, o.a)

        ppg.FileGeneratingJob(of, do_write).depends_on(job)
        ppg.run()
        assert read(of) == "shu"

    def test_chained(self):
        o = Dummy()

        def load():
            return "shu"

        def load2():
            return o.a + "sha"

        job = ppg.AttributeLoadingJob("load_dummy_shu", o, "a", load)
        job2 = ppg.AttributeLoadingJob("load_dummy_shu2", o, "b", load2)
        of = "out/a"

        def do_write(of):
            write(of, o.b)

        ppg.FileGeneratingJob(of, do_write).depends_on(job2)
        job2.depends_on(job)
        ppg.run()
        assert read(of) == "shusha"

    def test_attribute_loading_does_not_affect_mcp(self):
        o = Dummy()

        def load():
            return "shu"

        job = ppg.AttributeLoadingJob("load_dummy_shu", o, "a", load)
        of = "out/a"

        def do_write(of):
            write(of, o.a)

        ppg.FileGeneratingJob(of, do_write).depends_on(job)
        ppg.run()
        assert read(of) == "shu"
        assert not (hasattr(o, "a"))

    def test_attribute_loading_does_not_run_without_dependency(self):
        o = Dummy()
        tf = "out/testfile"

        def load():
            counter(tf)
            return "shu"

        ppg.AttributeLoadingJob(
            "load_dummy_shu", o, "a", load, depend_on_function=False
        )
        ppg.run()
        assert not (hasattr(o, "a"))  # never assigned
        assert not Path("tf").exists()
        ppg.run()
        assert not Path("tf").exists()
        assert not (hasattr(o, "a"))

    def test_attribute_loading_does_run_without_dependency_if_invalidated(self):
        o = Dummy()
        tf = "out/testfile"

        def load():
            write(tf, "hello")
            return "shu"

        ppg.AttributeLoadingJob("load_dummy_shu", o, "a", load)
        ppg.run()
        assert not Path(tf).exists()
        assert not (hasattr(o, "a"))

    def test_attribute_disappears_after_direct_dependency(self):
        o = Dummy()
        job = ppg.AttributeLoadingJob("load_dummy_shu", o, "a", lambda: "shu")
        of = "out/A"

        def do_write(of):
            write(of, o.a)

        fgjob = ppg.FileGeneratingJob(of, do_write).depends_on(job)
        of2 = "out/B"

        def later_write(of2):
            raise ValueError()
            write(of2, o.a)

        # might be pure luck that this job runs after the cleanup
        ppg.FileGeneratingJob(of2, later_write).depends_on(fgjob)
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert read(of) == "shu"
        assert not (Path(of2).exists())

    def ppg1_test_attribute_disappears_after_direct_dependencies(self):
        """I can't get tihs test to run in ppg2 - the cleanup does happen,
        but this can't show it. It is now a job, so
        it 's not a given that it runs before B or C
        (actually, I believe test_attribute_disappears_after_direct_dependency
        only works by sheer accident as well?)

        """
        o = Dummy()
        job = ppg.AttributeLoadingJob("load_dummy_shu", o, "a", lambda: "shu")
        of = "out/A"

        def do_write(of):
            write(of, o.a)

        fgjob = ppg.FileGeneratingJob(of, do_write).depends_on(job)
        of2 = "out/B"

        def later_write(of2):
            write(of2, o.a)

        fgjobB = ppg.FileGeneratingJob(of2, later_write).depends_on(
            fgjob
        )  # now, this one does not depend on job, o it should not be able to access o.a
        of3 = "out/C"

        def also_write(of3):
            write(of3, o.a)

        fgjobC = ppg.FileGeneratingJob(of3, also_write).depends_on(job)
        fgjobB.depends_on(
            fgjobC
        )  # otherwise, B might be started C returned, and the cleanup will not have occurred!
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
            pass
        assert read(of) == "shu"
        assert read(of3) == "shu"
        assert not (Path(of2).exists())

    def test_passing_non_string_as_attribute(self):
        o = Dummy()

        with pytest.raises(ValueError):
            ppg.AttributeLoadingJob("out/a", o, 5, 55)

    def test_passing_non_function(self):
        o = Dummy()

        with pytest.raises(TypeError):
            ppg.AttributeLoadingJob("out/a", o, "a", 55)

    def test_passing_non_string_as_jobid(self):
        o = Dummy()

        with pytest.raises(TypeError):
            ppg.AttributeLoadingJob(5, o, "a", lambda: 55)

    def test_no_swapping_attributes_for_one_job(self):
        def cache():
            return list(range(0, 100))

        o = Dummy()
        ppg.AttributeLoadingJob("out/A", o, "a", cache)

        with pytest.raises(ppg.JobRedefinitionError):
            ppg.AttributeLoadingJob("out/A", o, "b", cache)

    def test_raises_on_non_string_attribute_name(self):
        with pytest.raises(ValueError):
            o = Dummy()
            ppg.AttributeLoadingJob("out/A", o, 23, lambda: 5)

    def test_raises_on_non_function_callback(self):
        with pytest.raises(ValueError):
            o = Dummy()
            ppg.AttributeLoadingJob("out/A", o, 23, 55)

    def test_no_swapping_objects_for_one_job(self):
        def cache():
            return list(range(0, 100))

        o = Dummy()
        o2 = Dummy()
        ppg.CachedAttributeLoadingJob("out/A", o, "a", cache)

        with pytest.raises(ppg.JobRedefinitionError):
            ppg.CachedAttributeLoadingJob("out/A", o2, "a", cache)

    def test_no_swapping_callbacks(self):
        o = Dummy()
        ppg.AttributeLoadingJob("out/A", o, "a", lambda: 55, depend_on_function=False)

        with pytest.raises(ppg.JobRedefinitionError):
            ppg.AttributeLoadingJob(
                "out/A", o, "a", lambda: 55 + 1, depend_on_function=False
            )

    def test_no_swapping_callbacks_cached(self):
        o = Dummy()
        ppg.CachedAttributeLoadingJob(
            "out/A", o, "a", lambda: 55, depend_on_function=False
        )

        with pytest.raises(ppg.JobRedefinitionError):
            ppg.CachedAttributeLoadingJob(
                "out/A", o, "a", lambda: 55 + 1, depend_on_function=False
            )

    def test_ignore_code_changes(self):
        def a():
            append("out/Aa", "A")
            return "5"

        o = Dummy()
        jobA = ppg.AttributeLoadingJob("out/A", o, "a", a, depend_on_function=False)
        jobB = ppg.FileGeneratingJob("out/B", lambda of: write("out/B", o.a))
        jobB.depends_on(jobA)
        ppg.run()
        assert read("out/Aa") == "A"
        assert read("out/B") == "5"
        ppg.new()

        def b():
            append("out/Aa", "B")
            return "5"

        jobA = ppg.AttributeLoadingJob("out/A", o, "a", b, depend_on_function=False)
        jobB = ppg.FileGeneratingJob("out/B", lambda of: write("out/B", o.a))
        jobB.depends_on(jobA)
        ppg.run()
        # not rerun
        assert read("out/Aa") == "A"
        assert read("out/B") == "5"

    def test_callback_must_be_callable(self):
        with pytest.raises(TypeError):
            o = Dummy()
            ppg.AttributeLoadingJob("load_dummy_shu", o, "a", "shu")

    def test_returning_none_raises(self):
        ppg.new(run_mode=ppg.RunMode.NOTEBOOK)
        o = Dummy()
        a = ppg.FileGeneratingJob("out/a", lambda of: counter("a") and write(of, "A"))
        b = ppg.FileGeneratingJob("out/b", lambda of: counter("b") and write(of, "B"))
        c = ppg.AttributeLoadingJob("o", o, "o", lambda: counter("c") and None)
        b.depends_on(a)
        a.depends_on_params("x")
        a.depends_on(c)
        b.depends_on(c)
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert ppg.global_pipegraph.last_run_result[c.job_id].outcome is ppg.enums.JobOutcome.Failed

    def test_returning_none_via_ues_input_hashes_for_output(self):
        ppg.new(run_mode=ppg.RunMode.NOTEBOOK)
        o = Dummy()
        a = ppg.FileGeneratingJob("out/a", lambda of: counter("a") and write(of, "A"))
        b = ppg.FileGeneratingJob("out/b", lambda of: counter("b") and write(of, "B"))
        c = ppg.AttributeLoadingJob("o", o, "o", lambda: counter("c") and ppg.UseInputHashesForOutput(None))
        b.depends_on(a)
        a.depends_on_params("x")
        a.depends_on(c)
        b.depends_on(c)
        ppg.run()
        assert read("a") == "1"
        assert read("b") == "1"
        assert read("c") == "1"
        c.depends_on_params("y")
        ppg.run()
        assert read("a") == "2"
        assert read("c") == "2"
        assert read("b") == "2"  # ppg2_rust
        ppg.run()
        assert read("a") == "2"
        assert read("c") == "2"
        assert read("b") == "2"  # ppg2_rust


@pytest.mark.usefixtures("create_out_dir")
@pytest.mark.usefixtures("ppg2_per_test")
class TestTempFileGeneratingJob:
    def test_basic(self):
        temp_file = "out/temp"

        def write_temp(of):
            write(temp_file, "hello")

        temp_job = ppg.TempFileGeneratingJob(temp_file, write_temp)
        ofA = "out/A"

        def write_A(of):
            write(ofA, read(temp_file))

        fgjob = ppg.FileGeneratingJob(ofA, write_A)
        fgjob.depends_on(temp_job)
        ppg.run()
        assert read(ofA) == "hello"
        assert not (Path(temp_file).exists())

    def test_does_not_get_return_if_output_is_done(self):
        temp_file = "out/temp"
        out_file = "out/A"
        count_file = "out/count"
        normal_count_file = "out/countA"

        def write_count(of):
            try:
                count = read(out_file)
                count = count[: count.find(":")]
            except IOError:
                count = "0"
            count = int(count) + 1
            write(out_file, str(count) + ":" + read(temp_file))
            append(normal_count_file, "A")

        def write_temp(of):
            write(temp_file, "temp")
            append(count_file, "X")

        jobA = ppg.FileGeneratingJob(out_file, write_count)
        jobTemp = ppg.TempFileGeneratingJob(temp_file, write_temp)
        jobA.depends_on(jobTemp)
        ppg.run()
        assert not (Path(temp_file).exists())
        assert read(out_file) == "1:temp"
        assert read(count_file) == "X"
        assert read(normal_count_file) == "A"
        # now, rerun. Tempfile has been deleted,
        # and should not be regenerated
        ppg.new()
        jobA = ppg.FileGeneratingJob(out_file, write_count)
        jobTemp = ppg.TempFileGeneratingJob(temp_file, write_temp)
        jobA.depends_on(jobTemp)
        ppg.run()
        assert not (Path(temp_file).exists())
        assert read(out_file) == "1:temp"
        assert read(count_file) == "X"
        assert read(normal_count_file) == "A"

    def test_does_not_get_return_if_output_is_not(self):
        temp_file = "out/temp"
        out_file = "out/A"
        count_file = "out/count"
        normal_count_file = "out/countA"

        def write_count(out_file):
            try:
                count = read(out_file)
                count = count[: count.find(":")]
            except IOError:
                count = "0"
            count = int(count) + 1
            write(out_file, str(count) + ":" + read(temp_file))
            append(normal_count_file, "A")

        def write_temp(of):
            write(temp_file, "temp")
            append(count_file, "X")

        jobA = ppg.FileGeneratingJob(out_file, write_count)
        jobTemp = ppg.TempFileGeneratingJob(temp_file, write_temp)
        jobA.depends_on(jobTemp)
        ppg.run()
        assert not (Path(temp_file).exists())
        assert read(out_file) == "1:temp"
        assert read(count_file) == "X"
        assert read(normal_count_file) == "A"
        # now, rerun. Tempfile has been deleted,
        # and should  be regenerated
        Path(out_file).unlink()
        ppg.new()
        jobA = ppg.FileGeneratingJob(out_file, write_count)
        jobTemp = ppg.TempFileGeneratingJob(temp_file, write_temp)
        jobA.depends_on(jobTemp)
        ppg.run()
        assert read(out_file) == "1:temp"  # since the outfile was removed...
        assert read(count_file) == "XX"
        assert read(normal_count_file) == "AA"
        assert not (Path(temp_file).exists())

    def test_dependand_explodes(self, job_trace_log):
        temp_file = "out/temp"

        def write_temp(of):
            append(of, "hello")

        temp_job = ppg.TempFileGeneratingJob(temp_file, write_temp)
        ofA = "out/A"

        def write_A(of):
            raise ValueError("shu")

        fgjob = ppg.FileGeneratingJob(ofA, write_A)
        fgjob.depends_on(temp_job)
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        # ppg.run()
        assert not (Path(ofA).exists())
        assert Path(temp_file).exists()

        ppg.new()

        def write_A_ok(ofA):
            write(ofA, read(temp_file))

        temp_job = ppg.TempFileGeneratingJob(temp_file, write_temp)
        fgjob = ppg.FileGeneratingJob(ofA, write_A_ok)
        fgjob.depends_on(temp_job)
        ppg.run()

        assert read(ofA) == "hello"  # tempfile job has not been rerun
        assert not (Path(temp_file).exists())  # and the tempfile has been removed...

    def test_removes_tempfile_on_exception(self):
        temp_file = "out/temp"

        def write_temp(of):
            write(temp_file, "hello")
            raise ValueError("should")

        temp_job = ppg.TempFileGeneratingJob(temp_file, write_temp)
        ofA = "out/A"

        def write_A(of):
            write(ofA, read(temp_file))

        fgjob = ppg.FileGeneratingJob(ofA, write_A)
        fgjob.depends_on(temp_job)
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert Path(temp_file).exists()
        assert not (Path(ofA).exists())

    def test_passing_non_function(self):
        with pytest.raises(TypeError):
            ppg.TempFileGeneratingJob("out/a", "shu")

    def test_passing_non_string_as_jobid(self):
        with pytest.raises(TypeError):
            ppg.TempFileGeneratingJob(5, lambda of: 1)

    def test_rerun_because_of_new_dependency_does_not_rerun_old(self):
        jobA = ppg.FileGeneratingJob(
            "out/A",
            lambda of: append("out/A", read("out/temp")) or append("out/Ab", "A"),
        )
        jobB = ppg.TempFileGeneratingJob("out/temp", lambda of: write("out/temp", "T"))
        jobA.depends_on(jobB)
        ppg.run()
        assert not (Path("out/temp").exists())
        assert read("out/A") == "T"
        assert read("out/Ab") == "A"  # ran once

        ppg.new()
        jobA = ppg.FileGeneratingJob(
            "out/A", lambda of: append("out/A", read("out/temp"))
        )
        jobB = ppg.TempFileGeneratingJob("out/temp", lambda of: write("out/temp", "T"))
        jobA.depends_on(jobB)
        jobC = ppg.FileGeneratingJob(
            "out/C", lambda of: append("out/C", read("out/temp"))
        )
        jobC.depends_on(jobB)
        ppg.run()
        assert not (Path("out/temp").exists())
        assert read("out/Ab") == "A"  # ran once, not rewritten
        assert read("out/C") == "T"  # a new file

    def test_chaining_multiple(self):
        jobA = ppg.TempFileGeneratingJob("out/A", lambda of: write("out/A", "A"))
        jobB = ppg.TempFileGeneratingJob(
            "out/B", lambda of: write("out/B", read("out/A") + "B")
        )
        jobC = ppg.TempFileGeneratingJob(
            "out/C", lambda of: write("out/C", read("out/A") + "C")
        )
        jobD = ppg.FileGeneratingJob(
            "out/D", lambda of: write("out/D", read("out/B") + read("out/C"))
        )
        jobD.depends_on(jobC)
        jobD.depends_on(jobB)
        jobC.depends_on(jobA)
        jobB.depends_on(jobA)
        ppg.run()
        assert read("out/D") == "ABAC"
        assert not (Path("out/A").exists())
        assert not (Path("out/B").exists())
        assert not (Path("out/C").exists())

    def test_chaining_multiple_differently(self):
        jobA = ppg.TempFileGeneratingJob("out/A", lambda of: write("out/A", "A"))
        jobB = ppg.TempFileGeneratingJob(
            "out/B", lambda of: write("out/B", read("out/A") + "B")
        )
        jobD = ppg.FileGeneratingJob(
            "out/D", lambda of: write("out/D", read("out/B") + "D")
        )
        jobE = ppg.FileGeneratingJob(
            "out/E", lambda of: write("out/E", read("out/B") + "E")
        )
        jobF = ppg.FileGeneratingJob(
            "out/F", lambda of: write("out/F", read("out/A") + "F")
        )
        jobD.depends_on(jobB)
        jobE.depends_on(jobB)
        jobB.depends_on(jobA)
        jobF.depends_on(jobA)
        ppg.run()
        assert read("out/D") == "ABD"
        assert read("out/E") == "ABE"
        assert read("out/F") == "AF"
        assert not (Path("out/A").exists())
        assert not (Path("out/B").exists())
        assert not (Path("out/C").exists())

    def test_rerun_because_of_new_dependency_does_not_rerun_old_chained(self):
        jobA = ppg.TempFileGeneratingJob("out/A", lambda of: write("out/A", "A"))
        jobB = ppg.TempFileGeneratingJob(
            "out/B", lambda of: write("out/B", read("out/A") + "B")
        )
        jobC = ppg.FileGeneratingJob(
            "out/C",
            lambda of: write("out/C", read("out/B") + "C") or append("out/Cx", "1"),
        )
        jobB.depends_on(jobA)
        jobC.depends_on(jobB)
        ppg.run()
        assert read("out/C") == "ABC"
        assert read("out/Cx") == "1"

        ppg.new()
        jobA = ppg.TempFileGeneratingJob("out/A", lambda of: write("out/A", "A"))
        jobB = ppg.TempFileGeneratingJob(
            "out/B", lambda of: write("out/B", read("out/A") + "B")
        )
        jobC = ppg.FileGeneratingJob(
            "out/C",
            lambda of: write("out/C", read("out/B") + "C") or append("out/Cx", "1"),
        )
        jobD = ppg.FileGeneratingJob(
            "out/D",
            lambda of: write("out/D", read("out/A") + "D") or append("out/Dx", "1"),
        )
        jobB.depends_on(jobA)
        jobC.depends_on(jobB)
        jobD.depends_on(jobA)
        ppg.run()
        assert read("out/D") == "AD"
        assert read("out/Dx") == "1"
        assert read("out/C") == "ABC"
        assert read("out/Cx") == "1"

        ppg.new()
        jobA = ppg.TempFileGeneratingJob(
            "out/A", lambda of: write("out/A", "a")
        )  # note changing function code!
        jobB = ppg.TempFileGeneratingJob(
            "out/B", lambda of: write("out/B", read("out/A") + "B")
        )
        jobC = ppg.FileGeneratingJob(
            "out/C",
            lambda of: write("out/C", read("out/B") + "C") or append("out/Cx", "1"),
        )
        jobD = ppg.FileGeneratingJob(
            "out/D",
            lambda of: write("out/D", read("out/A") + "D") or append("out/Dx", "1"),
        )
        jobB.depends_on(jobA)
        jobC.depends_on(jobB)
        jobD.depends_on(jobA)
        ppg.run()
        assert read("out/D") == "aD"
        assert read("out/Dx") == "11"  # both get rerun
        assert read("out/C") == "aBC"
        assert read("out/Cx") == "11"

    def test_cleanup_if_never_run(self):
        temp_file = "out/temp"

        def write_temp(of):
            write(temp_file, "hello")

        def write_a(of):
            write("A", "hello")

        temp_job = ppg.TempFileGeneratingJob(
            temp_file, write_temp, depend_on_function=False
        )
        ppg.FileGeneratingJob("A", write_a, depend_on_function=False)
        write_a("A")  # so the file is there!
        ppg.run()
        assert not (Path("out/temp").exists())
        ppg.new()
        write_temp(temp_file)
        assert Path("out/temp").exists()
        # this job never runs...
        ppg.TempFileGeneratingJob(temp_file, write_temp, depend_on_function=False)
        # temp_job.do_cleanup_if_was_never_run = True
        ppg.run()
        assert Path("out/temp").exists()  # no run, no cleanup

    def test_cleanup_if_fails(self, job_trace_log):
        def fail(of):
            of.write_text("hello")
            raise ValueError("thisisit")

        a = ppg.TempFileGeneratingJob("a", fail)
        b = ppg.FileGeneratingJob("b", lambda of: of.write_text(read("a")))
        b.depends_on(a)
        with pytest.raises(ppg.JobsFailed):
            b()
        assert Path("a").exists()  # cleanup does not run
        assert not Path("b").exists()
        assert "thisisit" in str(a.exception)
        assert "Upstream" in str(b.exception)

    def test_temp_ds_fail_not_rerun(self, job_trace_log):
        def tf(of):
            of.write_text("A")
            counter("tf")

        jtf = ppg.TempFileGeneratingJob("A", tf)

        def j(of):
            if counter("j") == "0":
                raise ValueError()
            of.write_text("J")

        jj = ppg.FileGeneratingJob("J", j)
        jj.depends_on(jtf)
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert Path("A").read_text() == "A"
        assert Path("tf").read_text() == "1"
        assert Path("j").read_text() == "1"
        assert not Path("J").exists()
        ppg.run()
        assert Path("J").read_text() == "J"
        assert Path("j").read_text() == "2"
        assert Path("tf").read_text() == "1"

    def test_file_already_presen(self):
        def doit(output_filename):
            counter("tf")
            output_filename.write_text("done")

        j = ppg.TempFileGeneratingJob(".ppg/deleteme", doit)
        j2 = ppg.FileGeneratingJob(
            ".ppg/shu", lambda of: counter("j2") and of.write_text("hello")
        )
        j2.depends_on(j)
        j2()
        assert Path("j2").read_text() == "1"
        assert Path("tf").read_text() == "1"

        Path(".ppg/shu").unlink()
        Path(".ppg/deleteme").write_text("done")
        j2()
        assert Path("tf").read_text() == "1"  # same hash, file present
        assert Path("j2").read_text() == "2"

    def test_adding_removing_outputs_does_not_trigger_downstreams_that_depend_on_the_unchanged_inputs(
        self,
    ):
        def do_a(ofs):
            counter("a")
            if isinstance(ofs, Path):
                ofs = [ofs]
            for of in ofs:
                of.write_text(of.name)

        jobA = ppg.MultiFileGeneratingJob(["A", "B"], do_a)
        jobC = ppg.FileGeneratingJob(
            "C", lambda of: counter("c") and of.write_text(of.name)
        )
        jobC.depends_on("A")  # note how this depends on the file "A", not the job jobA.
        ppg.run()
        assert Path("C").read_text() == "C"
        assert Path("c").read_text() == "1"

        ppg.new()
        jobA = ppg.MultiFileGeneratingJob(["A", "B", "D"], do_a)
        jobC = ppg.FileGeneratingJob(
            "C", lambda of: counter("c") and of.write_text(of.name)
        )
        jobC.depends_on("A")
        print("part2")
        ppg.run()
        assert Path("D").read_text() == "D"
        assert Path("C").read_text() == "C"
        assert Path("a").read_text() == "2"
        assert Path("c").read_text() == "1"

        # now let's take it out again.
        print("part3")
        ppg.new()
        jobA = ppg.MultiFileGeneratingJob(["A", "B"], do_a)
        jobC = ppg.FileGeneratingJob(
            "C", lambda of: counter("c") and of.write_text(of.name)
        )
        jobC.depends_on("A")
        ppg.run()
        assert Path("D").read_text() == "D"
        assert Path("C").read_text() == "C"
        assert Path("a").read_text() == "2"
        assert Path("c").read_text() == "1"

        # take one out & put one in *at the same time*
        ppg.new()
        jobA = ppg.MultiFileGeneratingJob(["A", "B", "E"], do_a)
        jobC = ppg.FileGeneratingJob(
            "C", lambda of: counter("c") and of.write_text(of.name)
        )
        jobC.depends_on("A")
        ppg.run()
        assert Path("D").read_text() == "D"
        assert Path("C").read_text() == "C"
        assert Path("a").read_text() == "3"
        assert Path("c").read_text() == "1"

        # now turn it into a FG
        ppg.new()
        jobA = ppg.FileGeneratingJob("A", do_a)
        jobC = ppg.FileGeneratingJob(
            "C", lambda of: counter("c") and of.write_text(of.name)
        )
        jobC.depends_on("A")
        ppg.run()
        assert Path("D").read_text() == "D"
        assert Path("C").read_text() == "C"
        assert Path("a").read_text() == "4"
        assert Path("c").read_text() == "1"

        # now turn it into back into a MFG
        ppg.new()
        ppg.MultiFileGeneratingJob(["A", "B"], do_a)
        jobC = ppg.FileGeneratingJob(
            "C", lambda of: counter("c") and of.write_text(of.name)
        )
        jobC.depends_on("A")
        ppg.run()
        assert Path("D").read_text() == "D"
        assert Path("C").read_text() == "C"
        assert Path("a").read_text() == "4"
        assert Path("c").read_text() == "1"


@pytest.mark.usefixtures("create_out_dir")
@pytest.mark.usefixtures("ppg2_per_test")
class TestMultiTempFileGeneratingJob:
    def test_basic(self):
        temp_files = ["out/temp", "out/temp2"]

        def write_temp(of):
            for temp_file in temp_files:
                write(temp_file, "hello")

        temp_job = ppg.MultiTempFileGeneratingJob(temp_files, write_temp)
        ofA = "out/A"

        def write_A(of):
            write(ofA, read(temp_files[0]) + read(temp_files[1]))

        fgjob = ppg.FileGeneratingJob(ofA, write_A)
        fgjob.depends_on(temp_job)
        ppg.run()
        assert read(ofA) == "hellohello"
        assert not (Path(temp_files[0]).exists())
        assert not (Path(temp_files[1]).exists())

    def test_basic_dependes_were_done(self):
        temp_files = ["out/temp", "out/temp2"]

        def write_temp(of):
            write("temp_sentinel", "one")
            for temp_file in temp_files:
                write(temp_file, "hello")

        temp_job = ppg.MultiTempFileGeneratingJob(
            temp_files, write_temp, depend_on_function=False
        )
        ofA = "out/A"

        def write_A(of):
            write(ofA, read(temp_files[0]) + read(temp_files[1]))

        fgjob = ppg.FileGeneratingJob(ofA, write_A, depend_on_function=False)
        fgjob.depends_on(temp_job)
        write(ofA, "two")
        ppg.run()
        assert (
            read(ofA) == "hellohello"
        )  # change from ppg1 - we rerun if we don't have a hash recorded
        assert Path("temp_sentinel").exists()

    def raises_on_non_string_filnames(self):
        with pytest.raises(ValueError):
            ppg.MultiTempFileGeneratingJob(["one", 2], lambda of: write("out/A"))

    def test_raises_on_collision(self):
        with pytest.raises(ppg.JobOutputConflict):
            ppg.MultiTempFileGeneratingJob(["test1", "test2"], lambda of: 5)
            ppg.MultiTempFileGeneratingJob(["test2", "test3"], lambda of: 5)

    def test_duplicate_prevention(self):
        param = "A"
        ppg.FileGeneratingJob("out/A", lambda of: write("out/A", param))

        with pytest.raises(ppg.JobRedefinitionError):
            ppg.MultiTempFileGeneratingJob(["out/A"], lambda of: write("out/A", param))

    def test_non_str(self):
        param = "A"

        with pytest.raises(TypeError):
            ppg.MultiTempFileGeneratingJob([25], lambda of: write("out/A", param))

    def test_non_iterable(self):
        param = "A"
        with pytest.raises(TypeError):
            ppg.MultiTempFileGeneratingJob(25, lambda of: write("out/A", param))

    def test_order_of_files_is_kept_for_callback(self):
        def do_b(ofs):
            assert ofs[0].name == "b"
            assert ofs[1].name == "B1"
            ofs[0].write_text("B")
            ofs[1].write_text("B")

        a = ppg.MultiTempFileGeneratingJob(["b", "B1"], do_b)
        b = ppg.FileGeneratingJob("c", lambda of: of.write_text("b"))
        b.depends_on(a)

        ppg.run()

    def test_no_reshash_on_fail_of_dep(self, mocker):
        def gen():
            def do_b(ofs):
                ofs[0].write_text("b")
                ofs[1].write_text("B1")

            b = ppg.MultiTempFileGeneratingJob(["b", "B1"], do_b)

            def do_c(of):
                raise ValueError()

            c = ppg.FileGeneratingJob("c", lambda of: do_c)
            c.depends_on(b)

        gen()
        spy = mocker.spy(ppg.hashers, "hash_file")
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert Path("b").exists()
        assert Path("B1").exists()
        assert not Path("c").exists()

        till_here = spy.call_count

        ppg.new()
        gen()
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        now = spy.call_count
        assert now == till_here


@pytest.mark.usefixtures("ppg2_per_test")
class TestNoDotDotInJobIds:
    def test_no_dot_dot(self):
        """all ../ must be resolved before it becomes a job id"""
        from unittest.mock import patch

        collector = set()
        org_dedup = ppg.jobs._dedup_job

        def collecting_dedup(cls, job_id):
            collector.add(job_id)
            return org_dedup(cls, job_id)

        with patch("pypipegraph2.jobs._dedup_job", collecting_dedup):
            j = ppg.MultiFileGeneratingJob(["something/../shu"], lambda of: 5)
            assert j.job_id in collector
            assert not ".." in j.job_id
            assert not "something/../shu" in collector
            collector.clear()

            j = ppg.FileGeneratingJob("something/../shu2", lambda of: 5)
            assert j.job_id in collector
            assert not ".." in j.job_id
            assert not "something/../shu2" in collector
            collector.clear()

            j = ppg.FileInvariant("something/../shu3")
            assert j.job_id in collector
            assert not ".." in j.job_id
            assert not "something/../shu3" in collector
            collector.clear()

            with pytest.raises(TypeError):  # we don't resolve function invariant names
                ppg.FunctionInvariant("something/../shu3b")

            j = ppg.TempFileGeneratingJob("something/../shu4", lambda of: 5)
            assert j.job_id in collector
            assert not ".." in j.job_id
            assert not "something/../shu4" in collector
            collector.clear()

            j = ppg.MultiTempFileGeneratingJob(["something/../shu5"], lambda of: 5)
            assert j.job_id in collector
            assert not ".." in j.job_id
            assert not "something/../shu4" in collector
            collector.clear()

            o = object()
            j = ppg.CachedAttributeLoadingJob("something/../shu6", o, "attr", lambda: 5)
            assert j.calc.job_id in collector
            assert j.load.job_id in collector
            assert not ".." in j.calc.job_id
            assert not ".." in j.load.job_id
            assert not "something/../shu6" in collector
            assert not "loadsomething/../shu6" in collector
            collector.clear()

            j = ppg.CachedDataLoadingJob(
                "something/../shu7", lambda: 5, lambda value: 5
            )
            assert j.calc.job_id in collector
            assert j.load.job_id in collector
            assert not ".." in j.calc.job_id
            assert not ".." in j.load.job_id
            assert not "something/../shu7" in collector
            collector.clear()

            j = ppg.PlotJob(
                "something/or_other/../shu.png", lambda: None, lambda data: None
            )
            assert j.plot.job_id in collector
            assert j.cache.calc.job_id in collector
            assert j.cache.load.job_id in collector
            assert j.table.job_id in collector
            for job_id in collector:
                assert not ".." in job_id
