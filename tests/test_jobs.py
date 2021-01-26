from pathlib import Path
import sys
import pytest
import pypipegraph2 as ppg
from .shared import write, read, Dummy, append, counter

global_test = 0


@pytest.mark.usefixtures("ppg_per_test")
class TestJobs:
    def test_assert_singletonicity_of_jobs(self):
        ppg.new()
        of = "out/a"
        data_to_write = "hello"

        def do_write(of):
            write(of, data_to_write)

        job = ppg.FileGeneratingJob(of, do_write)
        job2 = ppg.FileGeneratingJob(of, do_write)
        assert job is job2 # change from ppg1

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


@pytest.mark.usefixtures("ppg_per_test")
class TestJobs2:
    def test_str(self):
        a = ppg.FileGeneratingJob("out/A", lambda of: write("out/A", "hello"))
        assert isinstance(str(a), str)

        a = ppg.ParameterInvariant("out/A", "hello")
        assert isinstance(str(a), str)

        a = ppg.JobGeneratingJob("out/Ax", lambda: "hello")
        assert isinstance(str(a), str)


@pytest.mark.usefixtures("create_out_dir")
@pytest.mark.usefixtures("ppg_per_test")
class TestFileGeneratingJob:
    def test_basic(self):
        of = "out/a"
        data_to_write = "hello"

        def do_write(ofof):
            print("do_write was called")
            write(of, data_to_write)

        job = ppg.FileGeneratingJob(of, do_write, depend_on_function=False)
        res = ppg.run()
        assert res[job.job_id].state == ppg.enums.JobState.Executed
        assert Path(of).exists()
        assert read(of) == data_to_write

    def test_cores_needed(self, job_trace_log):
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

    def test_cores_needed2(self, job_trace_log):
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

    def test_misspecified_job_does_not_hang_graph(self, job_trace_log):
        # this occurred during development
        ppg.new(cores=3)

        for i in range(1):
            j = ppg.FileGeneratingJob(
                "out/%i" % i,
                lambda i=i: write("out/%i" % i, "b"),
                depend_on_function=False,
            )

        with pytest.raises(ppg.RunFailed):
            ppg.run()
        assert "a number is required, not PosixPath" in str(
            ppg.global_pipegraph.last_run_result["out/0"].error
        )

    def test_job_not_creating_its_file(self, job_trace_log):
        # this occurred during development
        ppg.new(cores=3)

        for i in range(1):
            j = ppg.FileGeneratingJob(
                "out/%i" % i,
                lambda of, i=i: write("shu", "b"),
                depend_on_function=False,
            )

        with pytest.raises(ppg.RunFailed):
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

        with pytest.raises(ppg.RunFailed):
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
        with pytest.raises(ppg.RunFailed):
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

        job = ppg.FileGeneratingJob(of, do_write)
        with pytest.raises(ppg.RunFailed):
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
        func1 = lambda of: counter("a") and write(of, "A")
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
        with pytest.raises(ppg.RunFailed):
            ppg.run()
        assert read("a") == "2"
        assert read("A") == "B"
        with pytest.raises(ppg.RunFailed):
            ppg.run()
        assert read("a") == "3"
        assert read("A") == "B"

        job = ppg.FileGeneratingJob(of, func1)  # so we get the input we had previously!
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
            print("s" * (16 * 1024 ** 2))
            sys.stderr.write("I" * (256 * 1024 ** 2))

        job = ppg.FileGeneratingJob(of, do_write)
        with capsys.disabled():
            ppg.run()
        assert Path(of).exists()
        assert read(of) == "hello"
        assert job.stdout == "s" * (16 * 1024 ** 2) + "\n"
        assert job.stderr == "I" * (256 * 1024 ** 2)  # no \n here

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
        with pytest.raises(ppg.RunFailed):
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
        with pytest.raises(ppg.JobOutputConflict) as excinfo:
            ppg.FileGeneratingJob("out/D", lambda of: write("out/C", "C"))

    def test_invaliding_removes_file(self):
        of = "out/a"
        sentinel = "out/b"

        def do_write(of):
            if Path(sentinel).exists():
                raise ValueError("second run")
            write(of, "shu")
            write(sentinel, "done")

        job = ppg.FileGeneratingJob(of, do_write, empty_ok=True)
        dep = ppg.ParameterInvariant("my_params", (1,))
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
        with pytest.raises(ppg.RunFailed):
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

        with pytest.raises(ppg.RunFailed):
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

        with pytest.raises(ppg.RunFailed):
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
@pytest.mark.usefixtures("ppg_per_test")
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
        with pytest.raises(ppg.RunFailed):
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
        with pytest.raises(ppg.RunFailed):
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
        with pytest.raises(ppg.RunFailed):
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

        with pytest.raises(ppg.RunFailed):
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


test_modifies_shared_global = []
shared_value = ""


@pytest.mark.usefixtures("create_out_dir")
@pytest.mark.usefixtures("ppg_per_test")
class TestDataLoadingJob:
    def test_modifies_worker(self):
        # global shared
        # shared = "I was the the global in the mcp"
        def load():
            test_modifies_shared_global.append("shared data")

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
        assert read(of) == "1"  # runs once to capture the hashes, right?
        ppg.run()
        assert read(of) == "1"  # but does not run again
        ppg.new()
        ppg.DataLoadingJob("myjob", load)
        ppg.run()

    def test_does_not_get_run_in_chain_without_final_dep(self):
        of = "out/shu"

        def load():
            counter(of)

        job = ppg.DataLoadingJob("myjob", load)
        ofB = "out/sha"

        def loadB():
            counter(ofB)

        ppg.DataLoadingJob("myjobB", loadB).depends_on(job)
        ppg.run()
        assert read(of) == "1"
        assert read(ofB) == "1"
        ppg.run()
        assert read(of) == "1"
        assert read(ofB) == "1"

    def test_does_get_run_in_chain_all(self):
        of = "out/shu"

        def load():
            write(
                of, "shu"
            )  # not the fine english way, but we need a sideeffect that's checkable

        job = ppg.DataLoadingJob("myjob", load)
        ofB = "out/sha"

        def loadB():
            write(ofB, "sha")

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
        with pytest.raises(ppg.RunFailed):
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

        def b(of):
            append("out/B", "B")

        def c():
            o.c = "C"
            append("out/C", "C")

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

        with pytest.raises(ppg.RunFailed):
            ppg.run()

        assert isinstance(
            ppg.global_pipegraph.last_run_result[jobA.job_id].error.args[0],
            UnpickableException,
        )

    def test_sending_a_non_pickable_exception_file_generating(self, job_trace_log):
        class UnpickableException(Exception):
            def __getstate__(self):
                raise ValueError("Can't pickle me")

        def load(of):
            raise UnpickableException()

        jobB = ppg.FileGeneratingJob("out/B", load)

        with pytest.raises(ppg.RunFailed):
            ppg.run()
        assert isinstance(
            ppg.global_pipegraph.last_run_result[jobB.job_id].error.args[0], ppg.exceptions.JobDied
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

    def test_creating_jobs_in_data_loading(self, job_trace_log):
        def load():
            from loguru import logger  # noqa:F401

            logger.info("in load")
            ppg.FileGeneratingJob(
                "out/C", lambda of: write("out/C", "C"), depend_on_function=False
            )

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


@pytest.mark.usefixtures("create_out_dir")
@pytest.mark.usefixtures("ppg_per_test")
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

    def test_attribute_loading_does_not_run_without_dependency(self, job_trace_log):
        o = Dummy()
        tf = "out/testfile"

        def load():
            write(tf, "hello")
            return "shu"

        ppg.AttributeLoadingJob("load_dummy_shu", o, "a", load, depend_on_function=False)
        ppg.run()
        assert not (hasattr(o, "a"))
        assert not (Path(tf).exists())

    def test_attribute_loading_does_run_without_dependency_if_invalidated(self, job_trace_log):
        o = Dummy()
        tf = "out/testfile"

        def load():
            write(tf, "hello")
            return "shu"

        ppg.AttributeLoadingJob("load_dummy_shu", o, "a", load)
        ppg.run()
        assert (Path(tf).exists())
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
        with pytest.raises(ppg.RunFailed):
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
        with pytest.raises(ppg.RunFailed):
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

    def test_ignore_code_changes(self, job_trace_log):
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


@pytest.mark.usefixtures("create_out_dir")
@pytest.mark.usefixtures("ppg_per_test")
class TestTempFileGeneratingJob:
    def test_basic(self):
        temp_file = "out/temp"

        def write_temp(of):
            write(temp_file, "hello")

        temp_job = ppg.TempFileGeneratingJob(temp_file, write_temp)
        assert temp_job.is_temp_job
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

    def test_does_not_get_return_if_output_is_not(self, job_trace_log):
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

    def test_dependand_explodes(self):
        temp_file = "out/temp"

        def write_temp(of):
            append(temp_file, "hello")

        temp_job = ppg.TempFileGeneratingJob(temp_file, write_temp)
        ofA = "out/A"

        def write_A(of):
            raise ValueError("shu")

        fgjob = ppg.FileGeneratingJob(ofA, write_A)
        fgjob.depends_on(temp_job)
        with pytest.raises(ppg.RunFailed):
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
        with pytest.raises(ppg.RunFailed):
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

    def test_cleanup_if_never_run(self, job_trace_log):
        temp_file = "out/temp"

        def write_temp(of):
            write(temp_file, "hello")

        def write_a(of):
            write("A", "hello")

        temp_job = ppg.TempFileGeneratingJob(
            temp_file, write_temp, depend_on_function=False
        )
        jobA = ppg.FileGeneratingJob("A", write_a, depend_on_function=False)
        write_a("A")  # so the file is there!
        ppg.run()
        assert not (Path("out/temp").exists())
        ppg.new()
        write_temp(temp_file)
        assert Path("out/temp").exists()
        # this job never runs...
        temp_job = ppg.TempFileGeneratingJob(
            temp_file, write_temp, depend_on_function=False
        )
        # temp_job.do_cleanup_if_was_never_run = True
        ppg.run()
        assert Path("out/temp").exists()  # no run, no cleanup


@pytest.mark.usefixtures("create_out_dir")
@pytest.mark.usefixtures("ppg_per_test")
class TestMultiTempFileGeneratingJob:
    def test_basic(self):
        temp_files = ["out/temp", "out/temp2"]

        def write_temp(of):
            for temp_file in temp_files:
                write(temp_file, "hello")

        temp_job = ppg.MultiTempFileGeneratingJob(temp_files, write_temp)
        assert temp_job.is_temp_job
        ofA = "out/A"

        def write_A(of):
            write(ofA, read(temp_files[0]) + read(temp_files[1]))

        fgjob = ppg.FileGeneratingJob(ofA, write_A)
        fgjob.depends_on(temp_job)
        ppg.run()
        assert read(ofA) == "hellohello"
        assert not (Path(temp_files[0]).exists())
        assert not (Path(temp_files[1]).exists())

    def test_basic_dependes_were_done(self, job_trace_log):
        temp_files = ["out/temp", "out/temp2"]

        def write_temp(of):
            write("temp_sentinel", "one")
            for temp_file in temp_files:
                write(temp_file, "hello")

        temp_job = ppg.MultiTempFileGeneratingJob(
            temp_files, write_temp, depend_on_function=False
        )
        assert temp_job.is_temp_job
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
