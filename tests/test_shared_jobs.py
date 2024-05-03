from pathlib import Path
import json
from loguru import logger
import pytest
import pypipegraph2 as ppg
from .shared import write, read, counter
import os


def get_known(job):
    known = {}
    for fn in (job.output_dir_prefix / "used_by").glob("*.source"):
        key = fn.read_text().strip()
        value = Path(os.readlink(fn.with_suffix(".uses"))).name
        known[key] = value
    return known


@pytest.mark.usefixtures("ppg2_per_test")
class TestSharedJob:
    def test_simple(self, job_trace_log):
        def doit(output_files, output_prefix):
            for of in output_files:
                assert not "no_input" in str(of)
            count = counter("doit")
            write(output_files[0], "a" + str(count))
            write(output_files[1], "b")

        job = ppg.SharedMultiFileGeneratingJob(
            "out", ["a", "b"], doit, depend_on_function=False, remove_unused=False
        )

        def followup_c(of):
            of.write_text(read(job.files[0]) + read(job.files[1]))
            counter("c")

        def followup_d(of):
            of.write_text(read(job.files[1]))
            counter("d")

        ppg.FileGeneratingJob("C", followup_c).depends_on(job)
        ppg.FileGeneratingJob("D", followup_d).depends_on(job.files[1])

        ppg.util.log_error("one")
        ppg.run()
        assert read(job.find_file("a")) == "a0"
        assert read(job.find_file("b")) == "b"
        assert read("doit") == "1"
        assert read("C") == "a0b"
        assert read("D") == "b"
        assert read("c") == "1"
        assert read("d") == "1"

        ppg.util.log_error("two")
        ppg.run()
        assert read("doit") == "1"
        assert read("c") == "1"
        assert read("d") == "1"

        job.depends_on(ppg.ParameterInvariant("E", "e"))
        ppg.util.log_error("three")
        ppg.run()
        assert read("doit") == "2"
        assert read("c") == "2"
        assert read("d") == "1"
        assert read(job.files[0]) == "a1"
        assert read(job.files[1]) == "b"
        assert read("C") == "a1b"

        ppg.new(log_level=6)
        job = ppg.SharedMultiFileGeneratingJob(
            "out", ["a", "b"], doit, depend_on_function=False, remove_unused=False
        )
        ppg.FileGeneratingJob("C", followup_c).depends_on(job)
        ppg.FileGeneratingJob("D", followup_d).depends_on(job.files[1])

        job.depends_on(ppg.ParameterInvariant("E", "f"))
        ppg.util.log_error("three")
        ppg.run()
        assert read("doit") == "3"
        assert read("c") == "3"
        assert read("d") == "1"
        assert read(job.files[0]) == "a2"
        assert read(job.files[1]) == "b"
        assert read("C") == "a2b"
        assert len(list(job.output_dir.glob("*"))) == 3  # three different outputs
        assert len(list(job.input_dir.glob("*"))) == 3  # three different outputs

        job.remove_unused = True
        ppg.util.log_error("four")
        assert read("doit") == "3"
        ppg.run()
        assert read("doit") == "3"

        # no build, have history -> no cleanup
        assert len(list(job.output_dir.glob("*"))) == 3  # three different outputs
        assert len(list(job.input_dir.glob("*"))) == 3  # three different outputs
        ppg.global_pipegraph.get_history_filename().unlink()
        ppg.run()
        assert (
            len(list(job.output_dir.glob("*"))) == 1
        )  # loosing history is reason for cleanup
        assert (
            len(list(job.input_dir.glob("*"))) == 1
        )  # loosing history is reason for cleanup

    def test_multi_file_gen_job_lookup_colission(self):
        with pytest.raises(ValueError):
            ppg.MultiFileGeneratingJob({"a": "A", "b": "A"}, lambda of: None)
        with pytest.raises(ValueError):
            ppg.SharedMultiFileGeneratingJob(
                "shared", {"a": "A", "b": "A"}, lambda of, prefix: None
            )

    def test_subdirs(self):
        def doit(files, prefix):
            for f in files.values():
                f.parent.mkdir()
                f.write_text(f.name)

        job = ppg.SharedMultiFileGeneratingJob(
            "out", {"a": "a/a", "b": "b/b"}, doit, depend_on_function=False
        )
        with pytest.raises(AttributeError):
            job["a"]  # can't get a filename *before* the job has run
        # assert "__never_placed_here__" in str(job["a"])
        # job()
        ppg.run()
        assert read(job["a"]) == "a"
        assert read(job["b"]) == "b"

    def test_nested(self):
        def doit(output_files, prefix):
            count = counter("doit")
            write(output_files[0], "a" + str(count))
            write(output_files[1], "b")

        def func_c(files, prefix):
            counter("C")
            files[0].write_text(read(job[0]) + "c")

        job = ppg.SharedMultiFileGeneratingJob(
            "out", ["a", "b"], doit, depend_on_function=False
        )
        c = ppg.SharedMultiFileGeneratingJob("out2", ["c"], func_c)
        c.depends_on(job)
        ppg.FileGeneratingJob(
            "d", lambda of: counter("d") and of.write_text(read(c[0]) + "d")
        ).depends_on(c)
        ppg.run()

        assert read(job.find_file("a")) == "a0"
        assert read(job.find_file("b")) == "b"
        assert read(c[0]) == "a0c"
        assert read("C") == "1"
        assert read("d") == "a0cd"

        ppg.new()
        job = ppg.SharedMultiFileGeneratingJob(
            "out", ["a", "b"], doit, depend_on_function=False
        )
        c = ppg.SharedMultiFileGeneratingJob("out2", ["c"], func_c)
        c.depends_on(job)
        d = ppg.FileGeneratingJob(
            "d", lambda of: counter("d") and of.write_text(read(c[0]) + "d")
        ).depends_on(c)
        ppg.run()
        assert read(job.find_file("a")) == "a0"
        assert read(job.find_file("b")) == "b"

        assert read(c[0]) == "a0c"
        assert read("C") == "1"
        assert read("d") == "a0cd"

        ppg.global_pipegraph.get_history_filename().unlink()
        ppg.new()

        job = ppg.SharedMultiFileGeneratingJob(
            "out", ["a", "b"], doit, depend_on_function=False
        )
        c = ppg.SharedMultiFileGeneratingJob("out2", ["c"], func_c)
        c.depends_on(job)
        ppg.FileGeneratingJob(
            "d", lambda of: counter("d") and of.write_text(read(c[0]) + "d")
        ).depends_on(c)
        # subprocess.check_call(["fd", "-L"])
        ppg.util.log_error("last before bookm")
        ppg.run()

        assert read(job.find_file("a")) == "a0"
        assert read(job.find_file("b")) == "b"

        assert read(c[0]) == "a0c"
        assert read("C") == "1"
        assert read("d") == "a0cd"

    def test_nuking_on_error(self):
        def doit(output_files, prefix):
            raise ValueError()

        a = ppg.SharedMultiFileGeneratingJob("out", ["a"], doit)
        with pytest.raises(ppg.JobsFailed):
            a()
        assert not list(Path("out/build").glob("*"))
        a = ppg.SharedMultiFileGeneratingJob(
            "out", ["a"], doit, remove_build_dir_on_error=False
        )
        with pytest.raises(ppg.JobsFailed):
            a()
        assert list(Path("out/build").glob("*"))

    def test_multiple_histories(self):
        def doit(output_files, prefix):
            for of in output_files:
                assert not "no_input" in str(of)
            count = counter("doit")
            write(output_files[0], "a" + str(count))
            write(output_files[1], "b")

        job = ppg.SharedMultiFileGeneratingJob(
            "out", ["a", "b"], doit, depend_on_function=False, remove_unused=False
        )
        ppg.FileGeneratingJob(
            "a",
            lambda of: counter("A") and of.write_text(str(of)),
            depend_on_function=False,
        ).depends_on(job)
        ppg.run()
        assert read("A") == "1"
        h1 = ppg.global_pipegraph.get_history_filename()
        assert read("doit") == "1"

        ppg.new(dir_config=ppg.DirConfig(history_dir=".my_history"))
        job = ppg.SharedMultiFileGeneratingJob(
            "out", ["a", "b"], doit, depend_on_function=False, remove_unused=False
        )
        ppg.FileGeneratingJob(
            "a",
            lambda of: counter("A") and of.write_text(str(of)),
            depend_on_function=False,
        ).depends_on(job)
        h2 = ppg.global_pipegraph.get_history_filename()
        assert h1.resolve() != h2.resolve()
        logger.info("2nd run")
        ppg.run()
        assert read("doit") == "1"
        assert (
            read("A") == "2"
        )  # changing the history dir obviously triggers a rerun to capture hashes.

        ppg.new(dir_config=ppg.DirConfig(history_dir=".my_history2"))
        job = ppg.SharedMultiFileGeneratingJob(
            "out", ["a", "b"], doit, depend_on_function=False, remove_unused=False
        )
        job.depends_on(ppg.ParameterInvariant("e", "e"))
        ppg.FileGeneratingJob(
            "a",
            lambda of: counter("A") and of.write_text(str(of)),
            depend_on_function=False,
        ).depends_on(job)
        h3 = ppg.global_pipegraph.get_history_filename()
        assert h3.resolve() != h2.resolve()
        ppg.run()
        assert read("doit") == "2"
        assert read("A") == "3"  # changed history dir...

        known = get_known(job)
        ha2 = ppg.SharedMultiFileGeneratingJob._handle_anysnake2

        assert ha2(str(h1.absolute())) in known
        assert ha2(str(h2.absolute())) in known
        assert ha2(str(h3.absolute())) in known
        assert known[ha2(str(h1.absolute()))] == known[ha2(str(h2.absolute()))]
        assert known[ha2(str(h1.absolute()))] != known[ha2(str(h3.absolute()))]
        key3 = known[ha2(str(h3.absolute()))]
        assert (Path("out/by_input") / key3).exists()

        ppg.run()  # running again is harmless
        assert read("doit") == "2"
        assert read("A") == "3"

        logger.info("final")
        ppg.new(dir_config=ppg.DirConfig(history_dir=".my_history2"))
        job = ppg.SharedMultiFileGeneratingJob(
            "out", ["a", "b"], doit, depend_on_function=False, remove_unused=False
        )
        ppg.FileGeneratingJob(
            "a",
            lambda of: counter("A") and of.write_text(str(of)),
            depend_on_function=False,
        ).depends_on(job)
        h4 = ppg.global_pipegraph.get_history_filename()
        assert h3.resolve() == h4.resolve()
        ppg.run()
        assert read("A") == "4"  # changed input
        known = get_known(job)
        assert ha2(str(h3.absolute())) in known
        print(job.target_folder)
        print(known)
        assert (
            known[ha2(str(h3.absolute()))] == known[ha2(str(h2.absolute()))]
        )  # goes back to old value
        assert not (Path("out") / key3).exists()

    def test_simple_one_file(self):
        def doit(output_file, prefix):
            count = counter("doit")
            write(output_file[0], "a" + str(count))

        job = ppg.SharedMultiFileGeneratingJob(
            "out",
            ["a"],
            doit,
            depend_on_function=False,
            remove_unused=False,
            remove_build_dir_on_error=False,
        )
        with pytest.raises(AttributeError):
            job.target_folder
        try:
            ppg.run()
        except Exception:
            print("stdout", job.stdout)
            print("stderr", job.stderr)
            raise
        assert read("out/by_input/no_input/a") == "a0"
        assert read("doit") == "1"
        ppg.run()
        assert read("out/by_input/no_input/a") == "a0"
        assert read("doit") == "1"
        before = job.target_folder
        job.depends_on(ppg.ParameterInvariant("B", "shu"))
        with pytest.raises(AttributeError):
            job.target_folder
        ppg.run()
        after = job.target_folder
        assert before != after

    def test_remove_and_keep_build_dir(self):
        def dofail(ofs, prefix):
            raise ValueError()

        jobKeep = ppg.SharedMultiFileGeneratingJob(
            "out",
            ["a"],
            dofail,
            depend_on_function=False,
            remove_unused=False,
            remove_build_dir_on_error=False,
        )
        jobTrash = ppg.SharedMultiFileGeneratingJob(
            "outB",
            ["a"],
            dofail,
            depend_on_function=False,
            remove_unused=False,
            remove_build_dir_on_error=True,
        )
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert jobKeep.target_folder.exists()
        assert not jobTrash.target_folder.exists()
        assert "/build/" in str(jobKeep.target_folder)

    def test_direct_call(self):
        def doit(output_file, prefix):
            count = counter("doit")
            write(output_file[0], "a" + str(count))

        job = ppg.SharedMultiFileGeneratingJob(
            "out",
            ["a"],
            doit,
            depend_on_function=False,
            remove_unused=False,
            remove_build_dir_on_error=False,
        )
        job()
        assert (job.target_folder / "a").read_text() == "a0"

    def test_shared_job_with_dict_file_def(self):
        def doit(output_files, prefix):
            counter("doit")
            write(output_files["a"], "file_a")

        job = ppg.SharedMultiFileGeneratingJob("out", {"a": "file_a"}, doit)
        ppg.run()
        assert read(job.find_file("a")) == "file_a"
        # assert read(job.find_file("file_a")) == "file_a"
        assert read("doit") == "1"

        ppg.new(dir_config=ppg.DirConfig(history_dir=".my_history"))
        job = ppg.SharedMultiFileGeneratingJob("out", {"b": "file_a"}, doit)
        ppg.run()
        assert read(job.find_file("b")) == "file_a"
        # assert read(job.find_file("file_a")) == "file_a"
        assert read(job["b"]) == "file_a"
        # assert read(job["file_a"]) == "file_a"
        with pytest.raises(KeyError):
            job["file_a"]
        with pytest.raises(KeyError):
            job["c"]
        assert read("doit") == "1"
        res = job()
        assert set(res.keys()) == {"b"}
        assert read(res["b"]) == "file_a"

    def test_shared_job_with_changing_inputs(self):
        ppg.new(dir_config=ppg.DirConfig(history_dir="history_one"))
        out = ["a"]

        def doit(output_files, prefix, out=out):
            counter("doit")
            output_files[0].write_text("A" + out[0])

        job = ppg.SharedMultiFileGeneratingJob("out", ["a"], doit, remove_unused=False)
        ppg.run()
        assert read("doit") == "1"
        assert read(job["a"]) == "Aa"
        with pytest.raises(KeyError):
            job["b"]
        job.depends_on_params("shu")
        ppg.run()
        assert read("doit") == "2"
        assert len(list(Path("out/done").glob("*"))) == 1
        assert read(job["a"]) == "Aa"
        out[0] = "b"
        known = get_known(job)
        assert len(known) == 1

        ppg.new(dir_config=ppg.DirConfig(history_dir="my_history"))
        job = ppg.SharedMultiFileGeneratingJob("out", ["a"], doit, remove_unused=False)
        job.depends_on_params("sha")
        job()
        assert read("doit") == "3"
        assert len(list(Path("out/done").glob("*"))) == 2
        assert read(job["a"]) == "Ab"

        # we had run the job, we had hashes, the output files do exist
        # thanks to running in another history,
        # so the job reruns, but doesn't recalc
        # and we get no new output dir
        # and we finally trigger the last case in .output_needed

        ppg.new(dir_config=ppg.DirConfig(history_dir="history_one"))
        job = ppg.SharedMultiFileGeneratingJob("out", ["a"], doit, remove_unused=False)
        job.depends_on_params("sha")
        job()
        assert read("doit") == "3"  # we do not rerun the code..
        assert len(list(Path("out/done").glob("*"))) == 2
        assert read(job["a"]) == "Ab"
        new_known = get_known(job)
        k = list(known.keys())[0]
        assert new_known[k] != known[k]
        assert len(new_known) == 2

    def test_violation(self):
        """Test that the detecting of non-detemernistic outputs,
        best efforts as it is,
        will at least capture the case where two builds run in parallel
        and differ in outpu
        """
        import time

        symlink = []

        def doit(output_files, prefix):
            r = counter("doit")
            output_files[0].write_text(str(time.time()))
            time.sleep(0.1)
            if (
                r == "1"
            ):  # sneakily restore the symlink. So we can trigger the violation case
                symlink[0].symlink_to(symlink[1])

        job = ppg.SharedMultiFileGeneratingJob("out", ["a"], doit)
        ppg.run()
        assert read("doit") == "1"

        # now take away the symlink so the alterante history has to run again
        sl = list(Path("out/by_input").glob("*"))
        assert len(sl) == 1
        symlink.append(sl[0])
        ol = list(Path("out/done").glob("*"))
        assert len(ol) == 1
        symlink.append(ol[0])
        os.unlink(sl[0])
        ppg.new(dir_config=ppg.DirConfig(history_dir="history_two"))
        job = ppg.SharedMultiFileGeneratingJob("out", ["a"], doit, remove_unused=False)
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert "two different outputs" in str(job.exception)
        assert read("doit") == "2"

    def test_some_files_missing(self):
        def doit(output_files, prefix):
            for f in output_files:
                f.write_text(f.name)

        job = ppg.SharedMultiFileGeneratingJob("out", ["a", "b"], doit)
        ppg.run()
        job["b"].unlink()
        # with pytest.raises(ppg.JobContractError) as excinfo:
        # ppg.run()
        # assert 'some result files' in str(excinfo.value)
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert "some result files" in str(job.exception)

        # second path to that exception
        ppg.new(dir_config=ppg.DirConfig(history_dir="history2"))
        job = ppg.SharedMultiFileGeneratingJob("out", ["a", "b"], doit)
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert "some result files" in str(job.exception)

    def test_non_symlink_in_folder(self):
        def doit(output_files, prefix):
            for f in output_files:
                f.write_text(f.name)

        job = ppg.SharedMultiFileGeneratingJob("out", ["a", "b"], doit)
        ppg.run()

        sl = list(Path("out/by_input").glob("*"))
        sl[0].unlink()
        sl[0].mkdir()
        (sl[0] / "a").write_text("a")
        (sl[0] / "b").write_text("a")
        with pytest.raises(ppg.JobsFailed):
            ppg.run()
        assert isinstance(job.exception, ppg.JobEvaluationFailed)

    def assert_no_rehashing(self):
        org_hash = ppg.hashers.hash_file
        hash_counter = [0]

        def fake_hash(*args):
            hash_counter[0] += 1
            return org_hash(*args)

        ppg.hashers.hash_file = fake_hash
        try:
            ppg.run()
        finally:
            ppg.hashers.hash_file = org_hash
        assert hash_counter[0] == 0

    def test_depends_on_job_gen(self):
        a = ppg.JobGeneratingJob("a", lambda: None)

        def doit(output_files, prefix):
            counter("doit")
            for f in output_files:
                f.write_text(f.name)

        b = ppg.SharedMultiFileGeneratingJob("out", ["a"], doit)
        b.depends_on(a)
        ppg.run()
        assert read("doit") == "1"

        self.assert_no_rehashing()
        assert read("doit") == "1"

    def test_depends_on_various(self):
        class Loader:
            pass

        loader = Loader()

        def doit(output_files, prefix):
            counter("doit")
            for f in output_files:
                f.write_text(f.name)

        def gen():
            job = ppg.SharedMultiFileGeneratingJob("out", ["a"], doit)
            Path("input_file").write_text("hello")
            job.depends_on_file("input_file")
            job.depends_on(ppg.DataLoadingJob("shu", lambda: 55))
            job.depends_on(
                ppg.AttributeLoadingJob("shao", loader, "shao", lambda: "shao")
            )
            job.depends_on(ppg.FileGeneratingJob("a", lambda of: of.write_text("a")))

        gen()
        ppg.run()
        self.assert_no_rehashing()

        # the job runs.
        # It just doesn't do the rebild

        # attribute loadin gjob, dataloadingjob, FileInvariant, MultiFileGeneratingJob

    def test_cleanup_multiple(self):
        def doit(output_files, prefix):
            count = str(counter("doit"))
            for f in output_files:
                f.write_text(f.name + count)

        job = ppg.SharedMultiFileGeneratingJob("out", ["a"], doit, remove_unused=False)
        ppg.run()
        job.depends_on(ppg.ParameterInvariant("b", "b"))
        ppg.run()
        job.depends_on(ppg.ParameterInvariant("c", "c"))
        ppg.run()
        assert len(list(Path("out/done").glob("*"))) == 3
        ppg.new()
        job = ppg.SharedMultiFileGeneratingJob("out", ["a"], doit, remove_unused=True)
        job.depends_on(ppg.ParameterInvariant("b", "b"))
        job.depends_on(ppg.ParameterInvariant("c", "c"))
        ppg.run()  # job does not run
        assert len(list(Path("out/done").glob("*"))) == 3
        job.depends_on(ppg.ParameterInvariant("d", "d"))
        ppg.run()
        assert len(list(Path("out/done").glob("*"))) == 1

    def test_local_log_usage(self):
        def load():
            fn = (
                ppg.global_pipegraph.dir_config.history_dir
                / ppg.SharedMultiFileGeneratingJob.log_filename
            )
            return json.loads(fn.read_text())

        def doit(output_files, prefix):
            count = str(counter("doit"))
            for f in output_files:
                f.write_text(f.name + count)

        job = ppg.SharedMultiFileGeneratingJob("out", ["a"], doit, remove_unused=False)
        ppg.run()
        known = load()
        assert (
            Path("out/by_input") / known[str(job.output_dir_prefix)] / "a"
        ).read_text() == "a0"
        job.depends_on_params("shu")
        ppg.run()
        known2 = load()
        assert (
            Path("out/by_input") / known2[str(job.output_dir_prefix)] / "a"
        ).read_text() == "a1"
        assert known != known2

    def test_more_files(self):
        def doit(output_files, prefix):
            count = str(counter("doit"))
            for f in output_files:
                f.write_text(f.name + count)

        a = ppg.SharedMultiFileGeneratingJob("out", ["a"], doit)
        ppg.run()
        assert a["a"].read_text() == "a0"
        assert Path("doit").read_text() == "1"
        ppg.run()
        assert a["a"].read_text() == "a0"
        assert Path("doit").read_text() == "1"
        ppg.new()
        a = ppg.SharedMultiFileGeneratingJob("out", ["a", "b"], doit)
        ppg.run()
        assert Path("doit").read_text() == "2"
        assert a["a"].read_text() == "a1"
        assert a["b"].read_text() == "b1"
        ppg.run()
        assert Path("doit").read_text() == "2"
        assert a["a"].read_text() == "a1"
        assert a["b"].read_text() == "b1"

        ppg.new()
        a = ppg.SharedMultiFileGeneratingJob("out", ["b"], doit)
        ppg.run()
        assert Path("doit").read_text() == "3"
        assert a["b"].read_text() == "b2"

    def test_same_output_dir_multple_jobs(self):
        def doit(output_files, prefix):
            count = str(counter("doit"))
            for f in output_files:
                f.write_text(f.name + count)

        ppg.SharedMultiFileGeneratingJob("out", ["a"], doit)
        with pytest.raises(ppg.JobOutputConflict):
            ppg.SharedMultiFileGeneratingJob("out", ["b"], doit)

    def test_file_order(self):
        def doit(ofs, prefix):
            print(ofs)
            assert ofs[0].name == "b"
            assert ofs[1].name == "B1"
            ofs[0].write_text("B")
            ofs[1].write_text("B")

        ppg.SharedMultiFileGeneratingJob("out", ["b", "B1"], doit)
        ppg.run()
