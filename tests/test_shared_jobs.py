from pathlib import Path
from loguru import logger
import pytest
import pypipegraph2 as ppg
from .shared import write, read, counter


@pytest.mark.usefixtures("ppg2_per_test")
class TestSharedJob:
    def test_simple(self):
        def doit(output_files):
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

        fc = ppg.FileGeneratingJob("C", followup_c).depends_on(job)
        fd = ppg.FileGeneratingJob("D", followup_d).depends_on(job.files[1])

        ppg.run()
        assert read("out/done_no_input/a") == "a0"
        assert read("out/done_no_input/b") == "b"
        assert read("doit") == "1"
        assert read("C") == "a0b"
        assert read("D") == "b"
        assert read("c") == "1"
        assert read("d") == "1"

        ppg.run()
        assert read("doit") == "1"
        assert read("c") == "1"
        assert read("d") == "1"

        job.depends_on(ppg.ParameterInvariant("E", "e"))
        ppg.run()
        assert read("doit") == "2"
        assert read("c") == "2"
        assert read("d") == "1"
        assert read(job.files[0]) == "a1"
        assert read(job.files[1]) == "b"
        assert read("C") == "a1b"

        ppg.new()
        job = ppg.SharedMultiFileGeneratingJob(
            "out", ["a", "b"], doit, depend_on_function=False, remove_unused=False
        )
        fc = ppg.FileGeneratingJob("C", followup_c).depends_on(job)
        fd = ppg.FileGeneratingJob("D", followup_d).depends_on(job.files[1])

        job.depends_on(ppg.ParameterInvariant("E", "f"))
        ppg.run()
        assert read("doit") == "3"
        assert read("c") == "3"
        assert read("d") == "1"
        assert read(job.files[0]) == "a2"
        assert read(job.files[1]) == "b"
        assert read("C") == "a2b"
        assert (
            len(
                [
                    x
                    for x in Path("out").glob("*")
                    if x.is_dir() and x.name.startswith("done_")
                ]
            )
            == 3
        )

        job.remove_unused = True
        ppg.run()
        assert read("doit") == "3"
        # we had history, we didn't go there..
        assert (
            len(
                [
                    x
                    for x in Path("out").glob("*")
                    if x.is_dir() and x.name.startswith("done_")
                ]
            )
            == 3
        )
        ppg.global_pipegraph.get_history_filename().unlink()

        ppg.run()
        assert (
            len(
                [
                    x
                    for x in Path("out").glob("*")
                    if x.is_dir() and x.name.startswith("done_")
                ]
            )
            == 1
        )

    def test_multi_file_gen_job_lookup_colission(self):
        with pytest.raises(ValueError):
            ppg.MultiFileGeneratingJob({"a": "A", "b": "A"}, lambda of: None)
        with pytest.raises(ValueError):
            ppg.SharedMultiFileGeneratingJob(
                "shared", {"a": "A", "b": "A"}, lambda of: None
            )

    def test_subdirs(self):
        def doit(files):
            for f in files.values():
                f.parent.mkdir()
                f.write_text(f.name)

        job = ppg.SharedMultiFileGeneratingJob(
            "out", {"a": "a/a", "b": "b/b"}, doit, depend_on_function=False
        )
        assert "__never_placed_here__" in str(job["a"])
        # job()
        ppg.run()
        assert read(job["a"]) == "a"
        assert read(job["b"]) == "b"

    def test_nested(self):
        def doit(output_files):
            count = counter("doit")
            write(output_files[0], "a" + str(count))
            write(output_files[1], "b")

        def func_c(files):
            counter("C")
            files[0].write_text(read(job[0]) + "c")

        job = ppg.SharedMultiFileGeneratingJob(
            "out", ["a", "b"], doit, depend_on_function=False
        )
        c = ppg.SharedMultiFileGeneratingJob("out2", ["c"], func_c)
        c.depends_on(job)
        d = ppg.FileGeneratingJob(
            "d", lambda of: counter("d") and of.write_text(read(c[0]) + "d")
        ).depends_on(c)
        ppg.run()

        assert read("out/done_no_input/a") == "a0"
        assert read("out/done_no_input/b") == "b"
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

        assert read("out/done_no_input/a") == "a0"
        assert read("out/done_no_input/b") == "b"
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
        d = ppg.FileGeneratingJob(
            "d", lambda of: counter("d") and of.write_text(read(c[0]) + "d")
        ).depends_on(c)
        ppg.run()

        assert read("out/done_no_input/a") == "a0"
        assert read("out/done_no_input/b") == "b"
        assert read(c[0]) == "a0c"
        assert read("C") == "1"
        assert read("d") == "a0cd"

    def test_nuking_on_error(self):
        def doit(output_files):
            raise ValueError()

        a = ppg.SharedMultiFileGeneratingJob("out", ["a"], doit)
        with pytest.raises(ppg.RunFailed):
            a()
        assert not list(Path("out").glob("*"))
        a = ppg.SharedMultiFileGeneratingJob(
            "out", ["a"], doit, remove_build_dir_on_error=False
        )
        with pytest.raises(ppg.RunFailed):
            a()
        assert list(Path("out").glob("*"))

    def test_multiple_histories(self, job_trace_log):
        import json

        def doit(output_files):
            for of in output_files:
                assert not "no_input" in str(of)
            count = counter("doit")
            write(output_files[0], "a" + str(count))
            write(output_files[1], "b")

        job = ppg.SharedMultiFileGeneratingJob(
            "out", ["a", "b"], doit, depend_on_function=False
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

        ppg.new(history_dir=".my_history")
        job = ppg.SharedMultiFileGeneratingJob(
            "out", ["a", "b"], doit, depend_on_function=False
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

        ppg.new(history_dir=".my_history2")
        job = ppg.SharedMultiFileGeneratingJob(
            "out", ["a", "b"], doit, depend_on_function=False
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
        with open(job.output_dir_prefix / ".ppgs_using_this") as op:
            known = json.loads(op.read())
        assert str(h1.absolute()) in known
        assert str(h2.absolute()) in known
        assert str(h3.absolute()) in known
        assert known[str(h1.absolute())] == known[str(h2.absolute())]
        assert known[str(h1.absolute())] != known[str(h3.absolute())]
        key3 = known[str(h3.absolute())]
        assert (Path("out") / key3).exists()

        ppg.run()  # running again is harmless
        assert read("doit") == "2"
        assert read("A") == "3"

        logger.info("final")
        ppg.new(history_dir=".my_history2")
        job = ppg.SharedMultiFileGeneratingJob(
            "out", ["a", "b"], doit, depend_on_function=False
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
        with open(job.output_dir_prefix / ".ppgs_using_this") as op:
            known = json.loads(op.read())
        assert str(h3.absolute()) in known
        print(job.target_folder)
        print(known)
        assert (
            known[str(h3.absolute())] == known[str(h2.absolute())]
        )  # goes back to old value
        assert not (Path("out") / key3).exists()


    def test_simple_one_file(self):
        def doit(output_file):
            count = counter("doit")
            write(output_file[0], "a" + str(count))

        job = ppg.SharedMultiFileGeneratingJob(
            "out", ["a"], doit, depend_on_function=False, remove_unused=False,
            remove_build_dir_on_error=False
        )
        try:
            ppg.run()
        except Exception as e:
            print('stdout', job.stdout)
            print('stderr', job.stderr)
            raise
        assert read("out/done_no_input/a") == "a0"




