from pathlib import Path
import sys
import pytest
import pypipegraph2 as ppg
from .shared import write, read, Dummy, append, counter
import os
import subprocess


@pytest.mark.usefixtures("ppg2_per_test")
class TestExternalJobs:
    def test_basic(self):
        job = ppg.ExternalJob(
            "one",
            {"two": "two.txt"},
            ["bash", "-c", "echo hello; echo 42 > two.txt; echo hello_err >&2"],
        )
        ppg.run()
        assert job["two"].read_text().strip() == "42"
        assert job["stdout"].read_text().strip() == "hello"
        assert job["stderr"].read_text().strip() == "hello_err"

    def test_call_before_after(self):
        job = ppg.ExternalJob(
            "one",
            {"two": "two.txt", "three": "three.txt"},
            ["bash", "-c", "echo hello;  echo hello_err >&2"],
            call_before=lambda job: write(job["two"], "42"),
            call_after=lambda job: write(
                job["three"], "43-" + job["two"].read_text().strip()
            ),
        )
        ppg.run()
        assert job["two"].read_text().strip() == "42"
        assert job["three"].read_text().strip() == "43-42"
        assert job["stdout"].read_text().strip() == "hello"
        assert job["stderr"].read_text().strip() == "hello_err"

    def test_basic_command_callback_and_prefix(self):
        job = ppg.ExternalJob(
            "one",
            {"two": "two.txt"},
            lambda: ["bash", "-c", "echo hello; echo 42 > two.txt; echo hello_err >&2"],
            std_prefix="hello_",
        )
        ppg.run()
        assert job["two"].read_text().strip() == "42"
        assert job["stdout"].read_text().strip() == "hello"
        assert job["stderr"].read_text().strip() == "hello_err"
        assert (
            job["cmd"].read_text().strip()
            == "bash -c 'echo hello; echo 42 > two.txt; echo hello_err >&2'"
        )
        assert job["stdout"].name.startswith("hello_")
        assert job["stderr"].name.startswith("hello_")

    def test_allowed_return_codes(self):
        job = ppg.ExternalJob(
            "one",
            {},
            ["bash", "-c", "echo hello; echo hello_err >&2; exit 42"],
            # test_allowed_return_codes=[42],
        )
        with pytest.raises(ppg.JobsFailed):
            ppg.run()

        ppg.new()
        job = ppg.ExternalJob(
            "one",
            {},
            ["bash", "-c", "echo hello; echo hello_err >&2; exit 0"],
            allowed_return_codes=[42],
        )
        with pytest.raises(ppg.JobsFailed):
            ppg.run()

        ppg.new()
        job = ppg.ExternalJob(
            "one",
            {},
            ["bash", "-c", "echo hello; echo hello_err >&2; exit 42"],
            allowed_return_codes=[42],
        )
        ppg.run()
        assert job["stdout"].read_text().strip() == "hello"
        assert job["stderr"].read_text().strip() == "hello_err"

    def test_cwd(self):
        job = ppg.ExternalJob(
            "one",
            {},
            ["bash", "-c", "echo hello; echo 42 > two.txt; echo hello_err >&2"],
            cwd=Path(".").absolute(),
        )
        ppg.run()
        assert Path("two.txt").read_text().strip() == "42"
        assert job["stdout"].read_text().strip() == "hello"
        assert job["stderr"].read_text().strip() == "hello_err"

    def test_ExternalOutputPath(self):
        job = ppg.ExternalJob(
            "one",
            {"two": "two.txt"},
            ["touch", ppg.ExternalOutputPath("two.txt")],
            cwd=Path(".").absolute(),
        )
        ppg.run()
        assert job["two"].exists()
        assert job["stdout"].read_text().strip() == ""
        assert job["stderr"].read_text().strip() == ""

    def test_start_new_session(self):
        """Test that start_new_session really starts a new session."""
        job1 = ppg.ExternalJob(
            "no_new_session",
            {"session_info": "session.txt"},
            ["bash", "-c", "ps -o sess= -p $$ > session.txt"],
            start_new_session=False,
        )

        job2 = ppg.ExternalJob(
            "with_new_session",
            {"session_info": "session.txt"},
            ["bash", "-c", "ps -o sess= -p $$ > session.txt"],
            start_new_session=True,
        )
        our_session = (
            subprocess.check_output(["ps", "-o", "sess=", "-p", str(os.getpid())])
            .decode()
            .strip()
        )

        ppg.run()

        assert our_session == job1["session_info"].read_text().strip()
        assert our_session != job2["session_info"].read_text().strip()
        assert (
            job1["session_info"].read_text().strip()
            != job2["session_info"].read_text().strip()
        )
        
    def test_trigger_rebuild_on_change(self):
        """Test that changes to command, call_before, or call_after trigger a rebuild."""
        # Set up counter to track rebuilds
        c = counter()
        
        # Test command changes
        output_path = Path("rebuild_test_cmd")
        def cmd_func():
            c.count()
            return ["echo", str(c.count)]
        
        job1 = ppg.ExternalJob(
            output_path,
            {},
            cmd_func
        )
        ppg.run()
        assert c.count == 1
        cmd_value1 = job1["cmd"].read_text()
        
        # Change the output of the command function
        c.count += 1
        ppg.run()
        assert c.count == 3  # Called once more during depend_on hash calc, once for execution
        cmd_value2 = job1["cmd"].read_text()
        assert cmd_value1 != cmd_value2
        
        # Test call_before changes
        c.reset()
        output_path = Path("rebuild_test_before")
        def before_func(job):
            c.count()
            write(job["out"], f"before-{c.count}")
        
        job2 = ppg.ExternalJob(
            output_path,
            {"out": "out.txt"},
            ["echo", "test"],
            call_before=before_func
        )
        ppg.run()
        assert c.count == 1
        out_value1 = job2["out"].read_text()
        
        # Change the call_before function
        def before_func_modified(job):
            c.count()
            write(job["out"], f"modified-{c.count}")
        
        job2 = ppg.ExternalJob(
            output_path,
            {"out": "out.txt"},
            ["echo", "test"],
            call_before=before_func_modified
        )
        ppg.run()
        assert c.count > 1  # Should have run again
        out_value2 = job2["out"].read_text()
        assert out_value1 != out_value2
        assert "modified" in out_value2
        
        # Test call_after changes
        c.reset()
        output_path = Path("rebuild_test_after")
        def after_func(job):
            c.count()
            write(job["out"], f"after-{c.count}")
        
        job3 = ppg.ExternalJob(
            output_path,
            {"out": "out.txt"},
            ["echo", "test"],
            call_after=after_func
        )
        ppg.run()
        assert c.count == 1
        out_value1 = job3["out"].read_text()
        
        # Change the call_after function
        def after_func_modified(job):
            c.count()
            write(job["out"], f"modified-{c.count}")
        
        job3 = ppg.ExternalJob(
            output_path,
            {"out": "out.txt"},
            ["echo", "test"],
            call_after=after_func_modified
        )
        ppg.run()
        assert c.count > 1  # Should have run again
        out_value2 = job3["out"].read_text()
        assert out_value1 != out_value2
        assert "modified" in out_value2
