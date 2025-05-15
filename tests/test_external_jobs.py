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
