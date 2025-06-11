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
            == """bash \\
  -c 'echo hello; echo 42 > two.txt; echo hello_err >&2'"""
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

        output_path = Path("rebuild_test_cmd")

        job = ppg.ExternalJob(output_path, {}, ["echo", "1"])
        ppg.run()
        assert job["stdout"].read_text().strip() == "1"

        ppg.run()
        ppg.new()
        assert job["stdout"].read_text().strip() == "1"

        job = ppg.ExternalJob(output_path, {}, ["echo", "2"])
        ppg.run()
        ppg.new()
        assert job["stdout"].read_text().strip() == "2"

        job = ppg.ExternalJob(output_path, {}, lambda: ["echo", "3"])
        ppg.run()
        ppg.new()
        assert job["stdout"].read_text().strip() == "3"

        job = ppg.ExternalJob(output_path, {}, lambda: ["echo", "4"])
        ppg.run()
        ppg.new()
        assert job["stdout"].read_text().strip() == "4"

        job = ppg.ExternalJob(
            output_path,
            {},
            lambda: ["echo", "4"],
            call_before=lambda job: (job.output_path / "before").write_text("5"),
        )
        ppg.run()
        ppg.new()
        assert job["stdout"].read_text().strip() == "4"
        assert (job.output_path / "before").read_text().strip() == "5"

        job = ppg.ExternalJob(
            output_path,
            {},
            lambda: ["echo", "4"],
            call_before=lambda job: (job.output_path / "before").write_text("6"),
        )
        ppg.run()
        ppg.new()
        assert job["stdout"].read_text().strip() == "4"
        assert (job.output_path / "before").read_text().strip() == "6"

        job = ppg.ExternalJob(
            output_path,
            {},
            lambda: ["echo", "4"],
            call_before=lambda job: (job.output_path / "before").write_text("6"),
            call_after=lambda job: (job.output_path / "after").write_text("7"),
        )
        ppg.run()
        ppg.new()
        assert job["stdout"].read_text().strip() == "4"
        assert (job.output_path / "before").read_text().strip() == "6"
        assert (job.output_path / "after").read_text().strip() == "7"

        job = ppg.ExternalJob(
            output_path,
            {},
            lambda: ["echo", "4"],
            call_before=lambda job: (job.output_path / "before").write_text("6"),
            call_after=lambda job: (job.output_path / "after").write_text("8"),
        )
        ppg.run()
        # ppg.new()
        assert job["stdout"].read_text().strip() == "4"
        assert (job.output_path / "before").read_text().strip() == "6"
        assert (job.output_path / "after").read_text().strip() == "8"


def test_external_job_pretty_print_cmd():
    from pypipegraph2.jobs import external_job_pretty_print_cmd

    test_cases = [
        (
            [
                "python",
                "script.py",
                "--input",
                "data.txt",
                "--output",
                "results.txt",
                "--verbose",
            ],
            """python script.py \\
  --input data.txt \\
  --output results.txt \\
  --verbose""",
        ),
        (
            ["cmd", "--arg1", "val1", "--arg2", "val2", "--flag", "--arg3", "val3"],
            """cmd \\
  --arg1 val1 \\
  --arg2 val2 \\
  --flag \\
  --arg3 val3""",
        ),
        (
            ["exec", "--path", "/some/long/path with spaces", "--enable", "--timeout", "30"],
           """exec \\
  --path '/some/long/path with spaces' \\
  --enable \\
  --timeout 30""",
        ),
    ]
    for input, output in test_cases:
        assert output == external_job_pretty_print_cmd(input)
