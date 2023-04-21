import subprocess
import pytest  # noqa:F401
import unittest
from pathlib import Path


class Flake8TestCase(unittest.TestCase):
    def test_flake8(self):
        p = subprocess.Popen(
            "flake8",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=Path(__file__).parent.parent,
        )
        stdout, stderr = p.communicate()
        if p.returncode != 0:
            self.fail(
                "Flake 8 found issues: %s\n%s"
                % (stdout.decode("utf-8"), stderr.decode("utf-8"))
            )
