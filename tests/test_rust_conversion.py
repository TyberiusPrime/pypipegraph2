import shutil
import sys
import pytest

from pathlib import Path
import pypipegraph2 as ppg
from .shared import write, counter

# fmt: off
# has to be here for the indent to be the same...
# since the test case also change python versions


def _test_rewriting_history_do_a(ofs):
    counter('a')
    for of in ofs:
        of.write_text(of.name)
# fmt: on


@pytest.mark.usefixtures("ppg2_per_test")
class TestRustConversion:
    def test_rewriting_history(self):
        # not for python <  3.8
        if sys.version_info[:2] < (3, 8):
            return
        input = Path(__file__).parent / "old_history_for_conversion_test.gz"
        dir = ppg.global_pipegraph.get_history_filename().parent
        # dir.mkdir(parents=True)
        old_history_file = dir / "ppg_history.gz"
        shutil.copy(input, old_history_file)

        write("A", "A")
        write("C", "C")
        write("B", "B")

        jobA = ppg.MultiFileGeneratingJob(
            ["A", "C"],
            _test_rewriting_history_do_a,
            resources=ppg.Resources.AllCores,
            depend_on_function=False,
        )
        # I have to make the func invariant here myself.
        # the example was dumped from a run.py
        # where funcinvariant name = 'do_a'
        # but here were inside a func, so it's 'FITestRustConversion.test_rewriting_history.<locals>.do_a'
        # jobA.depends_on(ppg.FunctionInvariant("A:::C", do_a))
        jobA.depends_on(ppg.FunctionInvariant("do_a", _test_rewriting_history_do_a))

        jobB = ppg.FileGeneratingJob(
            "B",
            lambda of: counter("b") and of.write_text(of.name),
            resources=ppg.Resources.AllCores,
        )
        jobB.depends_on("A")
        # ppg.pypipegraph2.enable_logging()
        ppg.run()
        assert not old_history_file.exists()  # g got renamed

        assert not Path("a").exists()
        assert not Path("b").exists()
