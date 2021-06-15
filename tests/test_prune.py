from pathlib import Path
import pytest
import pypipegraph2 as ppg
from .shared import write, read


@pytest.mark.usefixtures("ppg2_per_test")
class TestPruning:
    def test_basic_prune(self):
        ppg.FileGeneratingJob("A", lambda of: write("A", "A"))
        b = ppg.FileGeneratingJob("B", lambda of: write("B", "B"))
        b.prune()
        ppg.run()
        assert Path("A").read_text() == "A"
        assert not Path("B").exists()

    def test_basic_prune2(self):
        a = ppg.FileGeneratingJob("A", lambda of: write("A", "A"))
        b = ppg.FileGeneratingJob("B", lambda of: write("B", "B"))
        b.depends_on(a)
        b.prune()
        ppg.run()
        assert Path("A").read_text() == "A"
        assert not Path("B").exists()

    def test_basic_prune3(self):
        a = ppg.FileGeneratingJob("A", lambda of: write("A", "A"))
        b = ppg.FileGeneratingJob("B", lambda of: write("B", "B"))
        c = ppg.FileGeneratingJob("C", lambda of: write("C", "C"))
        d = ppg.FileGeneratingJob("D", lambda of: write("D", "D"))
        b.depends_on(a)
        b.prune()
        c.depends_on(b)  # that is ok, pruning happens after complet build.
        d.depends_on(a)
        ppg.run()
        assert Path("A").read_text() == "A"
        assert Path("D").read_text() == "D"
        assert not Path("B").exists()
        assert not Path("C").exists()
        assert c.prune_reason == b.job_id
        ppg.run()  # just so we recurse_prune again.

    def test_tempfile_not_run_on_prune(self):
        a = ppg.TempFileGeneratingJob("A", lambda of: write("A", "A"))
        b = ppg.FileGeneratingJob("B", lambda of: write("B", "B" + read("A")))
        b.depends_on(a)
        b.prune()
        ppg.run()
        assert not Path("B").exists()
        assert not Path("A").exists()

    def test_tempfile_still_run_if_needed_for_other(self):
        a = ppg.TempFileGeneratingJob("A", lambda of: write("A", "A"))
        b = ppg.FileGeneratingJob("B", lambda of: write("B", "B" + read("A")))
        c = ppg.FileGeneratingJob("C", lambda of: write("C", "C" + read("A")))
        b.depends_on(a)
        c.depends_on(a)
        b.prune()
        ppg.run()
        assert not Path("B").exists()
        assert Path("C").exists()
        assert Path("C").read_text() == "CA"
        assert not Path("A").exists()

    def test_basic_prune_unprune(self):
        ppg.FileGeneratingJob("A", lambda of: write("A", "A"))
        b = ppg.FileGeneratingJob("B", lambda of: write("B", "B"))
        b.prune()
        ppg.run()
        assert Path("A").read_text() == "A"
        assert not Path("B").exists()
        b.unprune()
        ppg.run()
        assert Path("A").read_text() == "A"
        assert read("B") == "B"
