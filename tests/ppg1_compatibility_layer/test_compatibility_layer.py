import pytest
from pathlib import Path
from .shared import write, force_load
import pypipegraph2 as ppg2
import pypipegraph as ppg1

# to push test coverage...


class TestcompatibilityLayer:
    def test_repeated_apply_unapply(self):
        fg1 = ppg1.FileGeneratingJob
        fg2 = ppg2.FileGeneratingJob
        ppg2.replace_ppg1()
        assert ppg1.FileGeneratingJob is not fg1
        assert ppg1.FileGeneratingJob is not fg2  # actually it is wrapped...
        fg1_to_two = ppg1.FileGeneratingJob
        ppg2.unreplace_ppg1()
        assert ppg1.FileGeneratingJob is fg1
        ppg2.replace_ppg1()
        assert ppg1.FileGeneratingJob is fg1_to_two
        ppg2.replace_ppg1()
        assert ppg1.FileGeneratingJob is fg1_to_two
        ppg2.replace_ppg1()
        assert ppg1.FileGeneratingJob is fg1_to_two
        ppg2.unreplace_ppg1()
        assert ppg1.FileGeneratingJob is fg1
        ppg2.unreplace_ppg1()
        assert ppg1.FileGeneratingJob is fg1


@pytest.mark.usefixtures("ppg1_compatibility_test")
class TestcompatibilityLayerMore:
    def test_use_cores_exclusive(self):
        j = ppg1.FileGeneratingJob("shu", lambda of: of.write_text("j"))
        j.cores_needed = -2
        assert j.resources == ppg2.Resources.Exclusive
        assert j.cores_needed == -2

    def test_fg_did_not_create_its_file(self):
        j = ppg1.FileGeneratingJob("test_file_gen", lambda: 55)  # old school callback
        j2 = ppg1.FileGeneratingJob(
            "test_file_gen_does", lambda: Path("test_file_gen_does").write_text("A")
        )  # old school callback
        with pytest.raises(ppg2.RunFailed):
            ppg2.run()
        assert "did not create" in str(j.exception)

    def test_multifilegenerating_without_arguments(self):
        j1 = ppg1.MultiFileGeneratingJob(
            ["out/A", "out/B"], lambda: write("out/A", "A") or write("out/B", "B")
        )
        j2 = ppg1.MultiFileGeneratingJob(["out/C", "out/D"], lambda: 55)
        j3 = ppg1.MultiFileGeneratingJob(["out/G", "out/F"], lambda of: 55)
        with pytest.raises(ppg2.RunFailed):
            ppg2.run()
        assert "did not create" in str(j2.exception)
        assert "did not create" in str(j3.exception)
        assert Path("out/A").read_text() == "A"
        assert Path("out/B").read_text() == "B"

    def test_temp_file_with_and_without(self):
        a = ppg1.TempFileGeneratingJob("A", lambda: 55)
        b = ppg1.TempFileGeneratingJob("b", lambda of: 55)
        c = ppg1.MultiTempFileGeneratingJob(["C"], lambda: 55)
        d = ppg1.MultiTempFileGeneratingJob(["D"], lambda of: 55)
        force_load(a)
        force_load(b)
        force_load(c)
        force_load(d)
        with pytest.raises(ppg2.RunFailed):
            ppg2.run()
        assert "did not create" in str(a.exception)
        assert "did not create" in str(b.exception)
        assert "did not create" in str(c.exception)
        assert "did not create" in str(d.exception)

    def test_unsupported(self):
        with pytest.raises(NotImplementedError):
            ppg1.MemMappedDataLoadingJob()
