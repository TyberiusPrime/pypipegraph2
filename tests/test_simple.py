from pathlib import Path
import pytest
import pypipegraph2 as ppg
from .shared import write, read


def forget_job_status(invariant_status_filename=None):
    """Delete the job status file - usually only useful for testing"""
    if invariant_status_filename is None:
        invariant_status_filename = ppg.global_pipegraph.get_history_filename()
    try:
        Path(invariant_status_filename).unlink()
    except OSError:
        pass


def destroy_global_pipegraph():
    """Free the current global pipegraph - usually only useful for testing"""
    ppg.global_pipegraph = None


@pytest.mark.usefixtures("ppg2_per_test")
class TestSimple:
    def test_job_creation_before_pipegraph_creation_raises(self):
        destroy_global_pipegraph()
        with pytest.raises(ValueError):
            ppg.FileGeneratingJob("A", lambda: None)

    def test_using_after_destruction(self):
        a = ppg.FileGeneratingJob("A", lambda of: None)
        destroy_global_pipegraph()
        with pytest.raises(ValueError):
            a.readd()

    def test_run_pipegraph_without_pipegraph_raises(self):
        destroy_global_pipegraph()
        with pytest.raises(ValueError):
            ppg.run()

    def test_can_run_twice(self):
        ppg.run()
        ppg.run()
        ppg.run()

    def test_can_add_jobs_after_run(self):
        ppg.new()
        ppg.run()
        ppg.FileGeneratingJob("A", lambda of: write(of, "A"))
        ppg.run()
        assert read("A") == "A"

    def test_non_default_status_filename(self):
        ppg.new(dir_config=ppg.DirConfig(history_dir="shu"))
        ppg.FileGeneratingJob("A", lambda of: write(of, "A"))
        ppg.run()
        assert (Path("shu") / ppg.global_pipegraph.get_history_filename().name).exists()
        assert not (
            Path(".ppg") / ppg.global_pipegraph.get_history_filename().name
        ).exists()
