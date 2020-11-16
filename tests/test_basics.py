from pathlib import Path
import pytest
import pypipegraph2 as ppg


class TestPypipegraph2:
    def test_very_simple(self, ppg_per_test):
        assert not Path("A").exists()
        job = ppg.FileGeneratingJob("A", lambda of: of.write_text("Done"))
        ppg.run()
        assert Path("A").read_text() == "Done"

    def test_very_simple_chain(self, ppg_per_test):
        assert not Path("A").exists()
        assert not Path("B").exists()
        jobA = ppg.FileGeneratingJob("A", lambda of: of.write_text("AAA"))
        jobB = ppg.FileGeneratingJob(
            "B", lambda of: of.write_text("BBB" + Path("A").read_text())
        )
        jobB.depends_on(jobA)
        ppg.run()
        assert Path("A").read_text() == "AAA"
        assert Path("B").read_text() == "BBBAAA"

    def test_very_simple_chain_reverse(self, ppg_per_test):
        assert not Path("A").exists()
        assert not Path("B").exists()
        jobB = ppg.FileGeneratingJob(
            "B", lambda of: of.write_text("BBB" + Path("A").read_text())
        )
        jobA = ppg.FileGeneratingJob("A", lambda of: of.write_text("AAA"))
        jobB.depends_on(jobA)
        ppg.run()
        assert Path("A").read_text() == "AAA"
        assert Path("B").read_text() == "BBBAAA"


    def test_very_simple_chain_rerun(self, ppg_per_test):
        assert not Path("A").exists()
        assert not Path("B").exists()
        counter = 0
        jobA = ppg.FileGeneratingJob("A", lambda of: of.write_text(f"{counter}"))
        jobB = ppg.FileGeneratingJob(
            "B", lambda of: of.write_text("BBB" + Path("A").read_text())
        )
        jobB.depends_on(jobA)
        ppg.run()
        assert Path("B").read_text() == "BBB0"
        ppg.run()
        assert Path("B").read_text() == "BBB0"
        Path("A").unlink()
        counter = 1
        ppg.run()
        assert Path("B").read_text() == "BBB0"

    def test_isolation(self, ppg_per_test):
        assert not Path("B").exists()
        assert not Path("C").exists()

        def b(of):
            of.write_text("BBB")
            count = Path("counter").read_text()
            Path("outcount").write_text(count)

        jobB = ppg.FileGeneratingJob("B", b)
        jobC = ppg.FileGeneratingJob(
            "C", lambda of: of.write_text("CCC" + Path("outcount").read_text())
        )
        jobC.depends_on(jobB)
        Path("counter").write_text("0")
        ppg.run()
        assert Path("B").read_text() == "BBB"
        assert Path("C").read_text() == "CCC0"
        assert Path("outcount").read_text() == "0"
        ppg.run()
        assert Path("B").read_text() == "BBB"
        assert Path("C").read_text() == "CCC0"
        assert Path("outcount").read_text() == "0"

        Path("counter").write_text("1")
        ppg.run()  # since the counter is *not* a dependency...
        assert Path("B").read_text() == "BBB"
        assert Path("C").read_text() == "CCC0"
        assert Path("outcount").read_text() == "0"

        Path("B").unlink()  # will make it rerun.
        ppg.run()
        assert Path("outcount").read_text() == "1"
        # but C was not rerun, since the B output did not change.
        assert Path("C").read_text() == "CCC0"

    def test_job_redefinition(self):
        raise NotImplementedError()

    def test_cycles(self):
        jobA = ppg.FileGeneratingJob("A", lambda of: of.write_text("Done"))
        jobB = ppg.FileGeneratingJob("B", lambda of: of.write_text("Done"))
        jobA.depends_on(jobB)
        jobB.depends_on(jobA)
        with pytest.raises(ppg.exceptions.NotADag):
            ppg.run()

    def test_jobs_run_in_different_pids(self):
        raise NotImplementedError()
