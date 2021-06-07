import pytest
import pypipegraph2 as ppg
from .shared import write


@pytest.mark.usefixtures("ppg2_per_test")
class TestCycles:
    def test_simple_cycle(self):
        with pytest.raises(ppg.exceptions.NotADag):
            jobA = ppg.FileGeneratingJob("A", lambda of: write("A", "A"))
            jobB = ppg.FileGeneratingJob("B", lambda of: write("B", "A"))
            jobA.depends_on(jobB)
            jobB.depends_on(jobA)
            # ppg.run()

    def test_indirect_cicle(self):
        jobA = ppg.FileGeneratingJob("A", lambda of: write("A", "A"))
        jobB = ppg.FileGeneratingJob("B", lambda of: write("B", "A"))
        jobC = ppg.FileGeneratingJob("C", lambda of: write("C", "A"))
        jobC.depends_on(jobB)
        jobB.depends_on(jobA)
        jobA.depends_on(jobC)

        with pytest.raises(ppg.exceptions.NotADag):
            ppg.run()

    def test_exceeding_max_cycle(self):
        max_depth = 50
        # this raises
        jobs = []
        for x in range(0, max_depth - 1):
            j = ppg.FileGeneratingJob(str(x), lambda of: write(str(x), str(x)))
            if jobs:
                j.depends_on(jobs[-1])
            jobs.append(j)
        jobs[0].depends_on(j)

        with pytest.raises(ppg.exceptions.NotADag):
            ppg.run()

        ppg.new()
        jobs = []
        for x in range(0, max_depth + 100):
            j = ppg.FileGeneratingJob(str(x), lambda of: write(str(x), str(x)))
            if jobs:
                j.depends_on(jobs[-1])
            jobs.append(j)
        jobs[0].depends_on(j)

        with pytest.raises(ppg.exceptions.NotADag):
            ppg.run()
