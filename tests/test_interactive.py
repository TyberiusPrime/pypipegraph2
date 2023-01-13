from .shared import counter, read
from pathlib import Path
import pypipegraph2 as ppg
import pytest


@pytest.mark.usefixtures("ppg2_per_test")
class TestCallSyntax:
    def test_simple(self):
        a = ppg.FileGeneratingJob("A", lambda of: counter("a") and of.write_text("A"))
        b = ppg.FileGeneratingJob("B", lambda of: counter("b") and of.write_text("B"))
        assert a() == [Path("A")]
        assert read("A") == "A"
        assert not Path("B").exists()
        assert read("a") == "1"
        assert not Path("b").exists()
        b()
        assert read("B") == "B"
        assert read("a") == "1"
        assert read("b") == "1"

    def test_downstream(self):
        a = ppg.MultiFileGeneratingJob(
            {"a": "A"}, lambda of: counter("a") and of["a"].write_text("A")
        )
        b = ppg.FileGeneratingJob("B", lambda of: counter("b") and of.write_text("B"))
        b.depends_on(a)
        assert set(a().keys()) == set(["a"])
        assert read("A") == "A"
        assert not Path("B").exists()
        assert read("a") == "1"
        assert not Path("b").exists()
        b()
        assert read("B") == "B"
        assert read("a") == "1"
        assert read("b") == "1"
        Path("A").unlink()
        b()
        assert read("B") == "B"
        assert read("a") == "2"
        assert read("b") == "1"  # was shielded

    def test_job_is_pruned(self, job_trace_log):
        a = ppg.FileGeneratingJob("A", lambda of: counter("a") and of.write_text("A"))
        b = ppg.FileGeneratingJob("B", lambda of: counter("b") and of.write_text("B"))
        a.prune()
        a()
        assert read("A") == "A"
        assert not Path("B").exists()
        assert read("a") == "1"
        assert not Path("b").exists()
        b()
        assert read("B") == "B"
        assert read("a") == "1"
        assert read("b") == "1"

    def test_plot_job(self):
        import pandas as pd
        import plotnine

        def calc():
            return pd.DataFrame({"X": list(range(0, 100)), "Y": list(range(50, 150))})

        def plot(df):
            p = plotnine.ggplot(df)
            p = p + plotnine.geom_point(plotnine.aes("X", "Y"))
            return p

        of = "A.png"
        p, c, t = ppg.PlotJob(of, calc, plot)
        pout = p()
        assert Path(of).exists()
        assert isinstance(pout, plotnine.ggplot)
        ppg.new()
        p, c, t = ppg.PlotJob(of, calc, plot)
        pout = p()
        assert isinstance(pout, plotnine.ggplot)
