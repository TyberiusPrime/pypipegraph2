import pytest
from pathlib import Path
import os
from .shared import read, write, append
import pickle

try:
    import dppd
    import dppd_plotnine  # noqa: F401

    dp, X = dppd.dppd()
    has_pyggplot = True
except ImportError:
    has_pyggplot = False
    raise ValueError()
    pass


if has_pyggplot:  # noqa C901
    import pandas as pd
    import pypipegraph2 as ppg
    import subprocess

    def magic(filename):
        """See what linux 'file' commando says about that file"""
        if not os.path.exists(filename):
            raise OSError("Does not exists %s" % filename)
        p = subprocess.Popen(["file", filename], stdout=subprocess.PIPE)
        stdout, stderr = p.communicate()
        return stdout

    @pytest.mark.usefixtures("ppg2_per_test")
    class TestPlotJob:
        def test_basic(self):
            def calc():
                return pd.DataFrame(
                    {"X": list(range(0, 100)), "Y": list(range(50, 150))}
                )

            def plot(df):
                return dp(df).p9().add_point("X", "Y")

            def plot2(df):
                p = dp(df).p9().add_point("Y", "X")
                p.width = 5
                p.height = 2
                return p

            of = "out/test.png"
            p, c, t = ppg.PlotJob(of, calc, plot)
            # p.add_fiddle(lambda p: dp(p).scale_x_continuous(trans="log10").pd)
            p.add_another_plot("out/test2.png", plot2)
            ppg.run()
            assert magic(of).find(b"PNG image") != -1
            assert os.path.exists(of + ".tsv")
            assert os.path.exists("cache/out/test.png")
            assert os.path.exists("out/test2.png")
            assert not os.path.exists("cache/out/test2.png")
            assert not os.path.exists("cache/out/test2.png.tsv")

        def test_basic_skip_table(self):
            def calc():
                return pd.DataFrame(
                    {"X": list(range(0, 100)), "Y": list(range(50, 150))}
                )

            def plot(df):
                return dp(df).p9().add_point("X", "Y")

            of = "out/test.png"
            ppg.PlotJob(of, calc, plot, create_table=False)
            ppg.run()
            assert magic(of).find(b"PNG image") != -1
            assert not os.path.exists(of + ".tsv")
            assert os.path.exists("cache/out/test.png")

        def test_basic_return_dict(self):
            def calc():
                return {
                    "A": pd.DataFrame(
                        {"X": list(range(0, 100)), "Y": list(range(50, 150))}
                    )
                }

            def plot(df):
                p = dp(df["A"]).p9().add_point("X", "Y")
                p.width = 5
                p.height = 1
                return p

            of = "out/test.png"
            ppg.PlotJob(of, calc, plot)
            ppg.run()
            assert magic(of).find(b"PNG image") != -1
            assert read(of + ".tsv").find("#A\n") != -1

        def test_basic_return_dict_non_df_raises(self):
            def calc():
                return {
                    "A": pd.DataFrame(
                        {"X": list(range(0, 100)), "Y": list(range(50, 150))}
                    ),
                    "B": "not_a_df",
                }

            def plot(df):
                return dp(df["A"]).p9().add_point("X", "Y")

            of = "out/test.png"
            p, c, t = ppg.PlotJob(of, calc, plot)
            p.height = 1200
            p.width = 800
            with pytest.raises(ppg.JobsFailed):
                ppg.run()
            assert "did not return a DataFrame" in str(
                ppg.global_pipegraph.last_run_result[c[1].job_id].error
            )

        def test_skip_caching(self):
            def calc():
                write("B", "B")
                return pd.DataFrame(
                    {"X": list(range(0, 100)), "Y": list(range(50, 150))}
                )

            def plot(df):
                if not os.path.exists("A"):
                    raise ValueError()
                if not os.path.exists("B"):
                    raise ValueError()

                return dp(df).p9().add_point("X", "Y").pd

            def prep_job(output_filename):
                write("A", "A")

            def plot2(df):
                p = dp(df).p9().add_point("Y", "X")
                p.width = 5
                p.height = 2
                return p

            p = ppg.FileGeneratingJob("A", prep_job)

            # this tests the correct dependency setting on skip_caching
            of = "out/test.png"
            p2, c2, t2 = ppg.PlotJob(
                of, calc, plot, cache_calc=False, render_args={"width": 2, "height": 4}
            )
            p2.depends_on(p)
            t2.depends_on(
                p
            )  # if you don't cache, you have to take care of this yourself
            p2.add_another_plot("out/test2.png", plot2)
            ppg.run()
            assert magic(of).find(b"PNG image") != -1
            assert not os.path.exists("cache/out/test.png")
            assert os.path.exists("out/test.png")
            assert os.path.exists("out/test2.png")

        def xxxtest_redefiniton_and_skip_changes_raises(self):
            def calc():
                return pd.DataFrame(
                    {"X": list(range(0, 100)), "Y": list(range(50, 150))}
                )

            def plot(df):
                return dp(df).p9().add_point("X", "Y")

            of = "out/test.png"
            ppg.PlotJob(of, calc, plot)
            ppg.PlotJob(of, calc, plot)
            with pytest.raises(ppg.JobRedefinitionError):
                ppg.PlotJob(of, calc, plot, render_args={"something": 55})
            # does not remove the jobs though
            ppg.PlotJob(of, calc, plot, cache_calc=False)
            ppg.PlotJob(of, calc, plot, create_table=False)

            ppg.new(run_mode=ppg.RunMode.NOTEBOOK)
            ppg.PlotJob(of, calc, plot)
            ppg.PlotJob(of, calc, plot)
            ppg.PlotJob(of, calc, plot, cache_calc=False)
            ppg.PlotJob(of, calc, plot, create_table=False)
            ppg.PlotJob(of, calc, plot, render_args={"something": 55})

        def xxxtest_pdf(self):
            def calc():
                return pd.DataFrame(
                    {"X": list(range(0, 100)), "Y": list(range(50, 150))}
                )

            def plot(df):
                return dp(df).p9().add_point("X", "Y")

            of = "out/test.pdf"
            ppg.PlotJob(of, calc, plot)
            ppg.run()
            assert magic(of).find(b"PDF document") != -1

        def xxxtest_raises_on_invalid_filename(self):
            def calc():
                return pd.DataFrame(
                    {"X": list(range(0, 100)), "Y": list(range(50, 150))}
                )

            def plot(df):
                return dp(df).p9().add_point("X", "Y")

            of = "out/test.shu"

            def inner():
                ppg.PlotJob(of, calc, plot)

            with pytest.raises(ValueError):
                inner()

        def test_reruns_just_plot_if_plot_changed(self):
            def calc():
                append("out/calc", "A")
                return pd.DataFrame(
                    {"X": list(range(0, 100)), "Y": list(range(50, 150))}
                )

            def plot(df):
                append("out/plot", "B")
                return dp(df).p9().add_point("X", "Y")

            of = "out/test.png"
            ppg.PlotJob(of, calc, plot)
            ppg.run()
            assert magic(of).find(b"PNG image") != -1
            assert read("out/calc") == "A"
            assert read("out/plot") == "B"

            ppg.new()

            def plot2(df):
                append("out/plot", "B")
                return dp(df).p9().add_point("Y", "X")

            ppg.PlotJob(of, calc, plot2)
            ppg.run()
            assert magic(of).find(b"PNG image") != -1
            assert read("out/calc") == "A"
            assert read("out/plot") == "BB"

        def test_no_rerun_if_ignore_code_changes_and_plot_changes(self):
            def calc():
                append("out/calc", "A")
                return pd.DataFrame(
                    {"X": list(range(0, 100)), "Y": list(range(50, 150))}
                )

            def plot(df):
                append("out/plot", "B")
                return dp(df).p9().add_point("X", "Y")

            of = "out/test.png"
            # note that you already need to ignore the function here
            # otherwise, the fact that the function is now *missing*
            # would trigger downstream
            ppg.PlotJob(of, calc, plot, depend_on_function=False)
            ppg.run()
            assert magic(of).find(b"PNG image") != -1
            assert read("out/calc") == "A"
            assert read("out/plot") == "B"

            ppg.new()

            def plot2(df):
                append("out/plot", "B")
                return dp(df).p9().add_point("Y", "X")

            ppg.PlotJob(of, calc, plot2, depend_on_function=False)
            ppg.run()
            assert magic(of).find(b"PNG image") != -1
            assert read("out/calc") == "A"
            assert read("out/plot") == "B"

        def test_reruns_both_if_calc_changed(self):
            def calc():
                append("out/calc", "A")
                return pd.DataFrame(
                    {"X": list(range(0, 100)), "Y": list(range(50, 150))}
                )

            def plot(df):
                append("out/plot", "B")
                return dp(df).p9().add_point("X", "Y")

            of = "out/test.png"
            ppg.PlotJob(of, calc, plot)
            ppg.run()
            assert magic(of).find(b"PNG image") != -1
            assert read("out/calc") == "A"
            assert read("out/plot") == "B"

            ppg.new()

            def calc2():
                append("out/calc", "A")
                x = 5  # noqa: E157,F841
                return pd.DataFrame(
                    {
                        "X": list(range(1, 101)),
                        "Y": list(range(50, 150)),
                    }  # output must really change
                )

            ppg.PlotJob(of, calc2, plot)
            ppg.run()
            assert magic(of).find(b"PNG image") != -1
            assert read("out/calc") == "AA"
            assert read("out/plot") == "BB"

        def test_no_rerun_if_calc_change_but_ignore_codechanges(self):
            def calc():
                append("out/calc", "A")
                return pd.DataFrame(
                    {"X": list(range(0, 100)), "Y": list(range(50, 150))}
                )

            def plot(df):
                append("out/plot", "B")
                return dp(df).p9().add_point("X", "Y")

            of = "out/test.png"
            ppg.PlotJob(of, calc, plot, depend_on_function=False)
            ppg.run()
            assert magic(of).find(b"PNG image") != -1
            assert read("out/calc") == "A"
            assert read("out/plot") == "B"

            ppg.new()

            def calc2():
                append("out/calc", "A")
                x = 5  # noqa: E157,F841
                return pd.DataFrame(
                    {"X": list(range(0, 100)), "Y": list(range(50, 150))}
                )

            ppg.PlotJob(of, calc2, plot, depend_on_function=False)
            ppg.run()
            assert magic(of).find(b"PNG image") != -1
            assert read("out/calc") == "A"
            assert read("out/plot") == "B"

        def test_raises_if_calc_returns_non_df(self):
            def calc():
                return None

            def plot(df):
                append("out/plot", "B")
                return dp(df).p9().add_point("X", "Y")

            of = "out/test.png"
            job, cache_job, table_job = ppg.PlotJob(of, calc, plot)
            with pytest.raises(ppg.JobsFailed):
                ppg.run(print_failures=False)
            print(ppg.global_pipegraph.last_run_result[cache_job[1].job_id].error)
            assert isinstance(
                ppg.global_pipegraph.last_run_result[cache_job[1].job_id].error,
                ppg.JobError,
            )

        def test_raises_if_plot_returns_non_plot(self):
            # import pyggplot
            def calc():
                return pd.DataFrame(
                    {"X": list(range(0, 100)), "Y": list(range(50, 150))}
                )

            def plot(df):
                return None

            of = "out/test.png"
            ppg.PlotJob(of, calc, plot)
            with pytest.raises(ppg.JobsFailed):
                ppg.run()
            print(type(ppg.global_pipegraph.last_run_result[of].error))
            print(repr(ppg.global_pipegraph.last_run_result[of].error))
            assert isinstance(
                ppg.global_pipegraph.last_run_result[of].error, ppg.JobError
            )
            assert "did not return a plot object" in str(
                ppg.global_pipegraph.last_run_result[of].error
            )

        def test_passing_non_function_for_calc(self):
            def inner():
                ppg.PlotJob("out/a", "shu", lambda df: 1)

            with pytest.raises(ValueError):
                inner()

        def test_passing_non_function_for_plot(self):
            def inner():
                ppg.PlotJob("out/a", lambda: 55, "shu")

            with pytest.raises(ValueError):
                inner()

        def test_passing_non_string_as_jobid(self):
            def inner():
                ppg.PlotJob(5, lambda: 1, lambda df: 34)

            with pytest.raises(TypeError):
                inner()

        def test_unpickling_error(self):
            def calc():
                return pd.DataFrame(
                    {"X": list(range(0, 100)), "Y": list(range(50, 150))}
                )

            def plot(df):
                return dp(df).p9().add_point("X", "Y")

            of = "out/test.png"
            p = ppg.PlotJob(of, calc, plot)
            ppg.run()
            ppg.new()
            p = ppg.PlotJob(of, calc, plot)
            with open("cache/out/test.png", "w") as op:
                op.write("no unpickling")
            os.unlink("out/test.png")  # so it reruns
            with pytest.raises(ppg.JobsFailed):
                ppg.run()
            assert not os.path.exists("out/test.png")
            assert isinstance(
                ppg.global_pipegraph.last_run_result[p[1][0].job_id].error.args[0],
                pickle.UnpicklingError,
            )
            assert "Unpickling error in file" in str(
                ppg.global_pipegraph.last_run_result[p[1][0].job_id].error.args[0]
            )

        def test_add_another_not_returning_plot(self):
            def calc():
                return pd.DataFrame(
                    {"X": list(range(0, 100)), "Y": list(range(50, 150))}
                )

            def plot(df):
                return dp(df).p9().add_point("X", "Y")

            def plot2(df):
                return

            of = "out/test.png"
            p, c, t = ppg.PlotJob(of, calc, plot)
            # p.add_fiddle(lambda p: p.scale_x_log10())
            p2 = p.add_another_plot("out/test2.png", plot2)
            with pytest.raises(ppg.JobsFailed):
                ppg.run()
            assert isinstance(
                ppg.global_pipegraph.last_run_result[p2.job_id].error, ppg.JobError
            )

        def test_matplotlib(self):
            import matplotlib.pyplot as plt
            import numpy as np

            def calc():
                return pd.DataFrame({})

            def plot(df):
                # Data for plotting
                t = np.arange(0.0, 2.0, 0.01)
                s = 1 + np.sin(2 * np.pi * t)

                fig, ax = plt.subplots()
                ax.plot(t, s)

                ax.set(
                    xlabel="time (s)",
                    ylabel="voltage (mV)",
                    title="About as simple as it gets, folks",
                )
                ax.grid()
                return fig

            of = "out/test.png"
            p, c, t = ppg.PlotJob(of, calc, plot)
            ppg.run()
            assert Path(of).exists()
