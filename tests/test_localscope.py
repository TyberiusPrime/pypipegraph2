"""Test cases for our localscope only function requirements"""

import pytest
import pypipegraph2 as ppg2
from pathlib import Path


@pytest.mark.usefixtures("ppg2_per_test")
class TestLocalScope:
    def test_msg_contains_allowed_locals(self):
        def fail():
            gamma = 5
            return beta + alpha + gamma

        with pytest.raises(ppg2.FunctionUsesUndeclaredNonLocalsError) as exp:
            ppg2.FunctionInvariant("fail", fail)

        assert "job FIfail" in str(exp.value)
        assert "alpha=alpha" in str(exp.value)
        assert "beta=beta" in str(exp.value)
        assert not "gamma=gamma" in str(exp.value)
        assert "['alpha', 'beta']" in str(exp.value)

    def test_job_in_loop(self):
        for ii in range(10):
            ppg2.FileGeneratingJob(
                str(ii), lambda of: of.write_text(str(ii)), allowed_non_locals=["ii"]
            )
        ppg2.run()
        for ii in range(10):
            assert Path(str(ii)).read_text() == "9"

        ppg2.new()
        for ii in range(10):
            ppg2.FileGeneratingJob(str(ii), lambda of, ii=ii: of.write_text(str(ii)))
        ppg2.run()
        for ii in range(10):
            assert Path(str(ii)).read_text() == str(ii)

    def test_classes_are_fine(self):
        class A:
            pass

        def inner():
            return A()

        ppg2.FunctionInvariant("inner", inner)

    def test_functions_are_not_checked(self):
        def a():
            return 5

        ppg2.FileGeneratingJob("a", lambda of: of.write_text(str(a())))

        def a():
            return 10

        ppg2.run()
        assert Path("a").read_text() == "10"  # it's the rebinding

    def test_non_str_allowed_non_locals(self):
        def inner():
            return 5 + alpha  # noqa: F821

        with pytest.raises(TypeError):
            ppg2.FunctionInvariant(inner, allowed_non_locals=5)

        with pytest.raises(TypeError):
            ppg2.FunctionInvariant(inner, allowed_non_locals=[5])
