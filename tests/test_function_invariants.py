import pypipegraph2 as ppg
import pytest


@pytest.mark.usefixtures("ppg_per_test")
class TestBuildInCompabilty:
    def test_invariant_build_in_function(self):
        a = ppg.FunctionInvariant("test", sorted).run(None, None)[
            "FItest"
        ]["source"]
        assert a == "<built-in function sorted>"


@pytest.mark.usefixtures("ppg_per_test")
class TestCythonCompability:
    def source_via_func_invariant(self, name, func):
        return ppg.FunctionInvariant(name, func).run(None, None)["FIa"][
            "source"
        ]

    def test_just_a_function(self):
        import cython

        src = """
def a():
    return 1

def b():
    return 5
"""
        func = cython.inline(src)["a"]
        actual = self.source_via_func_invariant("a", func)
        should = """    def a():
        return 1
    """
        assert actual == should

    def test_nested_function(self):
        import cython

        src = """
def a():
    def b():
        return 1
    return b

def c():
    return 5
"""
        func = cython.inline(src)["a"]()
        actual = self.source_via_func_invariant("a", func)
        should = """        def b():
            return 1"""
        assert actual == should

    def test_class(self):
        import cython

        src = """
class A():
    def b(self):
        return 55

def c():
    return 5
"""

        func = cython.inline(src)["A"]().b
        actual = self.source_via_func_invariant("a", func)
        should = """        def b(self):
            return 55
    """
        assert actual == should

    def test_class_inner_function(self):
        import cython

        src = """
class A():
    def b(self):
        def c():
            return 55
        return c

def d():
    return 5
"""

        func = cython.inline(src)["A"]().b()
        actual = self.source_via_func_invariant("a", func)
        should = """            def c():
                return 55"""
        assert actual == should
