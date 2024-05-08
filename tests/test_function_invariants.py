import pypipegraph2 as ppg
import pytest


@pytest.mark.usefixtures("ppg2_per_test")
class TestBuildInCompabilty:
    def test_invariant_build_in_function(self):
        a = ppg.FunctionInvariant("test", sorted).run(None, None)["FItest"]["source"]
        assert a == "<built-in function sorted>"


_cytohn_func_counter = 0


@pytest.mark.usefixtures("ppg2_per_test")
class TestCythonCompability:
    def source_via_func_invariant(self, name, func):
        global _cytohn_func_counter
        _cytohn_func_counter = +1
        r = ppg.FunctionInvariant(name + str(_cytohn_func_counter), func).run(
            None, None
        )
        print(r)
        return r["FI" + name + str(_cytohn_func_counter)]["source"]

    def test_just_a_function(self):
        import cython

        src = """
def a():
    '''single line docstring'''
    return 1

def b():
    '''Multi
    line
    docstring
    '''


    shu = 55

    return shu
"""
        func = cython.inline(src)["a"]
        func2 = cython.inline(src)["b"]
        actual = self.source_via_func_invariant("a", func)
        should = """    def a():
        return 1"""
        assert actual == should

        actual = self.source_via_func_invariant("b", func2)
        should = "    def b():\n        shu = 55\n    \n        return shu"
        assert actual == should

        ppg.FunctionInvariant("a", func)  # not a redefinition
        with pytest.raises(ppg.JobRedefinitionError):
            ppg.FunctionInvariant("a", func2)  # cython vs cython
        with pytest.raises(ppg.JobRedefinitionError):
            ppg.FunctionInvariant("a", lambda: 1)  # cython vs python
        ppg.FunctionInvariant("b", lambda: 45)
        with pytest.raises(ppg.JobRedefinitionError):
            ppg.FunctionInvariant("b", func2)  # python vs cython

    def test_just_a_function_with_docstring(self):
        import cython

        src = (
            """
def a():
    ''' a() is used
    to do nothing
    '''
    return 1

"""
            '''def b():
    """ b() is used
    to do nothing as well
    """
    return 5
'''
        )
        func = cython.inline(src)["a"]
        actual = self.source_via_func_invariant("a", func)
        should = """    def a():
        return 1"""
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
            return 55"""
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
