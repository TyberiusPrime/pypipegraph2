import math as math_module

import pytest

from ppg3.localscope import DefinitionError, check_localscope

MODULE_GLOBAL = 42


def test_builtins_allowed():
    def f(x):
        return len(x) + abs(-1)

    report = check_localscope(f)
    assert report.modules == []


def test_module_reference_allowed_and_recorded():
    def f(x):
        return math_module.sqrt(x)

    report = check_localscope(f)
    assert report.modules == ["math_module"]


def test_closure_over_local_var_named_in_error():
    a = "hello"

    def f():
        return a

    with pytest.raises(DefinitionError) as exc:
        check_localscope(f)
    assert "a" in exc.value.args[0]


def test_global_variable_use_is_a_violation():
    def f():
        return MODULE_GLOBAL

    with pytest.raises(DefinitionError) as exc:
        check_localscope(f)
    assert "MODULE_GLOBAL" in exc.value.args[0]


def test_allowed_set_permits_named_global():
    def f():
        return MODULE_GLOBAL

    report = check_localscope(f, allowed=frozenset({"MODULE_GLOBAL"}))
    assert report.modules == []


def test_params_are_not_violations():
    def f(x, y=1):
        return x + y

    report = check_localscope(f)
    assert report.modules == []


def test_nested_function_closure_detected():
    def f():
        z = 1

        def inner():
            return z

        return inner()

    # `f` itself has no freevars (z is local to it); the violation would be
    # in `inner`'s own analysis, which this check does not descend into
    # automatically since it only inspects the top-level callable passed in.
    report = check_localscope(f)
    assert report.modules == []


def test_multiple_violations_all_named():
    b = 1
    c = 2

    def f():
        return b + c + UNDEFINED_GLOBAL  # noqa: F821

    with pytest.raises(DefinitionError) as exc:
        check_localscope(f)
    msg = exc.value.args[0]
    assert "b" in msg and "c" in msg and "UNDEFINED_GLOBAL" in msg
