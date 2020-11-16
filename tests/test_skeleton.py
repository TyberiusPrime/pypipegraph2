# -*- coding: utf-8 -*-

import pytest
from pypipegraph2.skeleton import fib

__author__ = "Florian Finkernagel"
__copyright__ = "Florian Finkernagel"
__license__ = "mit"


def test_fib():
    assert fib(1) == 1
    assert fib(2) == 1
    assert fib(7) == 13
    with pytest.raises(AssertionError):
        fib(-10)
