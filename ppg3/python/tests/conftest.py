import shutil
import sys
from pathlib import Path

import pytest

PYTHON_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PYTHON_DIR))

GOLDEN_DIR = PYTHON_DIR.parent / "tests" / "golden"


def _has_module(name: str) -> bool:
    try:
        __import__(name)
        return True
    except ImportError:
        return False


HAVE_CLOUDPICKLE = _has_module("cloudpickle")
HAVE_BLAKE3 = _has_module("blake3")


def _has_core() -> bool:
    try:
        from ppg3 import _core  # noqa: F401

        return True
    except ImportError:
        return False


HAVE_CORE = _has_core()

requires_cloudpickle = pytest.mark.skipif(
    not HAVE_CLOUDPICKLE, reason="cloudpickle not installed"
)
requires_blake3 = pytest.mark.skipif(
    not (HAVE_BLAKE3 or HAVE_CORE), reason="neither blake3 nor ppg3._core available"
)
requires_core = pytest.mark.skipif(
    not HAVE_CORE, reason="ppg3._core extension not built"
)
requires_golden = pytest.mark.skipif(
    not GOLDEN_DIR.is_dir(), reason="ppg3/tests/golden fixtures not present"
)
requires_nix = pytest.mark.skipif(
    shutil.which("nix") is None, reason="nix not on PATH"
)
