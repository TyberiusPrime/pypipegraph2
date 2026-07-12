"""Thin wrapper isolating the ``ppg3._core`` PyO3 extension import.

Every other module in this package is importable and testable without the
compiled extension (it does not exist yet — the Rust core is being written
concurrently). Only ``run.py``'s actual ``run()``/``write_generation()``
calls need it, and they go through :func:`get_core` so the failure mode is
one clear error message instead of an ``ImportError`` surfacing from deep
inside some unrelated import chain.
"""

from __future__ import annotations

from typing import Any


class CoreNotAvailable(RuntimeError):
    pass


def get_core() -> Any:
    """Import and return the ``ppg3._core`` extension module, or raise
    :class:`CoreNotAvailable` with a clear, actionable message."""
    try:
        from ppg3 import _core  # type: ignore
    except ImportError as e:
        raise CoreNotAvailable(
            "ppg3._core (the compiled Rust extension) is not built/importable. "
            "Build it from ppg3/python with `maturin develop` (see "
            "ppg3/python/pyproject.toml). Everything in the ppg3 python "
            "package except the final run()/write_generation() calls works "
            "without it."
        ) from e
    return _core
