"""Callback transport selection across interpreters (§6.5, §6.6).

Two transports:

- **cloudpickle** (same-env fast path): the coordinator's interpreter is
  byte-identical to the job's declared ``PyEnv`` (in this implementation:
  the ``PyEnv`` is ``PyEnv.current()``, i.e. literally captures the running
  interpreter — see the deviation note in STATUS.md about nix-PyEnv
  same-env detection not being implemented). Used only if the ``cloudpickle``
  package is importable; falls back to source mode otherwise.
- **source** (cross-env / opaque-file / paranoid): the callback ships as
  extracted source text (or, for ``Source(...)``, a file reference) and is
  reconstructed by execing in a fresh module namespace inside the worker.
  Subject to the localscope free-variable check (§6.5) at definition time
  (function form) — the ``Source`` opaque-file form defers that check to the
  worker shim, since the coordinator never parses it (§6.6).

``paranoid=True`` forces source mode even for same-env jobs (§6.5: "the more
hermetic of the two... keeps the verified path hot").
"""

from __future__ import annotations

import base64
from typing import Any, Callable, Dict, List, Optional, Sequence, Union

from . import recipe
from .localscope import DefinitionError, check_localscope
from .tools import PyEnv


class Source:
    """Opaque file reference form (§6.6): the coordinator never imports (or
    even parses) the job code. ``ref`` is ``"path/to/file.py::qualname"``.
    """

    def __init__(self, ref: str, includes: Sequence[str] = ()):
        if "::" not in ref:
            raise DefinitionError(
                f"ppg3.Source(ref={ref!r}): expected 'path/to/file.py::qualname'"
            )
        path, qualname = ref.rsplit("::", 1)
        if not path or not qualname:
            raise DefinitionError(
                f"ppg3.Source(ref={ref!r}): expected 'path/to/file.py::qualname'"
            )
        self.path = path
        self.qualname = qualname
        self.includes = list(includes)

    def __repr__(self) -> str:
        return f"Source({self.path!r}::{self.qualname!r}, includes={self.includes!r})"

    def read_bytes(self) -> bytes:
        with open(self.path, "rb") as fh:
            return fh.read()

    def include_bytes(self) -> List[bytes]:
        out = []
        for inc in self.includes:
            with open(inc, "rb") as fh:
                out.append(fh.read())
        return out

    def recipe_hash(self) -> str:
        return recipe.recipe_hash_source(
            self.read_bytes(), self.qualname, self.include_bytes()
        )

    def transport_spec(self) -> Dict[str, Any]:
        return {
            "mode": "source_file",
            "path": self.path,
            "qualname": self.qualname,
            "includes": list(self.includes),
        }


def is_same_env(python_env: Optional[PyEnv]) -> bool:
    """Whether ``python_env`` is the coordinator's own interpreter.

    Deviation (see STATUS.md): only ``PyEnv.current()`` is recognized as
    same-env. A ``PyEnv.nix(...)`` pointing at the exact store path of the
    running interpreter would, per §6.5, also qualify, but detecting that
    requires resolving the nix ref at definition/selection time and is out
    of scope for this pass; nix ``PyEnv``s always take the source-mode path.
    """
    return python_env is not None and python_env.kind == "current"


def select_transport(
    callback: Union[Callable, Source],
    python_env: Optional[PyEnv],
    paranoid: bool = False,
) -> Dict[str, Any]:
    """Choose and build the transport spec + recipe hash for a job callback.

    Returns ``{"transport": {...}, "recipe": "<hash>", "localscope_modules": [...]}``.
    """
    if isinstance(callback, Source):
        return {
            "transport": callback.transport_spec(),
            "recipe": callback.recipe_hash(),
            "localscope_modules": [],  # checked worker-side, §6.6
        }

    fn = callback
    use_cloudpickle = is_same_env(python_env) and not paranoid
    if use_cloudpickle:
        try:
            import cloudpickle
        except ImportError:
            use_cloudpickle = False

    recipe_hash = recipe.recipe_hash(fn)

    if use_cloudpickle:
        blob = base64.b64encode(cloudpickle.dumps(fn)).decode("ascii")
        return {
            "transport": {"mode": "cloudpickle", "blob": blob},
            "recipe": recipe_hash,
            "localscope_modules": [],
        }

    report = check_localscope(fn)
    source, name = recipe.extract_source_and_name(fn)
    return {
        "transport": {"mode": "source", "source": source, "name": name},
        "recipe": recipe_hash,
        "localscope_modules": report.modules,
    }
