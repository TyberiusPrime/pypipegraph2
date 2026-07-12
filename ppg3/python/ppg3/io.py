"""``JobIO`` — the object passed to a job callback inside the sandbox
(§7.1, CONTRACT.md `_shim.py` paragraph).

All paths handed to job code are virtualized (``/ppg/...`` inside a real
sandbox; in this cold-exec-shim v1 they are whatever real paths the spec
JSON names — the executor/sandbox layer, owned by the Rust core, is
responsible for making those look like ``/ppg/...`` to the job process; see
STATUS.md).
"""

from __future__ import annotations

import pickle
from pathlib import Path
from typing import Any, Dict, Optional

from . import canon

# Per-process memo for `io.load()`, keyed by resolved absolute path of the
# pickle file. "Per-process" in the forkserver-template design (§6.3 tier 1);
# in this cold-exec-per-job v1 a process only ever handles one job, so this
# is mostly future-proofing, but costs nothing to have.
_load_memo: Dict[str, Any] = {}


class JobIOError(RuntimeError):
    pass


class JobIO:
    def __init__(
        self,
        inputs: Dict[str, str],
        outputs: Dict[str, str],
        tools: Dict[str, str],
        log_dir: str,
        params: Optional[Dict[str, Any]] = None,
    ):
        self._inputs = dict(inputs)
        self._outputs = dict(outputs)
        self._tools = dict(tools)
        self._log_dir = log_dir
        raw_params = params or {}
        self.params: Dict[str, Any] = {
            k: canon.decanonicalize_value(v) for k, v in raw_params.items()
        }

    # -- inputs -----------------------------------------------------------
    def input(self, name: str) -> str:
        try:
            return self._inputs[name]
        except KeyError:
            raise JobIOError(
                f"no declared input named {name!r}; declared inputs: "
                f"{sorted(self._inputs)}"
            ) from None

    def load(self, name: str) -> Any:
        """Unpickle a `DataJob`-produced artifact (or any pickled input),
        memoized per process by resolved path."""
        raw_path = self.input(name)
        p = Path(raw_path)
        if p.is_dir():
            p = p / "data.pickle"
        key = str(p.resolve()) if p.exists() else str(p)
        if key in _load_memo:
            return _load_memo[key]
        with open(p, "rb") as fh:
            obj = pickle.load(fh)
        _load_memo[key] = obj
        return obj

    # -- outputs ------------------------------------------------------------
    def path(self, name: Optional[str] = None) -> str:
        """Path to a declared output. If the job has exactly one declared
        output, ``name`` may be omitted."""
        if name is None:
            if len(self._outputs) != 1:
                raise JobIOError(
                    "io.path() with no name requires exactly one declared "
                    f"output; this job declares {sorted(self._outputs)}"
                )
            name = next(iter(self._outputs))
        try:
            return self._outputs[name]
        except KeyError:
            raise JobIOError(
                f"no declared output named {name!r}; declared outputs: "
                f"{sorted(self._outputs)}"
            ) from None

    # `out.path(...)` in the CONTRACT.md text is the same object as `io`;
    # kept as an alias for readability at call sites.
    out_path = path

    # -- tools --------------------------------------------------------------
    def tool(self, name: str) -> str:
        try:
            return self._tools[name]
        except KeyError:
            raise JobIOError(
                f"no declared tool named {name!r}; declared tools: "
                f"{sorted(self._tools)}"
            ) from None

    # -- misc -----------------------------------------------------------
    @property
    def log_dir(self) -> str:
        return self._log_dir
