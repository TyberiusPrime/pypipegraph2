"""``python -I -m ppg3._shim`` — the cold-exec worker entry point
(CONTRACT.md scope deviation: forkserver templates §6.4 are deferred; this
is the v1 executor payload).

Must be importable with **no third-party dependencies** except optionally
``cloudpickle``/``blake3`` (both imported lazily, only on the code paths
that need them) — it runs inside a job's declared ``PyEnv``, which may not
have anything else installed.

Reads a single JSON spec on stdin (CONTRACT.md: "The shim reads a JSON job
spec on stdin"):

.. code-block:: json

    {
      "mode": "callback",
      "transport": {"mode": "cloudpickle", "blob": "<base64>"},
      "io": {
        "inputs": {"name": "/real/path"},
        "outputs": {"name": "/real/path"},
        "tools": {"name": "/real/path"},
        "log_dir": "/real/path",
        "params": {"...": "..."}
      },
      "pickle_output": false
    }

or, for ``FetchJob``:

.. code-block:: json

    {"mode": "fetch", "fetch": {"url": "...", "blake3": "ab12..." , "output_path": "/real/path"}}

Exit 0 on success, 1 on failure (traceback on stderr). ``transport.mode`` is
one of ``"cloudpickle"``, ``"source"`` (function shipped as source text,
execed and looked up by name), ``"source_file"`` (the ``ppg3.Source(...)``
opaque-file form — read straight off disk and execed, qualname looked up,
localscope re-checked worker-side since the coordinator never parsed it,
§6.6).

Interface-gap note (see STATUS.md): CONTRACT.md's current ``PreparedJob``/
``Executor`` Rust types (still placeholders as of this writing) have no
explicit "spec on stdin" field. This shim assumes whatever hosts it (the
``NoneExecutor``/bwrap runner, once written) pipes the JSON spec to the
process's stdin, matching the CONTRACT.md prose literally; if the Rust side
lands on a different delivery mechanism (e.g. a mounted spec file), only the
``main()``/``_read_spec()`` functions here need to change.
"""

from __future__ import annotations

import base64
import json
import pickle
import sys
import traceback
import urllib.request
from typing import Any, Dict

# Kept as a local import so this module has zero required third-party deps
# at import time; `io.py` itself is stdlib-only.
from .io import JobIO

# Duplicated from jobs.DataJob.OUTPUT_NAME (not imported — jobs.py pulls in
# tools.py/transport.py, which is more than this worker-side module should
# need to import just for a string constant).
DATA_PICKLE_NAME = "data.pickle"


class ShimError(RuntimeError):
    pass


def _reconstruct_callback(transport: Dict[str, Any]):
    kind = transport.get("mode")
    if kind == "cloudpickle":
        import cloudpickle

        blob = base64.b64decode(transport["blob"])
        return cloudpickle.loads(blob)

    if kind == "source":
        source = transport["source"]
        name = transport["name"]
        ns: Dict[str, Any] = {"__name__": "ppg3_job_module"}
        code = compile(source, f"<ppg3-source:{name}>", "exec")
        exec(code, ns)
        fn = ns.get(name)
        if fn is None:
            raise ShimError(
                f"source-mode transport: {name!r} not found in module namespace "
                "after exec (did the source get truncated?)"
            )
        _recheck_localscope(fn)
        return fn

    if kind == "source_file":
        path = transport["path"]
        qualname = transport["qualname"]
        with open(path, "r", encoding="utf-8") as fh:
            src = fh.read()
        ns = {"__name__": "ppg3_job_module"}
        code = compile(src, path, "exec")
        exec(code, ns)
        parts = qualname.split(".")
        obj: Any = ns.get(parts[0])
        if obj is None:
            raise ShimError(
                f"ppg3.Source: {parts[0]!r} not found in {path!r} after exec"
            )
        for part in parts[1:]:
            obj = getattr(obj, part)
        _recheck_localscope(obj)
        return obj

    raise ShimError(f"unknown transport mode {kind!r}")


def _recheck_localscope(fn) -> None:
    """Re-run the localscope free-variable check worker-side (§6.6: the
    coordinator cannot analyze what it does not parse, for the ``Source``
    opaque-file form; harmless and cheap to redo for the plain-function
    ``source`` form too, which was already checked at definition time)."""
    from .localscope import check_localscope

    check_localscope(fn)


def _hash_file_blake3(path: str, chunk_size: int = 1 << 20) -> str:
    try:
        from ppg3 import _core  # type: ignore

        return _core.blake3_file(path)
    except Exception:
        pass
    try:
        import blake3 as _blake3_mod
    except ImportError:
        raise ShimError(
            "blake3 is required to verify fetch output but neither "
            "`ppg3._core` nor the `blake3` pip package is available"
        ) from None
    hasher = _blake3_mod.blake3()
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def run_callback(spec: Dict[str, Any]) -> int:
    transport = spec["transport"]
    io_spec = spec.get("io", {})
    fn = _reconstruct_callback(transport)
    job_io = JobIO(
        inputs=io_spec.get("inputs", {}),
        outputs=io_spec.get("outputs", {}),
        tools=io_spec.get("tools", {}),
        log_dir=io_spec.get("log_dir", ""),
        params=io_spec.get("params", {}),
    )
    result = fn(job_io)
    if spec.get("pickle_output"):
        output_path = io_spec.get("outputs", {}).get(DATA_PICKLE_NAME)
        if output_path is None:
            raise ShimError(
                "pickle_output=True but no 'data.pickle' output was declared"
            )
        with open(output_path, "wb") as fh:
            pickle.dump(result, fh)
    return 0


def run_fetch(spec: Dict[str, Any]) -> int:
    fetch = spec["fetch"]
    url = fetch["url"]
    expected = fetch.get("blake3")
    output_path = fetch["output_path"]
    import os

    out_dir = os.path.dirname(output_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with urllib.request.urlopen(url) as resp, open(output_path, "wb") as out_f:
        while True:
            chunk = resp.read(1 << 20)
            if not chunk:
                break
            out_f.write(chunk)
    digest = _hash_file_blake3(output_path)
    print(f"blake3:{digest}")
    if expected is not None and digest != expected:
        sys.stderr.write(
            f"ppg3._shim: fetch hash mismatch for {url}: expected {expected}, "
            f"got {digest}\n"
        )
        return 1
    return 0


def _read_spec() -> Dict[str, Any]:
    raw = sys.stdin.read()
    return json.loads(raw)


def main(argv=None) -> int:
    try:
        spec = _read_spec()
    except Exception as e:
        sys.stderr.write(f"ppg3._shim: invalid JSON spec on stdin: {e}\n")
        return 1

    mode = spec.get("mode", "callback")
    try:
        if mode == "fetch":
            return run_fetch(spec)
        elif mode == "callback":
            return run_callback(spec)
        else:
            sys.stderr.write(f"ppg3._shim: unknown mode {mode!r}\n")
            return 1
    except Exception:
        traceback.print_exc(file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
