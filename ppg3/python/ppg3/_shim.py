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

Shim spec delivery (CONTRACT.md addendum, resolved — see STATUS.md "shim
stdin question"): the landed ``PreparedJob``/``Executor`` (``core/src/
executor.rs``) has no stdin-piping and no spec-file field — argv/env are the
only channels an ``Executor`` fills in. So `jobs.py` now delivers the spec
via **argv**, not stdin: the static part (transport, params, pickle_output,
or fetch url/hash — nothing path-shaped) travels as one base64 JSON blob
(``--spec-b64``); every real/virtual path travels as its own argv token
containing exactly one ``{in:NAME}``/``{out:NAME}``/``{tool:NAME}``
placeholder (``--in NAME <path>`` / ``--out NAME <path>`` / ``--tool NAME
<path>``, plus a bare ``--log-dir <path>``) — see `jobs.py`'s `_shim_argv`
for why the paths can't just live inside the base64 blob (the scheduler's
placeholder scanner and JSON's own `{`/`}` collide).

Stdin delivery (the literal CONTRACT.md prose, "the shim reads a JSON job
spec on stdin") is *also* still supported (`_read_spec`), both because it is
simpler to unit-test standalone and because it is not actually wrong — it is
just not what `jobs.py` uses now that the executor's real argv/env-only
interface is known. `main()` prefers `--spec-b64` argv delivery when present,
falling back to stdin otherwise.
"""

from __future__ import annotations

import base64
import json
import pickle
import sys
import traceback
import urllib.request
from typing import Any, Dict, List, Optional

# Kept as a local import so this module has zero required third-party deps
# at import time; `io.py` itself is stdlib-only.
from .io import JobIO

# Duplicated from jobs.DataJob.OUTPUT_NAME/FetchJob.OUTPUT_NAME (not
# imported — jobs.py pulls in tools.py/transport.py, which is more than
# this worker-side module should need to import just for two string
# constants).
DATA_PICKLE_NAME = "data.pickle"
FETCH_OUTPUT_NAME = "file"


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


def _parse_argv_spec(argv: List[str]) -> Optional[Dict[str, Any]]:
    """Reconstruct the internal spec dict (same shape `run_callback`/
    `run_fetch` expect from `_read_spec`) from the ``--spec-b64``/
    ``--in``/``--out``/``--tool``/``--log-dir`` argv delivery form (see the
    module docstring). Returns ``None`` (caller falls back to stdin) if
    ``--spec-b64`` is not present at all.
    """
    if "--spec-b64" not in argv:
        return None
    spec_b64: Optional[str] = None
    inputs: Dict[str, str] = {}
    outputs: Dict[str, str] = {}
    tools: Dict[str, str] = {}
    log_dir = ""
    i = 0
    while i < len(argv):
        tok = argv[i]
        if tok == "--spec-b64":
            spec_b64 = argv[i + 1]
            i += 2
        elif tok == "--in":
            inputs[argv[i + 1]] = argv[i + 2]
            i += 3
        elif tok == "--out":
            outputs[argv[i + 1]] = argv[i + 2]
            i += 3
        elif tok == "--tool":
            tools[argv[i + 1]] = argv[i + 2]
            i += 3
        elif tok == "--log-dir":
            log_dir = argv[i + 1]
            i += 2
        else:
            i += 1
    if spec_b64 is None:
        raise ShimError("--spec-b64 marker seen but no value found in argv")

    static_spec = json.loads(base64.b64decode(spec_b64))
    mode = static_spec.get("mode", "callback")
    if mode == "fetch":
        output_path = outputs.get(FETCH_OUTPUT_NAME)
        if output_path is None and outputs:
            output_path = next(iter(outputs.values()))
        return {
            "mode": "fetch",
            "fetch": {
                "url": static_spec["url"],
                "blake3": static_spec.get("blake3"),
                "output_path": output_path,
            },
        }
    return {
        "mode": "callback",
        "transport": static_spec["transport"],
        "io": {
            "inputs": inputs,
            "outputs": outputs,
            "tools": tools,
            "log_dir": log_dir,
            "params": static_spec.get("params", {}),
        },
        "pickle_output": static_spec.get("pickle_output", False),
    }


def main(argv=None) -> int:
    raw_argv = sys.argv[1:] if argv is None else argv
    try:
        spec = _parse_argv_spec(raw_argv)
        if spec is None:
            spec = _read_spec()
    except Exception as e:
        sys.stderr.write(f"ppg3._shim: invalid spec ({'argv' if '--spec-b64' in raw_argv else 'stdin'}): {e}\n")
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
