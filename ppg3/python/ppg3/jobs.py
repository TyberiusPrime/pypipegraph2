"""Job classes + ``Graph`` (PPG3_DESIGN.md §7, CONTRACT.md "Scheduler" for the
``JobDef`` JSON shape).

Each job's ``.job_def(graph)`` method assembles the CONTRACT.md ``JobDef``
dict (destined for ``ppg3._core.run(jobs_json, ...)``); nothing here ever
imports the compiled extension directly — leaf-file/tool hashing needs real
I/O (stat-cache, optionally ``nix build``) but no Rust.

Rust-serde shape note (reconciled against the real
``core/src/scheduler.rs``, see STATUS.md): the assumption made while that
module was still a placeholder — plain serde-default *externally tagged*
JSON (unit variants as bare strings, e.g. ``"InProcess"``/``"Default"``;
struct/tuple variants as ``{"VariantName": {...}}``/``{"VariantName":
value}``) — turned out to match the landed Rust exactly, so
`_retain_json`/`exec_template`/`_lower_input` below are unchanged. Two
things *did* need reconciling once `scheduler.rs` landed for real:

- ``JobDef.graph_job: bool`` (``#[serde(default)]`` in Rust, so its absence
  was never a hard error, but a ``GraphJob`` that omits it is silently
  treated as a plain ``InProcess`` "loader layer" job and gets
  ``run_in_process`` called on it instead of ``expand_graph_job`` — every
  ``job_def()`` below now sets it explicitly).
- Shim spec delivery (see ``_shim_argv`` below and STATUS.md): resolved by
  passing per-name real/virtual paths as individual argv tokens (each
  containing exactly one ``{in:NAME}``/``{out:NAME}``/``{tool:NAME}``
  placeholder, so the scheduler's naive first-``{``/first-``}`` token
  scanner in ``lower_argv`` resolves them correctly) rather than embedding
  paths inside the base64 spec blob.
"""

from __future__ import annotations

import base64
import inspect
import json
import os
import warnings
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple, Union

from . import canon, recipe
from .localscope import DefinitionError
from .statcache import StatCache
from .tools import PyEnv, ToolSpec
from .transport import Source, select_transport

SHIM_VERSION = "1"

_current_graph: Optional["Graph"] = None


# --------------------------------------------------------------------------
# Leaf input / parameter / resource / retention / store types
# --------------------------------------------------------------------------


class File:
    """A leaf file input, hashed via the stat-cache at run-lowering time."""

    def __init__(self, path: Union[str, "os.PathLike"]):
        self.path = str(path)

    def __repr__(self) -> str:
        return f"File({self.path!r})"


class Params:
    """A leaf parameter input: canonicalized per §5's closed type set."""

    def __init__(self, value: Any):
        self.value = value

    def canonical(self) -> Any:
        return canon.canonicalize_value(self.value, "$.params")

    def __repr__(self) -> str:
        return f"Params({self.value!r})"


class Resources:
    """Named resource-pool requirements, e.g. ``Resources(cores=4)``."""

    def __init__(self, **pools: int):
        self.pools: Dict[str, int] = {k: int(v) for k, v in pools.items()}

    def __repr__(self) -> str:
        return f"Resources({self.pools!r})"


class Retain:
    """``Retain.Default`` | ``Retain.Evict`` | ``Retain.Pin(name)`` (§7.3)."""

    class _Sentinel:
        def __init__(self, tag: str):
            self.tag = tag

        def __repr__(self) -> str:
            return f"Retain.{self.tag}"

    class Pin:
        def __init__(self, name: str):
            self.name = name

        def __repr__(self) -> str:
            return f"Retain.Pin({self.name!r})"


Retain.Default = Retain._Sentinel("Default")  # type: ignore[attr-defined]
Retain.Evict = Retain._Sentinel("Evict")  # type: ignore[attr-defined]


def _retain_json(retain: Any) -> Any:
    if retain is Retain.Default:
        return "Default"
    if retain is Retain.Evict:
        return "Evict"
    if isinstance(retain, Retain.Pin):
        return {"Pin": retain.name}
    raise DefinitionError(
        f"invalid retain= value {retain!r}; expected ppg3.Retain.Default, "
        "ppg3.Retain.Evict, or ppg3.Retain.Pin(name)"
    )


class Store:
    """A store entry for ``ppg3.new(stores=[...])`` (§4.1)."""

    def __init__(self, name: str, path: Union[str, "os.PathLike"], readonly: bool = False):
        self.name = name
        self.path = str(path)
        self.readonly = readonly

    def to_json(self) -> Dict[str, Any]:
        return {"name": self.name, "path": self.path, "readonly": self.readonly}

    def __repr__(self) -> str:
        return f"Store({self.name!r}, {self.path!r}, readonly={self.readonly!r})"


# --------------------------------------------------------------------------
# CommandJob argv placeholders
# --------------------------------------------------------------------------


class In:
    def __init__(self, name: str):
        self.name = name

    def _wire(self) -> str:
        return f"{{in:{self.name}}}"

    def __repr__(self) -> str:
        return f"In({self.name!r})"


class Out:
    def __init__(self, name: Optional[str] = None):
        self.name = name

    def _wire(self) -> str:
        return "{out}" if self.name is None else f"{{out:{self.name}}}"

    def __repr__(self) -> str:
        return f"Out({self.name!r})"


class Tool:
    def __init__(self, name: str):
        self.name = name

    def _wire(self) -> str:
        return f"{{tool:{self.name}}}"

    def __repr__(self) -> str:
        return f"Tool({self.name!r})"


def serialize_argv(argv: Sequence[Any]) -> list:
    """Lower a ``CommandJob`` argv template (mixed str/In/Out/Tool) to the
    wire-form list of strings (§6.2: placeholders, never real paths)."""
    out = []
    for i, item in enumerate(argv):
        if isinstance(item, str):
            out.append(item)
        elif isinstance(item, (In, Out, Tool)):
            out.append(item._wire())
        else:
            raise DefinitionError(
                f"$.argv[{i}]: CommandJob argv entries must be str/In/Out/Tool, "
                f"got {type(item).__name__} ({item!r})"
            )
    return out


# --------------------------------------------------------------------------
# Graph
# --------------------------------------------------------------------------


class Graph:
    def __init__(
        self,
        stores: Sequence[Store],
        default_python: Optional[PyEnv],
        project_dir: Union[str, "os.PathLike"],
        parallelism: Dict[str, int],
        frozen: bool,
        paranoid: bool,
        forkserver: bool = True,
    ):
        self.stores = list(stores)
        self.default_python = default_python
        self.project_dir = str(project_dir)
        self.parallelism = dict(parallelism)
        self.frozen = frozen
        self.paranoid = paranoid
        # §6.4: warm template processes for python FileJob/DataJob/FetchJob
        # dispatch. `False` disables it (`run.py` then passes an empty
        # `template_argv` to `ppg3._core.run`, which makes the Rust
        # `ForkserverExecutor` behave exactly like the pre-forkserver bare
        # `NoneExecutor` for every job — see STATUS.md).
        self.forkserver = forkserver
        self.jobs: Dict[str, "Job"] = {}
        self.data_job_ids: Set[str] = set()
        self._statcache: Optional[StatCache] = None
        # §6.7 watch mode: paths that should trigger a re-run of the
        # definition pass on change. Populated during job definition
        # (`Source` callback file + includes, recorded at __init__ time) and
        # during lowering (`ppg3.File(...)` leaf inputs, recorded from
        # `_lower_input` when `job_defs()`/`job_def()` runs — see
        # CONTRACT.md "Python package" watch addendum). Never includes the
        # pipeline script itself; `python -m ppg3 watch` adds that.
        self._watched_paths: Set[str] = set()

    def record_watched_path(self, path: Union[str, "os.PathLike"]) -> None:
        """Record a path that watch mode (§6.7) should poll for changes."""
        self._watched_paths.add(str(path))

    def watched_paths(self) -> List[str]:
        """Sorted, de-duplicated snapshot of every path recorded so far via
        :meth:`record_watched_path` (leaf ``File`` inputs + ``Source``
        callback files/includes). Does not include the pipeline script
        itself — the caller (``python -m ppg3 watch``) adds that."""
        return sorted(self._watched_paths)

    def add(self, job: "Job") -> "Job":
        existing = self.jobs.get(job.id)
        if existing is not None and existing is not job:
            raise DefinitionError(
                f"duplicate job id {job.id!r} (job ids are derived from `view`; "
                "pass name= to disambiguate two jobs that would otherwise share one)"
            )
        self.jobs[job.id] = job
        return job

    def statcache(self) -> StatCache:
        if self._statcache is None:
            self._statcache = StatCache(os.path.join(self.project_dir, "statcache.sqlite"))
        return self._statcache

    def job_defs(self) -> list:
        """Lower every job to its JobDef dict — what run.py hands to
        ``ppg3._core.run``. Requires real I/O (stat-cache, tool resolution)."""
        return [job.job_def(self) for job in self.jobs.values()]


def _is_ci() -> bool:
    for var in ("CI", "GITHUB_ACTIONS", "GITLAB_CI", "BUILDKITE"):
        if os.environ.get(var):
            return True
    return False


def new(
    stores: Optional[Sequence[Store]] = None,
    default_python: Optional[PyEnv] = None,
    project_dir: Union[str, "os.PathLike"] = ".ppg3",
    parallelism: Optional[Dict[str, int]] = None,
    frozen: Optional[bool] = None,
    paranoid: bool = False,
    forkserver: bool = True,
) -> Graph:
    """Create (and make current) a new ``Graph``. Module-level "current
    graph" like ppg2 — job constructors look it up implicitly.

    ``forkserver=True`` (the default, §6.4): python `FileJob`/`DataJob`/
    `FetchJob` dispatch runs through warm per-``(PyEnv, preload)`` template
    processes instead of a cold ``python -I -m ppg3._shim`` exec per job.
    Pass ``forkserver=False`` to opt out and get the old cold-exec-per-job
    behavior unconditionally (e.g. for isolating whether a bug is
    forkserver-related).
    """
    global _current_graph
    if frozen is None:
        try:
            interactive = os.isatty(0)
        except (OSError, ValueError):
            interactive = False
        frozen = (not interactive) or _is_ci()
    if parallelism is None:
        parallelism = {"cores": os.cpu_count() or 1}
    graph = Graph(
        stores=stores or [],
        default_python=default_python,
        project_dir=project_dir,
        parallelism=parallelism,
        frozen=frozen,
        paranoid=paranoid,
        forkserver=forkserver,
    )
    _current_graph = graph
    return graph


def _require_current_graph() -> Graph:
    if _current_graph is None:
        raise DefinitionError(
            "no active ppg3 graph — call ppg3.new(...) before defining jobs"
        )
    return _current_graph


def _derive_id(view: Any, kind: str, name: Optional[str] = None) -> str:
    if name:
        return name
    if isinstance(view, str):
        return view
    if isinstance(view, dict) and view:
        return "+".join(sorted(view.values()))
    raise DefinitionError(
        f"{kind}: cannot derive a stable job id from view={view!r}; pass name="
    )


# --------------------------------------------------------------------------
# Input lowering
# --------------------------------------------------------------------------


class OutputRef:
    """``job["output_name"]`` — a named-subset reference to a parent's output."""

    def __init__(self, job: "Job", name: str):
        self.job = job
        self.name = name

    def __repr__(self) -> str:
        return f"{self.job!r}[{self.name!r}]"


def _lower_input(input_name: str, value: Any, graph: Graph) -> Dict[str, Any]:
    if isinstance(value, OutputRef):
        return {"JobSubset": {"id": value.job.id, "names": [value.name]}}
    if isinstance(value, Job):
        return {"Job": {"id": value.id}}
    if isinstance(value, File):
        # §6.7 watch mode: leaf File inputs are watched paths, recorded here
        # at lowering time (job_defs()/job_def(), called from run()).
        graph.record_watched_path(value.path)
        h = graph.statcache().hash_file(value.path)
        return {"Leaf": {"hash": h}}
    if isinstance(value, Params):
        h = canon.input_key_local(value.canonical())
        return {"Leaf": {"hash": h}}
    raise DefinitionError(
        f"input {input_name!r}: expected a Job, job[\"output\"], ppg3.File(...), "
        f"or ppg3.Params(...), got {type(value).__name__} ({value!r})"
    )


def _lower_tools(tools: Sequence[ToolSpec]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for t in tools:
        if t.name in out:
            raise DefinitionError(f"duplicate tool name {t.name!r}")
        out[t.name] = t.resolve().hash
    return out


def _runtime_doc(python_env: Optional[PyEnv]) -> Dict[str, Any]:
    if python_env is None:
        return {"python_env": None, "preload": [], "shim": "0"}
    res = python_env.resolve()
    return {
        "python_env": res.hash,
        "preload": list(python_env.preload),
        "shim": SHIM_VERSION,
    }


# --------------------------------------------------------------------------
# Shim spec delivery (see module docstring + STATUS.md "shim stdin question")
# --------------------------------------------------------------------------


def _b64_json(obj: Any) -> str:
    return base64.b64encode(json.dumps(obj, sort_keys=True).encode("utf-8")).decode("ascii")


def _mounted_input_names(inputs: Dict[str, Any]) -> List[str]:
    """Input names backed by a real mounted path (`{in:NAME}` resolves for
    ``Job``/``JobSubset`` refs only — a `Leaf` ref has no mount and the
    scheduler's `{in:NAME}` placeholder resolution errors the job if asked
    for one, see `resolve_placeholder` in scheduler.rs)."""
    return sorted(n for n, v in inputs.items() if isinstance(v, (Job, OutputRef)))


def _leaf_params(inputs: Dict[str, Any]) -> Dict[str, Any]:
    """``Params(...)``-typed inputs, canonicalized, keyed by input name —
    what ends up as ``io.params`` inside the shim (§7.1)."""
    return {n: v.canonical() for n, v in inputs.items() if isinstance(v, Params)}


def _shim_argv(
    python_env: PyEnv,
    static_spec: Dict[str, Any],
    mounted_inputs: Sequence[str],
    output_names: Sequence[str],
    tool_names: Sequence[str],
) -> List[str]:
    """Build the ``python -I -m ppg3._shim`` argv (CONTRACT.md addendum,
    "Shim spec delivery"): the static part of the spec (transport, params,
    pickle_output/fetch url+hash — no paths) travels as one base64 JSON
    blob (``--spec-b64``); every real/virtual path travels as its own argv
    token carrying exactly one ``{in:NAME}``/``{out:NAME}``/``{tool:NAME}``
    placeholder so the scheduler's `lower_argv` (and, for `NoneExecutor`,
    its `/ppg/` string-rewrite) can resolve it — embedding those
    placeholders *inside* the JSON blob does not work, since `lower_argv`
    scans for the first ``{``/``}`` pair in the whole token and JSON's own
    structural braces collide with that (see STATUS.md for the trace)."""
    argv = [
        python_env.executable_hint(),
        "-I",
        "-m",
        "ppg3._shim",
        "--spec-b64",
        _b64_json(static_spec),
    ]
    for name in mounted_inputs:
        argv += ["--in", name, f"{{in:{name}}}"]
    for name in output_names:
        argv += ["--out", name, f"{{out:{name}}}"]
    for name in tool_names:
        argv += ["--tool", name, f"{{tool:{name}}}"]
    argv += ["--log-dir", "/ppg/log"]
    return argv


# --------------------------------------------------------------------------
# Job base
# --------------------------------------------------------------------------


class Job:
    kind = "base"

    def __init__(self, graph: Graph, job_id: str, view: Dict[str, str]):
        self.graph = graph
        self.id = job_id
        self.view = dict(view) if view else {}
        graph.add(self)

    def __getitem__(self, name: str) -> OutputRef:
        return OutputRef(self, name)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.id!r})"

    def job_def(self, graph: Optional[Graph] = None) -> Dict[str, Any]:
        raise NotImplementedError


# --------------------------------------------------------------------------
# FileJob
# --------------------------------------------------------------------------


class FileJob(Job):
    kind = "file"
    # Overridden to True by DataJob — see its class docstring.
    _pickle_output = False

    def __init__(
        self,
        view: Dict[str, str],
        run: Union[Callable, Source],
        tools: Sequence[ToolSpec] = (),
        inputs: Optional[Dict[str, Any]] = None,
        env: Optional[Dict[str, str]] = None,
        resources: Optional[Resources] = None,
        retain: Any = None,
        python: Optional[PyEnv] = None,
        store: Optional[str] = None,
        name: Optional[str] = None,
    ):
        if not isinstance(view, dict) or not view:
            raise DefinitionError(
                "FileJob(view=...) must be a non-empty dict of output-name -> "
                "view-relative path"
            )
        graph = _require_current_graph()
        python_env = python or graph.default_python
        if python_env is None:
            raise DefinitionError(
                "FileJob requires a PyEnv: pass python=... or set "
                "ppg3.new(default_python=...)"
            )
        job_id = name or _derive_id(view, "FileJob")
        super().__init__(graph, job_id, view)
        self.run = run
        if isinstance(run, Source):
            # §6.7 watch mode: Source callback files are watched paths,
            # recorded at definition time (as opposed to leaf File inputs,
            # recorded at lowering time in `_lower_input`).
            graph.record_watched_path(run.path)
            for inc in run.includes:
                graph.record_watched_path(inc)
        self.tools = list(tools)
        self.inputs = dict(inputs or {})
        self.env = dict(env or {})
        self.resources = resources.pools if isinstance(resources, Resources) else {}
        self.retain = retain if retain is not None else Retain.Default
        self.python_env = python_env
        self.store = store
        self._transport = select_transport(run, python_env, paranoid=graph.paranoid)

    def job_def(self, graph: Optional[Graph] = None) -> Dict[str, Any]:
        graph = graph or self.graph
        inputs_json = {
            n: _lower_input(n, v, graph) for n, v in self.inputs.items()
        }
        static_spec = {
            "mode": "callback",
            "transport": self._transport["transport"],
            "pickle_output": self._pickle_output,
            "params": _leaf_params(self.inputs),
        }
        argv = _shim_argv(
            self.python_env,
            static_spec,
            _mounted_input_names(self.inputs),
            sorted(self.view.keys()),
            [t.name for t in self.tools],
        )
        return {
            "id": self.id,
            "recipe": self._transport["recipe"],
            "inputs": inputs_json,
            "tools": _lower_tools(self.tools),
            "runtime": _runtime_doc(self.python_env),
            "env": dict(self.env),
            "outputs_declared": sorted(self.view.keys()),
            "resources": dict(self.resources),
            "store_target": self.store,
            "retain": _retain_json(self.retain),
            "exec_template": {
                "Argv": {
                    "argv": argv,
                    "allow_network": False,
                }
            },
            "view": dict(self.view),
            "fixed_output": None,
            "graph_job": False,
        }


# --------------------------------------------------------------------------
# CommandJob
# --------------------------------------------------------------------------


class CommandJob(Job):
    kind = "command"

    def __init__(
        self,
        view: Dict[str, str],
        argv: Sequence[Any],
        tools: Sequence[ToolSpec] = (),
        inputs: Optional[Dict[str, Any]] = None,
        env: Optional[Dict[str, str]] = None,
        resources: Optional[Resources] = None,
        retain: Any = None,
        store: Optional[str] = None,
        name: Optional[str] = None,
    ):
        if not isinstance(view, dict) or not view:
            raise DefinitionError(
                "CommandJob(view=...) must be a non-empty dict of output-name -> "
                "view-relative path"
            )
        graph = _require_current_graph()
        job_id = name or _derive_id(view, "CommandJob")
        super().__init__(graph, job_id, view)
        self.argv_template = serialize_argv(argv)
        self.tools = list(tools)
        self.inputs = dict(inputs or {})
        self.env = dict(env or {})
        self.resources = resources.pools if isinstance(resources, Resources) else {}
        self.retain = retain if retain is not None else Retain.Default
        self.store = store
        self._recipe = recipe.recipe_hash_command(self.argv_template)

    def job_def(self, graph: Optional[Graph] = None) -> Dict[str, Any]:
        graph = graph or self.graph
        inputs_json = {
            n: _lower_input(n, v, graph) for n, v in self.inputs.items()
        }
        return {
            "id": self.id,
            "recipe": self._recipe,
            "inputs": inputs_json,
            "tools": _lower_tools(self.tools),
            "runtime": {"python_env": None, "preload": [], "shim": "0"},
            "env": dict(self.env),
            "outputs_declared": sorted(self.view.keys()),
            "resources": dict(self.resources),
            "store_target": self.store,
            "retain": _retain_json(self.retain),
            "exec_template": {
                "Argv": {"argv": list(self.argv_template), "allow_network": False}
            },
            "view": dict(self.view),
            "fixed_output": None,
            "graph_job": False,
        }


# --------------------------------------------------------------------------
# DataJob
# --------------------------------------------------------------------------


class DataJob(FileJob):
    """A ``FileJob`` producing a single pickled artifact (§6.3 tier 1).

    The shim pickles the callback's return value to ``data.pickle``;
    consumers declare this job as an input and call ``io.load(name)``.
    """

    kind = "data"
    OUTPUT_NAME = "data.pickle"
    _pickle_output = True

    def __init__(self, view: Union[str, Dict[str, str]], run: Union[Callable, Source], **kwargs):
        if isinstance(view, str):
            view_map = {self.OUTPUT_NAME: view}
        elif isinstance(view, dict):
            if set(view.keys()) != {self.OUTPUT_NAME}:
                raise DefinitionError(
                    f"DataJob(view=...) as a dict must have exactly the key "
                    f"{self.OUTPUT_NAME!r}; got {sorted(view.keys())}. Pass a "
                    "plain str for the common case."
                )
            view_map = dict(view)
        else:
            raise DefinitionError(
                "DataJob(view=...) must be a str (view-relative path) or "
                f"{{{self.OUTPUT_NAME!r}: path}}"
            )
        super().__init__(view=view_map, run=run, **kwargs)
        self.graph.data_job_ids.add(self.id)


# --------------------------------------------------------------------------
# FetchJob + TOFU call-site recording (§7.6)
# --------------------------------------------------------------------------

# Absolute directory of the `ppg3` package itself — used by `_record_call_site`
# below to find the first stack frame *outside* the package, i.e. the user's
# own call site, per §7.6's TOFU DECISION ("at definition time each FetchJob
# records its call site (`inspect` — file, line)").
_PKG_DIR = os.path.dirname(os.path.abspath(__file__))


def _record_call_site() -> Optional[Tuple[str, int]]:
    """Best-effort ``(file, lineno)`` of the first stack frame outside the
    ``ppg3`` package, walking up from this function's caller (``FetchJob.
    __init__``). Returns ``None`` if no such frame exists (e.g. called from
    an interactive ``-c``/REPL frame with no real file, or the stack was
    exhausted) — the TOFU pass (``tofu.py``) treats that as "cannot patch,
    goes to the table" rather than erroring."""
    frame = inspect.currentframe()
    try:
        if frame is None:  # pragma: no cover - not all Python impls have frames
            return None
        frame = frame.f_back  # the caller of _record_call_site (FetchJob.__init__)
        while frame is not None:
            filename = os.path.abspath(frame.f_code.co_filename)
            if filename != _PKG_DIR and not filename.startswith(_PKG_DIR + os.sep):
                if not os.path.isfile(filename):
                    return None
                return (filename, frame.f_lineno)
            frame = frame.f_back
        return None
    finally:
        del frame


class FetchJob(Job):
    kind = "fetch"
    OUTPUT_NAME = "file"

    def __init__(
        self,
        view: str,
        url: str,
        blake3: Optional[str] = None,
        retain: Any = None,
        store: Optional[str] = None,
        name: Optional[str] = None,
    ):
        if not isinstance(view, str):
            raise DefinitionError("FetchJob(view=...) must be a single path string")
        graph = _require_current_graph()
        if blake3 is None and graph.frozen:
            raise DefinitionError(
                f"FetchJob(view={view!r}, url={url!r}): blake3=None is rejected "
                "in --frozen mode (the default outside an interactive terminal, "
                "or under CI). TOFU (trust-on-first-use, §7.6) is an interactive-"
                "only escape hatch; --frozen never TOFUs — pin the hash by hand "
                "or run interactively once to let ppg3 patch it in for you."
            )
        job_id = name or view
        super().__init__(graph, job_id, {self.OUTPUT_NAME: view})
        self.url = url
        self.blake3 = blake3
        # §7.6 TOFU: recorded unconditionally (cheap), consumed only for
        # jobs actually defined with blake3=None (see tofu.py). Multiple
        # FetchJobs may share one call site (a loop over URLs) — detected
        # later by `tofu.run_tofu_pass`'s grouping, not here.
        self._call_site = _record_call_site()
        self.retain = retain if retain is not None else Retain.Default
        self.store = store
        python_env = graph.default_python
        if python_env is None:
            raise DefinitionError(
                "FetchJob requires ppg3.new(default_python=...) to run its "
                "fetch shim invocation"
            )
        self.python_env = python_env
        self._recipe = canon.input_key_local(
            canon.canonicalize_value({"kind": "fetch", "url": url}, "$.fetch")
        )

    def job_def(self, graph: Optional[Graph] = None) -> Dict[str, Any]:
        static_spec = {"mode": "fetch", "url": self.url, "blake3": self.blake3}
        argv = _shim_argv(self.python_env, static_spec, [], [self.OUTPUT_NAME], [])
        return {
            "id": self.id,
            "recipe": self._recipe,
            "inputs": {},
            "tools": {},
            "runtime": _runtime_doc(self.python_env),
            "env": {},
            "outputs_declared": [self.OUTPUT_NAME],
            "resources": {},
            "store_target": self.store,
            "retain": _retain_json(self.retain),
            "exec_template": {
                "Argv": {
                    "argv": argv,
                    "allow_network": True,
                }
            },
            "view": dict(self.view),
            "fixed_output": self.blake3,
            "graph_job": False,
        }


# --------------------------------------------------------------------------
# GraphJob
# --------------------------------------------------------------------------


class GraphJob(Job):
    """Dynamic graph expansion (§7.4, ppg2's JobGeneratingJob). Runs
    in-process via ``HostCallbacks.expand_graph_job``; recorded but not
    keyed (its recipe hash appears in the run report only)."""

    kind = "graph"

    def __init__(self, fn: Callable, name: Optional[str] = None):
        graph = _require_current_graph()
        job_id = name or getattr(fn, "__qualname__", None)
        if not job_id:
            raise DefinitionError("GraphJob(fn) requires name= if fn has no __qualname__")
        super().__init__(graph, job_id, {})
        self.fn = fn
        self._recipe = recipe.recipe_hash(fn)

    def job_def(self, graph: Optional[Graph] = None) -> Dict[str, Any]:
        return {
            "id": self.id,
            "recipe": self._recipe,
            "inputs": {},
            "tools": {},
            "runtime": {"python_env": None, "preload": [], "shim": "0"},
            "env": {},
            "outputs_declared": [],
            "resources": {},
            "store_target": None,
            "retain": _retain_json(Retain.Default),
            "exec_template": "InProcess",
            "view": {},
            "fixed_output": None,
            # The one JobDef field that actually distinguishes a GraphJob
            # from a plain InProcess "loader layer" job (core/src/scheduler.rs
            # module docs) — without this the scheduler calls
            # `run_in_process` instead of `expand_graph_job`.
            "graph_job": True,
        }


# --------------------------------------------------------------------------
# UnsandboxedJob
# --------------------------------------------------------------------------


class UnsandboxedJob(Job):
    """The explicit escape hatch (§6.3 tier 3): forks from the coordinator,
    sees loader-layer results via COW, but is still publish-time
    determinism-checked. Warned about at definition time."""

    kind = "unsandboxed"

    def __init__(
        self,
        run: Callable,
        view: Optional[Union[str, Dict[str, str]]] = None,
        inputs: Optional[Dict[str, Any]] = None,
        env: Optional[Dict[str, str]] = None,
        resources: Optional[Resources] = None,
        retain: Any = None,
        name: Optional[str] = None,
    ):
        graph = _require_current_graph()
        if isinstance(view, dict):
            view_map = dict(view)
        elif isinstance(view, str):
            view_map = {"out": view}
        else:
            view_map = {}
        job_id = name
        if job_id is None and view_map:
            job_id = _derive_id(view_map, "UnsandboxedJob")
        if job_id is None:
            job_id = getattr(run, "__qualname__", None)
        if job_id is None:
            raise DefinitionError(
                "UnsandboxedJob requires name= when it has no view and run has "
                "no __qualname__"
            )
        super().__init__(graph, job_id, view_map)
        warnings.warn(
            f"UnsandboxedJob {self.id!r}: runs unsandboxed, forked from the "
            "coordinator (§6.3 tier 3) — marked sandboxed=false in its "
            "manifest and still determinism-checked at publish, but it "
            "inherits ppg2's fork-under-threads hazards. Prefer DataJob "
            "unless you genuinely need COW-shared in-process state.",
            UserWarning,
            stacklevel=2,
        )
        self.run = run
        self.inputs = dict(inputs or {})
        self.env = dict(env or {})
        self.resources = resources.pools if isinstance(resources, Resources) else {}
        self.retain = retain if retain is not None else Retain.Default
        self._recipe = recipe.recipe_hash(run)

    def job_def(self, graph: Optional[Graph] = None) -> Dict[str, Any]:
        graph = graph or self.graph
        inputs_json = {
            n: _lower_input(n, v, graph) for n, v in self.inputs.items()
        }
        return {
            "id": self.id,
            "recipe": self._recipe,
            "inputs": inputs_json,
            "tools": {},
            "runtime": {"python_env": None, "preload": [], "shim": "0"},
            "env": dict(self.env),
            "outputs_declared": sorted(self.view.keys()),
            "resources": dict(self.resources),
            "store_target": None,
            "retain": _retain_json(self.retain),
            "exec_template": "InProcess",
            "view": dict(self.view),
            "fixed_output": None,
            "graph_job": False,
        }
