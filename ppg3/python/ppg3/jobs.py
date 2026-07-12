"""Job classes + ``Graph`` (PPG3_DESIGN.md §7, CONTRACT.md "Scheduler" for the
``JobDef`` JSON shape).

Each job's ``.job_def(graph)`` method assembles the CONTRACT.md ``JobDef``
dict (destined for ``ppg3._core.run(jobs_json, ...)``); nothing here ever
imports the compiled extension directly — leaf-file/tool hashing needs real
I/O (stat-cache, optionally ``nix build``) but no Rust.

Rust-serde shape note (deviation, see STATUS.md): CONTRACT.md gives the
``InputRef``/``ExecTemplate``/``Retain`` Rust enum *shapes* but the core
crate's scheduler/executor modules are still placeholders (no
``#[serde(...)]`` attributes to check against). This module assumes plain
serde-default *externally tagged* JSON for those enums (unit variants as
bare strings, e.g. ``"InProcess"``/``"Default"``; struct/tuple variants as
``{"VariantName": {...}}``/``{"VariantName": value}``). If the Rust side
lands with different tagging, only the small `_retain_json`/exec_template/
`_lower_input` helpers below need updating.
"""

from __future__ import annotations

import os
import warnings
from typing import Any, Callable, Dict, Optional, Sequence, Set, Union

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
    ):
        self.stores = list(stores)
        self.default_python = default_python
        self.project_dir = str(project_dir)
        self.parallelism = dict(parallelism)
        self.frozen = frozen
        self.paranoid = paranoid
        self.jobs: Dict[str, "Job"] = {}
        self.data_job_ids: Set[str] = set()
        self._statcache: Optional[StatCache] = None

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
) -> Graph:
    """Create (and make current) a new ``Graph``. Module-level "current
    graph" like ppg2 — job constructors look it up implicitly."""
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
                    "argv": [self.python_env.executable_hint(), "-I", "-m", "ppg3._shim"],
                    "allow_network": False,
                }
            },
            "view": dict(self.view),
            "fixed_output": None,
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
# FetchJob
# --------------------------------------------------------------------------


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
                "only escape hatch; the source-patching side of TOFU is not "
                "implemented in this v1 (see STATUS.md) — pin the hash by hand."
            )
        job_id = name or view
        super().__init__(graph, job_id, {self.OUTPUT_NAME: view})
        self.url = url
        self.blake3 = blake3
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
                    "argv": [self.python_env.executable_hint(), "-I", "-m", "ppg3._shim"],
                    "allow_network": True,
                }
            },
            "view": dict(self.view),
            "fixed_output": self.blake3,
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
        }
