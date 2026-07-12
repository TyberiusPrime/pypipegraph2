"""``ppg3.run()`` — lowers a ``Graph`` to ``JobDef`` JSON, calls
``ppg3._core.run``, then assembles a ``ViewSpec`` and calls
``ppg3._core.write_generation`` (CONTRACT.md "PyO3 boundary" / "Python
package").

This module is the one place in the package that touches the compiled
extension (via :mod:`ppg3._bridge`).
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

from ._bridge import get_core
from .io import JobIO
from .jobs import File, Graph, GraphJob, Params, UnsandboxedJob


class RunResult:
    """The outcome of a :func:`run` call. ``.raw`` is the full decoded
    ``RunReport`` JSON (CONTRACT.md's ``built``/``hits``/``failed``/
    ``job_entries``) for anything not surfaced as a dedicated attribute."""

    def __init__(self, report: Dict[str, Any], generation: Optional[int] = None):
        self.built: List[str] = list(report.get("built", []))
        self.hits: List[str] = list(report.get("hits", []))
        self.failed: Dict[str, str] = dict(report.get("failed", {}))
        self.job_entries: Dict[str, Any] = dict(report.get("job_entries", {}))
        self.generation = generation
        self.raw = report

    def __repr__(self) -> str:
        return (
            f"RunResult(built={len(self.built)}, hits={len(self.hits)}, "
            f"failed={len(self.failed)}, generation={self.generation!r})"
        )


class PPGRunError(RuntimeError):
    """Raised by :func:`run` when ``report.failed`` is non-empty. The view
    is deliberately *not* updated on partial failure (§11: a generation is a
    consistent, all-or-nothing snapshot) — ``result.generation`` is
    ``None``; ``result.failed``/``result.raw`` carry the full report so
    callers can inspect what broke."""

    def __init__(self, result: RunResult):
        names = sorted(result.failed)
        preview = ", ".join(f"{n!r}: {result.failed[n]}" for n in names[:5])
        more = f" (+{len(names) - 5} more)" if len(names) > 5 else ""
        super().__init__(f"ppg3 run: {len(result.failed)} job(s) failed: {preview}{more}")
        self.result = result


class RunCallbacks:
    """The two coarse Rust->Python callbacks of CONTRACT.md's PyO3 boundary
    (§8.1 rule 1): ``expand_graph_job`` / ``run_in_process``. Passed as the
    ``py_callbacks`` object to ``ppg3._core.run``.
    """

    def __init__(self, graph: Graph):
        self.graph = graph
        # Memoizes `run_in_process` by (job_id, ik) within this run — a
        # `HostCallbacks::run_in_process` call carries the same key document
        # a store-backed job would hash; since InProcess jobs never consult
        # the StoreSet themselves (core/src/scheduler.rs's `dispatch_job`
        # only does `derive_key` for them, no `storeset.lookup`), any
        # caching is this callback's own responsibility. Mostly relevant if
        # a `GraphJob` expansion could cause the same `UnsandboxedJob` id to
        # be dispatched more than once in pathological graphs; cheap safety
        # net either way.
        self._inprocess_memo: Dict[str, None] = {}

    def expand_graph_job(self, job_id: str) -> str:
        """Run a ``GraphJob``'s callback in-process; it may call job
        constructors, which register into this same graph. Returns the
        JobDef JSON list of newly-added jobs."""
        job = self.graph.jobs.get(job_id)
        if job is None or not isinstance(job, GraphJob):
            raise RuntimeError(f"expand_graph_job: no GraphJob with id {job_id!r}")
        before = set(self.graph.jobs.keys())
        job.fn()
        new_ids = set(self.graph.jobs.keys()) - before
        new_defs = [self.graph.jobs[i].job_def(self.graph) for i in sorted(new_ids)]
        return json.dumps(new_defs)

    def run_in_process(self, job_id: str, key_doc_json: str) -> None:
        """Run an ``UnsandboxedJob``'s callback in-process (loader layer,
        §6.3 tier 3).

        Known limitation (see STATUS.md "run_in_process real-path gap"):
        ``HostCallbacks::run_in_process(job_id, key_doc)`` only carries the
        *declared* input hashes (the key document), never resolved real
        filesystem paths — that mapping only exists inside the Rust
        scheduler's dispatch-time state (`CompletedInfo`), which is not
        exposed across the PyO3 boundary (`ExecTemplate::InProcess` jobs get
        no `PreparedJob`/mounts at all — see `dispatch_job` in
        scheduler.rs). This implementation therefore only supports
        ``UnsandboxedJob``s whose declared inputs are `File`/`Params` (leaf)
        refs: a `File` input's real on-disk path is already known host-side
        (it was never mounted for *any* job kind, sandboxed or not — see
        `resolve_placeholder`'s `Leaf` case in scheduler.rs erroring on
        `{in:NAME}`), and a `Params` input's value is already known from
        `job.inputs` itself. A `Job`/`JobSubset`-typed input raises
        ``NotImplementedError`` naming the gap.
        """
        job = self.graph.jobs.get(job_id)
        if job is None or not isinstance(job, UnsandboxedJob):
            raise RuntimeError(f"run_in_process: no UnsandboxedJob with id {job_id!r}")
        if job_id in self._inprocess_memo:
            return
        json.loads(key_doc_json)  # currently unused beyond validating shape

        inputs: Dict[str, str] = {}
        params: Dict[str, Any] = {}
        for name, value in job.inputs.items():
            if isinstance(value, File):
                inputs[name] = value.path
            elif isinstance(value, Params):
                params[name] = value.canonical()
            else:
                raise NotImplementedError(
                    f"UnsandboxedJob {job_id!r}: run_in_process cannot resolve a real "
                    f"path for {type(value).__name__}-typed input {name!r} (only "
                    "File/Params leaf inputs are supported in this v1 — see "
                    "STATUS.md 'run_in_process real-path gap')."
                )

        job_io = JobIO(inputs=inputs, outputs={}, tools={}, log_dir="", params=params)
        job.run(job_io)
        self._inprocess_memo[job_id] = None


def _write_project_config(graph: Graph) -> None:
    """``<project_dir>/config.json`` (CONTRACT.md "Additive clarification:
    `--keep-generations` and `.ppg3/config.json`"): the standalone CLI has
    no ``ppg3.new(stores=...)`` call to hand it a ready ``StoreSet``, so
    this file — written here, the one place a project's store list is
    known on the Python side — lets it reconstruct one from disk."""
    os.makedirs(graph.project_dir, exist_ok=True)
    config_path = os.path.join(graph.project_dir, "config.json")
    payload = {"stores": [s.to_json() for s in graph.stores]}
    with open(config_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True)
        fh.write("\n")


def run(
    graph: Optional[Graph] = None,
    project_id: str = "default",
    ephemeral: bool = False,
) -> RunResult:
    """Run `graph` (or the current graph) to completion and update the view.

    Calls, in order: writes ``.ppg3/config.json``, ``_core.open_stores``
    (from ``graph.stores``), ``_core.run`` (the lowered ``JobDef`` list +
    parallelism config + :class:`RunCallbacks`). On success (no failed
    jobs), assembles a ``ViewSpec`` from the run report's ``job_entries``
    (``id -> (ik, oh)``, cross-referenced against ``_core.lookup`` for the
    *store index* each job's output actually landed in — the report itself
    doesn't carry that, see ``py/src/lib.rs``'s ``lookup`` doc comment) and
    every job's ``view`` map, then calls ``_core.write_generation``.

    On any job failure, the view is left untouched (§11: a generation must
    be a consistent, all-or-nothing snapshot) and :class:`PPGRunError` is
    raised with the full report attached.

    Raises :class:`ppg3._bridge.CoreNotAvailable` if the extension isn't
    built — every other part of this package works without it.
    """
    from .jobs import _current_graph as default_graph

    graph = graph or default_graph
    if graph is None:
        raise RuntimeError("ppg3.run(): no graph — call ppg3.new(...) first")

    core = get_core()

    _write_project_config(graph)
    work_dir = os.path.join(graph.project_dir, "work")
    os.makedirs(work_dir, exist_ok=True)

    jobs_json = json.dumps(graph.job_defs())
    parallelism_json = json.dumps(graph.parallelism)
    # Bare-array shape per CONTRACT.md's PyO3 boundary paragraph
    # (`open_stores(config [{"name","path","readonly"}...])`) — distinct
    # from `.ppg3/config.json`'s `{"stores": [...]}` wrapper object above.
    stores_config_json = json.dumps([s.to_json() for s in graph.stores])

    handle = core.open_stores(stores_config_json)
    callbacks = RunCallbacks(graph)
    report_json = core.run(handle, jobs_json, parallelism_json, callbacks, work_dir)
    report = json.loads(report_json)

    if report.get("failed"):
        raise PPGRunError(RunResult(report, generation=None))

    job_entries = report.get("job_entries", {})
    view_entries = []
    for job_id, job in graph.jobs.items():
        if not job.view:
            continue
        entry = job_entries.get(job_id)
        if entry is None:
            continue
        ik, oh = entry
        looked = core.lookup(handle, ik)
        if looked is None:
            raise RuntimeError(
                f"internal error: job {job_id!r} reported input key {ik!r} in "
                "its run report but a post-run lookup() found no manifest for it"
            )
        info = json.loads(looked)
        store_index = info["store_index"]
        for _output_name, view_path in job.view.items():
            # The job actually wrote to `/ppg/out/<view_path>` (scheduler.rs
            # `resolve_placeholder`'s `{out:NAME}` case resolves NAME via
            # `job.view`), so `view_path` is *also* the file's relative path
            # within the entry's `data/` — i.e. a content-manifest key.
            view_entries.append(
                {
                    "view_rel_path": view_path,
                    "oh": oh,
                    "path_within_entry": view_path,
                    "store_index": store_index,
                }
            )
    view_spec_json = json.dumps({"entries": view_entries})

    generation = core.write_generation(
        handle, graph.project_dir, project_id, view_spec_json, ephemeral
    )
    return RunResult(report, generation=generation)
