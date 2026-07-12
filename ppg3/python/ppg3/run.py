"""``ppg3.run()`` — lowers a ``Graph`` to ``JobDef`` JSON, calls
``ppg3._core.run``, then assembles a ``ViewSpec`` and calls
``ppg3._core.write_generation`` (CONTRACT.md "PyO3 boundary" / "Python
package").

This module is the one place in the package that touches the compiled
extension (via :mod:`ppg3._bridge`), and it genuinely cannot be exercised
end-to-end yet — ``ppg3._core`` is being written concurrently by another
agent (see ``ppg3/core/src/*.rs``, currently placeholders for scheduler/
executor/views). The call shapes below follow CONTRACT.md's PyO3 boundary
section as written; two pieces are explicitly flagged TODO below because
they depend on Rust-side interfaces (``PreparedJob``/``HostCallbacks``
dispatch shape) that do not exist yet.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional

from ._bridge import get_core
from .jobs import Graph, GraphJob, UnsandboxedJob


class RunCallbacks:
    """The two coarse Rust->Python callbacks of CONTRACT.md's PyO3 boundary
    (§8.1 rule 1): ``expand_graph_job`` / ``run_in_process``. Passed as the
    ``py_callbacks`` object to ``ppg3._core.run``.
    """

    def __init__(self, graph: Graph):
        self.graph = graph

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

        TODO (blocked on Rust WP4): the scheduler needs to hand this
        callback real input/output/tool paths (the same virtual->real
        mapping the executor gets via ``PreparedJob``) so a ``JobIO`` can be
        built here; CONTRACT.md's current ``HostCallbacks::run_in_process``
        signature only passes ``(job_id, key_doc_json)``, which has the
        *declared* inputs (as hashes, from the key document) but not their
        resolved real paths. Left unimplemented rather than guessed, since
        getting the path-resolution contract wrong here would silently
        break sandboxing invariants once real jobs use it.
        """
        raise NotImplementedError(
            "run_in_process: blocked on the Rust scheduler's real-path "
            "resolution for HostCallbacks being specified; see STATUS.md"
        )


def run(
    graph: Optional[Graph] = None,
    project_id: str = "default",
    ephemeral: bool = False,
) -> Dict[str, Any]:
    """Run `graph` (or the current graph) to completion and update the view.

    Calls, in order: ``_core.open_stores`` (once, from ``graph.stores``),
    ``_core.run`` (the lowered ``JobDef`` list + parallelism config +
    :class:`RunCallbacks`), then assembles a ``ViewSpec`` from the run
    report's ``job_entries`` (``id -> (ik, oh)``) and every job's ``view``
    map, and calls ``_core.write_generation``.

    Raises :class:`ppg3._bridge.CoreNotAvailable` if the extension isn't
    built — every other part of this package works without it.
    """
    from .jobs import _current_graph as default_graph

    graph = graph or default_graph
    if graph is None:
        raise RuntimeError("ppg3.run(): no graph — call ppg3.new(...) first")

    core = get_core()

    jobs_json = json.dumps(graph.job_defs())
    parallelism_json = json.dumps(graph.parallelism)
    stores_config_json = json.dumps({"stores": [s.to_json() for s in graph.stores]})

    handle = core.open_stores(stores_config_json)
    callbacks = RunCallbacks(graph)
    report_json = core.run(handle, jobs_json, parallelism_json, callbacks)
    report = json.loads(report_json)

    job_entries = report.get("job_entries", {})
    view_entries = []
    for job_id, job in graph.jobs.items():
        entry = job_entries.get(job_id)
        if entry is None:
            continue
        _ik, oh = entry
        for _output_name, view_path in job.view.items():
            view_entries.append([view_path, oh, job.store or ""])
    view_spec_json = json.dumps({"entries": view_entries})

    generation = core.write_generation(
        handle, graph.project_dir, project_id, view_spec_json, ephemeral
    )
    report["generation"] = generation
    return report
