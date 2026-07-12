"""TOFU (trust-on-first-use) source patcher (PPG3_DESIGN.md §7.6, CONTRACT.md
"Additive addendum: TOFU source patcher").

``FetchJob(blake3=None)`` is allowed outside ``--frozen`` mode (``jobs.py``
already rejects it under ``graph.frozen`` at definition time — see
``FetchJob.__init__``). Every such job records its call site at definition
time (``jobs.py``'s ``_record_call_site``, an ``inspect``-based walk to the
first stack frame outside the ``ppg3`` package). This module runs *after* a
successful :func:`ppg3.run.run` call and, for every unpinned ``FetchJob``:

1. Reads its actual output hash back from the run report + a post-run
   ``_core.lookup`` (the content-manifest ``blake3`` of the job's single
   output file — a ``FetchJob`` always has exactly one).
2. Groups unpinned jobs by call site ``(file, lineno)``.
3. Exactly one job at a call site ⇒ patches the ``blake3=`` keyword
   argument into the source at that exact ``FetchJob(...)`` call via
   ``libcst`` (used only to *locate* the call/keyword-argument spans via
   ``PositionProvider``; the actual edit is a plain text splice at those
   byte offsets so every other byte of the file — comments, tabs, unrelated
   formatting — is untouched, not merely "libcst-preserved").
4. More than one job at a call site (a loop over URLs), a call site that
   could not be determined, or a call site where the patcher can't find
   exactly one matching ``FetchJob(...)`` call on the recorded line (source
   edited since the run started, aliasing it couldn't resolve, etc.) ⇒ no
   patch; the job's ``(url, blake3)`` row goes into the printed table
   instead, per §7.6: "their code location, their data structure, their
   job".

The current run has already completed by the time this runs (the input key
document does not depend on ``fixed_output`` — see ``core/src/scheduler.rs``'s
``derive_key``, which never reads ``job.fixed_output`` — only publish-time
determinism enforcement does). Patching only affects what the *next*
definition pass reads; that next run is an all-hits re-run since the ``ik``
is unchanged, per §7.6: "the patched source is simply what the next
definition pass reads."

``libcst`` is an optional dependency (the ``ppg3[tofu]`` extra). If it is
not importable, this module never fails the run — it prints the table for
every unpinned job instead, plus a hint to install the extra.
"""

from __future__ import annotations

import json
import sys
from typing import Any, Dict, List, Optional, Tuple

from .jobs import FetchJob, Graph

_TableRow = Tuple["FetchJob", str]  # (job, hex digest)


def run_tofu_pass(graph: Graph, report: Dict[str, Any], core: Any, handle: Any) -> None:
    """Called by :func:`ppg3.run.run` after a successful run. Patches or
    tables every ``FetchJob`` in ``graph`` that was defined with
    ``blake3=None``. Never raises on account of missing ``libcst`` or an
    unpatchable call site — those degrade to the printed table."""
    job_entries = report.get("job_entries", {})
    unpinned = [
        job for job in graph.jobs.values() if isinstance(job, FetchJob) and job.blake3 is None
    ]
    if not unpinned:
        return

    resolved: List[_TableRow] = []
    for job in unpinned:
        entry = job_entries.get(job.id)
        if entry is None:
            # Not in this run's report at all (shouldn't happen once we're
            # past the report.failed check in run.py, but be defensive
            # rather than raise out of a post-run reporting pass).
            continue
        ik, _oh = entry
        looked = core.lookup(handle, ik)
        if looked is None:
            continue
        manifest = json.loads(looked)
        content = manifest.get("content", {})
        # §7.6 / task DECISION: "FetchJob has exactly one output; assert
        # that." — the sole content-manifest entry's blake3 is the hash to
        # pin, whatever its path-within-entry key happens to be.
        items = list(content.items())
        assert len(items) == 1, (
            f"FetchJob {job.id!r}: expected exactly one output file in its "
            f"manifest content, got {sorted(content.keys())!r}"
        )
        _rel_path, file_entry = items[0]
        resolved.append((job, file_entry["blake3"]))

    if not resolved:
        return

    by_site: Dict[Tuple[str, int], List[_TableRow]] = {}
    no_site: List[_TableRow] = []
    for job, digest in resolved:
        site = job._call_site
        if site is None:
            no_site.append((job, digest))
        else:
            by_site.setdefault(site, []).append((job, digest))

    try:
        import libcst  # noqa: F401

        libcst_available = True
    except ImportError:
        libcst_available = False
        sys.stderr.write(
            "ppg3 tofu: libcst is not installed — cannot auto-patch source; "
            "printing the pin table instead. Run `pip install 'ppg3[tofu]'` "
            "to enable automatic source patching.\n"
        )

    table_rows: List[_TableRow] = list(no_site)
    by_file: Dict[str, List[Tuple[int, "FetchJob", str]]] = {}
    for site, entries in by_site.items():
        if len(entries) == 1 and libcst_available:
            file, lineno = site
            job, digest = entries[0]
            by_file.setdefault(file, []).append((lineno, job, digest))
        else:
            table_rows.extend(entries)

    for file, site_entries in by_file.items():
        table_rows.extend(_patch_file(file, site_entries))

    if table_rows:
        _print_table(table_rows)


# --------------------------------------------------------------------------
# libcst-based patching
# --------------------------------------------------------------------------


def _fetchjob_local_aliases(module: Any) -> "set[str]":
    """Bare-name call sites (`FJ(...)`, not `p.FetchJob(...)`) only resolve
    to `FetchJob` if the name was imported as such — `from ppg3 import
    FetchJob as FJ`. Attribute-form calls (`ppg3.FetchJob(...)`, `p.
    FetchJob(...)`) don't need this: whatever the *module* is aliased to,
    the attribute's own last segment is still literally `FetchJob` (the
    task DECISION's "pragmatic" rule). A plain textual scan of `from ...
    import FetchJob [as X]` statements anywhere in the module — no real
    import-graph resolution, matching the rest of this module's "pragmatic,
    not exhaustive" approach to call-site matching."""
    import libcst as cst

    aliases = {"FetchJob"}

    class _ImportScanner(cst.CSTVisitor):
        def visit_ImportFrom(self, node: Any) -> None:
            names = node.names
            if isinstance(names, cst.ImportStar):
                return
            for alias in names:
                name_node = alias.name
                if isinstance(name_node, cst.Name) and name_node.value == "FetchJob":
                    local = alias.asname.name.value if alias.asname else "FetchJob"
                    if isinstance(local, str):
                        aliases.add(local)

    module.visit(_ImportScanner())
    return aliases


def _is_fetchjob_call(func: Any, aliases: "set[str]") -> bool:
    """`FetchJob(...)`, `ppg3.FetchJob(...)`, `p.FetchJob(...)` (module
    aliased), `FJ(...)` (name aliased via `aliases`, see
    `_fetchjob_local_aliases`) all match — the pragmatic "last attribute
    segment / known local alias" rule the task DECISION calls for, rather
    than real import resolution."""
    import libcst as cst

    if isinstance(func, cst.Name):
        return func.value in aliases
    if isinstance(func, cst.Attribute):
        return func.attr.value == "FetchJob"
    return False


def _find_blake3_arg(call: Any):
    for arg in call.args:
        if arg.keyword is not None and arg.keyword.value == "blake3":
            return arg
    return None


def _patch_file(file: str, site_entries: List[Tuple[int, "FetchJob", str]]) -> List[_TableRow]:
    """Patch every singleton call site in ``site_entries`` (all in ``file``)
    in one read-modify-write. Returns the ``(job, digest)`` rows that could
    not be patched (fall back to the table) — an empty list means every
    site in ``site_entries`` was patched."""
    import libcst as cst
    from libcst.metadata import MetadataWrapper, PositionProvider

    try:
        with open(file, "r", encoding="utf-8") as fh:
            source = fh.read()
    except OSError as e:
        sys.stderr.write(f"ppg3 tofu: cannot read {file!r} to patch: {e}\n")
        return [(job, digest) for _lineno, job, digest in site_entries]

    try:
        module = cst.parse_module(source)
    except Exception as e:
        sys.stderr.write(f"ppg3 tofu: cannot parse {file!r}: {e}\n")
        return [(job, digest) for _lineno, job, digest in site_entries]

    wrapper = MetadataWrapper(module, unsafe_skip_copy=True)
    positions = wrapper.resolve(PositionProvider)

    linenos = {lineno for lineno, _job, _digest in site_entries}
    aliases = _fetchjob_local_aliases(module)

    class _FetchCallFinder(cst.CSTVisitor):
        METADATA_DEPENDENCIES = (PositionProvider,)

        def __init__(self) -> None:
            super().__init__()
            self.by_line: Dict[int, List[Any]] = {}

        def visit_Call(self, node: Any) -> None:
            pos = self.get_metadata(PositionProvider, node)
            if pos.start.line in linenos and _is_fetchjob_call(node.func, aliases):
                self.by_line.setdefault(pos.start.line, []).append(node)

    finder = _FetchCallFinder()
    wrapper.visit(finder)

    # Byte-offset table for the whole source, so the actual edit is a plain
    # string splice (guarantees "byte-diff only at the kwarg" — libcst is
    # only used to *find* where that is).
    lines = source.splitlines(keepends=True)
    cum = [0]
    for line in lines:
        cum.append(cum[-1] + len(line))

    def offset(pos: Any) -> int:
        return cum[pos.line - 1] + pos.column

    unresolved: List[_TableRow] = []
    edits: List[Tuple[int, int, str]] = []  # (start, end, replacement), applied right-to-left
    patched_msgs: List[Tuple[str, int, str]] = []  # (view_path, lineno, digest)

    for lineno, job, digest in site_entries:
        matches = finder.by_line.get(lineno, [])
        if len(matches) != 1:
            sys.stderr.write(
                f"ppg3 tofu: {file}:{lineno}: expected exactly one FetchJob(...) "
                f"call starting on this line, found {len(matches)} — falling "
                "back to the pin table for this job rather than guessing.\n"
            )
            unresolved.append((job, digest))
            continue

        call = matches[0]
        quoted = f'"{digest}"'
        existing = _find_blake3_arg(call)
        if existing is not None:
            vpos = positions[existing.value]
            edits.append((offset(vpos.start), offset(vpos.end), quoted))
        else:
            last_arg = call.args[-1]
            vpos = positions[last_arg.value]
            end_off = offset(vpos.end)
            edits.append((end_off, end_off, f", blake3={quoted}"))

        view_path = job.view[FetchJob.OUTPUT_NAME]
        patched_msgs.append((view_path, lineno, digest))

    if edits:
        new_source = source
        for start_off, end_off, replacement in sorted(edits, key=lambda e: -e[0]):
            new_source = new_source[:start_off] + replacement + new_source[end_off:]
        with open(file, "w", encoding="utf-8") as fh:
            fh.write(new_source)
        for view_path, lineno, digest in patched_msgs:
            print(f"pinned {view_path} ({digest[:8]}…) in {file}:{lineno}")

    return unresolved


# --------------------------------------------------------------------------
# Table fallback
# --------------------------------------------------------------------------


def _print_table(table_rows: List[_TableRow]) -> None:
    print(
        "ppg3 tofu: could not auto-patch the following FetchJob pin(s) — "
        "wire these into your own lookup (their code location, their data "
        "structure, their job):"
    )
    for job, digest in table_rows:
        view_path = job.view[FetchJob.OUTPUT_NAME]
        print(f"  {job.url}\t{digest}\t(view={view_path})")
