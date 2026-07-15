"""jj (jujutsu) VCS integration, enabled per-graph via ``ppg3.new(jj=True)``.

Three responsibilities, all driven from :func:`ppg3.run`:

a) **Hard error on untracked job sources.** With jj support enabled, every
   file that *defines* jobs — the pipeline script / any module calling a job
   constructor, callback source files, ``Source`` refs + their includes
   (see ``Graph.source_paths()``) — must be tracked in the enclosing jj
   workspace. An untracked/ignored source, or one outside the workspace,
   raises :class:`JJError` before any job is dispatched: a generation whose
   defining sources aren't under version control could never be traced back
   to a source state, which defeats the point of capturing VCS info at all.

b) **Capture the jj state at run time.** :func:`capture_state` snapshots the
   working copy (any jj invocation does) and records the working-copy commit
   `@` (commit id + change id), the current operation-log id, and whether
   the working copy was *empty*. That block is persisted into the
   generation's ``meta.json`` (``views::VcsInfo`` on the Rust side).

c) **Committed vs op-log generations.** ``committed`` is ``True`` iff the
   working copy commit was empty — the job sources are then exactly
   ``parent_commit_id``, a durable commit in normal history. Otherwise the
   sources exist only as jj's automatic working-copy snapshot, i.e. they
   remain recoverable *solely through the op log* (``jj op restore
   <op_id>``) once the user amends onward — an "op-log generation". The
   split GC (``ppg3 gc`` / ``views::remove_old_generations``) prunes op-log
   generations under a separate, smaller budget.

Every jj invocation goes through the binary named by the ``PPG3_JJ``
environment variable (default ``"jj"``) — which is also what the test suite
uses to substitute a scripted fake, since neither CI nor every dev box has
jj installed.
"""

from __future__ import annotations

import os
import subprocess
from typing import Any, Dict, List, Optional, Sequence

#: Template for `jj log -r @`: one field per line, `empty` last (jj's
#: template language has no JSON output; line-based is unambiguous since
#: ids never contain newlines).
_LOG_TEMPLATE = 'commit_id ++ "\\n" ++ change_id ++ "\\n" ++ if(empty, "true", "false") ++ "\\n"'


class JJError(RuntimeError):
    """A jj precondition failed (no workspace, untracked job source, jj
    binary missing/broken). Always a *hard* error when jj support is
    enabled — there is deliberately no downgrade-to-warning path."""


def jj_binary() -> str:
    return os.environ.get("PPG3_JJ", "jj")


def _run_jj(args: Sequence[str], cwd: str) -> str:
    """Run one jj command, returning stdout; raise :class:`JJError` on a
    missing binary or non-zero exit (with jj's stderr in the message)."""
    argv = [jj_binary(), *args]
    try:
        proc = subprocess.run(
            argv,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except FileNotFoundError as e:
        raise JJError(
            f"jj support is enabled (ppg3.new(jj=True)) but the jj binary "
            f"{jj_binary()!r} was not found on PATH (override with PPG3_JJ)"
        ) from e
    if proc.returncode != 0:
        raise JJError(
            f"`{' '.join(argv)}` failed with exit code {proc.returncode}: "
            f"{proc.stderr.strip()}"
        )
    return proc.stdout


def find_workspace_root(start: str) -> Optional[str]:
    """Absolute path of the jj workspace root containing ``start`` (via
    ``jj workspace root``), or ``None`` when ``start`` is not inside one."""
    argv = [jj_binary(), "workspace", "root"]
    try:
        proc = subprocess.run(
            argv,
            cwd=start,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except FileNotFoundError as e:
        raise JJError(
            f"jj support is enabled (ppg3.new(jj=True)) but the jj binary "
            f"{jj_binary()!r} was not found on PATH (override with PPG3_JJ)"
        ) from e
    if proc.returncode != 0:
        return None
    root = proc.stdout.strip()
    return root or None


def capture_state(root: str) -> Dict[str, Any]:
    """Snapshot + record the current jj state of the workspace at ``root``.

    Returns the dict :func:`ppg3.run` serializes into
    ``_core.write_generation(..., vcs_json=...)`` — field-for-field the
    Rust ``views::VcsInfo`` shape.
    """
    out = _run_jj(["log", "--no-graph", "-r", "@", "-T", _LOG_TEMPLATE], cwd=root)
    lines = out.splitlines()
    if len(lines) < 3:
        raise JJError(f"unexpected `jj log -r @` template output: {out!r}")
    commit_id, change_id, empty_str = lines[0], lines[1], lines[2]
    committed = empty_str.strip() == "true"

    state: Dict[str, Any] = {
        "backend": "jj",
        "commit_id": commit_id.strip(),
        "change_id": change_id.strip(),
        "op_id": current_op_id(root),
        "committed": committed,
    }

    # Parent commit: for a committed (empty-@) generation this is the
    # durable commit the sources correspond to. A merge working copy has
    # several parents — recorded only when unambiguous.
    parents = _run_jj(
        ["log", "--no-graph", "-r", "@-", "-T", 'commit_id ++ "\\n"'], cwd=root
    ).split()
    if len(parents) == 1:
        state["parent_commit_id"] = parents[0]
    return state


def current_op_id(root: str) -> str:
    out = _run_jj(["op", "log", "--no-graph", "--limit", "1", "-T", "id"], cwd=root)
    op_id = out.strip().splitlines()[0].strip() if out.strip() else ""
    if not op_id:
        raise JJError("`jj op log` returned no operation id")
    return op_id


def list_tracked_files(root: str) -> List[str]:
    """Workspace-root-relative paths of every file jj tracks."""
    out = _run_jj(["file", "list"], cwd=root)
    return [line for line in out.splitlines() if line]


def assert_sources_tracked(root: str, source_paths: Sequence[str]) -> None:
    """Requirement (a): every job-source file must be tracked in the jj
    workspace at ``root``. Raises :class:`JJError` naming every offender —
    both files *outside* the workspace and files inside it that jj does not
    track (untracked or ignored)."""
    tracked = set(list_tracked_files(root))
    real_root = os.path.realpath(root)

    outside: List[str] = []
    untracked: List[str] = []
    for path in sorted(set(source_paths)):
        real = os.path.realpath(path)
        rel = os.path.relpath(real, real_root)
        if rel.startswith(os.pardir + os.sep) or rel == os.pardir or os.path.isabs(rel):
            outside.append(path)
        elif rel.replace(os.sep, "/") not in tracked:
            untracked.append(path)

    if outside or untracked:
        parts = []
        if untracked:
            parts.append(
                "not tracked by jj (fix: `jj file track`, or check your "
                "ignore patterns):\n  " + "\n  ".join(untracked)
            )
        if outside:
            parts.append(
                f"outside the jj workspace {root!r}:\n  " + "\n  ".join(outside)
            )
        raise JJError(
            "ppg3.new(jj=True): every job-source file must be under jj "
            "version control, but the following are " + "\n".join(parts)
        )
