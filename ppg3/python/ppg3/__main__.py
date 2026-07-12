"""``python -m ppg3 <subcommand>`` entry point (PPG3_DESIGN.md §6.7).

Only the ``watch`` subcommand exists so far — session-mode template
persistence (the other half of §6.7) is out of scope for this pass (see
STATUS.md). The Rust ``ppg3`` CLI binary (``cli/``, CONTRACT.md) is a
separate, unrelated entry point for store-level operations (``gc``,
``verify``, ...); this coordinator-side ``watch`` command is Python-only
and does not shell out to it.

Usage: ``python -m ppg3 watch <pipeline.py> [--interval SECONDS] [args...]``

``watch``'s own arguments are parsed by hand (see :func:`_parse_watch_argv`)
rather than via ``argparse``'s ``REMAINDER``: the required CLI shape puts
the pipeline script *before* our own ``--interval`` flag (``watch
script.py --interval 0.1``), and ``argparse.REMAINDER`` greedily swallows
every token — including a later ``--interval`` — once it starts consuming
at the first positional, which would silently ignore the flag. Instead:
scan the whole argv for ``--interval``/``--interval=VALUE`` wherever it
appears and strip it out; the first remaining token is the script path,
everything else (in original relative order) is forwarded to the script
unchanged as its own ``sys.argv[1:]``.
"""

from __future__ import annotations

import sys
from typing import List, Optional, Sequence, Tuple

from .watch import run_watch


class _UsageError(SystemExit):
    def __init__(self, message: str):
        super().__init__(f"python -m ppg3: {message}")


def _parse_watch_argv(argv: Sequence[str]) -> Tuple[str, List[str], float]:
    interval = 0.5
    remaining: List[str] = []
    i = 0
    n = len(argv)
    while i < n:
        tok = argv[i]
        if tok == "--interval":
            if i + 1 >= n:
                raise _UsageError("watch: --interval requires a value")
            try:
                interval = float(argv[i + 1])
            except ValueError:
                raise _UsageError(f"watch: --interval: invalid float {argv[i + 1]!r}")
            i += 2
            continue
        if tok.startswith("--interval="):
            value = tok.split("=", 1)[1]
            try:
                interval = float(value)
            except ValueError:
                raise _UsageError(f"watch: --interval: invalid float {value!r}")
            i += 1
            continue
        remaining.append(tok)
        i += 1

    if not remaining:
        raise _UsageError("watch: missing pipeline script path")
    script = remaining[0]
    script_args = remaining[1:]
    return script, script_args, interval


def main(argv: Optional[List[str]] = None) -> int:
    argv = sys.argv[1:] if argv is None else list(argv)
    if not argv or argv[0] in ("-h", "--help"):
        sys.stderr.write(
            "usage: python -m ppg3 watch <pipeline.py> [--interval SECONDS] [args...]\n"
        )
        return 0 if argv and argv[0] in ("-h", "--help") else 2

    command, rest = argv[0], argv[1:]
    if command != "watch":
        sys.stderr.write(f"python -m ppg3: unknown command {command!r} (only 'watch' exists)\n")
        return 2

    script, script_args, interval = _parse_watch_argv(rest)
    return run_watch(script, script_args, interval=interval)


if __name__ == "__main__":
    sys.exit(main())
