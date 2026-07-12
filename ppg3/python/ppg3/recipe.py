"""Recipe hashing: the source-derived, bytecode-noise-free strict hash that
feeds ``job_recipe`` in the §5 key document.

Ports the *semantics* of ppg2's ``extract_strict_hash``/``FunctionInvariant``
(clean-room reimplementation, no import from ``pypipegraph2``): the hash is
derived from the function's *source text*, not object identity or bytecode
addresses, so:

- moving a function to a different line in the same file does not change its
  hash (``inspect.getsource`` returns just that function's text regardless
  of position),
- renaming the function does not change its hash (the name on the ``def``
  line is blanked out before hashing — "qualname-independent"),
- decorator lines are stripped (re-decorating with an equivalent decorator,
  e.g. moving from ``@ppg3.something`` to a synonym, should not matter to
  the *body*'s identity — decorators are a Python-level concern, not part of
  what the job computes),
- default parameter values participate (they are data, not source noise) via
  the canonicalizer.

Reformatting that changes semantics (renaming a variable, changing a
constant) legitimately changes the hash — this is *not* meant to be
whitespace/comment-insensitive beyond what dedent naturally buys.
"""

from __future__ import annotations

import inspect
import re
import textwrap
from typing import Any, Callable, Iterable, Sequence

from . import canon


class RecipeError(RuntimeError):
    pass


_DEF_NAME_RE = re.compile(r"^(\s*(?:async\s+)?def\s+)[A-Za-z_][A-Za-z0-9_]*")


def _strip_decorator_lines(lines: list) -> list:
    start = 0
    for i, line in enumerate(lines):
        if line.strip().startswith("@"):
            continue
        start = i
        break
    else:
        start = len(lines)
    return lines[start:]


def _blank_def_name(source: str) -> str:
    return _DEF_NAME_RE.sub(r"\1<func>", source, count=1)


def extract_source_and_name(fn: Callable) -> tuple:
    """Extract ``fn``'s source, dedented and with decorator lines stripped,
    keeping its real name on the ``def`` line — the form actually shipped
    for source-mode transport (execed in a fresh module namespace, then
    looked up by name). Decorators are stripped because they resolve in the
    *defining module's* scope, not the function's own, and are almost never
    meaningful/resolvable in a divergent job environment; what crosses is
    the plain callable.

    Returns ``(source, name)``.
    """
    try:
        raw = inspect.getsource(fn)
    except (OSError, TypeError) as e:
        raise RecipeError(
            f"could not extract source for {fn!r}: {e}. Built-ins, "
            "dynamically-`exec`'d functions, and REPL-defined functions "
            "have no retrievable source; define the job callback in a file."
        ) from e
    raw = textwrap.dedent(raw)
    lines = _strip_decorator_lines(raw.split("\n"))
    source = "\n".join(lines).strip("\n")
    return source, fn.__name__


def extract_normalized_source(fn: Callable) -> str:
    """Like :func:`extract_source_and_name` but with the function's own name
    on the ``def`` line blanked out (qualname-independent) — the form used
    for the *recipe hash* (not for transport/execution, where the real name
    is needed to look the callable back up after exec)."""
    source, _name = extract_source_and_name(fn)
    return _blank_def_name(source)


def extract_defaults(fn: Callable) -> dict:
    """Map of parameter name -> default value, for parameters that have one."""
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return {}
    out = {}
    for name, param in sig.parameters.items():
        if param.default is not inspect.Parameter.empty:
            out[name] = param.default
    return out


def recipe_hash(fn: Callable) -> str:
    """Strict source-derived hash of a callback function.

    Stable across: line-number moves, renaming the function itself,
    decorator changes, docstring text. Changes when: the body changes, a
    default value changes.
    """
    source = extract_normalized_source(fn)
    defaults = extract_defaults(fn)
    doc = {
        "kind": "python_source",
        "source": source,
        "defaults": canon.canonicalize_value(defaults, "$.defaults"),
    }
    return canon.input_key_local(doc)


def recipe_hash_source(
    file_bytes: bytes,
    qualname: str,
    includes: Sequence[bytes] = (),
) -> str:
    """Recipe hash for the ``ppg3.Source("file.py::qualname", includes=[...])``
    opaque-file transport form (§6.6): blake3 of the file bytes + qualname +
    each include's bytes. The coordinator never parses this file, so this is
    the *only* signal available — content, not source-text-normalized.
    """
    doc = {
        "kind": "python_source_file",
        "file": {canon.BYTES_KEY: file_bytes.hex()},
        "qualname": qualname,
        "includes": [{canon.BYTES_KEY: b.hex()} for b in includes],
    }
    return canon.input_key_local(doc)


def recipe_hash_command(argv_template: Iterable[Any]) -> str:
    """Recipe hash for a ``CommandJob`` argv template.

    ``argv_template`` must already be lowered to plain strings with
    placeholders serialized in their wire form (``"{in:name}"``,
    ``"{out}"``, ``"{out:name}"``, ``"{tool:name}"``) — never real paths
    (§6.2): the whole point is that the recipe hash must not depend on where
    a job happens to run.
    """
    items = list(argv_template)
    for i, item in enumerate(items):
        if not isinstance(item, str):
            raise RecipeError(
                f"$.argv[{i}]: CommandJob argv template entries must already be "
                f"lowered to strings, got {type(item).__name__} ({item!r})"
            )
    doc = {"kind": "command_argv", "argv": items}
    return canon.input_key_local(doc)
