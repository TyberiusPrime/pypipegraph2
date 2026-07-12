"""Definition-time free-variable check for source-mode transport (§6.5/§6.6).

A pragmatic, from-scratch port of the *semantics* of ppg2's
``python/pypipegraph2/localscope`` (not its code): a callback shipped as
source (rather than cloudpickle) must carry no state through closures — all
data must flow in through ``JobIO`` (declared inputs/params) or through
plain default-parameter values. This module checks that at job-definition
time (or, for the ``ppg3.Source`` opaque-file form, is meant to be re-run
inside the worker shim at import time — see ``_shim.py``).

Kept intentionally small (~100 lines): unlike ppg2's ``localscope``, this
does not recurse into nested code objects with bytecode-level caching, does
not support a ``predicate=``/``allowed=`` decorator API, and does not try to
disassemble comprehensions separately — good enough for a definition-time
lint whose job is to name offending variables, not to be a general-purpose
sandboxing primitive.
"""

from __future__ import annotations

import builtins
import dis
import inspect
import types
from dataclasses import dataclass, field
from typing import Callable, FrozenSet, List


class DefinitionError(RuntimeError):
    """Raised when a callback cannot cross interpreters via source-mode
    transport: it references free variables beyond builtins/imported
    modules/its own parameters."""


@dataclass
class LocalscopeReport:
    """Result of a successful check: names allowed through because they
    resolved to modules (recorded for the `ppg3 lint` module-availability
    check, §6.6) rather than treated as violations."""

    modules: List[str] = field(default_factory=list)


def _global_names_loaded(code: types.CodeType) -> FrozenSet[str]:
    """Names actually fetched via LOAD_GLOBAL in this code object (and any
    nested code objects — comprehensions, inner defs, etc.), as opposed to
    every name in ``co_names`` (which also holds attribute names accessed
    via LOAD_ATTR/LOAD_METHOD and has nothing to do with global resolution).
    """
    names = set()
    for instr in dis.get_instructions(code):
        if instr.opname == "LOAD_GLOBAL":
            names.add(instr.argval)
    for const in code.co_consts:
        if isinstance(const, types.CodeType):
            names |= _global_names_loaded(const)
    return frozenset(names)


def check_localscope(
    fn: Callable, allowed: FrozenSet[str] = frozenset()
) -> LocalscopeReport:
    """Verify ``fn`` has no closure and no disallowed free global variables.

    - ``fn.__code__.co_freevars``: any non-empty tuple is an immediate
      violation — closures are never allowed in source-mode transport, since
      the worker shim reconstructs the function from source text alone with
      no enclosing scope to bind cells against.
    - Global name lookups (``LOAD_GLOBAL``, recursively through nested code
      objects) are allowed if: they resolve to a builtin, they are in
      ``allowed``, or they resolve (in ``fn.__globals__``) to a module —
      modules are allowed because they re-resolve inside the job's own
      environment (§6.6) and are recorded in the returned report for the
      lint to check availability later. Anything else — and any name that
      doesn't resolve at all, since referencing an as-yet-undefined global
      is exactly the kind of ambient-state bug this check exists to catch
      — is a violation.

    Raises ``DefinitionError`` naming every offending variable if any
    violation is found.
    """
    if not isinstance(fn, (types.FunctionType, types.MethodType)):
        raise TypeError(f"check_localscope expects a function, got {type(fn)!r}")
    func = fn.__func__ if isinstance(fn, types.MethodType) else fn
    code = func.__code__
    glb = func.__globals__

    violations: List[str] = []
    modules: List[str] = []

    if code.co_freevars:
        violations.extend(code.co_freevars)

    for name in sorted(_global_names_loaded(code)):
        if hasattr(builtins, name):
            continue
        if name in allowed:
            continue
        if name not in glb:
            violations.append(name)
            continue
        value = glb[name]
        if inspect.ismodule(value):
            modules.append(name)
            continue
        violations.append(name)

    if violations:
        uniq = sorted(set(violations))
        raise DefinitionError(
            f"{func.__qualname__} (file \"{code.co_filename}\", line "
            f"{code.co_firstlineno}) is not eligible for source-mode transport: "
            f"free variable(s) {uniq} are neither builtins, declared-allowed "
            "names, nor module references. Source-mode callbacks may not "
            "close over local state — pass data through JobIO (declared "
            "inputs/params) instead."
        )

    return LocalscopeReport(modules=sorted(set(modules)))
