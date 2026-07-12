"""Parameter canonicalizer + canonical JSON encoder (CONTRACT.md, PPG3_DESIGN.md §5).

Two-phase pipeline:

1. ``canonicalize_value(obj)`` walks an arbitrary Python value from the closed
   type set (None/bool/int/float/str/bytes/list/tuple/dict/set/frozenset/
   Enum/dataclass, plus the opt-in ``__ppg3_hash__`` escape hatch) and
   produces a plain JSON-compatible structure (only dict/list/str/int/bool/
   None) with sentinel wrapper objects for the types JSON cannot represent
   natively (bytes, float, set/frozenset, Enum, dataclass).
2. ``canonical_json_bytes(value)`` serializes that structure to the wire
   format: UTF-8, sorted keys, no whitespace, no floats.

Anything outside the closed type set raises ``TypeError`` naming the exact
path (``$.params.model.weights``) so users can find the offending value.
"""

from __future__ import annotations

import dataclasses
import enum
import json
import struct
from typing import Any

BYTES_KEY = "__ppg3_bytes__"
FLOAT_KEY = "__ppg3_float__"
SET_KEY = "__ppg3_set__"
ENUM_KEY = "__ppg3_enum__"
DATACLASS_KEY = "__ppg3_dataclass__"


def _qualname(cls: type) -> str:
    return f"{cls.__module__}.{cls.__qualname__}"


def canonicalize_value(obj: Any, path: str = "$") -> Any:
    """Convert ``obj`` into a JSON-compatible structure per the closed type set.

    Raises ``TypeError`` naming ``path`` for anything outside that set.
    """
    # bool must be checked before int (bool is an int subclass in Python).
    if obj is None or isinstance(obj, bool) or isinstance(obj, str):
        return obj
    if isinstance(obj, int):
        return obj
    if isinstance(obj, float):
        return {FLOAT_KEY: struct.pack("<d", obj).hex()}
    if isinstance(obj, (bytes, bytearray)):
        return {BYTES_KEY: bytes(obj).hex()}

    # Opt-in protocol: an object may supply its own canonicalizable stand-in.
    hash_hook = getattr(obj, "__ppg3_hash__", None)
    if callable(hash_hook) and not isinstance(obj, type):
        return canonicalize_value(hash_hook(), path)

    if isinstance(obj, (list, tuple)):
        return [canonicalize_value(v, f"{path}[{i}]") for i, v in enumerate(obj)]

    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if not isinstance(k, str):
                raise TypeError(
                    f"{path}: dict keys must be str, got {type(k).__name__} ({k!r})"
                )
            out[k] = canonicalize_value(v, f"{path}.{k}")
        return out

    if isinstance(obj, (set, frozenset)):
        items = [canonicalize_value(v, f"{path}{{}}") for v in obj]
        items.sort(key=lambda v: canonical_json_bytes(v))
        return {SET_KEY: items}

    if isinstance(obj, enum.Enum):
        cls = type(obj)
        return {ENUM_KEY: f"{_qualname(cls)}.{obj.name}"}

    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        cls = type(obj)
        fields = {}
        for f in dataclasses.fields(obj):
            fields[f.name] = canonicalize_value(
                getattr(obj, f.name), f"{path}.{f.name}"
            )
        return {DATACLASS_KEY: _qualname(cls), "fields": fields}

    raise TypeError(
        f"{path}: object of type {type(obj).__name__} is not in ppg3's closed "
        "canonicalizable type set (None/bool/int/float/str/bytes/list/tuple/"
        "dict/set/frozenset/Enum/dataclass) and has no __ppg3_hash__() method"
    )


def canonical_json_bytes(value: Any) -> bytes:
    """Serialize an already-canonicalized value to the wire format.

    UTF-8, sorted keys, no whitespace, ``ensure_ascii=False``. Raises
    ``AssertionError`` if a raw ``float`` instance sneaks in — after
    ``canonicalize_value`` this should be impossible (floats are always
    wrapped in the ``__ppg3_float__`` sentinel).
    """

    def _reject_floats(v: Any) -> None:
        if isinstance(v, float):
            raise AssertionError(
                "canonical_json_bytes: raw float instance found post-canonicalization "
                "(this is a ppg3 bug — canonicalize_value should have wrapped it)"
            )
        if isinstance(v, dict):
            for vv in v.values():
                _reject_floats(vv)
        elif isinstance(v, list):
            for vv in v:
                _reject_floats(vv)

    _reject_floats(value)
    text = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return text.encode("utf-8")


def canonicalize_and_encode(obj: Any, path: str = "$") -> bytes:
    """Convenience: canonicalize + encode in one call."""
    return canonical_json_bytes(canonicalize_value(obj, path))


class MissingBlake3Error(RuntimeError):
    pass


def blake3_hex(data: bytes) -> str:
    """blake3 hex digest of ``data``, via the compiled extension if present,
    else the ``blake3`` pip package, else a loud error.

    PPG3_DESIGN.md mandates BLAKE3 (§4); there is intentionally no
    blake2b/sha fallback that would silently produce different hashes.
    """
    try:
        from ppg3 import _core  # type: ignore

        return _core.blake3_bytes(data)  # exact name TBD by the Rust side
    except Exception:
        pass
    try:
        import blake3 as _blake3_mod

        return _blake3_mod.blake3(data).hexdigest()
    except ImportError:
        pass
    raise MissingBlake3Error(
        "blake3 is required but neither the compiled `ppg3._core` extension "
        "nor the `blake3` pip package is available. Build the extension "
        "(`maturin develop`) or `pip install blake3`."
    )


def decanonicalize_value(value: Any) -> Any:
    """Best-effort inverse of :func:`canonicalize_value`, for handing
    parameters back to job code as native Python values (``JobIO.params``).

    Fully reverses the bytes/float/set/frozenset sentinels. Enum is
    reconstructed if the encoded module is importable; dataclass is *not*
    reconstructed (the coordinator side would need the class, which the
    worker may not have imported) — it is returned as the plain
    ``{"__ppg3_dataclass__": ..., "fields": {...}}`` dict with `fields`
    recursively decanonicalized, which is usually good enough for read-only
    consumption.
    """
    if isinstance(value, dict):
        if BYTES_KEY in value and len(value) == 1:
            return bytes.fromhex(value[BYTES_KEY])
        if FLOAT_KEY in value and len(value) == 1:
            return struct.unpack("<d", bytes.fromhex(value[FLOAT_KEY]))[0]
        if SET_KEY in value and len(value) == 1:
            return frozenset(decanonicalize_value(v) for v in value[SET_KEY])
        if ENUM_KEY in value and len(value) == 1:
            dotted = value[ENUM_KEY]
            # "<module>.<QualName>.<MEMBER>" — module names can themselves
            # contain dots (subpackages), so there is no fixed split point.
            # Try the longest possible module prefix first (importlib
            # either succeeds or raises cleanly, so this is safe to probe).
            import importlib

            parts = dotted.split(".")
            for i in range(len(parts) - 1, 0, -1):
                module_name = ".".join(parts[:i])
                rest = parts[i:]
                if len(rest) < 1:
                    continue
                try:
                    mod = importlib.import_module(module_name)
                except ImportError:
                    continue
                obj = mod
                try:
                    for part in rest[:-1]:
                        obj = getattr(obj, part)
                    return getattr(obj, rest[-1])
                except AttributeError:
                    continue
            return value  # best-effort: leave the sentinel dict intact
        if DATACLASS_KEY in value and "fields" in value:
            return {
                DATACLASS_KEY: value[DATACLASS_KEY],
                "fields": {
                    k: decanonicalize_value(v) for k, v in value["fields"].items()
                },
            }
        return {k: decanonicalize_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [decanonicalize_value(v) for v in value]
    return value


def input_key_local(doc: Any) -> str:
    """Compute the input key (blake3 hex) of a key document.

    ``doc`` may be a raw JSON-compatible structure (already canonicalized) or
    canonical bytes. Prefers the compiled ``ppg3._core.input_key`` (the
    authoritative implementation, §5/CONTRACT.md PyO3 boundary); falls back
    to the pure-Python `blake3` package so tests can run before the
    extension is built.
    """
    if isinstance(doc, (bytes, bytearray)):
        canonical = bytes(doc)
    else:
        canonical = canonical_json_bytes(doc)

    try:
        from ppg3 import _core  # type: ignore

        return _core.input_key(canonical)
    except ImportError:
        pass
    try:
        import blake3 as _blake3_mod

        return _blake3_mod.blake3(canonical).hexdigest()
    except ImportError:
        pass
    raise MissingBlake3Error(
        "blake3 is required but neither the compiled `ppg3._core` extension "
        "nor the `blake3` pip package is available. Build the extension "
        "(`maturin develop`) or `pip install blake3`."
    )
