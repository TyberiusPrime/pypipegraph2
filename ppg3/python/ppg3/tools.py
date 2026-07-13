"""ToolSpec / PyEnv (CONTRACT.md "Python package", WP8).

Tools are inputs (PPG3_DESIGN.md §2): the executables a job runs participate
in its input key. Two mechanisms:

- ``ToolSpec.nix(ref)`` — a pinned Nix flake reference; resolved lazily (at
  run time, not definition time) via ``nix build --no-link --print-out-paths``,
  cached per process. The tool hash *is* the resulting Nix store path (it
  already encodes the whole input closure — no need to hash bytes).
- ``ToolSpec.binary(path)`` — a hash-of-file fallback for non-Nix
  environments; tool hash is the blake3 of the file, PATH entry is its
  parent directory.

``PyEnv`` is a specialization used for python jobs: it additionally drives
callback transport selection (§6.5) and the worker shim invocation.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence

from . import canon
from .localscope import DefinitionError

_PINNED_REV_RE = re.compile(r"\b[0-9a-f]{40}\b")
_LOCKED_REF_RE = re.compile(r"^[\w.+-]+:[^#]*/[0-9a-f]{40}(#.*)?$")


def _looks_pinned(flake_ref: str) -> bool:
    """A flake ref is pinned if it contains a 40-hex-char git revision
    somewhere (the common `github:owner/repo/<rev>#attr` shape and its
    relatives), or is a `path:`/store-path reference (inherently pinned to
    disk content, not a mutable branch).
    """
    if _PINNED_REV_RE.search(flake_ref):
        return True
    if flake_ref.startswith("/") or flake_ref.startswith("path:"):
        return True
    if flake_ref.startswith("/nix/store/"):
        return True
    return False


@dataclass(frozen=True)
class ToolResolution:
    hash: str
    # For nix tools: the resolved store path (also `hash`, kept separately
    # for readability). For binary tools: the file's own path.
    store_path: str
    bin_dir: str  # PATH entry


class ToolSpec:
    """A single external tool (executable) declared as a job input."""

    _nix_cache: Dict[str, str] = {}

    def __init__(self, kind: str, ref_or_path: str, name: Optional[str] = None):
        self.kind = kind  # "nix" | "binary"
        self.ref_or_path = ref_or_path
        self.name = name or ref_or_path

    @classmethod
    def nix(cls, flake_ref: str, name: Optional[str] = None) -> "ToolSpec":
        if not _looks_pinned(flake_ref):
            raise DefinitionError(
                f"ToolSpec.nix({flake_ref!r}): flake reference is not pinned "
                "(no 40-hex-char revision and not a path: reference). A bare "
                "branch reference is a definition-time error — tools are "
                "inputs (§2) and must be reproducible. Pin it, e.g. "
                "'github:owner/repo/<40-hex-rev>#attr'."
            )
        return cls("nix", flake_ref, name=name)

    @classmethod
    def binary(cls, path: str, name: Optional[str] = None) -> "ToolSpec":
        return cls("binary", str(path), name=name)

    def resolve(self) -> ToolResolution:
        if self.kind == "nix":
            store_path = self._resolve_nix(self.ref_or_path)
            return ToolResolution(
                hash=store_path, store_path=store_path, bin_dir=f"{store_path}/bin"
            )
        elif self.kind == "binary":
            path = os.path.abspath(self.ref_or_path)
            if not os.path.isfile(path):
                raise DefinitionError(
                    f"ToolSpec.binary({self.ref_or_path!r}): no such file"
                )
            with open(path, "rb") as fh:
                h = canon.blake3_hex(fh.read())
            return ToolResolution(
                hash=h, store_path=path, bin_dir=os.path.dirname(path)
            )
        raise AssertionError(f"unknown ToolSpec kind {self.kind!r}")

    @classmethod
    def _resolve_nix(cls, flake_ref: str) -> str:
        if flake_ref in cls._nix_cache:
            return cls._nix_cache[flake_ref]
        try:
            out = subprocess.run(
                ["nix", "build", "--no-link", "--print-out-paths", flake_ref],
                check=True,
                capture_output=True,
                text=True,
            )
        except FileNotFoundError as e:
            raise RuntimeError(
                "ToolSpec.nix resolution requires the `nix` binary on PATH "
                "(not found)."
            ) from e
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"nix build failed for {flake_ref!r}: {e.stderr.strip()}"
            ) from e
        store_path = out.stdout.strip().splitlines()[-1].strip()
        cls._nix_cache[flake_ref] = store_path
        return store_path

    def __repr__(self) -> str:
        return f"ToolSpec({self.kind!r}, {self.ref_or_path!r})"


class PyEnv:
    """A Python interpreter environment a job runs under (§7.5)."""

    def __init__(
        self,
        kind: str,
        preload: Sequence[str] = (),
        flake_ref: Optional[str] = None,
        weakly_hermetic: bool = False,
    ):
        self.kind = kind  # "current" | "nix"
        self.preload = list(preload)
        self.flake_ref = flake_ref
        self.weakly_hermetic = weakly_hermetic

    @classmethod
    def current(cls, preload: Sequence[str] = ()) -> "PyEnv":
        return cls("current", preload=preload, weakly_hermetic=True)

    @classmethod
    def nix(cls, flake_ref: str, preload: Sequence[str] = ()) -> "PyEnv":
        if not _looks_pinned(flake_ref):
            raise DefinitionError(
                f"PyEnv.nix({flake_ref!r}): flake reference is not pinned; "
                "see ToolSpec.nix for the same rule."
            )
        return cls("nix", preload=preload, flake_ref=flake_ref, weakly_hermetic=False)

    def resolve(self) -> ToolResolution:
        if self.kind == "current":
            # Identity of the *current* interpreter. Deliberately stable and
            # working-directory-independent: the interpreter's realpath and
            # version, nothing else.
            #
            # We do NOT fingerprint `sys.path` by directory mtime (as an
            # earlier version did). mtimes are not content, are not
            # reproducible across machines/checkouts, and — fatally —
            # `sys.path[0]` is the script's directory, i.e. the project dir
            # that ppg3 writes `store/`, `.ppg3/` and `outputs/` into on every
            # run. Hashing it re-keyed the PyEnv on each invocation, so every
            # Python job (FetchJob/FileJob/DataJob) re-ran — and re-fetched —
            # every single time. `current` is `weakly_hermetic` by design; a
            # job's own code is already captured by its recipe hash.
            doc = {
                "kind": "pyenv_current",
                "executable": os.path.realpath(sys.executable),
                "version": list(sys.version_info[:3]),
            }
            h = canon.input_key_local(canon.canonicalize_value(doc, "$.pyenv"))
            return ToolResolution(
                hash=h,
                store_path=os.path.realpath(sys.executable),
                bin_dir=os.path.dirname(os.path.realpath(sys.executable)),
            )
        elif self.kind == "nix":
            store_path = ToolSpec._resolve_nix(self.flake_ref)
            return ToolResolution(
                hash=store_path,
                store_path=store_path,
                bin_dir=f"{store_path}/bin",
            )
        raise AssertionError(f"unknown PyEnv kind {self.kind!r}")

    def executable_hint(self) -> str:
        """Best-effort path to the `python` executable for this env, for the
        cold-`python -I -m ppg3._shim` invocation (CONTRACT.md scope
        deviation: forkserver templates are deferred to a future WP)."""
        if self.kind == "current":
            return sys.executable
        # nix: conventional layout produced by pythonXY.withPackages(...)
        store_path = ToolSpec._resolve_nix(self.flake_ref)
        return f"{store_path}/bin/python3"

    def __repr__(self) -> str:
        return f"PyEnv({self.kind!r}, preload={self.preload!r})"
