"""Local stat-cache (PPG3_DESIGN.md §10.3): ``(path, size, mtime_ns) -> blake3``
for leaf files, so unchanged files skip re-hashing on every run.

Non-authoritative: losing/deleting the cache costs re-hashing only, never
wrong reuse (a cache miss always re-hashes from content). Lives at
``<project_dir>/statcache.sqlite``.
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Optional, Union

from . import canon

_SCHEMA = """
CREATE TABLE IF NOT EXISTS file_hashes (
    path TEXT NOT NULL,
    size INTEGER NOT NULL,
    mtime_ns INTEGER NOT NULL,
    blake3 TEXT NOT NULL,
    PRIMARY KEY (path, size, mtime_ns)
);
"""


class StatCache:
    def __init__(self, db_path: Union[str, Path]):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.execute(_SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "StatCache":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def _lookup(self, path: str, size: int, mtime_ns: int) -> Optional[str]:
        cur = self._conn.execute(
            "SELECT blake3 FROM file_hashes WHERE path=? AND size=? AND mtime_ns=?",
            (path, size, mtime_ns),
        )
        row = cur.fetchone()
        return row[0] if row else None

    def _store(self, path: str, size: int, mtime_ns: int, digest: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO file_hashes (path, size, mtime_ns, blake3) "
                "VALUES (?, ?, ?, ?)",
                (path, size, mtime_ns, digest),
            )
            self._conn.commit()

    def hash_file(self, path: Union[str, Path]) -> str:
        """blake3 hex of the file's content, cached on (path, size, mtime_ns)."""
        p = Path(path)
        st = p.stat()
        key_path = str(p.resolve())
        cached = self._lookup(key_path, st.st_size, st.st_mtime_ns)
        if cached is not None:
            return cached
        digest = _blake3_file(p)
        self._store(key_path, st.st_size, st.st_mtime_ns, digest)
        return digest


def _blake3_file(path: Path, chunk_size: int = 1 << 20) -> str:
    try:
        from ppg3 import _core  # type: ignore

        return _core.blake3_file(str(path))
    except Exception:
        pass
    try:
        import blake3 as _blake3_mod

        hasher = _blake3_mod.blake3()
        with open(path, "rb") as fh:
            while True:
                chunk = fh.read(chunk_size)
                if not chunk:
                    break
                hasher.update(chunk)
        return hasher.hexdigest()
    except ImportError:
        pass
    raise canon.MissingBlake3Error(
        "blake3 is required to hash leaf files but neither the compiled "
        "`ppg3._core` extension nor the `blake3` pip package is available."
    )
