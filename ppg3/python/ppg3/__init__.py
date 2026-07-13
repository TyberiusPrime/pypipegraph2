"""ppg3 — constructive-trace build system, Python front-end.

See ``ppg3/PPG3_DESIGN.md`` and ``ppg3/CONTRACT.md`` for the full design.
This package works entirely without the compiled ``ppg3._core`` extension
except for the final :func:`run`; everything else (job definition,
canonicalization, recipe hashing, transport selection) is pure Python and
importable/testable on its own.
"""

from .jobs import (
    CommandJob,
    DataJob,
    FetchJob,
    File,
    FileJob,
    GraphJob,
    In,
    Out,
    Params,
    Resources,
    Retain,
    Store,
    Tool,
    UnsandboxedJob,
    new,
)
from .localscope import DefinitionError
from .run import run, session_stop
from .tools import PyEnv, ToolSpec
from .transport import Source

__all__ = [
    "new",
    "run",
    "session_stop",
    "FileJob",
    "CommandJob",
    "DataJob",
    "UnsandboxedJob",
    "FetchJob",
    "GraphJob",
    "File",
    "Params",
    "Resources",
    "Retain",
    "Store",
    "ToolSpec",
    "PyEnv",
    "Source",
    "In",
    "Out",
    "Tool",
    "DefinitionError",
]

__version__ = "0.1.0"
