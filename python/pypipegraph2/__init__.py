# -*- coding: utf-8 -*-
__version__ = "3.4.0"

import logging
import contextlib
from pathlib import Path
from .graph import PyPipeGraph, ALL_CORES, DirConfig
from .jobs import (
    FileGeneratingJob,
    MultiFileGeneratingJob,
    TempFileGeneratingJob,
    MultiTempFileGeneratingJob,
    DataLoadingJob,
    ValuePlusHash,
    UseInputHashesForOutput,
    AttributeLoadingJob,
    DictEntryLoadingJob,
    CachedDataLoadingJob,
    CachedAttributeLoadingJob,
    CachedDictEntryLoadingJob,
    PlotJob,
    FunctionInvariant,
    _FunctionInvariant,
    FileInvariant,
    ParameterInvariant,
    JobGeneratingJob,
    Job,
    JobList,
    SharedMultiFileGeneratingJob,
    NotebookInvariant,
    NotebookJob,
    ExternalJob,
    ExternalOutputPath,
    DependsOnInvariant,
    CachedJobTuple,
    PlotJobTuple,
)

from .exceptions import (
    PPGException,
    NotADag,
    FatalGraphException,
    JobOutputConflict,
    JobContractError,
    JobDied,
    JobRedefinitionError,
    RunFailed,
    RunFailedInternally,
    JobsFailed,
    JobError,
    HistoryLoadingFailed,
    JobEvaluationFailed,
)
from .enums import Resources, RunMode
from . import util
from .util import assert_uniqueness_of_object
from .pypipegraph2 import enable_logging as enable_rust_logging

try:
    # if you're using numba, we need a thread and fork safe threading layer
    # we request this here,
    # but it will fail the first time numba parallelization is used
    # if tbb can't be found.
    # otherwise you'll get some shiny
    # 'Terminating: fork() called from a process already using GNU OpenMP, this is unsafe';
    # error messages.

    import numba

    numba.config.THREADING_LAYER = "safe"
except ImportError:
    pass


reuse_last_or_default = object()
default = object()

_last_new_arguments = {}
_ppg1_compatibility_mode = False


def _last_or_default(name, value, default_value):
    if value is default:  # pragma: no cover
        if hasattr(default_value, "callme"):
            result = default_value()
        else:
            result = default_value
    elif value is reuse_last_or_default:
        result = _last_new_arguments.get(
            name,
            default_value if not hasattr(default_value, "callme") else default_value(),
        )
    else:
        result = value
    _last_new_arguments[name] = result
    return result


def new(
    cores=reuse_last_or_default,
    run_mode=reuse_last_or_default,
    dir_config=reuse_last_or_default,
    log_level=reuse_last_or_default,
    allow_short_filenames=reuse_last_or_default,
    log_retention=reuse_last_or_default,
    cache_dir=reuse_last_or_default,
    prevent_absolute_paths=reuse_last_or_default,
    report_done_filter=reuse_last_or_default,
    push_events=reuse_last_or_default,
):
    """create a new pipegraph.
    You may pass reuse_last_or_default to all values
    to reuse the last value,
    or default to use the true default

    (or load defaults)


    Log retention is how many old logs (+ the current one) we
    keep.
    """
    global global_pipegraph
    locs = locals()
    arguments = {
        name: _last_or_default(name, locs[name], default_arg)
        for name, default_arg in [
            ("cores", ALL_CORES),
            ("dir_config", _get_default_dir_config),
            ("log_level", logging.INFO),  # that's the one for the log file
            ("allow_short_filenames", False),
            ("run_mode", RunMode.CONSOLE),
            ("log_retention", 3),
            ("prevent_absolute_paths", True),
            ("report_done_filter", 1),
            ("push_events", False),
        ]
    }
    util.do_jobtrace_log = arguments["log_level"] <= 6
    # if arguments['run_mode'] != RunMode.NONINTERACTIVE:
    # raise ValueError()
    global_pipegraph = PyPipeGraph(**arguments)

    if _ppg1_compatibility_mode:
        from . import ppg1_compatibility

        ppg1_compatibility._add_graph_comp(global_pipegraph)

    return global_pipegraph


def _get_default_dir_config():
    # we need the script name
    import lib_programname

    path_to_program = lib_programname.get_path_executed_script()  # type: pathlib.Path
    if path_to_program is None:
        raise ValueError(
            "Could not determine path to executed script. Set a DirConfig when calling ppg2.new()"
        )
    pn = Path(path_to_program).name
    if (  # fallback for before this change.
        Path(".ppg/history/ppg_history.2.zstd").exists()
        and (pn == "run.py")
        and not Path(".ppg/per_script/run.py/history/ppg_history.2.zstd").exists()
    ):
        return DirConfig(".ppg")
    else:
        print(f"defaulting to dirconfig = .ppg/per_script/{pn}")
        return DirConfig(".ppg/per_script/" + pn)


_get_default_dir_config.callme = True

global_pipegraph = None


def change_global_pipegraph(value):
    """Helper to swap out the global pipegraph from ppg1-compability.util"""
    global global_pipegraph
    global_pipegraph = value


def run(
    print_failures=True,
    raise_on_job_error=True,
    event_timeout=1,
    dump_graphml=None,
):
    if global_pipegraph is None:
        raise ValueError("Must instantiate a pipegraph before you can run it.")

    return global_pipegraph.run(
        print_failures=print_failures,
        raise_on_job_error=raise_on_job_error,
        event_timeout=event_timeout,
        dump_graphml=dump_graphml,
    )


def inside_ppg():
    return global_pipegraph is not None


@contextlib.contextmanager
def _with_changed_global_pipegraph(new):
    global global_pipegraph
    old = global_pipegraph
    try:
        global_pipegraph = new
        yield new
    finally:
        global_pipegraph = old


def replace_ppg1():
    """Turn all ppg1 references into actual ppg2
    objects.
    Best effort, but the commonly used API should be well supported.
    Try to do this before anything imports ppg1.


    One notably exception is in cores_needed/use_cores,
    where ppg2 only supports 1/almost_all/all, and ppg1 also supported
    a number of cores > 1. This get's convert to almost_all (-1 in ppg1 parlance)

    Also jobs often throw TypeError, instead of ValueError if you pass the arguments
    in the wrong order. This shouldn't affect any working code though.
    """
    from . import ppg1_compatibility

    global _ppg1_compatibility_mode
    _ppg1_compatibility_mode = True
    ppg1_compatibility.replace_ppg1()


def unreplace_ppg1():
    """undo replace_ppg1."""
    from . import ppg1_compatibility

    global _ppg1_compatibility_mode
    _ppg1_compatibility_mode = False

    ppg1_compatibility.unreplace_ppg1()


__all__ = [
    "new",
    "run",
    "Job",
    "JobList",
    "FileGeneratingJob",
    "MultiFileGeneratingJob",
    "TempFileGeneratingJob",
    "MultiTempFileGeneratingJob",
    "MultiFileGeneratingJob",
    "DataLoadingJob",
    "ValuePlusHash",
    "UseInputHashesForOutput",
    "AttributeLoadingJob",
    "CachedDataLoadingJob",
    "CachedAttributeLoadingJob",
    "DictEntryLoadingJob",
    "CachedDictEntryLoadingJob",
    "PlotJob",
    "FunctionInvariant",
    "_FunctionInvariant",
    "FileInvariant",
    "ParameterInvariant",
    "NotebookInvariant",
    "NotebookJob",
    "JobGeneratingJob",
    "SharedMultiFileGeneratingJob",
    "ExternalJob",
    "ExternalOutputPath",
    "PPGException",
    "NotADag",
    "FatalGraphException",
    "JobOutputConflict",
    "JobContractError",
    "JobDied",
    "JobRedefinitionError",
    "RunFailed",
    "RunFailedInternally",
    "JobsFailed",
    "JobError",
    "HistoryLoadingFailed",
    "JobEvaluationFailed",
    "Resources",
    "RunMode",
    "replace_ppg1",
    "unreplace_ppg1",
    "inside_ppg",
    "assert_uniqueness_of_object",
    "enable_rust_logging",
]
