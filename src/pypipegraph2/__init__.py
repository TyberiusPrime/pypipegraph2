# -*- coding: utf-8 -*-
__version__ = '2.3.0'

from pathlib import Path
import logging
from loguru import logger
import contextlib
from .graph import PyPipeGraph, ALL_CORES
from .jobs import (
    FileGeneratingJob,
    MultiFileGeneratingJob,
    TempFileGeneratingJob,
    MultiTempFileGeneratingJob,
    DataLoadingJob,
    ValuePlusHash,
    AttributeLoadingJob,
    CachedDataLoadingJob,
    CachedAttributeLoadingJob,
    PlotJob,
    FunctionInvariant,
    FileInvariant,
    ParameterInvariant,
    JobGeneratingJob,
    Job,
    JobList,
    SharedMultiFileGeneratingJob,
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
    JobEvaluationFailed,
)
from .enums import Resources, RunMode
from . import util

reuse_last_or_default = object()
default = object()

_last_new_arguments = {}


def _last_or_default(name, value, default_value):
    if value is default:  # pragma: no cover
        result = default_value
    elif value is reuse_last_or_default:
        result = _last_new_arguments.get(name, default_value)
    else:
        result = value
    _last_new_arguments[name] = result
    return result


def new(
    cores=reuse_last_or_default,
    run_mode=reuse_last_or_default,
    log_dir=reuse_last_or_default,
    error_dir=reuse_last_or_default,
    history_dir=reuse_last_or_default,
    run_dir=reuse_last_or_default,
    log_level=reuse_last_or_default,
    allow_short_filenames=reuse_last_or_default,
    log_retention=reuse_last_or_default,
    cache_dir=reuse_last_or_default,
    prevent_absolute_paths=reuse_last_or_default,
    report_done_filter=reuse_last_or_default,
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
            ("log_dir", Path(".ppg/logs")),
            ("error_dir", Path(".ppg/errors")),
            ("history_dir", Path(".ppg/history")),
            ("run_dir", Path(".ppg/run")),
            ("log_level", logging.DEBUG),  # that's the one for the log file
            ("allow_short_filenames", False),
            ("run_mode", RunMode.CONSOLE),
            ("log_retention", 3),
            ("cache_dir", Path("cache")),
            ("prevent_absolute_paths", True),
            ('report_done_filter', 1)
        ]
    }
    util.do_jobtrace_log = arguments["log_level"] <= 6
    # if arguments['run_mode'] != RunMode.NONINTERACTIVE:
    # raise ValueError()
    global_pipegraph = PyPipeGraph(**arguments)
    return global_pipegraph


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

    ppg1_compatibility.replace_ppg1()


def unreplace_ppg1():
    """undo replace_ppg1."""
    from . import ppg1_compatibility

    ppg1_compatibility.unreplace_ppg1()


from .util import assert_uniqueness_of_object


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
    "AttributeLoadingJob",
    "CachedDataLoadingJob",
    "CachedAttributeLoadingJob",
    "PlotJob",
    "FunctionInvariant",
    "FileInvariant",
    "ParameterInvariant",
    "JobGeneratingJob",
    "SharedMultiFileGeneratingJob",
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
    "JobEvaluationFailed",
    "Resources",
    "RunMode",
    "replace_ppg1",
    "unreplace_ppg1",
    "inside_ppg",
    "assert_uniqueness_of_object",
]
