# -*- coding: utf-8 -*-
__version__ = "0.1"

from pathlib import Path
import logging
from loguru import logger
import contextlib
from .graph import PyPipeGraph, ALL_CORES
from .jobs import *  # TODO
from .exceptions import *  # TODO
from . import enums
from .enums import Resources, RunMode

reuse_last_or_default = object()
default = object()

_last_new_arguments = {}


def _last_or_default(name, value, default_value):
    if value is default:
        result = default_value
    elif value is reuse_last_or_default:
        result = _last_new_arguments.get(name, default_value)
    else:
        result = value
    _last_new_arguments[name] = result
    return result


def new(
    cores=reuse_last_or_default,
    log_dir=reuse_last_or_default,
    history_dir=reuse_last_or_default,
    run_dir=reuse_last_or_default,
    log_level=reuse_last_or_default,
    allow_short_filenames=reuse_last_or_default,
    run_mode=reuse_last_or_default,
):
    """create a new pipegraph.
    You may pase reuse_last_or_default to all values
    to reuse the last value,
    or default to use the true default

    (or load defaults)
    """
    global global_pipegraph
    l = locals()
    arguments = {
        name: _last_or_default(name, l[name], default_arg)
        for name, default_arg in [
            ("cores", ALL_CORES),
            ("log_dir", Path(".ppg/logs")),
            ("history_dir", Path(".ppg/history")),
            ("run_dir", Path(".ppg/run")),
            ("log_level", logging.INFO),
            ("allow_short_filenames", False),
            ("run_mode", RunMode.CONSOLE),
        ]
    }
    print(arguments, run_mode)
    global_pipegraph = PyPipeGraph(**arguments)
    return global_pipegraph


global_pipegraph = new()


def run(print_failures=True, raise_on_job_error=True, event_timeout=5):
    if global_pipegraph is None:
        raise ValueError("Must instantiate a pipegraph before you can run it.")

    return global_pipegraph.run(
        print_failures=print_failures,
        raise_on_job_error=raise_on_job_error,
        event_timeout=event_timeout,
    )


def job_trace(msg):
    """log at the JobTrace level"""
    logger.opt(depth=1).log("JobTrace", msg)


logger.job_trace = job_trace


@contextlib.contextmanager
def _with_changed_global_pipegraph(new):
    global global_pipegraph
    old = global_pipegraph
    try:
        global_pipegraph = new
        yield new
    finally:
        global_pipegraph = old
