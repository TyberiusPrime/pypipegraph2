# -*- coding: utf-8 -*-
__version__ = "0.1"

from pathlib import Path
import logging
from loguru import logger
import contextlib
from .graph import PyPipeGraph, ALL_CORES
from .jobs import *  # TODO
from .exceptions import *  # TODO

_last_new_arguments = None


def new(
    cores=None,
    log_dir=None,
    history_dir=None,
    log_level=None,
    allow_short_filenames=None,
):
    """create a new pipegraph.
    If every argument is None, reuse last arguments
    (or load defaults)
    """
    global _last_new_arguments
    if (
        cores is None
        and log_dir is None
        and history_dir is None
        and log_level is None
        and allow_short_filenames is None
    ):
        if _last_new_arguments is not None:
            (
                cores,
                log_dir,
                history_dir,
                log_level,
                allow_short_filenames,
            ) = _last_new_arguments
    if cores is None:
        cores = ALL_CORES
    if log_dir is None:
        log_dir = Path(".ppg/logs")
    if history_dir is None:
        history_dir = Path(".ppg/history")
    if log_level is None:
        log_level = logging.INFO
    if allow_short_filenames is None:
        allow_short_filenames = False

    _last_new_arguments = cores, log_dir, history_dir, log_level, allow_short_filenames
    global global_pipegraph
    global_pipegraph = PyPipeGraph(
        cores=cores,
        log_dir=log_dir,
        history_dir=history_dir,
        log_level=log_level,
        allow_short_filenames=allow_short_filenames,
    )
    return global_pipegraph


global_pipegraph = new()


def run(print_failures=True, raise_on_job_error=True, event_timeout=5):
    global_pipegraph.run(
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
