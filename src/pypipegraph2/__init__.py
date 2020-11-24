# -*- coding: utf-8 -*-
__version__ = "0.1"

from pathlib import Path
import logging
from loguru import logger
import contextlib
from .graph import PyPipeGraph, ALL_CORES
from .jobs import *  # TODO
from .exceptions import *  # TODO


def new(
    cores=ALL_CORES,
    log_dir=Path(".ppg/logs"),
    history_dir=Path(".ppg/history"),
    log_level=logging.INFO,
):
    global global_pipegraph
    global_pipegraph = PyPipeGraph(
        cores=cores, log_dir=log_dir, history_dir=history_dir, log_level=log_level
    )
    return global_pipegraph


global_pipegraph = new()


def run():
    global_pipegraph.run()


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

