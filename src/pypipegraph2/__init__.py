# -*- coding: utf-8 -*-
__version__ = "0.1"

import networkx
from pathlib import Path
import loguru
import logging
from loguru import logger
from .graph import PyPipeGraph, ALL_CORES
from .jobs import *  # TODO
from .exceptions import * # TODO


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
