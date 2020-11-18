from typing import Optional, Union, Dict
import collections
import os
import textwrap
import sys
import pickle
import signal
import networkx
import time
from pathlib import Path
from loguru import logger
from . import exceptions
from .runner import Runner, JobState
from enum import Enum
from .util import escape_logging


logger.level("JobTrace", no=6, color="<yellow>", icon="🐍")


class ALL_CORES:
    pass


class RunMode(Enum):
    INTERACTIVE = 1  # certain redefinitions: FatalGraphException, interactive console, ctrl-c does not work
    NOTEBOOK = 2  # certain redefinitions: warning, no interactive console (todo: gui), control-c,/abort works TODO
    NONINTERACTIVE = 3  # such as testing, redefinitions like interactive, but no gui, ctrl-c works TODO


def default_run_mode():
    # TODO
    return RunMode.INTERACTIVE


class PyPipeGraph:
    history_dir: Optional[Path]
    log_dir: Optional[Path]
    log_level: int
    running: bool

    def __init__(
        self,
        cores: Union[int, ALL_CORES],
        log_dir: Optional[Path],
        history_dir: Path,
        log_level: int,
        paths: Optional[Dict[str, Union[Path, str]]] = None,
        run_mode: RunMode = default_run_mode(),
    ):
        self.cores = cores
        if log_dir:
            self.log_dir = Path(log_dir)
        else:
            self.log_dir = None
        self.history_dir = Path(history_dir) if history_dir else None
        self.log_level = log_level
        self.paths = {k: Path(v) for (k, v) in paths} if paths else None
        self.run_mode = run_mode

        self.jobs = {}
        self.job_dag = networkx.DiGraph()
        self.job_inputs = collections.defaultdict(set)
        self.running = False
        self.outputs_to_job_ids = {}

    def run(
        self, print_failures: bool = True, raise_on_job_error=True
    ) -> Dict[str, JobState]:
        if not networkx.algorithms.is_directed_acyclic_graph(self.job_dag):
            raise exceptions.NotADag()
        if self.log_dir:
            self.log_dir.mkdir(exist_ok=True, parents=True)
            logger.add(
                self.log_dir / f"ppg_run_{time.time():.0f}.log", level=self.log_level
            )
            logger.info(f"Run is go {id(self)} pid: {os.getpid()}")
        self.history_dir.mkdir(exist_ok=True, parents=True)
        try:
            result = None
            if self.run_mode == RunMode.INTERACTIVE:
                self._install_signals()
            self.running = True
            result = Runner(self).run()
            do_raise = False
            for job_id, job_state in result.items():
                if job_state.state == JobState.Failed:
                    if print_failures:
                        msg = textwrap.indent(str(job_state.error), "\t")
                        logger.error(f"{job_id} failed.\n {escape_logging(msg)}")
                    if raise_on_job_error:
                        do_raise = True
            self.last_run_result = result
            if do_raise:
                raise exceptions.RunFailed()
            return result
        finally:
            if result is not None:
                new_history = {
                    job_id: (
                        result[job_id].updated_input,
                        result[job_id].updated_output,
                    )
                    for job_id in result
                }
                self.save_historical(new_history)
            self.running = False
            if print_failures:
                self._print_failures()
            if self.run_mode == RunMode.INTERACTIVE:
                self._restore_signals()
            logger.trace("Run is done")

    def _get_history_fn(self):
        fn = Path(sys.argv[0]).name
        return self.history_dir / f"ppg_status_{fn}"

    def load_historical(self):
        logger.trace("load_historicals")
        if self.history_dir is None:
            return
        fn = self._get_history_fn()
        history = {}
        if fn.exists():
            logger.debug("Historical existed")
            with open(fn, "rb") as op:
                try:
                    while True:
                        job_id = pickle.load(op)
                        inputs = pickle.load(op)
                        history[job_id] = inputs
                except EOFError:
                    pass
        return history

    def save_historical(self, historical):
        logger.trace("save_historical")
        if self.history_dir is None:
            return
        fn = self._get_history_fn()
        with open(fn, "wb") as op:
            for job_id, input_hashes in historical.items():
                pickle.dump(job_id, op, pickle.HIGHEST_PROTOCOL)
                pickle.dump(input_hashes, op, pickle.HIGHEST_PROTOCOL)

    def _print_failures(self):
        logger.trace("print_failures")
        # TODO

    def _install_signals(self):
        """make sure we don't crash just because the user logged of.
        Should also block ctrl-c

        """
        logger.trace("_install_signals")

        def hup():  # pragma: no cover
            logger.debug("user logged off - continuing run")

        self._old_signal_up = signal.signal(signal.SIGHUP, hup)

    def _restore_signals(self):
        logger.trace("_restore_signals")
        if self._old_signal_up:
            signal.signal(signal.SIGHUP, self._old_signal_up)

    def add(self, job):
        for output in job.outputs:
            if output in self.outputs_to_job_ids:
                # already being done somewhere else
                if self.outputs_to_job_ids[output] == job.job_id:
                    # but it is in essence the same same job
                    pass  # we replace the job, keeping upstreams/downstream edges
                else:
                    # if self.run_mode != RunMode.NOTEBOOK: todo: accept in notebooks by removing the other  jobs and warning.
                    raise exceptions.JobOutputConflict(
                        job, self.jobs[self.outputs_to_job_ids[output]]
                    )
            self.outputs_to_job_ids[
                output
            ] = job.job_id  # todo: seperate this into two dicts?
        self.jobs[job.job_id] = job
        self.job_dag.add_node(job.job_id)

    def add_edge(self, upstream_job, downstream_job):
        self.job_dag.add_edge(upstream_job.job_id, downstream_job.job_id)
        if upstream_job.cleanup_job:
            self.job_dag.add_edge(
                downstream_job.job_id, upstream_job.cleanup_job.job_id
            )
            self.job_inputs[upstream_job.cleanup_job.job_id].add(downstream_job.job_id)
