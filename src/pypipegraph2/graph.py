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
from .util import escape_logging, CPUs
from .enums import RunMode
from .exceptions import _RunAgain


logger.level("JobTrace", no=6, color="<yellow>", icon="🐍")


class ALL_CORES:
    pass


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
        run_dir: Path,
        log_level: int,
        run_mode: RunMode,
        paths: Optional[Dict[str, Union[Path, str]]] = None,
        allow_short_filenames=False,
    ):

        if cores is ALL_CORES:
            self.cores = CPUs()
        else:
            self.cores = int(cores)
        if log_dir:
            self.log_dir = Path(log_dir)
        else:
            self.log_dir = None
        self.history_dir = Path(history_dir)
        self.run_dir = Path(run_dir)
        self.log_level = log_level
        self.paths = {k: Path(v) for (k, v) in paths} if paths else None
        self.run_mode = run_mode
        self.jobs = {}  # the job objects, by id
        self.job_dag = (
            networkx.DiGraph()
        )  # a graph. Nodes: job_ids, edges -> must be done before
        self.job_inputs = collections.defaultdict(
            set
        )  # necessary inputs (ie. outputs of other jobs)
        self.outputs_to_job_ids = (
            {}
        )  # so we can find the job that generates an output: todo: should be outputs_to_job_id or?
        self.run_id = 0
        self.allow_short_filenames = allow_short_filenames

    def run(
        self, print_failures: bool = True, raise_on_job_error=True, event_timeout=5
    ) -> Dict[str, JobState]:
        if not networkx.algorithms.is_directed_acyclic_graph(self.job_dag):
            print(networkx.readwrite.json_graph.node_link_data(self.job_dag))
            raise exceptions.NotADag()
        else:
            # print(networkx.readwrite.json_graph.node_link_data(self.job_dag))
            pass
        self.fill_dependency_callbacks()
        if self.log_dir:
            self.log_dir.mkdir(exist_ok=True, parents=True)
            logger.add(
                self.log_dir / f"ppg_run_{time.time():.0f}.log", level=self.log_level
            )
            logger.info(
                f"Run is go {id(self)} pid: {os.getpid()}, run_id {self.run_id}"
            )
        self.history_dir.mkdir(exist_ok=True, parents=True)
        self.run_dir.mkdir(exist_ok=True, parents=True)
        self.do_raise = []
        try:
            result = None
            if self.run_mode == RunMode.CONSOLE:
                self._install_signals()
            history = self.load_historical()
            max_runs = 5
            while True:
                max_runs -= 1
                if max_runs == 0:  # pragma: no cover
                    raise ValueError("endless loop")
                try:
                    runner = Runner(self, history, event_timeout)
                    result = runner.run(self.run_id, result)
                    self.run_id += 1
                    self.update_history(result, history)
                    break
                except _RunAgain as e:
                    result = e.args[0]
                    self.update_history(e.args[0], history)
                    pass
            for job_id, job_state in result.items():
                if job_state.state == JobState.Failed:
                    if print_failures:
                        msg = textwrap.indent(str(job_state.error), "\t")
                        logger.error(f"{job_id} failed.\n {escape_logging(msg)}")
                        print(f"{job_id} failed.\n {escape_logging(msg)}")
                    if raise_on_job_error and not self.do_raise:
                        self.do_raise.append("At least one job failed")
            self.last_run_result = result
            if self.do_raise:
                raise exceptions.RunFailed(*self.do_raise)
            return result
        finally:
            if print_failures:
                self._print_failures()
            if self.run_mode == RunMode.CONSOLE:
                self._restore_signals()
            logger.trace("Run is done")

    def update_history(self, job_results, history):
        # we must keep the history of jobs unseen in this run.
        # tly to allow partial runs
        new_history = (
            history  # .copy() don't copy. we reuse this in the subsequent runs
        )
        new_history.update(
            {
                job_id: (
                    job_results[job_id].updated_input,
                    job_results[job_id].updated_output,
                )
                for job_id in job_results
            }
        )
        self.save_historical(new_history)

    def _get_history_fn(self):
        fn = Path(sys.argv[0]).name
        return self.history_dir / f"ppg_status_{fn}.history"  # don't end on .py

    def load_historical(self):
        logger.trace("load_historicals")
        fn = self._get_history_fn()
        history = {}
        if fn.exists():
            logger.debug("Historical existed")
            try:
                with open(fn, "rb") as op:
                    try:
                        counter = 0
                        while True:
                            try:
                                logger.job_trace(f"History read {counter}")
                                counter += 1
                                job_id = None
                                job_id = pickle.load(op)
                                logger.job_trace(f"read job_id {job_id}")
                                inputs_and_outputs = pickle.load(op)
                                history[job_id] = inputs_and_outputs
                            except (TypeError, pickle.UnpicklingError) as e:
                                logger.job_trace(f"unipckling error {e}")
                                if job_id is None:
                                    raise exceptions.RunFailed(
                                        "Could not depickle job id - history file is borked beyond automatic recovery"
                                    )
                                else:
                                    msg = (
                                        f"Could not depickle invariant for {job_id} - "
                                        "check code for depickling bugs. "
                                        "Job will rerun, probably until the (de)pickling bug is fixed."
                                        f"\n Exception: {e}"
                                    )
                                    self.do_raise.append(msg)
                                # use pickle tools to read the pickles op codes until
                                # the end of the current pickle, hopefully allowing decoding of the next one
                                # of course if the actual on disk file is messed up beyond this,
                                # we're done for.
                                import pickletools

                                try:
                                    list(pickletools.genops(op))
                                except Exception as e:
                                    raise exceptions.RunFailed(
                                        "Could not depickle invariants - "
                                        f"depickling of {job_id} failed, could not skip to next pickled dataset"
                                        f" Exception was {e}"
                                    )

                    except EOFError:
                        pass
            except Exception as e:
                raise exceptions.RunFailed("Could not load history data", e)

        return history

    def save_historical(self, historical):
        logger.trace("save_historical")
        fn = self._get_history_fn()
        raise_keyboard_interrupt = False
        raise_run_failed_internally = False
        with open(fn, "wb") as op:
            # robust history saving.
            # for KeyboardInterrupt, write again
            # for other exceptions: skip job
            for job_id, input_and_output_hashes in historical.items():
                try_again = True
                while try_again:
                    try_again = False
                    try:
                        a = pickle.dumps(
                            job_id, pickle.HIGHEST_PROTOCOL
                        ) + pickle.dumps(
                            input_and_output_hashes, pickle.HIGHEST_PROTOCOL
                        )
                        op.write(a)
                    except KeyboardInterrupt:
                        try_again = True
                        raise_keyboard_interrupt = True
                    except Exception as e:
                        logger.error(f"Could not pickle state for {job_id} - {e}")
                        raise_run_failed_internally = (job_id, e)
        if raise_run_failed_internally:
            job_id, exc = raise_run_failed_internally
            raise exceptions.RunFailedInternally(
                f"Pickling of {job_id} inputs/outputs failed.", exc
            )
        if raise_keyboard_interrupt:
            logger.error("Keyboard interrupt")
            raise KeyboardInterrupt()

    def fill_dependency_callbacks(self):
        # we need this copy,
        # for the callbacks may create jobs
        # so we can't simply iterate over the jobs.values()
        with_callback = [j for j in self.jobs.values() if j.dependency_callbacks]
        # logger.info(f"with callbacks {[j.job_id for j in with_callback]}")
        if not with_callback:
            return
        for j in with_callback:
            dc = j.dependency_callbacks
            j.dependency_callbacks = (
                []
            )  # must reset before run, might add new ones, right?
            for c in dc:
                # logger.info(f"{j.job_id}, {c}")
                j.depends_on(c())
        self.fill_dependency_callbacks()  # nested?

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
        job.job_number = len(self.jobs) - 1

    def add_edge(self, upstream_job, downstream_job):
        if not upstream_job.job_id in self.jobs:
            raise KeyError(f"{upstream_job} not in this graph. Call job.readd() first")
        if not downstream_job.job_id in self.jobs:
            raise KeyError(
                f"{downstream_job} not in this graph. Call job.readd() first"
            )

        self.job_dag.add_edge(upstream_job.job_id, downstream_job.job_id)

    def has_edge(self, upstream_job, downstream_job):
        return self.job_dag.has_edge(upstream_job.job_id, downstream_job.job_id)
