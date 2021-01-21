from typing import Optional, Union, Dict
import shutil
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
from rich.logging import RichHandler



logger.level("JobTrace", no=6, color="<yellow>", icon="🐍")
logger.configure(handlers=[{"sink":RichHandler(markup=True), "format":"{message}"}])


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
        error_dir: Optional[Path],
        history_dir: Path,
        run_dir: Path,
        log_level: int,
        run_mode: RunMode,
        paths: Optional[Dict[str, Union[Path, str]]] = None,
        allow_short_filenames=False,
        log_retention = None
    ):

        if cores is ALL_CORES:
            self.cores = CPUs()
        else:
            self.cores = int(cores)
        self.time_str = time.strftime('%Y-%m-%d_%H-%M-%S')
        if log_dir:
            self.log_dir = Path(log_dir)
        else:
            self.log_dir = None
        if error_dir:
            self.error_dir = error_dir / self.time_str
        else:
            self.error_dir = None
        self.history_dir = Path(history_dir)
        self.run_dir = Path(run_dir)
        self.log_level = log_level
        self.log_retention = log_retention
        #self.paths = {k: Path(v) for (k, v) in paths} if paths else {}
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
        if self.error_dir:
            self.error_dir.mkdir(exist_ok=True, parents=True)
        if self.log_dir:
            self.log_dir.mkdir(exist_ok=True, parents=True)
            fn = Path(sys.argv[0]).name
            logger.add(
                self.log_dir / f"{fn}-{self.time_str}.log", level=self.log_level
            )
            if self.log_level != 20: #logging.INFO:
                logger.add(sink=sys.stdout, level=self.log_level)
            import threading
            logger.info(
                f"Run is go {threading.get_ident()} pid: {os.getpid()}, run_id {self.run_id}"
            )
            self.cleanup_logs()
            self.cleanup_errors()
        self.history_dir.mkdir(exist_ok=True, parents=True)
        self.run_dir.mkdir(exist_ok=True, parents=True)
        self.do_raise = []
        try:
            result = None
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
                    if raise_on_job_error and not "At least one job failed" in self.do_raise:
                        self.do_raise.append("At least one job failed")
            self.last_run_result = result
            if self.do_raise:
                raise exceptions.RunFailed(*self.do_raise)
            return result
        finally:
            logger.info("Run is done")
            if print_failures:
                self._print_failures()
            self._restore_signals()


    def cleanup_logs(self):
        if not self.log_dir or self.log_retention is None:
            return
        fn = Path(sys.argv[0]).name
        pattern = f"{fn}-*.log"
        files = sorted(self.log_dir.glob(pattern))
        if len(files) > self.log_retention:
            remove = files[:-self.log_retention]
            for f in remove:
                os.unlink(f)

    def cleanup_errors(self):
        if not self.error_dir or self.log_retention is None:
            return
        err_dirs = sorted([x for x in self.error_dir.parent.glob("*") if x.is_dir()])
        if len(err_dirs) > self.log_retention:
            remove = err_dirs[:-self.log_retention]
            for f in remove:
                shutil.rmtree(f)



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
        done = False
        while not done:
            try:
                self.save_historical(new_history)
                done = True
            except KeyboardInterrupt as e:
                self.do_raise.append(e) 
                pass

    def _get_history_fn(self):
        fn = Path(sys.argv[0]).name
        return self.history_dir / f"{fn}.ppg_history"  # don't end on .py

    def load_historical(self):
        logger.trace("load_historicals")
        fn = self._get_history_fn()
        history = {}
        if fn.exists():
            logger.job_trace("Historical existed")
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
        if Path(fn).exists():
            fn.rename(fn.with_suffix(fn.suffix + '.backup'))
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
        # TODO - actually, we kind of already do that inline, don't we.

    def _install_signals(self):
        """make sure we don't crash just because the user logged of.
        Should also block ctrl-c

        """
        logger.trace("_install_signals")

        def hup(*args, **kwargs):  # pragma: no cover
            logger.debug("user logged off - continuing run")
        def sigint(*args, **kwargs):
            logger.info("CTRL-C has been disabled")

        if self.run_mode is (RunMode.CONSOLE):
            self._old_signal_hup = signal.signal(signal.SIGHUP, hup)
        if self.run_mode in (RunMode.CONSOLE, RunMode.NOTEBOOK):
            self._old_signal_int = signal.signal(signal.SIGINT, sigint)

    def _restore_signals(self):
        logger.trace("_restore_signals")
        if hasattr(self, '_old_signal_hup'):
            signal.signal(signal.SIGHUP, self._old_signal_hup)
        if hasattr(self, '_old_signal_int'):
                signal.signal(signal.SIGINT, self._old_signal_int)
        else:
            pass
            #raise ValueError("WHen does this happen")

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
