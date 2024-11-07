import multiprocessing
from pathlib import Path
from . import exceptions
import sys
import os
import json
import queue
import time
import networkx
from .util import escape_logging
from .enums import (
    JobOutcome,
    RunMode,
)
from .exceptions import _RunAgain
from .parallel import CoreLock, async_raise, FakeLock
from threading import Thread
from . import ppg_traceback
import threading
from .interactive import ConsoleInteractive, StatusReport
from .util import (
    log_info,
    log_error,
    log_warning,
    log_debug,
    log_trace,
    log_job_trace,
    console,
)
import copy
from .job_status import RecordedJobOutcome
from .history_comparisons import history_is_different
from collections import deque
import signal
import psutil
import ctypes
import rich
import rich.rule


watcher_parent_pid = None
watcher_ignored_processes = None
watcher_session_id = None


def get_same_session_id_processes():
    for proc in psutil.process_iter():
        try:
            proc_sid = os.getpgid(proc.pid)
            if proc_sid == watcher_session_id:
                yield proc
        except psutil.ProcessLookupError:
            continue


def kill_process_group_but_ppg_runner():
    log_debug("kill_process_group_but_ppg_runner")
    children_to_reap = []
    my_pid = os.getpid()
    for proc in get_same_session_id_processes():
        log_debug(f"{proc} ignored: {proc in watcher_ignored_processes}")
        if not proc in watcher_ignored_processes:
            children_to_reap.append(proc)
    for p in children_to_reap:
        try:
            log_info(
                f"Abort: Watcher is terminating child processes {p.pid} {p.name()} {p.status()}"
            )
            p.terminate()
        except psutil.NoSuchProcess:
            pass
    gone, alive = psutil.wait_procs(children_to_reap, timeout=2)
    for p in alive:
        log_info(
            f"Abort: Watcher is killing child processes {p.pid} {p.name()} {p.status()}"
        )
        p.kill()
    gone, alive = psutil.wait_procs(children_to_reap, timeout=2)
    if alive:
        log_info(f"Still alive? {alive}")


def watcher_unexpected_death_of_parent(_signum, _frame):
    log_error(f"Watcher discovered parent process died unexpectedly {os.getpid()}")
    kill_process_group_but_ppg_runner()
    os._exit(0)


def ignore_ctrl_c(_signum, _frame):
    log_info("Watcher ignored ctrl-c")


def watcher_expected_death_of_parent(_signum, _frame):
    log_debug("Watcher was informed parent process ended regularly")
    kill_process_group_but_ppg_runner()
    os._exit(0)


def spawn_watcher():
    """The watcher registers to be informed when the parent process dies.
    It then goes and kills the process group,
    which we use to reliably tag the spawned processes.

    That means you can't spawn a daemon process from a ppg job.

    The watcher also get's a sig_int when the graph execution ends,
    which also signals it to go and terminate all stragglers.

    """
    global watcher_parent_pid, watcher_ignored_processes, watcher_session_id
    if watcher_session_id is None:
        log_debug("Capturing watcher process group id")
        my_pid = os.getpid()
        my_pgid = os.getpgid(my_pid)
        # ok, the shell gives us a group id, right?
        # we do not mess with that, or the session id, we are not a shell,
        # and we don't fork for the top level managment process.
        # (messing with setgrp and so on has only lead to pain...)
        watcher_session_id = my_pgid
        log_debug(
            f"Watcher identifies process by process group id {watcher_session_id}"
        )
    recv, send = multiprocessing.Pipe()
    ljt("Collecting already running process that will be ignored by the watcher")
    ljt("%s" % (watcher_ignored_processes,))
    pid = os.fork()
    if pid == 0:
        watcher_ignored_processes = list(get_same_session_id_processes())
        watcher_parent_pid = os.getppid()
        parent_process = psutil.Process(watcher_parent_pid)
        # these guys were around before, so we don't kill tehm.

        signal.signal(signal.SIGTERM, watcher_unexpected_death_of_parent)
        signal.signal(signal.SIGUSR2, watcher_expected_death_of_parent)
        signal.signal(signal.SIGINT, ignore_ctrl_c)
        # get watcher to detect parent's death
        prtcl = ctypes.CDLL("libc.so.6")["prctl"]
        PR_SET_PDEATHSIG = 1
        prtcl(PR_SET_PDEATHSIG, signal.SIGUSR2)  # 1 =
        log_debug("entering watcher loop")
        send.send_bytes(b"go")
        send.close()
        sys.stdin.close()
        while True:
            time.sleep(100)  # we Live by signal from here on
        log_error("watcher loop exit - should be unreachable?!")
        os._exit(0)
    else:
        recv.recv_bytes()
        recv.close()
        return pid


ljt = log_job_trace


ExitNow = "___!!!ExitNow!!___"
#   """Token for leave-this-thread-now-signal"""


class Runner:
    """Run a given JobGraph"""

    def __init__(
        self,
        job_graph,
        history,
        event_timeout,
        focus_on_these_jobs,
        jobs_already_run_previously,
        dump_graphml,
        run_id,
        jobs_do_dump_subgraph_debug,
    ):
        from . import _with_changed_global_pipegraph

        log_trace("Runner.__init__")
        self.event_timeout = event_timeout
        with _with_changed_global_pipegraph(JobCollector(job_graph.run_mode)):
            self.job_graph = job_graph
            self.jobs = job_graph.jobs.copy()
            self.job_inputs = copy.deepcopy(
                job_graph.job_inputs
            )  # job_graph.job_inputs.copy()
            self.outputs_to_job_ids = job_graph.outputs_to_job_ids.copy()
            self.next_job_number = self.job_graph.next_job_number
            self.core_lock = CoreLock(job_graph.cores)
            self.fail_counter = 0
            self.done_counter = 0
            self.stat_cache = {}
            self.should_report = True
            self.run_id = (
                run_id  # to allow jobgenerating jobs to run just once per graph.run()
            )

            if not networkx.algorithms.is_directed_acyclic_graph(
                self.job_graph.job_dag
            ):  # pragma: no cover - defensive
                error_fn = (
                    self.job_graph.dir_config.log_dir / "debug_edges_with_cycles.txt"
                )
                networkx.write_edgelist(
                    self.job_graph.job_dag, error_fn, delimiter="\t"
                )
                cycles = list(networkx.simple_cycles(self.job_graph.job_dag))
                raise exceptions.NotADag(
                    f"Not a directed *acyclic* graph. See {error_fn}. Cycles between {cycles}"
                )
            assert len(self.jobs) == len(job_graph.job_dag)

            log_job_trace(f"Focus on these jobs: {focus_on_these_jobs}")
            log_job_trace(f"jobs_already_run_previously: {jobs_already_run_previously}")
            self.dag, self.pruned = self.modify_dag(
                job_graph,
                focus_on_these_jobs,
                jobs_already_run_previously,
                history,  # do we need that?
                dump_graphml,
            )

            job_numbers = set()
            for job_id, job in self.jobs.items():
                # log_job_trace(f"{job_id} {type(self.jobs[job_id])}")
                if job.job_number in job_numbers:
                    raise ValueError(
                        "Duplicate job_number", job.job_number, job_id, job
                    )
                job_numbers.add(job.job_number)
            assert len(job_numbers) == len(self.jobs)
            if len(self.jobs) - len(self.pruned) != len(self.dag):
                raise NotImplementedError(
                    f"Mismatch between len(self.jobs) {len(self.jobs)} - prune_counter {len(self.pruned)} and len(self.dag) {len(self.dag)}"
                )

            log_job_trace(
                "dag "
                + escape_logging(
                    json.dumps(
                        networkx.readwrite.json_graph.node_link_data(self.dag), indent=2
                    )
                ),
            )

            if not networkx.algorithms.is_directed_acyclic_graph(
                self.dag
            ):  # pragma: no cover - defensive
                error_fn = (
                    self.job_graph.dir_config.log_dir / "debug_edges_with_cycles.txt"
                )
                networkx.write_edgelist(self.dag, error_fn, delimiter="\t")
                cycles = list(networkx.simple_cycles(self.dag))
                raise exceptions.NotADag(
                    f"Not a directed *acyclic* graph after modification. See {error_fn}. Cycles between {cycles}"
                )

            # self.event_lock = threading.Lock()
            self.console_lock = threading.Lock()
            self.jobs_to_run_que = queue.PriorityQueue()
            self.threads = []
            self.jobs_that_need_propagation = deque()
            if jobs_do_dump_subgraph_debug:
                j1 = self.jobs[list(jobs_do_dump_subgraph_debug)[0]]
                j1.dump_subgraph_for_debug(
                    jobs_do_dump_subgraph_debug, self.jobs, self.dag
                )

    def _apply_pruning(self, dag, focus_on_these_jobs, jobs_already_run_previously):
        def _recurse_pruning(job_id, reason):
            """This goes forward/downstream"""
            pruned.add(job_id)
            if not hasattr(self.jobs[job_id], "prune_reason"):
                self.jobs[job_id].prune_reason = reason
            for downstream_job_id in dag.successors(job_id):
                _recurse_pruning(downstream_job_id, reason)

        def _recurse_unpruning(job_id):
            """This goes upstream"""
            # log_job_trace(f"_recurse_unpruning {job_id}")
            try:
                pruned.remove(job_id)
                del self.jobs[job_id].prune_reason
            except (KeyError, AttributeError):
                pass
            for downstream_job_id in dag.predecessors(job_id):
                _recurse_unpruning(downstream_job_id)

        if jobs_already_run_previously:
            new_jobs = set(dag.nodes).difference(jobs_already_run_previously)
            ljt(f"new jobs {new_jobs}")
        else:
            new_jobs = set()

        pruned = set()
        if focus_on_these_jobs:  # which is only set in the first run...
            # prune all jobs,
            # then unprune this one and it's predecessors
            pruned.update(set(dag.nodes))  # prune all...
            focus_job_ids = set((x.job_id for x in focus_on_these_jobs))
            for job_id in focus_job_ids.union(new_jobs):
                _recurse_unpruning(job_id)
        else:
            focus_job_ids = set()

        # apply regular pruning
        if jobs_already_run_previously:
            pruned.update(jobs_already_run_previously)

        for job_id in self.jobs:
            if self.jobs[job_id]._pruned:
                if not job_id in focus_job_ids:
                    log_job_trace(f"pruning because of _pruned {job_id}")
                    _recurse_pruning(job_id, job_id)

        for job_id in new_jobs:
            _recurse_unpruning(job_id)

        for job_id in pruned:
            log_job_trace(f"pruned {job_id}")
            try:
                dag.remove_node(job_id)
            except (
                networkx.exception.NetworkXError
            ):  # happens with cleanup nodes that we  omitted
                pass
            # del self.jobs[job_id]
        return pruned

    def modify_dag(  # noqa: C901
        self,
        job_graph,
        focus_on_these_jobs,
        jobs_already_run_previously,
        history,
        dump_graphml,
    ):
        """Modify the DAG to be executed
        by pruning
            - focusing on selected jobs (i.e. prune everything outside of their connected component)
            - removing jobs we ran in the last run-through

        """

        dag = job_graph.job_dag.copy()
        if dump_graphml:
            for node in dag.nodes():
                dag.nodes[node]["label"] = node
                dag.nodes[node]["shape"] = self.jobs[node].__class__.__name__
            networkx.readwrite.graphml.write_graphml(
                dag,
                self.job_graph.log_file.with_name(
                    self.job_graph.log_file.stem + "pre_prune.graphml"
                ),
                named_key_ids=True,
            )

        pruned = self._apply_pruning(
            dag, focus_on_these_jobs, jobs_already_run_previously
        )
        return dag, pruned

    def compare_history(self, job_id_from, job_id_to, last_value, new_value):
        # called from rust
        return history_is_different(self, job_id_from, job_id_to, last_value, new_value)

    @staticmethod
    def get_job_inputs_str(job_graph, job_id):
        # called from rust...
        inputs = sorted(job_graph.job_inputs[job_id])
        return "\n".join(inputs)

    def build_evaluator(self, history):
        from .pypipegraph2 import PPG2Evaluator

        e = PPG2Evaluator(
            history,
            self.compare_history,
            lambda job_id: self.__class__.get_job_inputs_str(self.job_graph, job_id),
        )
        # todo: see how much we can push into rust of
        # the whole networkx business.
        # no need to keep multiple graphs, I suppose.
        for job_id in self.dag.nodes:
            job = self.jobs[job_id]
            e.add_node(job_id, job.eval_job_kind)
        for a, b in self.dag.edges:
            e.add_edge(b, a)
        return e

    def run(self, history, last_job_states, print_failures):  # noqa:C901
        """Actually run the current DAG"""
        from . import global_pipegraph

        job_count = len(global_pipegraph.jobs)  # track if new jobs are being created

        log_trace("Runner.__run__")

        self.pid = (
            os.getpid()
        )  # so we can detect if we return inside a forked process and exit (safety net)
        self.start_time = time.time()
        self.aborted = False
        self.stopped = False
        self.print_failures = print_failures
        self.output_hashes = {}
        self.last_job_states = last_job_states

        self.history = history  # so the jobs can peak at it and avoid reprocessing

        self.evaluator = self.build_evaluator(history)
        self.evaluator_lock = threading.Lock()
        self.evaluator.event_startup()
        self.evaluation_done = False
        self.main_thread_event = threading.Event()
        self.check_for_new_jobs = threading.Event()
        # for things that need to be performed in the main thread
        # like forking. Set the main_thread_event after filling it!
        # (We can't select on Events, nor reliably on Ques)
        self.main_thread_callbacks = queue.Queue()

        self.jobs_in_flight = []
        self.jobs_all_cores_in_flight = 0
        self.job_outcomes = {}

        self.watcher_pid = spawn_watcher()
        self._start_job_executing_threads()

        try:
            self._interactive_start()

            while True:
                # ljt("Waiting for evaluation done")
                if self.main_thread_event.wait(5):  # todo: timeout, sanity checks.
                    if self.evaluation_done:
                        # ljt(f"evaluation done happend? {self.stopped}")
                        break
                    try:
                        while True:
                            cb = self.main_thread_callbacks.get(block=False)
                            cb()
                    except queue.Empty:
                        pass
                else:  # periodic timeout
                    if self.aborted:
                        break
            # self._interactive_report()

        finally:
            # log_job_trace("Joining threads")
            with self.evaluator_lock:
                self.stopped = True
            for t in self.threads:
                t.join()
            if self.aborted:  # todo: refactor
                log_error("run was aborted")

                #           reap_children(children_before_run)

            if hasattr(self, "_status"):  # todo: what is this? rich status?
                print("status stop necessary")
                self._status.stop()
            # log_info("interactive stop")
            self._interactive_stop()
            # unregister_reaper(reaper)
            assert psutil.pid_exists(self.watcher_pid)
            ljt("Sending sigusr2 to watcher")
            os.kill(self.watcher_pid, signal.SIGUSR2)
            gone, alive = psutil.wait_procs(
                [psutil.Process(self.watcher_pid)], timeout=10
            )
            if alive:
                log_error("Watcher was still hanging around?!")
            # os.waitpid(self.watcher_pid, 1)
            log_debug("finished waiting for watcher")

        for job_id in self.pruned:
            if job_id in self.job_outcomes:
                raise ValueError("Pruned & having outcome?!")
            self.job_outcomes[job_id] = RecordedJobOutcome(
                job_id, JobOutcome.Pruned, None
            )
            ljt(f"Logging as pruned {job_id}")

        for job_id in self.evaluator.list_upstream_failed_jobs():
            ljt(f"upstream failed {job_id}")
            if job_id in self.job_outcomes:
                raise ValueError("Upstream Failed & having other outcome?!")
            self.job_outcomes[job_id] = RecordedJobOutcome(
                job_id, JobOutcome.UpstreamFailed, None
            )  # todo: payload = which upstream

        for job_id in self.jobs:
            if not job_id in self.job_outcomes:
                self.job_outcomes[job_id] = RecordedJobOutcome(
                    job_id, JobOutcome.Skipped, None
                )

        if len(global_pipegraph.jobs) != job_count and not self.aborted:
            log_info(
                f"created new jobs. _RunAgain issued {len(global_pipegraph.jobs)} != {job_count}"
            )
            for job_id in global_pipegraph.jobs:
                if job_id not in self.jobs:
                    log_job_trace(f"new job {job_id}")
            raise _RunAgain(
                (self.job_outcomes, self.evaluator.new_history())
            )  # self.job_states)
        log_trace("Left runner.run()")
        if self.aborted:
            self.evaluator.event_abort()

        return self.job_outcomes, self.evaluator.new_history()

    def _interactive_start(self):
        """Activate the interactive thread"""
        if self.job_graph.run_mode in (RunMode.CONSOLE, RunMode.CONSOLE_INTERACTIVE):
            self.interactive = ConsoleInteractive()
            self.last_status_time = time.time()
            self.interactive.start(self)

            def report():
                while not self.stopped and not self.aborted:
                    if self.should_report:
                        self.should_report = False
                        self._do_interactive_report()
                    time.sleep(1)

            t = Thread(target=report)
            self.threads.append(t)
            t.start()

    def _interactive_stop(self):
        """Stop the interactive thread (if present)"""
        if hasattr(self, "interactive"):
            self.interactive.stop()

    def _interactive_report(self):
        self.should_report = True

    def _do_interactive_report(self):
        if hasattr(self, "interactive") and hasattr(self.interactive, "status"):
            t = time.time()
            if (
                # t - self.last_status_time >= 0.5  # don't update more than every half second.
                True
            ):
                waiting = len(
                    [
                        x
                        for x in self.jobs_in_flight
                        if getattr(self.jobs[x], "waiting", False)
                    ]
                )
                self.interactive.report_status(
                    StatusReport(
                        len(self.jobs_in_flight) - waiting,
                        waiting,
                        self.done_counter,
                        len(self.dag),
                        self.fail_counter,
                    )
                )
                self.last_status_time = t

    def abort(self):
        """Kill all running jobs and leave runner.
        Called from the interactive interface
        """
        # must be reentrant, called from signal!
        #log_info("runner.abort called")
        self.abort_time = time.time()
        self.aborted = True
        self.check_for_new_jobs.set()
        self.evaluation_done = True
        self.main_thread_event.set()
        for t in self.threads:
            try:
                async_raise(t.ident, KeyboardInterrupt)
            except ValueError:
                pass

    def stop(self):
        """Leave runner after current jobs
        Called from the interactive interface

        """
        self.stopped = True
        self.abort_time = time.time()

    def _start_job_executing_threads(self):
        """Fire up the default number of threads"""
        for ii in range(self.job_graph.cores):
            self._start_another_thread()

    def _job_failed_last_time(self, job_id) -> bool:
        """Did this job fail last time?"""
        res = (
            self.last_job_states
            and job_id in self.last_job_states
            and self.last_job_states[job_id].outcome == JobOutcome.Failed
        )
        log_trace(f"_job_failed_last_time: {job_id}: {res}")
        return res

    def _start_another_thread(self):
        """Fire up another thread (if all current threads are blocked with multi core threads.

        This prevents stalling, since it will ensure that there's a thread around
        to do the SingleCore jobs.

        Note that we don't fire up threads without limit - at one point, you can still
        stall the graph
        """
        if self.stopped or self.aborted:
            return
        t = Thread(target=self._executing_thread)
        self.threads.append(t)
        t.start()

    def log_failed_job(self, job_id, error):
        if self.print_failures:
            log = log_error
        else:
            log = log_debug
        with self.console_lock:  # not self._job_failed_last_time(job_id): # Todo
            try:
                job = self.jobs[job_id]
                # mock failure in case of abort/stop
                if isinstance(error, exceptions.JobCanceled):
                    if self.aborted or self.stopped:
                        return
                    else:
                        raise NotImplementedError(
                            "JobCanceled outside of stopped/aborted state?!"
                        )
                # log error to file. Todo: move to job_state
                if self.job_graph.dir_config.error_dir is not None:
                    error_file = (
                        self.job_graph.dir_config.error_dir
                        / self.job_graph.time_str
                        / (str(job.job_number) + "_exception.txt")
                    )
                else:
                    error_file = None

                if hasattr(error.args[1], "stacks"):
                    stacks = error.args[1]
                else:
                    stacks = None
                if self.print_failures and self.job_graph.run_mode in (
                    RunMode.CONSOLE,
                    RunMode.CONSOLE_INTERACTIVE,
                ):
                    console.print("\n")
                    console.print(
                        rich.rule.Rule(
                            f"Error in {job_id}", characters="=", style="white"
                        )
                    )

                if self.job_graph.dir_config.error_dir is not None:
                    with open(error_file, "w") as ef:
                        ef.write(f"JobId: {job_id}\n")
                        ef.write(f"Class: {job.__class__.__name__}\n")
                        ef.write(
                            f"Runtime until failure: {job.stop_time - job.start_time:.2f}s\n"
                        )
                        ef.write("Input jobs:\n")
                        for parent_id in sorted(self.dag.predecessors(job_id)):
                            ef.write(
                                f"\t{parent_id} ({self.jobs[parent_id].__class__.__name__})\n"
                            )
                        ef.write("\n\n")
                        if stacks is not None:
                            ef.write(
                                stacks._format_rich_traceback_fallback(
                                    include_locals=True, include_formating=False
                                )
                            )
                        else:
                            ef.write(str(error))
                            ef.write("(no stack available)")
                        if hasattr(job, "stdout"):
                            ef.write("\n\n")
                            ef.write("job stdout:\n")
                            ef.write(str(job.stdout))
                        else:
                            ef.write("\n\nstdout: not available\n")
                        if hasattr(job, "stderr"):
                            ef.write("\n\n")
                            ef.write("job stderr:\n")
                            ef.write(str(job.stderr))
                        else:
                            ef.write("\n\nstderr: not available\n")
                        ef.flush()

                    log(
                        f"Job failed  : '{job_id}'\n"
                        f"\n\tMore details (stdout, locals) in {error_file}\n"
                        f"\tFailed after {job.run_time:.2}s.\n"
                    )
                else:
                    log(
                        f"\tJob failed: {job_id}\n"
                        f"\n\t(Error directory not configured - no extended error log)\n"
                        f"\tFailed after {job.run_time:.2}s.\n"
                    )
                if stacks is not None and self.print_failures:
                    if self.job_graph.run_mode in (
                        RunMode.CONSOLE,
                        RunMode.CONSOLE_INTERACTIVE,
                    ):
                        console.print(
                            stacks._format_rich_traceback_fallback(
                                False, True, skip_ppg=True
                            ).replace("\n", "\n\t")
                        )
                    else:
                        log_warning(
                            stacks._format_rich_traceback_fallback(False, False)
                        )

                else:
                    log(error)
                    log("(no stack available)")

            except Exception as e:
                log_error(
                    f"An exception ocurred reporting on a job failure for {job_id}: {e}. The original job failure has been swallowed."
                )

    def _executing_thread(self):
        """The inner function of the threads actually executing the jobs"""
        cwd = (
            os.getcwd()
        )  # so we can detect if the job cahnges the cwd (don't do that!)
        fake_lock = FakeLock()
        try:
            try:
                while not (self.stopped or self.aborted):
                    job_id = None
                    error = None
                    outputs = None
                    try:
                        do_sleep = False
                        with self.evaluator_lock:
                            cleanups = self.evaluator.jobs_ready_for_cleanup()

                            for cleanup_job_id in cleanups:
                                try:
                                    log_debug(f"Job cleanup: {cleanup_job_id}")
                                    self.jobs[cleanup_job_id].cleanup()
                                except Exception as e:
                                    log_error(f"Cleanup had an exception {repr(e)}")
                                self.evaluator.event_job_cleanup_done(cleanup_job_id)

                            rr = self.evaluator.next_job_ready_to_run()
                            if rr is None:
                                if self.evaluator.is_finished():
                                    # ljt("detected finished")
                                    self.stopped = True
                                    self.evaluation_done = True
                                    self.main_thread_event.set()
                                    break
                                else:
                                    # ljt("not finished")
                                    # this happens when we have more threads than jobs?
                                    # to run at *this* junction.
                                    do_sleep = True
                                    if not self.jobs_in_flight:
                                        log_error(
                                            f"jobs the engine considers running {self.evaluator.jobs_running()}"
                                        )
                                        log_error(
                                            f"The engine considers itself finished = {self.evaluator.is_finished()}"
                                        )
                                        log_error(
                                            "Evaluator is not finished, reports no jobs ready to run, but no jobs currently running -> a bug in the state machine. No way forward, aborting graph executing (cleanly). Graph written to debug.txt "
                                        )

                                        # from .pypipegraph2 import enable_logging_to_file
                                        # enable_logging_to_file("rust_debug.log")

                                        self.evaluator.debug_is_finished()
                                        Path("ppg_evaluator_debug.txt").write_text(
                                            self.evaluator.debug()
                                        )
                                        # self.evaluator.reconsider_all_jobs() # if this helps' we're looking at a propagation failure. Somewhere.
                                        if (
                                            not self.evaluator.is_finished()
                                            and not self.evaluator.jobs_ready_to_run()
                                        ):
                                            print(
                                                "reconsidering all jobs did not lead to recovery, going down hard (=die)"
                                            )
                                            self.interactive._cmd_die(False)
                                        else:
                                            log_error(
                                                "Could recover by doing reconsider_all. Still going down hard (=die), this needs debugging!"
                                            )
                                            self.interactive._cmd_die(False)

                            else:
                                # ljt(f"to run {rr}") # nice accidential O(n^2) there...
                                job_id = rr
                                self.jobs_in_flight.append(job_id)
                                # ljt(f"added {job_id} {self.jobs_in_flight}")
                                self.evaluator.event_now_running(job_id)

                                job = self.jobs[job_id]
                                job.waiting = True
                                job.actual_cores_needed = -1
                                self._interactive_report()
                                job.start_time = time.time()  # assign it just in case anything fails before acquiring the lock
                                job.stop_time = float("nan")
                                job.run_time = float("nan")

                                c = job.resources.to_number(self.core_lock.max_cores)
                                job.actual_cores_needed = c
                                # log_trace(
                                # f"{job_id} cores: {c}, max: {self.core_lock.max_cores}, jobs_in_flight: {len(self.jobs_in_flight)}, all_cores_in_flight: {self.jobs_all_cores_in_flight}, threads: {len(self.threads)}"
                                # )
                                job_failed_last_time = self._job_failed_last_time(
                                    job_id
                                )
                                if not job_failed_last_time and (c > 1):
                                    # we could stall all SingleCores/RunsHere by having all_cores blocking all but one thread (which executes another all_core).
                                    # if we detect that situation, we spawn another one.
                                    self.jobs_all_cores_in_flight += 1
                                    if (
                                        self.jobs_all_cores_in_flight
                                        >= len(self.threads)
                                        and len(self.threads)
                                        <= self.job_graph.cores
                                        * 5  # at one point, we either have to let threads die again, or live with
                                        # the wasted time b y stalling.
                                    ):
                                        ljt(
                                            "All threads blocked by Multi core jobs - starting another one"
                                        )
                                        self._start_another_thread()
                        # letting go of evaluator lock
                        if do_sleep:
                            self.check_for_new_jobs.wait(
                                60
                            )  # not sure why we even have a timeout?
                            self.check_for_new_jobs.clear()
                            continue
                        else:
                            pass

                        # ljt(f"wait for {job_id}")
                        if c == 0:
                            log_error(f"Cores was 0! {job.job_id} {job.resources}")

                        if job_failed_last_time:
                            lock = fake_lock
                        else:
                            lock = self.core_lock

                        with lock.using(c):
                            if self.stopped or self.aborted:
                                continue  # -> while not stopped -> break
                            job.start_time = time.time()  # the *actual* start time
                            log_info(f"Job started : '{job.job_id}'")
                            job.waiting = False
                            self._interactive_report()
                            ljt(f"Go {job_id}")

                            try:
                                # that's history-output
                                old_history_for_this_job = self.history.get(
                                    job_id, None
                                )
                                if old_history_for_this_job is not None:
                                    old_history_for_this_job = json.loads(
                                        old_history_for_this_job
                                    )
                                else:
                                    old_history_for_this_job = {}
                                if job_failed_last_time:
                                    ljt(
                                        "Job failed last time (new jobs were generated, graph rerun, not running again: {job_id}"
                                    )
                                    outputs = None
                                    error = self.last_job_states[job_id].payload
                                else:
                                    outputs = job.run(
                                        self, old_history_for_this_job
                                    )  # job_state.historical_output)
                                # ljt(f"job successfull {job_id}")
                            finally:
                                # we still check the cwd, even if the job failed!
                                if os.getcwd() != cwd:
                                    os.chdir(
                                        cwd
                                    )  # restore and hope we can recover enough to actually print the exception, I suppose.
                                    log_error(
                                        f"{job_id} changed current_working_directory. Since ppg2 is multithreaded, you must not do this in jobs that RunHere"
                                    )
                                    error = exceptions.JobContractError(
                                        f"{job_id} changed current_working_directory. Since ppg2 is multithreaded, you must not do this in jobs that RunHere"
                                    )
                                    outputs = None
                                    raise error

                    except SystemExit as e:  # pragma: no cover - happens in spawned process, and we don't get coverage logging for it thanks to os._exit
                        log_trace(
                            "SystemExit in spawned process -> converting to hard exit"
                        )
                        if os.getpid() != self.pid:
                            os._exit(e.args[0])
                    except Exception as e:
                        if isinstance(e, KeyboardInterrupt):  # happens on abort
                            raise
                        elif isinstance(e, exceptions.JobError):
                            pass  # take it at face value
                        else:
                            exception_type, exception_value, tb = sys.exc_info()
                            captured_tb = ppg_traceback.Trace(
                                exception_type, exception_value, tb
                            )
                            # print(captured_tb)
                            e = exceptions.JobError(
                                e,
                                captured_tb,
                            )
                        error = e
                    finally:
                        if job_id is not None:
                            job.stop_time = time.time()
                            job.run_time = job.stop_time - job.start_time
                            # ljt(f"end {job_id} {self.jobs_in_flight}")
                            # log_trace(f"Leaving thread for {job_id}")
                            if outputs is None and error is None:
                                # this happes when the job get's terminated by abort
                                # log_debug(f"job aborted {job_id}")
                                error = exceptions.JobError(
                                    "Aborted", KeyboardInterrupt()
                                )
                                # raise ValueError("Should not happen")
                            if outputs is not None and set(outputs.keys()) != set(
                                self.jobs[job_id].outputs
                            ):  # the 2nd one is a list...
                                # log_trace(
                                #     f"\t{job_id} returned the wrong set of outputs. "
                                #     f"Should be {escape_logging(str(set(self.jobs[job_id].outputs)))}, was {escape_logging(str(set(outputs.keys())))}"
                                # )

                                error = exceptions.JobContractError(
                                    f"\t{job_id} returned the wrong set of outputs. "
                                    f"Should be {escape_logging(str(set((self.jobs[job_id].outputs))))}, was {escape_logging(str(set(outputs.keys())))}",
                                )
                                outputs = None

                            if outputs is not None:
                                self.job_outcomes[job_id] = RecordedJobOutcome(
                                    job_id, JobOutcome.Success, outputs
                                )
                                str_history = json.dumps(
                                    outputs, sort_keys=True, indent=1
                                )
                                # ljt(f"success {job_id} str_history {str_history}")
                                if (job.stop_time != job.stop_time) or (
                                    job.stop_time - job.start_time > 1
                                ):
                                    runtime = job.stop_time - job.start_time
                                    finish_msg = f"Job finished: '{job_id}'. Runtime: {runtime:.2f}s"
                                    log_info(finish_msg)
                                else:
                                    runtime = -1
                                # log_debug(finish_msg)
                                self.done_counter += 1
                                # only on output jobs..
                                if job.eval_job_kind == "Output":
                                    self.job_graph.post_event(
                                        {
                                            "type": "job_done",
                                            "job_id": job_id,
                                            "run_time": runtime,
                                            "start_time": getattr(job, "start_time", 0),
                                            "stop_time": getattr(job, "stop_time", 0),
                                        }
                                    )
                                try:
                                    with self.evaluator_lock:
                                        self.evaluator.event_job_success(
                                            job_id, str_history
                                        )
                                    failed = False
                                except Exception as e:
                                    log_error(
                                        f"Recording job success failed for {job_id}. Likely constraint violation?: Message was '{e}'"
                                    )
                                    with self.evaluator_lock:  # just so we don't mess up the file.
                                        log_filename = (
                                            self.job_graph.dir_config.error_dir
                                            / self.job_graph.time_str
                                            / "constraint_violations.jobs"
                                        )
                                        log_error(
                                            f"Job id has been logged to {log_filename}\n. You might want to use ppg-filter-constraint-violations after fixing the problem."
                                        )
                                        with open(log_filename, "a") as op:
                                            op.write(f"{job_id}\n")

                                    self.job_outcomes[job_id] = RecordedJobOutcome(
                                        job_id, JobOutcome.Failed, str(e)
                                    )

                            else:
                                self.fail_counter += 1
                                ljt(f"failure {job_id} - {error}")
                                self.job_outcomes[job_id] = RecordedJobOutcome(
                                    job_id, JobOutcome.Failed, error
                                )
                                with self.evaluator_lock:
                                    self.evaluator.event_job_failure(job_id)
                                try:
                                    self.log_failed_job(job_id, error)
                                except Exception as e:
                                    log_error(
                                        f"logging job failure failed {job_id} {e}"
                                    )
                            self.jobs_in_flight.remove(job_id)
                            if c > 1:
                                self.jobs_all_cores_in_flight -= 1

                            self.job_outcomes[job_id].run_time = job.run_time

                            self._interactive_report()
                            self.check_for_new_jobs.set()
                        elif error:
                            print(error)
                            self.check_for_new_jobs.set()
                            raise error

            except (KeyboardInterrupt, SystemExit):  # happens on abort
                log_trace(
                    f"Keyboard Interrupt received {time.time() - self.abort_time}"
                )
                pass
            except Exception as e:
                log_error(
                    f"Captured exception outside of loop - should not happen {type(e)} {str(e)}. Check error log. Run is being aborted"
                )
                exception_type, exception_value, tb = sys.exc_info()
                captured_tb = ppg_traceback.Trace(exception_type, exception_value, tb)
                log_error(str(captured_tb))
                self.abort()
                raise
            except BaseException as e:
                log_error(f"panic from the rust side {e}. Killing process")
                os.kill(os.getpid(), signal.SIGTERM)
                sys.exit(1)
        finally:
            # log_job_trace(f"left thread {len(self.threads)} {job_id}")
            ...


class JobCollector:
    """only in place during the dag modification step of Runner.__init__,
    so that the jobs that are only created during run (cleanup, )
    do not end up in the actual graph.
    """

    def __init__(self, run_mode):
        self.clear()
        self.run_mode = run_mode

    def add(self, job):
        self.jobs[job] = job

    def clear(self):
        self.jobs = {}
        self.edges = set()
