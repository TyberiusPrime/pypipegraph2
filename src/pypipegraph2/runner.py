from . import exceptions
import sys
import os
import queue
import time
import networkx
from .util import escape_logging
from .enums import (
    JobKind,
    ValidationState,
    ProcessingStatus,
    JobOutcome,
    RunMode,
    ShouldRun,
    Resources,
)
from .exceptions import _RunAgain
from .parallel import CoreLock, async_raise
from threading import Thread
from . import ppg_traceback
import threading
from rich.console import Console
from .interactive import ConsoleInteractive, StatusReport
from .util import log_info, log_error, log_warning, log_debug, log_trace, log_job_trace
import copy
from .job_status import JobStatus
from collections import deque

ljt = log_job_trace


class ExitNow:
    """Token for leave-this-thread-now-signal"""

    pass


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
    ):
        from . import _with_changed_global_pipegraph

        log_trace("Runner.__init__")
        self.event_timeout = event_timeout
        with _with_changed_global_pipegraph(JobCollector(job_graph.run_mode)):
            self.job_graph = job_graph
            self.jobs = job_graph.jobs.copy()
            self.job_inputs = copy.deepcopy(
                job_graph.job_inputs
            )  #  job_graph.job_inputs.copy()
            self.outputs_to_job_ids = job_graph.outputs_to_job_ids.copy()
            self.next_job_number = self.job_graph.next_job_number
            self.core_lock = CoreLock(job_graph.cores)
            self.fail_counter = 0
            self.job_states = (
                {}
            )  # get's partially filled by modify_dag, and then later in this function
            self.run_id = (
                run_id  # to allow jobgenerating jobs to run just once per graph.run()
            )

            # flat_before = networkx.readwrite.json_graph.node_link_data(
            # job_graph.job_dag
            # )
            if not networkx.algorithms.is_directed_acyclic_graph(
                self.job_graph.job_dag
            ):  # pragma: no cover - defensive
                error_fn = self.job_graph.log_dir / "debug_edges_with_cycles.txt"
                networkx.write_edgelist(self.job_graph.job_dag, error_fn)
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
                history,
                dump_graphml,
            )
            # flat_after = networkx.readwrite.json_graph.node_link_data(job_graph.job_dag)
            # import json

            # assert flat_before == flat_after
            import json

            job_numbers = set()
            for job_id, job in self.jobs.items():
                # log_job_trace(f"{job_id} {type(self.jobs[job_id])}")
                job_numbers.add(job.job_number)
            assert len(job_numbers) == len(self.jobs)
            if len(self.jobs) - len(self.pruned) != len(self.dag):
                raise NotImplementedError(
                    f"Mismatch between len(self.jobs) {len(self.jobs)} - prune_counter {prune_counter} and len(self.dag) {len(self.dag)}"
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
                error_fn = self.job_graph.log_dir / "debug_edges_with_cycles.txt"
                networkx.write_edgelist(self.dag, error_fn)
                cycles = list(networkx.simple_cycles(self.dag))
                raise exceptions.NotADag(
                    f"Not a directed *acyclic* graph after modification. See {error_fn}. Cycles between {cycles}"
                )

            for job_id in networkx.algorithms.dag.topological_sort(
                self.dag
            ):  # must be topological so we can do upstreams whilst building
                historical_input, historical_output = history.get(
                    job_id, ({}, {})
                )  # todo: support renaming jobs.
                s = JobStatus(job_id, self, historical_input, historical_output)
                log_trace(
                    f"Loaded history for {job_id} in: {len(s.historical_input)}, out: {len(s.historical_output)}"
                )
                self.job_states[job_id] = s
                s.initialize()  # so that output_needed can access the history
            self.event_lock = threading.Lock()
            self.jobs_to_run_que = queue.SimpleQueue()
            self.threads = []
            self.jobs_that_need_propagation = deque()

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

        if focus_on_these_jobs:  # which is only set in the first run...
            # prune all jobs,
            # then unprune this one and it's predecessors
            pruned = set(dag.nodes)  # prune all...
            for job_id in set((x.job_id for x in focus_on_these_jobs)).union(new_jobs):
                _recurse_unpruning(job_id)
        else:
            # apply regular pruning
            if jobs_already_run_previously:
                pruned = jobs_already_run_previously
            else:
                pruned = set()
            for job_id in new_jobs:
                _recurse_unpruning(job_id)
            for job_id in self.jobs:
                if self.jobs[job_id]._pruned:
                    _recurse_pruning(job_id, job_id)
        for job_id in pruned:
            log_job_trace(f"pruned {job_id}")
            try:
                dag.remove_node(job_id)
            except networkx.exception.NetworkXError:  # happens with cleanup nodes that we  omitted
                pass
            # del self.jobs[job_id]
        return pruned

    def _add_cleanup(self, dag, job):
        downstreams = [
            x
            for x in dag.neighbors(job.job_id)
            if self.jobs[x].job_kind is not JobKind.Cleanup
        ]  # depending on other cleanups makes littlesense
        if not downstreams:
            # if the job has no downstreams
            # it won't run.
            log_debug(f"{job.job_id} had no downstreams - not adding a cleanup")
            return

        cleanup_job = job.cleanup_job_class(job)
        cleanup_job.job_number = self.next_job_number
        self.next_job_number += 1
        self.jobs[cleanup_job.job_id] = cleanup_job
        dag.add_node(cleanup_job.job_id)
        log_debug(f"creating cleanup {cleanup_job.job_id}")
        for o in cleanup_job.outputs:
            log_trace(f"Storing cleanup oututs_to_job_ids {o} = {cleanup_job.job_id}")
            self.outputs_to_job_ids[o] = cleanup_job.job_id
        log_trace(f"{job.job_id} cleanup adding")
        for downstream_job_id in downstreams:
            log_trace(f"add downstream edge: {downstream_job_id}, {cleanup_job.job_id}")

            dag.add_edge(downstream_job_id, cleanup_job.job_id)
            self.job_inputs[cleanup_job.job_id].update(
                self.jobs[downstream_job_id].outputs
            )
        return cleanup_job

    def _modify_dag_for_conditional_job(self, dag, job, history):
        """A a conditional job is one that only runs if it's downstreams need it.
        Examples are DataLoadingJobs and TempFileGeneratingJobs.

        We prune them if they have no downstreams,
        and we add cleanups

        """
        # upstreams = dag.predecessors(job.job_id)
        # todo: should just prune instead?
        # but we need to prune before this, and missing downstreams
        # might be the result of pruning...
        downstreams = list(dag.successors(job.job_id))
        if not downstreams:
            log_job_trace(f"ommiting conditional job because of no output {job.job_id}")
            dag.remove_node(job.job_id)
            del self.jobs[job.job_id]
            # mark it as skipped
            historical_input, historical_output = history.get(
                job.job_id, ({}, {})
            )  # todo: support renaming jobs.
            self.job_states[job.job_id] = JobStatus(
                job.job_id, self, historical_input, historical_output
            )
            # no need to do the downstream calls - this is just an ignored job
            self.job_states[job.job_id].proc_state = ProcessingStatus.Done
            self.job_states[job.job_id].outcome = JobOutcome.Skipped
            return 0
        elif job.cleanup_job_class:
            cleanup_job = self._add_cleanup(dag, job)
            return 1

    def modify_dag(  # noqa: C901
        self,
        job_graph,
        focus_on_these_jobs,
        jobs_already_run_previously,
        history,
        dump_graphml,
    ):
        """Modify the DAG to be executed
        by
            - splitting conditional jobs (DataLoading, TempFile)
              into one virtual job per downstream that is dependend
              on the downstreams hull (see below)
            - adding CleanupJobs, (e.g. for TempFileGeneratingJobs)
            - pruning
            - focusing on selected jobs (i.e. prune everything outside of their connected component)
            - removing jobs we ran in the last run-through

        """
        # import json

        dag = job_graph.job_dag.copy()
        if dump_graphml:
            for node in dag.nodes():
                dag.nodes[node]["label"] = node
                dag.nodes[node]["shape"] = self.jobs[node].__class__.__name__
            networkx.readwrite.graphml.write_graphml(
                dag,
                self.job_graph.log_dir / "graph_pre_mod.graphml",
                named_key_ids=True,
            )

        pruned = self._apply_pruning(
            dag, focus_on_these_jobs, jobs_already_run_previously
        )

        known_job_ids = list(networkx.algorithms.dag.topological_sort(dag))
        for job_id in reversed(known_job_ids):  # todo: do we need reversed
            job = self.jobs[job_id]
            if job.job_kind in (JobKind.Temp, JobKind.Loading):
                self._modify_dag_for_conditional_job(dag, job, history)
            elif job.cleanup_job_class:
                log_error(
                    f"Unconditionaly, but cleanup? {job}, {job.cleanup_job_class}"
                )
                raise NotImplementedError(
                    "Currently only 'conditional' jobs support cleanup jobs."
                )  # probably easy to fix though, just call _add_cleanup_job on it?

            else:
                log_trace(f"no modify dag for {job.job_id}")
        if dump_graphml:
            for node in dag.nodes():
                dag.nodes[node]["label"] = node
                dag.nodes[node]["shape"] = self.jobs[node].__class__.__name__
            networkx.readwrite.graphml.write_graphml(
                dag,
                self.job_graph.log_dir / "graph_post_mod.graphml",
                named_key_ids=True,
            )

        return dag, pruned

    def run(self, last_job_states, print_failures):  # noqa:C901
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
        self.new_history = {}  # what are the job outputs this time.
        self.last_job_states = last_job_states

        self.events = queue.Queue()

        todo = len(self.dag)
        log_job_trace("here we go")
        for job_id in self.dag.nodes:  # those are without the pruned nodes
            no_inputs = not self.job_inputs[job_id]
            # output_needed = self.jobs[job_id].output_needed(self)
            failed_last_time = self._job_failed_last_time(job_id)
            if no_inputs:  # could be an initial job
                log_job_trace(
                    f"{job_id} no inputs. failed_last_time: {failed_last_time}"
                )
                if failed_last_time:
                    log_job_trace(f"{job_id} Failing because of failure last time (1)")
                    self.job_states[job_id].failed(self.job_states[job_id].error, True)
                    todo -= 1  # no need to send a message for this
                else:
                    self.jobs_that_need_propagation.append(job_id)
            elif failed_last_time:
                log_job_trace(f"{job_id} Failing because of failure last time (2)")
                self.job_states[job_id].failed(self.job_states[job_id].error, True)
                todo -= 1  # no need to send a message for this
        log_job_trace(
            f"Finished initial pass, jobs_that_need_propagation now filled {len(self.jobs_that_need_propagation)}"
        )

        self.jobs_in_flight = []
        self.jobs_all_cores_in_flight = 0
        self._start_job_executing_threads()

        self.jobs_done = 0
        try:
            self._interactive_start()
            # self._interactive_report()
            while todo:
                while self.jobs_that_need_propagation:
                    log_job_trace(f"jtnp: {len(self.jobs_that_need_propagation)}")
                    # log_job_trace(f"jtnp: {self.jobs_that_need_propagation}")
                    check_job_id = self.jobs_that_need_propagation.popleft()
                    check_state = self.job_states[check_job_id]
                    should_run_before = check_state.should_run
                    validation_before = check_state.validation_state
                    new = check_state.update()
                    if new is None:
                        raise ValueError("none return")
                    log_job_trace(f"New for checking {new}")
                    self.jobs_that_need_propagation.extend(new)
                    log_job_trace(
                        f"{check_job_id}: State: {check_state.proc_state} {check_state.should_run}"
                    )
                    if check_state.proc_state is ProcessingStatus.ReadyToRun:
                        # this job has transitioned into ReadyToRun  .
                        if check_state.should_run is ShouldRun.No:
                            check_state.skipped()
                            self._push_event(
                                "JobSkipped", (check_job_id,)
                            )  # for accounting
                            # tell teh upstreams - we won't receive the other one.
                            for upstream_job_id in check_state.upstreams:
                                self.jobs_that_need_propagation.append(upstream_job_id)
                        elif check_state.should_run is ShouldRun.Yes:
                            check_state.proc_state = ProcessingStatus.Schedulded
                            if (
                                check_state.outcome is JobOutcome.Failed
                            ):  # internal failuer
                                self._push_event(
                                    "JobFailed",
                                    (
                                        check_job_id,
                                        exceptions.JobEvaluationFailed(
                                            "output_needed raised an exception",
                                            check_state.error,
                                        ),
                                    ),
                                )  # which will in turn upstream fail all downstreams
                            else:
                                self.jobs_to_run_que.put(check_job_id)
                    elif check_state.proc_state is ProcessingStatus.Schedulded:
                        pass
                    elif (check_state.validation_state != validation_before) or (
                        check_state.should_run != should_run_before
                    ):
                        log_job_trace(
                            f"validation changed. Tell upstreams {check_job_id}"
                        )
                        # we have just changed the validation state
                        # and the upstreams might care about that to decide
                        # whether they need to run
                        for upstream_job_id in check_state.upstreams:
                            if (
                                self.jobs[upstream_job_id].is_conditional()
                                and not self.job_states[
                                    upstream_job_id
                                ].should_run.is_decided()
                            ):
                                self.jobs_that_need_propagation.append(upstream_job_id)

                try:
                    ev = self.events.get(timeout=self.event_timeout)
                    if ev[0] == "AbortRun":
                        log_info("Aborting/Stopping run as requested")
                        todo = 0
                        # self.stopped = True no need to tell the threads, we KeyboardInterrupt them.
                        break
                except queue.Empty:
                    # long time, no event.
                    if not self.jobs_in_flight:
                        log_error(
                            f"Coding error lead to que empty with no jobs in flight? todo: {todo}"
                        )
                        # ok, a coding error has lead to us not finishing
                        # the todo graph.
                        for job_id in self.job_states:
                            log_warning(
                                f"{job_id}, {self.job_states[job_id].proc_state} {self.jobs[job_id].depth}"
                            )
                        raise exceptions.RunFailedInternally
                    continue

                log_job_trace(
                    f"<-handle {ev[0]} {escape_logging(ev[1][0])}, todo: {todo}"
                )
                d = self._handle_event(ev)
                todo += d
                self.jobs_done -= d
                self._interactive_report()
                log_trace(f"<-done - todo: {todo}")

            if not self.aborted:
                while self.jobs_in_flight and not self.aborted:
                    try:
                        ev = self.events.get(0.1)
                    except queue.Empty:  # pragma: no cover
                        break
                    else:
                        # log_trace(f"<-handle {ev[0]} {escape_logging(ev[1][0])}")
                        self._handle_event(ev)
                # once more for good measure...
                while True:
                    try:
                        ev = self.events.get_nowait()
                    except queue.Empty:
                        break
                    else:
                        # log_trace(f"<-handle {ev[0]} {escape_logging(ev[1][0])}")
                        self._handle_event(ev)

            if (
                self.aborted
            ):  # it might have gotten set by an 'abort' following a stop in the meantim!
                # log_job_tarce(f"No of threads when aborting {len(self.threads)}")
                for t in self.threads:
                    log_job_trace(
                        f"Asking thread {t.ident} to terminate at next Python call {time.time() - self.abort_time}"
                    )
                    try:
                        async_raise(t.ident, KeyboardInterrupt)
                    except ValueError:
                        pass

        finally:
            # log_job_trace("Joining threads")
            for t in self.threads:
                self.jobs_to_run_que.put(ExitNow)
            for t in self.threads:
                t.join()
            # log_job_trace("Joined threads")
            # now capture straglers
            # todo: replace this with something guranteed to work.
            while True:
                try:
                    ev = self.events.get_nowait()
                except queue.Empty:
                    break
                else:
                    log_trace(f"<-handle {ev[0]} {escape_logging(ev[1][0])}")
                    self._handle_event(ev)

            if hasattr(self, "_status"):
                self._status.stop()
            log_info("interactive stop")
            self._interactive_stop()

        for job_id in self.pruned:
            ljt(f"Logging as pruned {job_id}")
            assert not job_id in self.job_states
            self.job_states[job_id] = JobStatus(job_id, self, None, None)
            self.job_states[job_id].was_pruned()

        if len(global_pipegraph.jobs) != job_count and not self.aborted:
            log_info(
                f"created new jobs. _RunAgain issued {len(global_pipegraph.jobs)} != {job_count}"
            )
            for job_id in global_pipegraph.jobs:
                if job_id not in self.jobs:
                    log_job_trace(f"new job {job_id}")
            raise _RunAgain(self.job_states)
        log_trace("Left runner.run()")

        return self.job_states

    def _interactive_start(self):
        """Activate the interactive thread"""
        if self.job_graph.run_mode is RunMode.CONSOLE:
            self.interactive = ConsoleInteractive()
            self.last_status_time = time.time()
            self.interactive.start(self)

    def _interactive_stop(self):
        """Stop the interactive thread (if present)"""
        if hasattr(self, "interactive"):
            self.interactive.stop()

    def _interactive_report(self):
        if hasattr(self, "interactive"):
            t = time.time()
            if (
                t - self.last_status_time >= 0.5
            ):  # don't update more than every half second.
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
                        self.jobs_done,
                        len(self.dag),
                        self.fail_counter,
                    )
                )
                self.last_status_time = t

    def abort(self):
        """Kill all running jobs and leave runner.
        Called from the interactive interface
        """
        self.abort_time = time.time()
        self.aborted = True
        self._push_event("AbortRun", (False,))

    def stop(self):
        """Leave runner after current jobs
        Called from the interactive interface

        """
        self.stopped = True
        self.abort_time = time.time()
        self._push_event("AbortRun", (False,))

    def _handle_event(self, event):
        """A job came back"""
        todo = 0
        log_job_trace(f"reveiced event {escape_logging(event)}")
        if event[0] == "JobSuccess":
            self._handle_job_success(*event[1])
            todo -= 1
        elif event[0] == "JobSkipped":
            # self._handle_job_skipped(*event[1])
            todo -= 1
        elif event[0] == "JobFailed":
            self.fail_counter += 1
            self._handle_job_failed(*event[1])
            todo -= 1
        elif event[0] == "JobUpstreamFailed":
            todo -= 1
        elif event[0] == "AbortRun":
            todo = 0
        else:  # pragma: no cover # defensive
            raise NotImplementedError(event[0])
        return todo

    # def _handle_job_skipped(self, job_id):
    # self.job_states[job_id].skipped()

    def _handle_job_success(self, job_id, job_outputs):
        """A job was done correctly. Record it's outputs,
        decide on downstreams"""
        job = self.jobs[job_id]
        job_state = self.job_states[job_id]
        msg = f"Done in {job_state.run_time:.2f}s {job_id}"
        if job.run_time >= self.job_graph.report_done_filter:
            if job.job_kind in (
                JobKind.Temp,
                JobKind.Output,
                JobKind.JobGenerating,
                JobKind.Loading,
            ):
                log_info(msg)
            else:
                log_debug(msg)
                pass
        else:
            # this appears to be a dramatic slowdown. (factor 2!
            # log_debug(f"Done in {job_state.run_time:.2}s {job_id}")
            log_job_trace(f"{job_id} success")
            pass
        # record our success
        # or failure if thue job did not do what it said on the tin.
        # log_trace(f"\t{escape_logging(str(job_outputs)[:500])}...")
        if set(job_outputs.keys()) != set(job.outputs):
            log_trace(
                f"\t{job_id} returned the wrong set of outputs. "
                f"Should be {escape_logging(str(set(job.outputs)))}, was {escape_logging(str(set(job_outputs.keys())))}"
            )
            job_state.failed(
                exceptions.JobContractError(
                    f"\t{job_id} returned the wrong set of outputs. "
                    f"Should be {escape_logging(str(set(job.outputs)))}, was {escape_logging(str(set(job_outputs.keys())))}"
                )
            )
            log_error(job_state.error)
        else:
            for name, hash in job_outputs.items():
                log_trace(f"\tCapturing hash for {name} {escape_logging(hash)}")
                self.output_hashes[name] = hash
            job_state.succeeded(job_outputs)

    def _handle_job_failed(self, job_id, error):
        """A job did not succeed (wrong output, no output, exception...0, - log the error, fail all downstreams"""
        log_job_trace(f"{job_id} failed")
        job = self.jobs[job_id]
        job_state = self.job_states[job_id]
        job_state.failed(error)

        # log_error(f"Failed {job_id}")
        if self.print_failures:
            log = log_error
        else:
            log = log_job_trace
        if not self._job_failed_last_time(job_id):
            try:
                # mock failure in case of abort/stop
                if isinstance(job_state.error.args[0], exceptions.JobCanceled):
                    if self.aborted or self.stopped:
                        return
                    else:
                        raise NotImplementedError(
                            "JobCanceled outside of stopped/aborted state?!"
                        )
                # log error to file. Todo: move to job_state
                if hasattr(job_state.error.args[1], "stacks"):
                    stacks = job_state.error.args[1]
                else:
                    stacks = None
                if self.job_graph.error_dir is not None:
                    error_file = (
                        self.job_graph.error_dir
                        / self.job_graph.time_str
                        / (str(job.job_number) + "_exception.txt")
                    )
                    with open(error_file, "w") as ef:
                        ef.write(f"JobId: {job_id}\n")
                        ef.write(f"Class: {job.__class__.__name__}\n")
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
                            ef.write(str(job_state.error))
                            ef.write("no stack available")
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
                        f"Failed after {job_state.run_time:.2}s: {job_id}. Exception (incl. locals, stdout and stderr) logged to {error_file}"
                    )
                else:
                    log(f"Failed job: {job_id}")
                if stacks is not None:
                    log(escape_logging(stacks._format_rich_traceback_fallback(False)))
                else:
                    log(job_state.error)
                    log("no stack available")

            except Exception as e:
                log_error(
                    f"An exception ocurred reporting on a job failure for {job_id}: {e}. The original job failure has been swallowed."
                )
        else:
            raise ValueError("Did not expect this")

    def _job_failed_last_time(self, job_id) -> bool:
        """Did this job fail last time?"""
        res = (
            self.last_job_states
            and job_id in self.last_job_states
            and self.last_job_states[job_id].outcome == JobOutcome.Failed
        )
        log_trace(f"_job_failed_last_time: {job_id}: {res}")
        return res

    def _push_event(self, event, args, indent=0):
        """Push an event to be handled by the control thread"""
        with self.event_lock:
            log_trace("\t" * indent + f"->push {event} {args[0]}")
            self.events.put((event, args))

    def _start_job_executing_threads(self):
        """Fire up the default number of threads"""
        for ii in range(self.job_graph.cores):
            self._start_another_thread()

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

    def _executing_thread(self):
        """The inner function of the threads actually executing the jobs"""
        cwd = (
            os.getcwd()
        )  # so we can detect if the job cahnges the cwd (don't do that!)
        job_id = None
        try:
            while not self.stopped:
                job_id = self.jobs_to_run_que.get()
                self.jobs_in_flight.append(job_id)
                # log_job_trace(f"Executing thread, got {job_id}")
                if job_id is ExitNow:
                    break
                job = self.jobs[job_id]
                job.waiting = True
                job.actual_cores_needed = -1
                job_state = self.job_states[job_id]
                self._interactive_report()
                event = None
                try:
                    job.start_time = (
                        time.time()
                    )  # assign it just in case anything fails before acquiring the lock
                    job.stop_time = float("nan")
                    job.run_time = float("nan")

                    c = job.resources.to_number(self.core_lock.max_cores)
                    job.actual_cores_needed = c
                    log_trace(
                        f"{job_id} cores: {c}, max: {self.core_lock.max_cores}, jobs_in_flight: {len(self.jobs_in_flight)}, all_cores_in_flight: {self.jobs_all_cores_in_flight}, threads: {len(self.threads)}"
                    )
                    if c > 1:
                        # we could stall all SingleCores/RunsHere by having all_cores blocking all but one thread (which executes another all_core).
                        # if we detect that situation, we spawn another one.
                        self.jobs_all_cores_in_flight += 1
                        if (
                            self.jobs_all_cores_in_flight >= len(self.threads)
                            and len(self.threads)
                            <= self.job_graph.cores
                            * 5  # at one point, we either have to let threads die again, or live with
                            # the wasted time b y stalling.
                        ):
                            log_trace(
                                "All threads blocked by Multi core jobs - starting another one"
                            )
                            self._start_another_thread()

                    log_trace(f"wait for {job_id}")
                    if c == 0:
                        log_error(f"Cores was 0! {job.job_id} {job.resources}")
                    with self.core_lock.using(c):
                        if self.stopped or self.aborted:
                            # log_job_trace(f"aborted waiting {job_id} -> skip")
                            event = ("JobSkipped", (job_id,))  # for accounting
                            # self._push_event("JobFailed", (job_id, exceptions.JobError(exceptions.JobCanceled(), None)))
                            continue  # -> while not stopped -> break
                        job.start_time = time.time()  # the *actual* start time
                        job.waiting = False
                        self._interactive_report()
                        log_trace(f"Go {job_id}")
                        log_trace(f"\tExecuting {job_id}")

                        try:
                            outputs = job.run(self, job_state.historical_output)
                        finally:
                            # we still check the cwd, even if the job failed!
                            if os.getcwd() != cwd:
                                os.chdir(
                                    cwd
                                )  # restore and hope we can recover enough to actually print the exception, I suppose.
                                log_error(
                                    f"{job_id} changed current_working_directory. Since ppg2 is multithreaded, you must not do this in jobs that RunHere"
                                )
                                raise exceptions.JobContractError(
                                    f"{job_id} changed current_working_directory. Since ppg2 is multithreaded, you must not do this in jobs that RunHere"
                                )
                        # log_job_trace(f"pushing success {job_id}")
                        event = ("JobSuccess", (job_id, outputs))
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
                        e = exceptions.JobError(
                            e,
                            captured_tb,
                        )
                    event = ("JobFailed", (job_id, e))
                finally:
                    job.stop_time = time.time()
                    job.run_time = job.stop_time - job.start_time
                    self.job_states[job_id].run_time = job.run_time
                    log_trace(f"end {job_id}")
                    self.jobs_in_flight.remove(job_id)
                    if c > 1:
                        self.jobs_all_cores_in_flight -= 1
                    if event is not None:
                        self._push_event(*event)
                    # log_trace(f"Leaving thread for {job_id}")
        except (KeyboardInterrupt, SystemExit):  # happens on abort
            log_trace(f"Keyboard Interrupt received {time.time() - self.abort_time}")
            pass
        except Exception as e:
            log_error(
                f"Captured exception outside of loop - should not happen {type(e)} {str(e)}. Check error log"
            )
        # log_job_trace(f"left thread {len(self.threads)} {job_id}")


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
