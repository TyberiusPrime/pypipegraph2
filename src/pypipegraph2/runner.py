from . import exceptions
import sys
import os
import queue
import time
import networkx
from .util import escape_logging
from .enums import JobKind, ValidationState, JobState, RunMode
from .exceptions import _RunAgain
from .parallel import CoreLock, async_raise
from threading import Thread
from . import ppg_traceback
import threading
from rich.console import Console
from .interactive import ConsoleInteractive
from .util import log_info, log_error, log_warning, log_debug, log_job_trace


class JobStatus:
    """Job run information collector"""

    def __init__(self):
        self._state = JobState.Waiting
        self.validation_state = ValidationState.Unknown
        self.input_done_counter = 0
        self.upstreams_completed = False
        self.run_non_invalidated = False
        self.historical_input = {}
        self.historical_output = {}
        self.updated_input = {}
        self.updated_output = {}

        self.start_time = -1
        self.run_time = -1.0

        self.error = None

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        if (
            self._state is JobState.Failed and value != JobState.Failed
        ):  # pragma: no cover
            raise ValueError("Can't undo a Failed tag")
        self._state = value

    def __str__(self):
        return repr(self)

    def __repr__(self):
        if self.state is JobState.UpstreamFailed:  # pragma: no cover
            return f"JobStatus({self.state} - {self.error})"
        return f"JobStatus({self.state})"


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
    ):
        from . import _with_changed_global_pipegraph

        log_job_trace("Runner.__init__")
        self.event_timeout = event_timeout
        with _with_changed_global_pipegraph(JobCollector(job_graph.run_mode)):
            self.job_graph = job_graph
            self.jobs = job_graph.jobs.copy()
            self.job_inputs = job_graph.job_inputs.copy()
            self.outputs_to_job_ids = job_graph.outputs_to_job_ids.copy()
            self.core_lock = CoreLock(job_graph.cores)

            # flat_before = networkx.readwrite.json_graph.node_link_data(
            # job_graph.job_dag
            # )
            self.dag = self.modify_dag(
                job_graph, focus_on_these_jobs, jobs_already_run_previously
            )
            # flat_after = networkx.readwrite.json_graph.node_link_data(job_graph.job_dag)
            # import json

            # assert flat_before == flat_after
            # log_job_trace(
            # "dag "
            # + escape_logging(
            # json.dumps(
            # networkx.readwrite.json_graph.node_link_data(self.dag), indent=2
            # )
            # ),
            # )

            if not networkx.algorithms.is_directed_acyclic_graph(
                self.dag
            ):  # pragma: no cover - defensive
                raise exceptions.NotADag("modify_dag error")
            self.job_states = {}

            for job_id in self.jobs:
                s = JobStatus()
                s.historical_input, s.historical_output = history.get(
                    job_id, ({}, {})
                )  # todo: support renaming jobs.
                log_job_trace(
                    f"Loaded history for {job_id} {len(s.historical_input)}, {len(s.historical_output)}"
                )
                self.job_states[job_id] = s
            self.event_lock = threading.Lock()
            self.jobs_to_run_que = queue.SimpleQueue()
            self.threads = []

    def modify_dag(  # noqa: C901
        self, job_graph, focus_on_these_jobs, jobs_already_run_previously
    ):
        """Modify the DAG to be executed
        by adding CleanupJobs, _DownstreamNeedsMeChecker,
        applying pruning,
        focusing on selected jobs (ie. prune everything outside of their DAG),
        or removing jobs we ran in the last ran-through
        """
        from .jobs import _DownstreamNeedsMeChecker

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

        dag = job_graph.job_dag.copy()
        if jobs_already_run_previously:
            new_jobs = set(dag.nodes).difference(jobs_already_run_previously)
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
            try:
                dag.remove_node(job_id)
            except networkx.exception.NetworkXError:  # happens with cleanup nodes that we  omitted
                pass

        known_job_ids = list(networkx.algorithms.dag.topological_sort(dag))
        # ti = time.time()
        counter = 0
        hulls = {}
        for job_id in reversed(known_job_ids):
            job = self.jobs[job_id]
            if job.job_kind in (JobKind.Temp, JobKind.Loading):
                for downstream_job_id in dag.successors(job_id):
                    # part one: add the 'does the downstream need me to calculate' check?
                    downstream_job = self.jobs[downstream_job_id]
                    if downstream_job.job_kind is not JobKind.Cleanup:
                        counter += 1
                        downstream_needs_me_checker = _DownstreamNeedsMeChecker(
                            downstream_job
                        )
                        dag.add_node(downstream_needs_me_checker.job_id)
                        self.jobs[
                            downstream_needs_me_checker.job_id
                        ] = downstream_needs_me_checker
                        # self.job_inputs[downstream_needs_me_checker.job_id] =  set() # empty is covered by default dict
                        self.job_inputs[job_id].add(downstream_needs_me_checker.job_id)
                        self.outputs_to_job_ids[
                            downstream_needs_me_checker.job_id
                        ] = downstream_needs_me_checker.outputs[0]

                        dag.add_edge(downstream_needs_me_checker.job_id, job_id)
                        # part two - clone downstreams inputs:
                        # with special attention to temp jobs
                        # to avoid crosslinking
                        # print(len(self._iter_job_non_temp_upstream_hull(
                        # downstream_job_id, dag
                        # )))
                        if downstream_job_id not in hulls:
                            # this caching speeds up 'wide' graphs
                            # e.g. those in benchmarks/bench_wide.py
                            # goes from 192s with 1000 jobs to 1.23
                            # still not great, but much better.
                            # still quadratic in nature though :(.
                            hulls[
                                downstream_job_id
                            ] = self._iter_job_non_temp_upstream_hull(
                                downstream_job_id, dag
                            )
                        for down_upstream_id in hulls[downstream_job_id]:
                            counter += 1
                            if down_upstream_id != job_id:
                                downstream_upstream_job = self.jobs[down_upstream_id]
                                dag.add_edge(down_upstream_id, job_id)
                                self.job_inputs[job_id].update(
                                    downstream_upstream_job.outputs
                                )
            if hasattr(job, "cleanup_job_class"):
                cleanup_job = job.cleanup_job_class(job)
                self.jobs[cleanup_job.job_id] = cleanup_job
                self.outputs_to_job_ids[cleanup_job.outputs[0]] = cleanup_job.job_id
                dag.add_node(cleanup_job.job_id)
                downstreams = list(dag.neighbors(job_id))
                if not downstreams:
                    downstreams = [
                        job_id
                    ]  # nobody below you? your cleanup will run right after you
                for downstream_job_id in downstreams:
                    dag.add_edge(downstream_job_id, cleanup_job.job_id)
                    self.job_inputs[cleanup_job.job_id].update(
                        self.jobs[downstream_job_id].outputs
                    )
        # raise ValueError(counter, time.time() -ti)
        return dag

        # now add an initial job, so we can cut off the evaluation properly

    def _iter_job_non_temp_upstream_hull(self, job_id, dag):
        result = set()
        for upstream_job_id in dag.predecessors(job_id):
            upstream_job = self.jobs[upstream_job_id]
            if upstream_job.job_kind in (JobKind.Temp, JobKind.Loading):
                result.update(
                    self._iter_job_non_temp_upstream_hull(upstream_job_id, dag)
                )
            else:
                result.add(upstream_job_id)
        return result

    def run(self, run_id, last_job_states):  # noqa:C901
        """Actually run the current DAG"""
        from . import global_pipegraph

        job_count = len(global_pipegraph.jobs)  # track if new jobs are being created

        log_job_trace("Runner.__run__")

        self.aborted = False
        self.stopped = False
        self.output_hashes = {}
        self.new_history = {}  # what are the job outputs this time.
        self.run_id = (
            run_id  # to allow jobgenerating jobs to run just once per graph.run()
        )
        self.last_job_states = last_job_states

        job_ids_topological = list(networkx.algorithms.dag.topological_sort(self.dag))

        def is_initial(job_id):
            return (
                not self.job_inputs[job_id]
                and self.jobs[job_id].output_needed(self)
                and not self._job_failed_last_time(job_id)
            )

        def is_skipped(job_id):
            return (
                (
                    not self.job_inputs[job_id]
                    and not self.jobs[job_id].output_needed(self)
                )
                # or self.job_states[job_id].state == JobState.Failed
                or self._job_failed_last_time(job_id)
            )

        log_job_trace(f"job_ids_topological {job_ids_topological}")
        log_job_trace(f"self.job_inputs {escape_logging(self.job_inputs)}")
        initial_job_ids = [x for x in job_ids_topological if is_initial(x)]
        # for job_id in job_ids_topological:
        # log_info(f"{job_id} - inputs - {escape_logging(self.job_inputs[job_id])}")
        # log_info(f"{job_id} - outputneeded - {self.jobs[job_id].output_needed(self)}")
        skipped_jobs = [x for x in job_ids_topological if is_skipped(x)]
        self.events = queue.Queue()

        for job_id in sorted(initial_job_ids):
            self.job_states[job_id].state = JobState.ReadyToRun
            self._push_event("JobReady", (job_id,))
        for job_id in skipped_jobs:
            self._push_event("JobSkipped", (job_id,))
            # if we have no dependencies, we add the cleanup directly after the job
            # but it's not getting added to skipped_jobs
            # and I don't want to write an output_needed for the cleanup jobs
            if hasattr(self.jobs[job_id], "cleanup_job_class"):
                for downstream_id in self.dag.neighbors(job_id):
                    self._push_event("JobSkipped", (downstream_id,))
        self.jobs_in_flight = []
        self.jobs_all_cores_in_flight = 0
        self._start_job_executing_threads()
        todo = len(self.dag)
        log_job_trace(f"jobs: {self.jobs.keys()}")
        log_job_trace(f"skipped jobs: {skipped_jobs}")
        self.jobs_done = 0
        self._interactive_start()
        try:
            self._interactive_report()
            while todo:
                try:
                    ev = self.events.get(timeout=self.event_timeout)
                    if ev[0] == "AbortRun":
                        log_job_trace("AbortRun run on external request")
                        todo = 0
                        break
                except queue.Empty:
                    # long time, no event.
                    if not self.jobs_in_flight:
                        # ok, a coding error has lead to us not finishing
                        # the todo graph.
                        for job_id in self.job_states:
                            log_warning(f"{job_id}, {self.job_states[job_id].state}")
                        raise exceptions.RunFailedInternally
                    continue

                log_job_trace(
                    f"<-handle {ev[0]} {escape_logging(ev[1][0])}, todo: {todo}"
                )
                d = self._handle_event(ev)
                todo += d
                self.jobs_done -= d
                self._interactive_report()
                log_job_trace(f"<-done - todo: {todo}")

            if not self.aborted:
                while self.jobs_in_flight:
                    try:
                        ev = self.events.get(0.1)
                    except queue.Empty:  # pragma: no cover
                        break
                    else:
                        log_job_trace(f"<-handle {ev[0]} {escape_logging(ev[1][0])}")
                        self._handle_event(ev)
                # once more for good measure...
                while True:
                    try:
                        ev = self.events.get_nowait()
                    except queue.Empty:
                        break
                    else:
                        log_job_trace(f"<-handle {ev[0]} {escape_logging(ev[1][0])}")
                        self._handle_event(ev)

            else:
                for t in self.threads:
                    log_job_trace(
                        f"Asking thread to terminate at next Python call {time.time() - self.abort_time}"
                    )
                    async_raise(t.ident, KeyboardInterrupt)
        finally:
            log_job_trace("Joining threads")

            for t in self.threads:
                self.jobs_to_run_que.put(ExitNow)
            for t in self.threads:
                t.join()
            if hasattr(self, "_status"):
                self._status.stop()
            self._interactive_stop()

        if len(global_pipegraph.jobs) != job_count and not self.aborted:
            log_job_trace(
                f"created new jobs. _RunAgain issued {len(global_pipegraph.jobs)} != {job_count}"
            )
            for job_id in global_pipegraph.jobs:
                if job_id not in self.jobs:
                    log_job_trace(f"new job {job_id}")
            raise _RunAgain(self.job_states)
        log_job_trace("Left runner.run()")
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
                self.interactive.report_status(self.jobs_done, 0, len(self.dag))
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
        todo = 0
        if event[0] == "JobSuccess":
            self._handle_job_success(*event[1])
            todo -= 1
        elif event[0] == "JobSkipped":
            self._handle_job_skipped(*event[1])
            todo -= 1
        elif event[0] == "JobReady":
            self._handle_job_ready(*event[1])
        elif event[0] == "JobFailed":
            self._handle_job_failed(*event[1])
            todo -= 1
        elif event[0] == "JobUpstreamFailed":
            todo -= 1
        else:  # pragma: no cover # defensive
            raise NotImplementedError(event[0])
        return todo

    def _handle_job_success(self, job_id, job_outputs):
        """A job was done correctly. Record it's outputs,
        decide on downstreams"""
        job = self.jobs[job_id]
        job_state = self.job_states[job_id]
        msg = f"Done in {job_state.run_time:.2}s [bold]{job_id}[/bold]"
        if job.run_time >= 1:
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
            pass
        # record our success
        # log_job_trace(f"\t{escape_logging(str(job_outputs)[:500])}...")
        if set(job_outputs.keys()) != set(job.outputs):
            log_job_trace(
                f"\t{job_id} returned the wrong set of outputs. "
                f"Should be {escape_logging(str(set(job.outputs)))}, was {escape_logging(str(set(job_outputs.keys())))}"
            )

            job_state.state = JobState.Failed
            self._fail_downstream_by_outputs(job.outputs, job_id)
            job_state.error = exceptions.JobContractError(
                f"\t{job_id} returned the wrong set of outputs. "
                f"Should be {escape_logging(str(set(job.outputs)))}, was {escape_logging(str(set(job_outputs.keys())))}"
            )
        else:
            for name, hash in job_outputs.items():
                log_job_trace(f"\tCapturing hash for {name}")
                self.output_hashes[name] = hash
                job_state.updated_output[name] = hash
                # when the job is done, it's the time time to record the inputs
                # job_state.updated_input = {
                # name: self.output_hashes[name]
                # for name in self.get_job_inputs(job.job_id)
                # }
            job_state.state = JobState.Executed
            self._inform_downstreams_of_outputs(job_id, job_outputs)
        log_job_trace(
            f"after _handle_job_success {job_id}.state == {self.job_states[job_id]}"
        )

    def _inform_downstreams_of_outputs(self, job_id, job_outputs):
        """Tell all the downstreams of their updated inputs
        and decide what to do with them once all inputs are available.
        """
        if self.stopped:
            return
        for downstream_id in self.dag.successors(job_id):
            # log_job_trace(f"\t\tDownstream {downstream_id}")
            downstream_state = self.job_states[downstream_id]
            downstream_job = self.jobs[downstream_id]
            for name, hash in job_outputs.items():
                if name in self.job_inputs[downstream_id]:
                    # log_job_trace(f"\t\t\tHad {name}")
                    downstream_state.updated_input[name] = hash  # update any way.
                # else:
                # log_job_trace(f"\t\t\tNot an input {name}")
            if self._all_inputs_finished(downstream_id):
                old_input = downstream_state.historical_input
                new_input = downstream_state.updated_input
                invalidated = False
                log_job_trace(
                    f"new input {escape_logging(new_input.keys())} old_input {escape_logging(old_input.keys())}"
                )
                if len(new_input) != len(
                    old_input
                ):  # we lost or gained an input -> invalidate
                    log_job_trace(
                        f"{downstream_id} No of inputs changed _> invalidated {len(new_input)}, {len(old_input)}"
                    )
                    invalidated = True
                else:  # same length.
                    if set(old_input.keys()) == set(
                        new_input.keys()
                    ):  # nothing possibly renamed
                        log_job_trace(f"{downstream_id} Same set of input keys")
                        for key, old_hash in old_input.items():
                            cmp_job = self.jobs[self.outputs_to_job_ids[key]].__class__
                            if not self._compare_history(
                                old_hash, new_input[key], cmp_job
                            ):
                                log_job_trace(
                                    f"{downstream_id} input {key} changed {escape_logging(old_hash)} {escape_logging(new_input[key])}"
                                )
                                invalidated = True
                                break
                    else:
                        log_job_trace(
                            f"{downstream_id} differing set of keys. Prev invalidated: {invalidated}"
                        )
                        for old_key, old_hash in old_input.items():
                            if old_key in new_input:
                                log_job_trace(
                                    f"key in both old/new {old_key} {escape_logging(old_hash)} {escape_logging(new_input[old_key])}"
                                )
                                cmp_job = self.jobs[
                                    self.outputs_to_job_ids[old_key]
                                ].__class__
                                if not self._compare_history(
                                    old_hash, new_input[old_key], cmp_job
                                ):
                                    log_job_trace(
                                        f"{downstream_id} input {old_key} changed"
                                    )
                                    invalidated = True
                                    break
                            else:
                                # we compare on identity here. Changing file names and hashing methods at once,
                                # what happens if you change the job class as well... better to stay on the easy side
                                count = _dict_values_count_hashed(new_input, old_hash)
                                if count:
                                    if count > 1:
                                        log_job_trace(
                                            f"{downstream_id} {old_key} mapped to multiple possible replacement hashes. Invalidating to be better safe than sorry"
                                        )
                                        invalidated = True
                                        break
                                    # else:
                                    # pass # we found a match
                                else:  # no match found
                                    log_job_trace(
                                        f"{downstream_id} {old_key} - no match found"
                                    )
                                    invalidated = True
                                    break
                        log_job_trace(f"{downstream_id} invalidated: {invalidated}")
                if invalidated:
                    downstream_state.validation_state = ValidationState.Invalidated

                log_job_trace(f"\t\tAll inputs finished {downstream_id}")
                if (
                    downstream_job.job_kind is JobKind.Temp
                    and downstream_state.validation_state is ValidationState.Invalidated
                ):
                    if self._job_has_non_temp_somewhere_downstream(downstream_id):
                        self._ready_or_failed(downstream_id)
                    else:
                        # I actually don't think we ever visit this case
                        # even a temp job without downstreams has a cleanup job associated, right?
                        raise NotImplementedError(
                            "Did not expect to go down this case"
                        )  # pragma: no cover
                        # self._push_event("JobSkipped", (downstream_id,), 3)
                elif (
                    downstream_state.validation_state is ValidationState.Invalidated
                    or downstream_job.output_needed(self)
                ):
                    self._ready_or_failed(downstream_id)
                else:
                    if downstream_job.job_kind is JobKind.Cleanup:
                        if (
                            self.job_states[
                                downstream_job.parent_job.job_id
                            ].validation_state
                            is ValidationState.Invalidated
                        ):
                            downstream_state.validation_state = (
                                ValidationState.Invalidated
                            )
                            self._ready_or_failed(downstream_id)
                        else:
                            downstream_state.validation_state = (
                                ValidationState.Validated
                            )
                            self._push_event("JobSkipped", (downstream_id,), 3)
                    else:
                        downstream_state.validation_state = ValidationState.Validated
                        self._push_event("JobSkipped", (downstream_id,), 3)

    def _ready_or_failed(self, job_id):
        """Mark this job as either failed (if failed last time), or ready to go.
        Even with the 'prune jobs from the last run' logic, we
        might see a job again if it's a failed upstream of a newly generated job
        """

        if self._job_failed_last_time(job_id):
            self._push_event("JobFailed", (job_id, job_id))
            self.job_states[job_id].error = self.last_job_states[job_id].error
        else:
            self._push_event("JobReady", (job_id,), 3)

    def _job_failed_last_time(self, job_id):
        """Did this job fail last time?"""
        res = (
            self.last_job_states
            and job_id in self.last_job_states
            and self.last_job_states[job_id].state == JobState.Failed
        )
        log_job_trace(f"_job_failed_last_time: {job_id}: {res}")
        return res

    def _handle_job_skipped(self, job_id):
        """This job was skipped (inputs unchanged, outputs recorded & present)"""
        job_state = self.job_states[job_id]
        job_state.state = JobState.Skipped
        if job_state.historical_output:
            job_state.updated_output = job_state.historical_output.copy()
        else:
            # yelp, we skipped this job (because it's output existed?
            # but we do not have a historical state.
            # that means the downstream jobs will never have _all_inputs_finished
            # and we hang.
            # so...
            job_state.updated_output = job_state.historical_output.copy()

        # the input has already been filled.
        self._inform_downstreams_of_outputs(
            job_id, job_state.updated_output
        )  # todo: leave off for optimization - should not trigger anyway.

    def _handle_job_ready(self, job_id):
        """The job is ready to run."""
        # suppose we could inline this
        log_job_trace(f"putting {job_id}")
        self.jobs_to_run_que.put(job_id)

    def _handle_job_failed(self, job_id, source):
        """A job did not succeed (wrong output, no output, exception...0, - log the error, fail all downstreams"""
        job = self.jobs[job_id]
        job_state = self.job_states[job_id]
        job_state.state = JobState.Failed
        self._fail_downstream_by_outputs(job.outputs, job_id)
        # log_error(f"Failed {job_id}")
        if not self._job_failed_last_time(job_id):
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
                    ef.write(f"{job_id}\n")
                    if stacks is not None:
                        ef.write(stacks._format_rich_traceback_fallback(True))

                    else:
                        ef.write(str(job_state.error))
                        ef.write("no stack available")
                log_error(
                    f"Failed after {job_state.run_time:.2}s: [bold]{job_id}[/bold]. Exception (incl. locals) logged to {error_file}"
                )
            else:
                log_error(f"Failed job: {job_id}")
            if stacks is not None:
                log_error(
                    escape_logging(stacks._format_rich_traceback_fallback(False))
                )
            else:
                log_error(job_state.error)
                log_error("no stack available")

    def _all_inputs_finished(self, job_id):
        """Are all inputs for this job finished?"""
        job_state = self.job_states[job_id]
        if job_state.state in (JobState.Failed, JobState.UpstreamFailed):
            # log_job_trace("\t\t\tall_inputs_finished = false because failed")
            return False
        # log_job_trace(f"\t\t\tjob_inputs: {escape_logging(self.job_inputs[job_id])}")
        # log_job_trace(
        # f"\t\t\tupdated_input: {escape_logging(self.job_states[job_id].updated_input.keys())}"
        # )
        all_finished = len(self.job_states[job_id].updated_input) == len(
            self.job_inputs[job_id]
        )
        # log_job_trace(f"\t\t\tall_input_finished: {all_finished}")
        return all_finished

    def _push_event(self, event, args, indent=0):
        """Push an event to be handled by the control thread"""
        with self.event_lock:
            log_job_trace(
                "\t" * indent + f"->push {event} {args[0]}"
            )
            self.events.put((event, args))

    def _fail_downstream_by_outputs(self, outputs, source):
        """Identify all downstream jobs via a job's outputs, and _fail_downstream them"""
        for output in outputs:
            # can't I run this with the job_id? todo: optimization
            job_id = self.outputs_to_job_ids[
                output
            ]  # todo: don't continue if the state is already failed...
            self._fail_downstream(job_id, source)

    def _fail_downstream(self, job_id, source):
        """Fail a node because it's upstream failed, recursively"""
        log_job_trace(f"failed_downstream {job_id} {source}")
        job_state = self.job_states[job_id]
        if (
            job_state.state is not JobState.Failed
        ):  # we also call this on the failed job
            job_state.state = JobState.UpstreamFailed
            job_state.error = f"Upstream {source} failed"
            self._push_event("JobUpstreamFailed", (job_id,))
        for node in self.dag.successors(job_id):
            self._fail_downstream(node, source)

    def _compare_history(self, old_hash, new_hash, job_class):
        """Compare the hashes of two jobs"""
        return job_class.compare_hashes(old_hash, new_hash)

    def _job_has_non_temp_somewhere_downstream(self, job_id):
        """Does this job have a non-temp job somewhere downstream.

        Used to decide on whether we execute temp jobs at all.
        """
        for downstream_id in self.dag.neighbors(job_id):
            j = self.jobs[downstream_id]
            if j.job_kind is not JobKind.Temp:
                return True
            else:
                if self._job_has_non_temp_somewhere_downstream(downstream_id):
                    return True
        return False  # pragma: no cover - apperantly we never call this with a job with no downstreams

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
        t = Thread(target=self._executing_thread)
        self.threads.append(t)
        t.start()

    def _executing_thread(self):
        """The inner function of the threads actually executing the jobs"""
        my_pid = (
            os.getpid()
        )  # so we can detect if we return inside a forked process and exit (safety net)
        cwd = (
            os.getcwd()
        )  # so we can detect if the job cahnges the cwd (don't do that!)
        try:
            while not self.stopped:
                job_id = self.jobs_to_run_que.get()
                self.jobs_in_flight.append(job_id)
                log_job_trace(f"Executing thread, got {job_id}")
                if job_id is ExitNow:
                    break
                job = self.jobs[job_id]
                c = job.resources.to_number(self.core_lock.max_cores)
                log_job_trace(
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
                        log_job_trace(
                            "All threads blocked by Multi core jobs - starting another one"
                        )
                        self._start_another_thread()

                log_job_trace(f"wait for {job_id}")
                with self.core_lock.using(c):
                    log_job_trace(f"Go {job_id}")
                    job_state = self.job_states[job_id]
                    try:
                        log_job_trace(f"\tExecuting {job_id}")
                        job.start_time = time.time()
                        outputs = job.run(self, job_state.historical_output)
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
                        self._push_event("JobSuccess", (job_id, outputs))
                    except SystemExit as e:  # pragma: no cover - happens in spawned process, and we don't get coverage logging for it thanks to os._exit
                        log_job_trace(
                            "SystemExit in spawned process -> converting to hard exit"
                        )
                        if os.getpid() != my_pid:
                            os._exit(e.args[0])
                    except Exception as e:
                        if isinstance(e, exceptions.JobError):
                            job_state.error = e
                        else:
                            exception_type, exception_value, tb = sys.exc_info()
                            captured_tb = ppg_traceback.Trace(
                                exception_type, exception_value, tb
                            )
                            job_state.error = exceptions.JobError(
                                e,
                                captured_tb,
                            )
                        e = job_state.error
                        self._push_event("JobFailed", (job_id, job_id))
                    finally:
                        job.stop_time = time.time()
                        job.run_time = job.stop_time - job.start_time
                        self.job_states[job_id].run_time = job.run_time
                        log_job_trace(f"end {job_id}")
                        self.jobs_in_flight.remove(job_id)
                        if c > 1:
                            self.jobs_all_cores_in_flight -= 1
        except (KeyboardInterrupt, SystemExit):
            log_job_trace(f"Keyboard Interrupt received {time.time() - self.abort_time}")
            pass


class JobCollector:
    """only in place during the dag modification step of Runner.__init__,
    so that the jobs that are only created during run (cleanup, _DownstreamNeedsMeChecker)
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


def _dict_values_count_hashed(a_dict, count_this):
    """Specialised 'how many times does this hash occur in this dict for renamed inputs"""
    counter = 0
    for value in a_dict.values():
        if value == count_this:
            counter += 1
        elif (
            isinstance(value, dict)
            and isinstance(count_this, dict)
            and "hash" in value
            and "hash" in count_this
            and "size" in value
            and "size" in count_this
            and value["hash"] == count_this["hash"]
        ):
            counter += 1
        "hash" in value and isinstance(count_this, dict) and "hash" in count_this
    return counter
