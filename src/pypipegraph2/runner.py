from . import exceptions
import os
import queue
from loguru import logger
import time
import traceback
import networkx
from .util import escape_logging
from .enums import JobKind, ValidationState, JobState
from .exceptions import _RunAgain
from .parallel import CoreLock
import threading
from rich import get_console
import rich.traceback
from rich.console import Console
from rich import print as rprint


class JobStatus:
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
        self.run_time = -1

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
    pass


class Runner:
    def __init__(self, job_graph, history, event_timeout):
        from . import _with_changed_global_pipegraph

        logger.job_trace("Runner.__init__")
        self.event_timeout = event_timeout
        with _with_changed_global_pipegraph(JobCollector(job_graph.run_mode)):
            self.job_graph = job_graph
            self.jobs = job_graph.jobs.copy()
            self.job_inputs = job_graph.job_inputs.copy()
            self.outputs_to_job_ids = job_graph.outputs_to_job_ids.copy()
            self.core_lock = CoreLock(job_graph.cores)

            flat_before = networkx.readwrite.json_graph.node_link_data(
                job_graph.job_dag
            )
            self.dag = self.modify_dag(job_graph)
            flat_after = networkx.readwrite.json_graph.node_link_data(job_graph.job_dag)
            import json

            assert flat_before == flat_after
            logger.job_trace(
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
                raise exceptions.NotADag("modify_dag error")
            self.job_states = {}

            for job_id in self.jobs:
                s = JobStatus()
                s.historical_input, s.historical_output = history.get(
                    job_id, ({}, {})
                )  # todo: support renaming jobs.
                logger.trace(
                    f"Loaded history for {job_id} {len(s.historical_input)}, {len(s.historical_output)}"
                )
                self.job_states[job_id] = s
            self.event_lock = threading.Lock()
            self.jobs_to_run_que = queue.Queue()
            self.threads = self.start_job_executing_threads()

    def modify_dag(self, job_graph):
        from .jobs import _DownstreamNeedsMeChecker

        def _recurse_pruning(job_id, reason):
            pruned.add(job_id)
            if not hasattr(self.jobs[job_id], "prune_reason"):
                self.jobs[job_id].prune_reason = reason
            for downstream_job_id in dag.successors(job_id):
                _recurse_pruning(downstream_job_id, reason)

        dag = job_graph.job_dag.copy()
        pruned = set()
        for job_id in self.jobs:
            if self.jobs[job_id]._pruned:
                _recurse_pruning(job_id, job_id)
        for job_id in pruned:
            dag.remove_node(job_id)
        known_job_ids = list(networkx.algorithms.dag.topological_sort(dag))
        for job_id in reversed(known_job_ids):
            job = self.jobs[job_id]
            if job.job_kind in (JobKind.Temp, JobKind.Loading):
                for downstream_job_id in dag.successors(job_id):
                    # part one: add the 'does the downstream need me to calculate' check?
                    downstream_job = self.jobs[downstream_job_id]
                    if downstream_job.job_kind is not JobKind.Cleanup:
                        downstream_needs_me_checker = _DownstreamNeedsMeChecker(
                            downstream_job
                        )
                        dag.add_node(downstream_needs_me_checker.job_id)
                        self.jobs[
                            downstream_needs_me_checker.job_id
                        ] = downstream_needs_me_checker
                        # self.job_inputs[downstream_needs_me_checker.job_id] =  set() # empty is covered by default duct
                        self.job_inputs[job_id].add(downstream_needs_me_checker.job_id)
                        self.outputs_to_job_ids[
                            downstream_needs_me_checker.job_id
                        ] = downstream_needs_me_checker.outputs[0]

                        dag.add_edge(downstream_needs_me_checker.job_id, job_id)
                        # part two - clone downstreams inputs:
                        # with special attention to temp jobs
                        # to avoid crosslinking
                        for down_upstream_id in self.iter_job_non_temp_upstream_hull(
                            downstream_job_id, dag
                        ):
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
        return dag

        # now add an initial job, so we can cut off the evaluation properly

    def iter_job_non_temp_upstream_hull(self, job_id, dag):
        result = []
        for upstream_job_id in dag.predecessors(job_id):
            upstream_job = self.jobs[upstream_job_id]
            if upstream_job.job_kind in (JobKind.Temp, JobKind.Loading):
                result.extend(
                    self.iter_job_non_temp_upstream_hull(upstream_job_id, dag)
                )
            else:
                result.append(upstream_job_id)
        return result

    def run(self, run_id, last_job_states):
        from . import global_pipegraph

        job_count = len(global_pipegraph.jobs)  # track if new jobs are being created

        logger.job_trace("Runner.__run__")

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
                # and not self.jobs[job_id].is_temp_job() # what is wrong with starting with a temp job?
                and self.jobs[job_id].output_needed(self)
                and not self.job_failed_last_time(job_id)
            )

        def is_skipped(job_id):
            return (
                (
                    not self.job_inputs[job_id]
                    # and not self.jobs[job_id].is_temp_job()
                    and not self.jobs[job_id].output_needed(self)
                )
                # or self.job_states[job_id].state == JobState.Failed
                or self.job_failed_last_time(job_id)
            )

        logger.job_trace(f"job_ids_topological {job_ids_topological}")
        logger.job_trace(f"self.job_inputs {escape_logging(self.job_inputs)}")
        initial_job_ids = [x for x in job_ids_topological if is_initial(x)]
        # for job_id in job_ids_topological:
        # logger.info(f"{job_id} - inputs - {escape_logging(self.job_inputs[job_id])}")
        # logger.info(f"{job_id} - istemp - {self.jobs[job_id].is_temp_job()}")
        # logger.info(f"{job_id} - outputneeded - {self.jobs[job_id].output_needed(self)}")
        # if not initial_job_ids:
        # if self.jobs:
        # raise exceptions.RunFailedInternally("Could not identify inital jobs")
        # else:
        # return {}
        skipped_jobs = [x for x in job_ids_topological if is_skipped(x)]
        self.events = queue.Queue()
        for job_id in initial_job_ids:
            self.job_states[job_id].state = JobState.ReadyToRun
            self.push_event("JobReady", (job_id,))
        for job_id in skipped_jobs:
            self.push_event("JobSkipped", (job_id,))
            # if we have no dependencies, we add the cleanup directly after the job
            # but it's not getting added to skipped_jobs
            # and I don't want to write an output_needed for the cleanup jobs
            if hasattr(self.jobs[job_id], "cleanup_job_class"):
                for downstream_id in self.dag.neighbors(job_id):
                    self.push_event("JobSkipped", (downstream_id,))
        self.jobs_in_flight = 0
        for t in self.threads:
            t.start()
        todo = len(self.dag)
        logger.job_trace(f"jobs: {self.jobs.keys()}")
        logger.job_trace(f"skipped jobs: {skipped_jobs}")
        timeout_counter = 0
        try:
            while todo:
                try:
                    ev = self.events.get(timeout=self.event_timeout)
                except queue.Empty:
                    # long time, no event.
                    if not self.jobs_in_flight:
                        # ok, a coding error has lead to us not finishing
                        # the todo graph.
                        for job_id in self.job_states:
                            logger.info(f"{job_id}, {self.job_states[job_id].state}")
                        raise exceptions.RunFailedInternally
                    continue

                logger.job_trace(
                    f"<-handle {ev[0]} {escape_logging(ev[1][0])}, todo: {todo}"
                )
                todo += self.handle_event(ev)
                logger.job_trace(f"<-done - todo: {todo}")
            try:
                ev = self.events.get_nowait()
            except queue.Empty:
                pass
            else:
                logger.job_trace(f"<-handle {ev[0]} {escape_logging(ev[1][0])}")
                self.handle_event(ev)
        finally:

            for t in self.threads:
                self.jobs_to_run_que.put(ExitNow)
            logger.job_trace("Joining threads")
            for t in self.threads:
                t.join()

        if len(global_pipegraph.jobs) != job_count:
            logger.job_trace(
                f"created new jobs. _RunAgain issued {len(global_pipegraph.jobs)} != {job_count}"
            )
            for job_id in global_pipegraph.jobs:
                if job_id not in self.jobs:
                    logger.job_trace(f"new job {job_id}")
            raise _RunAgain(self.job_states)
        logger.job_trace("Left runner.run()")
        return self.job_states

    def handle_event(self, event):
        todo = 0
        if event[0] == "JobSuccess":
            self.handle_job_success(*event[1])
            todo -= 1
        elif event[0] == "JobSkipped":
            self.handle_job_skipped(*event[1])
            todo -= 1
        elif event[0] == "JobReady":
            self.handle_job_ready(*event[1])
        elif event[0] == "JobFailed":
            self.handle_job_failed(*event[1])
            todo -= 1
        elif event[0] == "JobUpstreamFailed":
            todo -= 1
        else:  # pragma: no cover # defensive
            raise NotImplementedError(event[0])
        return todo

    def handle_job_success(self, job_id, job_outputs):
        job = self.jobs[job_id]
        job_state = self.job_states[job_id]
        # record our success
        logger.job_trace(f"\t{escape_logging(str(job_outputs)[:500])}...")
        if set(job_outputs.keys()) != set(job.outputs):
            logger.job_trace(
                f"\t{job_id} returned the wrong set of outputs. "
                f"Should be {escape_logging(str(set(job.outputs)))}, was {escape_logging(str(set(job_outputs.keys())))}"
            )

            job_state.state = JobState.Failed
            self.fail_downstream_by_outputs(job.outputs, job_id)
            job_state.error = exceptions.JobContractError(
                f"\t{job_id} returned the wrong set of outputs. "
                f"Should be {escape_logging(str(set(job.outputs)))}, was {escape_logging(str(set(job_outputs.keys())))}"
            )
        else:
            for name, hash in job_outputs.items():
                logger.job_trace(f"\tCapturing hash for {name}")
                self.output_hashes[name] = hash
                job_state.updated_output[name] = hash
                # when the job is done, it's the time time to record the inputs
                # job_state.updated_input = {
                # name: self.output_hashes[name]
                # for name in self.get_job_inputs(job.job_id)
                # }
            job_state.state = JobState.Executed
            self.inform_downstreams_of_outputs(job_id, job_outputs)
        logger.job_trace(
            f"after handle_job_success {job_id}.state == {self.job_states[job_id]}"
        )

    def inform_downstreams_of_outputs(self, job_id, job_outputs):
        job = self.jobs[job_id]

        for downstream_id in self.dag.successors(job_id):
            logger.job_trace(f"\t\tDownstream {downstream_id}")
            downstream_state = self.job_states[downstream_id]
            downstream_job = self.jobs[downstream_id]
            for name, hash in job_outputs.items():
                if name in self.job_inputs[downstream_id]:
                    logger.job_trace(f"\t\t\tHad {name}")
                    old = downstream_state.historical_input.get(name, None)
                    new = hash
                    if not self.compare_history(old, new, job.__class__):
                        logger.job_trace("\t\t\tinput changed -> invalidate")
                        downstream_state.validation_state = ValidationState.Invalidated
                    downstream_state.updated_input[name] = hash  # update any way.
                else:
                    logger.job_trace(f"\t\t\tNot an input {name}")
            if self.all_inputs_finished(downstream_id):
                logger.job_trace(f"\t\tAll inputs finished {downstream_id}")
                if (
                    downstream_job.job_kind is JobKind.Temp
                    and downstream_state.validation_state is ValidationState.Invalidated
                ):
                    logger.job_trace(f"{downstream_id} was Temp")
                    if self.job_has_non_temp_somewhere_downstream(downstream_id):
                        if self.job_failed_last_time(downstream_id):
                            self.push_event("JobFailed", (downstream_id, downstream_id))
                            self.job_states[downstream_id].error = self.last_job_states[
                                downstream_id
                            ].error
                        else:
                            self.push_event("JobReady", (downstream_id,), 3)
                    else:
                        self.push_event("JobSkipped", (downstream_id,), 3)
                elif (
                    downstream_state.validation_state is ValidationState.Invalidated
                    or downstream_job.output_needed(self)
                ):
                    if self.job_failed_last_time(downstream_id):
                        self.push_event("JobFailed", (downstream_id, downstream_id))
                        self.job_states[downstream_id].error = self.last_job_states[
                            downstream_id
                        ].error
                    else:
                        self.push_event("JobReady", (downstream_id,), 3)
                    # todo: do I need to do this for loading jobs that are lacking downstream? check me, there is a test case I broke for this...
                else:
                    if len(downstream_state.updated_input) < len(
                        downstream_state.historical_input
                    ):
                        logger.job_trace(
                            f"\t\t\thistorical_input {downstream_state.historical_input.keys()}"
                        )
                        logger.job_trace("\t\t\tinput disappeared -> invalidate")
                        downstream_state.validation_state = ValidationState.Invalidated
                        if self.job_failed_last_time(downstream_id):
                            self.push_event("JobFailed", (downstream_id, downstream_id))
                            self.job_states[downstream_id].error = self.last_job_states[
                                downstream_id
                            ].error
                        else:
                            self.push_event("JobReady", (downstream_id,), 3)
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
                                if self.job_failed_last_time(downstream_id):
                                    self.push_event(
                                        "JobFailed", (downstream_id, downstream_id)
                                    )
                                    self.job_states[
                                        downstream_id
                                    ].error = self.last_job_states[downstream_id].error
                                else:
                                    self.push_event("JobReady", (downstream_id,), 3)
                            else:
                                downstream_state.validation_state = (
                                    ValidationState.Validated
                                )
                                self.push_event("JobSkipped", (downstream_id,), 3)
                        else:
                            downstream_state.validation_state = (
                                ValidationState.Validated
                            )
                            self.push_event("JobSkipped", (downstream_id,), 3)

    def job_failed_last_time(self, job_id):
        res = (
            self.last_job_states
            and job_id in self.last_job_states
            and self.last_job_states[job_id].state == JobState.Failed
        )
        logger.job_trace(f"job_failed_last_time: {job_id}: {res}")
        return res

    def handle_job_skipped(self, job_id):
        job_state = self.job_states[job_id]
        job_state.state = JobState.Skipped
        if job_state.historical_output:
            job_state.updated_output = job_state.historical_output.copy()
        else:
            # yelp, we skipped this job (because it's output existed?
            # but we do not have a historical state.
            # that means the downstream jobs will never have all_inputs_finished
            # and we hang.
            # so...
            job_state.updated_output = job_state.historical_output.copy()

        # the input has already been filled.
        self.inform_downstreams_of_outputs(
            job_id, job_state.updated_output
        )  # todo: leave off for optimization - should not trigger anyway.

    def handle_job_ready(self, job_id):
        self.jobs_to_run_que.put(job_id)

    def handle_job_failed(self, job_id, source):
        job = self.jobs[job_id]
        job_state = self.job_states[job_id]
        job_state.state = JobState.Failed
        self.fail_downstream_by_outputs(job.outputs, job_id)
        logger.error(f"failed {job_id}")
        if self.job_graph.error_dir is not None:
            error_file = self.job_graph.error_dir / (
                str(job.job_number) + "_exception.txt"
            )
            with open(error_file, "w") as ef:
                c = Console(file=ef, width=80, record=True)
                c.print(f"{job_id}\n")
                c.log(job_state.error.args[1])
            logger.error(
                f"Failed job: {job_id}. Exception logged to {error_file}/.html"
            )
        else:
            logger.error(f"Failed job: {job_id}")
        logger.info("out")
        get_console().log(job_state.error.args[1])
        logger.info("out done")

    def all_inputs_finished(self, job_id):
        job_state = self.job_states[job_id]
        if job_state.state in (JobState.Failed, JobState.UpstreamFailed):
            logger.job_trace("\t\t\tall_inputs_finished = false because failed")
            return False
        logger.job_trace(f"\t\t\tjob_inputs: {escape_logging(self.job_inputs[job_id])}")
        logger.job_trace(
            f"\t\t\tupdated_input: {escape_logging(self.job_states[job_id].updated_input.keys())}"
        )
        all_finished = len(self.job_states[job_id].updated_input) == len(
            self.job_inputs[job_id]
        )
        logger.job_trace(f"\t\t\tall_input_finished: {all_finished}")
        return all_finished

    def push_event(self, event, args, indent=0):
        with self.event_lock:
            logger.opt(depth=1).log(
                "JobTrace", "\t" * indent + f"->push {event} {args[0]}"
            )
            self.events.put((event, args))

    def fail_downstream_by_outputs(self, outputs, source):
        for output in outputs:
            # can't I run this with the job_id? todo: optimization
            job_id = self.outputs_to_job_ids[
                output
            ]  # todo: don't continue if the state is already failed...
            self.fail_downstream(job_id, source)

    def fail_downstream(self, job_id, source):
        logger.job_trace(f"failed_downstream {job_id} {source}")
        job_state = self.job_states[job_id]
        if (
            job_state.state is not JobState.Failed
        ):  # we also call this on the failed job
            job_state.state = JobState.UpstreamFailed
            job_state.error = f"Upstream {source} failed"
        self.push_event("JobUpstreamFailed", (job_id,))
        for node in self.dag.successors(job_id):
            self.fail_downstream(node, source)

    def compare_history(self, old_hash, new_hash, job_class):
        if old_hash is None:
            return False
        return job_class.compare_hashes(old_hash, new_hash)

    def job_has_non_temp_somewhere_downstream(self, job_id):
        for downstream_id in self.dag.neighbors(job_id):
            j = self.jobs[downstream_id]
            if j.job_kind is not JobKind.Temp:
                return True
            else:
                if self.job_has_non_temp_somewhere_downstream(downstream_id):
                    return True
        return False

    def start_job_executing_threads(self):
        count = self.job_graph.cores
        result = []
        for ii in range(count):
            result.append(threading.Thread(target=self.executing_thread))
        return result

    def executing_thread(self):
        my_pid = os.getpid()
        cwd = os.getcwd()
        while True:
            job_id = self.jobs_to_run_que.get()
            self.jobs_in_flight += 1
            logger.job_trace(f"Executing thread, got {job_id}")
            if job_id is ExitNow:
                break
            job = self.jobs[job_id]
            c = job.resources.to_number(self.core_lock.max_cores)
            logger.job_trace(f"cores: {c}, max: {self.core_lock.max_cores}")
            with self.core_lock.using(c):
                job_state = self.job_states[job_id]
                try:
                    logger.job_trace(f"\tExecuting {job_id}")
                    job.start_time = time.time()
                    outputs = job.run(self, job_state.historical_output)
                    if os.getcwd() != cwd:
                        os.chdir(
                            cwd
                        )  # restore and hope we can recover enough to actually print the exception, I suppose.
                        logger.error(
                            f"{job_id} changed current_working_directory. Since ppg2 is multithreaded, you must not do this in jobs that RunHere"
                        )
                        raise exceptions.JobContractError(
                            f"{job_id} changed current_working_directory. Since ppg2 is multithreaded, you must not do this in jobs that RunHere"
                        )

                    job.stop_time = time.time()
                    job.run_time = job.stop_time - job.start_time
                    self.push_event("JobSuccess", (job_id, outputs))
                except SystemExit as e:  # pragma: no cover - happens in spawned process, and we don't get coverage logging for it thanks to os._exit
                    logger.job_trace(
                        "SystemExit in spawned process -> converting to hard exit"
                    )
                    if os.getpid() != my_pid:
                        os._exit(e.args[0])
                except Exception as e:
                    if isinstance(e, exceptions.JobError):
                        job_state.error = e
                    else:
                        job_state.error = exceptions.JobError(
                            e,
                            rich.traceback.Traceback(
                                show_locals=True,
                            ),
                        )
                    e = job_state.error
                    self.push_event("JobFailed", (job_id, job_id))
                finally:
                    self.jobs_in_flight -= 1


class JobCollector:
    def __init__(self, run_mode):
        self.clear()
        self.run_mode = run_mode

    def add(self, job):
        self.jobs[job] = job

    def add_edge(self, upstream_id, downstream_id):
        self.edges.add((upstream_id, downstream_id))

    def clear(self):
        self.jobs = {}
        self.edges = set()
