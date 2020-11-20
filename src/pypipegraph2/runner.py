from enum import Enum, auto
from . import exceptions
from loguru import logger
import time
import traceback
import networkx
from .util import escape_logging


class JobState(Enum):
    Waiting = auto()
    ReadyToRun = auto()
    Executed = auto()
    Skipped = auto()
    Failed = auto()
    UpstreamFailed = auto()

    def is_terminal(self):
        return self in (
            JobState.Executed,
            JobState.Failed,
            JobState.UpstreamFailed,
            JobState.Skipped,
        )

    def is_failed(self):
        return self in (
            JobState.Failed,
            JobState.UpstreamFailed,
        )


class InvalidationState(Enum):
    Unknown = auto()
    NotInvalidated = auto()
    Invalidated = auto()
    MissingTempForDecision = auto()
    MissingOtherForDecision = auto()
    UpstreamFailed = auto()

    def is_missing(self):
        return self in (
            InvalidationState.MissingOtherForDecision,
            InvalidationState.MissingTempForDecision,
        )


class JobStatus:
    def __init__(self):
        self.state = JobState.Waiting
        self.invalidation_state = InvalidationState.Unknown
        self.skipped_descendant_counter = 0
        self.historical_input = {}
        self.historical_output = {}
        self.updated_input = {}
        self.updated_output = {}

        self.starttime = -1
        self.runtime = -1

        self.error = None


class Runner:
    def __init__(self, job_graph):
        logger.log("JobTrace", "Runner.__init__")
        self.job_graph = job_graph
        self.job_stati = {}
        history = self.job_graph.load_historical()
        import pprint

        logger.info(f"History \n{escape_logging(pprint.pformat(history))}")
        for job_id in job_graph.jobs:
            s = JobStatus()
            s.historical_input, s.historical_output = history.get(
                job_id, ({}, {})
            )  # todo: support renaming jobs.
            logger.trace(
                f"Loaded history for {job_id} {len(s.historical_input)}, {len(s.historical_output)}"
            )
            self.job_stati[job_id] = s

    def run(self):
        logger.log("JobTrace", "Runner.__run__")

        self.output_hashes = {}
        self.new_history = {}  # what are the job outputs this time.
        needs_rerun = True
        loop_safety = 10
        while needs_rerun:  # are there jobs we could'n get at the first time around?
            logger.log("JobTrace", "Loop enter")
            still_waiting = [
                job_id
                for job_id in self.job_graph.jobs
                if self.job_stati[job_id].state is JobState.Waiting
            ]
            logger.log("JobTrace", f"Still Waiting: {still_waiting[:5]}")
            needs_rerun = False
            loop_safety -= 1
            if loop_safety == 0:
                raise ValueError("Too many loop reruns")

            for job_id in networkx.algorithms.dag.topological_sort(
                self.job_graph.job_dag
            ):
                logger.log("JobTrace", f"Examining {job_id}")
                # this is the heart... Deciding what to redo when.
                job = self.job_graph.jobs[job_id]
                job_status = self.job_stati[job.job_id]
                self.update_invalidation_status(job, job_status)
                self.update_job_state(job, job_status)

                logger.log("JobTrace", f"Decided on {job_id} state {job_status.state}")

                if job_status.state is JobState.Skipped:
                    # There are two reasons to skip jobs: They had no input change
                    # (and thus, must by definition produce the same output)
                    # or they are not to run (TempFileGeneratingJobs without any currently building downstreams)
                    logger.log(
                        "JobTrace",
                        f"{job_id}.historical_output.len {len(job_status.historical_output)}",
                    )
                    for (name, hash) in job_status.historical_output.items():
                        logger.log("JobTrace", f"Storing hash for {name}")
                        self.output_hashes[name] = hash
                    job_status.updated_input = job_status.historical_input.copy()
                    job_status.updated_output = job_status.historical_output.copy()
                elif job_status.state == JobState.Executed:
                    pass  # assigning the new history is being done when the job is set to Executed
                elif job_status.state is JobState.Waiting:
                    needs_rerun = True
                elif job_status.state is JobState.ReadyToRun:
                    logger.log("JobTrace", f"Running {job_id}")
                    job_status.starttime = time.time()
                    try:
                        outputs = job.run(
                            job_status.invalidation_state
                            is InvalidationState.Invalidated
                        )  # why does the job need to know this?
                        job_status.state = JobState.Executed
                        logger.log(
                            "JobTrace",
                            f"job executed. Outputs {escape_logging(str(outputs))}",
                        )
                    except Exception as e:
                        job_status.error = str(e) + "\n" + traceback.format_exc()
                        logger.warning(f"Job {job_id} failed: {e}")
                        job_status.state = JobState.Failed
                        self.fail_downstream(job.outputs, job_id)
                        continue

                    job_status.runtime = time.time() - job_status.starttime
                    logger.success(f"Runtime: {job_id}: {job_status.runtime:.2f}")
                    if len(job.outputs) > len(outputs):
                        logger.warning("JobContractError: {msg}")
                        job_status.error = exceptions.JobContractError(
                            f"{job_id} returned an unexpected number of outputs {len(outputs)}, expected {len(job.outputs)}"
                        )
                        logger.warning(job_status.error)
                        self.fail_downstream(job.outputs, job_id)
                        job_status.status = JobStatus.Failed
                        continue

                    job_status.updated_output = {}
                    for name, hash in outputs.items():
                        if name not in job.outputs:
                            job_status.error = exceptions.JobContractError(
                                f"{job_id} returned undeclared output {name}"
                            )
                            logger.warning(job_status.error)
                            self.fail_downstream(job.outputs, job_id)
                            job_status.status = JobStatus.Failed
                            break
                        logger.log("JobTrace", f"Capturing hash for {name}")
                        self.output_hashes[name] = hash
                        job_status.updated_output[name] = hash
                    job_status.updated_input = {
                        name: self.output_hashes[name]
                        for name in self.get_job_inputs(job.job_id)
                    }
                    logger.trace(
                        f"Logging these inputs for {job_id} {escape_logging(job_status.updated_input)}"
                    )
                    logger.trace(
                        f"Logging these outputs for {job_id} {escape_logging(job_status.updated_output)}"
                    )
        return self.job_stati

    def update_invalidation_status(self, job, job_status):
        # -> Unexamined, Yes, No, MissingTempForDecision, MissingOtherForDecision
        logger.log(
            "JobTrace",
            f"update_invalidation_status {job.job_id} before = {job_status.invalidation_state}",
        )
        if job_status.invalidation_state in (
            InvalidationState.Invalidated,
            InvalidationState.NotInvalidated,
            InvalidationState.UpstreamFailed,
        ):
            return
        new_status = InvalidationState.NotInvalidated
        if (
            not job_status.historical_output
        ):  # if you've never been run, you get invalidated and therefore run.
            logger.log("JobTrace", f"\t{job.job_id} was never run -> invalidated")
            new_status = InvalidationState.Invalidated
        else:
            for input in self.get_job_inputs(job.job_id):
                input_job = self.job_graph.outputs_to_job_ids[input]
                logger.log(
                    "JobTrace",
                    f"\tlooking at {input_job} {self.job_stati[input_job].state}, {self.job_stati[input_job].invalidation_state}",
                )
                if self.job_stati[
                    input_job
                ].state.is_terminal():  # ie. it's done, one way or another
                    if self.job_stati[input_job].state.is_failed():
                        new_status = InvalidationState.UpstreamFailed
                        break
                    else:
                        self.job_stati[input_job].invalidation_state.is_missing()
                        if not self.compare_history(
                            job_status.historical_input.get(input, None),
                            self.output_hashes[input],
                        ):
                            new_status = InvalidationState.Invalidated
                            break
                else:  # it's not done.
                    # but might have been invalidated already?
                    if self.job_graph.jobs[input_job].is_temp_job():
                        new_status = InvalidationState.MissingTempForDecision
                    else:
                        new_status = InvalidationState.MissingOtherForDecision
        logger.log(
            "JobTrace", f"update_invalidation_status {job.job_id} assigns {new_status}"
        )
        job_status.invalidation_state = new_status

    def update_job_state(self, job, job_status):
        # jobs run
        # - if their input changed
        # -      (and their output is needed, but that's an optimization to not run leave tempfiles - ignore for now)
        # - or if they have temp output
        # -      and their output is needed

        # so, invalidation has just been updated
        # invalidated means input changed.
        logger.log(
            "JobTrace",
            f"decide_on_job_status: {job.job_id}, Was: {job_status.state} {job_status.invalidation_state}",
        )
        if (
            job_status.state.is_terminal() or job_status.state is JobState.ReadyToRun
        ):  # this job is cooked/about to be cooked.
            logger.log("JobTrace", f"{job.job_id} was terminal")
            return
        elif job_status.invalidation_state is InvalidationState.Invalidated:
            logger.log("JobTrace", f"{job.job_id} was Invalidated")
            if job.is_temp_job() and self.has_no_downstreams(job):
                logger.log(
                    "JobTrace",
                    f"\twas TempFileGeneratingJob without downstream ->Skipped",
                )
                job_status.state = JobState.Skipped
            else:
                if self.are_job_inputs_done(job.job_id):
                    logger.log("JobTrace", f"\t-> ReadyToRun")
                    job_status.state = JobState.ReadyToRun
                else:  # we are missing at least one temp job
                    self.ready_tempjobs_that_were_waiting(job.job_id)
        elif job_status.invalidation_state is InvalidationState.NotInvalidated:
            logger.log("JobTrace", f"{job.job_id} was NotInvalidated")
            if not job.is_temp_job():
                logger.log("JobTrace", "\t Not at temp job")
                if job.output_needed():
                    logger.log("JobTrace", "\t\t Output needed -> ReadyToRun")
                    job_status.state = JobState.ReadyToRun
                else:
                    logger.log("JobTrace", "\t\t Output not needed -> Skipped")
                    job_status.state = JobState.Skipped
            else:
                logger.log("JobTrace", "\t temp job -> Waiting")
                job_status.state = JobState.Waiting
        elif job_status.invalidation_state is InvalidationState.MissingOtherForDecision:
            logger.log(
                "JobTrace", f"{job.job_id} was MissingOtherForDecision -> Waiting"
            )
            job_status.state = JobState.Waiting
        elif job_status.invalidation_state is InvalidationState.MissingTempForDecision:
            logger.log("JobTrace", f"{job.job_id} was MissingTempForDecision")
            # at this point we know the job was not invalidated by a non temp job.
            # the temp job also either wasn't invalidate, or ran, and did not invalidate this one.
            if job.output_needed():
                logger.log("JobTrace", f"\t Output needed. Readying temp jobs")
                self.ready_tempjobs_that_were_waiting(self, job.job_id)
                logger.log(
                    "JobTrace", "\t -> waiting since the TempJobs must first run"
                )
                job_status.state = JobState.Waiting
            else:
                if job.is_temp_job():
                    logger.log("JobTrace", f"\t Output not needed, but temp job -> Wait")
                else: # but what if the temp job that still needs to run invalidates this one???
                    for input in self.get_job_inputs(job.job_id):
                        input_job = self.job_graph.outputs_to_job_ids[input]
                        if self.job_graph.jobs[input_job].is_temp_job() and not self.job_stati[input_job].state.is_terminal():
                            logger.log("JobTrace", f"\t\t Found an input job that was not terminal")
                            break
                    else:
                        logger.log("JobTrace", f"\t Output not needed -> Skip")
                        self.temp_job_skip_one(job.job_id)
                        job_status.state = JobState.Skipped

        logger.log("JobTrace", "leaving")

    def ready_tempjobs_that_were_waiting(self, job_id):
        logger.log("JobTrace", f"\t\t {job_id} ready_tempjobs_that_were_waiting")
        for upstream_id in self.job_graph.job_dag.predecessors(job_id):
            upstream_job = self.job_graph.jobs[upstream_id]
            if (
                upstream_job.is_temp_job()
                and self.job_stati[upstream_id].state is JobState.Waiting
            ):
                if self.are_job_inputs_done(upstream_id):
                    logger.log("JobTrace", f"\t\t {upstream_id} Inputs done -> ReadyToRun")
                    self.job_stati[upstream_id].state = JobState.ReadyToRun
                else:
                    logger.log(
                        "JobTrace", f"\t\t {upstream_id} Inputs not done -> Wait again"
                    )
                    # self.job_stati[job_id].state = JobState.Waiting

                self.ready_tempjobs_that_were_waiting(upstream_id)

    def temp_job_skip_one(self, job_id):
        for upstream_id in self.job_graph.job_dag.predecessors(job_id):
            upstream_job = self.job_graph.jobs[upstream_id]
            if upstream_job.is_temp_job():
                if not self.job_stati[
                    upstream_id
                ].state.is_terminal():  # might have run already, right?
                    logger.log(
                        "JobTrace",
                        f"\t\t{upstream_id}.skipped_descendant_counter = {self.job_stati[upstream_id].skipped_descendant_counter} + 1",
                    )
                    self.job_stati[upstream_id].skipped_descendant_counter += 1
                    if self.job_stati[upstream_id].skipped_descendant_counter == len(
                        list(self.job_graph.job_dag.neighbors(upstream_id))
                    ):
                        logger.log(
                            "JobTrace",
                            f"\t\t{upstream_id} all descendants skipped -> Skip",
                        )
                        self.job_stati[upstream_id].state = JobState.Skipped
                        self.temp_job_skip_one(upstream_id)

    def are_job_inputs_done(self, job_id):
        for input in self.get_job_inputs(job_id):
            if not input in self.output_hashes:
                return False
        return True

    def get_job_inputs(self, job_id):
        return self.job_graph.job_inputs[job_id]
        # return networkx.algorithms.dag.ancestors(self.job_graph.job_dag, job.job_id)

    def compare_history(self, old_hash, new_hash):
        if old_hash == new_hash:
            return True
        logger.trace(
            f"Comparing {old_hash} and {new_hash}".replace("{", "{{").replace("}", "}}")
        )
        return (
            False  # todo: this needs expanding...depending on what kind of hash it is.
        )

    def fail_downstream(self, outputs, source):
        logger.log("JobTrace", f"failed_downstream {outputs} {source}")
        for output in outputs:
            job_id = self.job_graph.outputs_to_job_ids[output]
            for node in networkx.algorithms.dag.descendants(
                self.job_graph.job_dag, job_id
            ):
                self.job_stati[node].state = JobState.UpstreamFailed
                self.job_stati[node].error = f"Upstream {source} failed"

    def has_no_downstreams(self, job):
        for node in networkx.algorithms.dag.descendants(
            self.job_graph.job_dag, job.job_id
        ):
            return False
        return True


class UnchangedButUpdate:
    pass
