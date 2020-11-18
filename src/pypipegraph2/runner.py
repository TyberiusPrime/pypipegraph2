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


class InvalidationState(Enum):
    Unknown = auto()
    NotInvalidated = auto()
    Invalidated = auto()


class WillNeedToRun(Enum):
    Yes = auto()
    DontBother = auto()
    CantDecide = auto()
    CleanUp = auto()


class JobStatus:
    def __init__(self):
        self.state = JobState.Waiting
        self.invalidation_state = InvalidationState.Unknown
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
            logger.trace(f"Loaded history for {job_id} {len(s.historical_input)}, {len(s.historical_output)}")
            self.job_stati[job_id] = s

    def run(self):
        logger.log("JobTrace", "Runner.__run__")

        self.output_hashes = {}
        self.new_history = {}  # what are the job outputs this time.
        needs_rerun = True
        loop_safety = 10
        while needs_rerun:  # are there jobs we could'n get at the first time around?
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
                self.update_job_state(job, job_status)

                logger.log("JobTrace", f"Decided on {job_id} state {job_status.state}")

                if job_status.state is JobState.Skipped:
                    # There are two reasons to skip jobs: They had no input change
                    # (and thus, must by definition produce the same output)
                    # or they are not to run (TempFileGeneratingJobs without any currently building downstreams)
                    logger.log("JobTrace", f"{job_id}.historical_output.len {len(job_status.historical_output)}")
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
                        for name in self.get_job_inputs(job)
                    }
                    logger.info( f"Logging these inputs for {job_id} {escape_logging(job_status.updated_input)}")
                    logger.info( f"Logging these outputs for {job_id} {escape_logging(job_status.updated_output)}")
        return self.job_stati

    def update_job_state(self, job, job_status):
        logger.log("JobTrace", f"decide_on_job_status: {job.job_id}")
        if job_status.state.is_terminal():
            logger.log("JobTrace", "update_job_status, terminal")
            return
        if job_status.invalidation_state is InvalidationState.Invalidated:
            invalidated = True
        elif job_status.invalidation_state is InvalidationState.NotInvalidated:
            invalidated = False
        else:  # calculate invalidation
            invalidated = False
            for input in self.get_job_inputs(job):
                logger.log("JobTrace", f"\tLooking at {input}")
                if not input in self.output_hashes:
                    logger.log(
                        "JobTrace", f"Waiting because {input} was not in output hashes"
                    )
                    job_status.state = JobState.Waiting
                    return
                old = job_status.historical_input.get(input, None)
                new = self.output_hashes[input]
                comp_result = self.compare_history(old, new)
                if isinstance(comp_result, UnchangedButUpdate):
                    self.output_hashes[input] = new
                    logger.log("JobTrace", f"UnchangedButUpdate {input}")
                    continue
                elif comp_result is True:
                    logger.log("JobTrace", f"{input} no change")
                    continue
                else:
                    invalidated = True
                    # break todo?
                    logger.info(f"Invalidated {job.job_id} because of {input} change")
            if invalidated:  # because input changed
                job_status.invalidation_state = InvalidationState.Invalidated
            else:
                job_status.invalidation_state = InvalidationState.NotInvalidated
        run_now = job.are_you_ready_to_run(self, invalidated)
        logger.log("JobTrace", f"WillNeedToRun {job.job_id} {run_now}")
        if run_now is WillNeedToRun.Yes or run_now is WillNeedToRun.CleanUp:
            job_status.state = JobState.ReadyToRun
        elif run_now is WillNeedToRun.DontBother:
            job_status.state = JobState.Skipped
        elif run_now is WillNeedToRun.CantDecide:
            job_status.state = JobState.Waiting

    def get_job_inputs(self, job):
        return self.job_graph.job_inputs[job.job_id]
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


class UnchangedButUpdate:
    pass
