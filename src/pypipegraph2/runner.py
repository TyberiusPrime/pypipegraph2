from enum import Enum, auto
from . import exceptions
from loguru import logger
import time
import traceback
import networkx


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
        self.starttime = -1
        self.runtime = -1
        self.error = None


class Runner:
    def __init__(self, job_graph):
        logger.log('JobTrace', "Runner.__init__")
        self.job_graph = job_graph
        self.job_stati = {job_id: JobStatus() for job_id in job_graph.jobs}

    def run(self):
        logger.log('JobTrace', "Runner.__run__")
        self.historical = self.job_graph.historical # what were the job outputs the last time?
        self.new_history = {} # what are the job outputs this time.
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
                logger.log('JobTrace', f"Examining {job_id}")
                job = self.job_graph.jobs[job_id]
                # this is the heart... Deciding what to redo when.
                job_status = self.job_stati[job.job_id]
                job_state = self.decide_on_job_status(job)
                job_status.state = job_state
                logger.log('JobTrace', f"Decided on {job_id} state {job_status.state}")
                if job_state is JobState.Skipped:
                    for name in job.outputs:
                        if name in self.historical: # we also skip non-producitve TempFileGeneratingJobs
                            self.new_history[name] = self.historical[name]
                # elif job_state == JobState.Executed:
                # pass  # assigning the new history is being done when the job is set to Executed
                elif job_state is JobState.Waiting:
                    needs_rerun = True
                elif job_state is JobState.ReadyToRun:
                    logger.log('JobTrace', f"Running {job_id}")
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

                    for name, hash in outputs.items():
                        if name not in job.outputs:
                            job_status.error = exceptions.JobContractError(
                                f"{job_id} returned undeclared output {name}"
                            )
                            logger.warning(job_status.error)
                            self.fail_downstream(job.outputs, job_id)
                            job_status.status = JobStatus.Failed
                            break
                        logger.log('JobTrace', f"Capturing hash for {name}")
                        self.new_history[name] = hash
                    else:  # if not break
                        continue
        self.historical.update(self.new_history)
        return self.job_stati

    def decide_on_job_status(self, job):
        logger.log('JobTrace', f"decide_on_job_status: {job.job_id}")
        if self.job_stati[job.job_id].state.is_terminal():
            logger.log('JobTrace', "decide_on_job_status - exit by fiat")
            return self.job_stati[job.job_id].state
        invalidated = False
        for input in self.get_job_inputs(job):
            logger.log('JobTrace', f"Looking at {input}")
            if not input in self.new_history:
                logger.log('JobTrace', f"Waiting because {input} was not done")
                return JobState.Waiting
            old = self.historical.get(input, None)
            new = self.new_history[input]
            comp_result = self.compare_history(old, new)
            if isinstance(comp_result, UnchangedButUpdate):
                self.historical[input] = new
                logger.log('JobTrace', f"UnchangedButUpdate {input}")
                continue
            elif comp_result is True:
                logger.log('JobTrace', f"{input} no change")
                continue
            else:
                invalidated = True
                # break todo?
                logger.info(f"Invalidated {job.job_id} because of {input} change")
        logger.log('JobTrace', f"Finished looking at inputs. Invalidated {invalidated}")
        if invalidated:  # because input changed
            self.job_stati[
                job.job_id
            ].invalidation_state = InvalidationState.Invalidated
        else:
            self.job_stati[
                job.job_id

            ].invalidation_state = InvalidationState.NotInvalidated

        run_now = job.are_you_ready_to_run(self, invalidated)
        logger.log('JobTrace', f"WillNeedToRun {job.job_id} {run_now}")
        if run_now is WillNeedToRun.Yes or run_now is WillNeedToRun.CleanUp:
            return JobState.ReadyToRun
        elif run_now is WillNeedToRun.DontBother:
            return JobState.Skipped
        elif run_now is WillNeedToRun.CantDecide:
            return JobState.Waiting

    def get_job_inputs(self, job):
        return self.job_graph.job_inputs[job.job_id]
        # return networkx.algorithms.dag.ancestors(self.job_graph.job_dag, job.job_id)

    def compare_history(self, old_hash, new_hash):
        if old_hash == new_hash:
            return True
        logger.trace(f"Comparing {old_hash} and {new_hash}".replace("{", "{{").replace("}","}}"))
        return (
            False  # todo: this needs expanding...depending on what kind of hash it is.
        )

    def fail_downstream(self, outputs, source):
        logger.log('JobTrace', f"failed_downstream {outputs} {source}")
        for output in outputs:
            job_id = self.job_graph.outputs_to_job_ids[output]
            for node in networkx.algorithms.dag.descendants(
                self.job_graph.job_dag, job_id
            ):
                self.job_stati[node].state = JobState.UpstreamFailed
                self.job_stati[node].error = f"Upstream {source} failed"


class UnchangedButUpdate:
    pass
