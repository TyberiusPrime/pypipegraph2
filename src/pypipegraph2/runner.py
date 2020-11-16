from enum import Enum, auto
from . import exceptions
from loguru import logger
import time
import traceback
import networkx


class JobState:
    Waiting = 'Waiting'
    ReadyToRun = 'ReadyToRun'
    Executed = 'Executed'
    Failed = 'Failed'
    UpstreamFailed = 'UpstreamFailed'


class JobStatus:
    def __init__(self):
        self.state = JobState.Waiting
        self.starttime = -1
        self.runtime = -1
        self.error = None
        self.was_invalidated = False


class Runner:
    def __init__(self, job_graph):
        logger.trace("Runner.__init__")
        self.job_graph = job_graph
        self.job_stati = {job_id: JobStatus() for job_id in job_graph.jobs}

    def run(self):
        logger.trace("Runner.__run__")
        self.historical = self.job_graph.historical
        self.new_history = {}
        for job_id in networkx.algorithms.dag.topological_sort(self.job_graph.job_dag):
            logger.trace(f"Examining {job_id}")
            job = self.job_graph.jobs[job_id]
            # this is the heart... Deciding what to redo when.
            job_status = self.job_stati[job.job_id]
            job_state = self.decide_on_job_status(job)
            job_status.state = job_state
            logger.trace(f"Decided on state {job_status.state}")
            if job_state == JobState.Executed:
                if job_id in self.historical:
                    self.new_history[job_id] = self.historical[job_id]
            elif job_state == JobState.ReadyToRun:
                logger.trace(f"Running {job_id}")
                job_status.starttime = time.time()
                try:
                    outputs = job.run(job_status.was_invalidated)
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
                    logger.trace(f"Capturing hash for {name}")
                    self.new_history[name] = hash
                else:  # if not break
                    continue
        self.historical.update(self.new_history)
        return self.job_stati

    def decide_on_job_status(self, job):
        logger.trace(f"decide_on_job_status: {job.job_id}")
        if self.job_stati[job.job_id].state in (JobState.Executed, JobState.Failed):
            logger.trace("decide_on_job_status - exit by fiat")
            return self.job_stati[job.job_id].state
        invalidated = False
        for input in self.get_job_inputs(job):
            logger.trace(f"Looking at {input}")
            if not input in self.new_history:
                logger.trace(f"Waiting because {input} was not done")
                return JobState.Waiting
            old = self.historical.get(input, None)
            new = self.new_history[input]
            comp_result = self.compare_history(old, new)
            if isinstance(comp_result, UnchangedButUpdate):
                self.historical[input] = new
                logger.trace(f"UnchangedButUpdate {input}")
                continue
            elif comp_result is True:
                logger.trace(f"{input} no change")
                continue
            else:
                invalidated = True
                # break todo?
                logger.info(f"Invalidated {job.job_id} because of {input} change")
        logger.trace(f"Finished looking at inputs. Invalidated {invalidated}")
        if invalidated:  # because input changed
            self.job_stati[job.job_id].was_invalidated = True
            logger.trace(f"ReadyToRun {job.job_id} because invalidated")
            return JobState.ReadyToRun
        else:
            if job.must_make_outputs():
                logger.trace(f"ReadyToRun {job.job_id} because must_make_outputs")
                return JobState.ReadyToRun
            else:
                logger.trace(f"No input change for {job.job_id}, not must_make_outputs")
                return JobState.Executed

    def get_job_inputs(self, job):
        return networkx.algorithms.dag.ancestors(self.job_graph.job_dag, job.job_id)

    def compare_history(self, old_hash, new_hash):
        if old_hash == new_hash:
            return True
        logger.trace(f"Comparing {old_hash} and {new_hash}")
        return (
            False  # todo: this needs expanding...depending on what kind of hash it is.
        )

    def fail_downstream(self, job_ids, source):
        for job_id in job_ids:
            for node in networkx.algorithms.dag.descendants(self.job_graph.job_dag, job_id):
                self.job_stati[node].state = JobState.UpstreamFailed
                self.job_stati[node].error = f"Upstream {source} failed"



class UnchangedButUpdate :
    pass
