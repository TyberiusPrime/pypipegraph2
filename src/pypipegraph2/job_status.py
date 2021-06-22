from .enums import JobState, ValidationState, ShouldRun, JobKind
from . import exceptions
import time

from .util import (
    log_error,
    log_info,
    log_job_trace,
    log_trace,
    log_warning,
    escape_logging,
)


class JobStatus:
    """Job run information collector"""

    def __init__(self, job_id, runner, historical_input, historical_output):
        self.job_id = job_id
        self.runner = runner
        self._state = JobState.Waiting
        self._validation_state = ValidationState.Unknown
        # if not historical_input and not self.runner.job_inputs[job_id]:# todo: do I need this?!
        # log_job_trace(
        # f"{self.job_id} was a no input job without history -> validated"
        # )
        # self._validation_state = ValidationState.Validated
        self.should_run = ShouldRun.Maybe
        self.input_done_counter = 0
        self.upstreams_completed = False
        self.run_non_invalidated = False
        self.historical_input = historical_input
        self.historical_output = historical_output
        self.updated_input = {}
        self.updated_output = {}

        self.start_time = -1
        self.run_time = -1.0

        self.error = None

    def __del__(self):
        self.runner = None  #  break the link

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        log_job_trace(f"{self.job_id} set state. Was {self._state}, becomes {value}")
        if self._state.is_terminal():  # pragma: no cover
            log_job_trace(f"{self.job_id} Can't undo or set again a terminal state")
            raise ValueError("Can't undo or set again a terminal state")
        self._state = value
        # if value.is_terminal(): # this is always true
        self.job_became_terminal()
        if value in (JobState.ReadyToRun, JobState.Skipped):
            self.job_decided_wether_to_run()

    def __str__(self):
        return repr(self)

    def __repr__(self):
        if self.state is JobState.UpstreamFailed:  # pragma: no cover
            return f"JobStatus({self.state} - {self.error})"
        return f"JobStatus({self.state})"

    @property
    def validation_state(self):
        return self._validation_state

    @validation_state.setter
    def validation_state(self, value):
        log_job_trace(
            f"{self.job_id} set validation_state. Was {self._validation_state}, becomes {value}"
        )
        if self._validation_state != value:
            if value == ValidationState.Invalidated and self.state == JobState.Waiting:
                pass
            elif self._validation_state != ValidationState.Unknown:
                raise ValueError(  # pragma: no cover - defensive
                    f"{self.job_id} Can't go from {self._validation_state} to {value} {self.state}"
                )
            self._validation_state = value
            # self.update_should_run()

    @property
    def job(self):
        return self.runner.jobs[self.job_id]

    def all_upstreams_terminal(self):
        # todo: combine with ready_to_run?
        for upstream_id in self.upstreams():
            s = self.runner.job_states[upstream_id].state
            if not s.is_terminal():
                log_job_trace(f"{self.job_id} all_upstreams_terminal->False")
                return False
        log_job_trace(f"{self.job_id} all_upstreams_terminal->True")
        return True

    def all_upstreams_terminal_or_conditional(self):
        for upstream_id in self.upstreams():
            s = self.runner.job_states[upstream_id]
            if not s.state.is_terminal():
                if self.runner.jobs[upstream_id].is_conditional():
                    if (
                        s.should_run == ShouldRun.Yes
                        or s.validation_state == ValidationState.Invalidated
                    ):
                        # this should be run first, but hasn't
                        log_job_trace(
                            f"{self.job_id} all_upstreams_terminal_or_conditional -->False, {upstream_id} was conditional, but shouldrun, and not yes"
                        )
                        return False
                    else:
                        if self.runner.job_states[
                            upstream_id
                        ].all_upstreams_terminal_or_conditional():
                            # import history from that one.
                            for name in self.runner.jobs[upstream_id].outputs:
                                if name in self.runner.job_inputs[self.job_id]:
                                    log_trace(
                                        f"\t\t\tHad {name} - non-running conditional job - using historical input"
                                    )
                                    if name in self.historical_input:
                                        self.updated_input[
                                            name
                                        ] = self.historical_input[name]
                                    # else: do nothing. We'll come back as invalidated, since we're missing an input
                                    # and then the upstream job will be run, and we'll be back here,
                                    # and it will be  in a terminal state.
                        else:
                            return False  # I can't tell yet!

                else:
                    log_job_trace(
                        f"{self.job_id} all_upstreams_terminal_or_conditional -->False, {upstream_id} was not terminal"
                    )
                    return False
        log_job_trace(f"{self.job_id} all_upstreams_terminal_or_conditional->True")
        return True

    def downstreams(self):
        yield from self.runner.dag.successors(self.job_id)

    def upstreams(self):
        yield from self.runner.dag.predecessors(self.job_id)

    def update_should_run(self):
        log_job_trace(f"{self.job_id} update_should_run")
        if self.should_run in (ShouldRun.Yes, ShouldRun.No):  # it was decided.
            log_job_trace(f"\t short circuit {self.should_run}")
            result = self.should_run
        else:
            self.update_invalidation()
            if self.validation_state == ValidationState.Invalidated:
                log_job_trace(f"\t update_should_run-> yes case invalidated")
                result = ShouldRun.Yes
            else:
                if not self.job.is_conditional():

                    if self.job.output_needed(self.runner):
                        log_job_trace(f"\t update_should_run-> yes case output_needed")
                        result = ShouldRun.Yes
                    else:
                        if self.validation_state == ValidationState.Validated:
                            log_job_trace(
                                f"\t update_should_run-> no output_needed needed, validated"
                            )
                            result = ShouldRun.No
                        else:
                            if self.job.job_kind == JobKind.Cleanup:
                                log_job_trace(f"\t -> cleanup_job")
                                parent_id = self.job.parent_job.job_id
                                parent_state = self.runner.job_states[parent_id].state
                                if parent_state == JobState.Skipped:
                                    log_job_trace(
                                        f"\t -> skip (was cleanup, parent skipped)"
                                    )
                                    result = ShouldRun.No
                                elif parent_state == JobState.Success:
                                    log_job_trace(
                                        f"\t -> run (was cleanup, parent success)"
                                    )
                                    result = ShouldRun.Yes
                                elif (
                                    parent_state == JobState.Failed
                                ):  # pragma: no cover
                                    raise ValueError(  # pragma: no cover
                                        "Why are we asking a job with failed upstream if it should run? (1)"
                                    )
                                    # todo: this is a judgment call. Is it the right one?
                                    # log_job_trace(
                                    #    f"\t -> run (was cleanup, parent failed)"
                                    # )
                                    # result = ShouldRun.Yes
                                elif (
                                    parent_state == JobState.UpstreamFailed
                                ):  # pragma: no cover
                                    raise ValueError(  # pragma: no cover
                                        "Why are we asking a job with failed upstream if it should run? (1)"
                                    )
                                    # result = ShouldRun.No
                                elif parent_state == JobState.Waiting:
                                    result = ShouldRun.Maybe
                                else:  # pragma: no cover
                                    raise ValueError(
                                        f"Should not happen {parent_state}"
                                    )

                            else:
                                if self.validation_state == ValidationState.Unknown:
                                    result = ShouldRun.Maybe
                                else:  # pragma: no cover
                                    raise ValueError(
                                        f"I did not expect this case {self.validation_state}"
                                    )

                else:  # a conditional job...
                    log_job_trace(
                        "\t was conditional"
                    )  # todo: maybe not recurse back to the job we came from?
                    ds_count = 0
                    ds_no_count = 0
                    for downstream_id in self.downstreams():
                        if (
                            self.runner.jobs[downstream_id].job_kind == JobKind.Cleanup
                        ):  # those don't count
                            continue
                        log_job_trace(f"\t downstream: {downstream_id}")
                        ds_count += 1
                        self.runner.job_states[downstream_id].update_should_run()
                        ds_should_run = self.runner.job_states[downstream_id].should_run
                        if ds_should_run == ShouldRun.Yes:
                            log_job_trace(
                                f"{self.job_id} update_should_run-> yes case Downstream needs me: {downstream_id}"
                            )
                            result = ShouldRun.Yes
                            break
                        elif ds_should_run == ShouldRun.No:
                            # if they are all no, I may have my answer
                            ds_no_count += 1
                        # else maybe...
                    else:  # no break
                        if ds_count == ds_no_count:  # no downstream needs me
                            self.update_invalidation()
                            if self.validation_state == ValidationState.Validated:
                                log_job_trace(
                                    f"\t {self.job_id} -> no ds_count ==ds_no_count & validated"
                                )
                                result = ShouldRun.No
                            elif (
                                self.validation_state == ValidationState.Invalidated
                            ):  # pragma: no cover
                                raise ValueError("Did not expect this case")
                                # log_job_trace(
                                # f"\t {self.job_id} -> yes ds_count ==ds_no_count & invalidated"
                                # )
                                # result = ShouldRun.Yes
                            else:
                                log_job_trace(
                                    f"\t {self.job_id} -> maybe ds_count ==ds_no_count & unknown validation"
                                )
                                result = ShouldRun.Maybe
                        else:
                            log_job_trace(
                                f"\t {self.job_id} -> maybe ds_count != ds_no_count"
                            )
                            result = ShouldRun.Maybe
        log_job_trace(
            f"{self.job_id} == update_should_run. Was {self.should_run} becomes {result}"
        )
        if self.should_run != result:
            self.should_run = result
            self.job_decided_wether_to_run()
            log_job_trace(f"{self.job_id} run_now in update_should_run")
        if self.should_run.is_decided() and self.state == JobState.Waiting:
            self.run_now_if_ready()

    def run_now_if_ready(self):
        log_job_trace(f"{self.job_id} run_now_if_ready")
        if self.all_upstreams_terminal():
            if self.should_run == ShouldRun.Yes:
                log_job_trace(f"\t -> run")
                self.run()
            else:
                log_job_trace(f"\t -> skip")
                self.skip()
        else:
            log_job_trace(f"\t -> not ready")

    def job_became_terminal(self):
        # where is the runner lebowsky, where is the runner?
        """This job is done."""
        log_job_trace(f"{self.job_id} job_became_terminal {self.job_id}")
        if self.state in (JobState.Success, JobState.Skipped):
            for downstream_id in self.downstreams():
                ds = self.runner.job_states[downstream_id]
                ds.update_from_upstream_output(  # todo:  Can I actually skip the comparison work here? Don't think so,
                    # it might have been skipped because it's present,
                    # but the later job might still be based on old history
                    self.job_id,
                    self.updated_output,
                )
                try:
                    ds.update_should_run()
                except Exception as e:
                    self.runner._push_event(
                        "JobFailed",
                        (
                            ds.job_id,
                            exceptions.JobEvaluationFailed(
                                "Update should run had an exception", e
                            ),
                        ),
                    ) # which will in turn upstream fail all downstreams

                # log_job_trace("run_now in update_should_run")
                # ds.run_now_if_ready()

            pass
        elif self.state == JobState.Failed:
            for downstream_job_id in self.downstreams():
                self.runner.job_states[downstream_job_id].upstream_failed(
                    f"Upstream {self.job_id} failed"
                )
            # upstream_failed all downstreams (reason: This job)
            pass
        elif self.state == JobState.UpstreamFailed:
            for downstream_job_id in self.downstreams():
                self.runner.job_states[downstream_job_id].upstream_failed(self.error)
            pass
        else:
            raise NotImplementedError("Should not be reached")

    def job_decided_wether_to_run(self):
        log_job_trace(
            f"{self.job_id} job_decided_wether_to_run {self.job_id}, {self.should_run}"
        )
        # we have been invalidated, or our output is needed.
        # our should_be_run is set.
        # so all we need to do is to call update should_be_run, right?
        for upstream_id in self.upstreams():
            if self.runner.jobs[upstream_id].is_conditional():
                self.runner.job_states[upstream_id].update_should_run()
        pass

    def failed(self, error):
        log_job_trace(f"{self.job_id} failed {error}")
        self.error = error
        self.state = JobState.Failed
        # -> job_became_terminal

    def upstream_failed(self, msg):
        log_job_trace(f"{self.job_id} upstream failed {msg}")
        if self.state != JobState.UpstreamFailed:
            self.error = msg
            self.invalidation_state = ValidationState.UpstreamFailed
            self.state = JobState.UpstreamFailed
            self.runner._push_event(
                "JobUpstreamFailed", (self.job_id,)
            )  # for accounting
        else:
            self.error += "\n" + msg  # multiple upstreams failed. Combine messages
        # -> job_became_terminal

    def succeeded(self, output):
        log_job_trace(f"{self.job_id} succeeded")
        self.updated_output = output
        self.run_time = time.time() - self.start_time
        self.state = JobState.Success

    def skipped(self):
        log_job_trace(f"{self.job_id} skipped")
        self.updated_output = self.historical_output.copy()
        self.state = JobState.Skipped

    def skip(self):
        log_job_trace(f"{self.job_id} skip called")
        if self.state != JobState.Waiting:  # pragma: no cover
            raise ValueError("Run/skip called twice")
        # log_job_trace(f"{job_id} skipped")
        self.runner._push_event("JobSkipped", (self.job_id,))  # for accounting
        self.skipped()

    def run(self):
        log_job_trace(f"{self.job_id} run called")
        if self.state != JobState.Waiting:  # pragma: no cover
            raise ValueError("Run/skip called twice")
        self._state = JobState.ReadyToRun
        self.runner.jobs_to_run_que.put(self.job_id)

    def update_from_upstream_output(self, upstream_job_id, upstream_output):
        log_job_trace(f"{self.job_id} update_from_upstream_output")
        for name, hash in upstream_output.items():
            if name in self.runner.job_inputs[self.job_id]:
                log_trace(f"\t\t\tHad {name}")
                self.updated_input[name] = hash  # update any way.
            else:
                log_trace(f"\t\t\tNot an input {name}")
        if self.validation_state != ValidationState.Invalidated:
            self.update_invalidation()

    def update_invalidation(self):
        if self.job.job_kind == JobKind.Cleanup:
            return False
        if self.all_upstreams_terminal_or_conditional():
            invalidated = self._consider_invalidation()
            log_job_trace(
                f"{self.job_id} - invalidation considered. Result: {invalidated}"
            )
            if invalidated:
                self.validation_state = ValidationState.Invalidated
            else:
                # if self.all_upstreams_terminal():
                if not self.job.output_needed(self.runner):
                    log_job_trace(
                        f"{self.job_id} - not invalidated, but "
                        "all_upstreams_terminal_or_conditional -> validated "
                        "and output not needed "
                        "and was not a cleanup job"
                    )
                    self.validation_state = ValidationState.Validated
            return True
        else:
            return False

    def _consider_invalidation(self):
        downstream_state = self
        old_input = self.historical_input
        new_input = self.updated_input
        invalidated = False
        log_job_trace(
            f"new input {escape_logging(new_input.keys())} old_input {escape_logging(old_input.keys())}"
        )
        if len(new_input) != len(old_input):  # we lost or gained an input -> invalidate
            log_info(
                f"{self.job_id} No of inputs changed (now {len(new_input)}, was {len(old_input)}) -> invalidated "
            )
            invalidated = True
        else:  # same length.
            if set(old_input.keys()) == set(
                new_input.keys()
            ):  # nothing possibly renamed
                log_trace(f"{self.job_id} Same set of input keys")
                for key, old_hash in old_input.items():
                    cmp_job = self.runner.jobs[self.runner.outputs_to_job_ids[key]]
                    if not cmp_job.compare_hashes(old_hash, new_input[key]):
                        log_job_trace(
                            f"{self.job_id} input {key} changed {escape_logging(old_hash)} {escape_logging(new_input[key])} {cmp_job}"
                        )
                        invalidated = True
                        break
            else:
                log_job_trace(
                    f"{self.job_id} differing set of keys. Prev invalidated: {invalidated}"
                )
                for old_key, old_hash in old_input.items():
                    if old_key in new_input:
                        log_trace(
                            f"key in both old/new {old_key} {escape_logging(old_hash)} {escape_logging(new_input[old_key])}"
                        )
                        cmp_job = self.runner.jobs[
                            self.runner.outputs_to_job_ids[old_key]
                        ]
                        if not cmp_job.compare_hashes(old_hash, new_input[old_key]):
                            log_info(f"{self.job_id} input {old_key} changed")
                            invalidated = True
                            break
                    else:
                        # we compare on identity here. Changing file names and hashing methods at once,
                        # what happens if you change the job class as well... better to stay on the easy side
                        count = _dict_values_count_hashed(new_input, old_hash)
                        if count:
                            if count > 1:
                                log_job_trace(
                                    f"{self.job_id} {old_key} mapped to multiple possible replacement hashes. Invalidating to be better safe than sorry"
                                )
                                invalidated = True
                                break
                            # else:
                            # pass # we found a match
                        else:  # no match found
                            log_trace(f"{self.job_id} {old_key} - no match found")
                            invalidated = True
                            break
                log_job_trace(f"{self.job_id} invalidated: {invalidated}")
        return invalidated


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
