from .enums import ProcessingStatus, JobOutcome, ValidationState, ShouldRun, JobKind
from . import exceptions
import time

from .util import (
    log_error,
    log_info,
    log_debug,
    log_job_trace,
    log_trace,
    log_warning,
    escape_logging,
    shorten_job_id,
)


class JobStatus:
    """Job run information collector"""

    def __init__(self, job_id, runner, historical_input, historical_output):
        self.job_id = job_id
        self.runner = runner

        self.proc_state = ProcessingStatus.Waiting
        self.outcome = JobOutcome.NotYet
        self.validation_state = ValidationState.Unknown
        self.should_run = ShouldRun.Maybe

        self.historical_input = historical_input
        self.historical_output = historical_output
        self.updated_input = {}
        self.updated_output = {}

        self.start_time = -1
        self.run_time = -1.0
        self.error = None
        self.update_counter = 0

    def __del__(self):
        self.runner = None  #  break the link for gc

    @property
    def job(self):
        return self.runner.jobs[self.job_id]

    def skipped(self):
        log_job_trace(f"{self.job_id} skipped")
        if self.proc_state != ProcessingStatus.Schedulded:
            raise NotImplementedError(f"skipped but state was {self.proc_state}")
        self.proc_state = ProcessingStatus.Done
        self.outcome = JobOutcome.Skipped
        self.updated_output = self.historical_output.copy()
        self._update_downstreams()

    def succeeded(self, output):
        if self.proc_state != ProcessingStatus.Schedulded:
            raise NotImplementedError(f"succeded but state was {self.proc_state}")
        log_job_trace(f"{self.job_id} succeeded")
        self.updated_output = output
        self.run_time = time.time() - self.start_time
        self.proc_state = ProcessingStatus.Done
        self.outcome = JobOutcome.Success
        self._update_downstreams()

    def failed(self, error, pre_fail=False):
        if self.proc_state != ProcessingStatus.Schedulded:
            if not pre_fail:
                raise NotImplementedError(
                    f"failed but state was {self.proc_state} {self.job_id}"
                )
        log_job_trace(f"{self.job_id} failed {error}")
        self.error = error
        self.proc_state = ProcessingStatus.Done
        self.outcome = JobOutcome.Failed
        self._update_downstreams_with_upstream_failure(
            f"Upstream failed: {self.job_id}"
        )
        # -> job_became_terminal

    def upstream_failed(self, msg):
        log_job_trace(f"{self.job_id}'s upstream failed {msg}")
        if self.outcome != JobOutcome.UpstreamFailed:
            if self.proc_state is not ProcessingStatus.Waiting:
                raise NotImplementedError(
                    "Upstream failed on a job that had already been decided is a bug"
                )
            self.error = msg
            self.validation_state = ValidationState.UpstreamFailed
            self.proc_state = ProcessingStatus.Done
            self.outcome = JobOutcome.UpstreamFailed
            self.runner._push_event("JobUpstreamFailed", (self.job_id,))
        else:
            self.error += "\n" + msg  # multiple upstreams failed. Combine messages

    def update(self) -> bool:
        if self.update_counter > 500:
            raise ValueError()
        self.update_counter += 1

        if self.proc_state != ProcessingStatus.Waiting:
            log_job_trace(f"update: {self.job_id} -> already decided")
            return False
        if self.job.job_kind is JobKind.Cleanup:
            log_job_trace(f"update: {self.job_id} -> cleanup")
            self.should_run = self.runner.job_states[
                self.job.parent_job.job_id
            ].should_run
        else:
            log_job_trace(
                f"update: {self.job_id} -> pre update validation {self.validation_state}"
            )
            self._update_validation()
            log_job_trace(
                f"update: {self.job_id} -> post update validation {self.validation_state}"
            )
            if self.job.is_conditional():
                log_job_trace(f"update: {self.job_id} -> Conditional")
                if self.validation_state == ValidationState.Invalidated:
                    self.should_run = ShouldRun.Yes
                else:
                    self.should_run = (
                        self._examine_downstream_to_decide_whether_conditional_is_needed()
                    )

            else:
                log_job_trace(f"update: {self.job_id} -> non-conditional")
                if self.validation_state == ValidationState.Invalidated:
                    self.should_run = ShouldRun.Yes
                elif self.validation_state == ValidationState.Validated:
                    # decide on whether the output is required
                    try:
                        output_needed = self.job.output_needed(self.runner)
                        if output_needed:
                            log_job_trace(f"update: {self.job_id} -> output needed")
                            self.should_run = ShouldRun.Yes
                        else:
                            self.should_run = ShouldRun.No
                    except Exception as e:
                        self.outcome = JobOutcome.Failed
                        # self.proc_state = ProcessingStatus.Done
                        self.should_run = ShouldRun.Yes
                        self.error = e

                else:  # we nac't tell yet.
                    self.should_run = ShouldRun.Maybe

        log_job_trace(f"update: {self.job_id} -> should_run {self.should_run}")
        if self.should_run.is_terminal() and self.all_upstreams_terminal():
            log_job_trace(f"update: {self.job_id} -> schedulde now...")
            self.proc_state = ProcessingStatus.ReadyToRun

    def _update_should_run(self):
        assert not self.proc_state is ProcessingStatus.ReadyToRun
        if self.validation_state == ValidationState.Invalidated:
            # ready to run means we now decide on run(->succes/failure) or ski# ready to run means we now decide on run(->succes/failure) or skipp
            self.should_run = ShouldRun.Yes
            return True
        elif self.validation_state == ValidationState.Validated:
            if self.job.is_conditional():
                # ok, so no intrinsic reason to run this
                # what about the downstreams
                self.should_run = (
                    self._examine_downstream_to_decide_whether_conditional_is_needed()
                )
                return self.should_run.is_terminal()
            else:  # a non conditional job, that is validated
                # question it's output
                if self.job.output_needed(self.runner):
                    self.should_run = ShouldRun.Yes
                else:
                    self.should_run = ShouldRun.No
                return True
        return False

    def _examine_downstream_to_decide_whether_conditional_is_needed(self):
        ds_count = 0
        ds_no_count = 0
        for downstream_id in self.downstreams():
            if (
                self.runner.jobs[downstream_id].job_kind == JobKind.Cleanup
            ):  # those don't count
                continue

            log_job_trace(f"\t downstream: {downstream_id}")
            ds_count += 1
            # self.runner.job_states[downstream_id].update_should_run()
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
                if self.validation_state == ValidationState.Validated:
                    log_job_trace(
                        f"\t {self.job_id} -> no ds_count ==ds_no_count & validated"
                    )
                    result = ShouldRun.No
                elif (
                    self.validation_state == ValidationState.Invalidated
                ):  # pragma: no cover
                    assert self.validation_state != ValidationState.Invalidated
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
                    f"\t {self.job_id} -> maybe ds_count {ds_count} != ds_no_count {ds_no_count}"
                )
                result = ShouldRun.Maybe
                # we need to check this again eventually
                for ds_id in self.downstreams():
                    self.runner.jobs_that_need_propagation.append(ds_id)
                # self.runner.jobs_that_need_propagation.append(self.job_id)
        return result

    def _update_validation(self):
        if self.job.job_kind == JobKind.Cleanup:  # cleanup jobs never invalidate.
            # self.validation_state = ValidationState.Validated
            return False
        if self.all_upstreams_terminal_or_conditional_or_decided():
            # we can make a decision.
            invalidated = self._consider_invalidation()
            if invalidated:
                self.validation_state = ValidationState.Invalidated
            else:
                self.validation_state = ValidationState.Validated
                # else:
                # self.validation_state = ValidationState.Invalidated
                # log_job_trace(
                # f"{self.job_id} - not invalidated, but "
                # "all_upstreams_terminal_or_conditional_or_decided -> validated "
                # "and output not needed "
                # "and was not a cleanup job"
                # )
            return True
        else:
            return False

    def _consider_invalidation(self):
        downstream_state = self
        old_input = self.historical_input
        new_input = self.updated_input
        invalidated = False
        # log_job_trace(
        # f"new input {escape_logging(new_input.keys())} old_input {escape_logging(old_input.keys())}"
        # )
        if len(new_input) != len(old_input):  # we lost or gained an input -> invalidate
            log_info(
                f"Invalidated {shorten_job_id(self.job_id)} - # of inputs changed ({len(old_input)}->{len(new_input)})"
            )
            new = set(new_input.keys())
            old = set(old_input.keys())
            mnew = "\n".join(["\t" + x for x in sorted(new)])
            mold = "\n".join(["\t" + x for x in sorted(old)])
            if len(new_input) > len(old_input):
                changed = sorted(new.difference(old))
                mchanged = "gained:\n"
            else:
                changed = sorted(old.difference(new))
                mchanged = "lost:\n"
                mchanged += "\n".join(["\t" + x for x in sorted(changed)])
            mchanged += "\n".join(["\t" + x for x in sorted(changed)])
            log_debug(
                f"Invalidated {shorten_job_id(self.job_id)} - # of inputs changed: \n old:\n{mold}\n new:\n{mnew}\n{mchanged}"
            )

            invalidated = True
        else:  # same length.
            if set(old_input.keys()) == set(
                new_input.keys()
            ):  # nothing possibly renamed
                log_job_trace(f"{self.job_id} Same set of input keys")
                for key, old_hash in old_input.items():
                    cmp_job = self.runner.jobs[self.runner.outputs_to_job_ids[key]]
                    if not cmp_job.compare_hashes(old_hash, new_input[key]):
                        log_info(
                            f"Invalidated {shorten_job_id(self.job_id)} - Hash change: {key}"
                        )
                        log_debug(
                            f"Invalidated {shorten_job_id(self.job_id)} - Hash change, {key} was {escape_logging(old_hash)} now {escape_logging(new_input[key])} {cmp_job}"
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
                            log_info(
                                f"Invalidated: {shorten_job_id(self.job_id)} hash change: {old_key}"
                            )
                            log_debug(
                                f"Invalidated: {shorten_job_id(self.job_id)} hash_change: {old_key} Was {escape_logging(old_hash)}, now {escape_logging(new_input[old_key])}"
                            )
                            invalidated = True
                            break
                    else:
                        # we compare on identity here. Changing file names and hashing methods at once,
                        # what happens if you change the job class as well... better to stay on the easy side
                        count = _dict_values_count_hashed(new_input, old_hash)
                        if count:
                            if count > 1:
                                # log_job_trace(
                                # f"{self.job_id} {old_key} mapped to multiple possible replacement hashes. Invalidating to be better safe than sorry"
                                # )
                                log_info(
                                    f"Invalidated: {shorten_job_id(self.job_id)}. Old matched multiple new keys"
                                )
                                invalidated = True
                                break
                            # else:
                            # pass # we found a match
                        else:  # no match found
                            # log_trace(f"{self.job_id} {old_key} - no match found")
                            log_info(
                                f"Invalidated: {shorten_job_id(self.job_id)}. Old matched no new keys"
                            )
                            invalidated = True
                            break
        log_job_trace(f"{self.job_id} invalidated: {invalidated}")
        return invalidated

    def all_upstreams_terminal_or_conditional_or_decided(self):
        for upstream_id in self.upstreams():
            s = self.runner.job_states[upstream_id]
            if not s.proc_state.is_terminal():
                if self.runner.jobs[upstream_id].is_conditional():
                    if (
                        s.should_run == ShouldRun.Yes
                        or s.validation_state == ValidationState.Invalidated
                    ):
                        # this should be run first, but hasn't
                        log_job_trace(
                            f"{self.job_id} all_upstreams_terminal_or_conditional_or_decided -->False, {upstream_id} was conditional, but shouldrun, and not yes"
                        )
                        return False
                    else:
                        if self.runner.job_states[
                            upstream_id
                        ].all_upstreams_terminal_or_conditional_or_decided():
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
                elif (
                    s.should_run == ShouldRun.No
                    and s.proc_state is ProcessingStatus.Waiting
                ):
                    pass
                else:
                    log_job_trace(
                        f"{self.job_id} all_upstreams_terminal_or_conditional_or_decided -->False, {upstream_id} was not terminal {s.proc_state}"
                    )
                    return False
        log_job_trace(
            f"{self.job_id} all_upstreams_terminal_or_conditional_or_decided->True"
        )
        return True

    def _update_downstreams(self):
        for ds_id in self.downstreams():
            self.runner.jobs_that_need_propagation.append(ds_id)
            for name, hash in self.updated_output.items():
                if name in self.runner.job_inputs[ds_id]:
                    log_trace(f"\t\t\t{ds_id} had {name}")
                    self.runner.job_states[ds_id].updated_input[
                        name
                    ] = hash  # update any way.
                else:
                    log_trace(f"\t\t\tNot an input for {ds_id} {name}")

    def _update_downstreams_with_upstream_failure(self, msg):
        for ds_id in self.downstreams():
            ds_state = self.runner.job_states[ds_id]
            ds_state.upstream_failed(msg)
            ds_state._update_downstreams_with_upstream_failure(msg)

    def all_upstreams_terminal(self):
        for upstream_id in self.upstreams():
            s = self.runner.job_states[upstream_id].proc_state
            if not s.is_terminal():
                log_job_trace(
                    f"{self.job_id} all_upstreams_terminal->False (because {upstream_id})"
                )
                return False
        log_job_trace(f"{self.job_id} all_upstreams_terminal->True")
        return True

    def downstreams(self):
        yield from self.runner.dag.successors(self.job_id)

    def upstreams(self):
        yield from self.runner.dag.predecessors(self.job_id)


def _dict_values_count_hashed(a_dict, count_this):
    """Specialised 'how many times does this hash occur in this dict. For renamed inputs"""
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
