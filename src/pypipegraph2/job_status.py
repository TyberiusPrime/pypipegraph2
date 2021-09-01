"""
There are two questions for any job,
building a kind of 2d time continuum:

- Can this Job Run now. And after it ran, which other jobs might  be 'progressed on'.
- Can we make a decision on this job now? 
    If not, are there other jobs, that, if decided would allow us to make this decision?
- Can we make a decision on the invalidation status?

After writing this in 4 different ways,
I'm no longer 100% convinced this is a decidable
in the face of 'condition' jobs - jobs that must only run if their downstream needs them
(but then they're able to invalidate downstreams that were already done).

Just look at test_ttcc, 4 jobs, and an utter mess of decidability...




"""


from .enums import (
    ProcessingStatus,
    JobOutcome,
    ValidationState,
    ShouldRun,
    JobKind,
    Action,
)
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


ljt = log_job_trace


class JobStatus:
    """Job run information collector"""

    def __init__(self, job_id, runner, historical_input, historical_output):
        self.job_id = job_id
        self.runner = runner

        self.proc_state = ProcessingStatus.Waiting
        self.outcome = JobOutcome.NotYet

        self.historical_input = historical_input
        self.historical_output = historical_output
        self.updated_input = {}
        self.updated_output = {}

        self.start_time = -1
        self.run_time = -1.0
        self.error = None
        self.update_counter = 0
        self.refresh_after = set()

    def was_pruned(self):
        self.validation_state = ValidationState.Unknown
        self.proc_state = ProcessingStatus.Done
        self.outcome = JobOutcome.Pruned
        self.should_run = ShouldRun.No

    def initialize(self):
        # let's make some initial assesments
        if self.job.job_kind == JobKind.Cleanup:  # cleanup jobs never invalidate.
            self.validation_state = ValidationState.Validated
            self._should_run = ShouldRun.IfParentJobRan
        else:
            if not self.upstreams:
                self.validation_state = ValidationState.Validated
            else:
                self.validation_state = ValidationState.Unknown

            if self.job.is_conditional():
                # we always have downstreams, or we would have been pruned
                self._should_run = ShouldRun.IfDownstreamNeedsMe
                # which may be either because it's output is needed,
                # or because it was invalidated...
            else:  # even if validated, we still might be needed
                if self.job.output_needed(self.runner):
                    if getattr(self.job, "run_only_post_validation", False):
                        self._should_run = ShouldRun.YesAfterValidation
                    else:
                        self._should_run = ShouldRun.Yes
                    self.progate_should_run_yes_to_upstream_conditionals()
                else:
                    if self.validation_state is ValidationState.Validated:
                        self._should_run = ShouldRun.No
                    else:
                        self._should_run = ShouldRun.Maybe
            log_job_trace(
                f"{self.job_id} - initial state: {self.validation_state}, {self.should_run}"
            )

    def progate_should_run_yes_to_upstream_conditionals(self):
        for upstream_id in self.upstreams:
            if self.runner.jobs[upstream_id].is_conditional():
                upstream_state = self.runner.job_states[upstream_id]
                upstream_state.should_run = ShouldRun.Yes
                log_job_trace(f"Propagated Should_run = yes to {upstream_id} from {self.job_id}")
                upstream_state.progate_should_run_yes_to_upstream_conditionals()

    def __del__(self):
        self.runner = None  #  break the link for gc

    @property
    def job(self):
        return self.runner.jobs[self.job_id]

    @property
    def should_run(self):
        return self._should_run

    @should_run.setter
    def should_run(self, value):
        if not isinstance(value, ShouldRun):
            raise TypeError()
        self._should_run = value

    # the job is Done. It was decided, and 'ran'
    # and we never need to look at it again.
    def skipped(self):
        # skip happens very quickly after update
        ljt(f"{self.job_id} skipped")
        if self.proc_state != ProcessingStatus.ReadyToRun:
            raise NotImplementedError(f"skipped but state was {self.proc_state}")
        self.proc_state = ProcessingStatus.Done
        self.outcome = JobOutcome.Skipped
        self.updated_output = self.historical_output.copy()
        self._update_downstreams()

    def succeeded(self, output):
        # succeed / fail only happens once the job has actually been processed
        if self.proc_state != ProcessingStatus.Schedulded:
            raise NotImplementedError(f"succeded but state was {self.proc_state}")
        ljt(f"{self.job_id} succeeded")
        self.updated_output = output
        #self.run_time = time.time() - self.start_time
        self.proc_state = ProcessingStatus.Done
        self.outcome = JobOutcome.Success
        self._update_downstreams()

    def failed(self, error, pre_fail=False):
        # succeed / fail only happens once the job has actually been processed
        if self.proc_state != ProcessingStatus.Schedulded:
            if not pre_fail:
                raise NotImplementedError(
                    f"failed but state was {self.proc_state} {self.job_id}"
                )
        ljt(f"{self.job_id} failed {error}")
        self.error = error
        self.proc_state = ProcessingStatus.Done
        self.outcome = JobOutcome.Failed
        self._update_downstreams_with_upstream_failure(
            f"Upstream failed: {self.job_id}"
        )
        # -> job_became_terminal

    # well it wasn't deceided, but the upstream decided for us.
    # no point in (re)calculating it if it's not certain whether the upstream will change
    # or not.
    def upstream_failed(self, msg):
        ljt(f"{self.job_id}'s upstream failed {msg}")
        if self.outcome != JobOutcome.UpstreamFailed:
            if self.proc_state is not ProcessingStatus.Waiting:
                raise NotImplementedError(
                    "Upstream failed on a job that had already been decided is a bug: "
                    f"{self.job_id} {self.proc_state}, {self.validation_state}, {self.outcome}"
                )
            self.error = msg
            self.validation_state = ValidationState.UpstreamFailed
            self.proc_state = ProcessingStatus.Done
            self.outcome = JobOutcome.UpstreamFailed
            self.runner._push_event("JobUpstreamFailed", (self.job_id,))
        else:
            self.error += "\n" + msg  # multiple upstreams failed. Combine messages

    def update(self) -> bool:
        # ab it of big defense spray...
        ljt(
            f"{self.job_id} update {self.update_counter} {self.should_run} {self.validation_state} {self.proc_state}"
        )
        if self.update_counter > len(self.runner.jobs) + 1: # seems like a reasonable upper bound
            self.update_counter += 1

        if self.proc_state != ProcessingStatus.Waiting:
            # we only leave waiting once we have made a decision and are ready to run!
            # so we can short-circuit here
            ljt(f"update: {self.job_id} -> already decided and ready to run")
            return []

        # we don't have rusts exhaustive pattern matching.
        # so we fake it ourselves.
        sr = ShouldRun
        vs = ValidationState
        action_map = {
            # we had already decided in the positive direction
            (sr.Yes, vs.Unknown): Action.Schedulde,
            (sr.Yes, vs.Validated): Action.Schedulde,
            (sr.Yes, vs.Invalidated): Action.Schedulde,
            (sr.Yes, vs.UpstreamFailed): Action.ShouldNotHappen,
            # we had already decided in the negative direction
            (sr.No, vs.Unknown): Action.Schedulde,
            (sr.No, vs.Validated): Action.Schedulde,
            (sr.No, vs.Invalidated): Action.Schedulde,
            (sr.No, vs.UpstreamFailed): Action.Schedulde,
            # yes but...
            (sr.YesAfterValidation, vs.Unknown): Action.RefreshValidationAndTryAgain,
            (sr.YesAfterValidation, vs.Validated): Action.GoYes,
            (sr.YesAfterValidation, vs.Invalidated): Action.GoYes,
            (sr.YesAfterValidation, vs.UpstreamFailed): Action.ShouldNotHappen,
            #
            # no decided yet
            (sr.Maybe, vs.Unknown): Action.RefreshValidationAndTryAgain,
            (sr.Maybe, vs.Validated): Action.GoNo,
            (sr.Maybe, vs.Invalidated): Action.GoYes,
            (sr.Maybe, vs.UpstreamFailed): Action.ShouldNotHappen,
            (sr.IfInvalidated, vs.Unknown): Action.RefreshValidationAndTryAgain,
            (sr.IfInvalidated, vs.Validated): Action.GoNo,
            (sr.IfInvalidated, vs.Invalidated): Action.GoYes,
            (sr.IfInvalidated, vs.UpstreamFailed): Action.ShouldNotHappen,
            # the conditional case.
            (sr.IfDownstreamNeedsMe, vs.Unknown): Action.RefreshValidationAndTryAgain,
            (sr.IfDownstreamNeedsMe, vs.Validated): Action.ConditionalValidated,
            (sr.IfDownstreamNeedsMe, vs.Invalidated): Action.GoYes,
            (sr.IfDownstreamNeedsMe, vs.UpstreamFailed): Action.ShouldNotHappen,
            # the cleanup jobs.
            (sr.IfParentJobRan, vs.Unknown): Action.ShouldNotHappen,
            (sr.IfParentJobRan, vs.Validated): Action.TakeFromParent,
            (sr.IfParentJobRan, vs.Invalidated): Action.ShouldNotHappen,
            (sr.IfParentJobRan, vs.UpstreamFailed): Action.ShouldNotHappen,
        }

        action = action_map[self.should_run, self.validation_state]
        ljt(f"{self.job_id} {self.should_run}, {self.validation_state} -> {action}")
        res = []

        if action == Action.ShouldNotHappen:
            raise NotImplementedError("ShouldNotHappen")

        if action == Action.RefreshValidationAndTryAgain:
            self.check_for_validation_update()
            action = action_map[self.should_run, self.validation_state]
            ljt(
                f"{self.job_id} post validation update {self.should_run}, {self.validation_state} -> {action}"
            )
        elif action == Action.TakeFromParent:
            self.should_run = self.runner.job_states[
                self.job.parent_job.job_id
            ].should_run
            action = action_map[self.should_run, self.validation_state]
            ljt(
                f"{self.job_id} post take from parent {self.should_run}, {self.validation_state} -> {action}"
            )

        if action == Action.ConditionalValidated:
            ljt(f"{self.job_id} Action.ConditionalValidated")
            # at this point we know we're not invalidated
            # but we need to ask the downstreams whether they need us...
            ds_count = 0
            ds_no_count = 0
            res = []
            for ds_id in self.downstreams:
                ds_count += 1
                ds_job_state = self.runner.job_states[ds_id]
                ds_job = self.runner.jobs[ds_id]
                ljt(
                    f"{self.job_id}\t{ds_id} {ds_job_state.should_run} {ds_job_state.validation_state}"
                )
                if ds_job_state.should_run == ShouldRun.Yes:
                    action = Action.GoYes
                    break
                elif ds_job_state.should_run in (ShouldRun.No, ShouldRun.IfInvalidated):
                    ds_no_count += 1
                    res.append(ds_id)
                elif ds_job_state.should_run == ShouldRun.IfParentJobRan:
                    ds_no_count += 1
                elif ds_job_state.should_run in (
                    ShouldRun.IfDownstreamNeedsMe,
                    ShouldRun.Maybe,
                ):
                    res.append(ds_id)
                elif ds_job_state.should_run in (
                    # ShouldRun.No,
                    ShouldRun.IfParentJobRan,
                ):  # can this
                    assert False  # that shouldn't happen?
                else:
                    raise ValueError(ds_job_state.should_run)
                    assert False
            ljt(f"{self.job_id}\t{ds_count}, {ds_no_count}")
            if action == Action.GoYes:
                pass
            else:
                if ds_count == ds_no_count:
                    action = Action.GoNo
                else:
                    return res

        if action == Action.GoYes:
            self.should_run = ShouldRun.Yes
            action = Action.Schedulde
            res = [
                x
                for x in self.upstreams
                if self.runner.jobs[x].is_conditional()
                and not self.runner.job_states[x].should_run.is_decided()
            ]
        elif action == Action.GoNo:
            self.should_run = ShouldRun.No
            action = Action.Schedulde
            res = [
                x
                for x in self.upstreams
                if self.runner.jobs[x].is_conditional()
                and not self.runner.job_states[x].should_run.is_decided()
            ]
        elif action == Action.RefreshValidationAndTryAgain:
            return []  # validation only changes when jobs actually finish

        if action == Action.Schedulde:
            ljt(
                f"{self.job_id} -> schedulde? {self.should_run}, {self.all_upstreams_terminal()}"
            )

            if self.should_run == ShouldRun.No:
                ljt(f"{self.job_id} -> schedulde for skip now...")
                self.proc_state = ProcessingStatus.ReadyToRun
            elif self.all_upstreams_terminal():
                ljt(f"{self.job_id} -> schedulde now...")
                self.proc_state = ProcessingStatus.ReadyToRun
            else:
                ljt(f"{self.job_id} -> Wants to run, but upstreams not done")
                res == []
            return res

        raise ValueError("unhandled", action)

    def check_for_validation_update(self):
        if self.job.job_kind == JobKind.Cleanup:  # cleanup jobs never invalidate.
            assert False
        if self.all_upstreams_terminal_or_conditional_or_decided():
            invalidated = self._consider_invalidation()
            if invalidated:
                self.validation_state = ValidationState.Invalidated
            else:
                self.validation_state = ValidationState.Validated

    def _consider_invalidation(self):
        downstream_state = self
        old_input = self.historical_input
        new_input = self.updated_input
        invalidated = False
        # ljt(
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
                ljt(f"\t{self.job_id} Same set of input keys")
                # ljt(f"{old_input}")
                # ljt(f"{new_input}")
                for key, old_hash in old_input.items():
                    cmp_job = self.runner.jobs[self.runner.outputs_to_job_ids[key]]
                    if not cmp_job.compare_hashes(old_hash, new_input[key]):
                        log_info(
                            f"\tInvalidated {shorten_job_id(self.job_id)} - Hash change: {key}"
                        )
                        log_debug(
                            f"\tInvalidated {shorten_job_id(self.job_id)} - Hash change, {key} was {escape_logging(old_hash)} now {escape_logging(new_input[key])} {cmp_job}"
                        )
                        invalidated = True
                        break
            else:
                ljt(
                    f"\t{self.job_id} differing set of keys. Prev invalidated: {invalidated}"
                )
                for old_key, old_hash in old_input.items():
                    if old_key in new_input:
                        log_trace(
                            f"\tkey in both old/new {old_key} {escape_logging(old_hash)} {escape_logging(new_input[old_key])}"
                        )
                        cmp_job = self.runner.jobs[
                            self.runner.outputs_to_job_ids[old_key]
                        ]
                        if not cmp_job.compare_hashes(old_hash, new_input[old_key]):
                            log_info(
                                f"\tInvalidated: {shorten_job_id(self.job_id)} hash change: {old_key}"
                            )
                            log_debug(
                                f"\tInvalidated: {shorten_job_id(self.job_id)} hash_change: {old_key} Was {escape_logging(old_hash)}, now {escape_logging(new_input[old_key])}"
                            )
                            invalidated = True
                            break
                    else:
                        # we compare on identity here. Changing file names and hashing methods at once,
                        # what happens if you change the job class as well... better to stay on the easy side
                        count = _dict_values_count_hashed(new_input, old_hash)
                        if count:
                            if count > 1:
                                # ljt(
                                # f"{self.job_id} {old_key} mapped to multiple possible replacement hashes. Invalidating to be better safe than sorry"
                                # )
                                log_info(
                                    f"\tInvalidated: {shorten_job_id(self.job_id)}. Old matched multiple new keys"
                                )
                                invalidated = True
                                break
                            # else:
                            # pass # we found a match
                        else:  # no match found
                            # log_trace(f"{self.job_id} {old_key} - no match found")
                            log_info(
                                f"\tInvalidated: {shorten_job_id(self.job_id)}. Old matched no new keys"
                            )
                            invalidated = True
                            break
        ljt(f"{self.job_id} invalidated: {invalidated}")
        return invalidated

    def all_upstreams_terminal_or_conditional_or_decided(self):  # todo: cache if true?
        for upstream_id in self.upstreams:
            s = self.runner.job_states[upstream_id]
            if not s.proc_state.is_terminal():
                if self.runner.jobs[upstream_id].is_conditional():
                    if (
                        s.should_run == ShouldRun.Yes
                        or s.validation_state == ValidationState.Invalidated
                    ):
                        # this should be run first, but hasn't
                        ljt(
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
                    and s.proc_state == ProcessingStatus.Waiting
                ):
                    raise ValueError(f"shoulrun no, but proc_state waiting {s.job_id}")
                else:
                    ljt(
                        f"{self.job_id} all_upstreams_terminal_or_conditional_or_decided -->False, {upstream_id} was not terminal {s.proc_state}"
                    )
                    return False
        ljt(f"\t{self.job_id} all_upstreams_terminal_or_conditional_or_decided->True")
        return True

    def _update_downstreams(self):
        for ds_id in self.downstreams:
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
        for ds_id in self.downstreams:
            ds_state = self.runner.job_states[ds_id]
            ds_state.upstream_failed(msg)
            ds_state._update_downstreams_with_upstream_failure(msg)

    def all_upstreams_terminal(self):
        for upstream_id in self.upstreams:
            s = self.runner.job_states[upstream_id].proc_state
            if not s.is_terminal():
                ljt(
                    f"{self.job_id} all_upstreams_terminal->False (because {upstream_id})"
                )
                return False
        ljt(f"{self.job_id} all_upstreams_terminal->True")
        return True

    @property
    def downstreams(self):
        yield from self.runner.dag.successors(self.job_id)

    @property
    def upstreams(self):
        yield from self.runner.dag.predecessors(self.job_id)

    def dump_subgraph_for_debug(self):
        """Take every job leading up to this job
        and write a mock dependency graph to
        subgraph_debug.py

        Very handy for debugging, but ugly :).

        Note that you can just throw out Jobs later,
        the edges of non-existant jobs will be ignored.
        """

        import pypipegraph2 as ppg

        nodes = []
        seen = set()
        edges = []
        counter = [0]
        node_to_counters = {}

        def descend(node):
            if node in seen:
                return
            seen.add(node)
            j = self.runner.jobs[node]
            if isinstance(j, ppg.FileInvariant):
                nodes.append(f"Path('{counter[0]}').write_text('A')")
                nodes.append(f"job_{counter[0]} = ppg.FileInvariant('{counter[0]}')")
            elif isinstance(j, ppg.ParameterInvariant):
                nodes.append(
                    f"job_{counter[0]} = ppg.ParameterInvariant('{counter[0]}', 55)"
                )
            elif isinstance(j, ppg.FunctionInvariant):
                nodes.append(
                    f"job_{counter[0]} = ppg.FunctionInvariant('{counter[0]}', lambda: 55)"
                )
            elif isinstance(j, ppg.SharedMultiFileGeneratingJob):
                nodes.append(
                    f"job_{counter[0]} = ppg.SharedMultiFileGeneratingJob('{counter[0]}', {[x.name for x in j.files]!r}, dummy_smfg, depend_on_function=False)"
                )
            elif isinstance(j, ppg.TempFileGeneratingJob):
                nodes.append(
                    f"job_{counter[0]} = ppg.TempFileGeneratingJob('{counter[0]}', dummy_fg, depend_on_function=False)"
                )
            elif isinstance(j, ppg.FileGeneratingJob):
                nodes.append(
                    f"job_{counter[0]} = ppg.FileGeneratingJob('{counter[0]}', dummy_fg, depend_on_function=False)"
                )
            elif isinstance(j, ppg.MultiTempFileGeneratingJob):
                files = [counter[0] + "/" + x.name for x in j.files]
                nodes.append(
                    f"job_{counter[0]} = ppg.MultiTempFileGeneratingJob({files!r}, dummy_mfg, depend_on_function=False)"
                )
            elif isinstance(j, ppg.MultiFileGeneratingJob):
                files = [str(counter[0]) + "/" + x.name for x in j.files]
                nodes.append(
                    f"job_{counter[0]} = ppg.MultiFileGeneratingJob({files!r}, dummy_mfg, depend_on_function=False)"
                )
            elif isinstance(j, ppg.DataLoadingJob):
                nodes.append(
                    f"job_{counter[0]} = ppg.DataLoadingJob('{counter[0]}', lambda: None, depend_on_function=False)"
                )
            elif isinstance(j, ppg.AttributeLoadingJob):
                nodes.append(
                    f"job_{counter[0]} = ppg.AttributeLoadingJob('{counter[0]}', DummyObject(), 'attr_{counter[0]}', lambda: None, depend_on_function=False)"
                )
            else:
                raise ValueError(j)
            node_to_counters[node] = counter[0]
            counter[0] += 1
            for parent in self.runner.dag.predecessors(node):
                descend(parent)

        def build_edges(node):
            for parent in self.runner.dag.predecessors(node):
                edges.append(
                    f"edges.append(('{node_to_counters[node]}', '{node_to_counters[parent]}'))"
                )
                build_edges(parent)

        descend(self.job_id)
        edges.append("edges = []")
        build_edges(self.job_id)
        edges.extend(
            [
                "for (a,b) in edges:",
                "    if a in ppg.global_pipegraph.jobs and b in ppg.global_pipegraph.jobs:",
                "        ppg.global_pipegraph.jobs[a].depends_on(ppg.global_pipegraph.jobs[b])",
            ]
        )
        with open("subgraph_debug.py", "w") as op:
            lines = """
class DummyObject:
    pass

def dummy_smfg(files, prefix):
    Path(prefix).mkdir(exist_ok=True, parents=True)
    for f in files:
        f.write_text("hello")


def dummy_mfg(files):
    for f in files:
        f.parent.mkdir(exist_ok=True, parents=True)
        f.write_text("hello")

def dummy_fg(of):
    of.parent.mkdir(exist_ok=True, parents=True)
    of.write_text("fg")

""".split(
                "\n"
            )
            lines += nodes
            lines += edges
            lines += ["", "ppg.run()", "ppg.run"]

            op.write("\n".join("        " + l for l in lines))


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
