class PPGException(Exception):
    """Baseclass for all our exceptions"""

    pass


class FatalGraphException(PPGException):
    """Could not finish executing the graph completly"""

    pass


class NotADag(FatalGraphException):
    """Your graph is not acyclic"""

    pass


class JobOutputConflict(ValueError):
    """Multiple jobs with overlapping (but not identical) outputs were defined"""

    pass


class JobContractError(PPGException):
    """A job did not do what it was supposed to do"""

    pass


class JobDied(PPGException):
    pass


class JobRedefinitionError(ValueError):
    pass


class JobEvaluationFailed(PPGException):
    """For some reason, we could not decide on whether to execute this job"""

    pass


class RunFailed(FatalGraphException):
    """The execution failed outside of the scope of a single job"""

    pass


class JobsFailed(RunFailed):
    """one or more jobs failed"""

    def __init__(self, msg, exceptions):
        super().__init__(msg)
        self.exceptions = exceptions


class RunFailedInternally(RunFailed):
    """There is a bug in pypipegraph2"""

    def __init__(self, *args, **kwargs):
        super().__init__(
            "RunFailedInternally: Due to some bug in the graph-running, we could not finish running. File a bug report.",
            *args,
            **kwargs,
        )


class _RunAgain(PPGException):
    """Internal to signal rerun"""

    pass


class JobError(PPGException):
    """Wrapper around the exceptions thrown by random jobs"""

    def __str__(self):
        return (
            ("ppg.JobError:\n")
            + (f"{self.args[0].__class__.__name__}: {self.args[0]}\n")
            # + (f"\tTraceback: {self.args[1]}\n")
            + ("")
        )

    def __repr__(self):
        return str(self)


class JobCanceled(PPGException):
    pass


class HistoryLoadingFailed(FatalGraphException):
    pass
