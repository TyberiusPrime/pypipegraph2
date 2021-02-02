class PPGException(Exception):
    pass


class FatalGraphException(PPGException):
    pass


class NotADag(FatalGraphException):
    pass


class JobOutputConflict(ValueError):
    """Multiple jobs with overlapping (but not identical) outputs were defined"""

    pass


class JobContractError(PPGException):
    pass


class JobDied(PPGException):
    pass


class JobRedefinitionError(ValueError):
    pass


class RunFailed(PPGException):
    pass


class RunFailedInternally(RunFailed):
    def __init__(self, *args, **kwargs):
        super().__init__(
            "RunFailedInternally: Due to some bug in the graph-running, we could not finish running. File a bug report.",
            *args,
            **kwargs,
        )


class _RunAgain(PPGException):
    pass


class JobError(PPGException):
    def __init__(self, *args):
        super().__init__(*args)

    def __str__(self):
        return (
            ("ppg.JobError:\n")
            + (f"{self.args[0].__class__.__name__}: {self.args[0]}\n")
            # + (f"\tTraceback: {self.args[1]}\n")
            + ("")
        )

class _TerminateThread(Exception):
    pass
