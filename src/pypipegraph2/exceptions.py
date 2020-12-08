class PPGException(Exception):
    pass


class FatalGraphException(PPGException):
    pass


class NotADag(FatalGraphException):
    pass


class JobOutputConflict(FatalGraphException):
    """Multiple jobs with overlapping (but not identical) outputs were defined"""

    pass


class JobContractError(PPGException):
    pass


class RunFailed(PPGException):
    pass


class _RunAgain(PPGException):
    pass


class JobError(PPGException):
    def __str__(self):
        return (
            ("ppg.JobError:\n")
            + (f"\tException:{self.args[0]}\n")
            + (f"\tTraceback: {self.args[1]}\n")
            + ("")
        )
