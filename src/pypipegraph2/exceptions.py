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
