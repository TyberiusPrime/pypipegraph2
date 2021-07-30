from enum import Enum, auto


class ProcessingStatus(Enum):
    Waiting = auto() # not yet ready
    ReadyToRun = auto() # short livened, turns into schedulded very soon
    Schedulded = auto() # it's in the pipeline. Stand by for outcome.
    Done = auto() # grilled and done.

    def is_terminal(self):
        return self is ProcessingStatus.Done


class JobOutcome(Enum):
    NotYet = auto() # default
    Success = auto()
    Skipped = auto()
    Failed = auto()
    UpstreamFailed = auto()
    Pruned = auto()


class ShouldRun(Enum):
    Maybe = auto()
    Yes = auto()
    YesAfterValidation = auto()
    No = auto()
    IfInvalidated = auto()
    IfDownstreamNeedsMe = auto()
    IfParentJobRan = auto()

    def is_decided(self):
        return self in (ShouldRun.Yes, ShouldRun.No)

    def almost_decided(self):
        return self in (ShouldRun.Yes, ShouldRun.No, ShouldRun.IfInvalidated)


class Action(Enum):
    Schedulde = auto()
    GoYes = auto()
    GoNo = auto()
    ShouldNotHappen = auto()
    TakeFromParent = auto()
    RefreshValidationAndTryAgain = auto()
    ConditionalValidated = auto()

class ValidationState(Enum):
    Unknown = auto()
    Validated = auto()
    Invalidated = auto()
    UpstreamFailed = auto()

    def is_terminal(self):
        return self != ValidationState.Unknown


class JobKind(Enum):
    Invariant = auto()
    Output = auto()
    Temp = auto()
    Cleanup = auto()
    Loading = auto()
    JobGenerating = auto()


class RunMode(Enum):
    CONSOLE = 1  # certain redefinitions: FatalGraphException, interactive console, ctrl-c does not work
    NOTEBOOK = 2  # certain redefinitions: warning, no interactive console (todo: gui), control-c,/abort works TODO
    NONINTERACTIVE = (
        3  # such as testing, redefinitions like console, but no gui, ctrl-c works TODO
    )

    def is_strict(self):
        return self is RunMode.CONSOLE or self is RunMode.NONINTERACTIVE


class Resources(Enum):
    SingleCore = "SingleCore"
    AllCores = "AllCores"
    # MemoryHog = "MemoryHog"  # todo
    Exclusive = "Exclusive"
    # RateLimited = "RateLimited"  # todo, think web requests
    RunsHere = "RunsHere"  # in this process
    _RaiseInCoreLock = "_RaiseInCoreLock ="  # for testing
    _RaiseInToNumber = "_RaiseInToNumber ="  # for testing

    def is_external(self):
        return self in (
            Resources.SingleCore,
            Resources.AllCores,
            Resources.Exclusive,
        )  # pragma: no cover - used by interactive

    def to_number(self, max_cores):
        if self is Resources.SingleCore:
            return 1
        elif self is Resources.AllCores:
            return max(max_cores - 1, 1)  # never return less than 1 core
        elif self is Resources.Exclusive:
            return max_cores
        elif self is Resources.RunsHere:
            return 1
        elif self is Resources._RaiseInCoreLock:
            return 0  # which the core lock does not like!
        else:
            raise ValueError("Not a Resource with a given number of cores")
