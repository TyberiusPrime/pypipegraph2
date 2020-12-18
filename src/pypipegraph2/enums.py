from enum import Enum, auto


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

    def is_failed(self):
        return self in (
            JobState.Failed,
            JobState.UpstreamFailed,
        )


class ValidationState(Enum):
    Unknown = auto()
    Validated = auto()
    Invalidated = auto()
    UpstreamFailed = auto()


class JobKind(Enum):
    Invariant = auto()
    Output = auto()
    Temp = auto()
    Cleanup = auto()
    Loading = auto()
    JobGenerating = auto()


class RunMode(Enum):
    INTERACTIVE = 1  # certain redefinitions: FatalGraphException, interactive console, ctrl-c does not work
    NOTEBOOK = 2  # certain redefinitions: warning, no interactive console (todo: gui), control-c,/abort works TODO
    NONINTERACTIVE = 3  # such as testing, redefinitions like interactive, but no gui, ctrl-c works TODO


class Resources(Enum):
    SingleCore = "SingleCore"
    AllCores = "AllCores"
    MemoryHog = "MemoryHog"  # todo
    Exclusive = "Exclusive"
    RateLimited = "RateLimited"  # todo, think web requests
    RunsHere = "RunsHere"  # in this process

    def to_number(self, max_cores):
        if self is Resources.SingleCore:
            return 1
        elif self is Resources.AllCores:
            return max_cores - 1
        elif self is Resources.Exclusive:
            return max_cores
        else:
            raise ValueError("Not an external Resource with a given number of cores")
