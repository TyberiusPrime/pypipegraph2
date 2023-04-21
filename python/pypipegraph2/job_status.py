"""
# todo: rename file.
"""

from .enums import (
    JobOutcome,
)


class RecordedJobOutcome:
    """Job run information collector"""

    def __init__(self, job_id, outcome, payload):
        if not isinstance(outcome, JobOutcome):
            raise ValueError("Not an JobOutcome")
        self.job_id = job_id
        self.outcome = outcome
        self.payload = payload
        self.runtime = -1

    @property
    def error(self):
        if self.outcome is JobOutcome.Failed:
            return self.payload
        elif self.outcome is JobOutcome.UpstreamFailed:
            return "Upstream"
        else:
            raise AttributeError(
                f"No error available on an non failed JobOutcome (was {self.outcome}"
            )

    def __repr__(self):
        return f"RecordedJobOutcome({self.job_id}, {self.outcome}"
