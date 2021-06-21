import sys
import pytest
import pypipegraph2 as ppg2
from . import fixtures


def run():
    """a ppg/no ppg aware run wrapper"""
    if ppg2.inside_ppg():
        ppg2.run()
    else:
        pass


run_pipegraph = run  # ppg1 compatibility

fl_count = 0


def force_load(job, prefix=None):
    """make sure a dataloadingjob has been loaded (if applicable)"""
    if ppg2.inside_ppg():
        if not isinstance(job, ppg2.Job):
            if prefix is None:
                global fl_count
                fl_count += 1
                prefix = "fl_%i" % fl_count
        else:
            prefix = job.job_id
        return ppg2.JobGeneratingJob(prefix + "_force_load", lambda: None).depends_on(
            job
        )


class RaisesDirectOrInsidePipegraph(object):
    """Piece of black magic from the depths of _pytest
    that will check whether a piece of code will raise the
    expected expcition (if outside of ppg), or if it will
    raise the exception when the pipegraph is running

    Use as a context manager like pytest.raises"""

    def __init__(self, expected_exception, search_message=None):
        self.expected_exception = expected_exception
        self.message = "DID NOT RAISE {}".format(expected_exception)
        self.search_message = search_message
        self.excinfo = None

    def __enter__(self):
        import _pytest

        self.excinfo = object.__new__(_pytest._code.ExceptionInfo)
        return self.excinfo

    def __exit__(self, *tp):
        from _pytest.outcomes import fail

        if ppg2.inside_ppg():
            with pytest.raises(ppg2.RunFailed) as e:
                run()
            ex = e.value.exceptions[0]
            if isinstance(ex, ppg2.JobError):
                ex = ex.args[0]
            if not isinstance(ex, self.expected_exception):
                raise ValueError(
                    f"Unexpected exception. Expected {self.expected_exception}, found {e.value.exceptions[0]}"
                )

            if self.search_message:
                assert self.search_message in str(e.value.exceptions[0])
        else:
            __tracebackhide__ = True
            if tp[0] is None:
                fail(self.message)
            self.excinfo.__init__(tp)
            suppress_exception = issubclass(self.excinfo.type, self.expected_exception)
            if sys.version_info[0] == 2:
                raise ValueError("No python2 support")
            if suppress_exception:
                return True  # seep PEP_0343
            else:
                return False
