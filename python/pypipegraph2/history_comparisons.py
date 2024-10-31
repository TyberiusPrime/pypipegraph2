import json
from .util import (
    # log_info,
    log_error,
    log_debug,
    log_warning,
    # log_trace,
    # log_job_trace
)
import sys
from . import ppg_traceback


def history_is_different(runner, job_upstream_id, job_downstream_id, str_last, str_now):
    while True:
        try: 
            # bad things happen if a KeyboardInterrupt happens here.
            # the Rust code really does not expect this to fail ever.
            return _history_is_different(runner, job_upstream_id, job_downstream_id, str_last, str_now)
        except KeyboardInterrupt:
            raise

def _history_is_different(runner, job_upstream_id, job_downstream_id, str_last, str_now):
    # note that at this point, we already know str_last != str_now,
    # that was tested in rust
    # log_error(f"history is maybe different {job_upstream_id} {job_downstream_id} {str_last == str_now}")
    job_upstream = runner.jobs[job_upstream_id]
    obj_last = json.loads(str_last)
    obj_now = json.loads(str_now)
    if job_downstream_id == "!!!":
        # special case where we compare a job to itself, not to the input it delivered into another job.
        # Has to do with ephemeral jobs not changing output when validated-but-rerun.
        outputs = job_upstream.outputs
        for ip in outputs:
            altered = not job_upstream.compare_hashes(obj_last[ip], obj_now[ip])
            if altered:
                log_debug(
                    f"history is actually different for {job_upstream_id}-> !!! {obj_last[ip]} {obj_now[ip]}"
                )
                return True

    else:
        try:
            outputs = job_upstream.outputs
            inputs = runner.job_inputs[job_downstream_id]
            for ip in inputs:
                if ip in outputs:
                    lip = obj_last[ip]
                    nip = obj_now[ip]
                    altered = not job_upstream.compare_hashes(lip, nip)
                    if altered:
                        log_warning(
                            "history is actually different for job-pair "
                            + f"'{job_upstream_id}'->'{job_downstream_id}'"
                        )

                        log_debug(
                            "history is actually different for job-pair "
                            + f"'{job_upstream_id}'->'{job_downstream_id}': "
                            + f"{obj_last[ip]} {obj_now[ip]}"
                        )

                        # log_warning(f"{obj_last[ip]} {obj_now[ip]}")
                        return True
        except:  # noqa: E722 yes we really want to capture and reraise *everything*
            exception_type, exception_value, tb = sys.exc_info()
            captured_tb = ppg_traceback.Trace(exception_type, exception_value, tb)
            log_error(f"old was {str_last}")
            log_error(f"now was {str_now}")
            log_error(f"{captured_tb}")
            raise

    # log_error(f"history the same {job_upstream_id} {job_downstream_id}")
    return False
