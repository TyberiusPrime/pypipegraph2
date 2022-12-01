import json
from logging import captureWarnings
from pypipegraph2.util import job_or_filename
from .util import log_info, log_error, log_warning, log_debug, log_trace, log_job_trace
import sys
from . import ppg_traceback

ljt = log_job_trace


def history_is_different(runner, job_upstream_id, job_downstream_id, str_last, str_now):
    # note that at this point, we already know str_last != str_now,
    # that was tested in rust
    job_upstream = runner.jobs[job_upstream_id]
    obj_last = json.loads(str_last)
    obj_now = json.loads(str_now)
    try:
        outputs = job_upstream.outputs
        inputs = runner.job_inputs[job_downstream_id]
        for ip in inputs:
            if ip in outputs:
                altered = not job_upstream.compare_hashes(obj_last[ip], obj_now[ip])
                if altered:
                    return True
    except:
        exception_type, exception_value, tb = sys.exc_info()
        captured_tb = ppg_traceback.Trace(
                                exception_type, exception_value, tb
                            )
        log_error(f"old was {str_last}")
        log_error(f"now was {str_now}")
        log_error(f"{captured_tb}")
        raise

    return False