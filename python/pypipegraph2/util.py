import os
import sys
from loguru import logger
from rich.console import Console
from . import ppg_traceback

console_args = {}
if "pytest" in sys.modules:
    console_args["width"] = 120
console = Console(**console_args)

cpu_count = None


def escape_logging(s):
    return str(s).replace("<", "\\<").replace("{", "{{").replace("}", "}}")


def CPUs():
    """
    Detects the number of CPUs on a system. Cribbed from pp.
    """
    global cpu_count
    if cpu_count is None:
        cpu_count = 1  # default
        # Linux, Unix and MacOS:
        if hasattr(os, "sysconf"):
            if "SC_NPROCESSORS_ONLN" in os.sysconf_names:
                # Linux & Unix:
                ncpus = os.sysconf("SC_NPROCESSORS_ONLN")
                if isinstance(ncpus, int) and ncpus > 0:
                    cpu_count = ncpus
            else:  # OSX: pragma: no cover
                cpu_count = int(
                    os.popen2("sysctl -n hw.ncpu")[1].read()
                )  # pragma: no cover
        # Windows:
        if "NUMBER_OF_PROCESSORS" in os.environ:  # pragma: no cover
            ncpus = int(os.environ["NUMBER_OF_PROCESSORS"])
            if ncpus > 0:
                cpu_count = ncpus
    return cpu_count


def job_or_filename(job_or_filename, invariant_class=None):
    """Take a filename, or a job. Return Path(filename), dependency-for-that-file
    ie. either the job, or a invariant_class (default: FileInvariant)"""
    from .jobs import Job, FileInvariant
    from . import global_pipegraph
    from pathlib import Path

    if global_pipegraph is None:
        return Path(job_or_filename), []

    if invariant_class is None:  # pragma: no cover
        invariant_class = FileInvariant

    if isinstance(job_or_filename, Job):
        filename = job_or_filename.files[0]
        deps = [job_or_filename]
    elif job_or_filename is not None:
        try:
            global_pipegraph.find_job_from_file(str(job_or_filename))
            deps = [str(job_or_filename)]
            filename = job_or_filename
        except KeyError:
            filename = Path(job_or_filename)
            deps = [invariant_class(filename)]
    else:
        filename = None
        deps = []
    return filename, deps


def assert_uniqueness_of_object(
    object_with_name_attribute, pipegraph=None, also_check=None
):
    """Makes certain there is only one object with this class & .name.

    This is necessary so the pipegraph jobs assign their data only to the
    objects you're actually working with."""
    if pipegraph is None:  # pragma: no branch
        from pypipegraph2 import global_pipegraph

        pipegraph = global_pipegraph

    if object_with_name_attribute.name.find("/") != -1:
        raise ValueError(
            f"Names must not contain /, it confuses the directory calculations. Name was {object_with_name_attribute.name}"
        )
    if pipegraph is None:  # pragma: no cover
        return
    if not hasattr(pipegraph, "object_uniquifier"):
        pipegraph.object_uniquifier = {}
    typ = object_with_name_attribute.__class__
    if typ not in pipegraph.object_uniquifier:
        pipegraph.object_uniquifier[typ] = {}
    if object_with_name_attribute.name in pipegraph.object_uniquifier[typ]:
        raise ValueError(
            "Doublicate object: %s, %s" % (typ, object_with_name_attribute.name)
        )
    if also_check:
        if not isinstance(also_check, list):
            also_check = [also_check]
        for other_typ in also_check:
            if (
                other_typ in pipegraph.object_uniquifier
                and object_with_name_attribute.name
                in pipegraph.object_uniquifier[other_typ]
            ):
                raise ValueError(
                    "Doublicate object: %s, %s"
                    % (other_typ, object_with_name_attribute.name)
                )
    object_with_name_attribute.unique_id = len(pipegraph.object_uniquifier[typ])
    pipegraph.object_uniquifier[typ][object_with_name_attribute.name] = True


def flatten_jobs(j):
    """Take an arbitrary deeply nested list of lists of jobs
    and return just the jobs"""
    from .jobs import Job

    if isinstance(j, Job):
        yield j
    else:
        for sj in j:
            yield from flatten_jobs(sj)


do_jobtrace_log = False


def log_warning(msg):
    logger.opt(depth=1).warning(msg)


def log_error(msg):
    logger.opt(depth=1).error(msg)


def log_info(msg):
    logger.opt(depth=1).info(msg)


def log_debug(msg):
    logger.opt(depth=1).debug(msg)


def log_job_trace(msg):
    if do_jobtrace_log:
        logger.opt(depth=1).log("JT", msg)


def log_trace(msg):
    if do_jobtrace_log:  # pragma: no cover
        logger.opt(depth=1).trace(msg)


def shorten_job_id(job_id):
    dotdotcount = job_id.count(":::")
    if dotdotcount:
        return job_id[: job_id.find(":::") + 3] + "+" + str(dotdotcount)
    else:
        return job_id


def pretty_log_errors(func):
    """capture exceptions (on a function outside of ppg)
    and format it with our fancy local logging exception logger

    This is a decorator!
    """

    def inner(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            exception_type, exception_value, tb = sys.exc_info()
            captured_tb = ppg_traceback.Trace(exception_type, exception_value, tb)
            logger.error(
                captured_tb._format_rich_traceback_fallback(
                    include_locals=True, include_formating=True
                )
            )
            raise

    return inner


def wrap_for_function_invariant(function, *args, **kwargs):
    """When you need to pass a function + parameters which ends up in a FunctionInvariant.
    Think lambda: some_func(filename).

    This marks it so that the FunctionInvariant tracks 'somefunc',
    not the lambda."""
    from .jobs import _mark_function_wrapped

    def wrapper(function=function, args=args, kwargs=kwargs):
        return function(*args, **kwargs)

    _mark_function_wrapped(wrapper, function, "wrap_for_function_invariant")

    return wrapper
