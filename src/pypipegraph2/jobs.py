from __future__ import annotations

import json
import signal

import time
import tempfile
import pickle
import os
import dis
import re
import sys
import inspect
import types
from typing import Union, List, Dict, Optional, Tuple, Callable
from pathlib import Path
from io import StringIO
from collections import namedtuple
from threading import Lock
from deepdiff.deephash import DeepHash

from . import hashers, exceptions, ppg_traceback
from .enums import JobKind, Resources, ValidationState
from .util import escape_logging
import hashlib
import shutil
from .util import log_info, log_error, log_warning, log_debug, log_trace, log_job_trace

module_type = type(sys)

non_chdired_path = Path(".").absolute()
python_version = ".".join(
    (str(x) for x in sys.version_info[:2])
)  # we only care about major.minor

DependsOnInvariant = namedtuple("DependsOnInvariant", ["invariant", "self"])
CachedJobTuple = namedtuple("CachedJobTuple", ["load", "calc"])
PlotJobTuple = namedtuple("PlotJobTuple", ["plot", "cache", "table"])


def _normalize_path(path):
    from . import global_pipegraph

    # this little bit of memoization here saves quite a bit of runtime.
    if global_pipegraph is not None:
        res = global_pipegraph._path_cache.get(path, None)
        if res is not None:
            return res
    org_path = path
    path = Path(path)
    if path.is_absolute():
        res = path.resolve()
    else:
        res = path.resolve().relative_to(Path(".").absolute())
    if global_pipegraph is not None:
        global_pipegraph._path_cache[org_path] = res
    return res


def _dedup_job(cls, job_id):
    from . import global_pipegraph

    if global_pipegraph is None:
        raise ValueError("Must instantiate a pipegraph before creating any Jobs")
    if not isinstance(job_id, str):
        raise TypeError(
            f"at this point, job_id must be str, was {job_id!r}, {type(job_id)}"
        )
    if "/../" in job_id:
        raise TypeError(f".. in job id not allowed. Was {job_id}")
    if global_pipegraph.run_mode.is_strict() and job_id in global_pipegraph.jobs:
        j = global_pipegraph.jobs[job_id]
        if type(j) != cls:
            raise exceptions.JobRedefinitionError(
                f"Redefining job {job_id} with different type - prohibited by RunMode. Was {type(j)}, wants to be {cls}"
            )
        return global_pipegraph.jobs[job_id]
    else:
        return object.__new__(cls)


def _mark_function_wrapped(outer, inner, desc="callback"):
    """mark a function as 'wrapped' - ie. the FunctionInvariant
    is being created on the callback, but what the graph calls is outer
    (which is supposed to call inner!)
    """
    if not callable(inner):
        raise TypeError(f"{desc} function must be callable")
    outer.wrapped_function = inner
    return outer


x = "hello"


def _safe_str(x):  # pragma: no cover
    try:
        return str(x)
    except:  # noqa:E722
        return "could not str(x)"


class JobList(list):
    """Jobs.depends_on already takes any iterable of jobs.
    But sometimes you want to say [jobs,...].depends_on,
    and this allows that"""

    def depends_on(self, *args, **kwargs):
        for job in self:
            job.depends_on(*args, **kwargs)


class Job:
    job_id: int
    historical: Optional[Tuple[str, Dict[str, str]]]
    cleanup_job_class = None

    def __new__(cls, outputs, *args, **kwargs):
        return _dedup_job(cls, ":::".join(sorted([str(x) for x in outputs])))

    def __init__(
        self,
        outputs: Union[str, List[str], Dict[str, str]],
        resources: Resources = Resources.SingleCore,
    ):
        self.use_resources(resources)
        if isinstance(outputs, list):
            self.outputs = self._validate_outputs(outputs)
        else:  # pragma: no cover
            raise TypeError("Invalid output definition.")
        self.outputs = sorted([str(x) for x in outputs])
        if not hasattr(
            self, "job_id"
        ):  # first definition. Otherwise we are a deduped job that has been initialized
            self.job_id = ":::".join(self.outputs)
        if not hasattr(
            self, "dependency_callbacks"
        ):  # first definition. Otherwise we are a deduped job that has been initialized
            self.dependency_callbacks = []
        self._validate()
        self.readd()
        if not hasattr(
            self, "_pruned"
        ):  # first definition. Otherwise we are a deduped job that has been initialized
            self._pruned = False

    def __str__(self):
        return f"{self.__class__.__name__}: {getattr(self, 'job_id', '*no_init*')}"

    def __repr__(self):
        return str(self)

    def _validate(self):
        pass

    def _validate_outputs(self, outputs):
        res = []
        for o in outputs:
            if isinstance(o, Path):
                o = str(o)
            elif not isinstance(o, str):
                raise TypeError(f"outputs must all be strings, was {type(o)}")
            res.append(o)
        return res

    def __iter__(self):
        """It yields self so you can use jobs and list of jobs uniformly"""
        yield self

    def readd(self):
        """Readd this job to the current global pipegraph
        (possibly the *new* global pipegraph).
        Without any dependencies!
        """
        from . import global_pipegraph

        log_trace(f"adding {self.__class__.__name__} {self.job_id}")

        if global_pipegraph is None:
            raise ValueError("Must instantiate a pipegraph before creating any Jobs")

        global_pipegraph.add(self)

    def _handle_function_dependency(self, func):
        while hasattr(
            func, "wrapped_function"
        ):  # the actual function is just an adaptor, ignore it for the wrapped function
            # e.g. FileGeneratingJob ppg1 compatibility with 'no-output-filename parameter'.
            func = func.wrapped_function
            # log_debug(f"Falling back to wrapped function {self.job_id}")
        func_invariant = FunctionInvariant(func, self.job_id)
        self.func_invariant = func_invariant  # we only store it so ppg1.compatibility ignore_code_changes can prune it
        self.depends_on(func_invariant)

    def use_resources(self, resources: Resources):
        if not isinstance(resources, Resources):
            raise TypeError("resources must by pipegraph2.enums.Resources")
        self.resources = resources
        return self

    def depends_on(
        self,
        other_job: Union[Union[str, Job], List[Union[str, Job]]] = False,
        *other_jobs: Union[Union[str, Job], List[Union[str, Job]]],
    ):
        """Depend on another Job, which must be done before this one can run.
        If the other job changes it's output, this job will be invalidated (and rerun).

        You may pass in one ore more Jobs, a list of such,
        ore a callable that will return such. The callable will
        be called when the ppg is run the first time
        (todo: when is the later useful)
        """

        from . import global_pipegraph

        if other_job is False and not other_jobs:
            # raise ValueError("You have to pass in at least one job")
            return self  # this is how ppg1 did it
        if isinstance(other_job, (CachedJobTuple, PlotJobTuple)):
            raise TypeError(
                "You passed in a CachedJobTuple/PlotJobTuple - unclear what to depend on. Pass in either .load/.calc or .plot/.cache/.table"
            )
        elif hasattr(other_job, "__iter__") and not isinstance(other_job, (Job, str)):
            for x in other_job:
                self.depends_on(x)
        else:
            if isinstance(other_job, Job):
                o_job = other_job
                o_inputs = other_job.outputs
            elif other_job is None:
                return self
            elif hasattr(other_job, "__call__"):
                self.dependency_callbacks.append(other_job)
                return self
            else:
                if isinstance(other_job, Path):
                    other_job = str(other_job)
                try:
                    o_job = global_pipegraph.jobs[
                        global_pipegraph.outputs_to_job_ids[other_job]
                    ]
                except KeyError as e:
                    raise KeyError(
                        f"Dependency specified via job_id {repr(other_job)}. No such job found"
                    )
                o_inputs = [other_job]
            if o_job.job_id == self.job_id:
                raise exceptions.NotADag("Job can not depend on itself")
            if global_pipegraph.has_edge(self, o_job):
                raise exceptions.NotADag(
                    f"{o_job.job_id} is already (directly) upstream of {self.job_id}, can't be downstream as well (cycle)"
                )

            log_trace(f"adding edge {o_job.job_id}, {self.job_id}")
            global_pipegraph.add_edge(o_job, self)
            global_pipegraph.job_inputs[self.job_id].update(o_inputs)
        if other_jobs:
            for o in other_jobs:
                self.depends_on(o)
        return self

    def output_needed(self, _ignored_runner):
        return False

    def compare_hashes(self, old_hash, new_hash):
        return old_hash == new_hash

    def extract_strict_hash(self, a_hash) -> bytes:
        """Our regular 'hashes' contain things
        like mtime/size, per-python-bytecode etc
        to allow flexible compare_hashes.

        For the Shared jobs, we need something that
        is strictly a set of bytes to compare.

        Note that we call this with only a single output hash.

        """
        raise NotImplementedError()  # pragma: no cover

    def depends_on_func(self, function, name=None):
        """Create a function invariant.
        Return a NamedTumple (invariant, self)
        (so you can change with job.depends_on_func(func)[1].depends_on(...))
        """
        if isinstance(function, str):
            function, name = name, function
        if not name:
            name = FunctionInvariant.func_to_name(function)

        upstream = FunctionInvariant(function, self.job_id + "_" + name)
        self.depends_on(upstream)
        return DependsOnInvariant(upstream, self)

    def depends_on_file(self, filename):
        job = FileInvariant(filename)
        self.depends_on(job)
        return DependsOnInvariant(job, self)

    def depends_on_params(self, params):
        job = ParameterInvariant(self.job_id, params)
        self.depends_on(job)
        return DependsOnInvariant(job, self)

    def prune(self):
        self._pruned = True

    def unprune(self):
        self._pruned = False

    def __call__(self):
        """execute just enough of the pipeline that this job get's evaluated.
        A job may return something here for interactive work (plot jobs for example
        will return the plot object
        """
        from . import global_pipegraph

        global_pipegraph.run_for_these(self)
        return self._call_result()

    def _call_result(self):  # pragma: no cover
        return None

    def is_conditional(self):
        return self.job_kind in (JobKind.Temp, JobKind.Loading)

    @property
    def exception(self):
        """Interrogate global pipegraph for this job's exception.
        Mostly for the ppg1 tests...
        """
        from . import global_pipegraph

        e = global_pipegraph.last_run_result[self.job_id].error
        if isinstance(e, exceptions.JobError):
            return e.args[0]
        else:  # pragma: no cover
            return e

    @property
    def failed(self):
        """Did this job fail in any way in the last run?
        I.e. does it have an exception?"""
        return bool(self.exception)

    @property
    def stack_trace(self):
        """Interrogate global pipegraph for this job's exception stacktrace.
        Mostly for the ppg1 tests...
        """
        from . import global_pipegraph

        e = global_pipegraph.last_run_result[self.job_id].error
        if isinstance(e, exceptions.JobError):
            return e.args[1]
        else:  # pragma: no cover - defensive
            raise ValueError("No stacktrace available")

    @property
    def upstreams(self):
        """Return a list of jobs that are directly upstream of this one by querying the
        global pipegraph"""
        from . import global_pipegraph

        gg = global_pipegraph
        return [gg.jobs[job_id] for job_id in gg.job_dag.predecessors(self.job_id)]

    @property
    def depth(self):
        if self.job_kind is JobKind.Cleanup:
            return self.parent_job.depth + 1
        upstreams = self.upstreams
        if upstreams:
            return 1 + max((upstream_job.depth for upstream_job in upstreams))
        else:
            return 1


class MultiFileGeneratingJob(Job):
    job_kind = JobKind.Output

    def __new__(cls, files, *args, **kwargs):
        files = cls._validate_files_argument(files)[0]
        return Job.__new__(cls, [str(x) for x in files])

    def __init__(
        self,
        files: List[Path],  # todo: extend type attribute to allow mapping
        generating_function: Callable[List[Path]],
        resources: Resources = Resources.SingleCore,
        depend_on_function: bool = True,
        empty_ok=True,
        always_capture_output=True,
    ):

        self.generating_function = self._validate_func_argument(generating_function)
        self.depend_on_function = depend_on_function
        self.files, self._lookup = self._validate_files_argument(files)
        if len(self.files) != len(set(self.files)):
            raise ValueError(
                "Paths were present multiple times in files argument. Fix your input"
            )
        self.org_files = self.files
        Job.__init__(self, [str(x) for x in self.files], resources)
        self._single_file = False
        self.empty_ok = empty_ok
        self.always_capture_output = always_capture_output
        self.stdout = "not captured"
        self.stderr = "not captured"
        self.pid = None

    def __getitem__(self, key):
        if not self._lookup:
            if isinstance(key, int):
                return self._map_filename(self.org_files[key])
            else:
                raise ValueError(
                    f"{self.job_id} has no lookup dictionary - files was not a dict, and key was not an integer index(into files)"
                )
        return self._map_filename(self._lookup[key])

    @staticmethod
    def _validate_func_argument(func):
        sig = inspect.signature(func)
        if len(sig.parameters) == 0:
            raise TypeError(
                "A *FileGeneratingJobs callback function must take at least one parameter: The file(s) to create"
            )
        return func

    @staticmethod
    def _validate_files_argument(files, allow_absolute=False):
        from . import global_pipegraph

        # print(files)

        if not hasattr(files, "__iter__"):
            raise TypeError("files was not iterable")
        if isinstance(files, (str, Path)):
            raise TypeError(
                "files must not be a single string or Path, but an iterable"
            )
        if isinstance(files, dict):
            lookup = list(files.keys())
            org_files = list(files.values())
            files = org_files
        else:
            lookup = None
        for f in files:
            if not isinstance(f, (str, Path)):
                raise TypeError("Files for (Multi)FileGeneratingJob must be Path/str")
            if (
                global_pipegraph is not None
                and global_pipegraph.prevent_absolute_paths
                or allow_absolute
            ) and Path(f).is_absolute():
                raise ValueError(
                    f"Absolute file path as job_ids prevented by graph.prevent_absolute_paths. Was {f}"
                )
        path_files = (Path(x) for x in files)
        abs_files = [_normalize_path(x) for x in path_files]
        if lookup:
            lookup = {lookup[ii]: abs_files[ii] for ii in range(len(lookup))}
        return sorted(abs_files), lookup

    def readd(self):
        super().readd()
        if self.depend_on_function:
            self._handle_function_dependency(self.generating_function)

    def callback(self):
        self.generating_function(*self.get_input())

    def run(self, runner, historical_output):  # noqa:C901
        self.files = [self._map_filename(fn) for fn in self.org_files]
        # we only rebuild the file if
        # - we were invalidated
        # - it was missing or
        # - we had no historical output (hash) to compare to or
        # - it is identical in mtime and size to the historical output
        # - it is identical in size and hash to the historical output
        # the later two cases are for tempfile jobs
        # so that when their downstream failed,
        # and they were not cleaned up
        # we still don't rebuild them every  time.

        all_present = True
        del_counter = 0
        for fn in self.files:
            if fn.exists():
                # if we were invalidated, we run-  mabye
                log_job_trace(
                    f"{fn} existed - invalidation: {runner.job_states[self.job_id].validation_state}, in history: {str(fn) in historical_output}"
                )
                if all_present:  # so far...
                    if (
                        runner.job_states[self.job_id].validation_state
                        is not ValidationState.Invalidated  # both Valid and Unknown are ok here
                    ):
                        if str(fn) in historical_output:
                            stat = fn.stat()
                            mtime_the_same = int(stat.st_mtime) == historical_output[
                                str(fn)
                            ].get("mtime", -1)
                            size_the_same = stat.st_size == historical_output[
                                str(fn)
                            ].get("size", -1)
                            if mtime_the_same and size_the_same:
                                continue
                            if size_the_same:
                                new_hash = hashers.hash_file(fn)
                                if new_hash["hash"] == historical_output[str(fn)].get(
                                    "hash", "No hash "
                                ):  # hash the same
                                    continue
                            raise ValueError(
                                historical_output, stat.st_mtime, stat.st_size
                            )
                # at least one file was missing
                log_trace(f"unlinking {fn}")
                fn.unlink()
                all_present = False
            else:
                all_present = False
        if all_present:
            return historical_output
        elif del_counter != len(self.files):
            # ok, we triggered rebuild - nuke all output files
            for fn in self.files:
                if fn.exists():
                    log_trace(f"unlinking {fn}")
                    fn.unlink()

        input = self.get_input()
        if self.resources in (
            Resources.SingleCore,
            Resources.AllCores,
            Resources.Exclusive,
        ):
            # que = multiprocessing.Queue() # replace by pipe
            log_trace(f"Forking for {self.job_id}")
            # these only get closed by the parent process
            # and we can't use tempfiles.
            # they would get closed by other forked jobs running in parallel
            stdout = open(
                runner.job_graph.run_dir
                / f"{runner.start_time:.2f}_{self.job_number}.stdout",
                "w+",
            )
            stderr = open(
                runner.job_graph.run_dir
                / f"{runner.start_time:.2f}_{self.job_number}.stderr",
                "w+",
            )
            exception_out = open(
                runner.job_graph.run_dir / f"{self.job_number}.exception", "w+b"
            )  # note the binary!

            def aborted(sig, stack):
                raise KeyboardInterrupt()  # pragma: no cover  todo: interactive testing

            try:
                error_exit_code = 1
                self.pid = os.fork()
                if (
                    self.pid == 0
                ):  # pragma: no cover - coverage doesn't see this, since the spawned job os._exits()
                    try:
                        signal.signal(signal.SIGUSR1, aborted)

                        # log_info(f"tempfilename: {stderr.name}")
                        stdout_ = sys.stdout
                        stderr_ = sys.stderr
                        sys.stdout = stdout
                        sys.stderr = stderr
                        os.dup2(stdout.fileno(), 1)
                        os.dup2(stderr.fileno(), 2)

                        try:
                            self.generating_function(*input)
                            stdout.flush()
                            stderr.flush()
                            # else:
                            os._exit(0)  # go down hard, do not call atexit and co.
                        except TypeError as e:
                            if hasattr(
                                self.generating_function, "__code__"
                            ):  # build ins
                                func_info = f"{self.generating_function.__code__.co_filename}:{self.generating_function.__code__.co_firstlineno}"
                            else:
                                func_info = "unknown"
                            if "takes 0 positional arguments but 1 was given" in str(e):
                                raise TypeError(
                                    e.args[0]
                                    + ". You have forgotten to take the output_files as your first parameter."
                                    + f"The function was defined in {func_info}."
                                )
                            else:
                                raise
                    except SystemExit as e:
                        error_exit_code = e.code
                        raise
                    except (Exception, KeyboardInterrupt) as e:
                        captured_tb = None  # if the capturing fails for any reason...
                        traceback_dumped = False
                        try:
                            exception_type, exception_value, tb = sys.exc_info()
                            captured_tb = ppg_traceback.Trace(
                                exception_type, exception_value, tb
                            )
                            pickle.dump(captured_tb, exception_out)
                            traceback_dumped = True
                            pickle.dump(e, exception_out)
                            exception_out.flush()
                        except Exception as e2:
                            # msg = f"FileGeneratingJob raised exception, but saving the exception failed: \n{type(e)} {escape_logging(e)} - \n {type(e2)} {escape_logging(e2)}\n"
                            # traceback is already dumped
                            # exception_out.seek(0,0) # might have dumped the traceback already, right?
                            # pickle.dump(captured_tb, exception_out)
                            if not traceback_dumped:
                                pickle.dump(None, exception_out)
                            pickle.dump(
                                exceptions.JobDied(repr(e), repr(e2)), exception_out
                            )
                            exception_out.flush()
                            raise
                    finally:
                        stdout.flush()
                        stderr.flush()
                        # sys.stdout = stdout_
                        # sys.stderr = stderr_
                        # os.dup2(stdout_, 1)
                        # os.dup2(stderr_, 2)
                        os._exit(error_exit_code)
                else:
                    sleep_time = 0.01  # which is the minimum time a job can take...
                    time.sleep(sleep_time)
                    wp1, waitstatus = os.waitpid(self.pid, os.WNOHANG)
                    try:
                        while wp1 == 0 and waitstatus == 0:
                            sleep_time *= 2
                            if sleep_time > 1:
                                sleep_time = 1
                            time.sleep(sleep_time)
                            wp1, waitstatus = os.waitpid(self.pid, os.WNOHANG)
                    except KeyboardInterrupt:  # pragma: no cover  todo: interactive testing
                        log_trace(
                            f"Keyboard interrupt in {self.job_id} - sigbreak spawned process"
                        )
                        os.kill(self.pid, signal.SIGUSR1)
                        time.sleep(1)
                        log_trace(
                            f"Keyboard interrupt in {self.job_id} - checking spawned process"
                        )
                        wp1, waitstatus = os.waitpid(self.pid, os.WNOHANG)
                        if wp1 == 0 and waitstatus == 0:
                            log_trace(
                                f"Keyboard interrupt in {self.job_id} - sigkill spawned process"
                            )
                            os.kill(self.pid, signal.SIGKILL)
                        raise
                    if os.WIFEXITED(waitstatus):
                        # normal termination.
                        exitcode = os.WEXITSTATUS(waitstatus)
                        if exitcode != 0:
                            self.stdout, self.stderr = self._read_stdout_stderr(
                                stdout, stderr
                            )
                            exception_out.seek(0, 0)
                            raw = exception_out.read()
                            # log_info(f"Raw exception result {raw}")
                            exception_out.seek(0, 0)

                            tb = None
                            exception = None
                            try:
                                tb = pickle.load(exception_out)
                                exception = pickle.load(exception_out)
                            except Exception as e:
                                log_error(
                                    f"Job died (=exitcode == {exitcode}): {self.job_id}"
                                )
                                log_error(f"stdout: {self.stdout} {self.stderr}")
                                exception = exceptions.JobDied(
                                    f"Job {self.job_id} died but did not return an exception object. Decoding exception {e}",
                                    None,
                                    exitcode,
                                )
                                exception.exit_code = exitcode
                            finally:
                                raise exceptions.JobError(exception, tb)
                        elif self.always_capture_output:
                            self.stdout, self.stderr = self._read_stdout_stderr(
                                stdout, stderr
                            )
                    else:
                        if os.WIFSIGNALED(waitstatus):
                            exitcode = -1 * os.WTERMSIG(waitstatus)
                            self.stdout, self.stderr = self._read_stdout_stderr(
                                stdout, stderr
                            )
                            # don't bother to retrieve an exception, there won't be anay
                            log_error(f"Job killed by signal: {self.job_id}")
                            raise exceptions.JobDied(
                                f"Job {self.job_id} was killed", None, exitcode
                            )

                        else:
                            raise NotImplementedError(  # pragma: no cover - purely defensive
                                "Process did not exit, did not signal, but is dead?. Figure out and extend, I suppose"
                            )
            finally:
                stdout.close()  # unlink these soonish.
                os.unlink(stdout.name)
                stderr.close()
                os.unlink(stderr.name)
                exception_out.close()
                os.unlink(exception_out.name)

                self.pid = None
        else:
            self.generating_function(*input)
        missing_files = [x for x in self.files if not x.exists()]
        if missing_files:
            raise exceptions.JobContractError(
                f"Job {self.job_id} did not create the following files: {[str(x) for x in missing_files]}"
            )
        if not self.empty_ok:
            empty_files = [x for x in self.files if x.stat().st_size == 0]
            if empty_files:
                raise exceptions.JobContractError(
                    f"Job {self.job_id} created empty files and empty_ok was False: {[str(x) for x in empty_files]}"
                )
        res = {
            str(of): hashers.hash_file(self._map_filename(of)) for of in self.org_files
        }
        return res

    def _read_stdout_stderr(self, stdout, stderr):
        try:
            stdout.flush()
            stdout.seek(0, os.SEEK_SET)
            stdout_text = stdout.read()
            try:
                stdout.close()
            except FileNotFoundError:
                pass
        except ValueError as e:  # pragma: no cover - defensive
            if "I/O operation on closed file" in str(e):
                stdout_text = (
                    "Stdout could not be captured / io operation on closed file"
                )
            else:
                raise
        try:
            stderr.flush()
            stderr.seek(0, os.SEEK_SET)
            stderr_text = stderr.read()
            try:
                stderr.close()
            except FileNotFoundError:
                pass
        except ValueError as e:  # pragma: no cover - defensive
            if "I/O operation on closed file" in str(e):
                stderr_text = (
                    "stderr could not be captured / io operation on closed file"
                )
            else:
                raise
        return stdout_text, stderr_text

    def get_input(self):
        if self._single_file:
            return (self.files[0],)
        else:
            if self._lookup:
                return (self._lookup,)
            else:
                return (self.files,)

    def output_needed(self, runner):
        for fn in self.files:
            if not fn.exists():
                log_job_trace(f"Output file {fn} did not exist")
                return True
            # other wise we have no history, and the skipping would
            # break the graph execution
            if str(fn) not in runner.job_states[self.job_id].historical_output:
                log_job_trace(
                    f"No history for {fn}, {escape_logging(runner.job_states[self.job_id].historical_output)}"
                )
                return True
        return False

    def _call_result(self):
        if self._lookup:
            return self._lookup
        else:
            return self.files

    def _map_filename(self, f):
        return f

    def kill_if_running(self):  # pragma: no cover - todo: interactive testing
        if self.pid is not None:
            os.kill(self.pid, signal.SIGTERM)

    def compare_hashes(self, old_hash, new_hash):
        return new_hash["hash"] == old_hash.get("hash", "")

    def extract_strict_hash(self, a_hash) -> bytes:
        return a_hash["hash"].encode("utf-8")


class FileGeneratingJob(MultiFileGeneratingJob):  # might as well be a function?
    def __new__(cls, output_filename, *args, **kwargs):
        return super().__new__(cls, [output_filename])

    def __init__(
        self,
        output_filename: Union[Path, str],
        generating_function: Callable[Path],
        resources: Resources = Resources.SingleCore,
        depend_on_function: bool = True,
        empty_ok=False,
        always_capture_output=True,
    ):
        MultiFileGeneratingJob.__init__(
            self,
            [output_filename],
            generating_function,
            resources,
            depend_on_function,
            empty_ok=empty_ok,
            always_capture_output=always_capture_output,
        )
        self._single_file = True


class MultiTempFileGeneratingJob(MultiFileGeneratingJob):
    """A job that creates files that are removed
    once all it's dependents have run.

    These will always run at least once,
    (due to the 'virtual clean up jobs' capturing the dependencies)
    """

    job_kind = JobKind.Temp

    def __init__(
        self,
        files: List[Path],
        generating_function: Callable[List[Path]],
        resources: Resources = Resources.SingleCore,
        depend_on_function: bool = True,
    ):
        MultiFileGeneratingJob.__init__(
            self, files, generating_function, resources, depend_on_function
        )
        self._single_file = False

        self.cleanup_job_class = _FileCleanupJob

    def output_needed(
        self, runner
    ):  # yeah yeah yeah the temp jobs need to delegate to their downstreams dude!
        raise ValueError("unreachable")
        for downstream_id in runner.dag.neighbors(self.job_id):
            job = runner.jobs[downstream_id]
            if job.output_needed(runner):
                log_job_trace(
                    f"Tempfile {self.job_id} said output needed because of {job.job_id}"
                )
                return True
        log_job_trace("Tempfile {self.job_id} said no output needed")
        return False


class TempFileGeneratingJob(
    MultiTempFileGeneratingJob
):  # todo: should theis be a func?
    job_kind = JobKind.Temp

    def __new__(cls, output_filename, *args, **kwargs):
        return super().__new__(cls, [output_filename])

    def __init__(
        self,
        output_filename: Union[Path, str],
        generating_function: Callable[Path],
        resources: Resources = Resources.SingleCore,
        depend_on_function: bool = True,
    ):
        MultiTempFileGeneratingJob.__init__(
            self, [output_filename], generating_function, resources, depend_on_function
        )
        self._single_file = True


class _FileCleanupJob(Job):
    """Jobs may register cleanup jobs that injected after their immediate downstreams.
    This encapsulates those

    """

    job_kind = JobKind.Cleanup

    def __new__(cls, parent_job):
        return _dedup_job(cls, f"CleanUp:{parent_job.job_id}")

    def __init__(self, parent_job):
        Job.__init__(self, [f"CleanUp:{parent_job.job_id}"], Resources.RunsHere)
        self.parent_job = parent_job

    def run(self, _ignored_runner, _historical_output):
        for fn in self.parent_job.files:
            if fn.exists():
                log_trace(f"unlinking {fn}")
                fn.unlink()

        return {self.outputs[0]: None}  # todo: optimize this awy?


class _InvariantMixin:
    def depends_on(
        self,
        other_job: Union[Union[str, Job], List[Union[str, Job]]] = None,
        *other_jobs: Union[Union[str, Job], List[Union[str, Job]]],
    ):
        raise exceptions.JobContractError(
            "Invariants may not depend on other jobs. "
            "They get evaluated every time anyway. "
            "And they would insulate from their upstreams. "
            "Makes no sense"
        )


class _FileInvariantMixin:
    def calculate(
        self, file, stat
    ):  # so that FileInvariant and FunctionInvariant can reuse it

        # ppg1 had the option of using an external .md5sum file for the hash
        # provided the filetime was exactly the same as the files'
        # it would accept it instead of calculating it's own.
        # todo:  decide wether we want to keep this here,
        # or move it into it's own class?
        # the pro argument is basically, ppg1. compatibility.
        # the draw back is the complexity for the common case,
        # and the weakness of the md5 algorithm (can't easily upgrade though)
        md5sum_path = Path(file.with_name(file.name + ".md5sum"))
        if (
            False and md5sum_path.exists()
        ):  # I think it's a good idea no to rely on files produced by ppg1
            new_stat = file.stat
            st_md5 = os.stat(md5sum_path)
            if stat.st_mtime == st_md5.st_mtime:
                checksum = md5sum_path.read_text()
            else:
                checksum = self._md5_fallback(file)
                with open(md5sum_path, "wb") as op:
                    op.write(checksum.encode("utf-8"))
                os.utime(md5sum_path, (stat.st_mtime, stat.st_mtime))

            stat = file.stat()
            return {
                "hash": checksum,
                "mtime": int(stat.st_mtime),
                "size": stat.st_size,
            }
        else:
            return hashers.hash_file(file)

    def _md5_fallback(self, filename):
        import hashlib

        file_size = filename.stat().st_size
        if file_size > 200 * 1024 * 1024:  # pragma: no cover
            print("Taking md5 of large file", filename)
        with open(filename, "rb") as op:
            block_size = 1024 ** 2 * 10
            block = op.read(block_size)
            _hash = hashlib.md5()
            while block:
                _hash.update(block)
                block = op.read(block_size)
            res = _hash.hexdigest()
        return res


class FunctionInvariant(_InvariantMixin, Job, _FileInvariantMixin):
    job_kind = JobKind.Invariant

    def __new__(cls, function, name=None):
        name, function = cls._parse_args(function, name)
        return super().__new__(cls, [name])

    @classmethod
    def _parse_args(cls, function, name):
        if isinstance(function, (str, Path)):
            name, function = function, name

        name = "FI" + (str(name) if name else FunctionInvariant.func_to_name(function))
        return name, function

    def __init__(
        self, function, name=None
    ):  # must support the inverse calling with name, function, for compatibility to pypipegraph
        name, function = self._parse_args(function, name)
        self.verify_arguments(name, function)
        self.function = function  # must assign after verify!

        self.source_file = self.get_source_file()
        Job.__init__(self, [name], Resources.RunsHere)

    def output_needed(self, _ignored_runner):
        return True

    def run(self, _runner, historical_output):
        # todo: Don't recalc if file / source did not change.
        # Actually I suppose we can (ab)use the the graph and a FileInvariant for that?
        res = {}
        sf = self.source_file
        if historical_output:
            historical_output = historical_output[self.job_id]
        else:
            historical_output = {}
        file_unchanged = False
        new_file_hash = None
        if sf and not sf.name.startswith(
            "<"
        ):  # we only have a source file for python functions.
            # sf = Path(sf)
            stat = sf.stat()
            if historical_output:
                if "source_file" in historical_output:
                    if int(stat.st_mtime) == historical_output["source_file"].get(
                        "mtime", -1
                    ) and stat.st_size == historical_output["source_file"].get(
                        "size", -1
                    ):
                        # the file did not change at all
                        file_unchanged = True
                        new_file_hash = historical_output["source_file"]
                    else:
                        new_file_hash = self.calculate(sf, stat)
                        if (
                            new_file_hash["hash"]
                            == historical_output["source_file"]["hash"]
                        ):
                            file_unchanged = True
                            new_file_hash = historical_output["source_file"]
            if not new_file_hash:
                new_file_hash = self.calculate(sf, stat)

        if not hasattr(self.function, "__code__"):  # build ins
            line_no = -1
        else:
            line_no = self.function.__code__.co_firstlineno
        line_unchanged = line_no == historical_output.get("source_line_no", False)
        log_trace(
            f"{self.job_id}, {file_unchanged}, {line_unchanged}, {escape_logging(new_file_hash)}, {escape_logging(historical_output)}"
        )

        if file_unchanged and line_unchanged and python_version in historical_output:
            dis = historical_output[python_version][0]
            source = historical_output["source"]
            is_python_func = self.is_python_function(self.function)
        else:
            source, is_python_func = self.get_source()
            if is_python_func:
                dis = (
                    self.get_dis(self.function),
                )  # returns (('',),) for cython functions? better to handel it ourselves
            else:
                if self.function is None:
                    dis = "None"
                else:
                    dis = ""

        if is_python_func:
            closure = self.extract_closure(
                self.function
            )  # returns an empty string for cython functions
        else:
            closure = ""

        res = {"source": source, "source_line_no": line_no}
        res[python_version] = (dis, closure)
        if new_file_hash:
            res["source_file"] = new_file_hash

        return {self.job_id: res}

    def compare_hashes(self, old_hash, new_hash, python_version=python_version):
        if python_version in new_hash and python_version in old_hash:
            res = new_hash[python_version] == old_hash[python_version]
            # log_trace(f"Comparing based on bytecode: result {res}")
            return res
        else:  # pragma: no cover
            # missing one python version, did the source change?
            # should we compare Closures here as well? todo
            res = new_hash["source"] == old_hash["source"]
            # log_trace(f"Comparing based on source: result {res}")
            return res

    def extract_strict_hash(self, a_hash) -> bytes:
        return a_hash["source"].encode("utf-8")

    def get_source_file(self):
        if self.is_python_function(self.function):
            sf = inspect.getsourcefile(self.function)
            if sf == sys.argv[0]:  # at least python 3.8 does not have this absolute.
                # might change with 3.9? https://bugs.python.org/issue20443
                return non_chdired_path / sf  # pragma: no cover
            else:
                return Path(sf)
        return None

    @staticmethod
    def is_python_function(function):
        if (not hasattr(function, "__code__")) or (
            "cython_function_or_method" in str(type(function))
            or (
                isinstance(function, types.MethodType)
                and "cython_function_or_method" in str(type(function.__func__))
            )
        ):
            return False
        else:
            return True

    def get_source(self):
        """Return the 'source' and whether this was a python function"""
        if self.function is None:
            # since the 'default invariant' is False, this will still read 'invalidated the first time it's being used'
            return None, False
        if self.is_python_function(self.function):
            return self._get_python_source(self.function), True
        else:
            return self._get_source_from_non_python_function(self.function), False

    @staticmethod
    def _get_python_source(function):
        source = inspect.getsource(function).strip()
        # cut off function definition / name, but keep parameters
        if source.startswith("def"):
            source = source[source.find("(") :]
        # filter doc string
        if function.__doc__:
            for prefix in ['"""', "'''", '"', "'"]:
                if prefix + function.__doc__ + prefix in source:
                    source = source.replace(
                        prefix + function.__doc__ + prefix,
                        "",
                    )
        return source

    @classmethod
    def get_dis(cls, function):
        return (cls.dis_code(function.__code__, function),)

    @classmethod
    def _get_source_from_non_python_function(cls, function):
        """get source for built ins, cython, etc"""
        if str(function).startswith("<built-in function"):
            return str(function)
        elif (
            hasattr(function, "im_func")
            and (
                "cyfunction" in repr(function.im_func)
                or repr(function.im_func).startswith("<built-in function")
            )
        ) or "cython_function_or_method" in str(type(function)):
            return cls.get_cython_source(function)
        elif isinstance(
            function, types.MethodType
        ) and "cython_function_or_method" in str(type(function.__func__)):
            return cls.get_cython_source(function.__func__)
        else:
            raise ValueError("Can't handle this object %s" % function)

    @staticmethod
    def functions_equal(a, b):
        if a is None and b is None:
            return True
        elif a is None or b is None:
            return False
        elif hasattr(a, "__code__") and hasattr(a, "__closure__"):
            if hasattr(b, "__code__") and hasattr(b, "__closure__"):
                return (a.__code__ == b.__code__) and (a.__closure__ == b.__closure__)
            else:
                return False
        else:
            return False
            # return ~(hasattr(b, "__code__") and hasattr(b, "__closure__"))

    @staticmethod
    def debug_function_differences(a, b):
        if a is None and b is None:
            return "No difference"
        elif a is None or b is None:
            return "one function was none"
        elif hasattr(a, "__code__") and hasattr(a, "__closure__"):
            if hasattr(b, "__code__") and hasattr(b, "__closure__"):
                if not (a.__code__ == b.__code__):
                    return "The function __code__ differed"
                elif not (a.__closure__ == b.__closure__):
                    in_a = "in A:\n"
                    in_b = "in B:\n"
                    set_a = set()
                    set_b = set()
                    for x in a.__closure__:
                        xc = x.cell_contents
                        in_a += f"\t{id(xc)} {type(xc) }{_safe_str(xc)[:40]}\n"
                        try:
                            set_a.add(xc)
                        except:
                            pass
                    for x in b.__closure__:
                        xc = x.cell_contents
                        in_b += f"\t{id(xc)} {type(xc)} {_safe_str(xc)[:40]}\n"
                        try:
                            set_b.add(xc)
                        except:
                            pass
                    only_in_a = sorted(_safe_str(xc) for xc in set_a.difference(set_b))
                    only_in_b = sorted(_safe_str(xc) for xc in set_b.difference(set_a))

                    return f"The function closures differed. Contents:: \n{in_a} \n {in_b}\n Only in a {only_in_a}\nOnly in b {only_in_b}"
                else:
                    return "The functions were identical"
            else:
                return "Could not get code & closure on both functions"
        else:
            return "Could not get code & closure on both functions"
        # return ~(hasattr(b, "__code__") and hasattr(b, "__closure__"))

    @staticmethod
    def function_to_str(func):
        if str(func).startswith("<built-in function"):
            return "%s" % func
        elif (
            hasattr(func, "im_func")
            and (
                "cyfunction" in repr(func.im_func)
                or ("<built-in function" in repr(func.im_func))
            )
        ) or ("<cyfunction " in str(func)):
            return "%s %i" % FunctionInvariant.get_cython_filename_and_line_no(func)
        else:
            return "%s %i" % (
                func.__code__.co_filename if func else "None",
                func.__code__.co_firstlineno if func else 0,
            )

    @staticmethod
    def extract_closure(function):
        """extract the bound variables from a function into a string representation"""
        try:
            closure = function.func_closure
        except AttributeError:
            closure = function.__closure__
        output = ""
        if closure:
            for name, cell in zip(function.__code__.co_freevars, closure):
                # we ignore references to self - in that use case you're expected
                # to make your own ParameterInvariants, and we could not detect
                # self.parameter anyhow (only self would be bound)
                # we also ignore bound functions - their address changes
                # every run.
                # IDEA: Make this recursive (might get to be too expensive)
                try:
                    if (
                        name != "self"
                        and not hasattr(cell.cell_contents, "__code__")
                        and not isinstance(cell.cell_contents, module_type)
                    ):
                        if isinstance(cell.cell_contents, dict):
                            x = repr(sorted(list(cell.cell_contents.items())))
                        elif isinstance(cell.cell_contents, set) or isinstance(
                            cell.cell_contents, frozenset
                        ):
                            x = repr(sorted(list(cell.cell_contents)))
                        else:
                            x = repr(cell.cell_contents)
                        if (
                            "at 0x" in x
                        ):  # if you don't have a sensible str(), we'll default to the class path. This takes things like <chipseq.quality_control.AlignedLaneQualityControl at 0x73246234>.
                            x = x[: x.find("at 0x")]
                        if "id=" in x:  # pragma: no cover - defensive
                            raise ValueError("Still an issue, %s", repr(x))
                        output += "\n" + x
                except ValueError as e:  # pragma: no cover - defensive
                    if str(e) == "Cell is empty":
                        pass
                    else:
                        raise
        return output

    inner_code_object_re = re.compile(
        r"(<code\sobject\s<?[^>]+>?\sat\s0x[a-f0-9]+[^>]+)"
        + "|"
        + "(<code\tobject\t<[^>]+>,\tfile\t'[^']+',\tline\t[0-9]+)"  # that's the cpython way  # that's how they look like in pypy. More sensibly, actually
    )

    @classmethod
    def dis_code(cls, code, function, version_info=sys.version_info):
        """'dissassemble' python code.
        Strips lambdas (they change address every execution otherwise),
        but beginning with 3.7 these are actually included
        """

        out = StringIO()
        dis.dis(code, file=out)
        discode = out.getvalue().split("\n")
        # now, eat of the line nos, if there are any
        res = []
        for row in discode:
            row = row.split()
            res.append("\t".join(row[1:]))
        res = "\n".join(res)
        res = cls.inner_code_object_re.sub("lambda", res)
        if function and hasattr(function, "__qualname__"):
            res = res.replace(function.__qualname__, "<func name ommited>")
        # beginning with  version 3.7, this piece of code is obsolete,
        # since dis does depth descend by itself way.
        if version_info < (3, 7):  # pragma: no cover
            for ii, constant in enumerate(code.co_consts):
                if hasattr(constant, "co_code"):
                    res += "inner no %i" % ii
                    res += cls.dis_code(constant, None)
        return res

    @staticmethod
    def get_cython_source(cython_func):
        """Attempt to get the cython source for a function.
        Requires cython code to be compiled with -p or #embed_pos_in_docstring=True in the source file

        Unfortunatly, finding the right module (to get an absolute file path) is not straight forward,
        we inspect all modules in sys.module, and their children, but we might be missing sub-sublevel modules,
        in which case we'll need to increase search depth
        """

        # check there's actually the file and line no documentation
        filename, line_no = FunctionInvariant.get_cython_filename_and_line_no(
            cython_func
        )

        # load the source code
        op = open(filename, "rb")
        d = op.read().decode("utf-8").split("\n")
        op.close()
        log_error(d)

        # extract the function at hand, minus doc string
        head = d[line_no - 1]
        text = "\n".join(d[line_no:])
        import re

        match = re.match("([ \t]*)(\"\"\"|'''|[^ \t]+)", text)
        if match.groups()[1] in ("'''", '"""'):  # starts with a doc string...
            search = match.groups()[1]
            text = text[text.find(search, text.find(search) + 3) + 3 :]
            # now eat up including the new line after the docstring
            text = text[text.find("\n") + 1 :]
        text = text.split("\n")
        while (
            text and text[0].strip() == ""
        ):  # cut off empty lines between docstring and code
            text = text[1:]
        log_error(text)
        first_line_indent = len(head) - len(head.lstrip())
        log_info(repr(head[:20]))
        body = []
        for line in text:
            indent = len(line) - len(line.lstrip())
            log_error(f"{indent}, {first_line_indent} {repr(line)}")
            if indent <= first_line_indent and line.strip():
                break
            body.append(line)
        while body and body[-1].strip() == "":  # remove empty lines at end
            body = body[:-1]
        return head + "\n" + "\n".join(body)

    def get_cython_filename_and_line_no(cython_func):
        pattern = re.compile(r'.* file "(?P<file_name>.*)", line (?P<line>\d*)>')
        match = pattern.match(str(cython_func.func_code))
        if match:
            line_no = int(match.group("line"))
            filename = match.group("file_name")
        else:
            first_doc_line = cython_func.__doc__.split("\n")[0]
            module_name = cython_func.__module__
            if not first_doc_line.startswith("File:"):
                raise ValueError(
                    "No file/line information in doc string. Make sure your cython is compiled with -p (or #embed_pos_in_docstring=True atop your pyx"
                )
            line_no = int(
                first_doc_line[
                    first_doc_line.find("starting at line ")
                    + len("starting at line ") : first_doc_line.find(")")
                ]
            )
            # find the right module
            module_name = cython_func.im_class.__module__
            found = False
            for name in sorted(sys.modules):
                if name == module_name or name.endswith("." + module_name):
                    try:
                        if (
                            getattr(sys.modules[name], cython_func.im_class.__name__)
                            == cython_func.im_class
                        ):
                            found = sys.modules[name]
                            break
                    except AttributeError:  # pragma: no cover
                        continue
                elif hasattr(sys.modules[name], module_name):
                    sub_module = getattr(sys.modules[name], module_name)
                    try:  # pragma: no cover
                        if (
                            getattr(sub_module, cython_func.im_class.__name__)
                            == cython_func.im_class
                        ):
                            found = sys.moduls[name].sub_module
                            break
                    except AttributeError:
                        continue
            if not found:  # pragma: no cover
                raise ValueError("Could not find module for %s" % cython_func)
            filename = found.__file__.replace(".so", ".pyx").replace(
                ".pyc", ".py"
            )  # pyc replacement is for mock testing
        return filename, line_no

    def verify_arguments(self, job_id, function):
        if not callable(function) and function is not None:
            raise TypeError("%s function was not a callable (or None)" % job_id)
        if hasattr(self, "function") and not FunctionInvariant.functions_equal(
            function, self.function
        ):
            from . import global_pipegraph

            if global_pipegraph.run_mode.is_strict():
                raise exceptions.JobRedefinitionError(
                    "FunctionInvariant %s created twice with different functions: \n%s\n%s\n%s"
                    % (
                        job_id,
                        FunctionInvariant.function_to_str(function),
                        FunctionInvariant.function_to_str(self.function),
                        FunctionInvariant.debug_function_differences(
                            self.function, function
                        ),
                    )
                )

    @staticmethod
    def func_to_name(function):
        """Automatically derive a name for a function"""
        if function is None:
            raise TypeError(
                "Can not derive a function name for FunctionInvariant(None)"
            )
        name = function.__qualname__
        if "<lambda>" in name:
            raise TypeError(
                "Could not automatically derive a function name for a lambda, pass a name please"
            )
        return name

    def __str__(self):
        if (
            hasattr(self, "function")
            and self.function
            and hasattr(self.function, "__code__")
        ):  # pragma: no cover
            # during creating, __str__ migth be called by a debug function before function is set...
            return "%s (job_id=%s,id=%s\n Function: %s:%s)" % (
                self.__class__.__name__,
                self.job_id,
                id(self),
                self.function.__code__.co_filename,
                self.function.__code__.co_firstlineno,
            )
        elif hasattr(self, "function") and str(self.function).startswith(
            "<built-in function"
        ):
            return "%s (job_id=%s,id=%s, Function: %s)" % (
                self.__class__.__name__,
                self.job_id,
                id(self),
                self.function,
            )
        else:
            return "%s (job_id=%s,id=%s, Function: None)" % (
                self.__class__.__name__,
                self.job_id,
                id(self),
            )


class FileInvariant(_InvariantMixin, Job, _FileInvariantMixin):
    job_kind = JobKind.Invariant

    def __new__(cls, file):
        return super().__new__(cls, [str(_normalize_path(file))])

    def __init__(self, file):
        from . import global_pipegraph

        self.file = Path(file)
        super().__init__([str(_normalize_path(file))])

        self.files = [
            self.file
        ]  # so it's the same whether you are looking at FG, MFG, or FI
        if len(self.job_id) < 3 and not global_pipegraph.allow_short_filenames:
            raise ValueError(
                "This is probably not the filename you intend to use: {}.".format(self)
                + " Use a longer filename or set graph.allow_short_filenames"
            )

    def output_needed(self, _ignored_runner):
        return True

    def run(self, _runner, historical_output):
        self.did_hash_last_run = False  # for testing.
        if not self.file.exists():
            raise FileNotFoundError(f"{self.file} did not exist")
        stat = self.file.stat()
        if not historical_output:
            self.did_hash_last_run = "no history"
            return {self.outputs[0]: self.calculate(self.file, stat)}
        else:
            mtime_the_same = int(stat.st_mtime) == historical_output[
                self.outputs[0]
            ].get("mtime", -1)
            size_the_same = stat.st_size == historical_output[self.outputs[0]].get(
                "size", -1
            )
            if mtime_the_same and size_the_same:
                return historical_output
            else:
                # log_info("File changed -> recalc")
                # log_info(f"{historical_output}, ")
                # log_info(f"mtime: {int(stat.st_mtime)}, size: {stat.st_size}")
                # log_info(f"mtime the same: {mtime_the_same}")
                # log_info(f"size the same: {size_the_same}")
                self.did_hash_last_run = (mtime_the_same, size_the_same)
                return {self.outputs[0]: self.calculate(self.file, stat)}

    def compare_hashes(self, old_hash, new_hash):
        return new_hash["hash"] == old_hash.get("hash", "")

    def extract_strict_hash(self, a_hash) -> bytes:
        return a_hash["hash"].encode("utf-8")


class ParameterInvariant(_InvariantMixin, Job):
    job_kind = JobKind.Invariant

    def __new__(cls, job_id, *args, **kwargs):
        if isinstance(job_id, Path):
            job_id = str(job_id)
        return _dedup_job(cls, "PI" + job_id)

    def __init__(self, job_id, parameters):
        if isinstance(job_id, Path):
            job_id = str(job_id)
        job_id = "PI" + job_id
        parameters = self.freeze(parameters)
        if hasattr(self, "parameters"):
            if parameters != self.parameters:
                raise exceptions.JobRedefinitionError(
                    f"Parameterinvariant with differing parameters {job_id}, was: {self.parameters}, now: {parameters}"
                )
        self.parameters = parameters
        super().__init__([job_id])

    def output_needed(self, _ignored_runner):
        return True

    def run(self, _runner, _historical_output):
        return {self.outputs[0]: str(self.parameters)}

    @staticmethod
    def freeze(obj):
        """Turn dicts into tuples of (key,value),
        lists into tuples, and sets
        into frozensets, recursively - useful
        to get a hash value..
        """

        if callable(obj):
            raise TypeError(
                "ParamaterInvariants do not store Functions. Use FunctionInvariant for that"
            )
        return DeepHash(obj, hasher=hashers.hash_str)[obj]

    def extract_strict_hash(self, a_hash) -> bytes:
        return str(ParameterInvariant.freeze(a_hash)).encode("utf-8")


class ValuePlusHash:
    """Wrapper to signal AttributeLoading/DataLoadingJob that you have already calculated a hash on this"""

    def __init__(self, value, hexdigest):
        self.value = value
        self.hexdigest = hexdigest


def _hash_object(obj):
    if isinstance(obj, str):
        my_hash = hashers.hash_bytes(obj.encode("utf-8"))
    elif isinstance(obj, bytes):
        my_hash = hashers.hash_bytes(obj)
    elif isinstance(
        obj, (int, float, complex)
    ):  # for these types, the build in hash should be good enough. This also covers numpy floats
        # todo: does it vary across python versionS?
        # todo: these do not get salted. at least up to 3.8..
        # todo: probably would be better to choose something deterministic...
        # but also lot of work.
        my_hash = str(hash(obj))  # since the others are also strings.
    elif isinstance(obj, ValuePlusHash):
        my_hash = obj.hexdigest
        obj = obj.value
    else:
        my_hash = DeepHash(obj, hasher=hashers.hash_str)[obj]
        # raise ValueError(f"Could not derive a hash for {type(obj)}")
    return obj, my_hash


class DataLoadingJob(Job):
    """A job that manipulates the currently running python program.

    Note that these don't run if they have no dependents.

    Also note that if this runs, and the load_function returns None,
    it *will* invalidate it's downstreams, possibly triggering reruns
    you did not want.

    The answer is to return something that's we can feed to
    hasher.hash_bytes - bytes or a string.

    Alternatively, if you have a hash handy, you may return it wrapped in
    an ValuePlusHash.
    """

    job_kind = JobKind.Loading

    def __new__(cls, job_id, *args, **kwargs):
        if isinstance(job_id, Path):
            job_id = str(_normalize_path(job_id))
        return _dedup_job(cls, job_id)

    def __init__(self, job_id, load_function, 
            resources: Resources = Resources.SingleCore,
            depend_on_function=True):
        if isinstance(job_id, Path):
            job_id = str(_normalize_path(job_id))
        self.depend_on_function = depend_on_function
        self.load_function = load_function
        super().__init__([job_id], resources=resources)

    def readd(self):
        super().readd()
        if self.depend_on_function:
            self._handle_function_dependency(self.load_function)

    def run(self, runner, historical_output):
        load_res = self.load_function()

        log_trace(
            f"dl {self.job_id} - historical: {historical_output.get(self.outputs[0], False)}"
        )
        log_trace(f"dl {self.job_id} - {escape_logging(historical_output)}")
        if load_res is None:
            log_warning(
                f"DataLoadingJob {self.job_id} returned None - downstreams will be invalidated whenever this runs."
            )
            try:
                my_hash = historical_output.get(self.outputs[0], 0) + 1
            except TypeError:
                my_hash = 0  # start over. Historical can't have been 0
            # so the downstream get's invalidated when ever this runs. This is a safe,
            # but potentially wasteful
        else:
            _, my_hash = _hash_object(load_res)  # could be a ValuePlusHash
        return {self.outputs[0]: my_hash}

    def extract_strict_hash(self, a_hash) -> bytes:
        return a_hash.encode("utf-8")


def CachedDataLoadingJob(
    cache_filename,
    calc_function,
    load_function,
    depend_on_function=True,
    resources: Resources = Resources.SingleCore,
):
    cache_filename = Path(cache_filename)
    # early func definition & checking so we don't create a calc job if the load job will fail

    def do_cache(output_filename):  # pragma: no cover - spawned job
        with open(output_filename, "wb") as op:
            pickle.dump(calc_function(), op, pickle.HIGHEST_PROTOCOL)

    def load():
        try:
            with open(cache_filename, "rb") as op:
                raw = op.read()
                res = pickle.loads(raw)
                load_function(res)
                return raw
        except pickle.UnpicklingError as e:
            raise pickle.UnpicklingError(
                f"Unpickling error in file {cache_filename}", e
            )

    _mark_function_wrapped(load, load_function, "load")
    _mark_function_wrapped(do_cache, calc_function, "calc")

    cache_job = FileGeneratingJob(
        cache_filename,
        do_cache,
        depend_on_function=depend_on_function,
        resources=resources,
    )

    load_job = DataLoadingJob(
        "load_" + cache_job.job_id,
        load,
        depend_on_function=depend_on_function,
    )
    load_job.depends_on(cache_job)
    # do this after you have sucessfully created both jobs
    return CachedJobTuple(load_job, cache_job)


class AttributeLoadingJob(
    Job
):  # Todo: refactor with DataLoadingJob. Also figure out how to hash the result?
    job_kind = JobKind.Loading

    def __new__(cls, job_id, *args, **kwargs):
        return _dedup_job(cls, job_id)

    def __init__(
        self, job_id, object, attribute_name, data_function, depend_on_function=True,
        resources: Resources = Resources.SingleCore,

    ):
        from . import global_pipegraph

        if global_pipegraph.run_mode.is_strict():
            if hasattr(self, "object"):  # inited before
                if self.object != object:
                    raise exceptions.JobRedefinitionError(job_id, "object changed")
                elif self.attribute_name != attribute_name:
                    raise exceptions.JobRedefinitionError(
                        job_id, "attribute_name changed"
                    )
                elif not FunctionInvariant.functions_equal(
                    self.callback, data_function
                ) or (
                    hasattr(
                        self.callback, "wrapped_function"
                    )  # CachedAttributeLoadingJob
                    and not FunctionInvariant.functions_equal(
                        self.callback.wrapped_function, data_function.wrapped_function
                    )
                ):
                    raise exceptions.JobRedefinitionError(job_id, "callback changed")

        if not isinstance(attribute_name, str):
            raise ValueError("attribute_name was not a string")
        self.depend_on_function = depend_on_function
        self.object = object
        self.attribute_name = attribute_name
        self.callback = data_function
        super().__init__([job_id], resources=resources)
        self.cleanup_job_class = _AttributeCleanupJob

    def readd(self):  # Todo: refactor
        super().readd()
        if self.depend_on_function:
            self._handle_function_dependency(self.callback)

    def run(self, _runner, historical_output):
        value = self.callback()
        if value is None:
            try:
                hash = historical_output.get(self.outputs[0], 0) + 1
            except TypeError:
                hash = 0
        else:
            value, hash = _hash_object(value)
        setattr(self.object, self.attribute_name, value)
        return {self.outputs[0]: hash}

    def extract_strict_hash(self, a_hash) -> bytes:
        return a_hash.encode("utf-8")


def CachedAttributeLoadingJob(
    cache_filename,
    object,
    attribute_name,
    data_function,
    depend_on_function=True,
    resources: Resources = Resources.SingleCore,
):
    cache_filename = Path(cache_filename)

    def do_cache(output_filename):  # pragma: no cover
        with open(output_filename, "wb") as op:
            pickle.dump(data_function(), op, pickle.HIGHEST_PROTOCOL)

    def load(object=object, attribute_name=attribute_name):
        try:
            with open(cache_filename, "rb") as op:
                raw = op.read()
                return ValuePlusHash(
                    pickle.loads(raw), hashers.hash_bytes(raw)
                )  # for hashing
        except pickle.UnpicklingError as e:
            raise pickle.UnpicklingError(
                f"Unpickling error in file {cache_filename}", e
            )

    _mark_function_wrapped(do_cache, data_function, "data")
    _mark_function_wrapped(load, data_function, "data")

    cache_job = FileGeneratingJob(
        cache_filename,
        do_cache,
        depend_on_function=depend_on_function,
        resources=resources,
    )
    load_job = AttributeLoadingJob(
        "load_" + cache_job.job_id,
        object,
        attribute_name,
        load,
        depend_on_function=depend_on_function,
    )
    load_job.depends_on(cache_job)
    return CachedJobTuple(load_job, cache_job)


class _AttributeCleanupJob(Job):
    """Jobs may register cleanup jobs that injected after their immediate downstreams.
    This encapsulates those

    """

    job_kind = JobKind.Cleanup

    def __new__(cls, parent_job):
        return _dedup_job(cls, f"CleanUp:{parent_job.job_id}")

    def __init__(self, parent_job):
        Job.__init__(self, [f"CleanUp:{parent_job.job_id}"], Resources.RunsHere)
        self.parent_job = parent_job  # what are we cleaning up?
        # and that is necessary for the invalidation to do it's thing
        # but the real parent will the one we read the files from

    def run(self, _ignored_runner, _historical_output):
        delattr(self.parent_job.object, self.parent_job.attribute_name)

        return {self.outputs[0]: None}  # todo: optimize this awy?


class JobGeneratingJob(Job):
    """A job generating job runs once per ppg.Graph.run(),
    and may alter the graph in essentially any way. The changes are ignored
    until the first run finishes, then the whole graph is rerun.

    This has has to run every time to actually create its downstream jobs,
    for example when the first pipegraph run crashed,
    and you're rerunning the whole program.

    If you depend on a JobGeneratingJob your job will be invalidated
    every time the JobGeneratingJob runs.

    """

    job_kind = JobKind.JobGenerating

    def __new__(cls, job_id, *args, **kwargs):
        return _dedup_job(cls, job_id)

    def __init__(self, job_id, callback, depend_on_function=True):
        self.depend_on_function = depend_on_function
        self.callback = callback
        self.last_run_id = None
        super().__init__([job_id])

    def readd(self):  # Todo: refactor
        super().readd()
        if self.depend_on_function:
            self._handle_function_dependency(self.callback)

    def output_needed(self, runner):
        if runner.run_id != self.last_run_id:
            return True
        return False

    def run(self, runner, historical_output):
        log_trace(f"running jobgenerating {self.job_id}")
        self.last_run_id = runner.run_id
        self.callback()
        # todo: is this the right approach
        # should we maybe instead return a sorted list of new jobs
        # if you depend on this, you're going te be triggered
        # *all* the time. Well once per graph.run
        return {
            self.outputs[0]: historical_output.get(self.outputs[0], 0) + 1
        }  # so the downstream get's invalidated

    def extract_strict_hash(self, a_hash) -> bytes:
        return b""  # job generating jobs never influence the input of SharedMultiFileGeneratingJobs by fiat


def _save_plot(
    plot, output_filename, plot_render_args
):  # pragma: no cover - this happens in spawned jobs
    if (
        not hasattr(plot, "render")
        and not hasattr(plot, "save")
        and not hasattr(plot, "save_fig")
    ):
        raise exceptions.JobContractError(
            f"{output_filename}.plot_function did not return a plot object (needs to have as render/save/save_fig function)"
        )
    if hasattr(plot, "pd"):  # dppd special..
        plot = plot.pd
    render_args = {}
    if "width" not in render_args and hasattr(plot, "width"):
        render_args["width"] = plot.width
    if "height" not in render_args and hasattr(plot, "height"):
        render_args["height"] = plot.height
    render_args.update(getattr(plot, "render_args", {}))
    render_args.update(plot_render_args)
    if hasattr(plot, "render"):
        plot.render(output_filename, **render_args)
    elif hasattr(plot, "save"):
        plot.save(output_filename, **render_args)
    elif hasattr(plot, "savefig"):
        plot.savefig(output_filename, **render_args)
    else:  # pragma: no cover
        raise NotImplementedError("Don't know how to handle this plotjob")


def PlotJob(  # noqa:C901
    output_filename,
    calc_function,
    plot_function,
    render_args=None,
    cache_dir="cache",
    depend_on_function=True,
    cache_calc=True,
    create_table=True,
):  # noqa:C901
    """Return a tuple of 3 jobs, the last two entries might be none.

    The first one is always a FileGeneratingJob
        around a wrapped plot_function, creating the output filename.

    If cache_calc is set, the second one is a CachedAttributeLoadingJob
    (wich again is a tuple, load_job, calc_job),
    loading a .data_ member on the first job returned.

    If create_table is set, the third one is a FileGeneratingJob
    writing (output_filename + '.tsv').
    """
    from . import global_pipegraph

    if render_args is None:
        render_args = {}
    output_filename = Path(output_filename)

    allowed_suffixes = (".png", ".pdf", ".svg")
    if not (output_filename.suffix in allowed_suffixes):
        raise ValueError(
            f"Don't know how to create a {output_filename.suffix} file, must end on one of {allowed_suffixes}."
        )

    def do_plot(output_filename):  # pragma: no cover - runs in spawned job
        if not hasattr(plot_job, "data_"):
            plot_job.data_ = calc_function()
        plot = plot_function(plot_job.data_)
        _save_plot(plot, output_filename, render_args)

    _mark_function_wrapped(do_plot, plot_function, "plot")

    plot_job = FileGeneratingJob(
        output_filename, do_plot, depend_on_function=depend_on_function
    )
    output_filename = plot_job.files[0]  # that's resolved!
    param_job = ParameterInvariant(output_filename, render_args)
    plot_job.depends_on(param_job)

    def _call_result():
        if not hasattr(plot_job, "data_"):
            plot_job.data_ = calc_function()
        return plot_function(plot_job.data_)

    plot_job._call_result = _call_result

    cache_filename = Path(cache_dir) / output_filename
    if cache_calc:

        def do_cache():  # pragma: no cover - runs in spawned job
            import pandas as pd

            Path(output_filename.parent).mkdir(exist_ok=True, parents=True)
            df = calc_function()
            if not isinstance(df, pd.DataFrame):
                do_raise = True
                if isinstance(df, dict):  # might be a list dfs...
                    do_raise = False
                    for x in df.values():
                        if not isinstance(x, pd.DataFrame):
                            do_raise = True
                            break
                if do_raise:
                    raise exceptions.JobContractError(
                        "%s.calc_function did not return a DataFrame (or dict of such), was %s "
                        % (output_filename, str(df.__class__))
                    )
            return df

        _mark_function_wrapped(do_cache, calc_function, "calc")

        cache_filename.parent.mkdir(exist_ok=True, parents=True)
        cache_job = CachedAttributeLoadingJob(
            cache_filename,
            plot_job,
            "data_",
            do_cache,
            depend_on_function=depend_on_function,
        )
        Job.depends_on(
            plot_job, cache_job.load
        )  # necessary because the ppg1 compatibility layer messes with this
    else:

        cache_job = None
        if str(cache_filename) in global_pipegraph.jobs:
            raise ValueError(
                "Redefining PlotJob and removing caching in the process "
                "not supported. Once cached, always cached. "
                "At least until somebody fixes job deletion "
                "or makes this a warning instead"
            )

    table_filename = output_filename.with_suffix(output_filename.suffix + ".tsv")
    if create_table:

        def dump_table(output_filename):  # pragma: no cover - runs in spawned job
            import pandas as pd

            if not hasattr(plot_job, "data_"):
                plot_job.data_ = calc_function()

            if isinstance(plot_job.data_, pd.DataFrame):
                plot_job.data_.to_csv(output_filename, sep="\t")
            else:
                with open(output_filename, "w") as op:
                    for key, dataframe in plot_job.data_.items():
                        op.write("#%s\n" % key)
                        dataframe.to_csv(op, sep="\t")

        table_job = FileGeneratingJob(
            table_filename, dump_table, depend_on_function=depend_on_function
        )
        if cache_calc:
            table_job.depends_on(cache_job.load)
    else:
        table_job = None
        if str(table_filename) in global_pipegraph.jobs:
            raise ValueError(
                "Redefining PlotJob and removing table in the process "
                "not supported. Once cached, always cached. "
                "At least until somebody fixes job deletion "
                "or makes this a warning instead"
            )

    def add_another_plot(
        output_filename, plot_function, render_args=None, depend_on_function=True
    ):
        if render_args is None:
            render_args = {}

        def do_plot_another_plot(output_filename):  # pragma: no cover
            if not hasattr(plot_job, "data_"):
                plot_job.data_ = calc_function()
            plot = plot_function(plot_job.data_)
            _save_plot(plot, output_filename, render_args)

        j = FileGeneratingJob(
            output_filename, do_plot_another_plot, depend_on_function=depend_on_function
        )
        if cache_calc:
            j.depends_on(cache_job.load)
        return j

    plot_job.add_another_plot = add_another_plot

    return PlotJobTuple(plot_job, cache_job, table_job)


class SharedMultiFileGeneratingJob(MultiFileGeneratingJob):
    """A shared MultiFileGeneratingJob.

    Sharing means that this can be produced by multiple pypipegraphs,
    each with it's own history.

    The trick here is that we hash the inputs into one hash,
    by which we name the target directory.

    But that's actually a symlink to another directory,
    named by the output hashes!

    That way, changing the input will trigger a recalc.
    But we will not store the output multiple times
    if it's identical.

    Multiple graphs, with the same input hashes,
    will not do the work twice. (Provided their runtimes don't overlap).
    Otherwise calc will happen twice, but one thrown away ( if identical )
    or with an exception ( if not identical: input-defines-output invariant violated).

    Broken builds get removed by default.

    Each pipegraph also logs it's use (on first usage & rebuild) and by default
    cleans up no longer used outputs / symlinks.

    """

    _log_local_lock = Lock()
    log_filename = "SharedMultiFileGeneratingJobs.json"
    run_only_post_validation = True

    def __new__(cls, output_dir_prefix, files, *_args, **_kwargs):
        output_dir_prefix = Path(output_dir_prefix)
        files = cls._validate_files_argument(files, allow_absolute=True)[0]
        files = [output_dir_prefix / "__never_placed_here__" / f for f in files] + [
            output_dir_prefix
        ]
        return Job.__new__(cls, [str(x) for x in files])

    def __init__(
        self,
        output_dir_prefix: Path,
        files: List[Path],  # todo: extend type attribute to allow mapping
        generating_function: Callable[[List[Path]], None],
        resources: Resources = Resources.SingleCore,
        depend_on_function: bool = True,
        empty_ok: bool = True,
        always_capture_output: bool = True,
        remove_build_dir_on_error: bool = True,
        remove_unused: bool = True,
    ):
        self.output_dir_prefix = Path(output_dir_prefix)

        self.build_dir = self.output_dir_prefix / "build"
        self.output_dir = self.output_dir_prefix / "done"  # done outputs
        self.input_dir = self.output_dir_prefix / "by_input"  # symlinks
        self.usage_dir = self.output_dir_prefix / "used_by"  # symlinks
        self.build_dir.mkdir(exist_ok=True, parents=True)
        self.output_dir.mkdir(exist_ok=True)
        self.input_dir.mkdir(exist_ok=True)
        self.usage_dir.mkdir(exist_ok=True)

        self.generating_function = self._validate_func_argument(generating_function)
        self.depend_on_function = depend_on_function

        self.files, self._lookup = self._validate_files_argument(
            files, allow_absolute=True
        )
        self.org_files = [
            (self.output_dir_prefix / "__never_placed_here__" / f) for f in self.files
        ]
        if self._lookup:
            self._lookup = {
                k: self.org_files[self.files.index(v)]
                for (k, v) in self._lookup.items()
            }
        self.files = self.org_files[:]

        if len(self.files) != len(set(self.files)):
            raise ValueError(
                "Paths were present multiple times in files argument. Fix your input"
            )

        init_files = self.files + [self.output_dir_prefix]
        Job.__init__(self, [str(x) for x in init_files], resources)

        self.empty_ok = empty_ok
        self.always_capture_output = always_capture_output
        self.stdout = "not captured"
        self.stderr = "not captured"
        self.pid = None
        self.remove_build_dir_on_error = remove_build_dir_on_error
        self.remove_unused = remove_unused
        self.building = False

    def depends_on(self, *args, **kwargs):
        # make sure that we throw away the _target_folder if the dependency list changes.
        if hasattr(self, "_target_folder"):
            delattr(self, "_target_folder")
        return super().depends_on(*args, **kwargs)

    @property
    def target_folder(self):
        """read the target folder as of the last ppg run,
        so you can find your actual files
        """
        if hasattr(self, "_target_folder"):
            return self._target_folder
        else:
            raise AttributeError(
                "Target folder is only available after a run (and disappears on .depends_on)"
            )

    def run(self, runner, historical_output):
        import socket
        from . import global_pipegraph

        by_input_key = self._derive_output_name(runner)
        log_job_trace(f"{self.job_id} run input key {by_input_key}")
        self._target_folder = self.input_dir / by_input_key
        if self._target_folder.exists() and not self._target_folder.is_symlink():
            raise exceptions.JobEvaluationFailed(
                f"The target folder was no symlink {self._target_folder}"
            )

        fns = [self._map_filename(fn).absolute() for fn in self.org_files]
        existing_files = [
            self._map_filename(fn).absolute().exists() for fn in self.org_files
        ]
        symlink = self.input_dir / by_input_key
        if all(existing_files):
            log_job_trace(
                f"{self.output_dir_prefix} -  all files existed - just hashing"
            )
            self.files = [self._map_filename(fn) for fn in self.org_files]
            mfg_res = None
            did_build = False
        elif not any(existing_files):
            did_build = True
            log_job_trace(f"{self.job_id} - output files missing, building them")
            # temp replace for the actual run
            self._target_folder = (
                self.build_dir / f"{socket.gethostname()}_{os.getpid()}_{time.time()}"
            )
            self._target_folder.mkdir(exist_ok=True, parents=True)
            log_job_trace(
                f"target folder during build {self._target_folder} {os.getpid()}"
            )
            self.building = True
            try:
                mfg_res = MultiFileGeneratingJob.run(self, runner, historical_output)
            except:  # noqa:E722
                if self.remove_build_dir_on_error:
                    shutil.rmtree(self._target_folder)
                raise
            self.building = False

            # calculate output shash
            output_key = self._hash_hashes(mfg_res, runner)
            store_folder = self.output_dir / output_key

            if (
                store_folder.exists()
            ):  # either a previous partial, or parallel build. let's keep that one
                shutil.rmtree(
                    self._target_folder
                )  # pragma: no cover - too much work to figure out how to trigger it
            else:
                self._target_folder.rename(store_folder)

            try:
                symlink.symlink_to(
                    Path("..")
                    / self.output_dir.relative_to(self.output_dir_prefix)
                    / output_key,
                )
            except FileExistsError as e:  # existed, or race condition... symlink.exist()   is lying.
                if not symlink.is_symlink():  # pragma: no cover
                    # can only happen in race condition, other wise captured in output_needed
                    # we really expect this to be a symlink to be a symlink,k?
                    raise ValueError(
                        f"{symlink.absolute()} was not a symlink. Fubared output directory."
                    )
                if symlink.resolve().absolute() != store_folder.absolute():
                    raise exceptions.JobContractError(
                        f"{self.job_id} created two different outputs from identical inputs. "
                        "This is a violation of the concept of SharedMultiFileGeneratingJobs - the output must be 100% determined by the input"
                    )
            # weg get here, everything is peachy

            self._target_folder = symlink  # so the downstream knows where to look
            self.files = [self._map_filename(fn) for fn in self.org_files]

        else:
            self._raise_partial_result_exception()
        missing = [x for x in fns if not x.exists()]
        if missing:
            raise ValueError(
                "missing output files - did somebody go and delete them?!", missing
            )

        # now log that we're the ones using this.
        # our key is a hash of our history path.
        abs_hd = str(global_pipegraph.get_history_filename().absolute())
        usage_dir_hash = hashlib.sha512(abs_hd.encode("utf-8")).hexdigest()
        log_job_trace(f"usage_dir_hash {usage_dir_hash}")
        lookup_file = self.usage_dir / (usage_dir_hash + ".source")
        lookup_text = abs_hd
        if lookup_file.exists():
            if lookup_file.read_text() != lookup_text:  # pragma: no cover
                raise ValueError(
                    "There was a output log directory collision. "
                    "I really did not expect tha to happen with our sha512 hash. "
                    "But it is possible. "
                    "Rename your project dir / history file, I suppose?"
                )
        else:
            lookup_file.write_text(abs_hd)

        used_symlink = self.usage_dir / (usage_dir_hash + ".uses")
        target = (
            Path("..")
            / self.input_dir.relative_to(self.output_dir_prefix)
            / by_input_key
        )
        try:
            used_symlink.symlink_to(target)
        except FileExistsError as e:  # existed, or race condition...
            log_job_trace(f"unlinking symlink that already existed {used_symlink}")
            used_symlink.unlink()
            used_symlink.symlink_to(target)

        # log_error(f"{used_symlink.resolve().absolute()} {symlink.resolve().absolute()}")
        # that's paranoia, and slowish...
        # assert used_symlink.resolve().absolute() == symlink.resolve().absolute()
        log_job_trace(f"output symlink {symlink}")
        # now let's apply the same logic we use in MultiFileGeneratingJobs.
        # same mtime, same size as the last time we saw this
        # no calcualting the hash again.
        # even if we're getting triggered and triggered by a JobGeneratingJob...
        # (which we ignore in input, by virtuo of extract_strict_hash being constant)
        res = {}
        for of in self.org_files:
            if mfg_res:
                res[str(of)] = mfg_res[str(of)]
            else:
                h = None
                of_on_disk = self._map_filename(of)
                if str(of) in historical_output:
                    stat = of_on_disk.stat()
                    hist = historical_output.get(str(of))
                    if (
                        hist is not None
                        and hist["mtime"] == int(stat.st_mtime)
                        and hist["size"] == stat.st_size
                    ):
                        h = hist
                if h is None:
                    h = hashers.hash_file(of_on_disk)
                res[str(of)] = h

        res[str(self.output_dir_prefix)] = {
            "hash": by_input_key,
            "size": 0,
            "mtime": time.time(),
        }  # so we can detect if the target changed

        # we clean up if we build,
        # or if we had no history
        if did_build or not runner.job_states[self.job_id].historical_output:
            self._cleanup(runner)
        self._log_local_usage(by_input_key)
        return res

    def _log_local_usage(self, key):
        """Write the input key we used to a log file,
        so that non-ppg-interactive stuff may read it back
        and find the files"""
        from . import global_pipegraph

        fn = global_pipegraph.history_dir / SharedMultiFileGeneratingJob.log_filename
        with SharedMultiFileGeneratingJob._log_local_lock:
            if fn.exists():
                keys = json.loads(fn.read_text())
            else:
                keys = {}
            keys[str(self.output_dir_prefix)] = key
            fn.write_text(json.dumps(keys))

    def _raise_partial_result_exception(self):
        raise exceptions.JobContractError(
            f"{self.job_id} some result files existed, some didn't. "
            "In the normal operation of SharedMultiFileGeneratingJob this is impossible."
            " This means your callback is not deterministic "
            ", ie your input does not define your output "
            "or something has gone terribly wrong in an unforseen way. "
            "for example, files were deleted from an output folder"
        )

    def _map_filename(self, filename):
        parts = filename.parts
        # if not hasattr(self, "_target_folder"):
        # tf = "__never_placed_here__"
        # else:
        tf = self.target_folder.name
        out_parts = []
        for x in parts:
            if x == "__never_placed_here__":
                if self.building:
                    out_parts.append("build")
                else:
                    out_parts.append("by_input")
                out_parts.append(tf)
            else:
                out_parts.append(x)

        return Path(*out_parts)

    def get_input(self):  # todo: fold in?
        # if self._single_file:
        # return self._map_filename(self.files[0])
        # else:
        if self._lookup:
            return (
                {k: self._map_filename(f) for (k, f) in self._lookup.items()},
                self.target_folder,
            )
        else:
            return ([self._map_filename(f) for f in self.files], self.target_folder)

    def output_needed(self, runner):
        # output needed is called at the very beginning
        # of the ppg run.
        # we nod'n know the hashed folder names yet.
        return True
        # log_job_trace(f"{self.job_id} - SharedMultiFileGeneratingJob output needed?")

        # if runner.job_states[self.job_id].validation_state == ValidationState.Unknown:
        #     log_job_trace("output needed = False because invalidated not done")
        #     return False
        # log_job_trace(
        #     f"{self.job_id} invalidation state {runner.job_states[self.job_id].validation_state}"
        # )
        # by_input_name = self._derive_output_name(runner)
        # self._target_folder = self.input_dir / by_input_name
        # if not self._target_folder.exists():
        #     log_job_trace(f"target folder did not exist {self._target_folder}")
        #     return True

        # for fn in self.org_files:
        #     if not self._map_filename(fn).exists():  # pragma: no cover
        #         # this case is unlikely ( same hash, but additional outputfiles?)
        #         # but it could happen...
        #         self._raise_partial_result_exception()
        #     if (
        #         str(fn) not in runner.job_states[self.job_id].historical_output
        #     ):  # we must record the hashes
        #         log_job_trace("hash did not exist")
        #         return True
        #     # else:
        #     # we have a hash... but our inputs may have changed us to a new target folder
        #     # but only if they changed. which is handled by invalidation
        #     # so we can safely ignore this case
        #     # pass # pragma: no cover

        # log_job_trace("output needed = False")
        # return False

    def _call_result(self):
        if self._lookup:
            return {k: self._map_filename(f) for (k, f) in self._lookup.items()}
        else:
            return [self._map_filename(f) for f in self.files]

    def _derive_output_name(self, runner):
        """Given the set of inputs, get as the key for by_input"""
        if (
            runner.job_states[self.job_id].validation_state == ValidationState.Unknown
        ):  # pragma: no cover
            raise ValueError(
                "deriving output name on SharedMultiFileGeneratingJob "
                "before all parent hashes are available is impossible"
            )

        input_names = runner.job_inputs[self.job_id]
        if not input_names:
            return "no_input"
        actual_input = runner.job_states[self.job_id].updated_input
        return self._hash_hashes(actual_input, runner)

    def extract_strict_hash(self, a_hash) -> bytes:
        return a_hash["hash"].encode("utf-8")

    def _hash_hashes(self, hashes, runner):
        """The problem at this point is that the hashes
        are not strictly what-the-input-depends-on
        but also things we use to
        omit recalculating (expensive) hashes if possible,
        e.g. size/mtime for FileInvariants or
        per-python-version-bytecodes for FunctionInvariants.

        We have to extract just the relevant data,
        and for that we need to lookup the actual jobs.
        """
        hasher = hashlib.sha512()
        for (key, value) in sorted(hashes.items()):
            job_id = runner.job_graph.outputs_to_job_ids[key]
            job = runner.jobs[job_id]
            if isinstance(job, SharedMultiFileGeneratingJob) and key == str(
                job.output_dir_prefix
            ):
                continue
            hasher.update(key.encode("utf-8"))
            hasher.update(job.extract_strict_hash(value))
        return hasher.hexdigest()

    def _cleanup(self, runner):
        """Remove outputs / symlinks that have no longer entries in the used folder"""
        if self.remove_unused:
            used_symlinks = set()
            used_outputs = set()
            for fn in self.usage_dir.glob("*.uses"):
                symlink = Path(os.readlink(fn))
                used_symlinks.add(symlink.name)
                target = Path(os.readlink(self.input_dir / symlink))
                used_outputs.add(target.name)

            log_job_trace(f"symlinks {used_symlinks}")
            log_job_trace(f"outputs {used_outputs}")
            for fn in self.input_dir.glob("*"):
                if fn.name not in used_symlinks:
                    log_job_trace(f"unlink fn {fn}")
                    fn.unlink()
                else:
                    log_job_trace(f"keeping fn {fn}")

            for fn in self.output_dir.glob("*"):
                if fn.name not in used_outputs:
                    log_job_trace(
                        f"Identified {fn} as no longer in use by any PPG. Removing"
                    )
                    log_job_trace(f"rmtree fn {fn}")
                    shutil.rmtree(fn)
                else:
                    log_job_trace(f"keeping fn {fn}")

    def find_file(self, output_filename):  # for compability with ppg1.
        "Search for a file named output_filename in the job's known created files"
        return self[output_filename]

    def __getitem__(self, key):
        if not self._lookup:
            if isinstance(key, int):
                return self._map_filename(self.org_files[key])
            else:
                for org_fn in self.org_files:
                    fn = str(org_fn)
                    fn = fn[
                        fn.find("__never_placed_here__/")
                        + len("__never_placed_here__/") :
                    ]
                    if fn == key:
                        return self._map_filename(org_fn)
                else:
                    search = [
                        fn[
                            fn.find("__never_placed_here__/")
                            + len("__never_placed_here__/") :
                        ]
                        for fn in [str(fn) for fn in self.org_files]
                    ]
                    raise KeyError(
                        f"Could not find {key} in {self.job_id}. Available {search}"
                    )
        return self._map_filename(self._lookup[key])
