from __future__ import annotations
import traceback
import pickle
import multiprocessing
import os
import dis
import re
import sys
import inspect
import types
from typing import Union, List, Dict, Optional, Tuple, Callable
from loguru import logger  # noqa:F401
from pathlib import Path
from io import StringIO
from collections import namedtuple
from . import hashers, exceptions
from .enums import JobKind, Resources
from .util import escape_logging

module_type = type(sys)

non_chdired_path = Path(".").absolute()
python_version = tuple(sys.version_info)[:2]  # we only care about major.minor

DependsOnInvariant = namedtuple("DependsOnInvariant", ["invariant", "self"])
CachedJobTuple = namedtuple("CachedJobTuple", ["load", "calc"])


class Job:
    job_id: int
    historical: Optional[Tuple[str, Dict[str, str]]]

    def __init__(
        self,
        outputs: Union[str, List[str], Dict[str, str]],
        resources: Resources = Resources.SingleCore,
    ):

        from . import global_pipegraph

        self.resources = resources
        if isinstance(outputs, str):
            self.outputs = [outputs]
            self.mapped_outputs = {}
        elif isinstance(outputs, dict):
            self.outputs = outputs.values()
            self.mapped_outputs = outputs
        elif isinstance(outputs, list):
            self.outputs = outputs
            self.mapped_outputs = {}
        else:
            raise TypeError("Invalid output definition.")
        self.outputs = sorted([str(x) for x in self.outputs])
        self.job_id = ":::".join(self.outputs)
        self.dependency_callbacks = []
        self.readd()

    def readd(self):
        """Readd this job to the current global pipegraph
        (possibly the *new* global pipegraph).
        Without any dependencies!
        """
        from . import global_pipegraph

        global_pipegraph.add(self)

    def use_resources(self, resources: Resources):
        self.resources = resources
        return self

    def depends_on(
        self,
        other_job: Union[Union[str, Job], List[Union[str, Job]]] = None,
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

        if isinstance(other_job, list):
            for x in other_job:
                self.depends_on(x)
        else:
            if isinstance(other_job, Job):
                o_job = other_job
                o_inputs = other_job.outputs
            elif other_job is None:
                return self
            elif hasattr(other_job, '__call__'):
                self.dependency_callbacks.append(other_job)
                return self
            else:
                o_job = global_pipegraph.jobs[
                    global_pipegraph.outputs_to_job_ids[other_job]
                ]
                o_inputs = [other_job]
            if o_job.job_id == self.job_id:
                raise exceptions.NotADag("Job can not depend on itself")
            if global_pipegraph.has_edge(self, o_job):
                raise exceptions.NotADag(
                    f"{o_job.job_id} is already upstream of {self.job_id}, can't be downstream as well (cycle)"
                )

            global_pipegraph.add_edge(o_job, self)
            global_pipegraph.job_inputs[self.job_id].update(o_inputs)
        if other_jobs:
            for o in other_jobs:
                self.depends_on(o)
        return self

    def is_temp_job(self):
        return False

    def output_needed(self, _ignored_runner):
        False

    def invalidated(self):
        """Inputs changed - nuke outputs etc"""
        pass

    @classmethod
    def compare_hashes(cls, old_hash, new_hash):
        return old_hash == new_hash

    def depends_on_func(self, function, name=None):
        """Create a function invariant.
        Return a NamedTumple (function_invariant, function_invariant, self)
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


class InitialJob(Job):
    job_kind = JobKind.Invariant

    def __init__(self):
        Job.__init__(self, "%%%initial%%%")

    def run(self, _ignored_runner, _historical_output):
        return {self.job_id: "I"}


class _DownstreamNeedsMeChecker(Job):
    job_kind = JobKind.Invariant

    def __init__(self, job_to_check):
        self.job_to_check = job_to_check
        Job.__init__(self, f"_DownstreamNeedsMeChecker_{job_to_check.job_id}")

    def output_needed(self, _ignored_runner):
        return True

    def run(self, runner, _historical_output):
        if self.job_to_check.output_needed(runner):
            return {self.job_id: "ExplodePlease"}
        else:
            return {self.job_id: "IgnorePlease"}

    @classmethod
    def compare_hashes(cls, old_hash, new_hash):
        if new_hash == "ExplodePlease":
            return False
        if new_hash == "IgnorePlease":
            return True
        raise NotImplementedError("Should not be reached")


class MultiFileGeneratingJob(Job):
    job_kind = JobKind.Output

    def __init__(
        self,
        files: List[Path],  # todo: extend type attribute to allow mapping
        generating_function: Callable[List[Path]],
        resources: Resources = Resources.SingleCore,
        depend_on_function: bool = True,
    ):

        self.generating_function = generating_function
        self.depend_on_function = depend_on_function
        Job.__init__(self, files, resources)
        self.files = [Path(x) for x in self.outputs]
        self._single_file = False

    def readd(self):
        if self.depend_on_function:
            func_invariant = FunctionInvariant(self.generating_function, self.job_id)
            self.depends_on(func_invariant)
        super().readd()

    def run(self, runner, _historical_output):
        for fn in self.files:  # we rebuild anyway!
            if fn.exists():
                fn.unlink()
        if self.resources in (
            Resources.SingleCore,
            Resources.AllCores,
            Resources.Exclusive,
        ):
            c = self.resources.to_number(runner.core_lock.max_cores)
            # logger.info(f'cores: {c}, max: {runner.core_lock.max_cores}')
            with runner.core_lock.using(c):
                # que = multiprocessing.Queue() # replace by pipe
                logger.job_trace(f"Forking for {self.job_id}")
                recv, sender = multiprocessing.Pipe(duplex=False)
                pid = os.fork()
                if pid == 0:
                    try:
                        try:
                            self.generating_function(self.get_input())
                            os._exit(0)  # go down hard
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
                    except Exception as e:
                        try:
                            tb = traceback.format_exc()
                            sender.send((e, tb))
                        except Exception as e:
                            print(f"FileGeneratingJob done, but send failed: {e}")
                            pass
                        os._exit(1)
                else:
                    _, waitstatus = os.waitpid(pid, 0)
                    if os.WIFEXITED(waitstatus):
                        # normal termination.
                        exitcode = os.WEXITSTATUS(waitstatus)
                        if exitcode != 0:
                            if recv.poll(1):
                                exception, tb_str = recv.recv()
                                raise exceptions.JobError(exception, tb_str)
                            else:
                                raise exceptions.JobContractError(
                                    f"Job {self.job_id} died but did not return an exception object"
                                )
                    else:
                        raise ValueError("Process died. Todo: extend tihs")

                # p = multiprocessing.Process(target=self._inner_run, args=())
                # p.start()
                # p.join()
                # res = que.get()
                # if isinstance(res, Exception):
                # raise res
        else:
            self.generating_function(self.get_input())
        missing_files = [x for x in self.files if not x.exists()]
        if missing_files:
            raise exceptions.JobContractError(
                f"Job {self.job_id} did not create the following files: {missing_files}"
            )

        res = {str(of): hashers.hash_file(of) for of in self.files}
        return res

    def _inner_run(self):
        try:
            self.generating_function(self.get_input())
            # que.put(None)
        except Exception as e:
            # que.put(e)
            pass

    def get_input(self):
        if self._single_file:
            return self.files[0]
        elif self.mapped_outputs:
            return self.mapped_outputs
        else:
            return self.files

    def output_needed(self, runner):
        for fn in self.files:
            if not fn.exists():
                return True
            # other wise we have no history, and the skipping will
            # break the graph execution
            if str(fn) not in runner.job_states[self.job_id].historical_output:
                return True
        return False

    def invalidated(self):
        for fn in self.files:
            logger.job_trace(f"unlinking {fn}")
            fn.unlink()


class FileGeneratingJob(MultiFileGeneratingJob):  # might as well be a function?
    def __init__(
        self,
        output_filename: Union[Path, str],
        generating_function: Callable[Path],
        resources: Resources = Resources.SingleCore,
        depend_on_function: bool = True,
    ):
        MultiFileGeneratingJob.__init__(
            self, [output_filename], generating_function, resources, depend_on_function
        )
        self._single_file = True


class MultiTempFileGeneratingJob(MultiFileGeneratingJob):
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

    def is_temp_job(self):
        return True

    def output_needed(
        self, runner
    ):  # yeah yeah yeah the temp jobs need to delegate to their downstreams dude!
        for downstream_id in runner.dag.neighbors(self.job_id):
            job = runner.jobs[downstream_id]
            if job.output_needed(runner):
                return True
        False

    def output_exists(self):
        for fn in self.files:
            if not fn.exists():
                return False
        return True


class TempFileGeneratingJob(MultiTempFileGeneratingJob):
    job_kind = JobKind.Temp

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

    def __init__(self, parent_job):
        Job.__init__(self, ["CleanUp:" + parent_job.job_id], Resources.RunsHere)
        self.parent_job = parent_job

    def run(self, _ignored_runner, _historical_output):
        for fn in self.parent_job.files:
            if fn.exists():
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
        return {
            "mtime": int(stat.st_mtime),
            "size": stat.st_size,
            "hash": hashers.hash_file(file),
        }


class FunctionInvariant(_InvariantMixin, Job, _FileInvariantMixin):
    job_kind = JobKind.Invariant

    def __init__(
        self, function, name=None
    ):  # must support the inverse calling with name, function, for compability to pypipegraph
        if isinstance(function, (str, Path)):
            name, function = function, name
        name = str(name)

        self.function = function
        name = (
            "FunctionInvariant:" + name
            if name
            else FunctionInvariant.func_to_name(function)
        )
        self.verify_arguments(name, function)
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
        if sf:  # we only have a source file for python functions.
            sf = Path(sf)
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
                    if ("source_file" in historical_output) and (
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
        logger.job_trace(
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

    @classmethod
    def compare_hashes(cls, old_hash, new_hash, python_version=python_version):
        if python_version in new_hash and python_version in old_hash:
            return new_hash[python_version] == old_hash[python_version]
        else:  # missing one python version, did the source change?
            # should we compare Closures here as well? todo
            return new_hash["source"] == old_hash["source"]

    def get_source_file(self):
        if self.is_python_function(self.function):
            try:
                sf = inspect.getsourcefile(self.function)
                if (
                    sf == sys.argv[0]
                ):  # at least python 3.8 does not have this absolute.
                    # might change with 3.9? https://bugs.python.org/issue20443
                    return non_chdired_path / sf
                else:
                    return Path(sf)
            except TypeError:
                pass
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
            return ~(hasattr(b, "__code__") and hasattr(b, "__closure__"))

    @staticmethod
    def function_to_str(func):
        if str(func).startswith("<built-in function"):
            return "%s" % func
        elif hasattr(func, "im_func") and (
            "cyfunction" in repr(func.im_func)
            or ("<built-in function" in repr(func.im_func))
        ):
            return "%s %i" % FunctionInvariant.get_cython_filename_and_line_no(func)
        else:
            return "%s %i" % (
                func.__code__.co_filename if func else "None",
                func.__code__.co_firstlineno if func else 0,
            )

    @classmethod
    def _hash_function(cls, function):
        key = id(function.__code__)
        new_source, new_funchash = cls._get_func_hash(key, function)
        new_closure = cls.extract_closure(function)
        return new_source, new_funchash, new_closure

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
        old_stdout = sys.stdout
        try:
            sys.stdout = out
            dis.dis(code)
        finally:
            sys.stdout = old_stdout
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
        if version_info < (3, 7):
            for ii, constant in enumerate(code.co_consts):
                if hasattr(constant, "co_code"):
                    res += "inner no %i" % ii
                    res += cls.dis_code(constant, None)
        return res

    @staticmethod
    def get_cython_source(cython_func):
        """Attemp to get the cython source for a function.
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

        # extract the function at hand, minus doc string
        remaining_lines = d[line_no - 1 :]  # lines start couting at 1
        first_line = remaining_lines[0]
        first_line_indent = len(first_line) - len(first_line.lstrip())
        start_tags = '"""', "'''"
        start_tag = False
        for st in start_tags:
            if first_line.strip().startswith(st):
                start_tag = st
                break
        if start_tag:  # there is a docstring
            text = "\n".join(remaining_lines).strip()
            text = text[3:]  # cut of initial ###
            text = text[text.find(start_tag) + 3 :]
            remaining_lines = text.split("\n")
        last_line = len(remaining_lines)
        for ii, line in enumerate(remaining_lines):
            if ii == 0:
                continue
            line_strip = line.strip()
            if line_strip:
                indent = len(line) - len(line.lstrip())
                if indent <= first_line_indent:
                    last_line = ii
                    break
        return "\n".join(remaining_lines[:last_line])

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
            # TODO: in notebooksNotebook, this is ok.
            raise exceptions.JobContractError(
                "FunctionInvariant %s created twice with different functions: \n%s\n%s"
                % (
                    job_id,
                    FunctionInvariant.function_to_str(function),
                    FunctionInvariant.function_to_str(self.function),
                )
            )

    @staticmethod
    def func_to_name(function):
        """Automatically derive a name for a function"""
        name = function.__qualname__
        if name == "<lambda>":
            raise ValueError(
                "Could not automatically generate a function name for a lambda, pass a name please"
            )
        return name

    def __str__(self):
        if (
            hasattr(self, "function")
            and self.function
            and hasattr(self.function, "__code__")
        ):  # during creating, __str__ migth be called by a debug function before function is set...
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

    def __init__(self, file):
        from . import global_pipegraph

        self.file = Path(file)
        super().__init__(str(self.file))
        if len(self.job_id) < 3 and not global_pipegraph.allow_short_filenames:
            raise ValueError(
                "This is probably not the filename you intend to use: {}.".format(self) + 
                " Use a longer filename or set graph.allow_short_filenames"
            )

    def output_needed(self, _ignored_runner):
        return True

    def run(self, _runner, historical_output):
        if not self.file.exists():
            raise FileNotFoundError(f"{self.file} did not exist")
        stat = self.file.stat()
        if not historical_output:
            return {self.outputs[0]: self.calculate(self.file, stat)}
        else:
            if int(stat.st_mtime) == historical_output.get(
                "mtime", -1
            ) and stat.st_size == historical_output.get("size", -1):
                return historical_output
            else:
                return {self.outputs[0]: self.calculate(self.file, stat)}

    @classmethod
    def compare_hashes(cls, old_hash, new_hash):
        return new_hash["hash"] == old_hash.get("hash", "")


class ParameterInvariant(_InvariantMixin, Job):
    job_kind = JobKind.Invariant

    def __init__(self, job_id, parameters):
        job_id = "PI" + job_id
        self.parameters = self.freeze(parameters)
        super().__init__(job_id)

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

        try:
            hash(obj)
            return obj
        except TypeError:
            pass

        if isinstance(obj, dict):
            frz = tuple(sorted([(k, ParameterInvariant.freeze(obj[k])) for k in obj]))
            return frz
        elif isinstance(obj, (list, tuple)):
            return tuple([ParameterInvariant.freeze(x) for x in obj])

        elif isinstance(obj, set):
            return frozenset(obj)
        else:
            msg = "Unsupported type: %r" % type(obj).__name__
            raise TypeError(msg)


class DataLoadingJob(Job):
    job_kind = JobKind.Loading

    def __init__(self, job_id, data_callback, depend_on_function=True):
        self.depend_on_function = depend_on_function
        self.callback = data_callback
        super().__init__(job_id)

    def readd(self):
        if self.depend_on_function:
            func_invariant = FunctionInvariant(self.callback, self.job_id)
            self.depends_on(func_invariant)
        super().readd()

    def run(self, _runner, historical_output):
        self.callback()
        return {
            self.outputs[0]: historical_output.get(self.outputs[0], 0) + 1
        }  # so the downstream get's invalidated

        # todo: there is a judgment call here
        # we could also invalidate on a hash based on the return of callback.
        # (which is more naturally available in an AttributeLoadingJob
        # that would be more inline with the 'only-recalc-if-the-input-actually-changed'
        # philosopy.
        # but it will cause false positives if you return things that have an instable str
        # (or what ever hash source we use)
        # and it will cause false negatives if the callback is just for the side effects...
        # option a) seperate into calculate and store, so that we always have the actual value?
        # that's of course an API change compared to the pypipegraph. Hm.

    def _output_needed(
        self, runner
    ):  # yeah yeah yeah the temp jobs need to delegate to their downstreams dude!
        for downstream_id in runner.dag.neighbors(self.job_id):
            job = runner.jobs[downstream_id]
            if job.output_needed(runner):
                return True
        False


def CachedDataLoadingJob(
    cache_filename,
    calc_callback,
    load_callback,
    depend_on_function=True,
    resources: Resources = Resources.SingleCore,
):
    cache_filename = Path(cache_filename)

    def do_cache(output_filename):
        with open(output_filename, "wb") as op:
            pickle.dump(calc_callback(), op, pickle.HIGHEST_PROTOCOL)

    cache_job = FileGeneratingJob(
        cache_filename, do_cache, depend_on_function=False, resources=resources
    )
    if depend_on_function:
        cache_job.depends_on(FunctionInvariant(cache_filename, calc_callback))

    def load():
        try:
            with open(cache_filename, "rb") as op:
                res = pickle.load(op)
                load_callback(res)
        except pickle.UnpicklingError as e:
            raise pickle.UnpicklingError(
                f"Unpickling error in file {cache_filename}", e
            )

    load_job = DataLoadingJob(
        "load" + str(cache_filename),
        load,
        depend_on_function=False,
    )
    load_job.depends_on(cache_job)
    if depend_on_function:
        load_job.depends_on(
            FunctionInvariant("load" + str(cache_filename), load_callback)
        )
    return CachedJobTuple(load_job, cache_job)


class AttributeLoadingJob(Job):  # Todo: refactor with DataLoadingJob
    job_kind = JobKind.Loading

    def __init__(
        self, job_id, object, attribute_name, data_callback, depend_on_function=True
    ):
        if not isinstance(attribute_name, str):
            raise ValueError("attribute_name was not a string")
        self.depend_on_function = depend_on_function
        self.object = object
        self.attribute_name = attribute_name
        self.callback = data_callback
        super().__init__(job_id)
        self.cleanup_job_class = _AttributeCleanupJob

    def readd(self):  # Todo: refactor
        if self.depend_on_function:
            func_invariant = FunctionInvariant(self.callback, self.job_id)
            self.depends_on(func_invariant)
        super().readd()

    def run(self, _runner, historical_output):
        setattr(self.object, self.attribute_name, self.callback())
        return {
            self.outputs[0]: historical_output.get(self.outputs[0], 0) + 1
        }  # so the downstream get's invalidated


def CachedAttributeLoadingJob(
    cache_filename,
    object,
    attribute_name,
    data_callback,
    depend_on_function=True,
    resources: Resources = Resources.SingleCore,
):
    cache_filename = Path(cache_filename)

    def do_cache(output_filename):
        with open(output_filename, "wb") as op:
            pickle.dump(data_callback(), op, pickle.HIGHEST_PROTOCOL)

    cache_job = FileGeneratingJob(
        cache_filename, do_cache, depend_on_function=False, resources=resources
    )
    if depend_on_function:
        cache_job.depends_on(FunctionInvariant(cache_filename, data_callback))

    def load():
        try:
            with open(cache_filename, "rb") as op:
                return pickle.load(op)
        except pickle.UnpicklingError as e:
            raise pickle.UnpicklingError(
                f"Unpickling error in file {cache_filename}", e
            )

    load_job = AttributeLoadingJob(
        "load" + str(cache_filename),
        object,
        attribute_name,
        load,
        depend_on_function=False,
    )
    load_job.depends_on(cache_job)
    return CachedJobTuple(load_job, cache_job)


class _AttributeCleanupJob(Job):
    """Jobs may register cleanup jobs that injected after their immediate downstreams.
    This encapsulates those

    """

    job_kind = JobKind.Cleanup

    def __init__(self, parent_job):
        Job.__init__(self, ["CleanUp:" + parent_job.job_id], Resources.RunsHere)
        self.parent_job = parent_job

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

    def __init__(self, job_id, callback, depend_on_function=True):
        self.depend_on_function = depend_on_function
        self.callback = callback
        self.last_run_id = None
        super().__init__(job_id)

    def readd(self):  # Todo: refactor
        if self.depend_on_function:
            func_invariant = FunctionInvariant(self.callback, self.job_id)
            self.depends_on(func_invariant)
        super().readd()

    def output_needed(self, runner):
        logger.error(
            f"JobGeneratingJob - last_run_id {self.last_run_id}, runner.run_id: {runner.run_id}"
        )
        if runner.run_id != self.last_run_id:
            return True
        return False

    def run(self, runner, historical_output):
        self.last_run_id = runner.run_id
        self.callback()
        # todo: is this the right approach
        # should we maybe instead return a sorted list of new jobs
        # if you depend on this, you're going te be triggered
        # *all* the time. Well once per graph.run
        return {
            self.outputs[0]: historical_output.get(self.outputs[0], 0) + 1
        }  # so the downstream get's invalidated


def _save_plot(plot, output_filename, plot_render_args):
    if not hasattr(plot, "render") and not hasattr(plot, "save"):
        raise exceptions.JobContractError(
            "%s.plot_function did not return a plot object (needs to have as render or save function"
            % (output_filename)
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
    else:
        raise NotImplementedError("Don't know how to handle this plotjob")


def PlotJob(
    output_filename,
    calc_function,
    plot_function,
    render_args=None,
    cache_dir="cache",
    depend_on_function=True,
    cache_calc=True,
    create_table=True,
):
    """Return a tuple of 3 jobs, the last two entries might be none.

    The first one is always a FileGeneratingJob
        around a wrapped plot_function, creating the output filename.

    If cache_calc is set, the second one is a CachedAttributeLoadingJob
    (wich again is a tuple, load_job, calc_job),
    loading a .data_ member on the first job returned.

    If create_table is set, the third one is a FileGeneratingJob
    writing (output_filename + '.tsv').
    """
    if render_args is None:
        render_args = {}
    output_filename = Path(output_filename)
    allowed_suffixes = (".png", ".pdf", ".svg")
    if not (output_filename.suffix in allowed_suffixes):
        raise ValueError(
            f"Don't know how to create a {output_filename.suffix} file, must end on one of {allowed_suffixes}."
        )

    def do_plot(output_filename):
        if not hasattr(plot_job, "data_"):
            plot_job.data_ = calc_function()
        plot = plot_function(plot_job.data_)
        _save_plot(plot, output_filename, render_args)

    plot_job = FileGeneratingJob(output_filename, do_plot, depend_on_function=False)
    if depend_on_function:
        plot_job.depends_on(FunctionInvariant(str(output_filename), plot_function))

    if cache_calc:

        def do_cache():
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

        cache_filename = Path(cache_dir) / output_filename
        cache_filename.parent.mkdir(exist_ok=True, parents=True)
        attribute_load_job, attribute_cache_job = CachedAttributeLoadingJob(
            cache_filename, plot_job, "data_", do_cache, depend_on_function=False
        )
        if depend_on_function:
            attribute_cache_job.depends_on(
                FunctionInvariant(cache_filename, calc_function)
            )
        plot_job.depends_on(attribute_load_job)
        cache_job = [attribute_load_job, attribute_cache_job]
    else:
        cache_job = None

    if create_table:

        def dump_table(output_filename):
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
            output_filename.with_suffix(output_filename.suffix + ".tsv"), dump_table
        )
        if cache_calc:
            table_job.depends_on(cache_job)
    else:
        table_job = None

    def add_another_plot(output_filename, plot_function, render_args={}):
        if render_args is None:
            render_args = {}

        def do_plot_another_plot(output_filename):
            if not hasattr(plot_job, "data_"):
                plot_job.data_ = calc_function()
            plot = plot_function(plot_job.data_)
            _save_plot(plot, output_filename, render_args)

        j = FileGeneratingJob(output_filename, do_plot_another_plot)
        if cache_calc:
            j.depends_on(cache_job)
        return j

    plot_job.add_another_plot = add_another_plot

    return (plot_job, cache_job, table_job)
