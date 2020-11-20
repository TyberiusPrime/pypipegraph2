from __future__ import annotations
import dis
import re
import sys
import inspect
import types
from typing import Union, List, Dict, Optional, Tuple, Callable
from loguru import logger
from pathlib import Path
from enum import Enum, auto
from io import StringIO
import networkx
from . import hashers, exceptions
from .runner import InvalidationState

module_type = type(sys)


class Resources(Enum):
    SingleCore = "SingleCore"
    AllCores = "AllCores"
    MemoryHog = "MemoryHog"
    Exclusive = "Exclusive"
    RateLimited = "RateLimited"  # todo
    RunsHere = "RunsHere"  # in this process


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
            raise ValueError("Invalid output definition")
        self.outputs = sorted([str(x) for x in self.outputs])
        self.job_id = ":::".join(self.outputs)
        self.cleanup_job = None
        global_pipegraph.add(self)

    def depends_on(
        self,
        other_job: Union[Union[str, Job], List[Union[str, Job]]],
        *other_jobs: Union[Union[str, Job], List[Union[str, Job]]],
    ):
        from . import global_pipegraph

        if isinstance(other_job, list):
            for x in other_job:
                self.depends_on(x)
        else:
            if isinstance(other_job, Job):
                o_job = other_job
                o_inputs = other_job.outputs
            else:
                o_job = global_pipegraph.jobs[
                    global_pipegraph.outputs_to_job_ids[other_job]
                ]
                o_inputs = [other_job]
            global_pipegraph.add_edge(o_job, self)
            global_pipegraph.job_inputs[self.job_id].update(o_inputs)
        if other_jobs:
            for o in other_jobs:
                self.depends_on(o)
        return self

    def is_temp_job(self):
        return False

    def output_needed(self):
        False

    def invalidated(self):
        """Inputs changed - nuke outputs etc"""
        pass


class MultiFileGeneratingJob(Job):
    def __init__(
        self,
        files: List[Path],  # todo: extend type attribute to allow mapping
        generating_function: Callable[List[Path]],
        resources: Resources = Resources.SingleCore,
        depend_on_function: bool = True,
    ):

        Job.__init__(self, files, resources)
        self.files = [Path(x) for x in self.outputs]
        self.generating_function = generating_function
        if depend_on_function:
            func_invariant = FunctionInvariant(self.generating_function, self.job_id)
            self.depends_on(func_invariant)

    def run(self, _was_invalidated):
        for fn in self.files:  # we rebuild anyway!
            if fn.exists():
                fn.unlink()
        self.generating_function(
            self.mapped_outputs if self.mapped_outputs else self.files
        )
        return {of.name: hashers.hash_file(of) for of in self.files}

    def output_needed(self):
        for fn in self.files:
            if not fn.exists():
                return True
        return False

    def invalidated(self):
        for fn in self.files:
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

    def run(self, _was_invalidated):
        """Call the generating function with just the one filename"""
        self.generating_function(self.files[0])
        return {str(of): hashers.hash_file(of) for of in self.files}


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

        def do_unlink():
            for fn in self.files:
                if fn.exists():
                    fn.unlink()

        self.cleanup_job = _CleanupJob(self.job_id, do_unlink)

    def is_temp_job(self):
        return True

    def output_needed(self):
        False

    def run_needed(self, runner):
        # invariants: true
        # FileGeneratingJob: True/False, depending on whether the output existed
        # TempJobs: True/False/CantTellYet.
        downstream = list(runner.job_graph.job_dag.neighbors(self.job_id))
        if not downstream:
            return False
        for downstream_job_id in downstream:
            if (
                runner.job_stati[downstream_job_id].invalidation_state
                is InvalidationState.Invalidated
            ):
                return True
            downstream_job = runner.job_graph.jobs[downstream_job_id]
            if downstream_job.run_needed(runner):
                return True
        return False


class TempFileGeneratingJob(MultiTempFileGeneratingJob):
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

    def run(self, _was_invalidated):
        """Call the generating function with just the one filename"""
        self.generating_function(self.files[0])
        return {str(of): hashers.hash_file(of) for of in self.files}


class _CleanupJob(Job):
    """Jobs may register cleanup jobs that injected after their immediate downstreams.
    This encapsulates those

    """

    def __init__(self, parent_job_id, cleanup_function):
        Job.__init__(self, ["CleanUp:" + parent_job_id], Resources.RunsHere)
        self.parent_job_id = parent_job_id
        self.cleanup_function = cleanup_function

    def run(self, _was_invalidated):
        self.cleanup_function()
        return {self.outputs[0]: None}  # todo: optimize this awy?


class FunctionInvariant(Job):
    def __init__(
        self, function, name=None
    ):  # todo, must support the inverse calling with name, function
        self.function = function
        name = (
            "FunctionInvariant:" + name
            if name
            else FunctionInvariant.func_to_name(function)
        )
        self.verify_arguments(name, function)
        Job.__init__(self, [name], Resources.RunsHere)

    def output_needed(self):
        return True

    def run(self, _was_invalidated):
        # todo: Don't recalc if file / source did not change.
        # Actually I suppose we can (ab)use the the graph and a FileInvariant for that?
        source, is_python_func = self.get_source()
        res = {"source": self.get_source()}
        if is_python_func:
            python_version = tuple(sys.version_info)
            res[python_version] = (
                self.get_dis(self.function),
                self.extract_closure(self.function),
            )

        return {self.job_id: res}

    def get_source(self):
        """Return the 'source' and whether this was a python function"""
        if self.function is None:
            # since the 'default invariant' is False, this will still read 'invalidated the first time it's being used'
            return None
        if (not hasattr(self.function, "__code__")) or (
            "cython_function_or_method" in str(type(self.function))
            or (
                isinstance(self.function, types.MethodType)
                and "cython_function_or_method" in str(type(self.function.__func__))
            )
        ):
            return self._get_source_from_non_python_function(self.function), False
        else:
            return self._get_python_source(self.function), True

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
    def _get_invariant_from_non_python_function(cls, function):
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

    @classmethod
    def _get_func_hash(cls, key, function):
        if not util.global_pipegraph or key not in util.global_pipegraph.func_hashes:
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
            value = (source,)
            if not util.global_pipegraph:
                return value
            util.global_pipegraph.func_hashes[key] = value

        return util.global_pipegraph.func_hashes[key]

    @classmethod
    def _hash_function(cls, function):
        key = id(function.__code__)
        new_source, new_funchash = cls._get_func_hash(key, function)
        new_closure = cls.extract_closure(function)
        return new_source, new_funchash, new_closure

    def _get_invariant(self, old, all_invariant_stati, version_info=sys.version_info):
        if self.function is None:
            # since the 'default invariant' is False, this will still read 'invalidated the first time it's being used'
            return None
        if (not hasattr(self.function, "__code__")) or (
            "cython_function_or_method" in str(type(self.function))
            or (
                isinstance(self.function, types.MethodType)
                and "cython_function_or_method" in str(type(self.function.__func__))
            )
        ):

            return self._get_invariant_from_non_python_function(self.function)
        new_source, new_funchash, new_closure = self._hash_function(self.function)
        return self._compare_new_and_old(new_source, new_funchash, new_closure, old)

    @staticmethod
    def _compare_new_and_old(new_source, new_funchash, new_closure, old):
        new = {
            "source": new_source,
            str(sys.version_info[:2]): (new_funchash, new_closure),
        }

        if isinstance(old, dict):
            pass  # the current style
        elif isinstance(old, tuple):
            # the previous style.
            old_funchash = old[2]
            old_closure = old[3]
            old = {
                # if you change python version and pypipegraph at the same time, you're out of luck and will possibly rebuild
                str(sys.version_info[:2]): (
                    old_funchash,
                    old_closure,
                )
            }
        elif isinstance(old, str):
            # the old old style, just concatenated.
            old = {"old": old}
            new["old"] = new_funchash + new_closure
        elif old is False:  # never ran before
            return new
        elif (
            old is None
        ):  # if you provided a None type instead of a function, you will run into this
            return new
        else:  # pragma: no cover
            raise ValueError(
                "Could not understand old FunctionInvariant invariant. Was Type(%s): %s"
                % (type(old), old)
            )
        unchanged = False
        for k in set(new.keys()).intersection(old.keys()):
            if k != "_version" and new[k] == old[k]:
                unchanged = True
        out = old.copy()
        out.update(new)
        out[
            "_version"
        ] = 3  # future proof, since this is *at least* the third way we're doing this
        if "old" in out:
            del out["old"]
        if unchanged:
            raise ppg_exceptions.NothingChanged(out)
        return out

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
            raise ValueError("%s function was not a callable (or None)" % job_id)
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
