from pathlib import Path
import types
import inspect
import os
import sys
import logging
import pypipegraph as ppg1
import pypipegraph.testing
import pypipegraph.testing.fixtures
import pypipegraph2 as ppg2
import pypipegraph2.testing
import pypipegraph2.testing.fixtures
import wrapt
import importlib
from .util import (
    # log_info, log_error, log_warning, log_debug,
    log_job_trace,
)


old_entries = {}
old_modules = {}
patched = False

exception_map = {
    "RuntimeError": "JobsFailed",
    "JobContractError": "JobContractError",
    "PyPipeGraphError": "FatalGraphException",
    "CycleError": "NotADag",
    "JobDiedException": "JobDied",
    "RuntimeException": "RunFailedInternally",
}


def replace_ppg1():
    """Turn all ppg1 references into actual ppg2
    objects.
    Best effort, but the commonly used API should be well supported.
    Try to do this before anything imports ppg1.
    """

    global patched
    if patched:
        return
    for x in dir(ppg1):
        old_entries[x] = getattr(ppg1, x)
        delattr(ppg1, x)
    for module_name, replacement in {
        "pypipegraph.job": job,
        "pypipegraph.testing": ppg2.testing,
        "pypipegraph.testing.fixtures": ppg2.testing.fixtures,
    }.items():
        if not module_name in sys.modules:
            importlib.import_module(module_name)

        old_modules[module_name] = sys.modules[module_name]
        sys.modules[module_name] = replacement

    # ppg1.__name__ == "pypipegraph2"
    # ppg1.__file__ == __file__
    # ppg1.__path__ == __path__
    # ppg1.__loader__ == __loader__
    # ppg1.__spec__ == __spec__
    # ppg1.__version__ == ppg2.__version__
    ppg1.__doc__ == """ppg1->2 compatibility layer.
Supports the commonly used the old ppg1 API
with ppg2 objects. Aspires to be a drop-in replacement.
    """

    for old, new in exception_map.items():
        setattr(ppg1, old, getattr(ppg2.exceptions, new))

    # invariants

    ppg1.ParameterInvariant = ParameterInvariant
    ppg1.FileInvariant = FileInvariant
    ppg1.FileTimeInvariant = FileInvariant
    ppg1.RobustFileChecksumInvariant = FileInvariant
    ppg1.FileChecksumInvariant = FileInvariant
    ppg1.FunctionInvariant = FunctionInvariant
    ppg1.MultiFileInvariant = MultiFileInvariant

    ppg1.MultiFileGeneratingJob = MultiFileGeneratingJob
    ppg1.FileGeneratingJob = FileGeneratingJob
    ppg1.CachedAttributeLoadingJob = CachedAttributeLoadingJob
    ppg1.CachedDataLoadingJob = CachedDataLoadingJob
    ppg1.TempFileGeneratingJob = TempFileGeneratingJob
    ppg1.MultiTempFileGeneratingJob = MultiTempFileGeneratingJob
    ppg1.PlotJob = PlotJob

    ppg1.Job = ppg2.Job  # don't wrap, we need the inheritance
    ppg1.DataLoadingJob = wrap_job(ppg2.DataLoadingJob)
    ppg1.AttributeLoadingJob = wrap_job(ppg2.AttributeLoadingJob)
    ppg1.JobGeneratingJob = wrap_job(ppg2.JobGeneratingJob)

    # unsupported
    for k in (
        "NotebookJob",
        "DependencyInjectionJob",
        "TempFilePlusGeneratingJob",
        "MemMappedDataLoadingJob",
        "FinalJob",
        "CombinedPlotJob",
        "NothingChanged",  # very implementation detail...
    ):
        setattr(ppg1, k, unsupported(k))

    # misc
    ppg1.resource_coordinators = ResourceCoordinators
    ppg1.new_pipegraph = new_pipegraph
    ppg1.run_pipegraph = run_pipegraph
    ppg1.util = util
    ppg1.graph = graph
    ppg1.job = job
    ppg1.JobList = ppg2.JobList
    ppg1.ppg_exceptions = ppg_exceptions
    ppg1.inside_ppg = ppg2.inside_ppg
    ppg1.assert_uniqueness_of_object = ppg2.assert_uniqueness_of_object
    ppg1.testing = ppg2.testing
    ppg1.is_ppg2 = True
    ppg1.testing = ppg2.testing
    ppg1.testing.fixtures = ppg2.testing.fixtures
    # todo: list unpatched...
    new_entries = set(dir(ppg1))
    # this was used to find unported code.
    # for k in set(old_entries).difference(new_entries): # pragma: no cover
    # if not k.startswith("__") and k != "all":
    # warnings.warn(f"not yet ppg1-compatibility layer implemented: {k}")
    patched = True


def unreplace_ppg1():
    """Turn ppg1 compatibility layer off, restoring ppg1
    not that well tested, I suppose...

    """
    global patched
    if not patched:
        return
    for x in dir(ppg1):
        delattr(ppg1, x)
    for k, v in old_entries.items():
        setattr(ppg1, k, v)
    for k, v in old_modules.items():
        sys.modules[k] = v
    ppg1.testing = old_modules["pypipegraph.testing"]
    ppg1.testing.fixtures = old_modules["pypipegraph.testing.fixtures"]
    patched = False


def wrap_job(cls):
    """Adapt for ppg1 api idiosyncracies"""
    return lambda *args, **kwargs: PPG1Adaptor(cls(*args, **kwargs))


class ResourceCoordinators:
    def LocalSystem(max_cores_to_use=ppg2.ALL_CORES, profile=False, interactive=True):
        return (max_cores_to_use, interactive)


class Util:
    @property
    def global_pipegraph(self):
        from . import global_pipegraph

        return global_pipegraph

    @global_pipegraph.setter
    def global_pipegraph(self, value):
        from . import change_global_pipegraph

        change_global_pipegraph(value)

    @staticmethod
    def checksum_file(filename):
        """was used by outside functions"""
        import stat as stat_module
        import hashlib

        file_size = os.stat(filename)[stat_module.ST_SIZE]
        if file_size > 200 * 1024 * 1024:  # pragma: no cover
            print("Taking md5 of large file", filename)
        with open(filename, "rb") as op:
            block_size = 1024**2 * 10
            block = op.read(block_size)
            _hash = hashlib.md5()
            while block:
                _hash.update(block)
                block = op.read(block_size)
            res = _hash.hexdigest()
        return res


def job_or_filename(
    job_or_filename, invariant_class=None
):  # we want to return the wrapped class
    if invariant_class is None:
        invariant_class = FileInvariant
    return ppg2.util.job_or_filename(job_or_filename, invariant_class)


util = Util()
util.job_or_filename = job_or_filename
util.inside_ppg = ppg2.inside_ppg
util.assert_uniqueness_of_object = ppg2.assert_uniqueness_of_object
util.flatten_jobs = ppg2.util.flatten_jobs
util.freeze = ppg2.jobs.ParameterInvariant.freeze


class PPGExceptions:
    pass


ppg_exceptions = PPGExceptions()
for old, new in exception_map.items():
    setattr(ppg_exceptions, old, getattr(ppg2.exceptions, new))


# earlier on, we had a different pickling scheme,
# and that's what the files were called.
if os.path.exists(".pypipegraph_status_robust"):  # old projects keep their filename
    invariant_status_filename_default = ".pypipegraph_status_robust"  # pragma: no cover
elif "/" in sys.argv[0]:  # no script name but an executable?
    invariant_status_filename_default = ".pypipegraph_status_robust"
else:
    # script specific pipegraphs
    invariant_status_filename_default = (
        ".ppg_status_%s" % sys.argv[0]
    )  # pragma: no cover


class Graph:
    invariant_status_filename_default = invariant_status_filename_default


graph = Graph()


class Job:
    _InvariantJob = ppg2.jobs._InvariantMixin
    pass


job = Job()
job.function_to_str = ppg2._FunctionInvariant.function_to_str


class FakeRC:
    @property
    def cores_available(self):
        return ppg2.global_pipegraph.cores


def new_pipegraph(
    resource_coordinator=None,
    quiet=False,
    invariant_status_filename=None,
    dump_graph=True,
    interactive=True,
    cache_folder="cache",
    log_file=None,
    log_level=logging.ERROR,
):
    cores = ppg2.ALL_CORES
    run_mode = ppg2.RunMode.CONSOLE
    if resource_coordinator:
        cores = resource_coordinator[0]
        interactive = resource_coordinator[1]  # rc overrides interactive setting
    if interactive:  # otherwise, we read the one passed into the function
        run_mode = ppg2.RunMode.CONSOLE
    else:
        run_mode = ppg2.RunMode.NONINTERACTIVE
    kwargs = {}
    dir_config_args = {}
    if invariant_status_filename:
        invariant_status_filename = Path(invariant_status_filename)
        dir_config_args = {"root": invariant_status_filename}
    dir_config_args["cache_dir"] = Path(cache_folder)
    kwargs["allow_short_filenames"] = False  # as was the default for ppg1
    kwargs["prevent_absolute_paths"] = False  # as was the default for ppg1

    res = ppg2.new(
        cores=cores,
        dir_config=ppg2.DirConfig(**dir_config_args),
        run_mode=run_mode,
        log_level=log_level,
        **kwargs,
    )
    _add_graph_comp(res)
    return res


def _add_graph_comp(graph):
    graph.cache_folder = graph.dir_config.cache_dir  # ppg1 compatibility
    graph.rc = FakeRC()
    util.global_pipegraph = graph


def run_pipegraph(*args, **kwargs):
    """Run the current global pipegraph"""
    if util.global_pipegraph is None:
        raise ValueError("You need to call new_pipegraph first")
    ppg2.run(**kwargs)


def _ignore_code_changes(job):
    job.depend_on_function = False
    if hasattr(job, "func_invariant"):
        log_job_trace(f"ignoring changes for {job.job_id}")
        util.global_pipegraph.job_dag.remove_edge(job.func_invariant.job_id, job.job_id)

        if hasattr(job.func_invariant, "usage_counter"):
            job.func_invariant.usage_counter -= 1
        if (
            not hasattr(job.func_invariant, "usage_counter")
            or job.func_invariant.usage_counter == 0
        ):
            util.global_pipegraph.job_dag.remove_node(job.func_invariant.job_id)
            for k in job.func_invariant.outputs:
                util.global_pipegraph.job_inputs[job.job_id].remove(k)
            del util.global_pipegraph.jobs[job.func_invariant.job_id]

        del job.func_invariant
    if hasattr(job, "lfg"):
        _ignore_code_changes(job.lfg)


class PPG1AdaptorBase:
    def ignore_code_changes(self):
        _ignore_code_changes(self)

    def use_cores(self, value):
        self.cores_needed = value
        return self

    @property
    def cores_needed(self):
        res = None
        if self.resources == ppg2.Resources.AllCores:
            res = -1
        elif self.resources == ppg2.Resources.Exclusive:
            res = -2
        else:
            res = 1
        return res

    @cores_needed.setter
    def cores_needed(self, value):
        if value == -1 or value > 1:
            self.use_resources(ppg2.Resources.AllCores)
        elif value == -2:
            self.use_resources(ppg2.Resources.Exclusive)
        else:  # elif value == 1:
            self.use_resources(ppg2.Resources.SingleCore)

    def depends_on(self, *args):  # keep the wrapper
        if hasattr(self, "__wrapped__"):
            res = self.__wrapped__.depends_on(*args)
        else:
            super().depends_on(*args)
        return self

    def depends_on_file(self, filename):
        job = FileInvariant(filename)
        self.depends_on(job)
        return ppg2.jobs.DependsOnInvariant(job, self)

    def depends_on_params(self, params):
        job = ParameterInvariant(self.job_id, params)
        self.depends_on(job)
        return ppg2.jobs.DependsOnInvariant(job, self)

    @property
    def filenames(self):
        return self.files

    @property
    def prerequisites(self):
        return self.upstreams


class PPG1Adaptor(wrapt.ObjectProxy, PPG1AdaptorBase):
    pass


class FileInvariant(PPG1AdaptorBase, ppg2.FileInvariant):
    pass


class FunctionInvariant(PPG1AdaptorBase, ppg2._FunctionInvariant):
    pass


class ParameterInvariant(PPG1AdaptorBase, ppg2.ParameterInvariant):
    pass


def assert_ppg_created():
    if not util.global_pipegraph:
        raise ValueError("Must instantiate a pipegraph before creating any Jobs")


def _first_param_empty(signature):
    """Check whether the first argument to this call is
    empty, ie. no with a default value"""
    try:
        first = next((signature.parameters.items()).__iter__())
        return first[1].default == inspect._empty
    except StopIteration:
        return True


def _wrap_func_if_no_output_file_params(function, accept_all_defaults=False):
    sig = inspect.signature(function)
    if len(sig.parameters) == 0 or not _first_param_empty(sig):
        # no or only default parameters = do it oldstyle.
        if not accept_all_defaults and not _first_param_empty(sig):
            raise TypeError(
                f"Could not correctly wrap {function}.\n"
                f"{ppg2.FunctionInvariant.function_to_str(function)}\n"
                "It has default parameter that would have been replaced "
                "with output_filename in ppg1 already. Fix your function arguments"
            )

        def wrapper(of):  # pragma: no cover - runs in spawned process
            function()
            if not isinstance(of, list):
                of = [of]
            for a_filename in of:
                if not a_filename.exists():
                    raise ppg2.exceptions.JobContractError(
                        "%s did not create its file(s) %s %s\n.Cwd: %s"
                        % (
                            a_filename,
                            function.__code__.co_filename,
                            function.__code__.co_firstlineno,
                            os.path.abspath(os.getcwd()),
                        )
                    )

        wrapper.wrapped_function = function
        func = wrapper
    else:
        func = function
    return func


class FileGeneratingJob(PPG1AdaptorBase, ppg2.FileGeneratingJob):
    def __new__(cls, *args, **kwargs):
        obj = ppg2.FileGeneratingJob.__new__(cls, *args, **kwargs)
        return obj

    def __init__(self, output_filename, function, rename_broken=False, empty_ok=False):
        func = _wrap_func_if_no_output_file_params(function)
        super().__init__(output_filename, func, empty_ok=empty_ok)


class MultiFileGeneratingJob(PPG1AdaptorBase, ppg2.MultiFileGeneratingJob):
    def __init__(self, output_filenames, function, rename_broken=False, empty_ok=False):
        func = _wrap_func_if_no_output_file_params(function, accept_all_defaults=True)
        res = super().__init__(output_filenames, func, empty_ok=empty_ok)


class TempFileGeneratingJob(PPG1AdaptorBase, ppg2.TempFileGeneratingJob):
    def __init__(self, output_filename, function, rename_broken=False):
        func = _wrap_func_if_no_output_file_params(function)
        super().__init__(output_filename, func)


class MultiTempFileGeneratingJob(PPG1AdaptorBase, ppg2.MultiTempFileGeneratingJob):
    def __init__(self, output_filenames, function, rename_broken=False):
        func = _wrap_func_if_no_output_file_params(function, accept_all_defaults=True)
        super().__init__(output_filenames, func)


def MultiFileInvariant(filenames):
    # ppg2 already detects when invariants are gained and lost -> no need for
    # special MultiFileInvariant
    res = []
    for f in filenames:
        res.append(ppg2.FileInvariant(f))
    return res


# no one inherits from these, so wrapping in a function is ok, I suppose
# they should have been functions in ppg1.e..


def CachedAttributeLoadingJob(
    cache_filename, target_object, target_attribute, calculating_function
):
    try:
        job = ppg2.CachedAttributeLoadingJob(
            cache_filename, target_object, target_attribute, calculating_function
        )
    except ppg2.JobRedefinitionError as e:
        raise ppg1.JobContractError(str(e))

    return wrap_old_style_lfg_cached_job(job)


def CachedDataLoadingJob(cache_filename, calculating_function, loading_function):
    job = ppg2.CachedDataLoadingJob(
        cache_filename, calculating_function, loading_function
    )
    return wrap_old_style_lfg_cached_job(job)


def PlotJob(
    output_filename,
    calc_function,
    plot_function,
    render_args=None,
    skip_table=False,
    skip_caching=False,
):
    pj = ppg2.PlotJob(
        output_filename,
        calc_function,
        plot_function,
        render_args=render_args,
        cache_calc=not skip_caching,
        create_table=not skip_table,
    )
    res = pj.plot
    if isinstance(pj.cache, ppg2.jobs.CachedJobTuple):
        res.cache_job = wrap_old_style_lfg_cached_job(pj.cache)
    else:
        res.cache_job = pj.cache
    res.table_job = pj.table
    res = PPG1Adaptor(res)

    def depends_on(
        self,
        *other_jobs,
    ):
        # FileGeneratingJob.depends_on(self, other_job)  # just like the cached jobs, the plotting does not depend on the loading of prerequisites
        if res.cache_job is None:
            ppg2.Job.depends_on(self, *other_jobs)
            if self.table_job is not None:
                self.table_job.depends_on(*other_jobs)
        elif (
            hasattr(self, "cache_job") and other_jobs[0] is not self.cache_job
        ):  # activate this after we have added the invariants...
            self.cache_job.depends_on(*other_jobs)
        return self

    res.depends_on = types.MethodType(depends_on, res)

    def ignore_code_changes(self):
        _ignore_code_changes(self)
        if self.cache_job is not None:
            _ignore_code_changes(self.cache_job)
        if self.table_job is not None:
            _ignore_code_changes(self.table_job)

    res.ignore_code_changes = types.MethodType(ignore_code_changes, res)

    return res


def wrap_old_style_lfg_cached_job(job):
    # adapt new style to old style
    if hasattr(job.load, "__wrapped__"):  # pragma: no cover
        res = job.load
        res.lfg = job.calc  # just assume it's a PPG1Adaptor
    else:
        res = PPG1Adaptor(job.load)
        res.lfg = PPG1Adaptor(job.calc)

    def depends_on(self, *args, **kwargs):
        if args and args[0] == self.lfg:  # repeated definition, I suppose
            # must not call self.__wrapped__.depends_on - that's a recursion for some reason?
            ppg2.Job.depends_on(self, *args, **kwargs)
        else:
            self.lfg.depends_on(*args, **kwargs)
        return self

    res.depends_on = depends_on.__get__(res)

    def use_cores(self, cores):
        self.lfg.use_cores(cores)
        return self

    res.use_cores = use_cores.__get__(res)

    return res


def unsupported(name):
    def inner():
        raise NotImplementedError(f"ppg2 no longer offers {name}")

    return inner
