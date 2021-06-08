from pathlib import Path
import warnings
from loguru import logger
import types
import inspect
import os
import sys
import logging
import pypipegraph as ppg1
import pypipegraph2 as ppg2
import wrapt


old_entries = {}
old_modules = {}
patched = False

exception_map = {
    "RuntimeError": "RunFailed",
    "JobContractError": "JobContractError",
    "PyPipeGraphError": "FatalGraphException",
    "CycleError": "NotADag",
    "JobDiedException": "JobDied",
    "RuntimeException": "FatalGraphException",
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
    if "pypipegraph.job" in sys.modules:
        old_modules["pypipegraph.job"] = sys.modules["pypipegraph.job"]

    # ppg1.__name__ == "pypipegraph2"
    # ppg1.__file__ == __file__
    # ppg1.__path__ == __path__
    # ppg1.__loader__ == __loader__
    # ppg1.__spec__ == __spec__
    # ppg1.__version__ == ppg2.__version__
    ppg1.__doc__ == """ppg1->2 compability layer.
Supports the commonly used the old ppg1 API
with ppg2 objects. Aspires to be a drop-in replacement.
    """

    for old, new in exception_map.items():
        setattr(ppg1, old, getattr(ppg2.exceptions, new))

    # invariants

    ppg1.ParameterInvariant = ppg2.ParameterInvariant
    ppg1.FileInvariant = ppg2.FileInvariant
    ppg1.FileTimeInvariant = ppg2.FileInvariant
    ppg1.RobustFileChecksumInvariant = ppg2.FileInvariant
    ppg1.FileChecksumInvariant = ppg2.FileInvariant
    ppg1.FunctionInvariant = ppg2.FunctionInvariant
    ppg1.MultiFileInvariant = wrap_job(MultiFileInvariant)

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
    ppg1.JobList = list
    ppg1.ppg_exceptions = ppg_exceptions
    ppg1.inside_ppg = ppg2.inside_ppg
    ppg1.assert_uniqueness_of_object = ppg2.assert_uniqueness_of_object
    sys.modules["pypipegraph.job"] = job
    # todo: list unpatched...
    new_entries = set(dir(ppg1))
    # this was used to find unported code.
    # for k in set(old_entries).difference(new_entries): # pragma: no cover
    # if not k.startswith("__") and k != "all":
    # warnings.warn(f"not yet ppg1-compability layer implemented: {k}")
    patched = True


def unreplace_ppg1():
    """Turn ppg1 compability layer off, restoring ppg1"""
    global patched
    if not patched:
        return
    for x in dir(ppg1):
        delattr(ppg1, x)
    for k, v in old_entries.items():
        setattr(ppg1, k, v)
    for k, v in old_modules.items():
        sys.modules[k] = v
    patched = False


def wrap_job(cls):
    """Adapt for ppg1 api idiosyncracies"""
    return lambda *args, **kwargs: PPG1Adaptor(cls(*args, **kwargs))


class ResourceCoordinators:
    def LocalSystem(max_cores_to_use=ppg2.ALL_CORES, profile=False, interactive=True):
        return (max_cores_to_use, interactive)


class Util:

    global_pipegraph = None


util = Util()
util.job_or_filename = ppg2.util.job_or_filename
util.inside_ppg = ppg2.inside_ppg
util.assert_uniqueness_of_object = ppg2.assert_uniqueness_of_object
util.flatten_jobs = ppg2.util.flatten_jobs


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
job.function_to_str = ppg2.FunctionInvariant.function_to_str


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
    if invariant_status_filename:
        invariant_status_filename = Path(invariant_status_filename)
        kwargs["log_dir"] = invariant_status_filename / "logs"
        kwargs["error_dir"] = invariant_status_filename / "errors"
        kwargs["history_dir"] = invariant_status_filename / "history"
        kwargs["run_dir"] = invariant_status_filename / "run"
    kwargs["allow_short_filenames"] = False  # as was the default for ppg1

    res = ppg2.new(
        cores=cores,
        run_mode=run_mode,
        log_level=log_level,
        cache_dir=Path(cache_folder),
        **kwargs,
    )
    res.cache_folder = res.cache_dir  # ppg1 compability
    util.global_pipegraph = res

    return res


def run_pipegraph(*args, **kwargs):
    """Run the current global pipegraph"""
    if util.global_pipegraph is None:
        raise ValueError("You need to call new_pipegraph first")
    ppg2.run()


def _ignore_code_changes(job):
    job.depend_on_function = False
    if hasattr(job, "func_invariant"):
        logger.info(f"ignoring changes for {job.job_id}")
        util.global_pipegraph.job_dag.remove_node(job.func_invariant.job_id)
        for k in job.func_invariant.outputs:
            util.global_pipegraph.job_inputs[job.job_id].remove(k)
        del job.func_invariant
    if hasattr(job, "lfg"):
        _ignore_code_changes(job.lfg)


class PPG1Adaptor(wrapt.ObjectProxy):
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


def assert_ppg_created():
    if not util.global_pipegraph:
        raise ValueError("Must instantiate a pipegraph before creating any Jobs")


def FileGeneratingJob(output_filename, function, rename_broken=False, empty_ok=False):
    assert_ppg_created()
    sig = inspect.signature(function)
    if len(sig.parameters) == 0:

        def wrapper(of):  # pragma: no cover - runs in spawned process
            function()
            if not of.exists():
                raise ppg2.exceptions.JobContractError(
                    "%s did not create its file %s %s\n.Cwd: %s"
                    % (
                        of,
                        function.__code__.co_filename,
                        function.__code__.co_firstlineno,
                        os.path.abspath(os.getcwd()),
                    )
                )

        wrapper.wrapped_function = function
        func = wrapper
    else:
        func = function

    res = ppg2.FileGeneratingJob(output_filename, func, empty_ok=empty_ok)
    return PPG1Adaptor(res)


def MultiFileGeneratingJob(
    output_filenames, function, rename_broken=False, empty_ok=False
):
    assert_ppg_created()
    sig = inspect.signature(function)
    if len(sig.parameters) == 0:

        def wrapper(ofs):  # pragma: no cover - runs in spawned process
            function()
            for of in ofs:
                if not of.exists():
                    raise ppg2.exceptions.JobContractError(
                        "%s did not create its file %s %s\n.Cwd: %s"
                        % (
                            of,
                            function.__code__.co_filename,
                            function.__code__.co_firstlineno,
                            os.path.abspath(os.getcwd()),
                        )
                    )

        func = ppg2.jobs._mark_function_wrapped(wrapper, function)

    else:
        func = function

    res = ppg2.MultiFileGeneratingJob(output_filenames, func, empty_ok=empty_ok)
    return PPG1Adaptor(res)


def TempFileGeneratingJob(output_filename, function, rename_broken=False):
    assert_ppg_created()
    sig = inspect.signature(function)
    if len(sig.parameters) == 0:

        def wrapper(of):  # pragma: no cover - runs in spawned process

            function()
            if not of.exists():
                raise ppg2.exceptions.JobContractError(
                    "%s did not create its file %s %s\n.Cwd: %s"
                    % (
                        of,
                        function.__code__.co_filename,
                        function.__code__.co_firstlineno,
                        os.path.abspath(os.getcwd()),
                    )
                )

        func = ppg2.jobs._mark_function_wrapped(wrapper, function)
    else:
        func = function

    res = ppg2.TempFileGeneratingJob(output_filename, func)
    return PPG1Adaptor(res)


def MultiTempFileGeneratingJob(output_filenames, function, rename_broken=False):
    assert_ppg_created()
    sig = inspect.signature(function)
    if len(sig.parameters) == 0:

        def wrapper(ofs):  # pragma: no cover - runs in spawned process

            function()
            for of in ofs:
                if not of.exists():
                    raise ppg2.exceptions.JobContractError(
                        "%s did not create its file %s %s\n.Cwd: %s"
                        % (
                            of,
                            function.__code__.co_filename,
                            function.__code__.co_firstlineno,
                            os.path.abspath(os.getcwd()),
                        )
                    )

        func = ppg2.jobs._mark_function_wrapped(wrapper, function)
    else:
        func = function

    res = ppg2.MultiTempFileGeneratingJob(output_filenames, func)
    return PPG1Adaptor(res)


def MultiFileInvariant(filenames):
    # ppg2 already detects when invariants are gained and lost -> no need for
    # special MultiFileInvariant
    res = []
    for f in filenames:
        res.append(ppg2.FileInvariant(f))
    return res


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
            logger.info("cache job ignore changes")
            _ignore_code_changes(self.cache_job)
        if self.table_job is not None:
            _ignore_code_changes(self.table_job)

    res.ignore_code_changes = types.MethodType(ignore_code_changes, res)

    return res


def wrap_old_style_lfg_cached_job(job):
    # adapt new style to old style
    if hasattr(job.load, "__wrapped__"): # pragma: no cover
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

    res.depends_on = types.MethodType(depends_on, res)

    def use_cores(self, cores):
        self.lfg.use_cores(cores)
        return self

    res.use_cores = types.MethodType(use_cores, res)

    return res


def unsupported(name):
    def inner():
        raise NotImplementedError(f"ppg2 no longer offers {name}")

    return inner
