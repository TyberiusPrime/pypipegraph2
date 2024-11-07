from typing import Optional, Union, Dict
import gzip
import threading
import json
import logging
import shutil
import collections
import os
import sys
import pickle
import signal
import networkx
import subprocess
import time
import datetime
import pyzstd
from pathlib import Path
from loguru import logger

from . import exceptions
from .runner import Runner
from .enums import RunMode, JobOutcome
from .exceptions import JobsFailed, _RunAgain
from .util import (
    CPUs,
    log_info,
    log_error,
    log_warning,
    log_debug,
    log_trace,
    escape_logging,
)
from . import util

try:
    logger.level("JT", no=6, color="<yellow>", icon="?")
    logger.level("INFO", color="")
except TypeError:
    pass
# if "pytest" in sys.modules:  # pragma: no branch
# log_out = sys.stderr
# else:  # pragma: no cover
# log_out = RichHandler(markup=True, console=console)
# logger.add(
# sink=log_out,
# format="{elapsed} {message}",
# level=logging.INFO,
# )

time_format = "%Y-%m-%d_%H-%M-%S"

start_cwd = Path(".").absolute()


class ALL_CORES:
    pass


class DirConfig:
    def __init__(
        self,
        root: Union[Path, str] = ".ppg",
        log_dir: Union[Path, str, None, bool] = False,
        error_dir: Union[Path, str, None, bool] = False,
        history_dir: Union[Path, str, None, bool] = False,
        run_dir: Union[
            Path, str, None, bool
        ] = False,  # used by FileGeneratingJobs to e.g. recorde stdout
        cache_dir="cache",
    ):
        self.root = Path(root)
        if log_dir is False:
            self.log_dir = self.root / "logs"
        else:
            self.log_dir = Path(log_dir) if log_dir is not None else None
        if error_dir is False:
            self.error_dir = self.root / "errors"
        else:
            self.error_dir = Path(error_dir) if error_dir is not None else None
        if history_dir is False:
            self.history_dir = self.root / "history"
        else:
            self.history_dir = Path(history_dir)
        if run_dir is False:
            self.run_dir = self.root / "run"
        else:
            self.run_dir = Path(run_dir)
        if cache_dir is False:
            self.cache_dir = self.root / "cache"
        else:
            self.cache_dir = Path(cache_dir) if cache_dir is not None else None

    def mkdirs(self):
        for dir in (
            self.root,
            self.log_dir,
            self.error_dir,
            self.history_dir,
            self.run_dir,
            self.cache_dir,
        ):
            if dir is not None:
                dir.mkdir(exist_ok=True, parents=True)


class PyPipeGraph:
    history_dir: Optional[Path]
    log_dir: Optional[Path]
    log_level: int
    running: bool

    def __init__(
        self,
        cores: Union[int, ALL_CORES],
        dir_config: Optional[Union[DirConfig, str, Path]],
        log_level: int,
        run_mode: RunMode,
        paths: Optional[Dict[str, Union[Path, str]]] = None,
        allow_short_filenames=False,
        log_retention=None,
        prevent_absolute_paths=True,
        report_done_filter=1,
        push_events=False,
    ):
        if cores is ALL_CORES:
            self.cores = CPUs()
        else:
            if isinstance(cores, (DirConfig, str, Path)):  # pragma: no cover
                raise ValueError(
                    "You tried to pass dir_config by positin, but the first argument is cores"
                )
            self.cores = int(cores)
        if isinstance(dir_config, (str, Path)):
            dir_config = DirConfig(dir_config)
        self.dir_config = dir_config

        self.log_level = log_level
        self.log_retention = log_retention
        # self.paths = {k: Path(v) for (k, v) in paths} if paths else {}
        self.run_mode = run_mode
        self.jobs = {}  # the job objects, by id
        self.job_dag = (
            networkx.DiGraph()
        )  # a graph. Nodes: job_ids, edges -> must be done before
        self.job_inputs = collections.defaultdict(
            set
        )  # necessary inputs (ie. outputs of other jobs)
        self.outputs_to_job_ids = {}  # so we can find the job that generates an output: todo: should be outputs_to_job_id or?
        self.run_id = 0
        self._test_failing_outside_of_job = False
        self.allow_short_filenames = allow_short_filenames
        self.dir_config.mkdirs()
        self.running = False
        self.prevent_absolute_paths = prevent_absolute_paths
        self._debug_allow_ctrl_c = False  # see examples/abort_when_stalled.py
        self.next_job_number = 0
        self.next_job_number_lock = threading.Lock()
        self._path_cache = {}
        self.report_done_filter = report_done_filter
        self.func_cache = {}
        self.dir_absolute = Path(".").absolute()
        self._jobs_do_dump_subgraph_debug = False
        self._jobs_to_prune_unrelated = False
        self._jobs_to_prune_but = False
        self.push_events = push_events

    def run(
        self,
        print_failures: bool = True,
        raise_on_job_error=True,
        event_timeout=5,
        dump_graphml=False,
    ) -> Dict[str, JobOutcome]:
        """Run the complete pypipegraph"""
        do_raise = [False, None, None]
        try:
            return self._run(
                print_failures,
                raise_on_job_error,
                event_timeout,
                None,
                dump_graphml=dump_graphml,
            )
        except JobsFailed as e:  # shorten the traceback considerably!
            do_raise = True, e.args[0], e.exceptions
        if do_raise[0]:
            raise JobsFailed(do_raise[1], exceptions=do_raise[2])

    def _run(
        self,
        print_failures: bool = True,
        raise_on_job_error=True,
        event_timeout=5,
        focus_on_these_jobs=None,
        dump_graphml=False,
    ) -> Dict[str, JobOutcome]:
        """Run the jobgraph - possibly focusing on a subset of jobs (ie. ignoring
        anything that's not necessary to calculate them - activated by calling a Job
        """
        ts = str(
            time.time()
        )  # include subsecond in log names - useful for the testing, I suppose.
        ts = ts[ts.rfind(".") :]
        self.time_str = datetime.datetime.now().strftime(time_format) + ts
        if not networkx.algorithms.is_directed_acyclic_graph(self.job_dag):
            print(networkx.readwrite.json_graph.node_link_data(self.job_dag))
            raise exceptions.NotADag()
        else:
            # print(networkx.readwrite.json_graph.node_link_data(self.job_dag))
            pass
        start_time = time.time()
        self._resolve_dependency_callbacks()
        self.running = True  # must happen after dependency callbacks
        if self.dir_config.error_dir:
            self._cleanup_errors()
            (self.dir_config.error_dir / self.time_str).mkdir(
                exist_ok=True, parents=True
            )
            self._link_errors()
        log_id = None
        if self.dir_config.log_dir:
            self._cleanup_logs()
            fn = Path(sys.argv[0]).name
            self.log_file = (
                self.dir_config.log_dir / f"{fn}-{self.time_str}.messages.log"
            )
            self.log_file_lookup = (
                self.dir_config.log_dir / f"{fn}-{self.time_str}.lookup.log"
            )
            self.create_event_socket()
            log_position_lookup = {}
            log_position_lookup_file = open(self.log_file_lookup, "w", buffering=1)
            logger.remove()  # no default logging
            # log_lock = threading.Lock()

            def smart_format(x):
                key = str((x["file"], x["function"], x["line"]))
                if not key in log_position_lookup:
                    next_number = len(log_position_lookup)
                    min_len = max(3, len(str(next_number)))
                    log_position_lookup[key] = ("{0:>" + str(min_len) + "}").format(
                        str(next_number)
                    )
                    log_position_lookup_file.write(
                        "{} | {:>15}:{:>4} | {}\nkey: {}\n".format(
                            log_position_lookup[key],
                            x["file"],
                            x["line"],
                            x["function"],
                            key,
                        )
                    )
                return f"{x['level']:<5} | {log_position_lookup[key]} | {x['time']:HH:mm:ss:SS} | {escape_logging(x['message'])}\n"

            logger.add(
                open(self.log_file, "w"),
                level=self.log_level,
                format=smart_format,
            )
            log_info(f"See {self.log_file_lookup} for code positions (2nd column)")

            # if "pytest" in sys.modules:  # pragma: no branch
            log_id = logger.add(
                sink=sys.stdout,
                level=(
                    logging.WARNING if not util.do_jobtrace_log else 6
                ),  # don't spam stdout
                format=(
                    "\r  <blue>{elapsed}s</blue> <bold>|</bold> <level>{message}</level>"
                    if not util.do_jobtrace_log
                    else "<blue>{elapsed}s</blue> | <level>{level.icon}</level> <bold>|</bold>{file:8.8}:{line:4} <level>{message}</level>"
                ),
            )  # pragma: no cover
            self._link_logs()

            log_debug(
                f"Run is go {threading.get_ident()} pid: {os.getpid()}, run_id {self.run_id}, log_level = {self.log_level}"
            )
        self.do_raise = []
        self._restart_afterwards = False

        # we want logs, so we do tihs 'late'.
        if self._jobs_to_prune_unrelated:
            self._prune_unrelated()
        if self._jobs_to_prune_but:
            self._prune_but()
        ok = False

        try:
            result = None
            self._install_signals()
            try:
                history = self._load_history()
            except Exception as e:
                log_error(f"{e}")
                raise exceptions.HistoryLoadingFailed(e)
            max_runs = 5
            jobs_already_run = set()
            final_result = {}
            aborted = False
            self.post_event({"type": "run_start", "time": time.time()})
            try:
                while True:
                    max_runs -= 1
                    if max_runs == 0:  # pragma: no cover
                        raise ValueError(
                            "Maximum graph-generating-jobs recursion depth exceeded"
                        )
                    do_break = False
                    job_count = len(self.job_dag)
                    try:
                        self.runner = Runner(
                            self,
                            history,
                            event_timeout,
                            focus_on_these_jobs,
                            jobs_already_run,
                            dump_graphml,
                            self.run_id,
                            self._jobs_do_dump_subgraph_debug,
                        )
                        result, new_history = self.runner.run(
                            history, result, print_failures=print_failures
                        )
                        aborted = self.runner.aborted
                        del self.runner
                        self.run_id += 1
                        do_break = True
                    except _RunAgain as e:
                        log_info("Jobs created - running again")
                        result, new_history = e.args[0]
                    self._log_runtimes(result, start_time)
                    # assert len(result) == job_count # does not account for cleanup jobs...
                    # leave out the cleanup jobs added virtually by the run
                    jobs_already_run.update(
                        (k for k in result.keys() if k in self.jobs)
                    )
                    log_debug(f"Result len {len(result)}")
                    for k, v in result.items():
                        if (
                            not k in final_result
                            or final_result[k].outcome != JobOutcome.Failed
                        ):
                            final_result[k] = v
                    # final_result.update(result)
                    if history != new_history:
                        self._save_history(new_history)
                    history = new_history

                    if do_break:
                        break
                    # final_result.update(result)
            except KeyboardInterrupt:
                self.do_raise.append(KeyboardInterrupt())
            del result
            log_debug(f"Left graph loop. Final result len {len(final_result)}")
            jobs_failed = False
            for job_id, job_state in final_result.items():
                if job_state.outcome == JobOutcome.Failed:
                    self.do_raise.append(job_state.payload)
                    jobs_failed = True
            self.last_run_result = final_result
            if raise_on_job_error and self.do_raise and not self._restart_afterwards:
                if jobs_failed:
                    self.post_event(
                        {
                            "type": "run_stop",
                            "time": time.time(),
                            "outcome": "job_failure",
                        }
                    )
                    raise exceptions.JobsFailed(
                        "At least one job failed", self.do_raise
                    )
                else:
                    self.post_event(
                        {
                            "type": "run_stop",
                            "time": time.time(),
                            "outcome": "internal_failure",
                        }
                    )
                    raise exceptions.RunFailedInternally()
            elif not raise_on_job_error and self.do_raise:
                log_error("At least one job failed")
            if aborted:
                self.post_event(
                    {"type": "run_stop", "time": time.time(), "outcome": "aborted"}
                )
                raise KeyboardInterrupt("Run aborted")  # pragma: no cover
            ok = True
            self.post_event({"type": "run_stop", "time": time.time(), "outcome": "ok"})
            return final_result
        except Exception as e:
            log_error(e)
            raise
        finally:
            self.close_event_socket()
            if ok:
                log_info("Run is done")
            else:
                log_info("Run is done - with failure")
            log_info("")
            self._restore_signals()
            if log_id is not None:
                logger.remove(log_id)
            self.running = False
            if (
                self._restart_afterwards
            ):  # pragma: no cover - todo: test with interactive
                log_info("Restart again issued - restarting via subprocess.check_call")
                subprocess.check_call([sys.executable] + sys.argv, cwd=start_cwd)

    def run_for_these(self, jobs):
        """Run graph for just these jobs (and their upstreams), ignoring everything else"""
        if not isinstance(jobs, list):
            jobs = [jobs]
        return self._run(
            print_failures=True, raise_on_job_error=True, focus_on_these_jobs=jobs
        )

    def _cleanup_logs(self):
        """Clean up old logs"""
        if (
            not self.dir_config.log_dir or self.log_retention is None
        ):  # pragma: no cover
            return
        fn = Path(sys.argv[0]).name
        pattern = f"{fn}-*.messages.log"
        files = sorted(self.dir_config.log_dir.glob(pattern))
        if len(files) > self.log_retention:
            remove = files[: -self.log_retention]
            for f in remove:
                f.unlink()
                lf = f.with_name(f.name[: -1 * len(".messages.log")] + ".lookup.log")
                if lf.exists():
                    lf.unlink()

    def _link_latest(self, dir, pattern, latest_name, target_is_directory):
        link_name = dir / latest_name
        if link_name.exists() or link_name.is_symlink():
            # print("unlinking", link_name)
            link_name.unlink()
        # else:
        # print("not found", link_name)

        files = sorted(dir.glob(pattern))
        if files:
            link_name.symlink_to(
                files[-1].name, target_is_directory=target_is_directory
            )

    def _link_logs(self):
        fn = Path(sys.argv[0]).name
        self._link_latest(
            self.dir_config.log_dir, f"{fn}-*.messages.log", "latest.messages", False
        )
        self._link_latest(
            self.dir_config.log_dir, f"{fn}-*.lookup.log", "latest.lookup", False
        )

    def _cleanup_errors(self):
        """Cleanup old errors and drop a 'latest' symlink"""
        if (
            not self.dir_config.error_dir or self.log_retention is None
        ):  # pragma: no cover
            return
        err_dirs = sorted(
            [
                x
                for x in (self.dir_config.error_dir / self.time_str).parent.glob("*")
                if x.is_dir() and not x.is_symlink()
            ]
        )
        if len(err_dirs) > self.log_retention:
            remove = err_dirs[: -self.log_retention]
            for f in remove:
                shutil.rmtree(f)

    def _link_errors(self):
        self._link_latest(self.dir_config.error_dir, "*", "latest", True)

    def _log_runtimes(self, job_results, run_start_time):
        """Log the runtimes to a file (ever growing. But only runtimes over a threshold)"""
        if self.dir_config.log_dir:
            rt_file = self.dir_config.log_dir / "runtimes.tsv"
            lines = []
            if not rt_file.exists():
                lines.append("jobid\trun_start_time\truntime_s")
            for job_id, job_result in job_results.items():  # pragma: no branch
                if job_result.outcome is JobOutcome.Success:
                    if getattr(job_result, "run_time", 0) >= 1:
                        lines.append(
                            f"{job_id}\t{int(run_start_time)}\t{job_result.run_time:.2f}"
                        )
            with open(rt_file, "a+") as op:
                op.write("\n".join(lines) + "\n")

    def get_history_filename(self):
        """where do we store the graph's history?"""
        # we by default share the history file
        # if it's the same history dir, it's the same project
        # and you'd retrigger the calculations too often otherwise
        return self.dir_config.history_dir / "ppg_history.2.zstd"  # don't end on .py

    def _load_history(self):
        log_trace("_load_history")
        fn = self.get_history_filename()
        old_fn = self.dir_config.history_dir / "ppg_history.2.gz"
        self._convert_old_history()
        history = {}
        if fn.exists():
            log_trace("Historical existed")
            with pyzstd.ZstdFile(fn, "rb") as op:
                history = json.loads(op.read().decode("utf-8"))
        elif old_fn.exists():
            log_trace("Historical existed - in gzip format. will be converted")
            with gzip.GzipFile(old_fn, "rb") as op:
                history = json.loads(op.read().decode("utf-8"))

        log_debug(f"Loaded {len(history)} history entries")
        return history

    def _save_history(self, historical):
        log_trace("_save_history")
        fn = self.get_history_filename()
        fn_temp = fn.with_suffix(".temp")
        raise_keyboard_interrupt = False

        try_again = True
        while try_again:
            with pyzstd.ZstdFile(fn_temp, "wb") as op:
                try:
                    op.write(json.dumps(historical, indent=2).encode("utf-8"))
                    if self._test_failing_outside_of_job:  # for unit test
                        self._test_failing_outside_of_job = False
                        log_error("keyboard interrupt raised in loop")
                        raise KeyboardInterrupt("simulated")
                    try_again = False
                except KeyboardInterrupt:
                    raise_keyboard_interrupt = True
        if Path(fn).exists():
            fn.rename(fn.with_suffix(fn.suffix + ".backup"))
        fn_temp.rename(fn)
        log_info("History was saved")

        if raise_keyboard_interrupt:
            log_error("Keyboard interrupt during history dumping")
            raise KeyboardInterrupt()

    def _convert_old_history(self):
        """Convert pre-rust history,
        by assuming that it's the same graph, and you changed *nothing* else in the upgrade
        """
        old_filename = self.dir_config.history_dir / "ppg_history.gz"
        new_filename = self.get_history_filename()
        if not old_filename.exists():
            return

        def failed(reason):
            sys.stderr.write("Could not convert history.\n")
            sys.stderr.write(f"Reason: {reason}.\n\n")
            sys.stderr.write(
                f"Safe choice: Remove {self.dir_config.history_dir}, rebuild everything\n"
            )
            sys.stderr.write(
                f"Unsafe choice: Remove {self.dir_config.history_dir}/ppg_history.gz, keeping ppg_history.2.gz.\n"
            )
            sys.stderr.write(
                "The old history had no knowledge of the set of inputs of a job\n"
            )
            sys.stderr.write(
                "So for the new one we must assume that those did not change.\n"
            )

            sys.stderr.write("And we can't do that automatically.\n")
            sys.stderr.write("\nAborting\n")
            sys.exit(1)

        if old_filename.exists() and new_filename.exists():
            raise ValueError(
                "Both new and old history existed. Presumably a failed conversion job. Aborting"
            )
        log_warning("Converting old history to new style")

        old = self._load_old_history(old_filename)  # todo
        new = {}
        # for job_id in self.jobs.keys():
        # new[job_id] = json.dumps(old[job_id], indent=2)

        for job_id, job in self.jobs.items():
            upstreams = job.upstreams
            # incoming_edges = sorted([x.job_id for x in upstreams])
            # "\n".join(incoming_edges)
            new[job_id + "!!!"] = Runner.get_job_inputs_str(self, job_id)
            new[job_id] = json.dumps(old[job_id][1])  # 1, that's the output hash.

            for input_name in self.job_inputs[job_id]:
                upstream_job_id = self.outputs_to_job_ids[input_name]
                key = f"{upstream_job_id}!!!{job_id}"
                new[key] = json.dumps(
                    {input_name: old[job_id][0][input_name]}
                )  # 0 - that's the input hashs.

        for k, v in new.items():
            if not isinstance(v, str):
                raise ValueError(k, v)
        self._save_history(new)

        keys = set(self.jobs.keys())
        if len(old) != len(keys):
            missing = set(old.keys()).difference(keys)
            gained = set(keys).difference(old.keys())
            return failed(
                f"Number of jobs changed. Was {len(old)} is now {len(keys)}. Missing {missing}. Gained: {gained}"
            )
        if keys != set(old):
            return failed("Job names changed")

        old_filename.rename(old_filename.with_suffix(".1.gz.converted_to_2_format"))

    @staticmethod
    def _load_old_history(path):
        with gzip.GzipFile(Path(path)) as of:
            old = {}
            try:
                while True:
                    key = pickle.load(of)
                    value = pickle.load(of)
                    if key in old:
                        raise KeyError()
                    old[key] = value
            except EOFError:
                pass
        return old

    def _resolve_dependency_callbacks(self):
        """jobs may depend on functions that return their actual dependencies.
        This resolves them
        """
        # we need this copy,
        # for the callbacks may create jobs
        # so we can't simply iterate over the jobs.values()
        with_callback = [j for j in self.jobs.values() if j.dependency_callbacks]
        # log_info(f"with callbacks {[j.job_id for j in with_callback]}")
        if not with_callback:
            return
        for j in with_callback:
            dc = j.dependency_callbacks
            j.dependency_callbacks = []  # must reset before run, might add new ones, right?
            for c in dc:
                # log_info(f"{j.job_id}, {c}")
                j.depends_on(c())
        self._resolve_dependency_callbacks()  # nested?

    def _install_signals(self):
        """make sure we don't crash just because the user logged of.
        Also blocks CTRl-c in console, and transaltes into save shutdown otherwise.
        """
        log_trace("_install_signals")
        install_pid = os.getpid()

        def hup(*args, **kwargs):  # pragma: no cover
            if os.getpid() != install_pid:
                return
            print(
                "user logged off - continuing run"
            )  # do not log, we must not aquire locker in a signal!

        def sigint(*args, **kwargs):
            if os.getpid() != install_pid:
                return
            print("Received SIGINT")
            if self.run_mode in (RunMode.CONSOLE, RunMode.CONSOLE_INTERACTIVE):
                if self._debug_allow_ctrl_c == "abort":  # pragma: no cover
                    print("CTRL-C from debug - calling interactive abort")
                    self.runner.interactive.reentrace_safe_command_from_signal(
                        [("abort", (None,))]
                    )
                # for testing the abort facility.
                elif self._debug_allow_ctrl_c == "stop":  # pragma: no cover
                    print("CTRL-C from debug - calling interactive stop")
                    self.runner.interactive.reentrace_safe_command_from_signal(
                        [("default", ()), ("stop", (None,))]
                    )
                    # for testing the abort facility.
                elif self._debug_allow_ctrl_c == "stop&abort":  # pragma: no cover
                    print("CTRL-C from debug - calling interactive stop")
                    self.runner.interactive.reentrace_safe_command_from_signal(
                        [("stop", (None,))]
                    )
                    # for testing the abort facility.
                    self._debug_allow_ctrl_c = "abort"

                else:
                    print("CTRL-C has been disabled. Type 'abort<CR>' to abort")
                # TODO remove
            else:  # pragma: no cover - todo: interactive
                print("CTRL-C received. Killing all running jobs.")
                if hasattr(self, "runner"):
                    print("calling abort")
                    self.runner.abort()

        def usr1(*args, **kwargs):
            if os.getpid() != install_pid:
                return
            print("USR1 received - aborting")
            self.runner.abort()

        if self.run_mode in (RunMode.CONSOLE, RunMode.CONSOLE_INTERACTIVE):
            self._old_signal_hup = signal.signal(signal.SIGHUP, hup)
        # if self.run_mode in (RunMode.CONSOLE, RunMode.NOTEBOOK):
        # we always steal ctrl c
        self._old_signal_int = signal.signal(signal.SIGINT, sigint)
        self._old_signal_usr1 = signal.signal(signal.SIGUSR1, usr1)

    def _restore_signals(self):
        """Restore signals to pre-run values"""
        log_trace("_restore_signals")
        if hasattr(self, "_old_signal_hup"):  # pragma: no branch
            signal.signal(signal.SIGHUP, self._old_signal_hup)
        if hasattr(self, "_old_signal_int"):  # pragma: no branch
            signal.signal(signal.SIGINT, self._old_signal_int)

    def add(self, job):
        """Add a job.
        Automatically called when a Job() is created
        """

        for output in job.outputs:
            if output in self.outputs_to_job_ids:
                # already being done somewhere else
                if self.outputs_to_job_ids[output] == job.job_id:
                    # but it is in essence the same same job
                    pass  # we replace the job, keeping upstreams/downstream edges
                else:
                    # if self.run_mode != RunMode.NOTEBOOK: todo: accept in notebooks by removing the other  jobs and warning.
                    raise exceptions.JobOutputConflict(
                        job, self.jobs[self.outputs_to_job_ids[output]]
                    )
            self.outputs_to_job_ids[output] = (
                job.job_id
            )  # todo: seperate this into two dicts?
        # we use job numbers during run
        # to keep output files unique etc.

        # this does not happen - jobs are deduped by job_id, and it's up to the job
        # to verify that it's not an invalid redefinition
        # if job.job_id in self.jobs and self.jobs[job.job_id] is not job:
        # if self.run_mode.is_strict():
        # raise ValueError(
        # "Added new job in place of old not supported in run_mode == strict"
        # f"new job: {job} id: {id(job)}",
        # f"old job: {self.jobs[job.job_id]} id: {id(self.jobs[job.job_id])}",
        # )
        if not self.running:  # one core., no locking
            job.job_number = self.next_job_number
            self.next_job_number += 1
        else:  # multiple jobGeneratingJobs might be creating jobs at the same time.
            with self.next_job_number_lock:
                job.job_number = self.next_job_number
                self.next_job_number += 1
        self.jobs[job.job_id] = job
        self.job_dag.add_node(job.job_id)
        # assert len(self.jobs) == len(self.job_dag) - we verify this when running

    def find_job_from_file(self, filename):
        if isinstance(filename, Path):
            filename = str(filename)
        return self.jobs[self.outputs_to_job_ids[filename]]

    def find_job(self, job_or_id_or_path):
        from .jobs import Job

        if isinstance(job_or_id_or_path, (str, Path)):
            return self.find_job_from_file(job_or_id_or_path)
        elif isinstance(job_or_id_or_path, Job):
            return job_or_id_or_path
        else:
            raise TypeError("job was not a Job nor a job_id")

    def add_edge(self, upstream_job, downstream_job):
        """Declare a dependency between jobs

        Implementation note:
        While we connect the DAG based on the Jobs,
        we calculate invalidation based on the job_inputs (see Job.depends_on),
        allowing a MultiFileGeneratingJob to (optionally) only invalidate some of it's downstreams.
        """
        if not upstream_job.job_id in self.jobs:
            raise KeyError(f"{upstream_job} not in this graph. Call job.readd() first")
        if not downstream_job.job_id in self.jobs:
            raise KeyError(
                f"{downstream_job} not in this graph. Call job.readd() first"
            )

        self.job_dag.add_edge(upstream_job.job_id, downstream_job.job_id)

    def has_edge(self, upstream_job, downstream_job):
        """Does this edge already exist?"""
        if not isinstance(upstream_job, str):
            upstream_job_id = upstream_job.job_id
        else:
            upstream_job_id = upstream_job
        if not isinstance(downstream_job, str):
            downstream_job_id = downstream_job.job_id
        else:
            downstream_job_id = downstream_job
        return self.job_dag.has_edge(upstream_job_id, downstream_job_id)

    def restart_afterwards(self):
        """Restart the whole python program afterwards?
        Used by the interactive console
        """
        self._restart_afterwards = True  # pragma: no cover - todo: interactive

    def dump_subgraph_for_debug(self, jobs):  # pragma: no cover
        """Write a subgraph_debug.py
        with a faked-out version of this graph.
        See Job.dump_subgraph_for_debug for details"""
        if jobs:
            jall = list(jobs)
        else:
            jall = list(self.jobs.keys())
        j1 = self.jobs[jall[0]]
        j1.dump_subgraph_for_debug(jall)

    def dump_subgraph_for_debug_at_run(self, job_ids):  # pragma: no cover
        """Write a subgraph_debug.py
        with a faked-out version of this graph.
        See Job.dump_subgraph_for_debug for details.

        This version dumps *after* the cleanup jobs
        and so on have been assembled (ie. when run is called)

        """
        if job_ids:
            jall = list(job_ids)
        else:
            jall = list(self.jobs.keys())
        # j1 = self.jobs[jall[0]]
        # j1.dump_subgraph_for_debug(jall)
        self._jobs_do_dump_subgraph_debug = jall

    def prune_all_unrelated_to(self, job_ids):
        """Remove all jobs that are not related to the given jobs.
        This takes effect *after* the cleanup jobs
        and so on have been assembled (ie. when run is called)

        """
        self._jobs_to_prune_unrelated = job_ids

    def _prune_unrelated(self):
        keep = []
        # graph_undirected = self.job_dag.to_undirected()
        # for job_id in self._jobs_to_prune_unrelated:
        #     keep.extend(networkx.node_connected_component(graph_undirected, job_id))
        for job_id in self._jobs_to_prune_unrelated:
            keep.extend(networkx.ancestors(self.job_dag, job_id))
            keep.extend(networkx.descendants(self.job_dag, job_id))

        keep = set(keep)
        log_info(f"keeping {len(keep)} jobs")
        for k in keep:
            log_info(f"keeping {k}")
        if len(keep) == len(self.jobs):
            log_warning("pruning all unrelated jobs, but *everything's connected*")
        to_remove = set(self.jobs.keys()) - keep
        for k in to_remove:
            log_info(f"pruning unrelated job {k}")
            self.jobs[k].prune()

    def prune_all_but(self, job_ids):
        self._jobs_to_prune_but = job_ids

    def _prune_but(self):
        """prune after _resolve_dependency_callback"""
        keep = set(self._jobs_to_prune_but)
        for j in self.jobs:
            if not j in keep:
                self.jobs[j].prune()

    def create_event_socket(self):
        if self.push_events:
            self._event_socket_path = self.dir_config.log_dir / "ppg_event_socket"
            import socket

            # only try to reconnect every few seconds.
            now = time.time()
            if (not hasattr(self, "_event_socket_last_connect_time")) or (
                now - self._event_socket_last_connect_time > 5
            ):
                self._event_socket_last_connect_time = now
                try:
                    self._event_socket = socket.socket(
                        socket.AF_UNIX, socket.SOCK_DGRAM
                    )
                except:
                    print("No listener found")

    def close_event_socket(self):
        if hasattr(self, "_event_socket"):
            self._event_socket.close()
            del self._event_socket

    def post_event(self, event_dict):
        if self.push_events:
            message = json.dumps(event_dict).encode("utf-8") + b"\n"
            if not hasattr(self, "_event_socket"):
                self.create_event_socket()
            if hasattr(self, "_event_socket"):
                try:
                    p = self._event_socket_path
                    self._event_socket.sendto(message, str(p))
                except (ConnectionRefusedError, TypeError, FileNotFoundError) as e:
                    # log_error(f"closed graph connection {self._event_socket_path}")
                    self._event_socket.close()
                    del self._event_socket
