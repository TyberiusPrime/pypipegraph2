## pypipegraph2.new


```python
def new(
    cores=reuse_last_or_default,
    run_mode=reuse_last_or_default,
    dir_config=reuse_last_or_default,
    log_level=reuse_last_or_default,
    allow_short_filenames=reuse_last_or_default,
    log_retention=reuse_last_or_default,
    cache_dir=reuse_last_or_default,
    prevent_absolute_paths=reuse_last_or_default,
    report_done_filter=reuse_last_or_default,
):
```

 * cores - How many cores can the pipegraph use (default: all of them)
 * run_mode - RunMode.CONSOLE | RunMode.NOTEBOOK | RunMode.NONINTERACTIVE
 * dir_config - DirConfig(".ppg") - where are the runtime files stored (history, log, errors, ...)
 * log_level - how much do we log (default: logging.INFO)
 * allow_short_filenames - allow short (<= 3 letter) filenames (default: False)
 * log_retention - how many runs of logfiles & errors do we keep (default 3)
 * prevent_absolute_paths - prevent absolute paths in the pipeline (default: False)
 * report_done_filter - (unused)

The default for all values is 'reuse_last_or_default' - so they start with default,
but ppg.new(cores=4), followed by another ppg.new() will also use 4 cores.

### RunMode

#### RunMode.CONSOLE

In the default runmode (CONSOLE), you can not redefine jobs. 
You also get an interactive shell while the pipeline is running to interact with jobs (see [interactive](interactive)).
Ctrl-C is disabled in this mode.
Signal Hup is ignored, so the pipegraph does not end when the terminal is closed.
(But you still need to do have your graph running in  [screen](https://www.gnu.org/software/screen/)/[tmux](https://github.com/tmux/tmux/wiki)/[dtach](https://github.com/crigler/dtach)/([abduco](https://github.com/martanne/abduco) to get be able to get it back).

Errors / failed jobs are printed briefly to the console, and more detail is provided in a job specific logfile.

#### RunMode.NOTEBOOK
In this mode you *can* redefine jobs (throwing away the former job definition, but keeping all links).
There is no interactive shell. Ctrl-C aborts the pipegraph execution and kills all currently running jobs.


#### RunMode.NONINTERACTIVE
For internal testing.

### DirConfig
The ppg by default writes logs to .ppg/log, history to .ppg/history and errors to .ppg/errors.
Job output during job-runs is stored in .ppg/run (the `run_dir`).
If you want to run multiple independend pipegraphs in one project, you can use DirConfig to overwrite
all these at once with a new prefix directory.
The DirConfig also allows you to overwrite each folder independently.

```python
 def DirConfig.__init__(
        self,
        root: Union[Path, str] = ".ppg",
        log_dir: Union[Path, str, None, bool] = False,
        error_dir: Union[Path, str, None, bool] = False,
        history_dir: Union[Path, str, None, bool] = False,
        run_dir: Union[
            Path, str, None, bool
        ] = False,  # used by FileGeneratingJobs to e.g. recorde stdout
        cache_dir="cache",
        )
```


## pypipegraph2.run

```python
def pypipegraph2.run(
    print_failures=True,
    raise_on_job_error=True,
    event_timeout=1,
    dump_graphml=None,
):
```

 * print_failures - if true, errors are logged with log_error (and show up on the console). If False, they're log_debug (which may show up in your .ppg/logs.messages if you have set log_level to debug in ppg2.new().  
   Errors are stored in per-job error logs either way.
 * raise_on_job_error: Wether to raise an exception (after processing all jobs) if any job failed. If False, the result value of run is a dict of `job_id` -> RecordedJobOutcome. If True, this value is ppg.global_pipegraph.last_run_result
 * event_timeout - (unused)
 * dump_graphml - Dump the final structure of the user facing graph into .ppg/logs/latest/<log_file_name>).pre_prune.graphml.





### pypipegraph2.RecordedJobOutcome
An named tuple (outcome, error) showing the actual outcome of a job.

The outcome is o ppg2.JobOutcome, which is one of:

 * .Success  - Everything ok.
 * .Skipped - this job did not need to run and was not run.
 * .Failed  - this job failed. Error message is in .error (on the RecordedJobOutcome)
 * .UpstreamFailed - a job before this job failed and this job was not attempted.
 * .Pruned - This job was pruned (created, but job.prune() was called), and therefore not run.


## pypipegraph2.global_pipegraph

The current pipegraph, created with pypipegraph2.new()

A useful place to put 'singletonizations' that work per graph.
