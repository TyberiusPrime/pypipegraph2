# Interactive

When running in [RunMode.CONSOLE](../..//managing-pypipegraph/#runmodeconsole),
you have an interactive shell while the graph is running.

* Press _Enter_ to see the currently running jobs.
* Type Help and press _Enter_ to list the available commands.


## Commands

 * abort - kill all currently running jobs, do not start new ones and finish graph execution as soon as safely possible
 * stop - Let the current jobs finish, but do not start new ones, finishing once the current batch is done
 * die - Kill the ppg2 manager process (and all it's children). Do not record history. Perfectly **unsafe**.
 * again - after the graph ends (because it has evaluated all jobs, was stopped or aborted), restart the current process.
 * stop_and_again - combines stop and again
 * kill - Kill a running job by it's number (not job_id). Numbers are listed in the _enter_ view.


