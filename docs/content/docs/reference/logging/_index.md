+++
title= "Logging"
+++

## Console 

The console log is (by default) very brief.

It outputs when a job's input history changed 
(so you know why a job was rerun).

It outputs a short stack trace when jobs fail,
and the name of the error log file (see below)
which contains more details.

And it shows a counter for
 
* T: Total jobs
* D: Done jobs
* R: Running jobs
* W: Waiting jobs
* F: Failed jobs

## Log file(s)

The log file (.ppg/logs/latest.messages) contains more info.

Compared to the console log it omits the counters,
but logs every job start/stop (together with it's runtime).

It's messages contain references into the ppg2 source, these
are abstracted away into latest.lookup to denoise the log.

You can increase the amount of logging by passing a lower 
low level to [ppg2.new()](../managing-pypipegraph#pypipegraph2new)

## Error log files

For each failed job, an error log in .ppg/errors/latest/<job_number>_exception.txt.

It contains the full stack trace (including locals!), the message of the exception, and - if available - stdout and stderr.


## Log rotation
By default the log files and error logs of the last few ppg2 runs are kept. Technically, we store each log under the run date, and symlink 'latest' to the latest one.


## Runtimes:
Every time a job takes more than a second, we log it's runtime at the end of the run to .ppg/logs/runtimes.tsv.

