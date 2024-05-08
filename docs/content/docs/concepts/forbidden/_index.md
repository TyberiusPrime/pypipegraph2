+++ 
title= "Forbidden actions" 
+++

# Forbidden actions

Unfortunately, the way ppg2 is running your computation
does put on a light burden of prohibited actions on the user.

Blame the POSIX standard.

## Changing the cwd

You must not change the current working directory in jobs that run inside the
ppg2 process (e.g. DataLoading, CachedDataLoading's load job, JobGeneratingJo).

This is because these run multi-threaded.

There is detection for this in ppg2, but at that point the cat's already out of
the bag.

Note that changing the cwd within a forked job (*FileGenerating) is fine.

See [process management](../#process-management) for more details.

## Holding a lock across forks

The forking nature of ppg2 means that in-process jobs (e.g. DataLoading,
CachedDataLoading's load job, JobGeneratingJob) must be ready to be forked at
any moment.

That means they must not hold any locks.

That applies to logging - if you call the python logging functions from within a
DataLoadingJob, you are liable for hanging forked processes.
(Which is a bit of a shame since the stdout of DataLoadingProcesses is shared with all other in-process jobs, so you can't just print to stdout either. PR welcome).
