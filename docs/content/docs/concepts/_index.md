---
weight: 30
bookFlatSection: true
title: "Concepts"
---
# Pypipegraph2 concepts

## The directed acyclic graph

The pypipegraph2 models 'Jobs', which are named, distinct units of computation that form
[direct acyclic graph](https://en.wikipedia.org/wiki/Directed_acyclic_graph) (DAG).

We distinguish jobs that are 'upstream' and 'downstream' in the DAG. 

The jobs directly (distance = 1) upstream of a job are called it's 'inputs'.

## Invalidation

When a job is evaluated (after all it's 'upstreams' have been successfully evaluated), 
it produces an output - typically a hash, but at least something 'compact' that allows for equality comparisons.

Now for each job we record the output (hashes) of it's inputs. 
If the inputs have changed we know we need to rerun the job.
We're calling this 'job invalidation'.

But if the output of that node does not change, it's downstreams do not need to be rerun.

Job invalidation also happens when the set of inputs changes, i.e. an input is added or removed.


## Job types

Independent of the python [job classes](../reference/jobs), ppg2 considers three types of jobs internally:

* Output jobs - Job's whose output must exist after the graph is evaluated.
* Ephemeral jobs - Jobs whose output must exist while it's direct downstreams are evaluated, but can be cleaned up right afterwards.
* Always jobs - Jobs that we always need to evaluate: invariants for example.

If we had only output & always, the internal evaluation engine would be straight forward.
But with the rules around ephemeral jobs - they are only done if a downstream 'needs' them, but if they're invalidated themselves,
they can invalidate downstreams - it's a fairly gnarly state machine.


# Job names (job_ids)

All our jobs have a unique name, which is how the ppg2 keeps track of them.
For the file related jobs they are (relative) paths (The constructors usually can take a pathlib.Path as well,
but the job_id is always a string). For jobs with multiple file outputs, the job_id is the sorted list of files,
concatenated with ':::'.

# Jobs vs files
A job may produce multiple files, and dependant jobs may depend on only a subset of them (using job_obj.depends_on(filename).
This is all handled behind the scenes.

# Comparing 'outputs'
Depending on the job type, we store more than a simple hash. 
For example for file generating jobs, we store the file size and modification time as well.
This allows us to not calculate the hash every time.
(We do not defend against modifications of files outside ppg runs that preserve these two meta datums).


## Process management

Modern systems have many cores.
Python comes from 1992 when the number of cores was 1.
Accordingly, python has a 'global interpreter lock' that effectively limits the concurrency of python programs, with the exception of C-extensions, to only one core.

Pypipegraph2 circumvents these limitations in two ways:

1. Jobs changing the ppg2 process are run in multiple threads, and things like hashing files happens in a C extension.

2. Jobs that are supposed to be isolated from the ppg2 process (e.g. all *FileGeneratingJobs) happen in a fork of the process.

The advantage of the fork is that the child process inherits
all loaded python objects for free, and effectively isolates against
all kinds of crashes.

The disadvantage of the fork is all the trouble of safely forking in the first place - forks only retain the main thread, file handles are trouble some, locks across forks spell inexplicable hang ups etc

It also effectively prevents any ppg2 from ever running on windows.

We also do our own process reaping - parallel to the main ppg2 process, there's a watcher spawned that makes sure that on shutdown (think abort), all children spawned by any of the forked processes are terminated as well

