---
weight: 30
bookFlatSection: true
title: "Concepts"
---
# Pypipegraph2 concepts

## What is modeled

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


