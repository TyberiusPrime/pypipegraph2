+++
title = "FAQ"
+++

## I'm defining jobs in a loop, but they all have the same output.

This is a common python pitfall. Python function definition only binds variables
the **first** time. You need to pass the variable as default parameters.

So instead of

```python
for ii, element in enumerate(whatever):
    ppg.FileGeneratingJob(str(ii) + '.txt', lambda of: of.write_text(element))
```

you need to use:
```python
for ii, element in enumerate(whatever):
    ppg.FileGeneratingJob(str(ii) + '.txt', lambda of, element=element: of.write_text(element))
```

to get the correct element.

## I'm experiencing the weirdest hangs

Your jobs are not using cpu time, but not returning either?

Chances are you have a lock that's stuck across the fork all FileGeneratingJobs perform. 

See the [lock](../concepts/forbidden/#holding-a-lock-across-forks) section of the forbidden actions page.


## My pipegraph evaluation fails with an internal error.

You see something like this:

``` Internal error. Something in the pipegraph2 engine is wrong. Graph execution aborted.```

This is a bug in pypipegraph. Please report it on our [github issue tracker](https://github.com/TyberiusPrime/pypipegraph2/issues).

Background: The [ephemeral jobs](../concepts/#job-types) push the complexity of deciding wether a job needs to be done from something fairly trivial into a nightmarish complexity. It's not yet perfect.

And the bugs always happen when you have a few ten-thousand nodes in the graph - but every single one of them has boiled down to a small example.

If this happens, there are other options besides 
sending you as a complete snapshot of your project.
(E.g. `graph.dump_subgraph_for_debug` and `graph.dump_subgraph_for_debug_at_ run`)

Contact the authors, and we will walk you threw them.

In the meantime, you can often get the ppg2 unstuck 
by deleting the right output files.

