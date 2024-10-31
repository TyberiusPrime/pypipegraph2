+++ 
title= "DataLoadingJob" 
Weight= 14 
+++

# ppg2.DataLoadingJob

```python
ppg2.DataLoadingJob(
        job_id,
        load_function,
        resources: Resources = Resources.SingleCore,
        depend_on_function=True,
    ):
```

- job_id: A job_id
- load_function: A parameter less function that stores data somewhere.
- resources: See [Resources](../resources).
- depend_on_function: Whether to create a FunctionInvariant for the
  load_function. See [FunctionInvariant](../functioninvariant).

This is an [ephemeral job](../../concepts/ephemeral-jobs), but there is no
unloading.

This job runs **in** the controlling process (see
[process-structure](../concept/process-structure)).

Prefer an [AttributeLoadingJob](../attributeloadingjob) if possible, since that
can unload the data as well.

The _load_function_ should return an object - it's [DeepHashed](https://zepworks.com/deepdiff/current/deephash.html) representation is
used to calculate the [tracking hash](../concepts/tracking-hash) of the job.

Alternatively, you may return
[UseInputHashesForOutput()](../UseInputHashesForOutput/), which signals the job
to use a hash of it's input hashes as the [tracking hash](../../../concepts/tracking_hashes/), making it essentially
'transparent'. This is useful for objects that are currently not supported by
DeepDiff.

Returning None is no longer supported. 

You must return a value, or [UseInputHashesForOutput](../UseInputHashesForOutput/).

Use a constant value if you really don't want this job to not invalidate downstreams ever.


If you already have a hash handy (or your data is not pickleable), you can
return a ppg2.ValuePlusHash(value, hash_hexdigest) object to circumvent the pickling requirement.
