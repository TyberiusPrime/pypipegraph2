+++ 
title= "DataLoadingJob" 
Weight= 14 
+++

ppg2.DataLoadingJob

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

The _load_function_ should return an object - it's pickled representation is
used to calculate the [tracking hash](../concepts/tracking-hash) of the job.

If you don't return a value (= return None), this job can never invalidate it's
downstreams.

If you already have a hash handy (or your data is not pickleable), you can
return a ppg2.ValuePlusHash(value, hash_hexdigest) object to circumvent the pickling requirement.
