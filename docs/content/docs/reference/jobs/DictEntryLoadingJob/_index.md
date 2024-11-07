+++
title= "DictEntryLoadingJob"
Weight= 15
+++

# ppg2.DictEntryLoadingJob

```python
ppg2.DictEntryLoadingJob(
        job_id,
        object,
        key
        load_function,
        resources: Resources = Resources.SingleCore,
        depend_on_function=True,
    ):
```

A DictEntryLoadingJob that stores it's results in `obj[<key>]`.

* obj - the oject to store data in
* key the attribute name to store the data at.

For further parameters, see [AttributeLoadingJob](../attributeloadingjob).

This job runs **in** the controlling process (see
[process-structure](../concept/process-structure)).

The _load_function_'s return value is stored 
in obj[key].

It's [DeepHashed](https://zepworks.com/deepdiff/current/deephash.html) representation is
used to calculate the [tracking hash](../../../concepts/tracking_hashes).

If you already have a hash handy, or need to work around an object type that is not supported by [DeepDiff/DeepHash](https://pypi.org/project/deepdiff/),
you can return a [ppg2.ValuePlusHash(value, hash_hexdigest)](../valueplushash) object to circumvent the pickling requirement.

Alternatively, you may return [UseInputHashesForOutput](../useinputhashesforoutput/), which signals the job to use a hash of it's input hashes as the tracking hash,
making it essentially 'transparent'. This is useful for objects that are currently not supported by DeepDiff.

