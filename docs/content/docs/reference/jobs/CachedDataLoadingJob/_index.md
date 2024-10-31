+++
title= "CachedDataLoadingJob"
Weight= 15
+++

# ppg2.CachedDataLoadingJob

```python
ppg2.CachedDataLoadingJob(
    cache_filename,
    calc_function,
    load_function,
    depend_on_function=True,
    resources: Resources = Resources.SingleCore,
):
```

 * cache_filename - where to store the (pickled) output of the calc_function
 * calc_function - a callback tha calculates the data to store
 * load_function - a callback that takes one parameter - the (unpickled) calc function output, and assigns it somewhere in the current processs.

 See [DataLoadingJob](../dataloadingjob) for the other parameters.

 Creates two jobs: One that calculates the data, and stores it in cache_filename (using pickle),
 and one that loads the data from cache_filename, and passes it to load_function

 This function returns a namedtuple with two fields: `.load` & `.calc` for the two jobs created.

The load job calculates it's hash from the byte representation of the pickled object, beware
if you're for example returning dictionaries with undefined insertion order. 
