+++
title= "CachedDictEntryLoadingJob"
Weight= 16
+++

# ppg2.CachedDictEntryLoadingJob

```python
def CachedDictEntryLoadingJob(
    cache_filename,
    object,
    attribute_name,
    data_function,
    depend_on_function=True,
    resources: Resources = Resources.SingleCore,
):
```

Similar to [CachedAttributeLoadingJob](../cachedattributeloadingjob), but stores the data in an entry on a dict.

See CachedAttributeLoadingJob for the arguments.



