+++
title= "CachedAttributeLoadingJob"
Weight= 16
+++

# ppg2.CachedAttributeLoadingJob

```python
def CachedAttributeLoadingJob(
    cache_filename,
    object,
    attribute_name,
    data_function,
    depend_on_function=True,
    resources: Resources = Resources.SingleCore,
):
```

Similar to [CachedDataLoadingJob](../cacheddataloadingjob), but stores the data in an attribute of an object.

Unlike the CachedDataLoadingJob you only need one function

 * data_function that returns the value to store & assign.

 See [AttributeLoadingJob](../attributeloadingjob) for the other parameters.

