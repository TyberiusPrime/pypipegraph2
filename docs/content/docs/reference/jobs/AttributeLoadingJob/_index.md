+++
title= "AttributeLoadingJob"
Weight= 15
+++

ppg2.AttributeLoadingJob

```python
ppg2.AttributeLoadingJob(
        job_id,
        obj,
        attr_name
        load_function,
        resources: Resources = Resources.SingleCore,
        depend_on_function=True,
    ):
```

A DataLoadingJob that stores it's results in obj.<attr_name>

* obj - the oject to store data in
* attr_name the attribute name to store the data in obj.<attr_name>

For further parameters, see [DataLoadingJob](../dataloadingjob).

All the DataLoadingJob remarks apply here as well.

