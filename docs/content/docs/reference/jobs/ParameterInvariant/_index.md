+++
title= "ParameterInvariant"
Weight= 21
+++

# ppg2.ParameterInvariant

```python
ppg2.ParameterInvariant(job_id, paramaters)
```

A named job ('PI' + job_id) that triggers if the parameters have changed.

Parameters are 'frozen' into a hash  using [DeepHash](https://pypi.org/project/deepdiff/), 
which should work with all the pytohn value types.


Instead of calling job.depends_on(ParameterInvariant(other_job.job_id, parameters)),
you can also use other_job.depends_on_params(parameters).
See [depends_on_params](../all#depends_on_params) for details.
