+++
title= "FunctionInvariant"
Weight= 22
+++

# ppg2.FunctionInvariant

```python
ppg2.FunctionInvariant(job_id, function)
```

Function Invariants change their output hash if the function they cover changes.

Technically, they compare the python byte code (if you did not change the python version in between runs)
 or alternatively the source code (if you did).

 Byte code is created using the [dis](https://docs.python.org/3/library/dis.html) module from the python's standard library.

Instead of calling job.depends_on(FunctionInvariant(name, func)),
you can also use other_job.depends_on_func(parameters).
See [depends_on_params](../all#depends_on_params) for details.

Note that FunctionInvariant can also be called with just a function (in which case the name is automatically deduced,
or with reversed parameters).
