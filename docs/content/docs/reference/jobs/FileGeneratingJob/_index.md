+++
title= "FileGeneratingJob"
Weight= 10
+++

# ppg2.FileGeneratingJob

A [Job](../concepts/jobs) that creates a single file.

```python
ppg2.FileGeneratingJob(
    job_id: str,
    callback_function: Callable[[Path], None],
    resources: Resources,
    depend_on_function: bool = True,
    empty_ok: bool = False,
    always_capture_output: bool = False
) -> None
```

* job_id -> The filename to create. Maybe a string or path
* `callback_function` takes a single parameter: The `Path` of the file to create.
* resources: See [Resources](../resources).
* depend_on_function: Whether to create a FunctionInvariant for the callback_function. See [FunctionInvariant](../functioninvariant).
* empty_ok: If it's ok when the job creates an empty file. Otherwise an exception is raised and the job fails.
* always_capture_output - if True, the job's stdout/stderr are stored on `job.stdout`/`job.stderr`. Otherwise they're discarded in a successful run


