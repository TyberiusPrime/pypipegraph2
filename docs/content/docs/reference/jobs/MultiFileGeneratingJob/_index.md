+++ 
title= "MultiFileGeneratingJob" 
Weight= 10 
+++

# ppg2.MultiFileGeneratingJob

A job that creates multiple files.

```python
pypipegraph2.MultiFileGeneratingJob(
    files: List[Path],  # todo: extend type attribute to allow mapping
    generating_function: Callable[List[Path]],
    resources: Resources = Resources.SingleCore,
    depend_on_function: bool = True,
    empty_ok=True,
    always_capture_output=True,
)
```

*files* may be either a list of filenames, or a dictionary 'friendly_key' -> filename.

Filenames must be either str or pathlib.Path.



This is passed to the generating_function as a list/dictionary of Path-objects.

If a list, the order is preserved.

For the other parameters see [FileGeneratingJob](../filegeneratingjob).






```
