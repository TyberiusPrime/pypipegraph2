+++
title= "SharedMultiFileGeneratingJob"
Weight= 30
+++

# ppg2.SharedMultiFileGeneratingJob


```python
ppg2.SharedMultiFileGeneratingJob(
    output_dir_prefix: Path,
    files: List[Path],  
    generating_function: Callable[[List[Path]], None],
    resources: Resources = Resources.SingleCore,
    depend_on_function: bool = True,
    empty_ok: bool = True,
    always_capture_output: bool = True,
    remove_build_dir_on_error: bool = True,
    remove_unused: bool = True,
    )
```


A job that can be shared by multiple Pipegraphs,
for example if you have to prepare expensive 'indices' for genomes.

The Store (output_dir_prefix) is content-addressed, and contains
symlinks from an *input* hash of the SharedMultiFileGeneratingJob to the actual outputs.

That way you get full dedup, and can share the outputs between multiple pipegraphs.

There is auto-garbage-collection ('remove_unused').




