+++
title= "FileInvariant"
Weight= 20
+++

# ppg2.FileInvariant

``` python
ppg2.FileInvariant(filename: str | Path)
```

An [invariant](../concepts/invariants) that triggers if a file was changed.

A file will only be rehashed, if it's modification time or size have changed.

Changing just the file time will not lead to an invalidation of downstream jobs.

Instead of creating a FileInvariant(filename), you can also use job.depends_on_file(filename).
See [depends_on_file](../all#depends_on_file) for details.

