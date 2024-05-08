---
title: Introduction
type: docs
---

# PyPipegraph2

{{< columns >}}
## A modern make for the hashes-are-cheap age

PyPipegraph2 tracks every single one of our inputs and outputs with hashes,
and rebuilds just what is necessary. 

<--->

## Advanced python introspection

PyPipegraph2 jobs wrap python functions. And they track the source code of those functions.
If it changes, the job [invalidates](concepts#invalidation) it's downstreams.

{{< /columns >}}

# What is tracked?
{{< mermaid class="optional" >}}
graph TD;
    Input_Files-->Intermediary_and_Temporary_Files;
    Intermediary_and_Temporary_Files-->Output_files;
    Python_Functions-->Output_files;
    Python_Functions-->Intermediary_and_Temporary_Files;
    Parameters-->Output_files;
    Parameters-->Intermediary_and_Temporary_Files;
    Intermediary_and_Temporary_Files-->Output_files;
{{< /mermaid >}}



* input files (d'oh), 
* intermediary files, 
* output files,
* parameters,
* python function definitions,


## Minimal example

```python
import pypipegraph2 as ppg

ppg.new()

job_a = ppg.FileGeneratingJob("a.txt", lambda of: of.write_text("hello"))
job_b = ppg.FileGeneratingJob("b.txt", 
                lambda of: of.write_text(Path('a.txt').read_text() + " world")
        ).depends_on(job_a)

ppg.run()
```


## Diátaxis 

Our documentation follows the [Diátaxis](https://diataxis.fr) standard, 
so head on by using the side bar to the left to learn more.


## Source code
Source code for the MIT-Licenced PyPipegraph2 is available on [GitHub](https://github.com/TyberiusPrime/pypipegraph2).

