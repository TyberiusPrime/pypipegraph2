---
weight: 20
bookFlatSection: true
title: "Tutorials"
---
# Tutorial


This tutorial takes you step by step trough a pypipegraph2 toy example.

## The very first pipegraph

Let's assume we have an input file 'input.dat', in which for reasons we want to count
the lower case letters. We want to do this in a pipegraph, to properly keep track of the dependencies.

Here is input.dat
```text
HelloWorld
```

And here's the python function to count lower case letters. It's a stand in for any kind of brittle 
bioinformatics software that transforms files.

```python
import collections
def count_letters(input_string):
   counts = collections.Counter()
   for c in input_string:
       if c.islower():
           counts[c] += 1
       else:
           raise ValueError("invalid letter")
    return counts
```

Now in an ideal world, this would work:

```python
import pypipegraph2 as ppg
import json
import collections


def count_letters(input_string):
    counts = collections.Counter()
    for c in input_string:
        if c.islower():
            counts[c] += 1
        else:
            raise ValueError("invalid letter")
    return counts


input_file = Path("input.dat")
ppg.new()
ppg.FileGeneratingJob(
    "output.dat",
    lambda of: of.write_text(json.dumps(count_letters(input_file.read_text()))),
).depends_on_file(input_file)
ppg.run()
```

But alas, the input file is not quite compatible with the count_letters function,
as we see when we run our file wit python:

```python
   0:00:00.125824s | Job failed  : 'output.dat'

	More details (stdout, locals) in .ppg/errors/2024-05-07_12-55-54.2642035/0_exception.txt
	Failed after 0.013s.

Exception: ValueError invalid letter
        Traceback (most recent call last):
        /home/tp/upstream/ppg2_rust/python/pypipegraph2/jobs.py":798, 
in run
                  795 
                  796                         try:
                > 797                             
self.generating_function(*input)
                  798                             stdout.flush()
                  799                             stderr.flush()
                  800                             # else:
        /home/tp/upstream/ppg2_rust/ex/shu.py":21, in <lambda>
                  18 ppg.FileGeneratingJob(
                  19     "output.dat",
                > 20     lambda of: of.write_text(json.dumps(count_letters(input_file.read_text()))),
                  21 ).depends_on_file(input_file)
                  22 ppg.run()
                  23 
        /home/tp/upstream/ppg2_rust/ex/shu.py":14, in count_letters
                  11             counts += 1
                  12         else:
                > 13             raise ValueError("invalid letter")
                  14 
                  15 
                  16 input_file = Path("input.dat")
        Exception (repeated from above): ValueError invalid letter
  0:00:00.146061s | At least one job failed
Traceback (most recent call last):
  File "/home/tp/upstream/ppg2_rust/ex/shu.py", line 23, in <module>
    ppg.run()
  File "/home/tp/upstream/ppg2_rust/python/pypipegraph2/__init__.py", line 135, in run
    return global_pipegraph.run(
           ^^^^^^^^^^^^^^^^^^^^^
  File "/home/tp/upstream/ppg2_rust/python/pypipegraph2/graph.py", line 184, in run
    raise JobsFailed(do_raise[1], exceptions=do_raise[2])
pypipegraph2.exceptions.JobsFailed: At least one job failed
```

## Transforming the input

We need to transform the input file so it works with the count_letters function.

```python
import pypipegraph2 as ppg
import json
import collections
from pathlib import Path



def count_letters(input_string):
    counts = collections.Counter()
    for c in input_string:
        if c.islower():
            counts[c] += 1
        else:
            raise ValueError("invalid letter")
    return counts


input_file = Path("input.dat")
ppg.new()


job_transform = ppg.FileGeneratingJob('transformed.dat', 
    lambda of: of.write_text(input_file.read_text().lower().strip())
).depends_on_file(input_file).self # depends_on_file returns a named tuple (invariant, self)

job_output = ppg.FileGeneratingJob(
    "output.dat",
    lambda of: of.write_text(json.dumps(count_letters(job_transform.files[0].read_text()))),
).depends_on(job_transform) # depends_on returns self.

ppg.run()
```

This time we get the expected output: no output on the console and a file output.dat containing:

```json
{"h": 1, "e": 1, "l": 3, "o": 2, "w": 1, "r": 1, "d": 1}

```

## Building intuition on recalculations

At this point, you have a graph that looks like this:
{{< mermaid class="optional" >}}
graph TD;
    FileInvariant:input.dat-->FileGeneratingJob:transformed.dat;
    FileGeneratingJob:transformed.dat-->FileGeneratingJob:output.dat;
    FunctionInvariant:FItransformed.dat-->FileGeneratingJob:transformed.dat;
    FunctionInvariant:FIoutput.dat-->FileGeneratingJob:output.dat;
{{< /mermaid >}}

And you should investigate the following questions:

* What happens if you change input.dat?
* What happens when you change the function doing the transformation
* Does anything happen when you change the function count_letters?
* How do you get it to track changes in count_letters()

To investigate, you might want to add a 'tracking' function that adds a counter to a file,
as a side effect to your job functions (promote them from lambdas to real defs!)

```python
def count(filename):
    try:
        count = int(Path(filename).read_text().strip())
    except:
        count = 0
    count += 1
    Path(filename).write_text(str(count))
```
