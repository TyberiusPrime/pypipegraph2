+++
title= "JobGeneratingJob"
Weight= 30
+++

# ppg2.JobGeneratingJob


```python
ppg2.JobGeneratingJob(
        job_id,
        gen_function,
    )
```


A job that runs in the current process,
runs once per ppg.run() and is meant to create more jobs.

These created jobs are than added to the graph, and it is reevaluated
after the current run is done.

Useful if you need to dynamically generate jobs based on the results of previous jobs.

