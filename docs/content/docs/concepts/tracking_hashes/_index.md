# Tracking hashes


For each and  every job, ppg2 tracks multiple hashes.


* The output hash (what did this job produce last time).
* Input hashes for each incoming job.
* A hash on the input-job-job_ids, allowing to detect if inputs have been added or removed


Depending on the job type, the hashes are different kinds of 'enhanced hashes', not just hexdigests,
allowing an efficient comparisons and recalculations of hashes.

For files and bytes (as in Python `bytes' objects), we use xxh3_128.

File related jobs also store the file size and modification time, and recalculate the hash only if either has changed.

Jobs that return python objects, such as [AttributeLoadingJobs](../../jobs/attributeloadingjobs) use [DeepDiff's DeepHash](https://pyppi.org/project/deepdiff/) to calculate hashes.

FunctionInvariants store the byte-code keyed on python version as well as the source code. They are therefore 
capable of ignoring no-op changes like comments or docstrings.




