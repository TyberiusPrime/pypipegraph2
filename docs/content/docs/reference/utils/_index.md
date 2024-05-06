## job_or_filename

Take a filename, or a job.

Return (Path(filename), dependency_for_that_file).
The dependency might be a Job that creates the file, or a FileInvariant

## assert_uniqueness_of_object

assert that a given object of type(object) with a .name attribute is unique 
within *this* ppg.global_pipegraph lifetime.

## flatten_jobs
Take an arbitrary deeply nested list of lists of jobs and return just the jobs


## pretty_log_errors

A decorator to capture exceptions and print really pretty tracebacks,
for use outside of jobs.

## wrap_for_function_invariant
When you need to pass a function + parameters which ends up in a FunctionInvariant.

Think a job with this function: ```lambda: some_func(filename)```

This marks it so that the automatically generated FunctionInvariant tracks 'somefunc', not the lambda.

