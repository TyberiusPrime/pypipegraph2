+++
title = "Common job methods"
weight = 1
+++


# Job.depends_on_file(filename)

Introduce a [FileInvariant](../fileinvariant) as dependency on Job. 

Returns a namedtuple (invariant, self), so you can continue your fluent call chain with either job.

# Job.depends_on_params(parameters)

Introduce a [ParameterInvariant](../parameterinvariant) called 'PI' + Job.job_id as dependency on Job.

Returns a namedtuple (invariant, self), so you can continue your fluent call chain with either job.

# Job.depends_on_func(func, name):

Introduce a [FunctionInvariant](../functioninvariant) as dependency on Job.

Returns a namedtuple (invariant, self), so you can continue your fluent call chain with either job.





...
