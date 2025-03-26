+++
title= "FunctionInvariant"
Weight= 22
+++

# ppg2.FunctionInvariant

```python
ppg2.FunctionInvariant(job_id, function)
```

Function Invariants change their output hash if the function they cover changes.

Technically, they compare the python byte code (if you did not change the python version in between runs)
or alternatively the source code (if you did).

Byte code is created using the [dis](https://docs.python.org/3/library/dis.html) module from the python's standard library.

Instead of calling job.depends_on(FunctionInvariant(name, func)),
you can also use other_job.depends_on_func(parameters).
See [depends_on_params](../all#depends_on_params) for details.

Note that FunctionInvariant can also be called with just a function (in which case the name is automatically deduced,
or with reversed parameters).

Most jobs autamatically create FunctionInvariants for their callbacks, unless, `depend_on_function=False` is set in their creation.

## Function restrictions

### The problem

By default, any tracked function must not access non-local variables.

This prevents two classes of bugs:

a) Defining jobs & functions in a loop, but binding only the last variable:

```python
for ii in range(10):
    ppg.FileGeneratingJob(str(ii), lambda of: of.write_text(str(ii))
```

would write the 10 files containing '9'.

(Even worse, if you redefine ii within the same scope later on, but before the jobs run,
it will use that value!)

b) Dragging in global / non local variables that you didn't intend to share between jobs.

They do get added to the FunctionInvariants hash's input,
but in a str()ified manner. This has always been suboptimal - a separate ParameterInvariant
is the way to model these, since it then supports deep hashes.

### The solutions

Pypipegraph2 will raise a FunctionUsesUndeclaredNonLocalsError listing the offending variables
if it detects such a situation.

You then can either

a) declare them as 'default' parameters, which makes python copy them when the function is being defined

or

b) pass their names in a list as allowed_non_locals to the Job constructor.

Generally a) is the preferred alternative, as b) will suffer from the loop problem above.
That way the value also won't be tracked by the FunctionInvariant.

Note that though referenced classes, functions and modules are also non-local, we ignore
them during the check (and during the hash calculation) for convenience.
But beware redefining functions - that has the same binding errors as the example in a) in the problem section.
