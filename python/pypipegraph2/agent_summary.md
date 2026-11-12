# pypipegraph2 in one page (for coding agents)

A dense digest of the parts of ppg2 that are easy to get wrong when writing
pipelines. Read it before writing or changing job code.

This file ships inside the package (next to `__init__.py`); it is the one
copy, the docs site only points here. Written against 3.5.1; every claim
points at its source so it can be re-checked when the code moves: `jobs.py`
is the module next to this file, "concepts" / "forbidden" / "tracking_hashes"
are pages of the ppg2 docs (`docs/content/docs/concepts/` in the repository),
`tests/` is the repository's test folder.

## The script lifecycle

- `ppg.new()` starts a fresh graph and **throws away every job defined before**.
  Define jobs, then `ppg.run()`. Re-running a script re-defines everything.
- Jobs are identified by `job_id`: the output path for file jobs, the sorted
  output paths joined by `:::` for multi-file jobs, the name you give
  otherwise (concepts page, "Job names").
- A job reruns when an input's output hash changed, or when the *set* of its
  inputs changed (concepts page, "Invalidation"). Adding or removing a
  `depends_on` therefore invalidates the job once.

## Where your code runs

- `*FileGeneratingJob`, `MultiFileGeneratingJob`, `PlotJob`'s plot job,
  `CachedDataLoadingJob`'s calc: run in a **fork**. They see every Python
  object loaded before the fork; anything they set on Python objects is lost.
  Safe place for heavy imports that start things (rpy2/R, CUDA, ...).
- `DataLoadingJob`, `AttributeLoadingJob`, `DictEntryLoadingJob`,
  cache *load* jobs, `JobGeneratingJob`: run **in the main process, in
  threads**. Don't `chdir`, don't hold locks, don't use `logging`
  (concepts/forbidden). Their return value is what downstream forks see.

## Dependencies: what gets tracked, and what doesn't

A job only reruns for things that are dependencies. There are four ways to
make something a dependency:

| what changed | how to track it |
|---|---|
| another job's output | `job.depends_on(other_job)` (or a filename: depends on just that file of a multi-file job) |
| a file not made by the graph | `job.depends_on_file(path)` / `ppg.FileInvariant(path)` |
| a value (params, config, R code as a string, a sorted list) | `job.depends_on_params(value)` / `ppg.ParameterInvariant(name, (param1, ...))`: DeepHash of the value |
| a function's code | automatic for the job's own callback; `job.depends_on_func(f)` for helpers it calls |

`depends_on` returns the job; `depends_on_func` / `_params` / `_file` return a
namedtuple `(invariant, self)`: chain on `.self` (`jobs.py`: `DependsOnInvariant`, `Job.depends_on_func`).

### FunctionInvariant: what is in the hash

(`jobs.py`: `_FunctionInvariant.run`, `_FunctionInvariant.extract_closure`)

- **Bytecode** (per Python version), so comments and docstrings don't matter.
- **The function's closure cells**: variables of an *enclosing function* that
  the function reads. Each is `repr()`ed into the hash (dicts and sets
  sorted first, `at 0x...` addresses cut off). Skipped: `self`, anything with
  `__code__` (functions), modules.
- **Not in the hash**:
  - **default argument values** (`__defaults__` is never read),
  - **module-level globals**, even when allowed via `allowed_non_locals`
    (globals are not closure cells),
  - **functions it calls**: each needs its own `depends_on_func`.

### The non-locals check

(`jobs.py`: `_verify_function_outside_variables`)

A tracked function that reads a variable from outside its own scope raises
`FunctionUsesUndeclaredNonLocalsError` at job creation, unless:

- the name is bound as a **default parameter** (`def f(of, params=params)`),
  which is preferred: it also fixes the loop late-binding bug, or
- the name is listed in `allowed_non_locals=[...]` on the job constructor
  (`PlotJob`: `allowed_non_locals_plot` / `_cache`).

Always allowed (never an error): modules, functions, classes, `Job` objects,
dppd's `dp` proxy.

`depends_on_func(f)` runs the same check on `f` (it builds a
`FunctionInvariant` with `check_non_locals=True`).

**Consequence:** the common pattern `def calc(of, objects=objects,
params=params)` passes the check, but **neither value is tracked**. Add
`depends_on_params(...)` for whatever in them should trigger a rerun.

### Loops

Bind the loop variable as a default, or every job sees the last value:

```python
for name in names:
    ppg.FileGeneratingJob(f"{name}.txt", lambda of, name=name: of.write_text(name))
```

## Job types that are tuples, not jobs

- `ppg.PlotJob(...)` returns `PlotJobTuple(plot, cache, table)`
  (`jobs.py`: `PlotJobTuple`, end of `PlotJob`):
  - `.plot`: the `FileGeneratingJob` that renders. Its plot callback is tracked
    automatically. Helpers called from the plot function: `pj.plot.depends_on_func(helper)`.
  - `.cache`: a `CachedJobTuple(load, calc)`. Data inputs and helpers of
    the calc function go on **`pj.cache.calc`**: `.depends_on(...)`,
    `.depends_on_params(...)`, `.depends_on_func(...)`.
  - `.table`: writes `<output>.tsv` with the plotted data (or `None`).
  - The tuple itself has no `depends_on*` methods. Passing a `PlotJobTuple`
    or `CachedJobTuple` *into* `depends_on` raises `TypeError` (`Job.depends_on`);
    calling `depends_on*` *on* one raises an `AttributeError` saying where to put it
    (`_JobTupleHints`). Either way:
    pick `.plot` / `.cache.calc` / `.table` / `.load`.
- `CachedDataLoadingJob` / `CachedAttributeLoadingJob` return
  `CachedJobTuple(load, calc)`: dependencies of the computation go on `.calc`.

## Multi-file outputs

- `MultiFileGeneratingJob(files, func)`: `files` is a list or a dict
  `key -> path`; `func` gets the same shape back as `Path`s.
- Depending on one file of it (`other.depends_on(path)`) only invalidates on
  that file.

## SharedMultiFileGeneratingJob (content-addressed store)

(`jobs.py`: `SharedMultiFileGeneratingJob`)

- `SharedMultiFileGeneratingJob(store_dir, files, func)`: outputs live at
  `store_dir/by_input/<input hash>/<file>` (a symlink into `done/`), shared
  between projects, garbage-collected when unused.
- **The real paths are only known after the job ran in this run.** `job.files`
  holds placeholder paths (`.../__never_placed_here__/...`). Don't use
  `ppg.util.job_or_filename(job)` for these either: it returns `job.files[0]`.
- The one way to get a real path: `job["key"]` (or `job[0]` for list outputs)
  **inside the callback of a job that `depends_on(job)`**. This is what the
  tests do (`tests/test_shared_jobs.py:165`):

  ```python
  shared_job = ppg.SharedMultiFileGeneratingJob(store, {"out": "x.csv"}, make)

  def use(of, shared_job=shared_job):
      data = shared_job["out"].read_text()
      ...

  ppg.FileGeneratingJob("result.txt", use).depends_on(shared_job)
  ```
  The job's `output_needed` is always true, so its `_target_folder` is set
  in every run before downstream jobs fork.
- `depends_on` on a shared job drops its cached target folder (`SharedMultiFileGeneratingJob.depends_on`).
  Add all its dependencies before using it.

## Loading jobs and hashes

- `DictEntryLoadingJob(job_id, obj, key, func)`: `obj[key] = func()`, in the
  main process. Downstream invalidation uses a DeepHash of the value.
- Return `ppg.UseInputHashesForOutput(value)` when DeepHash can't handle the
  value (e.g. large DataFrames) or it is fully determined by the inputs: the
  hash is then derived from the job's inputs. Return `ppg.ValuePlusHash(value,
  hexdigest)` if you already have a hash.
- A `<file>.sha256` beside a file (no older than the file) is used instead
  of hashing it (concepts/tracking_hashes).

## Checklist before handing over a pipeline script

1. Every callback reads only its arguments, defaults, modules and functions.
   No surprise globals.
2. Every value bound as a default that should trigger reruns is also in a
   `depends_on_params`.
3. Every helper a callback calls has a `depends_on_func` on the job that calls it
   (for `PlotJob`: `.plot` or `.cache.calc`, never the tuple).
4. Loop variables are bound as defaults.
5. Paths of shared-job outputs are resolved inside callbacks, not at graph
   definition time.
6. Heavy or stateful imports (rpy2, ...) happen inside forked callbacks.
