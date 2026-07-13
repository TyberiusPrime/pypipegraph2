---
title: "4. DataJobs and io"
weight: 14
---

# Step 4 — DataJobs and the `io` object

So far every job produced *files*. Often you want to pass a rich Python value
— a dict, a dataframe, a fitted model — from one job to the next without
inventing a serialization format. That's what a `DataJob` is for.

## `DataJob` — a job that returns a Python object

A `DataJob` is a `FileJob` whose callback **returns** a value. ppg3 pickles
that return value into the store; consumers load it back with `io.load(...)`.

```python
def build_table(io):
    # do real work; return any picklable object
    return {"rows": [1, 2, 3], "label": "counts"}

table = ppg3.DataJob("table.pickle", build_table)
```

Note the `view=` here is a single **string** (`"table.pickle"`), not a dict —
a `DataJob` always has exactly one output, so you just name where it lands in
`outputs/`.

Downstream, declare the `DataJob` as an input and call `io.load`:

```python
def summarize(io):
    data = io.load("table")          # unpickles build_table's return value
    with open(io.path("summary"), "w") as fh:
        fh.write(f"{data['label']}: {sum(data['rows'])}")

ppg3.FileJob(
    view={"summary": "summary.txt"},
    run=summarize,
    inputs={"table": table},
)
```

`io.load("table")` returns the exact object `build_table` returned:
`{"rows": [1, 2, 3], "label": "counts"}`. The pickle round-trips through the
store, so the value is cached and content-addressed just like any file.

## The full `io` object

Every Python callback (`FileJob`, `DataJob`, `UnsandboxedJob`) receives one
`io` argument. Here is everything it offers:

```python
def my_job(io):
    # --- inputs ---
    path  = io.input("some_input")   # real path of a file/job input
    obj   = io.load("some_data")     # unpickle a DataJob (or any pickled input)

    # --- outputs ---
    out   = io.path("some_output")   # path to write a declared output to
    only  = io.path()                # shortcut when there's exactly one output

    # --- parameters ---
    cfg   = io.params["some_param"]  # a Params(...) input, by input name

    # --- tools ---
    exe   = io.tool("samtools")      # resolved path of a declared tool

    # --- logging ---
    logs  = io.log_dir               # a directory you may write log files into
```

A few rules the sandbox enforces for you:

- You may only touch paths `io` hands you. Reading or writing anything else is
  either invisible to the cache (so it would break reproducibility) or blocked
  outright.
- Ask for a name you didn't declare — `io.input("typo")` — and you get a clear
  `JobIOError` listing the names that *are* declared.
- `io.path()` with no argument only works when the job declares exactly one
  output; otherwise pass the name.

## Declaring tools

If your job shells out to an external binary and you want that binary's
identity folded into the cache key, declare it as a `ToolSpec` and reach it
via `io.tool(...)` (or `{tool:NAME}` in a `CommandJob`'s argv):

```python
from ppg3.tools import ToolSpec

# Tool references must be pinned (a 40-hex revision), because a tool is an
# input and must be reproducible — a bare branch name is a definition error.
samtools = ToolSpec.nix(
    "github:NixOS/nixpkgs/<40-hex-rev>#samtools", name="samtools"
)

ppg3.CommandJob(
    view={"sorted": "sorted.bam"},
    argv=["{tool:samtools}", "sort", "{in:reads}", "-o", "{out:sorted}"],
    tools=[samtools],
    inputs={"reads": some_bam_job},
)
```

Now an upgrade to `samtools` changes the tool's resolved hash, which changes
the input key, which triggers a rebuild — exactly as it should.

## A note on the other job kinds

You've now met the everyday jobs. For completeness, ppg3 also has:

- **`FetchJob(view, url, blake3=...)`** — download a file, verified against a
  pinned BLAKE3 hash. Under `frozen=False` you may omit `blake3=` once and let
  ppg3 pin it for you (trust-on-first-use); `frozen=True` requires the hash up
  front.
- **`GraphJob(fn)`** — a job whose callback *defines more jobs* at run time
  (dynamic graph expansion), for when the shape of the work depends on an
  earlier result.
- **`UnsandboxedJob(run, ...)`** — an explicit escape hatch that runs
  in-process, forked from the coordinator, for the rare case you genuinely
  need shared in-memory state. It warns at definition time; prefer `DataJob`.

Next: [what all of this looks like on disk]({{< relref "5-stores-views-generations" >}}).
