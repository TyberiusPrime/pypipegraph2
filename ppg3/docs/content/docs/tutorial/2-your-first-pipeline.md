---
title: "2. Your first pipeline"
weight: 12
---

# Step 2 — Your first pipeline

We'll define two jobs and run them. Add to `pipeline.py` (below the
`ppg3.new(...)` from step 1):

```python
# 1. A shell command that writes a greeting file.
greeting = ppg3.CommandJob(
    view={"greeting": "greeting.txt"},
    argv=["/bin/sh", "-c", "echo hello > {out:greeting}"],
)

# 2. A Python callback that reads the greeting and writes a summary.
def make_summary(io):
    with open(io.input("greeting")) as fh:
        text = fh.read()
    with open(io.path("summary"), "w") as fh:
        fh.write(text.strip().upper())

ppg3.FileJob(
    view={"summary": "summary.txt"},
    run=make_summary,
    inputs={"greeting": greeting},
)

# 3. Run it.
result = ppg3.run()
print(result)
```

Run the script:

```bash
python pipeline.py
```

You'll see something like:

```
RunResult(built=2, hits=0, failed=0, generation=1)
```

and two new things on disk: a `store/` directory and an `outputs/` symlink.
Open `outputs/summary.txt` — it says `HELLO`.

Let's understand every piece.

## The `view=` argument — naming your outputs

Every job that produces files declares a **view**: a dict mapping an
**output name** to a **view-relative path**.

```python
view={"greeting": "greeting.txt"}
#      ^output name  ^where it shows up in outputs/
```

- The **output name** (`"greeting"`) is the handle other jobs and the job's
  own code use to refer to the file.
- The **view path** (`"greeting.txt"`) is where the file appears under
  `outputs/` after a successful run. It can include subdirectories, e.g.
  `"results/counts.tsv"`.

The view is what turns an opaque content-addressed store into a browsable
directory of named results. More on that in
[step 5]({{< relref "5-stores-views-generations" >}}).

## `CommandJob` — running a shell command

```python
ppg3.CommandJob(
    view={"greeting": "greeting.txt"},
    argv=["/bin/sh", "-c", "echo hello > {out:greeting}"],
)
```

`argv` is the command, as a list. The `{out:greeting}` **placeholder** is
replaced, at run time, with the real path the command must write to. ppg3
never lets you hard-code output paths — you always write through a
placeholder, so the same recipe can be materialised anywhere.

The placeholders you can use in an `argv` string are:

| Placeholder      | Resolves to                                        |
| ---------------- | -------------------------------------------------- |
| `{out:NAME}`     | the output path for view entry `NAME`              |
| `{out}`          | the single output, if the job declares exactly one |
| `{in:NAME}`      | the path of a declared input named `NAME`          |
| `{tool:NAME}`    | the resolved path of a declared tool `NAME`        |

If you prefer typed helpers over string placeholders, `ppg3.In("name")`,
`ppg3.Out("name")`, and `ppg3.Tool("name")` produce the same wire form when
placed as list items in `argv`.

## `FileJob` — running a Python callback

```python
def make_summary(io):
    with open(io.input("greeting")) as fh:
        text = fh.read()
    with open(io.path("summary"), "w") as fh:
        fh.write(text.strip().upper())

ppg3.FileJob(
    view={"summary": "summary.txt"},
    run=make_summary,
    inputs={"greeting": greeting},
)
```

A `FileJob` runs your Python function `make_summary` in a fresh, sandboxed
subprocess. The function receives a single argument, conventionally called
`io`, which is your only channel to the outside world:

- `io.input("greeting")` → the real path of the input named `greeting`.
- `io.path("summary")` → the real path you must write the `summary` output
  to. (`io.path()` with no name works when there's exactly one output.)

Your callback must be **self-contained**: it should only reference its `io`
argument, its own arguments, and things it imports inside its body. ppg3 ships
the function to the subprocess (either by pickling it or by shipping its
source), so a callback that closes over a random module-level variable may not
survive the trip. Module-level functions with body-local imports are the safe,
always-valid form.

## Wiring jobs together with `inputs=`

```python
inputs={"greeting": greeting}
```

By passing the `greeting` **job object** as an input, we told ppg3 two things:

1. **Ordering** — `make_summary` cannot run until `greeting` has produced its
   output.
2. **Cache dependency** — `make_summary`'s cache identity now depends on
   `greeting`'s output. If the greeting changes, the summary is rebuilt.

Inside the callback, `io.input("greeting")` resolves to the greeting job's
output file. (When a job declares several outputs, use `greeting["name"]` to
depend on just one of them.)

## `ppg3.run()` — executing the graph

`ppg3.run()` runs the current graph to completion and returns a `RunResult`:

```python
result = ppg3.run()
result.built        # view paths that were (re)built this run
result.hits         # view paths served from the store cache
result.failed       # {view_path: error message} for anything that failed
result.generation   # the generation number this run produced (1, 2, 3, …)
```

On our first run, `built` holds both files and `hits` is empty. `generation`
is `1`.

### When a job fails

If any job fails, `run()` raises `PPGRunError` and **leaves your view
untouched** — a generation is all-or-nothing, so you never end up with a
half-updated `outputs/` directory:

```python
from ppg3.run import PPGRunError

try:
    ppg3.run()
except PPGRunError as exc:
    print(exc.result.failed)   # what broke
    # exc.result.generation is None; outputs/ still points at the last good run
```

Next: [run it again and watch the cache work]({{< relref "3-inputs-and-caching" >}}).
