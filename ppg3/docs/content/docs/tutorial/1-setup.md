---
title: "1. Setup"
weight: 11
---

# Step 1 — Setup

## Install

ppg3 ships as a Python package (`ppg3`) backed by a compiled Rust extension
(`ppg3._core`). Install it into the environment you plan to run pipelines
from:

```bash
pip install ppg3          # or your project's usual install step
```

Almost all of ppg3's front-end — defining jobs, canonicalizing inputs,
hashing recipes — is pure Python and works even without the compiled
extension. Only the final `ppg3.run()` needs `ppg3._core`. If the extension
isn't built, everything imports fine and `run()` raises a clear
`CoreNotAvailable` error.

## The shape of a pipeline script

A ppg3 pipeline is an ordinary Python script. It does three things, in order:

1. **Create a graph** with `ppg3.new(...)`.
2. **Define jobs** by constructing job objects (`CommandJob`, `FileJob`, …).
3. **Run** the graph with `ppg3.run()`.

Create a working directory and a file `pipeline.py`:

```python
import ppg3
from ppg3.tools import PyEnv

graph = ppg3.new(
    stores=[ppg3.Store("main", "store")],
    default_python=PyEnv.current(),
    project_dir=".ppg3",
    frozen=False,
)
```

That's the whole preamble. Let's unpack each argument, because these choices
determine *where your results live* and *how reproducible they are*.

### `stores=[...]` — where built artifacts are cached

A **store** is a content-addressed directory of build results. You pass one or
more:

```python
ppg3.Store("main", "store")          # name, path
ppg3.Store("shared", "/nfs/ppg-cache", readonly=True)   # read from, don't write to
```

- The **name** is a stable label used in metadata (so a store can move on disk
  without invalidating anything).
- The **path** is where the store's `v1/` layout is created (on first write).
- `readonly=True` stores are consulted for cache **hits** but never written
  to — handy for a shared team cache you read but only CI populates.

When several stores are listed, ppg3 looks for a hit across all of them and
writes new entries to the first writable one (or the one a job names with
`store=`).

### `default_python=PyEnv.current()` — which interpreter runs Python jobs

Python jobs (`FileJob`, `DataJob`, `FetchJob`) run in a subprocess. `PyEnv`
says which interpreter and what to preload:

```python
PyEnv.current()                       # the interpreter running this script
PyEnv.current(preload=["numpy"])      # ...with numpy imported before the job
PyEnv.nix("github:you/env#default")   # a pinned Nix flake environment
```

`PyEnv.current()` is *weakly hermetic* — reproducible enough for local work.
`PyEnv.nix(...)` pins the whole environment by hash and is fully hermetic.
`CommandJob`s don't need a `PyEnv` at all.

### `project_dir=".ppg3"` — the per-project scratch and view directory

This is where ppg3 keeps the stat-cache, per-run working directories, and the
**view** (the symlink tree pointing at your latest results). It is safe to
delete — it is a cache, not a source of truth. The store holds the real data.

### `frozen=` — reproducibility guard

`frozen` controls whether "trust-on-first-use" conveniences are allowed
(e.g. a `FetchJob` without a pinned hash). Its default is *context-aware*:

- **Interactive terminal** → `frozen=False` (conveniences allowed).
- **Non-interactive / CI** → `frozen=True` (everything must be pinned).

We set `frozen=False` explicitly here so the tutorial behaves the same whether
or not you run it under CI.

---

`ppg3.new()` also makes this graph the **current graph** — like ppg2, job
constructors find it implicitly, so you rarely pass the graph around by hand.

Next: [define some jobs and run them]({{< relref "2-your-first-pipeline" >}}).
