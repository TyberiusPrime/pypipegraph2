---
title: "6. CLI and watch mode"
weight: 16
---

# Step 6 — The CLI and watch mode

Two things maintain a project over time: the standalone **`ppg3` CLI**
(store- and generation-level operations, written in Rust) and the Python
**watch mode** (`python -m ppg3 watch`, for a fast edit-run loop). They are
separate tools with separate jobs.

## The `ppg3` CLI

The `ppg3` binary operates on stores and on a project's generation history. It
finds a project's stores by reading `.ppg3/config.json`, which `ppg3.run()`
writes for it on every run, so you don't have to repeat your store list on the
command line.

### Inspect and roll back generations

```bash
ppg3 generations list                 # every generation, newest first, current marked
ppg3 rollback                         # repoint outputs/ at the previous generation
ppg3 rollback 2                       # ...or at a specific generation number
ppg3 generations rm 2                 # drop a generation (unregisters its store roots)
ppg3 generations keep 5               # keep the newest 5, drop the rest
ppg3 generations keep 5 --keep-explicit   # ...but never drop pinned/explicit ones
```

A **rollback** is instant and non-destructive: it just swaps the `current`
symlink to point at an existing generation's view tree. Your `outputs/`
directory immediately reflects the older results; nothing is rebuilt.

### Explain why an output changed

```bash
ppg3 explain summary.txt
```

`explain` compares a view path between the current generation and the previous
one and tells you *why* its content differs — which input, recipe, tool, or
runtime hash changed. This is the "why did this rebuild?" button.

### Maintain the store

Generations protect entries from garbage collection; once you drop the
generations that referenced an entry, the entry becomes collectable. The store
commands operate on a raw store path (`--store`), independent of any project:

```bash
ppg3 store gc --store store                    # sweep unreferenced entries
ppg3 store gc --store store --max-size 50GB    # ...down to a size budget
ppg3 store gc --store store --dry-run          # show what would be swept
ppg3 store verify --store store                # check entries against their manifests
ppg3 diff-entries <oh1> <oh2> --store store    # compare two entries file-by-file
```

The usual maintenance rhythm: `ppg3 generations keep N` to prune old
snapshots, then `ppg3 store gc` to reclaim the space they were protecting.

## Watch mode — the edit-run loop

While developing a pipeline you want it to re-run automatically as you edit.
`python -m ppg3 watch` does that:

```bash
python -m ppg3 watch pipeline.py
python -m ppg3 watch pipeline.py --interval 0.2
python -m ppg3 watch pipeline.py -- --my-arg value   # args after the script go to it
```

Watch mode:

- re-runs `pipeline.py` whenever the script, a `ppg3.File(...)` input, or a
  job's source file changes (ppg3 tracks these **watched paths** for you as
  the graph is defined);
- keeps warm Python worker processes alive **between runs** (the coordinator
  *session*), so the second and later runs skip interpreter startup — an edit
  usually only pays for the jobs that actually changed;
- marks the generations it produces as **ephemeral**, so your generation
  history isn't flooded with one entry per keystroke. Your script doesn't need
  to know it's being watched — it's written for normal use.

When you're done, tear the warm session down (or just let the process exit):

```python
ppg3.session_stop()   # kill warm workers, clear the loader memo
```

## Where to go next

You now have the full loop: **define** jobs in a Python script → **run** to
build only what changed → **browse** results in `outputs/` → **maintain**
history and the store with the CLI.

From here, reach for:

- **`ppg3.Resources(cores=4)`** on a job to reserve slots from a named
  parallelism pool (set the pool sizes with `ppg3.new(parallelism=...)`).
- **`ppg3.Retain.Evict` / `ppg3.Retain.Pin("name")`** to control which
  outputs survive garbage collection.
- **`store=`** on a job to steer its output into a specific named store.
- **`PyEnv.nix(...)`** and **`ToolSpec.nix(...)`** for fully-hermetic,
  hash-pinned environments and tools.

The design documents (`PPG3_DESIGN.md`) and the implementation contract
(`CONTRACT.md`) go deeper on every one of these.
