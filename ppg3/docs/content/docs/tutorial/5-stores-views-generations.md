---
title: "5. Stores, views, generations"
weight: 15
---

# Step 5 — Stores, views, and generations

You've been running pipelines and reading results out of `outputs/`. Let's
look at what ppg3 actually wrote to disk, because the three concepts —
**store**, **view**, **generation** — are the whole model.

After the runs from steps 2–3, your working directory looks like:

```
pipeline.py
store/                    # the content-addressed STORE
outputs/  ->  .ppg3/views/current      # the VIEW (a symlink)
.ppg3/                    # per-project state
```

## The store — immutable, content-addressed results

The store is where built artifacts actually live. Its layout, under
`store/v1/`:

```
store/v1/
  entries/<oh>/            # one directory per OUTPUT HASH
    data/                  # the job's actual output files (read-only after publish)
    manifest.json          # what produced this entry, and the hash of every file
  inputs/<ik>  ->  entries/<oh>/     # INPUT KEY -> entry (a symlink; the cache index)
  logs/                    # captured job logs
  roots/                   # GC-protection: which generations reference which entries
  intents/  pins/          # more GC bookkeeping
```

Two hashes do all the work:

- **`oh` (output hash)** addresses an entry by its *content*. Two jobs that
  produce byte-identical output share one entry — automatic deduplication.
- **`ik` (input key)** is the cache index from
  [step 3]({{< relref "3-inputs-and-caching" >}}). `inputs/<ik>` is a symlink
  pointing at the entry that input key resolved to. A cache **hit** is
  literally "an `inputs/<ik>` symlink already exists."

Entries are **immutable**: once published, `entries/<oh>/data/` is made
read-only. Nothing overwrites a result; new work creates new entries. This is
what makes flipping a parameter back and forth free — old entries are never
destroyed by a run, only (later, deliberately) by garbage collection.

## The view — a browsable snapshot of named results

The store is content-addressed, so on its own it's unreadable: a directory of
hash-named folders. The **view** projects it back into the human-friendly
paths you declared in each job's `view=` map.

`outputs/` is a symlink chain:

```
outputs/  ->  .ppg3/views/current  ->  .ppg3/views/3   (the latest generation)
```

and `.ppg3/views/3/` is a tree of symlinks pointing into the relevant
`store/v1/entries/<oh>/data/` files, arranged under the view paths you named:

```
.ppg3/views/3/
  greeting.txt  ->  ../../../store/v1/entries/<oh_greeting>/data/greeting.txt
  summary.txt   ->  ../../../store/v1/entries/<oh_summary>/data/summary.txt
```

So when you `open("outputs/summary.txt")`, you're reading straight out of an
immutable store entry — no copy was made.

## Generations — every run is a numbered snapshot

Each successful `run()` writes a new **generation**: `.ppg3/views/1`,
`.ppg3/views/2`, `.ppg3/views/3`, … — that's the `result.generation` number
you've been seeing. A generation is:

- **Complete** — it names an entry for *every* view path in that run.
- **Atomic** — `outputs/` is switched to the new generation with a single
  symlink rename. Readers never see a half-updated view. (And if a run fails,
  no new generation is written at all — `outputs/` keeps pointing at the last
  good one.)
- **Immutable** — generation 2's view is still there after generation 3 is
  written. You can roll back to it.

Because `outputs/` points one hop away (at `current`, which points at the
generation), publishing a new generation only ever repoints `current` —
`outputs/` itself is never touched after the first run.

Each generation also **registers roots** in the store for the entries it
references. That's how the garbage collector knows an entry is still reachable:
an entry with no root from any live generation is eligible for collection.

### Putting it together

```
run()  ─┬─►  for each job: compute ik ─► store lookup
        │        hit  ─► reuse entry (touch atime)
        │        miss ─► sandbox run ─► hash output (oh) ─► publish entry ─► link inputs/<ik>
        │
        └─►  assemble ViewSpec (view path ─► oh) ─► write generation N
                 register roots ─► build symlink tree views/N ─► atomically repoint current
```

- **Delete `store/`** and you lose your cache — the next run rebuilds
  everything.
- **Delete `.ppg3/`** and you lose the view/generation history, but the store
  still has every entry, so the next run is all hits and just rebuilds the
  view.
- **Delete `outputs/`** and the next run recreates the symlink.

Next: [maintaining all of this with the CLI]({{< relref "6-cli-and-watch" >}}).
