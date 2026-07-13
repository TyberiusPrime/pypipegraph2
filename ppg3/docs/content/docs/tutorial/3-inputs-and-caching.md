---
title: "3. Inputs and caching"
weight: 13
---

# Step 3 — Inputs and caching

This is the step that shows *why* ppg3 exists. Run the exact same script from
step 2 a second time:

```bash
python pipeline.py
```

```
RunResult(built=0, hits=2, failed=0, generation=2)
```

Nothing rebuilt. Both files came straight from the store as **hits**. The
generation number ticked up to `2` (each successful run produces a new
generation even when nothing was recomputed — more in
[step 5]({{< relref "5-stores-views-generations" >}})).

## How ppg3 decides: the input key

For every job, ppg3 computes an **input key** (`ik`) — a hash that captures
*everything that could change the output*:

```
input key  =  hash( recipe  +  every input's hash  +  tools  +  runtime env )
```

- **recipe** — for a `CommandJob`, the argv template; for a `FileJob`, the
  callback's source (or pickle). Change the command or the function body and
  the recipe hash changes.
- **inputs** — each declared input contributes a hash:
  - a **job** input contributes that job's *output hash*,
  - a **`File`** input contributes the file's content hash,
  - a **`Params`** input contributes a hash of its canonicalized value.
- **tools** and **runtime** (the `PyEnv`) contribute their resolved hashes.

Then ppg3 asks the store: *do you already have an entry for this input key?*

- **Hit** → reuse the stored output. No subprocess is spawned. (ppg3 just
  touches the entry's access time so the garbage collector knows it's live.)
- **Miss** → run the job in a sandbox, hash its output into an **output hash**
  (`oh`), publish a new store entry, and record the `ik → entry` mapping.

Because the key is content-addressed, a second identical run is *all hits* —
which is exactly what you just saw.

## Adding a parameter input

Let's give the summary job a real knob. `ppg3.Params(...)` turns any
JSON-like value into a cache-tracked input:

```python
def make_summary(io):
    with open(io.input("greeting")) as fh:
        text = fh.read()
    n = io.params["cfg"]["n"]
    with open(io.path("summary"), "w") as fh:
        fh.write(text.strip().upper() + str(n))

ppg3.FileJob(
    view={"summary": "summary.txt"},
    run=make_summary,
    inputs={
        "greeting": greeting,
        "cfg": ppg3.Params({"n": 1}),
    },
)
```

Two things changed:

- The input `"cfg"` is now a `ppg3.Params({"n": 1})`. Params inputs don't
  arrive as files — they're delivered to the callback via `io.params`, keyed
  by input name. So `io.params["cfg"]` is `{"n": 1}`.
- The output now depends on `n`.

Run it (this is a new recipe, so the summary rebuilds once):

```
RunResult(built=1, hits=1, failed=0, generation=3)
```

`outputs/summary.txt` now reads `HELLO1`. The greeting was a hit; only the
summary rebuilt.

## Watch selective rebuilds

Now flip the parameter to `2` and run again:

```python
inputs={"greeting": greeting, "cfg": ppg3.Params({"n": 2})}
```

```
RunResult(built=1, hits=1, failed=0)
```

Only the **summary** rebuilt — its input key changed because `cfg` changed.
The greeting job's key was unaffected, so it stayed a hit. `outputs/summary.txt`
now reads `HELLO2`.

## Flip back — and the deeper guarantee

Set `n` back to `1` and run once more:

```
RunResult(built=0, hits=2, failed=0)
```

**Zero builds.** Even though `n=1` is "old", its output was never thrown away
— it still lives in the store under its content-addressed key. Flipping back
to a previously-seen configuration is instant. This is the constructive-trace
guarantee: results are keyed by *what produced them*, not by *when*, so ppg3
never has to trust a timestamp and never recomputes something it has already
seen.

## The kinds of input you can declare

| Input value                | Meaning                                                      | Reaches the callback as         |
| -------------------------- | ----------------------------------------------------------- | ------------------------------- |
| a **job** (`greeting`)     | depend on that job's whole output                           | `io.input("name")` → a path     |
| `job["output_name"]`       | depend on just one named output of a multi-output job       | `io.input("name")` → a path     |
| `ppg3.File("data.csv")`    | depend on a file *outside* the graph (hashed by content)    | `io.input("name")` → a path     |
| `ppg3.Params(value)`       | depend on a literal value (canonicalized, hashed)           | `io.params["name"]` → the value |

`ppg3.File(...)` inputs are also **watched paths** — see
[watch mode]({{< relref "6-cli-and-watch" >}}).

Next: [passing rich Python values between jobs with DataJobs]({{< relref "4-datajobs-and-io" >}}).
