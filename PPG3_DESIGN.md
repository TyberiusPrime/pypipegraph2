# ppg3 — design document

Status: draft for implementation. Audience: implementation agents working in
parallel. Every section marked **DECISION** is settled; do not re-litigate it
in code, raise an issue instead. Open questions are collected in §14 and
nowhere else.

Heritage: pypipegraph2. The prototype for the core mechanism already exists
in-tree as `SharedMultiFileGeneratingJob` (`python/pypipegraph2/jobs.py`):
input-hash-keyed symlinks into output-hash-named directories, with a usage
registry. ppg3 generalizes that to *every* job and deletes the machinery it
replaces (mutable output tree, global history file, invalidation state
machine).

---

## 1. Vision

ppg2 is a *verifying-trace* build system: a global history of hashes decides
what to rerun, and outputs are overwritten in place. Exactly one
configuration of the pipeline can exist on disk at a time; flipping a
parameter back and forth recomputes every time; a crashed job has already
deleted its old output.

ppg3 is a *constructive-trace* build system: outputs are kept in an
input-addressed store, the visible output tree is a generated view of links
into that store, and "running the graph" is memoized function application.

Consequences we are explicitly buying:

- Switching parameters back = re-linking existing store entries. Instant.
- Multiple configurations coexist; views are cheap; A/B is a first-class
  workflow.
- Runs are transactional: the view is always a consistent state; rollback to
  any retained generation is one command.
- The global history file, the `!!!` edge keys, invalidation propagation,
  and the Ephemeral demand-propagation state machine are **deleted**, not
  ported. Temp-file semantics become cache-eviction policy.
- Renaming a job or an output never causes recomputation (identity is
  inputs, not names).
- Stores can be shared between projects, machines, and users
  (SharedMultiFileGeneratingJob's purpose, generalized), and layered.

## 2. Non-negotiable principles

**DECISION — Determinism is mandatory.** A job is a pure function from
declared inputs to outputs. Same input key must produce byte-identical
output. A detected violation is a *build failure* with a diff report — never
a warning, never tolerated dedup-miss. Tools that embed timestamps, host
names, locales, or randomness get fixed (flags, env pinning, seed inputs),
not coddled. The escape hatch for inherently-impure steps (network
downloads) is the fixed-output job (§7.6), which trades impurity for an
up-front declared output hash — the Nix model.

**DECISION — Jobs are sandboxed.** A job sees exactly its declared inputs at
virtualized paths, its declared tools, a writable output directory, a tmpfs,
and nothing else. No network unless fixed-output. Undeclared dependency =
file-not-found at build time, not a latent correctness bug.

**DECISION — Tools are inputs.** The executables a job runs participate in
its input key. The blessed mechanism is a Nix store path (from a flake
reference); a hash-of-binary fallback exists for non-Nix environments.

**DECISION — Multiple stores, layered.** An ordered list of stores; lookup
walks the list, writes go to the first writable store, hits from remote
stores are substituted (copied/linked) into the local one.

## 3. Terminology

- **input key**: hash identifying a job invocation (§5). Shallow: derived
  from the *output hashes of direct parents*, not transitive leaves — this
  preserves ppg2's early cutoff (upstream reruns with identical output ⇒
  downstream still hits).
- **store entry**: immutable directory holding one job invocation's outputs
  plus its manifest.
- **output hash**: hash of a store entry's content manifest (relative path →
  file hash, mode bit).
- **view**: a generated tree of links presenting selected store entries
  under human-chosen paths. Views have numbered **generations**.
- **root**: something that protects store entries from GC (view generations,
  pins, run leases).
- **substitution**: satisfying a local miss from a lower-priority store.

## 4. Store layout

**DECISION** — one store is one directory:

```
<store>/
  v1/                          # layout version; bump = migration tool
    inputs/<ik>                # symlink -> ../entries/<oh>   (the memo table)
    entries/<oh>/              # immutable after publish; chmod a-w
      data/...                 # the job's output files
      manifest.json            # §10
    staging/<hostname>-<pid>-<rand>/   # builds in progress
    leases/<runid>.json        # live-run GC protection, heartbeat mtime
    pins/<name> -> ../entries/<oh>
    gc.lock                    # advisory lock for GC and publish rename
```

- `ik` = input key (hex). `oh` = output hash (hex).
- Publish protocol: build in `staging/`, hash content, `rename()` into
  `entries/<oh>` (if it already exists: **verify byte-identity of the
  content manifests; mismatch = corrupt store, hard error**; identical =
  discard staging, dedup achieved), then atomically create the
  `inputs/<ik>` symlink. If `inputs/<ik>` already exists and points to a
  *different* `oh`: **determinism violation — hard error** with a
  file-level diff of the two entries (§9).
- Entries are made read-only on publish (`chmod -R a-w`). GC is the only
  deleter.

**DECISION — hash algorithm: BLAKE3, 256-bit, hex.** Rationale: stores are
shared across users/machines; a non-cryptographic hash (ppg2's xxh3) allows
cache poisoning by collision. xxh3 remains fine for the local
stat-cache (§10.3). File hashing must stream and may parallelize.

### 4.1 Multiple stores

**DECISION** — configuration is an ordered list:

```python
ppg3.new(stores=[
    Store("~/.ppg3/store"),                      # first writable = write target
    Store("/lab/shared/ppg3-store"),             # writable shared store
    Store("s3://bucket/ppg3", readonly=True),    # substitute-only
])
```

- Lookup: first store whose `inputs/<ik>` exists wins.
- Substitution from a non-first store: copy (reflink if same fs) the entry
  into the write store, **re-hashing content and verifying it equals `oh`**
  (integrity check; authenticity/signing is v2, §14).
- Writes (publish, leases, pins created by this run) go to the write store
  only. Shared stores are published to explicitly (`ppg3 store push`).
- Remote (`s3://`, `http://`) stores implement `get(ik) -> manifest`,
  `fetch(oh) -> tar-stream`; they are substitute-only in v1.

## 5. Input key derivation

**DECISION** — the input key is `blake3` of a canonical JSON document:

```json
{
  "ppg3_key_version": 1,
  "job_recipe": "<recipe hash>",
  "inputs": {"<input name>": "<parent output hash or leaf hash>", ...},
  "tools": {"<tool name>": "<tool hash>", ...},
  "runtime": {"python_env": "<PyEnv tool hash>",
              "preload": ["numpy", "pandas"],
              "shim": "<worker-shim version>"},
  "env": {"<var>": "<value>", ...},
  "outputs_declared": ["relative/path", ...]
}
```

- Canonical JSON: UTF-8, sorted keys, no whitespace, no floats (floats are
  forbidden in keys; parameters serialize floats via `struct.pack('<d')`
  hex — reuse the audit's ParameterInvariant recommendations).
- `job_recipe` = strict hash of the job's function (ppg2's
  `extract_strict_hash` semantics: source-derived, bytecode-noise-free) or,
  for command jobs, the canonicalized argv/script with input/output
  placeholders (never real paths — see §6.2).
- `inputs`: for a parent job, its store entry's *output hash* (shallow key —
  early cutoff). If the job depends on a *subset* of a parent's outputs
  (ppg2's per-file `depends_on(mfg["name"])`), the value is the hash of the
  named files only, taken from the parent's content manifest.
- Leaf inputs (source files under version control or user data): content
  hash via the stat-cache (§10.3).
- `env`: only explicitly declared variables. Nothing is inherited (§6).
- Parameters are just a leaf input kind: canonicalized (closed type set:
  None/bool/int/float/str/bytes/list/tuple/dict/set/frozenset/Enum/
  dataclass; everything else rejected loudly; opt-in `__ppg3_hash__`
  protocol).

The full key document (not just its hash) is stored in the manifest — this
is what makes `ppg3 explain` a dictionary diff instead of archaeology.

## 6. Sandboxing

**DECISION — executor: Linux user namespaces via bubblewrap (`bwrap`),
vendored invocation, no daemon.** Fallback executor (`sandbox="none"`, for
CI containers without userns) runs in a staged directory with the same
virtual layout but without enforcement; it prints a prominent warning and
marks the manifest `"sandboxed": false`. macOS/Windows: out of scope v1.

### 6.1 The job's world

```
/ppg/in/<input-name>/...   # read-only bind mounts of parent entries' data/
/ppg/out/                  # writable; becomes the store entry's data/
/ppg/tools/<tool-name>/    # read-only bind mounts (nix store paths mounted
                           #   at their real /nix/store path *additionally*,
                           #   since nix binaries hardcode those)
/tmp                       # private tmpfs
/dev/{null,zero,urandom}   # minimal /dev; urandom is allowed — determinism
                           #   is enforced by output comparison, not by
                           #   pretending entropy doesn't exist
```

- Env: `PATH` assembled from tool inputs' `bin/`; `HOME=/tmp`; `TMPDIR=/tmp`;
  `TZ=UTC`; `LC_ALL=C.UTF-8`; `SOURCE_DATE_EPOCH=0`; plus declared `env`
  key/values. Nothing else.
- No network namespace unless the job is fixed-output (§7.6).
- The process runs as the invoking user inside a userns (no root).
- cwd = `/ppg/out`.

### 6.2 Path hygiene

Jobs must never learn a store path. Callbacks/commands receive `/ppg/...`
virtual paths only. Because every invocation sees the *same* virtual paths,
tools that embed their input paths into outputs embed stable strings —
this defuses the classic "output differs because the store path leaked"
problem instead of fighting it per-tool.

### 6.3 The data-loading tension (resolved)

ppg2's `DataLoadingJob` is not merely an in-process job: its purpose is that
*forked* FileGeneratingJobs inherit the loaded objects via COW. Sandboxed
jobs are exec'd from a clean interpreter, so that channel does not exist in
ppg3 — sandboxing and fork-COW data sharing are fundamentally incompatible.
One of them had to go, and per §2 it is fork-COW. The replacement is a
three-tier data plane:

1. **`DataJob` — the default replacement.** The calc callback runs
   *sandboxed*, exactly like a FileJob; its output is a serialized artifact
   in the store (pickle by default; Arrow/npy/HDF5 encouraged). Consumers
   declare it as a normal input and call `io.load("name")`, which
   deserializes with per-worker-process memoization. Physical-memory
   sharing is recovered through the **page cache**: N sandboxed jobs
   mmap-ing the same read-only store file share pages, which for
   mmap-friendly formats (Arrow, npy) approaches COW performance without
   fork. This is ppg2's `CachedDataLoadingJob` promoted to the primitive,
   with the uncached variant deleted.
2. **The loader layer** for genuinely in-process consumers (GraphJob
   callbacks, coordinator-side plot assembly, interactive exploration):
   runs in the coordinating process, keyed by the same input-key scheme,
   memoized per `(process, ik)`, never a store entry. Purity is encouraged
   by localscope (ported unchanged) but not enforced — nothing it produces
   reaches the store except through declared inputs of sandboxed jobs.
3. **`UnsandboxedJob` — the explicit escape hatch** for the case DataJob
   cannot serve: a python-object working set so large that per-consumer
   deserialization is prohibitive and no mmap-able serialization exists.
   It forks from the coordinator (COW, sees loader results), is marked
   `"sandboxed": false` in its manifest, and still passes through
   publish-time determinism enforcement. It is greppable, warned about at
   definition time, and inherits ppg2's fork-under-threads hazards — the
   docs say so.

**DECISION**: no shared-memory object plane (plasma-style object store,
shm-backed pickles) in v1. Mmap-able formats plus the page cache cover the
common case with a fraction of the machinery; revisit only with profiling
evidence.

### 6.4 Forkserver executor (warm starts, divergent pythons)

Exec-ing a cold interpreter per job pays import cost every time — seconds to
tens of seconds for scientific stacks, which is exactly what ppg2's
fork model amortized. ppg3 recovers that via per-environment **template
processes**, and gains something ppg2 never had: different jobs in one
graph running under *different* python environments.

**DECISION — one template per `(PyEnv, preload-list)` pair.**

- Lazily started on first demand; killed at run end; restarted on death.
- The template is launched **inside the same hygiene as jobs** (read-only
  nix store mounts, scrubbed env per §6.1, no network, `HOME=/tmp`): its
  fork-time state must be a pure function of `(PyEnv, preload, shim
  version)` — which is precisely the `runtime` field of the key document
  (§5). A template whose state depended on host config files would poison
  every key derived under it.
- The template is **guaranteed single-threaded**: it imports the preload
  list, then blocks on its control socket. It never runs user code
  directly and never starts threads. All forking in ppg3 happens here —
  the multi-threaded coordinator never forks. This eliminates ppg2's
  fork-under-threads hazard class by construction rather than mitigation.
- Templates hold **code only** (imports). Data goes through the DataJob
  mmap plane (§6.3); loading data into a template would make its state
  depend on store content and contaminate keys.

Dispatch protocol: scheduler sends the job spec over a socketpair; the
template `fork()`s (cheap, COW of the warmed import state); the child
enters the job sandbox **without exec** — `os.unshare(user|mount|net)`,
then a second fork for the PID namespace, bind-mount the §6.1 layout,
`pivot_root`, drop to `/ppg/out` — and runs the callback. The template
reaps its children and reports exit status + captured stdio back to the
scheduler.

**DECISION — sandbox mechanisms by job type**: `CommandJob` keeps bwrap
(it execs anyway; nothing to preserve). Python `FileJob`/`DataJob` use the
forkserver with unshare-based self-sandboxing. The no-exec sandbox entry is
the riskiest implementation item in this document; WP3 prototypes it first
and the `sandbox="none"` fallback applies until it lands.

### 6.5 Callback transport across interpreters

The coordinator's python and a job's PyEnv may be *divergent* (3.10 lab
legacy env next to 3.13). cloudpickle serializes functions as bytecode,
which does not cross interpreter versions.

**DECISION — two transports, selected at definition time:**

- **Same-env fast path**: if the coordinator interpreter is byte-identical
  to the job's PyEnv (same nix store path), callbacks ship via cloudpickle.
- **Cross-env source mode**: otherwise the callback ships as *source*
  (file content + qualname — the same extraction the recipe hash already
  performs), is imported by the template's child, and must satisfy: no
  closures, no free variables beyond the localscope-allowed set, all data
  flowing in through `JobIO` (declared inputs/params) or default parameter
  values from the §5 closed canonicalizable type set. ppg2's localscope
  discipline was designed for exactly this shape; the check runs at
  definition time and names the offending variables, so a job that cannot
  cross interpreters fails before the run starts, not inside a worker.

Source mode is also the more hermetic of the two (what runs is exactly the
hashed text), so `paranoid=True` (§9) forces source mode even for same-env
jobs to keep the verified path hot.

### 6.6 Declaring jobs without importing their dependencies

The coordinator executes the pipeline script, but it must never need the
*job's* python environment to declare a job. Three callback declaration
forms, in increasing order of decoupling:

1. **Inline function, body-local imports** (the recommended idiom):

   ```python
   def normalize(io):
       import scanpy as sc          # resolves inside the job's PyEnv,
       ...                          # never in the coordinator
   ppg3.FileJob(..., run=normalize, python=py310_legacy)
   ```

   The coordinator only parses the function; `import scanpy` executes in
   the template's child. Top-level imports of job-only packages in the
   *pipeline script itself* are a definition-time footgun the docs warn
   about; the linter (`ppg3 lint`) flags callbacks whose free variables
   resolve to modules absent from their declared PyEnv.

2. **Function object from a coordinator-importable module** — standard
   source extraction, as in §6.5.

3. **Opaque file reference — the coordinator never imports (or even
   parses) the job code**:

   ```python
   ppg3.FileJob(..., run=ppg3.Source("steps/legacy_norm.py::normalize"),
                python=py310_legacy)
   ```

   The file participates as content: recipe hash = blake3 of the file
   bytes + the qualname. It is shipped read-only into the sandbox at
   `/ppg/src/` and imported there by the worker shim. This is the only
   form that works when the module has top-level imports unavailable in
   the coordinator env, or syntax the coordinator's parser rejects
   (older- or newer-python-only code). `Source` files may declare sibling
   imports (`ppg3.Source(..., includes=["steps/lib.py"])`); each include
   is hashed into the recipe and shipped alongside.

**DECISION**: `Source` form implies source-mode transport regardless of
env match, and localscope checking for it happens *inside the worker shim*
at import time (the coordinator cannot analyze what it does not parse) —
a violation fails the job before the callback runs, with the same named-
variables report as definition-time checks.

`CommandJob` remains the zero-python-coupling alternative: driving a
script with the env's own interpreter (`[Tool("python"), In("script")]`)
never involves callback transport at all.

## 7. Job API (user-facing)

Constructors keep ppg2's flavor; semantics change underneath.

### 7.1 `FileJob` (replaces FileGeneratingJob/MultiFileGeneratingJob)

```python
job = ppg3.FileJob(
    view={"counts": "results/counts.tsv", "log": "results/counts.log"},
    run=count_fn,                      # def count_fn(out: JobIO): ...
    tools=[samtools],                  # ToolSpec, §7.5
    inputs={"reads": other_job,        # whole parent
            "ref": other_job["fasta"], # named subset of parent outputs
            "raw": ppg3.File("data/raw.fq.gz"),
            "params": ppg3.Params({"n_trees": 200})},
    env={"OMP_NUM_THREADS": "4"},
    resources=ppg3.Resources(cores=4),
    retain=ppg3.Retain.Default,        # or Evict (temp), or Pin
)
```

- `view` maps output names → where the link lands in the generated view
  tree. Job "identity" for humans; irrelevant to caching.
- `run` executes **inside the sandbox** in a forked-then-exec'd worker (the
  callback is shipped by cloudpickle to a clean `python -I` started from a
  declared python tool — the interpreter itself is a tool input). ppg2's
  bare-fork model is gone; this closes the fork-under-threads hazard by
  construction.
- `JobIO` exposes `out.path("counts")`, `io.input("reads")`,
  `io.tool("samtools")` — all `/ppg/...` paths.

### 7.2 `CommandJob`

Argv/script with `In("name")`/`Out("name")`/`Tool("name")` placeholders;
recipe hash = the canonicalized template. Covers ppg2's
ExternalJob/ShellJob.

### 7.3 Temp semantics

`retain=Evict` marks the entry as first-in-line for GC once no *running*
lease references it. There is no cleanup callback and no
`ReadyButDelayed`-style logic: an evicted entry that is needed again is a
cache miss and reruns. `retain=Pin` creates a named pin.

### 7.4 Dynamic graphs

`GraphJob` (ppg2's JobGeneratingJob): runs in-process each `run()`, may add
jobs; added jobs mostly hit the store, so repeated expansion is cheap. Its
callback's recipe hash is recorded in the run report but does not key
anything.

### 7.5 Tools

```python
samtools = ppg3.ToolSpec.nix("github:nixos/nixpkgs/<rev>#samtools")
# resolved once per run: nix build --no-link --print-out-paths; tool hash =
# the nix store path string (it already encodes the input closure).
mytool  = ppg3.ToolSpec.binary("/opt/mytool/bin/mytool")   # hash of file
```

**DECISION**: `ToolSpec.nix` requires the flake ref to be pinned (rev or
lock); a bare branch ref is a definition-time error.

Python environments are a specialization that additionally drives the
forkserver (§6.4) and callback transport (§6.5):

```python
py310_legacy = ppg3.PyEnv.nix(
    "github:nixos/nixpkgs/<rev>#python310.withPackages(p: [p.scanpy ...])",
    preload=["numpy", "scanpy"],           # imported once in the template
)
py313 = ppg3.PyEnv.nix("...#python313.withPackages(...)", preload=["polars"])

job_a = ppg3.FileJob(..., python=py310_legacy, ...)   # divergent pythons
job_b = ppg3.FileJob(..., python=py313, ...)          # in one graph

ppg3.new(default_python=py313, ...)  # required; no implicit host python
```

**DECISION**: every python job runs under a declared `PyEnv`; there is no
implicit "whatever interpreter launched the script". `PyEnv.current()`
exists for non-Nix environments (tool hash = realpath + version + a hash of
`sys.path` entries' fingerprints) and is marked weakly-hermetic in
manifests, like `sandbox="none"`.

### 7.6 Fixed-output jobs

```python
ppg3.FetchJob(view="inputs/genome.fa.gz", url=..., blake3="ab12...")
```

Network allowed, sandbox relaxed; the declared hash *is* the output hash and
is verified after fetch. This is the only impurity door, and it is
airtight because the result is content-verified.

## 8. Engine

**DECISION — the core is a Rust crate (`ppg3-core`); Python is the
definition front-end and the worker shim.** This reverses an earlier draft
("engine in Python") after re-weighing the ppg2 audit evidence: the Rust
engine was ppg2's most reliable component, the Python runner its bug farm,
and everything that remains hard in ppg3 — the publish/lease/GC protocols,
scheduler concurrency, the forkserver control plane, the no-exec sandbox
entry — is systems code, Rust's home turf. What ppg2 got wrong was not the
language but the boundary; ppg3 draws it differently (§8.1).

`ppg3-core` contains: store (lookup/staging/publish/verify/leases/GC),
key hashing (over canonical-JSON documents it receives — it never inspects
Python objects), the scheduler with named resource pools, multi-store
substitution, the forkserver control protocol, and the sandbox-entry
helper. It is consumed two ways:

- as a **PyO3 extension** hosted in the coordinator process, and
- as a **standalone `ppg3` CLI binary** — store operations (`gc`,
  `verify`, `push`, `explain`, `rollback`) work on any machine with no
  Python environment at all. Operationally significant: you can GC a
  shared lab store from a cron job without the pipeline's env existing
  there.

### 8.1 The language boundary (three rules)

Rule 1 — **Python calls Rust; Rust does not call back into Python on hot
paths.** The two exceptions are coarse-grained and scheduler-initiated:
"expand this GraphJob" and "run this loader-layer job", both executed on
the Python side while the scheduler continues. This kills ppg2's
per-edge-callback design (GIL round-trips inside the engine, comparison
semantics invisible to the Rust test rigs).

Rule 2 — **All concurrency lives in Rust.** The scheduler owns its
threads; the Python front-end is single-threaded from the user's point of
view. No `evaluator_lock`, no async-exception aborts, no Python-side
worker pool. Abort is a flag the scheduler polls plus SIGTERM to sandbox
process groups.

Rule 3 — **Everything crossing the boundary is canonical JSON (or raw
bytes).** Python canonicalizes parameters and extracts function sources —
semantics that require Python — and hands finished key documents (§5) to
Rust. Rust hashes, stores, schedules. No Python object ever enters
`ppg3-core`; no store path semantics ever live in Python.

The forkserver template children (§6.4) enter the sandbox via a thin PyO3
binding over the same Rust sandbox-entry code the CLI uses — the
unshare/pivot_root logic is written once, in Rust, and merely *hosted* by
the Python child.

Scheduling loop (single logical owner of all state — adopting the audit's
recommendation; workers are dumb):

```
ready = jobs whose parents all have store entries (or are done in-process)
for job in ready (respecting Resources via named pools):
    ik = derive_key(job)
    if lookup(ik):            link-count it, mark done   # cache hit
    else:                     dispatch to worker (sandbox build)
on worker completion:         publish (§4), mark done, wake dependents
```

Abort is cooperative: a flag checked between dispatches plus SIGTERM to the
sandbox process group. No `async_raise`, no injected KeyboardInterrupt.

## 9. Determinism enforcement

1. **Publish-time** (always on): `inputs/<ik>` exists with different `oh` ⇒
   hard error, plus report: per-file added/removed/changed, and for changed
   files a summary (size, first differing offset, both hashes). Exit
   nonzero, entry quarantined under `staging/violations/`.
2. **`ppg3 verify [--sample N% | --job X]`**: rebuild store-hit jobs in the
   sandbox and compare output hashes. CI-friendly (`--sample 5%` nightly).
3. **First-build double-run** (`ppg3.new(paranoid=True)`): every cache-miss
   job is built twice before publish; mismatch fails the job immediately.
   Recommended default for the first weeks of a new pipeline.
4. Diff tooling: `ppg3 diff-entries <oh1> <oh2>` (files + offer to shell out
   to `diffoscope` if present).

## 10. Metadata

### 10.1 `manifest.json` (per entry, immutable)

```json
{
  "ppg3_manifest_version": 1,
  "input_key": "<ik>",
  "key_document": { ...full §5 document... },
  "content": {"relative/path": {"blake3": "...", "mode": "0644", "size": 123}, ...},
  "output_hash": "<oh>",
  "built": {"start": ..., "end": ..., "host": "...", "sandboxed": true,
            "ppg3_version": "..."},
  "job_view_name": "results/counts.tsv"   // informational only
}
```

`output_hash` = blake3 of canonical-JSON `content`. Symlinks inside outputs
are forbidden (publish error). Empty dirs are represented with a `.keep`
convention documented to users.

### 10.2 There is no global history file.

`ppg3 explain <view-path>` = load the entry's `key_document`, load the
previous generation's entry for the same view path, diff the two documents,
resolve parent `oh`s recursively where they differ. All read-only.

### 10.3 Local stat-cache

`.ppg3/statcache.sqlite`: `(path, size, mtime_ns) -> blake3` for leaf files,
plus function-source → recipe-hash memoization. **Non-authoritative**: mtime
granularity is `st_mtime_ns` (fixing ppg2's same-second blind spot); losing
or deleting this file costs re-hashing only, never wrong reuse.

## 11. Views, generations, GC

- A run produces `.ppg3/views/<n>/` (tree of relative symlinks into the
  write store) and repoints `.ppg3/views/current`; the user-visible
  `outputs/` is itself a symlink to `current` (**DECISION**; keeps
  `outputs/` atomic-swappable).
- `ppg3 rollback [n]` repoints `current`. `ppg3 generations list/rm/keep N`.
- GC (`ppg3 store gc [--max-size X] [--keep-generations N]`), per store:
  1. Take `gc.lock`.
  2. Roots: all retained generations' link targets, all pins, all entries
     referenced by live leases (lease = heartbeat file, stale after 30 min
     without mtime refresh — the running scheduler refreshes every 5 min).
  3. Mark reachable (`inputs/` symlinks whose target survives are kept;
     dangling ones removed).
  4. Sweep: delete unrooted entries, `retain=Evict` first, then LRU by an
     `atime` file touched on every hit, until under budget.
- Publish takes `gc.lock` in shared mode for the final rename+symlink, so
  GC can never sweep between rename and root creation (closes the
  SharedMFG race found in the ppg2 audit).

## 12. Testing strategy

The ppg2 test suite encodes overwrite semantics ("file rewritten in place",
"job reruns on invalidation", cleanup ordering) — **most of it does not
apply and will not be ported.** Port only: hashing/canonicalization tests,
localscope tests, FunctionInvariant source/bytecode extraction tests.

New suite, in order of construction:

1. **Store unit tests**: publish/dedup/violation/verify/GC protocols,
   including crash-injection between every pair of publish steps
   (staging→rename→symlink) and concurrent publisher+GC via two processes.
2. **Key derivation golden tests**: canonical JSON fixtures; any change to a
   key document version-bumps `ppg3_key_version` or fails CI.
3. **Property tests on the engine** (Rust: `proptest`/`cargo-afl`,
   continuing ppg2's fuzz-with-oracles methodology and reusing its AFL
   harness patterns): random DAGs + random subsets of pre-populated store
   entries ⇒ oracle: every miss built exactly once, every hit built zero
   times, resulting view complete, build order topological. Random
   parameter flip sequences ⇒ oracle: k-th distinct configuration builds
   nothing the (k-2)-th already built. Concurrent-interleaving fuzzing of
   publish+GC+substitution against a single store (the `fuzz_interleave`
   role, retargeted at the store protocols where the risk now lives).
4. **Sandbox integration tests**: undeclared input ⇒ ENOENT; network
   blocked; env empty; store paths never visible; nix tool runs.
5. **Determinism harness**: a corpus of deliberately-nasty jobs (timestamp
   embedder, `$RANDOM` user, locale-dependent sort) each asserted to *fail*
   with the violation report.

## 13. Work packages (parallelizable)

Interfaces are the contract; each WP lists what it may import.

Language per WP: **Rust** = part of `ppg3-core`; **Python** = the
front-end package. Mixed WPs name their split.

- **WP1 store (Rust)**: `Store` — `lookup(ik)`, `publish(staging, ik)`,
  `open_staging()`, `gc(policy)`, `verify(oh)`, lease API; blake3.
  Acceptance: §12.1 tests, including the crash-injection and
  concurrent-process suites, in Rust.
- **WP2 keys (mixed)**: Python side — parameter canonicalizer, function
  source / recipe extraction (port `extract_strict_hash` semantics from
  ppg2), emitting canonical-JSON key documents. Rust side — canonical-JSON
  validation + hashing (rejects floats, non-sorted keys: the validator is
  the spec). Acceptance: §12.2 golden tests run against *both* sides.
- **WP3 sandbox executors (Rust core + thin Python hosting)**: (a) bwrap
  runner for CommandJobs; (b) the forkserver — template lifecycle and
  dispatch protocol in `ppg3-core`; the no-exec unshare/pivot_root sandbox
  entry as a Rust function with a PyO3 binding, called by the Python
  template child (§6.4, §8.1). The no-exec entry is the highest-risk item
  in this document: build it as a standalone prototype ("fork, unshare,
  mount fixture layout, prove ENOENT on undeclared path, prove no
  network") before integrating. Includes the `sandbox="none"` fallback.
  Acceptance: §12.4 plus template-hermeticity tests (two templates for the
  same (PyEnv, preload) on different fake-HOME hosts produce
  byte-identical job outputs).
- **WP4 engine/scheduler (Rust)**: scheduler owning all threads, named
  resource pools (a real Condvar-based multi-unit semaphore — ppg2's
  Python `CoreLock` race, audit B2, must not be ported), cooperative
  abort, the two coarse Python callbacks of §8.1 rule 1. Depends on
  WP1/WP2 interfaces (mockable). Acceptance: §12.3.
- **WP5 views/generations/GC policy + CLI (Rust)**: the standalone `ppg3`
  binary — `rollback|generations|store gc|store push|verify|explain`.
  Works with no Python present. Depends on WP1.
- **WP6 multi-store substitution (Rust)**: ordered lookup,
  integrity-verified copy-in, s3/http readonly backends. Depends on WP1.
- **WP7 user API (Python)**: job classes (§7 — FileJob, CommandJob, DataJob,
  UnsandboxedJob, FetchJob, GraphJob), `ppg3.new`, view assembly, the
  loader layer with per-process memoization + localscope port, `io.load`
  with mmap-aware deserializers, the §6.5 transport selection with its
  definition-time closure check, and the §6.6 `Source` opaque-file form
  (worker-shim-side localscope check). Depends on WP2/WP4.
- **WP8 ToolSpec/PyEnv (Python)**: nix flake resolution (subprocess `nix
  build`), binary hashing, PATH assembly, `PyEnv.current()`
  fingerprinting, preload validation (imports must exist in the env —
  checked at template start). Depends on nothing internal.
- **WP9 explain/diff (Rust, in the WP5 CLI)**: §9, §10.2 — key-document
  diffing needs only manifests, no Python. Depends on WP1/WP2.
- **WP10 determinism corpus & CI harness (Python + CI config)**: §12.5,
  nightly `verify --sample`.

Suggested integration order: WP1+WP2 → WP4 (mocked executor) → WP3 → WP7 →
WP5/WP6/WP8/WP9/WP10.

## 14. Open questions (the only ones)

1. **Signing for shared stores.** v1 verifies integrity (content matches
   `oh`) but trusts the `ik → oh` claim of anyone who can write the store.
   Nix-style signatures on manifests are the known fix — v2.
2. **Windows/macOS sandboxing.** v1 is Linux-only enforced; `sandbox="none"`
   elsewhere. Is that acceptable for the user base? (Assumed yes:
   bioinformatics.)
3. **Very large temp intermediates**: is `retain=Evict` + GC budget enough,
   or do we need `store=False` jobs whose outputs live only in staging and
   are consumed by exactly one downstream within the same run? (Deferred
   until a real pipeline hurts.)
4. **In-process loader purity**: enforce with a second interpreter +
   seccomp someday, or accept localscope-level checking? (Accepted for v1.)
5. **ppg2 interop**: a shim exposing ppg2's constructor API on ppg3
   semantics would ease migration but drags overwrite-era expectations
   along. Proposal: don't; ship a migration guide instead.
6. **Standalone coordinator daemon.** With `ppg3-core` in Rust, a
   long-running daemon (watch mode, remote execution, one coordinator per
   lab store) becomes the same crate behind an IPC front instead of PyO3.
   Deliberately *not* v1 — the PyO3-hosted coordinator must prove the
   crate's API first; nothing in v1 may assume it is the only host.

## 15. What we deliberately gave up (vs ppg2)

- Running unsandboxed with inherited env/cwd/network as the default.
- Fork-COW as the default data plane. Shared data is a serialized store
  artifact; memory sharing happens via the page cache (mmap), and the
  residual COW use case is an explicit, marked `UnsandboxedJob` (§6.3).
  Warm *code* (imports) is COW-shared again via per-PyEnv forkserver
  templates (§6.4) — but code only, never data, never coordinator state.
- A single python for the whole pipeline. Jobs declare their `PyEnv`;
  divergent interpreter versions coexist in one graph, at the price of the
  cross-env source-transport restrictions (§6.5).
- Jobs mutating files in place, appending, or writing outside declared
  outputs.
- Nondeterministic jobs "working anyway".
- The interactive redefinition dance around a mutable output tree (views
  replace it).
- The Rust *invalidation* engine and its test rigs (the problem it solved
  no longer exists). Rust itself returns at a better boundary: `ppg3-core`
  owns store, scheduler, and sandbox entry (§8), and the fuzz-with-oracles
  methodology carries forward into §12 — retargeted at the protocols where
  the risk now lives.
- Per-edge Python callbacks from inside the engine (ppg2's
  `is_history_altered`/`get_input_list` design). The boundary now passes
  canonical JSON downward and coarse job-execution requests upward,
  nothing else (§8.1).
