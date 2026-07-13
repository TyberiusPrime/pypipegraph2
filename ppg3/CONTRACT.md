# ppg3 implementation contract

Read `../PPG3_DESIGN.md` first. Every **DECISION** there is settled. This file
adds the concrete layout, module boundaries, and cross-WP interfaces so work
packages can proceed independently. Do not change an interface here without
updating this file and every consumer.

## Scope deviations for v1-in-this-repo (agreed)

- No `bwrap`/user-namespaces in the dev container: the `sandbox="none"`
  staged-directory executor is the tested path. bwrap argv construction is
  implemented and unit-tested as *command construction only*; the unshare
  no-exec entry is left as a documented stub behind feature `linux-sandbox`.
- Forkserver templates (§6.4) are deferred: python jobs run via cold
  `python -I -m ppg3._shim` exec. The shim reads a JSON job spec on stdin.
  The scheduler/executor boundary below is forkserver-ready (ExecSpec is
  argv-based; a future forkserver replaces the exec, not the interfaces).
- Remote stores (s3/http) deferred; `StoreSet` supports N POSIX stores with
  ordered lookup and in-place consumption (§4.1).
- `ppg3 watch`: implemented (`python -m ppg3 watch`, Python-only coordinator
  loop — see "Additive addendum: `python -m ppg3 watch`" below), but
  **polling** instead of §6.7's inotify (no inotify Python binding in this
  container, no new third-party deps) — see STATUS.md. Session mode
  (templates persisting across `run()` calls inside one coordinator
  process, §6.7's other bullet), TOFU source patching, R shim: still
  deferred.
- Record deviations you add in `ppg3/STATUS.md`.

## Layout

```
ppg3/
  Cargo.toml            # workspace; default-members = ["core", "cli"]
  core/                 # crate ppg3-core (lib)
    src/lib.rs          # module wiring only — already written, do not rewrite
    src/error.rs        # shared error enum (thiserror)
    src/hash.rs         # blake3 helpers
    src/canon.rs        # WP2-Rust: canonical JSON validator + input_key()
    src/manifest.rs     # WP1: manifest types + output_hash()
    src/store.rs        # WP1: Store
    src/storeset.rs     # WP1: ordered multi-store
    src/lease.rs        # WP1: leases + intents
    src/gc.rs           # WP1: mark/sweep; policy knobs used by WP5
    src/executor.rs     # WP3: Executor trait, NoneExecutor, bwrap argv builder
    src/sandbox.rs      # WP3: stub, feature "linux-sandbox"
    src/resources.rs    # WP4: named multi-unit semaphore pools
    src/scheduler.rs    # WP4: scheduler
    src/views.rs        # WP5: generations, atomic swap, roots registration
    src/explain.rs      # WP9: key-document diff
    tests/              # integration tests (crash injection, 2-process)
  cli/                  # crate ppg3-cli, binary name "ppg3"  (WP5)
  py/                   # crate ppg3-py, cdylib pyo3 module "ppg3._core" (WP7)
  python/               # python package (WP7)
    pyproject.toml      # maturin backend, manifest-path = "../py/Cargo.toml"
    ppg3/               # __init__.py, jobs.py, canon.py, recipe.py, tools.py,
                        # io.py, transport.py, localscope.py, _shim.py, run.py
    tests/
  tests/golden/         # key-document fixtures shared by Rust & Python tests
  STATUS.md             # living log of what is done / deviated
```

Rust edition 2021. Deps allowed in core: blake3, serde, serde_json,
thiserror, fs2 (fcntl locks), tempfile (dev), libc. cli adds clap. py adds
pyo3 (abi3-py39). Keep the tree `cargo fmt`-clean and warning-free.

## Shared vocabulary

- `ik`: input key, 64-char lowercase blake3 hex of the canonical key document.
- `oh`: output hash, 64-char hex, blake3 of canonical-JSON of the manifest
  `content` map.
- All hashes lowercase hex, no prefixes.

## Canonical JSON (canon.rs — the validator IS the spec, §5)

UTF-8, object keys strictly sorted (byte order), no whitespace, no floats
anywhere (integer JSON numbers allowed within i64/u64; a number with `.`,
`e`, `E` is rejected), no duplicate keys. Strings NFC-normalized is NOT
required (document this). API:

```rust
pub fn validate(bytes: &[u8]) -> Result<(), Error>;
pub fn canonicalize(v: &serde_json::Value) -> Result<Vec<u8>, Error>; // sorts keys, rejects floats
pub fn input_key(canonical_key_doc: &[u8]) -> Result<String, Error>;  // validate + blake3
```

Key document shape: exactly §5. `ppg3_key_version: 1`.

## Store (store.rs, §4)

```rust
pub struct Store { /* name, root, readonly */ }
pub enum PublishOutcome { Published { oh: String }, DedupHit { oh: String } }

impl Store {
    pub fn open(name: &str, root: &Path, readonly: bool) -> Result<Store>; // creates v1/ layout if writable
    pub fn lookup(&self, ik: &str) -> Result<Option<Manifest>>;
    pub fn entry_dir(&self, oh: &str) -> PathBuf;        // .../v1/entries/<oh>
    pub fn data_dir(&self, oh: &str) -> PathBuf;         // .../entries/<oh>/data
    pub fn open_staging(&self) -> Result<Staging>;       // v1/staging/<host>-<pid>-<rand>/, has .path() -> &Path (the future data/)
    pub fn publish(&self, staging: Staging, ik: &str, key_document: &serde_json::Value, built: BuiltInfo, job_view_name: Option<&str>) -> Result<PublishOutcome>;
    pub fn verify_entry(&self, oh: &str) -> Result<VerifyReport>; // rehash content vs manifest
    pub fn lease(&self, run_id: &str) -> Result<Lease>;  // lease.heartbeat(), Drop releases
    pub fn write_intent(&self, ik: &str) -> Result<Intent>;      // advisory, §11.1
    pub fn add_root(&self, project_id: &str, generation: u64, ohs: &[String]) -> Result<()>;
    pub fn remove_root(&self, project_id: &str, generation: u64) -> Result<()>;
    pub fn pin(&self, name: &str, oh: &str) -> Result<()>;
    pub fn log_dir_for(&self, ik: &str) -> Result<PathBuf>;      // logs/<ik>/<ts>-<host>/, creates
    pub fn gc(&self, policy: &GcPolicy) -> Result<GcReport>;
}
```

Publish protocol exactly §4: hash files in staging (streaming blake3; reject
symlinks; record mode as 4-digit octal of permission bits, size), compute
`oh`, take `gc.lock` SHARED (fs2), then:
- `entries/<oh>` exists → compare content manifests byte-identically:
  identical ⇒ remove staging, DedupHit; different ⇒ `Error::CorruptStore`.
- else write `manifest.json` into staging's parent-to-be layout
  (`entries/<oh>/{data,manifest.json}`), `rename()` into place, chmod -R a-w.
- `inputs/<ik>` symlink: create atomically (symlink to tmp name + rename).
  Exists pointing to different oh ⇒ `Error::DeterminismViolation { report }`
  with per-file added/removed/changed diff (§9), quarantine staging under
  `staging/violations/`.
- Publish must be resumable/crash-safe at every step boundary (tests inject
  crashes between steps by calling internal step functions directly —
  expose `#[doc(hidden)] pub` step functions or a `publish_steps` test API).

GC (§11): exclusive `gc.lock`; roots = all `roots/**`, `pins/*`, entries
referenced by leases fresher than 30min, plus everything reachable...
(entries do not reference each other — reachability is just the root set).
Sweep unrooted: `retain=Evict`-marked first (marker file
`entries/<oh>/.ppg3-evict-ok` written at publish when requested — add
`retain_evict: bool` to BuiltInfo), then LRU by `entries/<oh>/.atime` file
touched on every lookup hit; delete until under `max_size` budget. Dangling
`inputs/*` symlinks removed. `logs/` evicted before entries.

## StoreSet (storeset.rs, §4.1)

```rust
pub struct StoreSet { pub stores: Vec<Store> }
impl StoreSet {
    pub fn lookup(&self, ik: &str) -> Result<Option<(usize, Manifest)>>; // first hit
    pub fn write_store(&self, job_target: Option<&str>) -> Result<&Store>; // named or first writable
}
```
Hits are consumed in place (data_dir of the owning store).

## Manifest (manifest.rs, §10.1)

Serde types mirroring §10.1 exactly; `output_hash(content) -> String` =
blake3 of canonical-JSON of the content map. `BuiltInfo { start_ms, end_ms,
host, sandboxed: bool, ppg3_version, retain_evict: bool }` (extra field ok —
serialize as `"built"` object; key doc version guards compat).

## Executor (executor.rs)

```rust
pub struct PreparedJob {
    pub ik: String,
    pub argv: Vec<String>,               // already lowered; no python semantics here
    pub env: BTreeMap<String, String>,   // full final env (§6.1 scrub done by caller/py side)
    pub inputs: Vec<Mount>,              // Mount { virtual_path: "/ppg/in/<name>", source: PathBuf } — read-only
    pub tools: Vec<Mount>,               // "/ppg/tools/<name>"
    pub out_dir: PathBuf,                // staging data dir (becomes entry data/)
    pub log_dir: PathBuf,
    pub allow_network: bool,             // fixed-output only
    pub cwd_out: bool,                   // cwd = /ppg/out
}
pub struct ExecResult { pub exit_code: i32, pub stdout: Vec<u8>, pub stderr: Vec<u8> }
pub trait Executor: Send + Sync { fn run(&self, job: &PreparedJob) -> Result<ExecResult>; }
pub struct NoneExecutor;   // staged dir: build <work>/ppg/{in,out,tools,log,tmp} with SYMLINKS
                           // for inputs/tools, out -> real out_dir; run argv with env + PPG_ROOT
                           // env var pointing at the staged root; argv/env strings containing
                           // "/ppg/" are rewritten to "<work>/ppg/" before exec. Warns once.
pub fn bwrap_argv(job: &PreparedJob, bwrap: &Path) -> Vec<String>;  // pure function, unit-tested
```

Paths inside argv/env always use virtual `/ppg/...` form (§6.2); executors
translate. NoneExecutor sets `TMPDIR=<work>/ppg/tmp` etc.

## Scheduler (scheduler.rs, resources.rs, §8.1)

All threads live here. Input graph:

```rust
pub struct JobDef {
    pub id: String,
    pub recipe: String,                       // recipe hash (py side computes)
    pub inputs: BTreeMap<String, InputRef>,   // name -> ref
    pub tools: BTreeMap<String, String>,      // name -> tool hash
    pub runtime: serde_json::Value,           // §5 runtime object (no floats)
    pub env: BTreeMap<String, String>,
    pub outputs_declared: Vec<String>,
    pub resources: BTreeMap<String, u64>,     // pool name -> units, e.g. {"cores": 4}
    pub store_target: Option<String>,
    pub retain: Retain,                       // Default | Evict | Pin(name)
    pub exec_template: ExecTemplate,          // how to lower to PreparedJob
    pub view: BTreeMap<String, String>,       // output name -> view-relative path
    pub fixed_output: Option<String>,         // declared oh for FetchJob-style
}
pub enum InputRef { Job { id: String }, JobSubset { id: String, names: Vec<String> }, Leaf { hash: String } }
pub enum ExecTemplate { Argv { argv: Vec<String>, allow_network: bool }, InProcess }  // InProcess => host callback
```

Key derivation at dispatch time: parents' `oh` (or subset hash: blake3 of
canonical JSON `{name: file-content-hash-from-parent-manifest, ...}`)
fill `inputs`; assemble the §5 document with canon::canonicalize; ik =
input_key. Lookup StoreSet: hit ⇒ done (touch .atime); miss ⇒ acquire
pools, lower ExecTemplate (argv `{in:NAME}`/`{out}`/`{tool:NAME}`
placeholders → virtual paths), run Executor, publish, wake dependents.

```rust
pub trait HostCallbacks: Send + Sync {
    fn expand_graph_job(&self, job_id: &str) -> Result<Vec<JobDef>>;    // §7.4
    fn run_in_process(&self, job_id: &str, key_doc: &serde_json::Value) -> Result<()>; // loader layer
}
pub struct RunReport { pub built: Vec<String>, pub hits: Vec<String>, pub failed: BTreeMap<String, String>, pub job_entries: BTreeMap<String, (String, String)> } // id -> (ik, oh)
pub fn run(storeset: &StoreSet, executor: &dyn Executor, jobs: Vec<JobDef>, callbacks: &dyn HostCallbacks, parallelism: &BTreeMap<String, u64>, abort: &AtomicBool) -> Result<RunReport>;
```

Failure policy: a failed job fails its transitive dependents
(reported, not run); independent subgraph continues. Cycle ⇒ error before
any dispatch. Resource pools: Condvar-based multi-unit semaphore; a request
larger than pool capacity is a definition error; acquisition order must not
deadlock (single lock over all pools or sorted acquisition).

## Views (views.rs, §11)

```rust
pub struct ViewSpec { pub entries: Vec<(String, String, String)> } // (view-rel path, oh, store-root path or index)
pub fn write_generation(project_dir: &Path /* .ppg3/ */, project_id: &str, stores: &StoreSet, spec: &ViewSpec, ephemeral: bool) -> Result<u64>; // registers roots in every store linked into, then symlink tree views/<n>/, repoint current
pub fn rollback(project_dir: &Path, generation: u64) -> Result<()>;
pub fn list_generations(project_dir: &Path) -> Result<Vec<GenInfo>>;
pub fn drop_generation(project_dir: &Path, stores: &StoreSet, generation: u64) -> Result<()>; // unregister roots
```
`outputs` symlink → `.ppg3/views/current` → `views/<n>` (atomic swap via
symlink+rename).

### Additive clarification (WP5, see STATUS.md for the full rationale)

The `ViewSpec` sketch above — `Vec<(String, String, String)>` as "(view-rel
path, oh, store-root path or index)" — is under-specified: it can't address
*which file inside a multi-file entry* a view path should resolve to. As
implemented, `ViewSpec` is:

```rust
pub struct ViewEntry {
    pub view_rel_path: String,   // e.g. "results/counts.tsv"
    pub oh: String,
    pub path_within_entry: String, // relative path inside that entry's data/, matches a content-manifest key
    pub store_index: usize,        // index into the StoreSet passed to write_generation
}
pub struct ViewSpec { pub entries: Vec<ViewEntry> }
```
i.e. the caller (eventually `ppg3.run()` on the Python side) already knows,
per view path, exactly one `(oh, path-within-entry)` pair — there is no
directory-of-symlinks fan-out inside `views.rs` itself.

Also implemented (used by `explain.rs` and the CLI, not in the original
sketch): `write_generation` writes a `views/<n>/meta.json` (`created_at`,
`project_id`, `ephemeral`, `entries: [{view_rel_path, oh, path_within_entry,
store_name}]` — entries are recorded by store *name*, not index, since a
later reader reconstructs its own `StoreSet` and only names are stable
across processes) and, when `ephemeral` is set, a `views/<n>/.ephemeral`
marker file. `list_generations` additionally exposes
`GenInfo { n, created_at, ephemeral, current, n_entries }`. `keep_last
(project_dir, stores, n, keep_explicit) -> Result<Vec<u64>>` implements the
§6.7 GC-keep policy and returns the dropped generation numbers.

## CLI (cli/, WP5)

`ppg3 <cmd>` with clap: `store gc [--max-size BYTES] [--store PATH]
[--dry-run] [--evict-logs]`, `store verify [--sample PCT|--entry OH]
--store PATH`, `generations list|rm N|keep N [--keep-explicit]`, `rollback
[N]`, `explain <view-path>`, `diff-entries <oh1> <oh2> --store PATH`.
Project commands find `.ppg3/` by walking up from cwd; `--project PATH`
overrides (accepts either the project root or the `.ppg3` dir itself).
Human-readable output + `--json`. Exit codes: `0` ok, `1` operational
failure (verify mismatch, determinism violation, missing/current
generation, ...), `2` usage error (also `clap`'s own default for malformed
arguments).

### Additive clarification: `--keep-generations` and `.ppg3/config.json`

`store gc` here does **not** take `--keep-generations N` (unlike the one
line in PPG3_DESIGN.md §11's bullet list): it operates purely at the store
level (`entries/`, `roots/`, `pins/`, `leases/`, `logs/`), which has no
notion of "a project's generations". Generation lifecycle is a separate,
per-project concern (`ppg3 generations keep N`), which itself unregisters
roots in whichever stores a dropped generation referenced — run it before
`store gc` if you want the freed entries actually swept. See STATUS.md.

Project subcommands (`generations *`, `rollback`, `explain`) need a
`StoreSet` to unregister roots / resolve manifests, but the CLI has no
Python `ppg3.new(stores=...)` call to hand it one — so this WP defines
`.ppg3/config.json`, written by whoever sets up the project (`run.py` on
the Python side, WP7):

```json
{"stores": [{"name": "main", "path": "/abs/path/to/store", "readonly": false}]}
```
`readonly` defaults to `false` if omitted. `store gc`/`store verify`/
`diff-entries` take a raw `--store PATH` instead and never read
`config.json`.

## explain/diff (explain.rs, §9/§10.2)

```rust
pub fn diff_key_documents(a: &serde_json::Value, b: &serde_json::Value) -> KeyDocDiff; // structured: changed inputs/tools/env/recipe...
pub fn diff_entries(store: &Store, oh_a: &str, oh_b: &str) -> Result<EntryDiff>;       // files added/removed/changed(size, first differing offset, hashes)
pub fn explain_view_path(project_dir: &Path, stores: &StoreSet, view_path: &str) -> Result<Explanation>;
pub fn list_entries(store: &Store) -> Result<Vec<String>>; // every oh currently published; used by `store verify --sample`
```

### Additive clarification (WP9, see STATUS.md)

`diff_entries` returns `Result<EntryDiff>` rather than a bare `EntryDiff`
(reading manifests/files can fail — every other WP1/WP9 API in this crate
surfaces I/O failure via `Result`). `explain_view_path` — not in the
original one-line sketch — is `Explanation::FirstAppearance { view_path,
generation, oh }` when the view path has no earlier generation to diff
against, else `Explanation::Diff { view_path, previous_generation,
current_generation, oh_a, oh_b, diff: Box<KeyDocDiff>, why_chain:
Vec<WhyStep> }`; `why_chain` recursively resolves changed `inputs` entries
that are themselves resolvable parent-job output hashes (looked up by `oh`
across every configured store), depth-capped at 8. "Previous generation"
skips generation numbers whose directory has been dropped (by
`drop_generation`/`keep_last`) rather than requiring exactly `n-1`.

## PyO3 boundary (py/, §8.1 rules)

Module `ppg3._core`. Functions (all JSON-string or bytes in/out — no rich
objects): `input_key(canonical: &[u8]) -> String`, `canonicalize(json:
&str) -> String`, `blake3_file(path) -> String`, `open_stores(json config)
-> StoreSetHandle`, `run(handle, jobs_json, parallelism_json, py_callbacks)
-> report_json` (callbacks object with `expand_graph_job(id) -> jobs_json`,
`run_in_process(id, key_doc_json)`), `write_generation(...) -> u64`,
`lookup(handle, ik) -> Option<manifest_json>`. Python never sees store
entry paths except via manifest/report JSON (view assembly happens in Rust).

## Python package (python/, WP7)

- `ppg3.new(stores=[Store(name, path, readonly=False)], default_python=None, project_dir=".ppg3", parallelism={"cores": N}) -> Graph` (module-level current graph like ppg2).
- Job classes per §7 building `JobDef` dicts. `FileJob(view=, run=|Source,
  tools=[], inputs={}, env={}, resources=, retain=, python=, store=)`,
  `CommandJob(view=, argv=[...placeholders...])`, `DataJob`, `FetchJob(view=,
  url=, blake3=)` (frozen mode: blake3 required), `GraphJob(fn)`,
  `UnsandboxedJob` (runs via InProcess host callback; warned).
- Parameter canonicalizer: closed type set §5; floats via
  `struct.pack('<d')` hex under `{"__ppg3_float__": "<hex>"}`; set/frozenset
  sorted by canonical encoding; Enum as module.qualname.name; dataclass as
  dict with `__ppg3_dataclass__` = qualname; reject everything else with a
  loud TypeError naming the path. Opt-in `__ppg3_hash__` protocol.
- Recipe extraction (`recipe.py`): source-derived strict hash — port the
  *semantics* of ppg2 `extract_strict_hash` (dis-based, bytecode-noise-free)
  or dedent-source + consts; must be stable across equivalent definitions;
  golden-tested.
- Transport (§6.5): same-env ⇒ cloudpickle if importable else source-mode;
  source-mode = extracted source + qualname, localscope check at definition
  time (port `python/pypipegraph2/` localscope usage or a minimal
  free-variable check via `inspect`/`ast`: free variables beyond builtins/
  params ⇒ error naming them).
- `_shim.py`: stdin JSON `{transport: {...}, io: {inputs: {name: path},
  outputs: {name: path}, tools: {}, log_dir, params}}`; reconstruct callback,
  build `JobIO` (`io.input(name)`, `io.path(name)`/`out.path`, `io.tool`,
  `io.load(name)`, `io.log_dir`, `io.params`), run, exit code.
- ToolSpec/PyEnv (`tools.py`, WP8): `ToolSpec.nix(ref)` (pinned-ref check;
  subprocess `nix build --no-link --print-out-paths`; skip-if-no-nix),
  `ToolSpec.binary(path)` (blake3 of file), `PyEnv.current(preload=[])`
  (realpath + version + sys.path fingerprint; weakly-hermetic flag),
  `PyEnv.nix(...)`.
- `run()`: lower jobs to JobDef JSON, call `_core.run`, then assemble
  ViewSpec from report + view maps, `_core.write_generation`.

## Golden fixtures (tests/golden/)

`keydoc_NN.json` = `{"doc": <key document>, "canonical": "<exact canonical
string>", "ik": "<blake3 hex>"}`. WP1/2-Rust agent creates ≥6 covering:
sorting, unicode, nested params, subset inputs, env, no-floats rejection
(`keydoc_reject_NN.json` with `{"doc":..., "error": true}`). Rust tests and
Python tests both consume the same files.

## Testing bar

- `cargo test` green at `ppg3/` root (default members), `cargo clippy`
  no warnings, on this machine (no bwrap, no nix — skip, don't fail).
- Python: `cd ppg3/python && uv run --with maturin,pytest maturin develop
  && pytest` (or an equivalent documented one-liner) green.
- End-to-end smoke test (python): two-job pipeline (CommandJob producing a
  file, FileJob python callback consuming it) runs, produces a view; second
  run = all hits, zero builds; parameter flip back = hits (§12.3 oracle).

## Rules for implementing agents

1. Do not modify anything outside `ppg3/` (and never `PPG3_DESIGN.md`).
2. Do not rewrite `core/src/lib.rs` module wiring; fill the module files.
3. Interfaces above are the contract; extend, don't break. If a signature
   must change, update CONTRACT.md and note it in STATUS.md.
4. Keep every DECISION of the design doc; deviations only via STATUS.md
   with a one-line rationale.
5. Leave the tree compiling and tests green; note anything unfinished in
   STATUS.md under "TODO".

## Addendum (WP7-Rust/py-binding pass): PyO3 boundary + shim spec delivery

Written once `core/src/{scheduler,executor,views}.rs` landed for real and
`py/src/lib.rs` was implemented against them. Full rationale in STATUS.md;
this section only records the resulting interface, since "extend, don't
break" (rule 3 above) applies to the PyO3 boundary same as everywhere else.

### `py/src/lib.rs` — additive over the original one-line sketch

- `run(handle, jobs_json, parallelism_json, callbacks, work_dir) -> report_json`
  takes a 5th argument, `work_dir: &str` — the root `NoneExecutor::new`
  stages per-job work directories under. Not in the original sketch (which
  predates `executor.rs`'s real `NoneExecutor::new(work_parent)` signature).
- `lookup(handle, ik) -> Option<manifest_json>` — `manifest_json`, when
  present, is the `Manifest` JSON with one extra top-level field spliced in:
  `"store_index": <usize>` (the index into the `StoreSet` that `lookup`
  walked, i.e. matching the index space `write_generation`'s `ViewSpec`
  entries expect). Needed because `RunReport.job_entries` only carries
  `(ik, oh)`, not *which* configured store the entry lives in, and that
  index is otherwise purely Rust-internal (`StoreSet::lookup`'s return
  value). An index is not a store entry path, so this stays inside "Python
  never sees store entry paths except via manifest/report JSON."
- `open_stores(config_json)` — `config_json` is the **bare JSON array**
  `[{"name","path","readonly"}, ...]`, exactly as this section's original
  one-liner already implied. This is a different, unrelated shape from
  `.ppg3/config.json`'s `{"stores": [...]}` wrapper object (a CLI-only, WP5
  concern documented separately above under "Additive clarification:
  `--keep-generations` and `.ppg3/config.json`") — `ppg3.run()` (WP7,
  python side) now writes *both* files/payloads from the same `Store` list,
  each in its own required shape.
- `write_generation(handle, project_dir, project_id, view_spec_json,
  ephemeral) -> u64` — `view_spec_json` is `{"entries": [...]}` where each
  entry is the real, 4-field `ViewEntry` shape from the "Views" section's
  "Additive clarification (WP5)" above (`view_rel_path`, `oh`,
  `path_within_entry`, `store_index`), **not** the original 3-tuple sketch.
- No abort-flag plumbing in v1: `run()` constructs its own private
  `AtomicBool` (always `false`) for the `scheduler::run` call every
  invocation; there is currently no way for Python to request an abort
  mid-run. `scheduler::run`'s `abort: &AtomicBool` parameter is otherwise
  unused from the Python side.

### Shim spec delivery (resolves the WP7 "interface gap" flagged for
`core/src/executor.rs`'s implementer)

`core/src/executor.rs` landed with **no stdin-piping and no spec-file
field** — `PreparedJob` only carries `argv`/`env`/`inputs`/`tools` Mounts;
`NoneExecutor` execs with `Stdio::null()` for stdin. So `python -I -m
ppg3._shim` (`jobs.py`'s `FileJob`/`DataJob`/`FetchJob` argv, built by the
new `_shim_argv` helper) receives its spec via **argv**, not stdin:

- `--spec-b64 <base64 JSON>`: the static part of the spec — transport info,
  `pickle_output`, `Params`-typed input values, or (`FetchJob`) `url`/
  `blake3` — i.e. nothing path-shaped.
- `--in NAME <path>` / `--out NAME <path>` / `--tool NAME <path>` (each
  repeatable): one argv token pair per mounted input / declared output /
  tool. The `<path>` value is always exactly one `{in:NAME}` / `{out:NAME}`
  / `{tool:NAME}` placeholder token (see `resolve_placeholder` in
  scheduler.rs) — **never** embedded inside the `--spec-b64` blob, because
  `lower_argv`'s placeholder scanner finds the first `{`...`}` pair in a
  token verbatim, and JSON's own structural braces inside a base64-decoded
  string collide with that (confirmed by tracing `lower_token` — an
  unmatched/malformed brace pair is echoed back unchanged, which silently
  *skips* resolving a real placeholder nested inside it). One placeholder
  per bare argv token sidesteps the collision entirely, and composes for
  free with `NoneExecutor`'s separate `/ppg/`-prefix string-rewrite of the
  now-resolved argv values.
- `--log-dir <path>`: always the literal `/ppg/log` (a `NoneExecutor`
  constant, not a `{...}` placeholder — no per-job resolution needed).
- `_shim.py`'s `main()` prefers this argv form when `--spec-b64` is present
  in `sys.argv`, else falls back to reading a spec from stdin (the literal
  CONTRACT.md prose above, "the shim reads a JSON job spec on stdin" — kept
  working, still exercised directly by `python/tests/test_shim.py`, just no
  longer what `jobs.py` itself uses to invoke real jobs).

This supersedes the originally-imagined fix (a `PPG_ROOT` env var for the
shim to translate embedded `/ppg/...` paths itself): unnecessary, since
paths never travel inside the opaque blob in the first place. **No
`core/src/executor.rs` change was needed.**

## Additive addendum: `python -m ppg3 watch` (§6.7 "watch" bullet)

Python-only, no Rust changes; `core/src/views.rs`'s existing
`write_generation(..., ephemeral: bool)` (see "Views" above) already
covered everything the Rust side needs to know. New/changed files:
`python/ppg3/{__main__.py, watch.py}` (new), `python/ppg3/{run.py,
jobs.py}` (additive), `python/tests/test_watch.py` (new).

- **Entry point**: `python -m ppg3 watch <pipeline.py> [--interval
  SECONDS] [args...]` (default interval 0.5s). `python/ppg3/__main__.py`
  parses this by hand rather than via `argparse`'s `REMAINDER` for
  `script_args`: the required CLI shape puts the pipeline script *before*
  `--interval` (`watch script.py --interval 0.1`), and `REMAINDER`
  greedily swallows every token — including a later `--interval` — once it
  starts consuming at the first positional, silently dropping the flag.
  `_parse_watch_argv` instead scans the whole argv for
  `--interval`/`--interval=VALUE` wherever it appears and strips it out;
  the first remaining token is the script, the rest is forwarded verbatim
  (as `sys.argv[1:]`) to the pipeline script. The Rust `ppg3` CLI binary
  (`cli/`) is a separate, unrelated entry point (store-level ops); this
  command never shells out to it.
- **`ppg3.run()`'s existing `ephemeral: bool = False` parameter** (already
  present in `run.py` from an earlier pass — CONTRACT.md never previously
  documented it, noted here for completeness) is left unchanged for normal
  callers. Watch mode does *not* require the pipeline script to pass
  `ephemeral=True` itself: `python/ppg3/run.py` adds a module-level
  `watch_mode()` context manager + a plain `_watch_active` bool flag it
  toggles (not a `contextvar` — the watch loop drives `runpy.run_path()`
  synchronously on one thread, so there is never a concurrent non-watch
  `run()` call while it's set). `run()`'s effective ephemeral flag passed
  to `_core.write_generation` is `ephemeral or _watch_active`. `watch.py`'s
  `_run_definition_pass` wraps every `runpy.run_path()` call in `with
  run.watch_mode():`.
- **"Last run info" slot**: `run.py` adds a module-level dict,
  `_last_run_info` (`graph`, `watched_paths`, `generation`, `report`),
  updated by every `run()` call (success *or* `PPGRunError` — updated
  before the failure check, so a failed run's watch-set is still
  captured) and read back via `run.get_last_run_info()`. Needed because
  `runpy.run_path()`'s temporary module namespace — and the pipeline
  script's own local `graph` variable inside it — is gone once
  `run_path()` returns; this is the only channel back to the watcher.
  Never cleared, so a pass that raises *before* ever calling `run()` (e.g.
  a syntax error) leaves it holding the previous successful pass's info —
  exactly the "keep watching the last-known watch set" failure policy
  below.
- **Watch-set collection**: `jobs.py`'s `Graph` gains
  `record_watched_path(path)` / `watched_paths() -> List[str]` (sorted,
  deduplicated). Two recording sites, matching how each is actually known:
  `_lower_input`'s `File` branch records the leaf path at **lowering**
  time (`job_defs()`/`job_def()`, i.e. whenever `run()` calls them —
  includes paths added later via `GraphJob` expansion, since
  `RunCallbacks.expand_graph_job` also calls `job_def()` on the newly
  added jobs mid-`run()`); `FileJob.__init__` records a `Source` callback's
  `.path` + every `.includes` entry at **definition** time (no lowering
  needed — the path is already known from the constructor argument). The
  pipeline script's own path is *not* part of `Graph.watched_paths()` —
  `watch.py`'s loop adds it itself (it's not a Graph concept).
- **`PollingWatcher` (`watch.py`)** — the deviation from §6.7's "inotify":
  no inotify Python binding in this container and the project takes no
  new third-party dependencies, so this polls `(mtime_ns, size)` per
  tracked path (same non-authoritative stat signal §10.3's stat-cache
  already uses, for the same reason). Same semantics as inotify would give
  (re-run exactly when a watched path's content/existence changes),
  strictly worse latency/efficiency, bounded by `--interval`. Exposes only
  `poll() -> List[str]` (plus `set_paths()` to change the tracked set
  between iterations without manufacturing spurious changes) so a future
  inotify-backed watcher is a drop-in replacement for the loop in
  `run_watch`; see STATUS.md.
- **Debounce**: `wait_for_change(watcher, interval)` blocks until
  `poll()` reports a change, then sleeps one more `interval` and polls
  once more, merging in anything caught during that settle window, so
  multi-file saves batch into a single re-run trigger.
- **Failure policy**: a definition-pass exception (including
  `ppg3.run.PPGRunError`, on job failure — the view is left untouched per
  §11) is reported (summary + counts to the configurable output stream;
  the actual traceback to real `sys.stderr`, not that stream, so it's
  distinguishable from ordinary iteration status) and the loop keeps
  running, watching the last-known watch set — just the script on the very
  first iteration, since no `run()` call has happened yet to populate one.
  `KeyboardInterrupt` (SIGINT) exits the loop with exit code 0 and a
  `n runs, n failures` summary; it is never caught anywhere else in the
  loop, so it always propagates out cleanly.
- **Graph-state reset**: `_run_definition_pass` sets
  `ppg3.jobs._current_graph = None` before every `runpy.run_path()` call —
  defensive belt-and-suspenders on top of the pipeline script calling
  `ppg3.new(...)` itself each pass (which already replaces the module
  global with a fresh `Graph`); guards against a script that skips or
  conditionally skips that call from silently reusing a stale graph and
  accumulating jobs across passes.

## Additive addendum: TOFU source patcher (§7.6)

Python-only, no Rust changes (`core/src/scheduler.rs`'s `derive_key` was
read, not changed, to confirm it never consults `job.fixed_output` — the
fact this WP's "second run is an all-hits re-run" guarantee depends on).
New: `python/ppg3/tofu.py`, `python/tests/test_tofu.py`. Additive:
`python/ppg3/jobs.py` (`FetchJob._call_site` + `_record_call_site()`),
`python/ppg3/run.py` (one `tofu.run_tofu_pass(...)` call site),
`python/pyproject.toml` (new `tofu` extra, `libcst` added to `test`).
Full rationale for every design corner is in STATUS.md's "TOFU source
patcher" entry; this section only records the resulting interface/shape.

- **`FetchJob._call_site: Optional[Tuple[str, int]]`** — `(absolute file
  path, 1-indexed line number)` of the first stack frame outside the
  `ppg3` package at `FetchJob.__init__` time, or `None` if none has a real
  on-disk file (e.g. `-c`/stdin/REPL). Recorded for every `FetchJob`
  regardless of whether `blake3` was given.
- **`ppg3.tofu.run_tofu_pass(graph, report, core, handle) -> None`** —
  called by `run()` immediately after the existing `report.get("failed")`
  check (i.e. only on a successful run). For every `FetchJob` in `graph`
  with `blake3 is None`: resolves its actual output hash via
  `report["job_entries"][job.id]` + `core.lookup(handle, ik)`, groups by
  `_call_site`, and either patches (exactly one job at that site, `libcst`
  importable) or appends to a printed fallback table (everything else:
  shared call sites, unrecorded call sites, unpatchable sites, or `libcst`
  missing entirely — in which case a stderr hint to `pip install
  'ppg3[tofu]'` is also printed). Never raises on account of a missing
  optional dependency or an unpatchable file; a genuine internal-invariant
  violation (a `FetchJob`'s manifest content map not having exactly one
  entry) still raises via a plain `assert`, deliberately not swallowed.
- **Patch mechanism**: `libcst` (`MetadataWrapper` + `PositionProvider`) is
  used only to locate the target `Call` node (matching on
  `PositionProvider.start.line == recorded_lineno` and the callee's last
  attribute segment / a same-file import-alias scan resolving to
  `"FetchJob"`) and, within it, the `blake3=` keyword's value span (if
  present) or the last argument's end position (if not). The edit itself
  is a **plain string splice** at the resulting byte offsets — not a
  libcst tree transform + codegen round-trip (empirically, libcst's
  codegen does not reliably reproduce multi-line
  `ParenthesizedWhitespace(indent=True)` indentation for a freshly-spliced
  node — see STATUS.md). This guarantees "every other byte of the file is
  untouched" by construction, not merely "libcst-preserved". Multiple
  singleton call sites in one file are patched together (edits applied
  right-to-left by offset in a single read-modify-write).
- **Pin message** (stdout, one line per patched job): `pinned <view-path>
  (<digest[:8]>…) in <file>:<lineno>`.
- **Table fallback** (stdout, printed once per `run_tofu_pass` call if
  any row remains unpatched): a header line, then one `  <url>\t<blake3>\t
  (view=<view_path>)` line per unpatched job.
- **`pyproject.toml`**: `[project.optional-dependencies]` gained `tofu =
  ["libcst"]`; the existing `test` extra now also includes `libcst` so
  `test_tofu.py` runs unconditionally in the dev venv.

## Additive clarification (§6.7 session mode)

`_core.open_session(work_dir, template_argv) -> Session`,
`_core.run(..., session=None)` (session's TemplateManager wins over the
per-call `template_argv` when given), `_core.session_shutdown(session)`,
`_core.session_template_count(session)`. Python keeps one module-level
Session per process (run.py); `ppg3.session_stop()` ends it and clears the
ik-keyed loader memos.
