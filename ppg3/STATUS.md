# ppg3 implementation status

Living log. Each work-package agent appends: what is done, deviations from
PPG3_DESIGN.md / CONTRACT.md (one-line rationale each), and TODOs.

## Scaffold
- Workspace layout + CONTRACT.md written. Module files are placeholders.

## Deviations (project-wide, pre-agreed — see CONTRACT.md "Scope deviations")
- sandbox="none" NoneExecutor is the tested executor (no bwrap/userns in dev container).
- Cold-exec worker shim instead of forkserver templates (§6.4) for v1 here.
- POSIX multi-store only; s3/http backends deferred (WP6).
- Session mode / watch / TOFU patcher / R shim deferred (WP11, WP12).

## WP1 (store) + WP2-Rust (keys/canonical JSON) — done

Implemented `core/src/{hash,canon,manifest,store,lease,gc,storeset}.rs`
(all were placeholders), a test-only `core/src/bin/store_helper.rs` (spawned
as a real OS process by `core/tests/concurrent.rs`), a one-off fixture
generator `core/examples/gen_golden.rs`, 9 files under `tests/golden/`
(7 `keydoc_*.json` + 2 `keydoc_reject_*.json`), and integration tests
`core/tests/{golden,publish_crash,concurrent}.rs`.

Result: `cargo test -p ppg3-core` → 74 tests green (63 unit + 3 concurrent +
1 golden + 7 crash-injection). `cargo clippy -p ppg3-core --all-targets` →
zero warnings. `cargo fmt` clean on all files this WP owns (did not run
`cargo fmt` over `lib.rs`/`error.rs`, which this WP didn't otherwise touch,
to keep the diff scoped to files actually owned here).

Golden fixtures: computed `canonical`/`ik` via `ppg3_core::canon` itself
(via `gen_golden.rs`, kept in `core/examples/` for future regeneration),
then independently cross-checked every valid fixture's `ik` against `pip
install blake3` (Python bindings around the reference blake3 implementation,
installed fresh for this check — not a project dependency) hashing the
`canonical` string, and independently re-verified key-sortedness + compact
formatting with a small ad-hoc Python script. All 7 valid fixtures matched;
both reject fixtures confirmed to contain a float (`.`/`e` form) and to fail
`canonicalize()`.

### Deviations (each a considered call on an underspecified corner of
CONTRACT.md/PPG3_DESIGN.md §4 — longer rationale in `core/src/store.rs`'s
module doc comment and `core/src/gc.rs`'s module doc comment):

- `chmod -R a-w` (§4) is applied to `entries/<oh>/data/**` and
  `entries/<oh>/manifest.json`, not to the `entries/<oh>` directory entry
  itself — otherwise `.atime` (§11, touched on every lookup hit) and
  `.ppg3-evict-ok` (written at publish time) could never be created once an
  entry is published.
- `inputs/<ik>` creation uses a direct `symlink()` syscall (POSIX-atomic,
  `EEXIST` on conflict) instead of "symlink to tmp name + rename" — a
  tmp+rename would unconditionally clobber an existing destination symlink
  on Linux, which is exactly the silent-overwrite a conflicting concurrent
  publisher must never trigger. `pin()` (where replacing an existing name
  *is* the intended behavior) does use tmp-name+rename.
- Determinism-violation "quarantine staging under `staging/violations/`":
  by the time the violation is detected (step 6), the staging dir has
  already been consumed by the `entries/<oh>` resolution in step 5 (either
  renamed into a brand-new entry or discarded as a dedup). Quarantine
  writes a forensic record (`report.txt` + `attempted_oh.txt`) under
  `staging/violations/<ik>-<ts>/` instead of moving live, possibly-shared
  entry content — store immutability is kept intact.
- `ContentEntry.mode` is recorded as the file's staging-time permission
  bits with write bits pre-masked off (`mode & 0o777 & !0o222`), not the
  raw staging-time mode. Rationale: publish always strips write bits at
  chmod time, so recording the raw pre-chmod mode would make
  `verify_entry` report a spurious mismatch on literally every published
  entry, and would make identical output content staged under different
  umasks spuriously fail to dedup (different oh for the same real file).
- GC (`gc.rs`): `retain=Evict`-marked (`.ppg3-evict-ok`) unrooted entries
  are swept on every `gc()` call regardless of `max_size` (§7.3 reads as
  "first in line", not "only under space pressure"). Log eviction
  (`GcPolicy.evict_logs`) is budget-driven only — logs are removed (oldest
  first, always before any ordinary entry) only when `max_size` is set and
  currently exceeded; a routine `gc()` call with no budget never touches
  `logs/`, since §6.1a documents logs as valuable cross-machine cache-hit
  debugging data, not disposable-by-default.
- `Lease` gained `protect(oh)`/`unprotect(oh)` beyond bare
  create/heartbeat/drop. CONTRACT.md's GC rule needs a lease to be *about*
  a concrete oh set ("entries referenced by leases") but the one-line
  `Store::lease(run_id)` signature gives no other place to record that set;
  `heartbeat()` preserves the protected set (read-modify-write of the
  timestamp only).
- `Manifest.built` uses CONTRACT.md's field names (`start_ms`/`end_ms`)
  rather than PPG3_DESIGN.md §10.1's example (`start`/`end`) — CONTRACT.md
  is the authoritative signature source per the assignment brief.
- `StoreSet` gained a `new()` constructor alongside the public `stores`
  field (field stays public/constructible directly; purely additive).
- Race-safety extras beyond the literal protocol text: `publish_step5`
  treats a `rename()` failure with `ENOTEMPTY`/`AlreadyExists` (i.e. a
  concurrent publisher won the race to create `entries/<oh>` first) as
  "fall through to the dedup-compare path", rather than erroring — required
  for the two-process concurrent-publish acceptance test to pass rather
  than flake.

### Repo-hygiene fix (outside the module list, but inside `ppg3/`)

The repo-root `.gitignore` has a `core*` rule (meant for crash-dump files
like `core`, `core.12345`) that also matches this workspace's `core/`
crate directory, so `ppg3/core/` — including every module this WP and
several others (executor.rs, resources.rs, scheduler.rs, views.rs,
explain.rs) live in — was entirely untracked and invisible to `git
status`/`git add -A`. Fixed by adding `!core/` / `!core/**` negation rules
to `ppg3/.gitignore` (in-scope: inside `ppg3/`, does not touch the
repo-root `.gitignore`). Verified with `git add -n ppg3/core` — all 21
files under `core/` (mine and other WPs' placeholders) are now stageable.
Flagging this prominently since without it, the coordinator's first
`git add`/commit of `ppg3/` would have silently dropped the entire
`ppg3-core` crate.

## TODO
- Stale `staging/<host>-<pid>-<rand>/` directories left behind by a crash
  between steps 1-4 (before `entries/<oh>` exists) are never automatically
  cleaned up by `gc()` — only `entries/`, `logs/`, and dangling
  `inputs/*` are swept. A future GC extension could sweep `staging/*`
  (and `staging/violations/*`) older than some age threshold. Not required
  by any test in scope here (crash recovery is verified via a fresh
  publish succeeding, which does not depend on the old staging dir being
  removed).
- `gc()`'s `max_size` budget check treats `entries/` + `logs/` as the only
  size-relevant trees (`staging/`, `roots/`, `pins/`, `leases/`,
  `intents/` are metadata/symlinks only, assumed negligible). If `staging/`
  accumulates a lot of leaked directories (see above TODO) this
  assumption would need revisiting.
- No `proptest`/fuzz harness (PPG3_DESIGN.md §12, item 3) — out of scope
  for WP1/WP2-Rust per the assigned work packages; left for whichever WP
  picks up engine-level property testing.
- `pip install blake3` was used only as a one-off, interactive
  cross-check of the golden fixtures and is not a project dependency (not
  added to any `Cargo.toml`/`pyproject.toml`).

## WP7 (python package) — done

Built the pure-Python front-end under `python/`, importable and testable
with **zero third-party deps required** (`blake3`/`cloudpickle` are optional,
probed lazily; `ppg3._core` is probed lazily and never imported at module
load time anywhere in the package).

Files written (all new):
- `python/pyproject.toml` — maturin backend per CONTRACT.md, `test` extra =
  `pytest, cloudpickle, blake3`.
- `python/ppg3/__init__.py` — public API surface exactly as specified.
- `python/ppg3/canon.py` — `canonicalize_value`/`canonical_json_bytes`
  (closed type set, all six sentinel wrappers, `__ppg3_hash__` hook,
  path-naming `TypeError`s), `input_key_local` (tries `ppg3._core` →
  `blake3` pip package → loud `MissingBlake3Error`), plus
  `decanonicalize_value` (best-effort inverse, used by `JobIO.params`;
  added beyond the literal spec list because `io.params` needs to hand back
  usable Python values, not raw sentinel dicts).
- `python/ppg3/recipe.py` — `recipe_hash` (dedent + decorator-strip +
  def-name-blank + defaults, hashed via canon), `recipe_hash_source` (for
  `ppg3.Source`), `recipe_hash_command` (CommandJob argv templates).
  Clean-room reimplementation of ppg2 `extract_strict_hash`'s *semantics*
  (source-derived, line-number- and rename-independent) — no code or
  imports from `python/pypipegraph2/`; deliberately simpler than ppg2's
  `dis`-based bytecode-noise-stripping (CONTRACT.md's own spec text for
  this file already prescribes the simpler source-text approach).
- `python/ppg3/localscope.py` — `check_localscope`/`DefinitionError`, a
  from-scratch ~140-line port of the ppg2 `localscope` package's semantics
  (not its code): `co_freevars` closures always rejected, `LOAD_GLOBAL`
  names (recursively through nested code objects) checked against
  builtins/`allowed`/module-references (modules recorded, not rejected).
  Intentionally does not replicate ppg2's bytecode-disassembly caching or
  `predicate=`/decorator API.
- `python/ppg3/tools.py` — `ToolSpec.nix`/`.binary`, `PyEnv.current`/`.nix`,
  pinned-ref check (`[0-9a-f]{40}` or `path:`/absolute-path), lazy
  per-process `nix build --no-link --print-out-paths` cache.
- `python/ppg3/jobs.py` — `Graph`, `ppg3.new`, `FileJob`, `CommandJob`,
  `DataJob`, `FetchJob`, `GraphJob`, `UnsandboxedJob`, `In`/`Out`/`Tool`,
  `File`, `Params`, `Resources`, `Retain`, `Store`; every job has a
  `.job_def(graph)` assembling the CONTRACT.md `JobDef` dict.
- `python/ppg3/statcache.py` — sqlite `(path,size,mtime_ns)->blake3` at
  `<project_dir>/statcache.sqlite`.
- `python/ppg3/transport.py` — `Source`, `select_transport` (cloudpickle
  same-env fast path / source-mode cross-env+paranoid), wired to the
  localscope check at definition time.
- `python/ppg3/io.py` — `JobIO` (`input`/`load`/`path`/`tool`/`log_dir`/
  `params`), per-process `io.load()` memo.
- `python/ppg3/_shim.py` — `python -I -m ppg3._shim` worker entry, JSON
  spec **on stdin** (matches CONTRACT.md's prose literally — see interface
  gap note below), `cloudpickle`/`source`/`source_file` transports,
  `pickle_output` flag for `DataJob`, `fetch` mode via `urllib` + blake3
  verification.
- `python/ppg3/_bridge.py` — `get_core()`, the only place that imports
  `ppg3._core`; raises `CoreNotAvailable` with a clear message otherwise.
- `python/ppg3/run.py` — `run()` per the PyO3 boundary call shapes
  (`open_stores`/`run`/`write_generation`), `RunCallbacks` implementing
  `expand_graph_job`/`run_in_process`.
- `python/tests/` — `conftest.py` (skip-marker helpers: `requires_blake3`,
  `requires_cloudpickle`, `requires_core`, `requires_golden`, `requires_nix`)
  + `test_canon.py`, `test_recipe.py`, `test_localscope.py`,
  `test_transport.py`, `test_shim.py`, `test_jobs.py`, `test_statcache.py`.
  Golden fixtures (`ppg3/tests/golden/keydoc_*.json`, already present from
  WP1/WP2-Rust) are consumed directly: `canonical_json_bytes(doc)` compared
  byte-for-byte against the fixture's `"canonical"` string, and (when
  blake3 is available) `input_key_local(doc)` compared against `"ik"`;
  reject fixtures assert `canonical_json_bytes` raises.

**Test command** (from `ppg3/python/`):
```
uv venv .venv && source .venv/bin/activate && uv pip install pytest cloudpickle blake3
PYTHONPATH=$(pwd) python -m pytest tests/ -q
```
(The compiled extension is not built by this WP — `maturin develop`/`pip
install -e .` will currently fail since `ppg3/py`'s Rust crate has nothing
to build against yet in a way that resolves; that's fine, nothing in this
suite needs it. `PYTHONPATH` is only needed if not `pip install -e`'ing;
`conftest.py` also inserts `python/` onto `sys.path` itself.)

**Result**: with `blake3`+`cloudpickle` installed: **92 passed, 0 skipped,
0 failed**. With neither installed (bare `pytest` only, simulating a
pre-extension checkout): **49 passed, 43 skipped, 0 failed** — every skip
is a `requires_blake3`/`requires_cloudpickle` guard, nothing silently
broken. `ppg3._core`-dependent code (`run.py`'s actual `run()` call,
`_bridge.get_core()`) has no dedicated test since the extension does not
exist yet in this tree; `_bridge.py` is exercised only indirectly (nothing
in the test suite calls `ppg3.run()`).

### Deviations / ambiguity calls (see also inline comments at each site)

- **Recipe hash decorator handling**: decorator lines are stripped from
  both the hash source *and* the transported source-mode text (not just
  the hash, as ppg2 did) — a decorator resolves in the defining module's
  scope, which a divergent job env / worker exec cannot see; shipping the
  bare `def` is the only form that can actually run cross-env.
- **`InputRef`/`ExecTemplate`/`Retain` JSON tagging**: CONTRACT.md gives
  Rust enum *shapes* but `core/src/scheduler.rs` (where they'd be defined)
  was still a placeholder when this WP started. Assumed plain serde-default
  external tagging (unit variants as bare strings, e.g. `"InProcess"`,
  `"Default"`; struct/tuple variants as `{"VariantName": {...}}`). If the
  Rust WP4 agent's actual struct picks different `#[serde(...)]` attributes,
  only `jobs.py`'s `_retain_json`/`_lower_input`/`exec_template` literals
  need updating — flagging here for cross-agent coordination since I
  cannot touch `core/`.
- **Shim spec delivery mechanism (interface gap)**: CONTRACT.md's prose
  says "the shim reads a JSON job spec on stdin", but the `PreparedJob`/
  `Executor` types shown in CONTRACT.md have no stdin field (and
  `core/src/executor.rs` is still a placeholder). `_shim.py` reads stdin,
  matching the prose; `jobs.py`'s `exec_template.Argv.argv` for
  `FileJob`/`DataJob`/`FetchJob` is just `[python_executable, "-I", "-m",
  "ppg3._shim"]` with no spec-delivery placeholder. **Whoever implements
  the Rust executor needs to either pipe the per-job spec JSON to the
  child's stdin, or this needs a follow-up CONTRACT.md amendment** — flagged
  here rather than guessed further since getting it wrong would silently
  break every python job.
- **`run_in_process` (`UnsandboxedJob`) is unimplemented** (raises
  `NotImplementedError` with an explanatory message) — `HostCallbacks
  ::run_in_process(job_id, key_doc_json)`'s two arguments carry declared
  input *hashes* (from the key document) but not the resolved real
  filesystem paths a `JobIO` needs; that mapping only exists inside the
  Rust scheduler's dispatch state. Building it here would mean guessing a
  third, unreviewed wire format. Same root cause affects `GraphJob`'s
  `expand_graph_job`, which *is* implemented (it only needs to run Python
  code and report newly-added `JobDef`s, no path resolution needed) but is
  equally untested end-to-end for the same reason (`_core.run` doesn't
  exist yet to drive it).
- **`PyEnv.nix` same-env detection not implemented**: §6.5's same-env fast
  path is defined as "byte-identical to the coordinator interpreter (same
  nix store path)"; this WP only recognizes `PyEnv.current()` as same-env.
  A `PyEnv.nix(...)` pointing at the coordinator's own resolved store path
  would, per spec, also qualify for cloudpickle, but detecting that needs
  resolving the nix ref (a `nix build` subprocess) at transport-selection
  time, which felt like an expensive/surprising side effect to trigger
  implicitly at every `FileJob(...)` construction. `PyEnv.nix` jobs always
  take source-mode transport in this implementation — correct, just more
  conservative (more jobs source-mode than strictly necessary).
- **TOFU source-patching (§7.6) is not implemented** — `FetchJob` accepts
  `blake3=None` outside `--frozen`/CI mode and simply carries `fixed_output:
  null` in its `JobDef` (frozen-mode rejection *is* implemented and
  tested). The libcst call-site rewrite is WP11-adjacent and explicitly
  out of scope per CONTRACT.md's deferral list.
- **`DataJob` view argument**: accepts a plain `str` (the common case,
  `view="results/model"`) or a dict with exactly the key `"data.pickle"`;
  the output name is always the fixed string `"data.pickle"`
  (`DataJob.OUTPUT_NAME`), matching the design text's "producing a single
  pickled artifact `data.pickle`" literally.
- **Job id derivation**: `"+".join(sorted(view.values()))` when no explicit
  `name=` is given and `view` has multiple entries (single-entry views use
  the path itself). Not specified further by CONTRACT.md beyond "derived
  from view or explicit name="; documented here as the concrete rule.
- **CommandJob/non-python jobs' `runtime.python_env`**: set to `null`
  (`{"python_env": null, "preload": [], "shim": "0"}`) rather than omitting
  the field, since §5's key document always has a `runtime` object present
  in every example shown; CommandJobs don't run through the shim and have
  no `PyEnv` to report.

## TODO (superseded — see "WP7-Rust/py-binding pass" below)
- ~~Wire `run_in_process`...~~ partially done below (Leaf-only inputs).
- ~~Resolve the shim stdin-delivery interface gap...~~ done below.
- ~~Once `ppg3._core` exists: add `requires_core`-gated integration
  tests...~~ done below (`python/tests/test_e2e.py`).
- `ppg3 lint` (checking that `select_transport`'s recorded
  `localscope_modules` actually resolve inside a job's declared `PyEnv`) is
  not implemented — only the recording (via `LocalscopeReport.modules`) is
  in place, per CONTRACT.md's WP7 scope (the lint proper reads like a CLI
  concern, WP5). Still open.
- No `ppg3.repl()`/session-mode/watch support (WP11, explicitly deferred).
  Still open.

## WP7-Rust/py-binding pass (py/src/lib.rs, JobDef reconciliation, shim
wiring, run.py completion, e2e) — done

Picked up once `core/src/{scheduler,executor,views}.rs` had landed for
real (WP7 above was written against placeholders). Scope: the PyO3
extension crate, the remaining Python bridge/run wiring, serde-shape
reconciliation against the real Rust types, and the end-to-end smoke test.
`core/src/**` was treated as read-only (another agent was concurrently
writing `core/tests/scheduler.rs`); **no core/src change was needed at
all** (see "Shim spec delivery" below for why the originally-anticipated
`PPG_ROOT` change turned out to be unnecessary).

### A) `py/src/lib.rs` (crate `ppg3-py`, module `ppg3._core`)

Implemented every function CONTRACT.md's "PyO3 boundary" lists —
`input_key`, `canonicalize`, `blake3_file`, `blake3_hex`, `open_stores`,
`lookup`, `run`, `write_generation` — plus the `StoreSetHandle` pyclass.
Full rationale for the three additive extensions (`run`'s extra
`work_dir` arg, `lookup`'s spliced-in `store_index`, the real 4-field
`ViewEntry` wire shape for `write_generation`) is in the CONTRACT.md
addendum added alongside this entry ("Addendum (WP7-Rust/py-binding
pass)") — not duplicated here.

`run()` calls `scheduler::run` inside `py.allow_threads(...)`; the two
`HostCallbacks` methods (`PyHostCallbacks`) each re-acquire the GIL via
`Python::with_gil` only for the duration of the single Python call — no
scheduler worker thread holds the GIL otherwise, per §8.1 rule 1.

Compiles clean: `cargo check -p ppg3-py` and `cargo clippy -p ppg3-py
--all-targets` both zero-warning on first pass (pyo3 0.23's `Bound<'_,
PyModule>`/`#[pyfunction]`/`&StoreSetHandle`-as-borrowed-argument APIs all
worked as expected, no version-shim workarounds needed).

### B) Reconciling `jobs.py` against the real `scheduler.rs`

The placeholder-era assumption ("plain serde-default externally-tagged
JSON") turned out to match exactly — `InputRef`/`ExecTemplate`/`Retain`
needed **zero** changes. Two things genuinely needed fixing once the real
`JobDef` was readable:

- **`graph_job: bool`** — `scheduler.rs`'s module docs explain this is the
  *only* field distinguishing a `GraphJob` (wants `expand_graph_job`) from
  a plain `InProcess` loader-layer job (wants `run_in_process`); it has
  `#[serde(default)]` so its absence was never a hard error, just silently
  wrong routing. Every `Job.job_def()` now sets it explicitly
  (`GraphJob` → `true`, everything else → `false`).
- **Shim spec delivery** (below).

### C) Shim spec delivery — resolved, no core change

`core/src/executor.rs` landed with no stdin/spec-file channel at all —
`PreparedJob` is strictly argv/env/Mounts, and `NoneExecutor` execs with
`Stdio::null()` for the child's stdin. Two candidate fixes were on the
table per the work brief: (a) an env var (`PPG_ROOT`) the shim could use
to translate embedded `/ppg/...` paths itself, requiring a
`core/src/executor.rs` change; (b) keep paths out of the opaque blob
entirely. Went with (b) — **no core change needed**:

- The static, non-path part of the spec (transport, `pickle_output`,
  `Params`-typed input values, or `FetchJob`'s `url`/`blake3`) travels as
  one `--spec-b64 <base64 JSON>` argv token.
- Every real/virtual path travels as its own argv token pair (`--in NAME
  <path>` / `--out NAME <path>` / `--tool NAME <path>`, plus a bare
  `--log-dir <path>`), where `<path>` is *exactly* one `{in:NAME}`/
  `{out:NAME}`/`{tool:NAME}` placeholder (`jobs.py`'s new `_shim_argv`).
  `scheduler.rs`'s `lower_argv` resolves each of these to the job's real
  virtual path, and `NoneExecutor`'s separate `/ppg/`-prefix string-rewrite
  (already existing, unmodified) then turns that into the real staged
  filesystem path — for free, no new machinery.
- Embedding the placeholders *inside* the base64 JSON blob instead (the
  first thing tried) does not work: traced `lower_token`'s scanner by
  hand — it takes the **first** `{`...`}` pair in a token literally, so a
  `{in:name}` placeholder nested inside JSON's own structural `{`/`}`
  braces gets swallowed into one bogus non-matching token and echoed back
  *unchanged* (silently un-resolved) rather than raising an error. One
  placeholder per bare argv token sidesteps this collision entirely.
- `_shim.py`'s `main()` now accepts **both** delivery forms: argv
  (`--spec-b64`, preferred when present) and the original stdin form
  (`_read_spec`, still fully functional and still what
  `python/tests/test_shim.py`'s existing subprocess tests drive directly —
  kept working both because it's simpler to unit-test standalone and
  because CONTRACT.md's literal prose ("the shim reads a JSON job spec on
  stdin") isn't wrong, it's just no longer what `jobs.py` uses to invoke
  real jobs). Added two new argv-delivery-specific tests to
  `test_shim.py` (`test_shim_argv_spec_b64_delivery_writes_output`,
  `test_shim_argv_spec_b64_fetch_mode`).
- `-I`/isolated-mode discovery finding, folded into the same investigation:
  the work brief anticipated needing to set `env["PYTHONPATH"]` (a
  documented "v1 wart" poisoning the key document with a host path) so
  `python -I -m ppg3._shim` could find the `ppg3` package. Verified by
  hand this is **not needed**: `-I` implies `-E` (ignore `PYTHON*` env
  vars) and `-s` (no *user* site-packages), but does **not** imply `-S` —
  the interpreter's own venv/site-packages (including a `.pth`-based
  editable install, which is what `maturin develop` produces) is still on
  `sys.path`. Confirmed empirically: `.venv/bin/python -I -m ppg3._shim`
  imports `ppg3` fine with zero `PYTHONPATH`, as long as `ppg3` is actually
  installed into the same venv `PyEnv.current()`'s `sys.executable` points
  at (true by construction for the coordinator's own interpreter — it
  already needs `ppg3` importable to call `ppg3.new()` at all). No
  PYTHONPATH env var is set anywhere in `jobs.py`.

### D) `run.py` + `_bridge.py`

`run()` now: writes `.ppg3/config.json` (`{"stores": [...]}`, the shape
`cli/src/config.rs` reads — confirmed by reading that file directly), lowers
the graph (`graph.job_defs()` — leaf hashing via `StatCache`, tool
resolution, transport payload construction were all already implemented by
the original WP7 pass and needed no changes), calls `_core.open_stores`
(bare-array shape) then `_core.run` with a `RunCallbacks` instance, and — 
**only if `report["failed"]` is empty** — cross-references each job's
`(ik, oh)` against `_core.lookup(handle, ik)` (for the `store_index` that
isn't otherwise available on the Python side, see the CONTRACT.md addendum)
to build a `ViewSpec` and calls `_core.write_generation`. On any failure,
raises `PPGRunError(RunResult(report, generation=None))` — the view is left
completely untouched, matching §11 "a generation is a consistent,
all-or-nothing snapshot." `RunResult` exposes `.built`/`.hits`/`.failed`/
`.job_entries`/`.generation`/`.raw`.

`RunCallbacks.expand_graph_job` was already correct (just needed the
`graph_job` fix above to actually get invoked). `RunCallbacks.
run_in_process` (`UnsandboxedJob`) is now **partially** implemented rather
than an unconditional `NotImplementedError`: it works for `UnsandboxedJob`s
whose declared inputs are `File`/`Params` (leaf) refs only — a `File`'s
real path is known host-side without any mount (leaf inputs are never
mounted for *any* job kind, sandboxed or not: `resolve_placeholder`'s
`Leaf` arm in `scheduler.rs` errors if an argv job even tries `{in:NAME}`
on one), and a `Params` value is already known from `job.inputs` itself.
A `Job`/`JobSubset`-typed input still raises `NotImplementedError` naming
the gap — this is the real, unresolved limitation flagged by the original
WP7 pass: `HostCallbacks::run_in_process(job_id, key_doc)` only carries the
*declared* input hashes (from the key document), never resolved real
filesystem paths, and `ExecTemplate::InProcess` jobs get no `PreparedJob`/
mounts at all in `scheduler.rs`'s `dispatch_job` — that mapping is
Rust-internal dispatch state with no PyO3-boundary exposure. Fixing this
for real needs either a `HostCallbacks` signature change (out of scope: a
core interface change, not "minimal", and no test in this pass exercises
it) or a separate resolved-paths side-channel; left as `NotImplementedError`
for the `Job`/`JobSubset` case, `TODO` below.

### E) Build + test — exact commands

```
cd ppg3/python
uv venv .venv                       # already existed in this tree
source .venv/bin/activate
uv pip install maturin cloudpickle blake3 pytest   # pytest was already present
maturin develop                      # NOT `maturin develop --manifest-path ../py/Cargo.toml` — see wart below
python -m pytest tests/ -q
```

**Wart — `maturin --manifest-path` picks the wrong project**: running
`maturin develop --manifest-path ../py/Cargo.toml` (or `maturin build
--manifest-path ...`) from `ppg3/python/` does **not** use
`ppg3/python/pyproject.toml`. It instead walks up from the *Cargo
manifest's own directory* (`ppg3/py/`, whose ancestors are `ppg3/py` →
`ppg3` → the outer `pypipegraph2/` repo root) and finds
`pypipegraph2/pyproject.toml` first (`ppg3/python/pyproject.toml` is a
*sibling* of `ppg3/py/`, not an ancestor, so it's never reached this way).
Symptom: `maturin develop --manifest-path ...` failed outright with `error:
The dependency group 'dev' was not found in the project: pyproject.toml`
(that dependency-groups table only exists in the outer repo's
`pyproject.toml`); `maturin build --manifest-path ... -o dist` "succeeded"
but silently built a wheel named `pypipegraph2-3.4.3-...whl` exporting
`PyInit_pypipegraph2` — completely the wrong package, would have failed to
import `ppg3._core` with a confusing error two steps later. **Fix**: run
bare `maturin develop` / `maturin build -o dist` with cwd = `ppg3/python/`
and *no* `--manifest-path` flag — maturin then reads
`ppg3/python/pyproject.toml`'s own `manifest-path = "../py/Cargo.toml"` key
and resolves correctly relative to *that* file's location. Confirmed this
produces `ppg3-0.1.0-cp39-abi3-...whl` / a correctly-named editable install
(`ppg3._core` imports and every function is present).

**Result**: `cargo check -p ppg3-py` / `cargo clippy -p ppg3-py
--all-targets` clean. `cargo check --workspace` clean (all four crates:
`ppg3-core`, `ppg3-cli`, `ppg3-py`, plus the default members). `cargo test
-p ppg3-core` could not be independently re-run to completion in this pass
— the sibling agent editing `core/tests/scheduler.rs` had a live `cargo
test -p ppg3-core --test scheduler` loop holding the shared `target/`
build lock essentially continuously during this session's tail end; not
re-attempted to avoid fighting that agent for the lock. `ppg3-core` itself
is exercised transitively and heavily by every one of the 97 Python tests
below (including three real end-to-end runs through the actual compiled
scheduler/store/views code in `test_e2e.py`), which is strong indirect
evidence it's sound; a direct `cargo test -p ppg3-core` re-run is still
worth doing once that lock frees up.

`cd ppg3/python && python -m pytest tests/ -q` → **97 passed, 0 skipped, 0
failed** (blake3/cloudpickle/`ppg3._core` all present in this venv). The
pre-existing 92 all still pass unmodified in behavior (the `graph_job`/
shim-argv changes to `jobs.py` are additive fields / internal argv
construction — no existing assertion touched them); +2 new `test_shim.py`
cases for the argv `--spec-b64` delivery path, +3 new `test_e2e.py` cases.

### `python/tests/test_e2e.py` — the CONTRACT.md "Testing bar" smoke test

Gated entirely by `requires_core` (skipped, not failed, if the extension
isn't built). Three cases, all passing against the real compiled
extension:

1. `test_e2e_two_job_pipeline_build_then_hit_then_param_flip` — exactly the
   CONTRACT.md scenario: `CommandJob` (`/bin/sh -c "echo hello >
   {out:greeting}"`) → `FileJob` python callback (reads the greeting,
   writes an uppercased+param-suffixed summary). First run: `built ==
   [greeting.txt, summary.txt]`, `hits == []`, `generation == 1`, real file
   content verified through the `outputs/` view symlink tree. Second run
   (fresh `Graph`, same project/store): `built == []`, `hits ==
   [greeting.txt, summary.txt]`. Third run with a flipped `Params`: only
   `summary.txt` rebuilds, `greeting.txt` hits. Fourth run flipping the
   param back: all hits again (§12.3 oracle) — content matches the first
   run's, confirming no drift/corruption from the rebuild-then-revert
   cycle.
2. `test_e2e_graphjob_expansion_runs_and_publishes` — a `GraphJob` whose
   callback adds a `CommandJob` at dispatch time; confirms `expand_graph_job`
   is actually invoked (this is exactly what the `graph_job` field fix in
   (B) makes work — before that fix this test fails with the added job
   never running, since the scheduler would call `run_in_process` on the
   `GraphJob` itself instead) and the expanded job's output reaches the
   view.
3. `test_e2e_partial_failure_raises_and_leaves_view_untouched` — one
   succeeding + one failing independent `CommandJob`; confirms
   `PPGRunError` is raised, `result.generation is None`, and no `outputs/`
   symlink is created at all (never having existed for this fresh
   project) — i.e. a failed run truly leaves no view-level trace.

**Wart discovered and worked around, documented in the test file itself**:
`_make_graph`'s `FileJob` callback is a *module-level* function in
`test_e2e.py`. With `PyEnv.current()` + `cloudpickle` installed (both true
in this venv), `select_transport` picks the cloudpickle fast path — but
cloudpickle pickles a module-level function **by reference** (module name +
qualname) when it believes the defining module is importable, not by
value. `test_e2e` *is* importable in the pytest parent process (pytest put
it there) but is **not** importable in the cold `python -I` shim
subprocess (isolated mode does not inherit pytest's rootdir `sys.path`
insertion, and `test_e2e` isn't installed as a package) — so the subprocess
raised `ModuleNotFoundError: No module named 'test_e2e'` trying to
unpickle. Worked around in the test by passing `paranoid=True` to
`ppg3.new(...)`, forcing the localscope-checked source-mode transport
(which ships the actual source text, no by-reference module lookup). This
is a **real, general limitation** of the cloudpickle same-env fast path as
currently implemented, not just a test artifact: *any* job callback defined
in a script's own top-level module (the common case for a real pipeline
script, not just tests) will hit the same `ModuleNotFoundError` once it
reaches the `-I` subprocess, unless that script also happens to be
`pip install`ed as an importable package. Not fixed here (would mean
either forcing `cloudpickle.register_pickle_by_value` process-globally in
`transport.py`, a behavior change affecting every job regardless of
paranoia, or detecting "is this module going to be importable from a fresh
`-I` interpreter" some other way — both felt like they needed their own
review rather than a fold-in here). Flagged as `TODO` below;
`paranoid=True` is the correct, already-available user-facing mitigation
today.

## TODO (new, from this pass)
- `RunCallbacks.run_in_process` still can't resolve `Job`/`JobSubset`-typed
  inputs to real paths for `UnsandboxedJob` (see (D) above) — needs either
  a `HostCallbacks` signature change surfacing resolved paths, or a
  separate side-channel; out of scope for this pass (core interface
  change).
- Cloudpickle same-env transport pickles top-level-module functions **by
  reference**, which breaks for any callback defined in a script's own
  top-level module once it reaches the cold `-I` shim subprocess (that
  module generally isn't importable there) — see the `test_e2e.py` wart
  writeup above. `paranoid=True` is today's user-facing workaround;
  consider forcing `cloudpickle.register_pickle_by_value` for the
  defining module of same-env callbacks as a real fix.
- No abort-flag wiring from Python (`py/src/lib.rs`'s `run()` always passes
  a fresh, always-`false` `AtomicBool`) — see the CONTRACT.md addendum.
- Re-run `cargo test -p ppg3-core` (and `-p ppg3-cli`, `-p ppg3-py` once it
  has tests) to completion once the sibling agent's `core/tests/
  scheduler.rs` work has released the shared `target/` build lock —
  `cargo check --workspace` is clean but that's not a substitute for the
  full suite. *(Done in the sandbox-verification pass below: 161 tests
  green.)*

## Sandbox verification pass (bwrap + unshare, WP3 runtime debt) — done

This environment (unlike the original dev container) has bwrap 0.11.2 and
working unprivileged user namespaces, so the never-executed enforcement
paths could finally be *run*. Both had real bugs that only runtime
execution could catch. Files touched: `core/src/executor.rs`,
`core/src/sandbox.rs`, `core/src/bin/sandbox_helper.rs` (new),
`core/tests/sandbox_unshare.rs` (new), `core/Cargo.toml` (bin target).
`core/src/scheduler.rs` etc. untouched; `py/src/lib.rs` untouched (see
TODO).

### bwrap (`executor.rs`) — bugs found & fixed

- **The old smoke test failed the moment bwrap actually existed**
  (`assert 1 == 0`): it ran `/bin/true` with zero mounts — bwrap's root is
  an empty tmpfs, so there was nothing to exec (and this NixOS host has no
  `/bin/true` anyway, only `/bin/sh`).
- **Nix dependency-closure bug (the big one)**: `bwrap_argv`'s per-tool
  "double mount" (`/nix/store/<tool>` bound at its own real path) is not
  sufficient to run *any* dynamically linked nix binary — the ELF
  interpreter (`ld-linux` in glibc's *separate* store path) isn't bound,
  so `execvp` fails with ENOENT before the program even starts. Verified
  by hand with the host's nix bash. Fixed: one `--ro-bind /nix/store
  /nix/store` emitted when any tool is nix-sourced (coarser than a
  per-closure bind, but the closure is not computable from a `Mount`
  alone; a future refinement could consume `nix path-info -r`).
- Additions for parity/realism, all argv-unit-tested: `--proc /proc`,
  `--tmpfs /ppg/tmp` + the same TMPDIR/HOME=`/ppg/tmp` defaulting
  NoneExecutor does, `--dir /ppg/in` / `--dir /ppg/tools` (uniform layout
  for jobs with no inputs/tools), `--die-with-parent`, and signal-aware
  exit codes (128+sig, shared `exit_code_of` helper with NoneExecutor).
- New `bwrap_runtime_available()` (spawns a trivial sandboxed command):
  the binary being on PATH does not imply user namespaces work.
- **4 real runtime integration tests** replace the old smoke test (skip,
  not fail, when bwrap/userns is unavailable or `/bin/sh` isn't
  nix-sourced): output write + cwd=/ppg/out; ro-input enforcement (read
  ok, write fails, host file untouched); env scrub + host-fs invisibility
  + writable /ppg/tmp; netns has only `lo` (via `/proc/net/dev`).
  Finding from the env-scrub test: with PATH scrubbed, nixpkgs bash
  reports its compiled-in `PATH=/no-such-path` — that's bash defaulting,
  not a leak; the test asserts the host PATH value doesn't appear instead.

### unshare (`sandbox.rs` `enter_sandbox`) — bugs found & fixed

Runtime-tested for the first time via a new test-only `sandbox_helper`
binary (`required-features = ["linux-sandbox"]`, same real-OS-process
pattern as `store_helper`) driven by `core/tests/sandbox_unshare.rs` —
`unshare(CLONE_NEWUSER)` cannot be called from the multi-threaded test
harness, and a successful `pivot_root` would hijack it. Helper exit code 2
= "userns unavailable" ⇒ test skips. Two real bugs:

1. **uid/gid captured after `unshare`**: `write_uid_gid_maps` called
   `geteuid()` *after* `unshare(CLONE_NEWUSER)`, which returns the overflow
   id (65534) in a not-yet-mapped namespace; writing `0 65534 1` to
   `uid_map` is EPERM (65534 is not the creator's parent-ns euid). Ids are
   now captured before `unshare`.
2. **ro-remount dropped locked mount flags**: the `MS_BIND|MS_REMOUNT|
   MS_RDONLY` pass didn't carry over flags inherited from the source mount
   (`nosuid`, `nodev`, atime modes). Inside a user namespace those are
   *locked*; dropping any of them makes the remount fail EPERM (hit
   immediately with a tempdir under this host's `/tmp`). Fixed by
   `statvfs`-ing the target and OR-ing the `ST_*`→`MS_*` flags in — the
   same strategy bwrap itself uses.

After the fixes the helper verifies from inside the pivoted namespace:
cwd, ro-mount readable + EROFS on write, `/etc/passwd` invisible,
old-root detached, tmpfs `/tmp` writable.

### Test-portability fix

`none_executor_cleans_up_workdir_on_success_keeps_on_failure` used
`/bin/true`/`/bin/false`, which don't exist on NixOS hosts (only `/bin/sh`
is POSIX-guaranteed) — was failing here before any sandbox work. Now
`/bin/sh -c true|false`.

### Results

- `cargo test -p ppg3-core` → **161 green** (default features; includes
  the 4 bwrap runtime tests actually executing here).
- `cargo test -p ppg3-core --features linux-sandbox` → **161 green**
  (includes `enter_sandbox_runtime_smoke` actually entering a namespace).
- `cargo clippy -p ppg3-core --all-targets` clean in **both** feature
  configs (also fixed 2 pre-existing unused-import warnings the feature
  build had). `rustfmt` run on all touched files. `cargo check -p
  ppg3-cli` clean. `cargo check -p ppg3-py` not possible in this sandbox:
  pyo3 is absent from the offline cargo cache (environment limitation;
  `py/src` untouched by this pass).

### TODO (new, from this pass)

- **Executor selection is still hardwired**: `py/src/lib.rs` constructs
  `NoneExecutor` unconditionally. Flipping to `BwrapExecutor` when
  `bwrap_runtime_available()` is *not* safe yet: python jobs exec
  `sys.executable` (a host venv path) with no corresponding tool mount, so
  under bwrap they'd ENOENT. Needs the job's `PyEnv` lowered as a proper
  `/ppg/tools/...` mount (plus the venv/nix-env closure story) before
  bwrap can run python jobs; `CommandJob`s with nix tools would work
  today. Suggest a config knob (`sandbox="bwrap"|"none"`) rather than
  auto-detection when wired.
- Non-nix tools (e.g. a `/usr/bin`-sourced Mount) generally cannot run
  under bwrap — their FHS library closure isn't mounted and `PreparedJob`
  has no vocabulary for it. Nix-sourced tools are the supported bwrap
  path; the integration tests skip if `/bin/sh` isn't nix-sourced.
- Whole-store `/nix/store` bind is coarser than the per-closure ideal
  (hermeticity: a job can read store paths it didn't declare; it cannot
  write anything). Refine via `nix path-info -r` at tool-resolution time
  if that matters for §5 key-document honesty.
- `enter_sandbox` still lacks the PID-namespace second fork (pre-existing,
  documented in `sandbox.rs` module docs; forkserver-scoped).
