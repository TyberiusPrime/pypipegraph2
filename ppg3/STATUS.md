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
  full suite.

## Forkserver templates (§6.4, sandbox=none children) — done

`core/src/forkserver.rs` + `python/ppg3/_template.py` + wiring
(`executor.rs` staging extracted for reuse; `PreparedJob.runtime` added;
`py/src/lib.rs` run() takes optional `template_argv`; `ppg3.new(...,
forkserver=True)` default-on with off-switch).

- One warm template per (interpreter, canonical preload list); JSON-lines
  protocol ready/run/started/exited; one reader thread per template; lazy
  start, respawn-on-death, kill-on-Drop at run end (§6.7 cross-run
  persistence not in scope).
- Template is single-threaded, imports preloads, forks per job; the child
  redirects stdio to the job log dir, enters the staged sandbox=none
  layout, and runs the shim entry inside the warm interpreter (no exec).
  It never runs user code at top level and never starts threads.
- The unshare/pivot_root child sandbox entry remains the feature-gated
  stub — forkserver and sandbox entry are decoupled; only the latter
  needs user namespaces.
- Warts: the template inherits PYTHONPATH/VIRTUAL_ENV/PATH from the
  coordinator so `ppg3` is importable — weakly hermetic until the shim
  ships as a tool input; enforced-by-review, not by namespace.
- Proof tests (python e2e): same-template reuse via parent pid equality,
  preload warm at callback start, env scrubbed (planted secret absent),
  forkserver on/off produce identical iks/ohs (templates do not poison
  keys). Core integration tests drive the protocol with a self-contained
  fake template (no ppg3 python dependency in ppg3-core tests).
- Totals after this WP: 180 Rust + 101 Python tests, clippy clean.

## Watch mode (§6.7 "watch" bullet) — done

Python-only, no Rust changes (`core/src/views.rs`'s existing
`write_generation(..., ephemeral: bool)` already covered what the Rust
side needed). New: `python/ppg3/__main__.py`, `python/ppg3/watch.py`,
`python/tests/test_watch.py`. Additive: `python/ppg3/run.py` (`watch_mode()`
context manager, `_watch_active` flag, `_last_run_info` slot +
`get_last_run_info()`), `python/ppg3/jobs.py` (`Graph.record_watched_path`/
`.watched_paths()`, wired into `_lower_input`'s `File` branch and
`FileJob.__init__`'s `Source` branch). Full rationale for every piece is in
CONTRACT.md's new "Additive addendum: `python -m ppg3 watch`" section
(cross-referenced from a rewritten "Scope deviations" bullet); this entry
only records the headline deviation and totals.

**Deviation (the one that matters): polling instead of inotify.** §6.7 says
the coordinator watches leaf inputs/script/`Source` files "via inotify".
This dev container has no inotify Python binding, and the project takes no
new third-party dependencies (CONTRACT.md's own rule) — implemented
`PollingWatcher` instead: polls `(mtime_ns, size)` per tracked path,
default interval 0.5s (`--interval` flag). Same semantics (a definition
pass re-runs exactly when a watched path's content or existence changes,
including appear/disappear), strictly worse latency/efficiency (bounded by
the poll interval instead of kernel-immediate). `PollingWatcher` exposes
only a narrow `poll() -> List[str]` (+ `set_paths()`) interface so a future
inotify-backed implementation is a drop-in replacement for the loop in
`watch.run_watch` — nothing else in the loop, in `run.py`, or in `jobs.py`
would need to change.

Other notable decisions (all detailed in CONTRACT.md):

- `python -m ppg3 watch <script> [--interval SECONDS] [args...]` argv is
  parsed by hand, not via `argparse`'s `REMAINDER` — `REMAINDER` greedily
  swallows every token once it starts consuming at the first positional,
  which silently ate a later `--interval` (confirmed by hand before
  settling on the manual parser: `watch script.py --interval 0.1` — the
  exact required CLI shape — parsed with `args.interval` still at its
  default and `--interval`/`0.1` dumped into `script_args` instead).
- Discovered and worked around a real footgun while wiring `watch.py` to
  `run.py`: `ppg3/__init__.py` does `from .run import run`, which rebinds
  the *package* attribute `ppg3.run` to that function, shadowing the
  submodule of the same name. `from . import run as x` (and even `import
  ppg3.run as x`, confirmed by hand — both resolve via attribute access on
  the already-imported parent, not via `sys.modules['ppg3.run']`) silently
  bind to the *function*, not the module, so `x.watch_mode()`/
  `x.get_last_run_info()` raise `AttributeError`. Fixed by importing the
  specific names needed (`from .run import PPGRunError, get_last_run_info,
  watch_mode`) — `from module import name` resolves `name` against the
  freshly-imported submodule object itself, not through the parent
  package's (overwritten) attribute. Left an explicit comment in
  `watch.py` and a matching one in `test_watch.py` (which hits the same
  trap reaching for `_watch_active` in one test) so nobody "fixes" it back
  to the natural-looking `from . import run`.
- E2E tests (`test_watch.py`) drive a real `python -m ppg3 watch`
  subprocess, poll `.ppg3/views/<n>` on disk with a 30s deadline / 50ms
  poll (never a bare `sleep`-and-hope), and always `proc.kill()` in a
  `finally` — a hang here would otherwise hang the whole suite. Covers:
  first generation + `outputs/` content, a leaf-file change producing
  generation 2 (content updated, `.ephemeral` marker present per
  `views.rs`'s `EPHEMERAL_MARKER`), clean exit 0 on `SIGINT`; and,
  separately, a syntax error introduced mid-watch (subprocess stays alive,
  traceback lands on real stderr — see the "traceback to stderr, not the
  status stream" note in CONTRACT.md) followed by fixing the file and
  observing generation 2 appear.
- Manually verified end-to-end outside pytest too (scratch pipeline,
  `python -m ppg3 watch pipeline.py --interval 0.2`): confirmed `.ephemeral`
  markers on both generations' `meta.json` (`"ephemeral": true`) and on
  disk (`views/<n>/.ephemeral`), debounced batching, and the syntax-error
  survive/resume path, before writing the automated tests above.
- `Graph.watched_paths()` never includes the pipeline script itself — a
  `Graph` has no notion of "the script that defined it"; `watch.run_watch`
  adds the script path itself to the tracked set on every iteration.

Totals after this WP: 180 Rust tests unchanged (no Rust touched;
`cargo test -p ppg3-core --lib` reconfirmed green: 121 passed, 1 ignored —
`--lib` only, the rest of the 180 lives in `core/tests/*` integration
suites not run by that command) + 122 Python tests (101 prior + 21 new in
`test_watch.py`), all green.

## TOFU source patcher (§7.6, WP11-adjacent) — done

Python-only (rule: touch only `python/ppg3/{jobs,run}.py`, new
`python/ppg3/tofu.py`, `python/pyproject.toml`, new
`python/tests/test_tofu.py`, this file, CONTRACT.md — no Rust, no
`PPG3_DESIGN.md`). Picked up from the STATUS.md TODO left by the WP7-Rust
pass ("TOFU source-patching (§7.6) is not implemented ... `FetchJob`
accepts `blake3=None` outside `--frozen`/CI mode and simply carries
`fixed_output: null`") — that half was already correct and untouched here;
this WP adds the other half: call-site recording + the post-run patcher.

**What changed:**

- `jobs.py`: `FetchJob.__init__` now records `self._call_site = (file,
  lineno)` via a new `_record_call_site()` helper — an `inspect`-based walk
  up the stack to the first frame outside the `ppg3` package directory
  (`_PKG_DIR`), returning `None` if none is found with a real on-disk file
  (e.g. a `-c`/stdin/REPL frame). Recorded unconditionally (cheap — a few
  frame hops), not just when `blake3=None`, since it costs nothing and
  keeps the logic in one place. `--frozen` rejection of `blake3=None` is
  unchanged (already implemented, already tested by
  `test_fetchjob_frozen_mode_requires_blake3` in `test_jobs.py`, which
  still passes untouched); the fixed_output/key-derivation shape it already
  had (`fixed_output=None` for the unpinned case, recipe = `{"kind":
  "fetch", "url": ...}` — no `blake3` in the recipe) also needed no
  changes, which matters: `core/src/scheduler.rs`'s `derive_key` never
  reads `job.fixed_output` (confirmed by reading it, not assumed), so
  patching the hash in later never changes `ik` — the whole "second run is
  an all-hits re-run" guarantee falls out of that for free.
- New `python/ppg3/tofu.py`: `run_tofu_pass(graph, report, core, handle)`,
  called from `run.py` right after the `report.get("failed")` check (i.e.
  only on a successful run, per the DECISION text). For every `FetchJob`
  with `blake3 is None`: reads `report["job_entries"][job.id]` for `(ik,
  _oh)`, calls `core.lookup(handle, ik)`, asserts the manifest's `content`
  map has exactly one entry (FetchJob always has exactly one output — the
  task's own "assert that" instruction, kept as a hard assertion since a
  violation would mean an internal invariant broke, not a normal
  degrade-gracefully case) and takes its `blake3`. Groups by call site;
  singleton sites get patched (libcst available) or tabled (not); shared
  sites and sites with no recorded call-site always go straight to the
  table. `libcst` import is inside a `try/except ImportError` — if missing,
  every unpinned job goes to the table with a stderr hint to `pip install
  'ppg3[tofu]'`; the run itself never fails on this account.
- Patch mechanism (`tofu._patch_file`): libcst (`MetadataWrapper` +
  `PositionProvider`) is used only to *locate* — the `Call` node(s)
  starting on the recorded line whose callee's last name/attribute segment
  is `FetchJob` (plus a same-line import-alias scan for `from ppg3 import
  FetchJob as FJ`-style bare-name aliasing — attribute-form aliasing like
  `import ppg3 as p; p.FetchJob(...)` needs no special-casing since the
  attribute's own last segment is still literally `FetchJob`), and within
  a match, the `blake3=` keyword arg's value span if present, or the last
  argument's end position if not. The actual edit is a **plain string
  splice** at those byte offsets (source split into lines once, a
  cumulative-offset table converts libcst's 1-indexed-line/0-indexed-column
  `CodePosition` into a flat offset) — not a libcst tree
  transform-and-regenerate. This was a deliberate change of approach after
  hand-verifying that libcst's `codegen` does not reliably reproduce
  `ParenthesizedWhitespace(indent=True)` multi-line indentation when a
  *new* node is spliced into an existing args list (empirically confirmed:
  copying an existing trailing-comma `Comma` node's `whitespace_after`
  verbatim onto a newly-appended `Arg` rendered flush-left instead of
  matching the sibling indentation — a real codegen quirk, not a
  misunderstanding of the API). Text-splicing sidesteps this entirely and
  is strictly stronger for the "preserve exact formatting elsewhere"
  requirement: it is by construction a no-op everywhere except the spliced
  span, so "byte-diff only at the kwarg" holds for tabs/comments/any other
  content without needing a dedicated formatting-preservation proof.
  Multiple singleton call sites in the same file are patched in one
  read-modify-write (edits applied right-to-left by offset so earlier
  edits' offsets stay valid).
- Multi-line calls ("kwargs on following lines, call starts at the recorded
  line"): patched correctly (valid Python, correct kwarg value) but not
  claimed byte-perfect on indentation — the new/replaced text lands
  immediately after the last argument's value (before any existing
  trailing comma), which for a no-trailing-comma multi-line call produces
  e.g. `url="y", blake3="..."` on the last arg's own line rather than a
  new indented line. Explicitly in scope per the task ("patched correctly"
  is the multi-line bar, exact-text is only required for the single-line
  case) — `test_patch_multiline_call_starting_at_recorded_line` and
  `test_patch_multiline_no_trailing_comma` in `test_tofu.py` assert
  validity + correct kwarg values via `ast.parse`, not exact text.
- `run.py`: one new block right after the existing `if
  report.get("failed"): raise ...` — `from . import tofu; tofu.
  run_tofu_pass(graph, report, core, handle)`. Local import (not
  module-level) so `tofu.py` (and its lazy `libcst` import) isn't on the
  hot path for every `import ppg3`.
- `_shim.py`: **no changes needed** — verified by reading `run_fetch()`:
  `expected = fetch.get("blake3")` is already `None`-safe (`if expected is
  not None and digest != expected`), the digest is always computed via
  `_hash_file_blake3` and printed regardless, and it lands in the store
  entry's manifest content the normal way (publish always hashes staged
  files) — the coordinator's `run_tofu_pass` reads it back from there via
  `core.lookup`, exactly as the task described. This matches the "Fix only
  if broken" instruction: it wasn't.
- `pyproject.toml`: added `[project.optional-dependencies] tofu =
  ["libcst"]`, and added `libcst` to the existing `test` extra so
  `test_tofu.py` runs unconditionally in the dev venv (installed via `uv
  pip install libcst`, `libcst==1.8.6` at time of writing). No graceful
  skip-marker was added for "libcst not installed" in `test_tofu.py`
  (unlike `requires_blake3`/`requires_core`) since the `test` extra now
  always pulls it in; the "libcst missing" *code path* itself is still
  covered (`test_run_tofu_pass_libcst_missing_prints_table`, via a
  monkeypatched `builtins.__import__` that only fails for `"libcst"`, not
  by actually uninstalling it).

**§7.6 corners interpreted (none change a DECISION, all pragmatic
"how exactly" fills-ins the design text leaves open):**

- "the Call node starting on that line": confirmed empirically (small
  standalone script, not shipped) that CPython 3.11's `frame.f_lineno`
  inside a class `__init__` called from a multi-line call site reports the
  line where the call *starts* (e.g. `obj = Foo(` on line 9 of a call whose
  args run lines 9-12), i.e. `_record_call_site()`'s `frame.f_lineno` and
  libcst's `Call` node `PositionProvider.start.line` agree by construction
  for this Python version — this is *why* matching on `node.start.line` is
  correct and not a lucky guess, not just an assertion from the design
  text.
- Aliased bare-name imports (`from ppg3 import FetchJob as FJ`): resolved
  via a one-off textual scan of the target file's `ImportFrom` statements
  collecting every local name `FetchJob` is imported as, unioned with the
  literal name `"FetchJob"` itself — deliberately not real import-graph
  resolution (matches the task's own "resolve pragmatically" framing).
  Does not attempt to detect *shadowing* (e.g. a local variable later
  named `FetchJob` that isn't the ppg3 class) — out of scope, same spirit
  as the attribute-segment rule already accepting `anything.FetchJob(...)`
  without checking what `anything` actually is.
- "FetchJob has exactly one output; assert that": implemented as a Python
  `assert`, deliberately not a soft-degrade path — an unpinned `FetchJob`
  publishing a manifest with anything other than exactly one content entry
  would mean `FetchJob.job_def()`'s `outputs_declared=[OUTPUT_NAME]`
  (singular, fixed) stopped matching what actually got published, which is
  an internal-invariant violation, not a normal "TOFU can't figure out
  what to do" case that should degrade to the table.
- Watch-mode integration (patch triggers watcher re-run → all hits): not
  added as an automated test. `python -m ppg3 watch` (`watch.py`) polls
  tracked paths and reruns `runpy.run_path` on change (§6.7, done in an
  earlier WP); since `run()` now unconditionally runs the TOFU pass on
  success and the pinned-hash re-run is provably an all-hits run (same
  `ik`, see above), the interaction should just work as a consequence of
  two already-tested independent mechanisms rather than needing its own
  proof — but a full subprocess-based watch e2e test (spawn `python -m
  ppg3 watch`, wait for generation 1, mutate nothing itself but observe the
  *patcher* mutate the watched script out from under the watcher, wait for
  generation 2, assert all-hits) was judged not cheap enough to add
  confidently non-flaky in the time available, matching the task's own
  "skip if flaky, note in STATUS.md" allowance. The non-watch e2e test
  (`test_e2e_tofu_pins_hash_then_second_run_is_all_hits`) covers the
  all-hits-on-repin claim directly via two `runpy.run_path` calls instead.

**Pin message format** (printed to stdout, one line per patched job):
`pinned <view-path> (<digest[:8]>…) in <file>:<lineno>` — e.g. `pinned
inputs/genome.fa.gz (ab12cd34…) in pipeline.py:42`, matching §7.6's own
example modulo the task instructions' explicit "first 8 hex" (the design
snippet elides after 4; the task text overrides that with 8, which is what
is implemented and tested). Table fallback (unresolved jobs, printed once
per `run_tofu_pass` call if non-empty): a header line ("could not
auto-patch ... wire these into your own lookup ...") followed by one `  
<url>\t<blake3>\t(view=<view_path>)` line per job.

Totals after this WP: 180 Rust tests unchanged (no Rust touched this WP;
`cargo test -p ppg3-core --lib` reconfirmed green: 121 passed, 1 ignored)
+ 145 Python tests (122 prior + 23 new in `test_tofu.py`), all green:
`cd ppg3/python && source .venv/bin/activate && python -m pytest tests/ -q`
→ `145 passed`.

## Session mode: cross-run template persistence (§6.7) — done

Rust half (previous commit): TemplateManager split from ForkserverExecutor,
Session pyclass, run(session=...), template key includes runtime.python_env
(changed PyEnv ⇒ new template automatically; old one idles until session
end — no idle reaping in v1).

Python half (this commit, coordinator-written after the agent hit a usage
limit): module-level lazy session in run.py (created on first forkserver
run; stable mkdtemp work dir per process), run() dispatches through it, so
watch iterations and repl use reuse warm templates with no further wiring.
ppg3.session_stop() (exported) kills templates and clears the §6.7
loader-layer memos, which are now module-level keyed by ik (an InProcess
job with an unchanged key document runs once per session, not per run).
Proof tests (test_session.py): same template pid across two runs with
distinct graphs/stores/projects; session_stop → count 0 → next run
respawns (new pid); loader memo hit across runs, cleared by session_stop;
idempotent stop. Totals: 184 Rust + 149 Python tests, clippy clean.
