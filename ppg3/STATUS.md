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

## TODO
- Wire `run_in_process` (`UnsandboxedJob`) once the Rust scheduler's
  `HostCallbacks` real-path-resolution shape is settled (see deviation
  above).
- Resolve the shim stdin-delivery interface gap with whichever agent
  implements `core/src/executor.rs`.
- Once `ppg3._core` exists: add `requires_core`-gated integration tests in
  `python/tests/` that actually call `ppg3.run()` end-to-end (two-job
  pipeline per CONTRACT.md's "Testing bar" smoke test), and confirm/adjust
  the `InputRef`/`ExecTemplate`/`Retain` JSON-tagging assumption above
  against the real Rust `#[derive(Serialize)]` output.
- `ppg3 lint` (checking that `select_transport`'s recorded
  `localscope_modules` actually resolve inside a job's declared `PyEnv`) is
  not implemented — only the recording (via `LocalscopeReport.modules`) is
  in place, per CONTRACT.md's WP7 scope (the lint proper reads like a CLI
  concern, WP5).
- No `ppg3.repl()`/session-mode/watch support (WP11, explicitly deferred).
