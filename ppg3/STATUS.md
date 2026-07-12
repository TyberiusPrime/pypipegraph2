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
