# pypipegraph2 codebase audit (2026-07)

Scope: full read of `src/engine.rs`, `src/lib.rs`, `python/pypipegraph2/`
(runner, graph, jobs, hashers, history_comparisons, parallel, util,
interactive, localscope, cli, ppg1_compatibility skimmed), fuzz harness docs,
build config. Focus: correctness, per request. Verified state: `cargo test
--release` passes (81 tests); `pytest tests/test_basics.py tests/test_jobs.py
tests/test_invariants_and_dependencies.py` passes (264 passed, 7 skipped).
Bug B1 below was confirmed with a live reproduction.

---

## 1. Architecture overview

- **Rust engine** (`src/engine.rs`, ~2600 lines): a signal-driven state
  machine over three job kinds (Always / Output / Ephemeral). Decides, per
  job, run vs. skip vs. upstream-failure, drives ephemeral
  demand-propagation, and owns history production (`new_history`).
- **PyO3 boundary** (`src/lib.rs`): `PPG2Evaluator` exposes event methods;
  the engine calls *back into Python* for two things: history comparison
  (`is_history_altered`) and the per-job sorted input list
  (`get_input_list`). History values are JSON strings; job identity is
  strings with in-band separators (`:::` for multi-output jobs, `!!!` for
  history edge keys).
- **Python layer**: `graph.py` (graph assembly, history load/save, signals),
  `runner.py` (thread pool that polls the evaluator under a lock, forks
  subprocesses for file-generating jobs, watcher process for orphan
  reaping), `jobs.py` (~3600 lines, all job classes + hashing semantics).

## 2. Strengths

1. **The engine's testing regime is genuinely excellent.** Exhaustive
   enumeration of all graphs up to 7 nodes (and failure combinations up to
   6), 81 in-tree Rust tests, and two AFL fuzzers with real semantic oracles
   (deadlock, topological order, missed-rerun/spurious-rerun convergence
   checks). `fuzz/README.md` documents found-and-fixed bug families with
   regression inputs. This is far beyond what most workflow engines have.
2. **The three-kind reduction (Always/Output/Ephemeral) is a good idea.**
   It collapses ~15 user-facing job classes into a small, exhaustively
   testable core — this is the right factoring of the problem.
3. **Determinism is taken seriously**: `FxBuildHasher` instead of
   RandomState with an explicit comment why; sorted input lists; sorted JSON
   history (`sort_keys=True`).
4. **Defense in depth on invariants**: `localscope` bytecode analysis
   rejecting undeclared globals in job callbacks is a rare and valuable
   correctness feature; function invariants compare bytecode with source
   fallback; `.sha256` sidecar files are validated for staleness
   (`hashers.py`).
5. **Careful post-mortem comments.** Many past bugs are documented in place
   (watcher lifecycle in `runner.py`, skip-then-upstream-failure conversion
   in `engine.rs:1455`, the generation-counter deadlock fix).
6. **History write is crash-safe-ish**: temp file + `.backup` + rename
   (`graph.py:_save_history`).

## 3. The Rust/Python split — is it in the right place?

**Verdict: the split is conceptually right, but the boundary is drawn
slightly wrong, and the scariest code ended up on the Python side.**

- The engine is *not* the performance-critical part (each edge is one string
  comparison; 100k-job graphs are small work). The real value of Rust here
  is the type-checked state machine plus the exhaustive/fuzz test
  infrastructure. That value is real — keep it.
- But the engine is not self-contained: it calls back into Python per edge
  for `is_history_altered` and per job for `get_input_list`. That makes the
  engine's behavior depend on Python code that the Rust test rigs replace
  with a trivial string-compare strategy (`StrategyForTesting`) — i.e. the
  fuzzers do not exercise the *actual* comparison semantics
  (`history_comparisons.py`, `Job.compare_hashes` per class). The subtle
  mtime/size/bytecode-tolerant comparison logic lives outside the
  exhaustively tested core.
- Meanwhile the most failure-prone code in the project is the Python
  runner's threading: N worker threads all polling one mutex-guarded
  evaluator, coordinating via two `Event`s, an unused `PriorityQueue`,
  `async_raise(KeyboardInterrupt)` for abort, and fork-via-main-thread
  callbacks. Several concrete bugs below (B2, B5, B12) live exactly there,
  and the repo's own last commit ("robustness & detector for double running
  jobs?!") plus the die-hard fallbacks in `runner.py:834-890`
  (`ppg_evaluator_debug.txt`, `ppg_double_dispatch_debug.txt`) show the
  authors don't fully trust the engine/runner combination yet.

**Recommendations** (in order of value):

1. Move the remaining history semantics into Rust: pass structured hashes
   (or at least precomputed comparison outcomes) across the boundary once,
   instead of callbacks. The engine then becomes a pure function of
   (graph, history, disk-oracle), the fuzzers test the real semantics, and a
   whole class of reentrancy/GIL concerns disappears.
2. Replace the N-self-coordinating-threads runner with one scheduler thread
   that owns the evaluator plus a dumb worker pool. Most of the locking,
   the `check_for_new_jobs` 60s-timeout mystery (`runner.py:929`), and the
   double-dispatch class of bugs go away structurally.
3. Do **not** rewrite the engine back into Python — the test infrastructure
   is the asset, and it's Rust-side.

## 4. Confirmed / high-confidence bugs

### B1 — `AttributeLoadingJob` crashes on rerun in the same process (confirmed by repro)

`cleanup()` deletes the loaded attribute but leaves the stored
`_<attr>_hash` on the user's object (`jobs.py:2362-2375`). On a later run in
the same process (interactive, `_RunAgain` after a `JobGeneratingJob`, or
plain re-`new()`+`run()` in a notebook/script), if the job's inputs are
unchanged but a downstream forces it to run, `run()` sees
`current_hash == get_hash()` and calls `get_value()` →
`AttributeError: '<O>' object has no attribute 'attr'` → the job and all its
downstreams fail. Reproduced with a 30-line script (two runs, second run
invalidates only the downstream).

`DictEntryLoadingJob` half-knows about this: its cleanup deletes the hash
and skips cleanup entirely in CONSOLE_INTERACTIVE (`jobs.py:2458-2464`).
`AttributeLoadingJob.cleanup` ignores the runmode and keeps the hash — fix by
mirroring the DictEntry behavior (delete the hash in cleanup, and/or skip
cleanup in interactive mode).

### B2 — `CoreLock` has a lost-wakeup race (`parallel.py:55-84`)

`_acquire` checks `remaining` under `self.lock`, releases it, *then* waits
on `self.condition`. A `_release` (with its `notify_all`) can land in that
window; the acquirer then sleeps having missed the only wakeup. If that
release was the last one in flight (e.g. an `AllCores` job waiting while the
final other job finishes), the graph stalls until an unrelated release or
forever. Classic fix: one `Condition`, re-check the predicate inside
`wait()` (`while self.remaining < count: cond.wait()`), mutate `remaining`
under the condition's lock.

### B3 — Renamed multi-output-job history is never pruned; the filter is dead code (`engine.rs:683-704`)

`multi_parts_to_jobs` is keyed by the *parts* of multi ids (and full ids of
plain jobs). `filter_if_renamed` then looks up the *full* `a:::b` id — which
can never be a key (parts can't contain `:::`, plain ids don't either). So
the lookup always misses, the function always returns `true`, and the
"remove old history when a MultiFileGeneratingJob is renamed" logic never
removes anything (verified by simulating the lookup semantics). Tests
(`test_job_gen_leading_to_missing_history*`) show aggressive removal was
deliberately weakened because it destroyed history of pruned jobs — but the
current state is misleading dead code plus unbounded accumulation of stale
`a:::b`-keyed entries in the history file. Either fix the lookup to check
parts (`job_id.split(":::")` → does any part now belong to a *different*
job?) or delete the machinery and document that renamed-MFG history is
garbage-collected never.

### B4 — Signal-driven abort/stop path crashes: `int` has no `.write` (`interactive.py:93-95`)

`reentrace_safe_command_from_signal` does `self.signaler[1].write(b"x")`, but
`self.signaler = os.pipe()` yields raw fds. Must be
`os.write(self.signaler[1], b"x")`. Every caller
(`graph.py:_install_signals`, the `_debug_allow_ctrl_c` abort/stop paths)
gets an `AttributeError` inside a signal handler instead of an abort.

### B5 — `abort()` can strand a worker in `fork_signal.wait()` forever

A worker thread queues `fork_callback` on `runner.main_thread_callbacks` and
blocks on `fork_signal.wait()` (no timeout, `jobs.py:958-972`). `abort()`
sets `evaluation_done` and `main_thread_event` (`runner.py:612-627`); the
main loop breaks *without draining the callback queue*
(`runner.py:461-474`), so the fork never happens and `fork_signal` is never
set. The `async_raise(KeyboardInterrupt)` escape hatch does not help: async
exceptions are only delivered on bytecode boundaries, and `Event.wait()`
blocks in C. `Runner.run`'s `finally` then hangs in `t.join()`. Small
window, but it is exactly the "abort hangs" failure mode. Fix: drain
`main_thread_callbacks` after the loop, or give the wait a timeout that
checks `runner.aborted`.

### B6 — `SIGUSR1` handler is never restored (`graph.py:704-717`)

`_install_signals` saves `_old_signal_usr1` but `_restore_signals` only
restores HUP and INT. After a run, SIGUSR1 still invokes
`self.runner.abort()` — and `self.runner` has been deleted
(`graph.py:337`), so a stray USR1 between runs raises `AttributeError`
in the handler instead of whatever the host application installed.

### B7 — `NameError` in `_hash_object`'s error path (`jobs.py:2174-2178`)

`for k in res[UNPROCESSED_KEY]` — `res` is undefined (should be `my_hash`).
When `DeepHash` fails to process an object, the user gets a `NameError`
instead of the intended "Hashing failed on parent obj" `ValueError`.

### B8 — Wrong-outputs failures get their error message swallowed (`runner.py:707`)

`log_failed_job` does `error.args[1]` unconditionally. The
`JobContractError` for "returned the wrong set of outputs"
(`runner.py:1035-1038`) is constructed with a single arg, so reporting it
raises `IndexError`, caught by the blanket handler which prints "The
original job failure has been swallowed." The failure itself still
propagates via `job_outcomes`, but the console/error-file report — the thing
the user reads — is lost.

### B9 — Dead `all_present` logic in `MultiFileGeneratingJob.run` (`jobs.py:895-912`)

Both branches of the exists-check set `all_present = False`, so the
"all files present → return historical_output" shortcut can never fire, and
the follow-up "nuke all output files" loop re-unlinks files already
unlinked. Behaviorally this is *safe* (if the evaluator decided to run, a
rebuild is correct), but the block is misleading; the comment above it
describes behavior the code does not have. Delete it.

### B10 — Job priorities are silently ignored

`Runner.__init__` creates `self.jobs_to_run_que = queue.PriorityQueue()`
(`runner.py:286`) which is never used; `Job.que_priority` (`jobs.py:261`,
set to -2 for MFGs at `jobs.py:724`) is never read. Dispatch order is
whatever `FxHashSet::iter().next()` yields (`engine.rs:588-591`). Either
implement priority dispatch in `next_job_ready_to_run` or delete the fields.

### B11 — The KeyboardInterrupt "retry" in history comparison is a no-op (`history_comparisons.py:14-21`)

`while True: try: return ... except KeyboardInterrupt: raise` — the comment
says a KeyboardInterrupt here must not reach Rust ("the Rust code really
does not expect this to fail ever"), but the handler re-raises, so the
guard does nothing. Given B12 (async KeyboardInterrupt injection), an abort
landing inside the history callback still propagates into
`event_job_success` → pyo3 → `expect("History comparison failed on python
side")` → panic → the worker's `except BaseException` kills the process
with SIGTERM (`runner.py:1142-1145`). If the intent is "never interrupt
here", the except needs to swallow-and-retry (or better: stop using async
exceptions, see B12).

### B12 — Abort via `async_raise(KeyboardInterrupt)` is structurally unsafe (`runner.py:623-627`)

Asynchronously injected exceptions can land anywhere in a worker: while
holding `evaluator_lock`, inside the Rust→Python callbacks, between
`event_now_running` and the bookkeeping that pairs with it. This is the
likeliest root cause of the residual "evaluator not finished / nothing
ready / nothing running" and double-dispatch states that the runner now
detects and dumps (`runner.py:834-890`) — states the serial Rust fuzzers
can't reproduce because they never interrupt mid-event. A cooperative
cancellation flag checked between jobs (plus killing child processes, which
already happens via the watcher) would remove the whole class.

### B13 — `os.fork()` from a multi-threaded process (`jobs.py:961`)

Every (Multi)FileGeneratingJob forks while ~cores worker threads plus
logging threads are live; the child then runs arbitrary user code. Python
3.12 flags this (the test run emits 397 `DeprecationWarning`s: "use of
fork() may lead to deadlocks in the child"), and it can genuinely deadlock
if another thread held e.g. a logging or allocator lock at fork time.
Forking from the *main thread* (already implemented via
`main_thread_callbacks`) reduces but does not remove the risk — the other
threads still exist. Given cheap COW data sharing is a design goal, this
may be an accepted tradeoff, but it deserves a documented decision and
ideally an opt-in spawn/forkserver path for jobs that don't need COW.

## 5. Design-level correctness concerns (arguably by design, worth a decision)

### D1 — mtime+size fast path misses same-second modifications

`FileInvariant.run` (`jobs.py:2049-2056`), `MultiTempFileGeneratingJob.run`
(`jobs.py:1252-1254`) and `hash_file` treat "same `int(mtime)`, same size"
as unchanged without hashing. A file rewritten within the same second with
equal size (common for fixed-size binary records, or fast test loops) is a
missed invalidation. Make lives with the same tradeoff, but ppg2 could use
`st_mtime_ns`, or hash whenever `mtime >= history-write-time`.

### D2 — Abort discards history of jobs that never ran

On abort, every unfinished job — including ones that never started — becomes
`FinishedAborted`, and `new_history` deletes their job/input keys
(`engine.rs:783-805`). Ctrl-C on a large graph therefore forces re-running
(at minimum re-hashing/validating, for Output jobs a full rebuild since
`identify_missing_outputs` treats missing job history as "redo",
`engine.rs:2550-2566`) of everything that hadn't finished, even jobs whose
outputs are present and would have validated. Correct but very expensive;
keeping history for jobs still in `NotReady(Unknown)`/never-started states
would be safe.

### D3 — Recursion depth limits vs. the "100k jobs" claim

Deep chains hit hard limits well before graph *size* does:
- Rust: `inner_process_signals` recurses one level per signal generation —
  a >1500-deep skip-chain returns `InternalError("Depth ConsiderJob
  loop...")` (`engine.rs:1172-1174`); `propagate_job_required`,
  `reconsider_delayed_upstreams`, `_job_and_downstreams_are_ephemeral` are
  unboundedly recursive over ephemeral chains (stack overflow = process
  abort, not a Python exception).
- Python: `_recurse_pruning`/`_recurse_unpruning` (`runner.py:296-313`) and
  `Job.dump_subgraph_for_debug` recurse per node — `RecursionError` at
  ~1000-deep chains when pruning is used.
Wide graphs are fine; deep ones are not. Converting these to explicit
worklists is mechanical.

### D4 — `SharedMultiFileGeneratingJob._cleanup` can race concurrent graphs

`_cleanup` (`jobs.py:3278-3306`) rmtree's every `done/` entry not referenced
by a `used_by/*.uses` symlink. A *concurrent* graph in another process that
has just renamed its build dir into `done/` but not yet created its
`by_input` symlink (`jobs.py:3061-3068`) can have its output deleted from
under it. The file lock introduced later in `run` only guards the JSON log,
not cleanup. The docstring says overlapping runtimes are "calc twice, one
thrown away" — this race is worse (missing files → the defensive
`ValueError("missing output files - did somebody go and delete them?!")`).
Also: the `filelock.Timeout` retry loop deletes the lock file (defeating
the lock for other waiters) and, if retries exhaust, silently skips logging
usage — which later feeds `_cleanup`'s notion of "unused".

### D5 — The engine tolerates upstream-failed upstreams oddly in `update_validation_status`

`engine.rs:1880-1887`: the `is_upstream_failure` guard has an empty body
(the `return Ok(Unknown)` is commented out, with a comment describing the
stale-ConsiderJob situation it was meant to handle). Execution then falls
into `edge_invalidated`, which errors with `InternalError("No current
history for job? Unexpected")` if that upstream has an old edge-history
entry and no `history_output`. The fuzzers haven't hit it (the ignore-set
usually eats the stale ConsiderJob first), but the empty guard is either
needed (restore the return) or dead (delete it plus the comment); as-is it
documents a hazard without defusing it.

## 6. Smaller findings / hygiene

- `__init__.py:61-87`: the numba probe compiles and runs a parallel JIT
  kernel *at import time* (slow import, import-order side effects), and the
  try has an unreachable duplicate `except ImportError` block.
- `cli.py:31-32`: dead second `if pipegraph is None` branch references
  undefined `ppg.DirConfig` (`NameError` if ever reached);
  `main_filter_constraint_violations` mutates the history dict before the
  "yes" confirmation (harmless only because saving is conditional).
- `runner.py:1078-1084`: the `except` around `event_job_success` has
  misleading indentation (the `log_error` call's body), and the
  constraint-violation logging path assumes `dir_config.error_dir` is not
  None — with error_dir disabled it raises `TypeError` inside the handler.
- `_convert_old_history` (`graph.py:574-614`) writes the new history file
  *before* validating job counts/names; a failed validation `sys.exit`s
  leaving both files, and the next start raises "Both new and old history
  existed".
- `_executing_thread`: if `event_now_running` or earlier steps raise after
  `job_id` is set but before `job = self.jobs[job_id]`, the `finally` uses a
  stale `job` from the previous loop iteration (wrong job gets
  `stop_time`/`run_time` scribbled on).
- `fail_counter`/`done_counter`/`jobs_all_cores_in_flight` are mutated with
  `+=` from multiple threads without a lock — display-only today, but
  they'll be wrong under contention.
- `AttributeLoadingJob.run` logs at WARNING level on every execution
  (`jobs.py:2385, 2413`) — log spam that trains users to ignore warnings.
- `engine.rs` carries large commented-out blocks (e.g. the ~65-line dead
  alternative in `event_job_finished_success`) and error messages like
  `"unexpected was 7"` — fine for the author, hostile to the next reader.
- `verify_order_was_topological` is O(edges × order²) — only used in tests,
  but trivially O(edges) with an index map.
- `Job.__init__` double-computes `self.outputs` (validated result is
  immediately overwritten by an equivalent expression, `jobs.py:245-249`).

## 7. Overall assessment

The core evaluation semantics are in the best shape of anything in the
project: a small typed state machine with exhaustive and fuzz testing, and
the recorded history of found-and-fixed deadlock families shows the
methodology works. The weaknesses cluster in three places: (1) the Python
runner's thread/fork/abort choreography, which contains the reproducible
bug (B1), the races (B2, B5), and the structurally unsafe abort (B12);
(2) the boundary itself — stringly-typed callbacks that keep the *real*
comparison semantics outside the tested core, plus vestigial features
(priorities, rename-pruning) that silently don't work; and (3) accepted-
but-undocumented tradeoffs (fork-under-threads, mtime granularity,
abort-discards-history) that deserve explicit decisions.

Priority order if correctness is paramount: fix B1 (user-visible failures
today), B2/B5 (stalls/hangs), replace async-exception abort (B12, likely
the root of the residual state-machine mysteries), then move history
comparison into the engine so the fuzzers test what production runs.
