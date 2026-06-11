# ppg2 fuzzers (cargo-afl)

Two targets:

* `fuzz_history` (`src/main.rs`, seeds in `seeds/`) — the original
  multi-run history fuzzer, strictly serial execution. Documented below.
* `fuzz_interleave` (`src/interleave.rs`, seeds in `seeds_interleave/`) —
  the advanced target: concurrent-execution interleavings plus semantic
  oracles. See [fuzz_interleave](#fuzz_interleave) at the end.

# fuzz_history

Fuzzes the rust `PPGEvaluator` state machine across **multiple consecutive
runs with carried-over history** — the "resumed project" situation in which
the `"Evaluator is not finished, reports no jobs ready to run, but no jobs
currently running"` bug (python/pypipegraph2/runner.py, ~line 797) shows up.

History is never synthesized: it is only ever produced by `new_history()` of
the previous simulated run, exactly like a real project. Between runs the
fuzzer may, per node:

* fail the job in this run,
* toggle the job's output (models "code changed" and "change reverted"),
* delete the job's output (models files removed on disk),
* remove the node from the graph for a run (models the user editing the
  graph between sessions; the last run always has the full graph),

and may abort a run after the first *k* jobs (models ctrl-c), optionally
failing the job that was running, mirroring what `runner.py` does on abort.

The harness panics (= AFL crash) on

* **deadlock**: evaluator not finished, nothing ready, nothing running —
  the exact condition runner.py aborts on,
* any `InternalError` / `APIError` from any event call,
* `new_history()` errors or panics.

`EphemeralChangedOutput` is tolerated (defined behavior).

## Input encoding

```
byte 0           node count        2 + (b % 6)            -> 2..=7
next n bytes     job kinds         b % 3                  (Always/Output/Ephemeral)
next n*(n-1)/2   edges             b & 1, pair (up, down) for up < down
next byte        run count         2 + (b % 3)            -> 2..=4
per run:
  n bytes        per-node flags    bit0 fail, bit1 toggle output,
                                   bit2 delete output, bit3 absent this run
  1 byte         abort             bit7 set -> abort after (b & 0x3f) jobs,
                                   bit6 -> fail the running job first
```

Missing bytes decode as 0 ("nothing special").

## Usage

The build needs a rustc with edition2024 support (>= 1.85; the afl 0.18
dependency tree requires it), and the cargo-afl wrapper:

```bash
cd fuzz
cargo-afl afl build --release
cargo-afl afl fuzz -i seeds -o out target/release/fuzz_history
```

Reproduce / triage a crash (binary reads stdin outside of AFL):

```bash
target/release/fuzz_history < out/default/crashes/id:...
RUST_LOG=debug target/release/fuzz_history < ...   # engine debug log
```

The panic message includes the decoded scenario and the engine's
`debug_()` dump (which prints ready-to-paste `add_node`/`depends_on` code
for a rust test case).

## known_crashes/

Inputs that reproduce known bugs (kept out of `seeds/` because AFL refuses
crashing seed corpora; fixed entries are kept as regression inputs).

* `ephemeral_validated_no_job_history` — **fixed 2026-06-10** (downgrade to
  Invalidated in `update_validation_status` when an ephemeral has no
  job-output history): an ephemeral that ran & failed in run 2 lost its
  `E`/`E!!!` history keys but kept its edge keys; on resume it was validated
  purely from edges and a downstream's `update_validation_status` died with
  `InternalError("Should have had history for it, if it was validated?!")`.
  See `test_ephemeral_failed_last_run_validated_by_edges_only` in
  `src/tests.rs`.
* `new_history_assert_skipped_eph_no_history` — **fixed by the same change**;
  same poisoned-history family, surfacing as the
  `assert!(job.state.is_failed() || _job_and_downstreams_are_ephemeral)`
  panic in `new_history()` (engine.rs ~796): a FinishedSkipped ephemeral with
  `history_output = None` whose downstreams are not all ephemeral.
* `upstream_failure_after_skip_unexpected_was_7` — **fixed 2026-06-10**
  (the upstream-failure wave now stops at a converted skipped job): an
  upstream-failure wave reached a job that already `FinishedSkipped`
  (it validated against a pending-but-validated ephemeral); the skip was
  converted to `FinishedUpstreamFailure` (deliberate, pinned by
  `test_ephemeral_retriggered_changing_output`) and then *propagated* into
  downstreams that already proceeded (`ReadyToRun`/`Running`/
  `FinishedSuccess`) -> `InternalError("unexpected was 7 ...")`. Since the
  skipped job never ran and its on-disk output is unchanged, its
  downstreams' premises still hold - the conversion is kept for reporting,
  the propagation is not. See
  `test_upstream_failure_after_skip_hits_proceeded_downstream` in
  `src/tests.rs`.

Before the fixes, a 4-minute AFL run (2.9M execs) found all three buckets
(149 crashes); with both fixes in, all 227 historical crash inputs pass and
fresh AFL runs find nothing.

### interleave_deadlock_* (fuzz_interleave, campaign 2026-06-12) — **fixed 2026-06-12**

A longer `fuzz_interleave` campaign (`out_interleave/`, ~250M execs/instance
across 30 instances) turned up 84 crashes — **all** the `DEADLOCK` oracle
(evaluator not finished, `query_ready_to_run()` empty, nothing running:
runner.py's "no way forward" abort). They are one root-cause family with a
few distinct surrounding states:

* an ephemeral's go/no-go decision is never made — it stays
  `Ephemeral(ReadyButDelayed)` while a `NotReady(Validated)` downstream waits
  on its decision and a terminal Output is stranded `NotReady(Unknown)`.

**Root cause / fix:** `set_upstream_edges` flipped an edge's `required`
weight to `Yes` (the input that `any_downstream_required` reads for a
`ReadyButDelayed` ephemeral) **without advancing the generation counter**, so
the follow-up `reconsider_ephemeral_upstreams` `ConsiderJob` for that
ephemeral was suppressed by `reconsider_job!`'s "already considered in this
gen" dedup guard — the ephemeral never re-evaluated and never ran. Fixed by
advancing the generation in `set_upstream_edges` whenever an edge weight
actually changes (engine.rs).

Six deduplicated, minimised representatives are archived here; each used to
deadlock and now completes via the binary
(`fuzz_interleave < known_crashes/interleave_…`):

* `interleave_deadlock_a_validated_eph_chain` — the bare core: a validated
  ephemeral chain stalls behind an undecided `ReadyButDelayed` head.
* `interleave_deadlock_b_skipped_eph` — same, with an upstream ephemeral
  already `FinishedSkipped`.
* `interleave_deadlock_c_cleanup_pending` — a sibling ephemeral parked in
  `FinishedSuccessNotReadyForCleanup` whose cleanup never unblocks.
* `interleave_deadlock_d_skipped_output_multi` — a downstream Output already
  `FinishedSkipped`, terminal Output is a multi-output `:::` job.
* `interleave_deadlock_e_cleanup_multi` — multi-output validated chain.
* `interleave_deadlock_f_skipped_output_multi` — multi-output skipped Output
  plus a stranded `NotReady(Unknown)` Output.

In-tree regression tests (direct engine API, no AFL needed) live in
`src/tests.rs` as `test_interleave_deadlock_{a,b,c,d}_*`. They replay the
captured multi-run history setup and drive each run to completion via
`drive_run_to_completion`, which asserts the deadlock does not recur (it fires
on the pre-fix engine, passes on the fixed one). `PPG2_FUZZ_TRACE=1
fuzz_interleave < <input>` emits the captured engine call sequence as
copy-pasteable Rust for turning any further crash into a test.

# fuzz_interleave

The advanced target. Where `fuzz_history` executes strictly serially
(start job → finish job, one at a time), `fuzz_interleave` mirrors what
`runner.py` actually does: a thread pool keeps **several jobs in `Running`
simultaneously**, they finish in arbitrary order, and cleanups happen at
arbitrary points in between. The fuzz input drives that schedule explicitly
via an action stream.

Additional scenario dimensions over `fuzz_history`:

* per-run **job kind changes** (Output ↔ Ephemeral ↔ Always between
  sessions — legal, each run is a fresh evaluator with carried history),
* per-run **edge changes** (user changed dependencies; xor mask against the
  base edge set),
* **multi-output job names** (`a:::b:::c`) whose part composition reshuffles
  between runs, driving the rename-matching machinery in `new_history()`
  (`filter_if_renamed`, `try_finding_renamed_multi_output_job`),
* mid-run **`reconsider_all_jobs()`** calls (exposed to python, capped at 8
  per run),
* **abort with multiple jobs running** (all running jobs are mock-failed,
  then `abort_remaining()` — exactly runner.py's ctrl-c sequence),
* cleanup **removes the ephemeral's output from "disk"** (`fuzz_history`
  leaves it present; both situations are reachable in reality, the abort
  path here still covers the lingering-output case).

On top of the crash/deadlock oracles of `fuzz_history` it checks **semantic
oracles**:

* `next_job_ready_to_run()` must agree with `query_ready_to_run()`,
* the order jobs were started in must be topological
  (`verify_order_was_topological`),
* **missing output**: in a completed run without any failure, every present
  Output job whose output was missing on disk at run start must have run,
* **convergence**: an unchanged re-run after a clean, fully successful run
  must not run any Output job, and every Ephemeral it does run must have a
  direct downstream that also ran.

The last two catch spurious-rerun / missed-rerun logic bugs — a bug class
that never deadlocks and is invisible to `fuzz_history`.

## Input encoding

```
byte 0           node count        2 + (b % 6)            -> 2..=7
next n bytes     node spec         kind = b % 3 (Always/Output/Ephemeral),
                                   multi-output names if b & 8
next n*(n-1)/2   base edges        b & 1, pair (up, down) for up < down
next byte        run count         2 + (b % 3)            -> 2..=4
per run:
  n bytes        per-node flags    bit0 fail, bit1 toggle output,
                                   bit2 delete output, bit3 absent this run,
                                   bits4-5 kind override (0 keep, 1 Always,
                                   2 Output, 3 Ephemeral),
                                   bits6-7 name variant (multi nodes only)
  ceil(pairs/8)  edge xor mask     flips base edges for this run
rest             action stream, consumed across all runs as needed:
                   op = b >> 4, sel = b & 0x0f
                   0..=5   start sorted_ready[sel % len]
                   6..=11  finish running[sel % len] (fail per node flag)
                   12..=13 cleanup sorted_pending[sel % len]
                   14      reconsider_all_jobs()
                   15      sel == 15: abort, else finish
```

Missing bytes decode as 0; an exhausted action stream degrades to
start-everything-then-finish-in-order, so every input terminates. Each
action falls back along start → finish → deadlock-check when its chosen
operation isn't possible, so the harness can never stall without proving
the deadlock.

## Usage

```bash
cd fuzz
cargo-afl afl build --release
cargo-afl afl fuzz -i seeds_interleave -o out_interleave target/release/fuzz_interleave
```

Triage is the same as for `fuzz_history` (binary reads stdin outside of
AFL, `PPG2_FUZZ_DUMP=1` dumps the decoded scenario, panic messages include
the engine's `debug_()` dump).
