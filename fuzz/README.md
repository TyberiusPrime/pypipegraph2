# ppg2 history fuzzer (cargo-afl)

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
* `upstream_failure_after_skip_unexpected_was_7` — **still open**: an
  upstream-failure wave reaches an Output that already `FinishedSkipped`
  (it validated against a pending-but-validated ephemeral); the skip is
  converted to `FinishedUpstreamFailure` and propagated into downstreams
  that already proceeded (`ReadyToRun`/`Running`/`FinishedSuccess`) ->
  `InternalError("unexpected was 7 ...")`. See
  `test_upstream_failure_after_skip_hits_proceeded_downstream` in
  `src/tests.rs` (ignored test).

Before the fix, a 4-minute AFL run (2.9M execs) found all three buckets
(149 crashes); after it, only the `unexpected was 7` bucket remains.
