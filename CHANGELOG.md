# Changelog

## Version 3.5.0

### New features

* ``ShellJob``: run a shell script (must start with a ``#!`` she-bang, or be
  provided as a callback that generates one per run) as a regular job.
  Exported from the top-level ``pypipegraph2`` namespace.

### Breaking Changes

* Local injection prevention: we use a vendored [localscope](https://pypi.org/project/localscope/)
  to verify you're not dragging in unexpected variables into your closures.
  
  To explicitly use an outside variable, bind it as a default parameter (`func(var=var)`).

  This prevents the common pitfall of defining a function in the loop,
  but finding it only bound the very first value of the loop variable for all jobs!.

* ``DataLoadingJob`` no longer content-hashes its returned objects
  (deepdiff's hashing of pandas objects was unreliable and produced spurious
  "changes"). The job's output hash is now derived from its declared
  *inputs* only. Consequence: changing the *content* of a returned
  DataFrame without changing any declared input does **not** re-run
  downstream jobs; changing a declared input does. Bare pandas objects can
  no longer be hashed at all (``TypeError``). 

  (This is not model breaking, as long as you do have the default FunctionInvariant
  on your DataLoadingJob.)

* Cython support has been removed. 

### Improvements

* Fuzzing infrastructure that discovered a few remaining logic bugs.
* Job runtimes are now appended to ``runtimes.tsv`` as each job finishes, 
  instead of a single batch write at the end of
  the run. Only jobs with a runtime of >= 1 s are recorded.

### Bug fixes

Several of these were found by the new fuzzers in the first fuzzing round:

* Fixed a crash in the Rust engine state machine (fuzzer-discovered).
* Fixed a second fuzzer-discovered engine bug: a skipped job that
  retroactively learns of an upstream failure (a validated ephemeral it was
  skipped "on the word of" ran anyway and failed/changed) is now flagged
  ``FinishedUpstreamFailure`` without spuriously propagating to downstreams;
  the next run re-converges. Fixes a class of deadlocks
  ("unexpected was 7" scenarios).
* Ephemeral jobs without a job-output history entry can no longer be treated
  as validated: after a failed/aborted run the edge keys alone prove nothing
  about the output, so such jobs are conservatively re-run.
* Fixed in-object caching and job-generation jobs (April 2026).
* Fixed a race in the process watcher: a bare ``ProcessLookupError`` from
  ``os.getpgid`` no longer crashes the watcher's signal handler mid-reap
  (which would leak the watcher).
* The run watcher is now always reaped (and force-killed if it survives the
  run); a leaked watcher targets the session-wide process group and would
  kill the *next* run's job subprocesses.
* Guard against state-machine double dispatch: if the evaluator reports a
  job as ready-to-run while it is already in flight, the evaluator state is
  dumped to ``ppg_double_dispatch_debug.txt`` and the job's ``pid`` is
  snapshotted locally so ``os.waitpid`` cannot crash on a concurrently
  nulled pid.
* Fixed a race between job creation and the file watcher (localscope
  memoization + watcher interaction).

### Minor Behavior changes
----------------

* Path normalization is now purely lexical (``os.path.normpath``); job
  creation no longer performs per-file ``realpath``/symlink resolution.
  Symlinked path components are therefore no longer canonicalized.
* The Rust engine now uses deterministic ``rustc-hash`` maps/sets (FxHashMap
  / FxHashSet) everywhere, including the graph hasher: output and job
  ordering are identical run-to-run (previously one hold-out was
  petgraph's toposort).

### Performance
-----------

- Substantially faster job/graph creation: ``_normalize_path`` is memoized
  (keyed by plain str), localscope bytecode disassembly is memoized per
  graph (only the disassembly is cached; validation against globals still
  runs on every call, so changed globals are always honoured),
  (Multi)FileGeneratingJob reuses the file validation computed in
  ``__new__`` instead of repeating it in ``__init__``, and several hot
  paths avoid throwaway ``Path`` constructions.

### Build, packaging & tooling

- Minimum Python version bumped (setup.cfg: 3.7.10 -> 3.8.12), and
  ``setup.cfg`` was then removed; the flake8 configuration moved to
  ``tox.ini`` (with adjusted lint rules).
- Nix flake upgraded: nixpkgs 26.05, Python 3.14 (test/dev environments now
  built straight from ``uv.lock`` via pyproject-nix/uv2nix, preferring prebuilt
  PyPI wheels), cargo-afl in the dev shell, flake made universal.
- CI reworked to be Nix-based (``nix flake check`` via the nix installer +
  magic nix cache); the Python version matrix moved into the flake.
- GitHub Actions bumped and pinned by commit SHA; pins maintained with
  pinact (``.pinact.yaml``).
- Dev/test Python dependencies are declared as PEP 735 dependency-groups,
  resolved and pinned via the committed ``uv.lock`` (reproducible CI);
  extra build dependencies (setuptools/maturin) declared for sdist-only
  packages.
- Newer Rust toolchain; ``rustc-hash`` added as a dependency.

#### Tests

* ~700 new lines of Rust engine tests, including regressions for all
  fuzzer-found crashes/deadlocks.
* Updated/added Python tests pinning the new behaviors (input-based
  DataFrame hashing, determinism, watcher/double-dispatch guards).


