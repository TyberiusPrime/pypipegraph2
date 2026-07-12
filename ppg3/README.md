# ppg3

Greenfield implementation of the PPG3_DESIGN.md constructive-trace build
system. See CONTRACT.md for layout/interfaces, STATUS.md for progress.

- core/: ppg3-core Rust crate (store, keys, scheduler, executors, views)
- cli/:  standalone `ppg3` binary
- py/:   PyO3 extension (ppg3._core)
- python/: Python front-end package
