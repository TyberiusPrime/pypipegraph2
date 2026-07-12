
## Coordinator: test-harness deadlock fix

`core/tests/scheduler.rs`'s MockExecutor had two edition-2021
guard-lifetime bugs: `if let Some(b) = self.state.lock()...cloned()` keeps
the MutexGuard alive to the end of the `if let` block, so (a) the mutex
was held across `Barrier::wait()` — deadlocking
`determinism_violation_surfaces` whenever both barrier parties needed the
lock — and (b) held across `thread::sleep` in the delay path, serializing
all mock executions. Fixed by binding the lookups in `let` statements
before the `if let`. All 14 oracle tests pass in 0.2s; the scheduler
itself needed no change. Final state: 166 Rust tests + 97 Python tests
green, clippy clean workspace-wide.
