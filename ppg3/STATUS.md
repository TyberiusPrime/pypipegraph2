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

## TODO
- (agents append here)
