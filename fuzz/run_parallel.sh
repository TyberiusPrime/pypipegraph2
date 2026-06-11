#!/usr/bin/env bash
#
# run_parallel.sh — drive the `fuzz_interleave` cargo-afl target across many
# cores for an unattended (overnight) run.
#
# AFL parallel model: one "main" instance (-M, does deterministic + havoc) plus
# N-1 "secondary" instances (-S, havoc/random), all sharing a single output dir
# so they cross-pollinate their corpora. afl-whatsup aggregates their stats.
#
# Quick start (from anywhere):
#     fuzz/run_parallel.sh
#
# It builds the target, launches JOBS instances, and blocks until they finish
# (DURATION seconds) or you Ctrl-C — at which point all instances are killed.
# To survive logout, run it under tmux/screen or with nohup:
#     nohup fuzz/run_parallel.sh > fuzz/out_interleave/run.log 2>&1 &
#
# Tunables (env vars, all optional):
#     JOBS       number of parallel instances        (default: nproc - 2)
#     DURATION   seconds to fuzz, then self-stop      (default: 43200 = 12h)
#                set DURATION=0 to run until Ctrl-C
#     OUT        AFL output dir (under fuzz/)         (default: out_interleave)
#     SEEDS      input corpus dir                     (default: seeds_interleave)
#     SKIP_BUILD set =1 to skip the cargo-afl build   (default: build first)
#     RUST_BIN   dir of an edition2024 rustc          (default: nix 1.95 store path)
#     CARGO_AFL  path to the cargo-afl wrapper        (default: nix store path)
#
set -euo pipefail

# --- locate ourselves / the fuzz crate -------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# --- toolchain (overridable) -----------------------------------------------
# The afl 0.18 dep tree needs edition2024 (rustc >= 1.85); the default shell
# rust in this repo's flake is 1.78, so we prepend a newer one. cargo-afl must
# match the `afl = "=0.18.1"` pin in Cargo.toml.
RUST_BIN="${RUST_BIN:-/nix/store/0s4hchhcmppcw7qn6jlzzf46bh4cw734-rust-default-1.95.0/bin}"
CARGO_AFL="${CARGO_AFL:-/nix/store/7bbgfnflvi7i2fgvw5dv10bvbjqzrkdi-cargo-afl-wrapped/bin/cargo-afl}"

if [[ -d "$RUST_BIN" ]]; then
    export PATH="$RUST_BIN:$PATH"
fi
if [[ ! -x "$CARGO_AFL" ]]; then
    # fall back to whatever's on PATH
    CARGO_AFL="$(command -v cargo-afl || true)"
fi
if [[ -z "$CARGO_AFL" ]]; then
    echo "error: cargo-afl not found (set CARGO_AFL=/path/to/cargo-afl)" >&2
    exit 1
fi

# cargo respects this for the build *and* afl uses it to find the binary.
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-target_claude}"
BIN="$CARGO_TARGET_DIR/release/fuzz_interleave"

# --- AFL runtime environment -----------------------------------------------
# Skip the CPU-governor "performance" nag — overnight, unattended, usually no
# root to flip the governor. Throughput cost is small.
export AFL_SKIP_CPUFREQ=1
# Re-running the script resumes an existing OUT dir instead of erroring out.
export AFL_AUTORESUME=1
# Stop the secondaries from whining on stdout; keep the main one chatty.
export AFL_NO_UI="${AFL_NO_UI:-1}"

# Only suppress the missing-crashes abort if core_pattern actually pipes to a
# crash handler (then aborts/segfaults would be swallowed). Here it's normally
# "core", so we leave AFL's safety check on and capture every crash.
if [[ "$(cat /proc/sys/kernel/core_pattern 2>/dev/null || echo core)" == "|"* ]]; then
    echo "note: core_pattern pipes to a handler; setting AFL_I_DONT_CARE_ABOUT_MISSING_CRASHES=1" >&2
    echo "      (crashes may be swallowed by the host crash reporter — fix core_pattern for full fidelity)" >&2
    export AFL_I_DONT_CARE_ABOUT_MISSING_CRASHES=1
fi

# --- parameters -------------------------------------------------------------
CORES="$(nproc)"
JOBS="${JOBS:-$(( CORES - 2 ))}"
(( JOBS < 1 )) && JOBS=1
DURATION="${DURATION:-43200}"
OUT="${OUT:-out_interleave}"
SEEDS="${SEEDS:-seeds_interleave}"

if [[ ! -d "$SEEDS" ]]; then
    echo "error: seed dir '$SEEDS' not found (run from the repo so fuzz/ exists)" >&2
    exit 1
fi

# --- build ------------------------------------------------------------------
if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
    echo ">> building fuzz_interleave (cargo-afl afl build --release) ..."
    "$CARGO_AFL" afl build --release --bin fuzz_interleave
fi
if [[ ! -x "$BIN" ]]; then
    echo "error: built binary not found at $BIN" >&2
    echo "       (is CARGO_TARGET_DIR right? try SKIP_BUILD=0)" >&2
    exit 1
fi

mkdir -p "$OUT/logs"

# -V <secs>: each instance fuzzes for that wall-clock time then exits cleanly.
VFLAG=()
if (( DURATION > 0 )); then
    VFLAG=(-V "$DURATION")
fi

echo "======================================================================"
echo " fuzz_interleave — parallel run"
echo "   cores available : $CORES"
echo "   instances (JOBS): $JOBS   (1 main + $((JOBS-1)) secondary)"
if (( DURATION > 0 )); then
    echo "   duration        : ${DURATION}s (~$(awk "BEGIN{printf \"%.1f\", $DURATION/3600}")h), self-stops"
else
    echo "   duration        : until Ctrl-C"
fi
echo "   output dir      : fuzz/$OUT/   (logs in $OUT/logs/)"
echo "   monitor with    : $CARGO_AFL afl whatsup -s $OUT"
echo "======================================================================"

# --- launch -----------------------------------------------------------------
PIDS=()

cleanup() {
    echo
    echo ">> stopping ${#PIDS[@]} fuzzer instance(s) ..."
    # SIGINT lets AFL flush its corpus/stats before exiting.
    for pid in "${PIDS[@]}"; do
        kill -INT "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    echo ">> all instances stopped."
    "$CARGO_AFL" afl whatsup -s "$OUT" 2>/dev/null || true
    exit 130
}
trap cleanup INT TERM

launch() {
    local id="$1"; shift
    local log="$OUT/logs/$id.log"
    # AFL_NO_UI on secondaries keeps their logs to plain status lines.
    "$CARGO_AFL" afl fuzz "${VFLAG[@]}" -m none \
        -i "$SEEDS" -o "$OUT" "$@" -- "$BIN" \
        > "$log" 2>&1 &
    PIDS+=("$!")
    echo "   started $id (pid $!) -> $log"
}

# Main instance first; give it a moment to initialise the shared bitmap.
echo ">> launching instances ..."
AFL_NO_UI=0 launch main -M main
sleep 2

for (( i = 1; i < JOBS; i++ )); do
    id="$(printf 's%02d' "$i")"
    launch "$id" -S "$id"
done

echo
echo ">> ${#PIDS[@]} instance(s) running. Tail the main one with:"
echo "     tail -f fuzz/$OUT/logs/main.log"
echo ">> waiting for completion (Ctrl-C to stop early) ..."

# Block until every instance exits (DURATION reached) or we're interrupted.
# `|| true` so a non-zero AFL exit doesn't trip set -e before the summary.
wait || true
trap - INT TERM

echo
echo ">> run complete."
"$CARGO_AFL" afl whatsup -s "$OUT" 2>/dev/null || true

# Surface any crashes/hangs found.
CRASHES=$(find "$OUT" -path '*/crashes/id:*' 2>/dev/null | wc -l)
HANGS=$(find "$OUT" -path '*/hangs/id:*' 2>/dev/null | wc -l)
echo
echo ">> crashes found: $CRASHES   hangs found: $HANGS"
if (( CRASHES > 0 )); then
    echo "   triage a crash with:"
    echo "     $BIN < <crash-file>"
    echo "     PPG2_FUZZ_DUMP=1 RUST_LOG=debug $BIN < <crash-file>"
fi
