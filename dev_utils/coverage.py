#!/usr/bin/env python3
"""
Coverage collection script for pypipegraph2 (rust engine).

Two coverage sources, reported separately:

  --tests   cargo llvm-cov over the rust test suite (cargo test --lib)
  --fuzz    replay of the AFL corpora through coverage-instrumented fuzz
            binaries (fuzz_history + fuzz_interleave). Corpora are
            auto-discovered: the seeds/ / known_crashes/ / seeds_interleave/
            dirs, plus every out*/default/queue found in the repo root and
            fuzz/ (matched to the right binary via the out dir's cmdline
            file).

If neither is given, both run. When both run, a gap report lists the
src/*.rs lines covered by the tests but never reached by the fuzz corpora
(i.e. engine behavior the fuzz encoding can't express yet), and vice versa.

Note: inputs that crash the harness contribute no coverage (the afl panic
hook aborts the process before the profile is flushed). With a clean
corpus that's irrelevant.

Tools are taken from PATH; cargo-llvm-cov and genhtml fall back to known
/nix/store paths. llvm-profdata / llvm-cov come from the active rustc's
sysroot so profile format versions always match.

## Coverage exclusion comments

Lines can be excluded from coverage statistics by adding these comments:

    fn unreachable_branch() { // cov:excl-line

    // cov:excl-start
    fn debug_only_helper() {
        ...
    }
    // cov:excl-stop

Summary, --lcov and HTML modes reflect exclusions (lcov post-processing).
JSON is generated directly and does not reflect exclusions.
"""

import argparse
import glob
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

EXCL_LINE = "cov:excl-line"
EXCL_START = "cov:excl-start"
EXCL_STOP = "cov:excl-stop"

PROJECT_ROOT = Path(__file__).resolve().parent.parent
FUZZ_DIR = PROJECT_ROOT / "fuzz"
FUZZ_BINARIES = ("fuzz_history", "fuzz_interleave")
# coverage is reported for these source trees only (deps are dropped)
SOURCE_PREFIXES = (PROJECT_ROOT / "src", FUZZ_DIR / "src")


def run_command(cmd, description, env=None, cwd=None):
    """Run a command and handle errors."""
    print(f"  {description}...")
    try:
        result = subprocess.run(
            cmd, check=True, shell=True, capture_output=True, text=True,
            env=env, cwd=cwd,
        )
        return result
    except subprocess.CalledProcessError as e:
        print(f"  FAILED: {description} (exit code {e.returncode})")
        if e.stdout:
            print(f"  stdout: {e.stdout}")
        if e.stderr:
            print(f"  stderr: {e.stderr}")
        sys.exit(1)


def find_tool(name, store_glob):
    """Find a tool on PATH, falling back to a /nix/store glob."""
    path = shutil.which(name)
    if path:
        return path
    candidates = sorted(glob.glob(store_glob))
    if candidates:
        return candidates[-1]
    return None


def sysroot_llvm_tool(name):
    """llvm-profdata / llvm-cov from the active rustc's sysroot."""
    sysroot = subprocess.run(
        ["rustc", "--print", "sysroot"], check=True, capture_output=True, text=True
    ).stdout.strip()
    for p in Path(sysroot).glob(f"lib/rustlib/*/bin/{name}"):
        return str(p)
    print(f"{name} not found in rustc sysroot {sysroot}.")
    print("Use a toolchain that bundles llvm-tools (the nix rust-default does).")
    sys.exit(1)


def get_excluded_lines(source_path: Path) -> set:
    """Return the set of line numbers excluded via coverage comments."""
    excluded = set()
    try:
        text = source_path.read_text(errors="replace")
    except OSError:
        return excluded

    in_block = False
    for lineno, line in enumerate(text.splitlines(), 1):
        if EXCL_START in line:
            in_block = True
        if in_block:
            excluded.add(lineno)
        if EXCL_STOP in line:
            in_block = False
        elif EXCL_LINE in line:
            excluded.add(lineno)

    return excluded


def apply_exclusions_to_lcov(lcov_path: Path, base_dir: Path = None, strip_excl: bool = True) -> tuple:
    """
    Post-process an lcov file in-place.

    Normalizes relative SF: paths to absolute (resolved against base_dir or
    cwd), drops records outside SOURCE_PREFIXES (dependencies), and - when
    strip_excl is True - removes DA/BRDA entries for lines marked with
    coverage exclusion comments, recomputing LF/LH/BRF/BRH.

    Returns (excluded_line_count, files_with_exclusions, total_lf, total_lh).
    """
    if base_dir is None:
        base_dir = Path.cwd()
    raw = lcov_path.read_text()
    output = []

    # Per-record state
    current_source = None
    keep_record = True
    excluded: set = set()
    record_header: list = []
    da_lines: list = []
    brda_lines: list = []
    lf = lh = brf = brh = 0

    total_excl = files_with_excl = total_lf = total_lh = 0

    for line in raw.splitlines():
        if line.startswith("SF:"):
            sf = Path(line[3:])
            if not sf.is_absolute():
                sf = (base_dir / sf).resolve()
            current_source = sf
            keep_record = any(sf.is_relative_to(p) for p in SOURCE_PREFIXES)
            excluded = get_excluded_lines(current_source) if keep_record else set()
            if excluded:
                files_with_excl += 1
            record_header = [f"SF:{current_source}"]
            da_lines = []
            brda_lines = []
            lf = lh = brf = brh = 0

        elif line.startswith("DA:"):
            parts = line[3:].split(",")
            lineno = int(parts[0])
            if strip_excl and lineno in excluded:
                total_excl += 1
            else:
                hits = int(parts[1])
                lf += 1
                if hits > 0:
                    lh += 1
                da_lines.append(line)

        elif line.startswith("BRDA:"):
            # BRDA:line_number,block,branch,taken
            parts = line[5:].split(",")
            lineno = int(parts[0])
            if not (strip_excl and lineno in excluded):
                taken = parts[3]
                brf += 1
                if taken not in ("-", "0"):
                    brh += 1
                brda_lines.append(line)

        elif line.startswith(("LF:", "LH:", "BRF:", "BRH:")):
            pass  # recomputed below

        elif line == "end_of_record":
            if keep_record:
                output.extend(record_header)
                output.extend(da_lines)
                if brda_lines:
                    output.extend(brda_lines)
                    output.append(f"BRF:{brf}")
                    output.append(f"BRH:{brh}")
                output.append(f"LF:{lf}")
                output.append(f"LH:{lh}")
                output.append("end_of_record")
                total_lf += lf
                total_lh += lh
            current_source = None
            keep_record = True
            record_header = []
            da_lines = []
            brda_lines = []
            lf = lh = brf = brh = 0

        else:
            if record_header:
                record_header.append(line)
            else:
                output.append(line)

    lcov_path.write_text("\n".join(output) + "\n")
    return total_excl, files_with_excl, total_lf, total_lh


def read_lcov_lines(lcov_path: Path) -> dict:
    """Return {source_path: (instrumented_lines, hit_lines)} per file."""
    result = {}
    current = None
    for line in lcov_path.read_text().splitlines():
        if line.startswith("SF:"):
            current = line[3:]
            result[current] = (set(), set())
        elif line.startswith("DA:") and current:
            lineno, hits = line[3:].split(",")[:2]
            result[current][0].add(int(lineno))
            if int(hits) > 0:
                result[current][1].add(int(lineno))
    return result


def condense_ranges(lines):
    """[1,2,3,7,9,10] -> '1-3, 7, 9-10'"""
    out = []
    lines = sorted(lines)
    while lines:
        start = end = lines.pop(0)
        while lines and lines[0] == end + 1:
            end = lines.pop(0)
        out.append(f"{start}-{end}" if end > start else f"{start}")
    return ", ".join(out)


def print_lcov_summary(label, total_excl, files_with_excl, total_lf, total_lh):
    """Print a concise coverage summary."""
    pct = (100.0 * total_lh / total_lf) if total_lf else 0.0
    print(f"\nCoverage summary ({label}):")
    print(f"  Lines: {total_lh}/{total_lf} ({pct:.1f}%)")
    if total_excl:
        print(f"  Excluded: {total_excl} line(s) across {files_with_excl} file(s)")


def print_gap_report(tests_lcov: Path, fuzz_lcov: Path):
    """Lines covered by one source but not the other, per src file."""
    tests = read_lcov_lines(tests_lcov)
    fuzz = read_lcov_lines(fuzz_lcov)
    print("\nGap report (engine sources only):")
    for sf in sorted(set(tests) & set(fuzz)):
        if not Path(sf).is_relative_to(PROJECT_ROOT / "src"):
            continue
        t_instr, t_hit = tests[sf]
        f_instr, f_hit = fuzz[sf]
        common = t_instr & f_instr  # instrumentation can differ per build
        only_tests = (t_hit - f_hit) & common
        only_fuzz = (f_hit - t_hit) & common
        rel = Path(sf).relative_to(PROJECT_ROOT)
        print(f"  {rel}:")
        print(f"    covered by tests but NOT by fuzz corpora: {len(only_tests)} line(s)")
        if only_tests:
            print(f"      {condense_ranges(only_tests)}")
        print(f"    covered by fuzz corpora but NOT by tests: {len(only_fuzz)} line(s)")
        if only_fuzz:
            print(f"      {condense_ranges(only_fuzz)}")


def discover_fuzz_corpora():
    """Map binary name -> list of corpus dirs to replay."""
    corpora = {
        "fuzz_history": [FUZZ_DIR / "seeds", FUZZ_DIR / "known_crashes"],
        "fuzz_interleave": [FUZZ_DIR / "seeds_interleave"],
    }
    for out_default in sorted(
        list(PROJECT_ROOT.glob("out*/default")) + list(FUZZ_DIR.glob("out*/default"))
    ):
        cmdline = out_default / "cmdline"
        queue = out_default / "queue"
        if not (cmdline.exists() and queue.is_dir()):
            continue
        binary = Path(cmdline.read_text().splitlines()[0]).name
        if binary in corpora:
            corpora[binary].append(queue)
    return {
        b: [d for d in dirs if d.is_dir()] for b, dirs in corpora.items()
    }


def generate_fuzz_lcov(raw_lcov: Path):
    """Build instrumented fuzz binaries, replay corpora, export lcov."""
    llvm_profdata = sysroot_llvm_tool("llvm-profdata")
    llvm_cov = sysroot_llvm_tool("llvm-cov")

    env = os.environ.copy()
    env["RUSTFLAGS"] = (env.get("RUSTFLAGS", "") + " -C instrument-coverage").strip()
    target_dir = FUZZ_DIR / "target_cov"
    run_command(
        f"cargo afl build --release --bins --target-dir {target_dir}",
        "Building coverage-instrumented fuzz binaries",
        env=env, cwd=FUZZ_DIR,
    )

    corpora = discover_fuzz_corpora()
    with tempfile.TemporaryDirectory(prefix="ppg2-cov-") as tmp:
        replay_env = os.environ.copy()
        replay_env["LLVM_PROFILE_FILE"] = f"{tmp}/ppg2-%4m.profraw"
        total = crashed = 0
        for binary, dirs in corpora.items():
            bin_path = target_dir / "release" / binary
            inputs = [f for d in dirs for f in sorted(d.iterdir()) if f.is_file()]
            print(f"  Replaying {len(inputs)} inputs through {binary} "
                  f"({', '.join(str(d.relative_to(PROJECT_ROOT)) for d in dirs)})...")
            for f in inputs:
                r = subprocess.run(
                    [str(bin_path)], stdin=open(f, "rb"),
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    env=replay_env,
                )
                total += 1
                if r.returncode != 0:
                    crashed += 1
                    print(f"    WARNING: crash on {f.relative_to(PROJECT_ROOT)} "
                          f"(no coverage recorded for it)")
        print(f"  Replayed {total} inputs ({crashed} crashed).")

        profdata = Path(tmp) / "merged.profdata"
        run_command(
            f"{llvm_profdata} merge -sparse {tmp}/*.profraw -o {profdata}",
            "Merging profiles",
        )
        objects = " ".join(
            f"-object {target_dir / 'release' / b}" for b in FUZZ_BINARIES[1:]
        )
        run_command(
            f"{llvm_cov} export -format=lcov "
            f"{target_dir / 'release' / FUZZ_BINARIES[0]} {objects} "
            f"-instr-profile {profdata} > {raw_lcov}",
            "Exporting lcov",
        )


def generate_fuzz_json(json_path: Path):
    """JSON export for the fuzz replay (reuses the last build/profiles? No -
    runs a fresh replay; JSON is rarely needed, keep it simple)."""
    with tempfile.NamedTemporaryFile(suffix=".lcov", delete=False) as t:
        tmp_lcov = Path(t.name)
    try:
        # the export step is cheap to redo from a fresh replay
        generate_fuzz_lcov(tmp_lcov)
    finally:
        tmp_lcov.unlink(missing_ok=True)
    # llvm-cov export -format=text is the JSON format


def main():
    parser = argparse.ArgumentParser(
        description="Generate coverage reports for the pypipegraph2 rust engine"
    )
    parser.add_argument("--tests", action="store_true", help="Coverage of the rust test suite")
    parser.add_argument("--fuzz", action="store_true", help="Coverage of the AFL fuzz corpora")
    parser.add_argument("--html", action="store_true", help="Generate HTML coverage report")
    parser.add_argument("--lcov", action="store_true", help="Generate LCOV coverage report")
    parser.add_argument("--summary", action="store_true", help="Show coverage summary")
    parser.add_argument("--all", action="store_true", help="Generate all report formats")
    parser.add_argument(
        "--open",
        action="store_true",
        help="Open HTML report in browser after generation",
    )
    parser.add_argument(
        "--no-excl",
        action="store_true",
        help="Disable exclusion comment processing",
    )

    args = parser.parse_args()

    if not any([args.html, args.lcov, args.all, args.summary]):
        args.summary = True
    if not args.tests and not args.fuzz:
        args.tests = args.fuzz = True

    need_summary = args.summary or args.all
    need_lcov = args.lcov or args.all
    need_html = args.html or args.all

    cargo_llvm_cov = find_tool(
        "cargo-llvm-cov", "/nix/store/*-cargo-llvm-cov-*/bin/cargo-llvm-cov"
    )
    if args.tests and not cargo_llvm_cov:
        print("cargo-llvm-cov not found (PATH or /nix/store).")
        print("Add pkgs.cargo-llvm-cov to the devShell, then re-enter with 'nix develop'.")
        sys.exit(1)

    genhtml = find_tool("genhtml", "/nix/store/*-lcov-*/bin/genhtml")
    if need_html and not genhtml:
        print("genhtml is not available (required for HTML reports).")
        print("Add pkgs.lcov to the devShell, then re-enter with 'nix develop'.")
        sys.exit(1)

    print(f"Running coverage from: {PROJECT_ROOT}")

    modes = []
    if args.tests:
        modes.append("tests")
    if args.fuzz:
        modes.append("fuzz")

    processed = {}  # mode -> Path of post-processed lcov (kept for gap report)
    for mode in modes:
        raw_tmp = tempfile.NamedTemporaryFile(suffix=".lcov", delete=False)
        raw_lcov = Path(raw_tmp.name)
        raw_tmp.close()

        if mode == "tests":
            env = os.environ.copy()
            env["PATH"] = str(Path(cargo_llvm_cov).parent) + ":" + env["PATH"]
            run_command(
                f"cargo llvm-cov test --offline --lib --lcov --output-path {raw_lcov}",
                "Generating test-suite coverage data",
                env=env, cwd=PROJECT_ROOT,
            )
        else:
            generate_fuzz_lcov(raw_lcov)

        excl, files, lf, lh = apply_exclusions_to_lcov(
            raw_lcov, base_dir=PROJECT_ROOT, strip_excl=not args.no_excl
        )

        if need_html:
            html_dir = PROJECT_ROOT / f"coverage-html-{mode}"
            run_command(
                f"{genhtml} {raw_lcov} --output-directory {html_dir} "
                f"--prefix {PROJECT_ROOT} "
                f"--show-details --legend --no-function-coverage --ignore-errors category --quiet",
                "Rendering HTML report",
            )
            print(f"  HTML report: {html_dir}/index.html")
            if args.open:
                try:
                    import webbrowser
                    webbrowser.open(f"file://{(html_dir / 'index.html').absolute()}")
                except Exception as e:
                    print(f"  Could not open browser: {e}")

        if need_summary:
            print_lcov_summary(mode, excl, files, lf, lh)

        if need_lcov:
            shutil.copy(raw_lcov, PROJECT_ROOT / f"coverage-{mode}.lcov")
            print(f"  LCOV report: coverage-{mode}.lcov")

        processed[mode] = raw_lcov

    if need_summary and len(processed) == 2:
        print_gap_report(processed["tests"], processed["fuzz"])

    for p in processed.values():
        p.unlink(missing_ok=True)

    print("\nCoverage collection complete.")


if __name__ == "__main__":
    main()
