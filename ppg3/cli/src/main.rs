//! Standalone `ppg3` binary (WP5, CONTRACT.md "CLI").
//!
//! Exit codes (CONTRACT.md): `0` ok, `1` operational failure (verify
//! mismatch, determinism violation, missing generation, ...), `2` usage
//! error. Usage errors from malformed arguments are handled by `clap`
//! itself (it calls `process::exit(2)`); this binary only needs to map its
//! own `AppError` to `1` (operational) or `2` (the few usage checks clap's
//! declarative parser can't express, e.g. `.ppg3` not found via `--project`
//! is treated as operational, not usage, since the *syntax* was fine).

mod config;

use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use serde::Serialize;

use ppg3_core::explain::{self, Explanation};
use ppg3_core::gc::GcPolicy;
use ppg3_core::store::Store;
use ppg3_core::views;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("{0}")]
    Usage(String),
    #[error("{0}")]
    Operational(String),
    #[error("{0}")]
    Core(String),
    #[error("io error: {0}")]
    Io(String),
}

impl From<ppg3_core::Error> for AppError {
    fn from(e: ppg3_core::Error) -> Self {
        AppError::Core(e.to_string())
    }
}

impl From<std::io::Error> for AppError {
    fn from(e: std::io::Error) -> Self {
        AppError::Io(e.to_string())
    }
}

impl From<serde_json::Error> for AppError {
    fn from(e: serde_json::Error) -> Self {
        AppError::Operational(e.to_string())
    }
}

#[derive(Parser)]
#[command(name = "ppg3", version, about = "ppg3 store/views maintenance CLI")]
struct Cli {
    /// Machine-readable output (serde_json to stdout) instead of human text.
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Store-level maintenance (works on a raw store path, no project).
    Store {
        #[command(subcommand)]
        cmd: StoreCmd,
    },
    /// Per-project generation lifecycle.
    Generations {
        #[command(subcommand)]
        cmd: GenerationsCmd,
    },
    /// Project-level GC, split into two explicitly separate phases:
    /// 1. remove old generations (per-bucket budgets: properly *committed*
    ///    generations vs *op-log*/ephemeral ones — see `generations list`'s
    ///    VCS column), which unregisters their store roots;
    /// 2. mark/sweep every writable store in `.ppg3/config.json` so the
    ///    entries those roots kept alive are actually reclaimed.
    Gc {
        /// How many old committed (or VCS-less) generations to keep, in
        /// addition to the current one.
        #[arg(long, default_value_t = 10)]
        keep: u64,
        /// How many old op-log/ephemeral generations to keep (dirty jj
        /// working copy at run time, or watch-mode ephemeral) — their
        /// source state is not durably committed, so the default is
        /// deliberately smaller.
        #[arg(long, default_value_t = 2)]
        keep_oplog: u64,
        /// Store sweep budget; without it phase 2 still removes
        /// evict-marked entries and dangling input links.
        #[arg(long)]
        max_size: Option<u64>,
        /// Allow phase 2 to evict `logs/` under budget pressure.
        #[arg(long)]
        evict_logs: bool,
        /// Report both phases without deleting anything.
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        project: Option<PathBuf>,
    },
    /// Repoint `views/current` at an earlier (or explicit) generation.
    Rollback {
        /// Generation to roll back to; default is the previous existing
        /// generation before the current one.
        generation: Option<u64>,
        #[arg(long)]
        project: Option<PathBuf>,
    },
    /// Explain why a view path's content changed vs. the previous generation.
    Explain {
        /// View-relative path, e.g. `results/counts.tsv` (an `outputs/`
        /// prefix, if present, is stripped automatically).
        view_path: String,
        #[arg(long)]
        project: Option<PathBuf>,
    },
    /// Per-file diff between two store entries.
    DiffEntries {
        oh1: String,
        oh2: String,
        #[arg(long)]
        store: PathBuf,
    },
}

#[derive(Subcommand)]
enum StoreCmd {
    /// Mark/sweep GC on a single store.
    Gc {
        #[arg(long)]
        max_size: Option<u64>,
        #[arg(long)]
        store: PathBuf,
        #[arg(long)]
        dry_run: bool,
        /// Additive beyond the WP5 brief's flag list (documented in
        /// STATUS.md): allow GC to evict `logs/` under budget pressure,
        /// matching `GcPolicy.evict_logs` (opt-in per WP1's gc.rs module
        /// doc — logs are valuable debugging data, not swept by default).
        #[arg(long)]
        evict_logs: bool,
    },
    /// Rehash entries and compare against their manifest.
    Verify {
        /// Deterministic-random subset, as a percentage (e.g. `5` or `5%`).
        /// Selection is a hash-threshold sample (entries whose `oh` falls
        /// below the percentage threshold) so the same store state always
        /// yields the same sample, without needing a stored RNG seed.
        #[arg(long, value_parser = parse_percent, conflicts_with = "entry")]
        sample: Option<f64>,
        /// Verify exactly one entry by output hash.
        #[arg(long, conflicts_with = "sample")]
        entry: Option<String>,
        #[arg(long)]
        store: PathBuf,
    },
}

#[derive(Subcommand)]
enum GenerationsCmd {
    List {
        #[arg(long)]
        project: Option<PathBuf>,
    },
    Rm {
        generation: u64,
        #[arg(long)]
        project: Option<PathBuf>,
    },
    Keep {
        n: u64,
        /// Design §6.7: budget `n` applies only to ephemeral generations;
        /// explicit generations are always kept.
        #[arg(long)]
        keep_explicit: bool,
        #[arg(long)]
        project: Option<PathBuf>,
    },
}

fn parse_percent(s: &str) -> Result<f64, String> {
    let trimmed = s.trim().trim_end_matches('%');
    let v: f64 = trimmed
        .parse()
        .map_err(|e| format!("invalid percentage {s:?}: {e}"))?;
    if !(0.0..=100.0).contains(&v) {
        return Err(format!("percentage must be within 0..=100, got {v}"));
    }
    Ok(v)
}

fn main() {
    let cli = Cli::parse();
    let json = cli.json;
    match run(cli) {
        Ok(code) => std::process::exit(code),
        Err(e) => {
            eprintln!("error: {e}");
            let code = match e {
                AppError::Usage(_) => 2,
                _ => 1,
            };
            let _ = json; // already printed to stderr above regardless of --json
            std::process::exit(code);
        }
    }
}

fn run(cli: Cli) -> Result<i32, AppError> {
    let json = cli.json;
    match cli.command {
        Command::Store { cmd } => match cmd {
            StoreCmd::Gc {
                max_size,
                store,
                dry_run,
                evict_logs,
            } => cmd_store_gc(&store, max_size, dry_run, evict_logs, json),
            StoreCmd::Verify {
                sample,
                entry,
                store,
            } => cmd_store_verify(&store, sample, entry, json),
        },
        Command::Generations { cmd } => match cmd {
            GenerationsCmd::List { project } => {
                let project_dir = config::resolve_project_dir(project.as_deref())?;
                cmd_generations_list(&project_dir, json)
            }
            GenerationsCmd::Rm {
                generation,
                project,
            } => {
                let project_dir = config::resolve_project_dir(project.as_deref())?;
                cmd_generations_rm(&project_dir, generation, json)
            }
            GenerationsCmd::Keep {
                n,
                keep_explicit,
                project,
            } => {
                let project_dir = config::resolve_project_dir(project.as_deref())?;
                cmd_generations_keep(&project_dir, n, keep_explicit, json)
            }
        },
        Command::Gc {
            keep,
            keep_oplog,
            max_size,
            evict_logs,
            dry_run,
            project,
        } => {
            let project_dir = config::resolve_project_dir(project.as_deref())?;
            cmd_gc(
                &project_dir,
                keep,
                keep_oplog,
                max_size,
                evict_logs,
                dry_run,
                json,
            )
        }
        Command::Rollback {
            generation,
            project,
        } => {
            let project_dir = config::resolve_project_dir(project.as_deref())?;
            cmd_rollback(&project_dir, generation, json)
        }
        Command::Explain { view_path, project } => {
            let project_dir = config::resolve_project_dir(project.as_deref())?;
            cmd_explain(&project_dir, &view_path, json)
        }
        Command::DiffEntries { oh1, oh2, store } => cmd_diff_entries(&store, &oh1, &oh2, json),
    }
}

fn store_display_name(path: &Path) -> String {
    path.file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("store")
        .to_string()
}

fn print_json<T: Serialize>(value: &T) -> Result<(), AppError> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

// ---- store gc ----

fn cmd_store_gc(
    store_path: &Path,
    max_size: Option<u64>,
    dry_run: bool,
    evict_logs: bool,
    json: bool,
) -> Result<i32, AppError> {
    let store = Store::open(&store_display_name(store_path), store_path, false)?;
    let policy = GcPolicy {
        max_size,
        evict_logs,
        dry_run,
    };
    let report = store.gc(&policy)?;
    if json {
        print_json(&report)?;
    } else {
        println!(
            "{}removed {} entries, {} logs, {} dangling input link(s); freed {} bytes, {} bytes remaining",
            if report.dry_run { "[dry-run] " } else { "" },
            report.removed_entries.len(),
            report.removed_logs.len(),
            report.removed_dangling_inputs.len(),
            report.bytes_freed,
            report.remaining_size,
        );
        for oh in &report.removed_entries {
            println!("  removed entry {oh}");
        }
    }
    Ok(0)
}

// ---- store verify ----

fn select_deterministic_sample(all: &[String], pct: f64) -> Vec<String> {
    if pct >= 100.0 {
        return all.to_vec();
    }
    if pct <= 0.0 {
        return Vec::new();
    }
    // Hash-threshold sampling: since `oh` is a blake3 hash, its leading
    // bits are uniformly distributed, so "entries whose numeric prefix
    // falls below a threshold" is a fair, seedless, deterministic random
    // sample that is stable across repeated runs against the same store.
    let threshold = ((pct / 100.0) * (u32::MAX as f64)) as u32;
    all.iter()
        .filter(|oh| {
            oh.get(..8)
                .and_then(|prefix| u32::from_str_radix(prefix, 16).ok())
                .map(|v| v <= threshold)
                .unwrap_or(false)
        })
        .cloned()
        .collect()
}

fn cmd_store_verify(
    store_path: &Path,
    sample: Option<f64>,
    entry: Option<String>,
    json: bool,
) -> Result<i32, AppError> {
    let store = Store::open(&store_display_name(store_path), store_path, true)?;
    let targets: Vec<String> = if let Some(oh) = entry {
        vec![oh]
    } else {
        let all = explain::list_entries(&store)?;
        select_deterministic_sample(&all, sample.unwrap_or(100.0))
    };

    let mut reports = Vec::with_capacity(targets.len());
    let mut any_fail = false;
    for oh in &targets {
        let report = store.verify_entry(oh)?;
        if !report.ok {
            any_fail = true;
        }
        reports.push(report);
    }

    if json {
        print_json(&reports)?;
    } else {
        for r in &reports {
            if r.ok {
                println!("OK    {}", r.oh);
            } else {
                println!("FAIL  {}", r.oh);
                for m in &r.mismatches {
                    println!("        {m}");
                }
            }
        }
        println!(
            "{}/{} entries verified ok",
            reports.iter().filter(|r| r.ok).count(),
            reports.len()
        );
    }
    Ok(if any_fail { 1 } else { 0 })
}

// ---- gc (project-level, two-phase) ----

/// Combined report of the two split GC phases. `stores` maps store name ->
/// its mark/sweep report; readonly stores are listed under
/// `skipped_readonly_stores` instead of being swept.
#[derive(Serialize)]
struct GcCombinedReport {
    generations: ppg3_core::views::RemoveOldReport,
    stores: std::collections::BTreeMap<String, ppg3_core::gc::GcReport>,
    skipped_readonly_stores: Vec<String>,
}

#[allow(clippy::too_many_arguments)]
fn cmd_gc(
    project_dir: &Path,
    keep: u64,
    keep_oplog: u64,
    max_size: Option<u64>,
    evict_logs: bool,
    dry_run: bool,
    json: bool,
) -> Result<i32, AppError> {
    let stores = config::load_storeset(project_dir)?;

    // Phase 1: generation lifecycle — old generations go first so their
    // roots are unregistered before the store sweep marks.
    let generations =
        views::remove_old_generations(project_dir, &stores, keep, keep_oplog, dry_run)?;

    // Phase 2: per-store mark/sweep of what is genuinely unreferenced now.
    let mut store_reports = std::collections::BTreeMap::new();
    let mut skipped_readonly_stores = Vec::new();
    for store in &stores.stores {
        if store.is_readonly() {
            skipped_readonly_stores.push(store.name().to_string());
            continue;
        }
        let report = store.gc(&GcPolicy {
            max_size,
            evict_logs,
            dry_run,
        })?;
        store_reports.insert(store.name().to_string(), report);
    }

    let combined = GcCombinedReport {
        generations,
        stores: store_reports,
        skipped_readonly_stores,
    };
    if json {
        print_json(&combined)?;
    } else {
        let prefix = if dry_run { "[dry-run] " } else { "" };
        println!(
            "{prefix}phase 1 (old generations): dropped {} committed ({}), {} op-log ({}); kept {}",
            combined.generations.dropped_committed.len(),
            fmt_u64s(&combined.generations.dropped_committed),
            combined.generations.dropped_oplog.len(),
            fmt_u64s(&combined.generations.dropped_oplog),
            fmt_u64s(&combined.generations.kept),
        );
        println!("{prefix}phase 2 (unreferenced store entries):");
        for (name, r) in &combined.stores {
            println!(
                "{prefix}  store {name}: removed {} entries, {} logs, {} dangling input link(s); freed {} bytes, {} bytes remaining",
                r.removed_entries.len(),
                r.removed_logs.len(),
                r.removed_dangling_inputs.len(),
                r.bytes_freed,
                r.remaining_size,
            );
        }
        for name in &combined.skipped_readonly_stores {
            println!("{prefix}  store {name}: skipped (readonly)");
        }
    }
    Ok(0)
}

fn fmt_u64s(ns: &[u64]) -> String {
    if ns.is_empty() {
        return "-".to_string();
    }
    ns.iter().map(u64::to_string).collect::<Vec<_>>().join(", ")
}

// ---- generations ----

fn cmd_generations_list(project_dir: &Path, json: bool) -> Result<i32, AppError> {
    let gens = views::list_generations(project_dir)?;
    if json {
        print_json(&gens)?;
    } else {
        println!(
            "{:<6} {:<16} {:<10} {:<8} {:<10} {:<14} ENTRIES",
            "GEN", "CREATED_AT_MS", "EPHEMERAL", "CURRENT", "VCS", "CHANGE_ID"
        );
        for g in &gens {
            let (vcs_state, change_id) = match &g.vcs {
                Some(v) if v.committed => ("committed", v.change_id.as_str()),
                Some(v) => ("op-log", v.change_id.as_str()),
                None => ("-", "-"),
            };
            let change_short: String = change_id.chars().take(12).collect();
            println!(
                "{:<6} {:<16} {:<10} {:<8} {:<10} {:<14} {}",
                g.n, g.created_at, g.ephemeral, g.current, vcs_state, change_short, g.n_entries
            );
        }
    }
    Ok(0)
}

fn cmd_generations_rm(project_dir: &Path, generation: u64, json: bool) -> Result<i32, AppError> {
    let stores = config::load_storeset(project_dir)?;
    views::drop_generation(project_dir, &stores, generation)?;
    if json {
        print_json(&serde_json::json!({"dropped": [generation]}))?;
    } else {
        println!("dropped generation {generation}");
    }
    Ok(0)
}

fn cmd_generations_keep(
    project_dir: &Path,
    n: u64,
    keep_explicit: bool,
    json: bool,
) -> Result<i32, AppError> {
    let stores = config::load_storeset(project_dir)?;
    let dropped = views::keep_last(project_dir, &stores, n, keep_explicit)?;
    if json {
        print_json(&serde_json::json!({"dropped": dropped}))?;
    } else if dropped.is_empty() {
        println!("nothing to drop");
    } else {
        println!(
            "dropped {} generation(s): {}",
            dropped.len(),
            dropped
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    Ok(0)
}

// ---- rollback ----

fn cmd_rollback(project_dir: &Path, generation: Option<u64>, json: bool) -> Result<i32, AppError> {
    let target = match generation {
        Some(n) => n,
        None => {
            let current = views::current_generation_number(project_dir)?.ok_or_else(|| {
                AppError::Operational("no current generation to roll back from".to_string())
            })?;
            views::previous_existing_generation(project_dir, current)?.ok_or_else(|| {
                AppError::Operational("no earlier generation to roll back to".to_string())
            })?
        }
    };
    views::rollback(project_dir, target)?;
    if json {
        print_json(&serde_json::json!({"rolled_back_to": target}))?;
    } else {
        println!("rolled back to generation {target}");
    }
    Ok(0)
}

// ---- explain ----

/// `outputs/results/counts.tsv` and `results/counts.tsv` must both resolve
/// to the same `meta.json` entry: strip a leading `./` and an `outputs/`
/// prefix (the convention `write_generation`'s `ViewSpec::view_rel_path`
/// uses, documented in `views.rs`).
fn normalize_view_path(raw: &str) -> String {
    let mut s = raw.trim_start_matches("./");
    if let Some(rest) = s.strip_prefix("outputs/") {
        s = rest;
    }
    s.trim_start_matches('/').to_string()
}

fn cmd_explain(project_dir: &Path, raw_view_path: &str, json: bool) -> Result<i32, AppError> {
    let stores = config::load_storeset(project_dir)?;
    let view_path = normalize_view_path(raw_view_path);
    let explanation = explain::explain_view_path(project_dir, &stores, &view_path)?;
    if json {
        print_json(&explanation)?;
    } else {
        print_explanation_human(&explanation);
    }
    Ok(0)
}

fn print_explanation_human(explanation: &Explanation) {
    match explanation {
        Explanation::FirstAppearance {
            view_path,
            generation,
            oh,
        } => {
            println!("{view_path}: first appearance in generation {generation} (oh {oh})");
        }
        Explanation::Diff {
            view_path,
            previous_generation,
            current_generation,
            oh_a,
            oh_b,
            diff,
            why_chain,
        } => {
            println!(
                "{view_path}: generation {previous_generation} ({oh_a}) -> {current_generation} ({oh_b})"
            );
            print_keydoc_diff_human("  ", diff);
            for step in why_chain {
                println!(
                    "  why (depth {}): input {:?} changed ({} -> {})",
                    step.depth, step.input_name, step.oh_a, step.oh_b
                );
                print_keydoc_diff_human("    ", &step.diff);
            }
        }
    }
}

fn print_keydoc_diff_human(indent: &str, diff: &explain::KeyDocDiff) {
    if diff.recipe_changed {
        println!("{indent}recipe changed");
    }
    if !diff.inputs.is_empty() {
        println!(
            "{indent}inputs: +{:?} -{:?} ~{:?}",
            diff.inputs.added, diff.inputs.removed, diff.inputs.changed
        );
    }
    if !diff.tools.is_empty() {
        println!(
            "{indent}tools: +{:?} -{:?} ~{:?}",
            diff.tools.added, diff.tools.removed, diff.tools.changed
        );
    }
    if !diff.env.is_empty() {
        println!(
            "{indent}env: +{:?} -{:?} ~{:?}",
            diff.env.added, diff.env.removed, diff.env.changed
        );
    }
    if diff.runtime_changed {
        println!("{indent}runtime changed");
    }
    if diff.outputs_declared_changed {
        println!("{indent}outputs_declared changed");
    }
}

// ---- diff-entries ----

fn cmd_diff_entries(store_path: &Path, oh1: &str, oh2: &str, json: bool) -> Result<i32, AppError> {
    let store = Store::open(&store_display_name(store_path), store_path, true)?;
    let diff = explain::diff_entries(&store, oh1, oh2)?;
    if json {
        print_json(&diff)?;
    } else {
        println!("diff {oh1} -> {oh2}");
        for p in &diff.added {
            println!("  + {p}");
        }
        for p in &diff.removed {
            println!("  - {p}");
        }
        for c in &diff.changed {
            println!(
                "  ~ {} (size {}->{}, first differing byte offset {:?})",
                c.path, c.size_a, c.size_b, c.first_diff_offset
            );
        }
        if diff.added.is_empty() && diff.removed.is_empty() && diff.changed.is_empty() {
            println!("  (identical)");
        }
    }
    Ok(0)
}
