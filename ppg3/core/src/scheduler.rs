//! Scheduler (WP4, PPG3_DESIGN.md §8.1, CONTRACT.md "Scheduler").
//!
//! All concurrency lives here (design §8.1 rule 2): a fixed pool of worker
//! threads share one `Mutex<State>` + `Condvar` — "single logical owner of
//! all state", workers are dumb. `HostCallbacks` are the only two places
//! Rust calls back into Python (rule 1), and they run on a scheduler worker
//! thread with no lock held.
//!
//! ## Additive extension: `JobDef.graph_job`
//!
//! CONTRACT.md's `JobDef` sketch has no field distinguishing a `GraphJob`
//! (§7.4 — expands into more jobs at dispatch time) from a plain
//! `ExecTemplate::InProcess` "loader layer" job (§6.3 item 2 — runs a
//! callback, produces no store entry, otherwise inert). Both need
//! `ExecTemplate::InProcess`, but they invoke *different* `HostCallbacks`
//! methods (`expand_graph_job` vs `run_in_process`) and there is no way to
//! tell them apart from the rest of `JobDef`'s fields. This module adds
//! `pub graph_job: bool` (`#[serde(default)]`, so old JSON without the
//! field still deserializes as `false` / a plain InProcess job) — see the
//! CONTRACT.md addendum below this module's doc comment reference and
//! STATUS.md.
//!
//! ## Failure policy (implementation note)
//!
//! Per CONTRACT.md, "a failed job fails its transitive dependents...;
//! independent subgraph continues." This module goes one step further in
//! how errors are routed: *every* error a specific job's own processing can
//! raise — store I/O, `DeterminismViolation`, executor spawn failure,
//! non-zero exit, a `HostCallbacks` `Err`, a malformed `{in:NAME}`
//! placeholder — becomes that job's `report.failed` entry, never a `run()`-
//! level `Err`. `run()` only returns `Err` for the pre-dispatch validation
//! pass (`Error::Graph`: cycles, unknown ids, duplicate ids, resource
//! requests that can never be satisfied). This is what makes "independent
//! subgraphs continue" true even when a job hits a store-level hard error.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::canon;
use crate::error::Error;
use crate::executor::{Executor, Mount, PreparedJob};
use crate::lease::Lease;
use crate::manifest::{BuiltInfo, ContentMap};
use crate::resources::Pools;
use crate::storeset::StoreSet;
use crate::Result;

// =========================================================== public types

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobDef {
    pub id: String,
    /// Strict hash of the job's function/argv (§5); computed by the caller.
    pub recipe: String,
    pub inputs: BTreeMap<String, InputRef>,
    /// name -> tool hash. For `CommandJob`s this is expected to double as a
    /// real filesystem path (nix store path, §7.5) used to build the
    /// tool's `Mount`; see STATUS.md for the assumption this WP made about
    /// non-nix (`ToolSpec.binary`) tools, which is genuinely underspecified
    /// at this layer.
    pub tools: BTreeMap<String, String>,
    pub runtime: Value,
    pub env: BTreeMap<String, String>,
    /// Relative paths, validated against `{out:NAME}` placeholders via
    /// `view` (see `resolve_placeholder`).
    pub outputs_declared: Vec<String>,
    pub resources: BTreeMap<String, u64>,
    pub store_target: Option<String>,
    pub retain: Retain,
    pub exec_template: ExecTemplate,
    /// output name -> view-relative path.
    pub view: BTreeMap<String, String>,
    pub fixed_output: Option<String>,
    /// Additive extension — see module docs. `false` (plain job, or a
    /// non-graph `InProcess` loader-layer job) unless explicitly set.
    #[serde(default)]
    pub graph_job: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InputRef {
    Job { id: String },
    JobSubset { id: String, names: Vec<String> },
    Leaf { hash: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExecTemplate {
    Argv { argv: Vec<String>, allow_network: bool },
    /// => host callback (`run_in_process`, or `expand_graph_job` when
    /// `JobDef.graph_job` is set). Produces no store entry.
    InProcess,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Retain {
    Default,
    Evict,
    Pin(String),
}

pub trait HostCallbacks: Send + Sync {
    /// §7.4: expand a `GraphJob` into more `JobDef`s, merged into the
    /// running graph. New jobs may depend on existing ones; existing jobs
    /// may not gain new dependencies (their `inputs` are fixed at
    /// insertion time).
    fn expand_graph_job(&self, job_id: &str) -> Result<Vec<JobDef>>;
    /// §6.3 item 2 (the loader layer): run a job's callback in the
    /// coordinating process. `key_doc` is the same §5 document a normal
    /// job would hash, handed over unhashed-by-Rust so the Python side can
    /// memoize per `(process, ik)` itself.
    fn run_in_process(&self, job_id: &str, key_doc: &Value) -> Result<()>;
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RunReport {
    pub built: Vec<String>,
    pub hits: Vec<String>,
    pub failed: BTreeMap<String, String>,
    /// id -> (ik, oh), for every non-`InProcess` job (hit or built).
    pub job_entries: BTreeMap<String, (String, String)>,
}

/// Entry point (CONTRACT.md "Scheduler"). `parallelism` doubles as both the
/// resource pool capacities (every key except `"workers"`) and the worker
/// thread count (`"workers"`, default 4) — the pool set and the thread
/// count are deliberately distinct knobs (a job's declared `resources`
/// bound *concurrent resource usage*; `"workers"` bounds how many worker
/// threads exist to dispatch jobs at all).
pub fn run(
    storeset: &StoreSet,
    executor: &dyn Executor,
    jobs: Vec<JobDef>,
    callbacks: &dyn HostCallbacks,
    parallelism: &BTreeMap<String, u64>,
    abort: &AtomicBool,
) -> Result<RunReport> {
    let pools_cap = pool_capacities(parallelism);
    validate_and_index(&jobs, &HashMap::new(), &pools_cap)?;

    let mut state = State::new();
    for job in jobs {
        insert_job(&mut state, job);
    }

    let pools = Pools::new(pools_cap);
    let worker_count = parallelism.get("workers").copied().unwrap_or(4).max(1) as usize;

    let run_id = generate_run_id();
    // Best-effort: a run with no writable store configured (e.g. entirely
    // InProcess jobs, or a validate-only pass over readonly mirrors) should
    // not hard-fail just because there is nothing to lease. See STATUS.md.
    let run_lease = storeset
        .write_store(None)
        .ok()
        .and_then(|s| s.lease(&run_id).ok());

    let shared = Shared {
        state: Mutex::new(state),
        cv: Condvar::new(),
        storeset,
        executor,
        callbacks,
        pools,
        abort,
        run_lease,
    };

    thread::scope(|scope| {
        for _ in 0..worker_count {
            scope.spawn(|| worker_loop(&shared));
        }
    });

    Ok(shared.state.into_inner().unwrap().report)
}

fn pool_capacities(parallelism: &BTreeMap<String, u64>) -> BTreeMap<String, u64> {
    parallelism
        .iter()
        .filter(|(k, _)| k.as_str() != "workers")
        .map(|(k, v)| (k.clone(), *v))
        .collect()
}

fn generate_run_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("run-{}-{nanos:x}", std::process::id())
}

// ============================================================ validation

/// Static graph validation (CONTRACT.md: "unknown input job ids, cycles
/// (Kahn), duplicate job ids, resource requests vs pools -> Error::Graph
/// BEFORE any dispatch"). Reused for both the initial `run()` call
/// (`existing_jobs` empty) and merging a `GraphJob`'s expansion
/// (`existing_jobs` = the graph so far).
fn validate_and_index(
    new_jobs: &[JobDef],
    existing_jobs: &HashMap<String, JobDef>,
    pools_cap: &BTreeMap<String, u64>,
) -> Result<()> {
    let mut seen: HashSet<&str> = HashSet::new();
    for j in new_jobs {
        if !seen.insert(j.id.as_str()) {
            return Err(Error::Graph(format!("duplicate job id: {:?}", j.id)));
        }
        if existing_jobs.contains_key(&j.id) {
            return Err(Error::Graph(format!(
                "job id {:?} already exists in the running graph",
                j.id
            )));
        }
    }

    let new_by_id: HashMap<&str, &JobDef> = new_jobs.iter().map(|j| (j.id.as_str(), j)).collect();

    for j in new_jobs {
        for r in j.inputs.values() {
            let pid = match r {
                InputRef::Job { id } | InputRef::JobSubset { id, .. } => Some(id.as_str()),
                InputRef::Leaf { .. } => None,
            };
            if let Some(pid) = pid {
                let parent: &JobDef = if let Some(p) = existing_jobs.get(pid) {
                    p
                } else if let Some(p) = new_by_id.get(pid) {
                    p
                } else {
                    return Err(Error::Graph(format!(
                        "job {:?} depends on unknown job {:?}",
                        j.id, pid
                    )));
                };
                if matches!(parent.exec_template, ExecTemplate::InProcess) {
                    return Err(Error::Graph(format!(
                        "job {:?} references InProcess job {:?} as a store input (Job/JobSubset); \
                         InProcess jobs produce no store entry and can only be a scheduling barrier",
                        j.id, pid
                    )));
                }
            }
        }
        for (pool, amount) in &j.resources {
            match pools_cap.get(pool) {
                None => {
                    return Err(Error::Graph(format!(
                        "job {:?} requests unknown resource pool {pool:?}",
                        j.id
                    )))
                }
                Some(cap) if amount > cap => {
                    return Err(Error::Graph(format!(
                        "job {:?} requests {amount} units of pool {pool:?}, capacity is {cap}",
                        j.id
                    )))
                }
                _ => {}
            }
        }
    }

    detect_cycle(new_jobs)?;
    Ok(())
}

fn parent_ids(job: &JobDef) -> Vec<String> {
    job.inputs
        .values()
        .filter_map(|r| match r {
            InputRef::Job { id } | InputRef::JobSubset { id, .. } => Some(id.clone()),
            InputRef::Leaf { .. } => None,
        })
        .collect()
}

/// Kahn's algorithm over `jobs` alone (edges into a job outside this slice
/// — i.e. into `existing_jobs` during a merge — are ignored: "existing jobs
/// may NOT gain new deps" guarantees no back-edge from existing into new
/// can exist, so a cycle can only ever be formed within `jobs` itself).
fn detect_cycle(jobs: &[JobDef]) -> Result<()> {
    let ids: HashSet<String> = jobs.iter().map(|j| j.id.clone()).collect();
    let mut indeg: HashMap<String, usize> = jobs.iter().map(|j| (j.id.clone(), 0usize)).collect();
    let mut adj: HashMap<String, Vec<String>> = HashMap::new();
    for j in jobs {
        for pid in parent_ids(j) {
            if ids.contains(&pid) {
                adj.entry(pid).or_default().push(j.id.clone());
                *indeg.get_mut(&j.id).unwrap() += 1;
            }
        }
    }
    let mut queue: VecDeque<String> = indeg
        .iter()
        .filter(|(_, d)| **d == 0)
        .map(|(k, _)| k.clone())
        .collect();
    let mut visited = 0usize;
    while let Some(n) = queue.pop_front() {
        visited += 1;
        if let Some(children) = adj.get(&n) {
            for c in children.clone() {
                let d = indeg.get_mut(&c).unwrap();
                *d -= 1;
                if *d == 0 {
                    queue.push_back(c);
                }
            }
        }
    }
    if visited != jobs.len() {
        return Err(Error::Graph("cycle detected in job graph".to_string()));
    }
    Ok(())
}

// =============================================================== state

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Status {
    Pending,
    Ready,
    Running,
    Done,
    Failed,
}

#[derive(Clone)]
struct CompletedInfo {
    oh: String,
    content: ContentMap,
    store_idx: usize,
}

struct State {
    jobs: HashMap<String, JobDef>,
    status: HashMap<String, Status>,
    /// parent id -> [child ids] (built from every job's Job/JobSubset
    /// inputs, in both directions of the DAG this is the "wake dependents"
    /// edge set).
    dependents: HashMap<String, Vec<String>>,
    remaining_parents: HashMap<String, usize>,
    ready: VecDeque<String>,
    completed: HashMap<String, CompletedInfo>,
    report: RunReport,
    total_jobs: usize,
    running: usize,
    terminal: usize,
}

impl State {
    fn new() -> State {
        State {
            jobs: HashMap::new(),
            status: HashMap::new(),
            dependents: HashMap::new(),
            remaining_parents: HashMap::new(),
            ready: VecDeque::new(),
            completed: HashMap::new(),
            report: RunReport::default(),
            total_jobs: 0,
            running: 0,
            terminal: 0,
        }
    }
}

/// Insert a validated job into the running graph (initial load, or a
/// `GraphJob` expansion merge): wires `dependents`/`remaining_parents`,
/// and immediately marks it Ready (zero live parents) or cascades a
/// Failed status if a parent already failed before this job existed.
fn insert_job(state: &mut State, job: JobDef) {
    let id = job.id.clone();
    let parents = parent_ids(&job);
    let mut remaining = 0usize;
    let mut failed_parent: Option<String> = None;
    for p in &parents {
        match state.status.get(p) {
            Some(Status::Done) => {}
            Some(Status::Failed) => {
                if failed_parent.is_none() {
                    failed_parent = Some(p.clone());
                }
            }
            _ => remaining += 1,
        }
        state.dependents.entry(p.clone()).or_default().push(id.clone());
    }
    state.jobs.insert(id.clone(), job);
    state.remaining_parents.insert(id.clone(), remaining);
    state.status.insert(id.clone(), Status::Pending);
    state.total_jobs += 1;
    if let Some(fp) = failed_parent {
        fail_job(state, &id, format!("upstream failed: {fp}"));
    } else if remaining == 0 {
        mark_ready_if_pending(state, &id);
    }
}

fn mark_ready_if_pending(state: &mut State, id: &str) {
    if state.status.get(id) == Some(&Status::Pending) {
        state.status.insert(id.to_string(), Status::Ready);
        state.ready.push_back(id.to_string());
    }
}

fn complete_success(state: &mut State, id: &str) {
    state.status.insert(id.to_string(), Status::Done);
    state.terminal += 1;
    if let Some(children) = state.dependents.get(id).cloned() {
        for c in children {
            if let Some(rem) = state.remaining_parents.get_mut(&c) {
                if *rem > 0 {
                    *rem -= 1;
                }
                if *rem == 0 {
                    mark_ready_if_pending(state, &c);
                }
            }
        }
    }
}

/// Mark `id` Failed (idempotent — a no-op if already terminal) and cascade
/// to every not-yet-run dependent, recursively. Dependents that are already
/// `Running` are left alone: they were dispatched before `id` failed and
/// will report their own outcome normally (their *own* dependents, if any,
/// still get the cascade once they finish, via the normal completion path
/// — or immediately here if they too are still Pending/Ready).
fn fail_job(state: &mut State, id: &str, reason: String) {
    if matches!(state.status.get(id), Some(Status::Done) | Some(Status::Failed)) {
        return;
    }
    state.status.insert(id.to_string(), Status::Failed);
    state.report.failed.insert(id.to_string(), reason);
    state.terminal += 1;
    state.ready.retain(|x| x != id);
    if let Some(children) = state.dependents.get(id).cloned() {
        for c in children {
            if matches!(state.status.get(&c), Some(Status::Pending) | Some(Status::Ready)) {
                fail_job(state, &c, format!("upstream failed: {id}"));
            }
        }
    }
}

// ============================================================== workers

struct Shared<'a> {
    state: Mutex<State>,
    cv: Condvar,
    storeset: &'a StoreSet,
    executor: &'a dyn Executor,
    callbacks: &'a dyn HostCallbacks,
    pools: Pools,
    abort: &'a AtomicBool,
    run_lease: Option<Lease>,
}

fn worker_loop(shared: &Shared) {
    loop {
        let mut guard = shared.state.lock().unwrap();
        loop {
            let aborting = shared.abort.load(Ordering::SeqCst);
            let finished = guard.terminal >= guard.total_jobs && guard.running == 0;
            if finished || (aborting && guard.running == 0) {
                return;
            }
            if !aborting && !guard.ready.is_empty() {
                break;
            }
            guard = shared.cv.wait(guard).unwrap();
        }
        let id = guard.ready.pop_front().expect("checked non-empty above");
        guard.status.insert(id.clone(), Status::Running);
        guard.running += 1;
        drop(guard);

        let outcome = dispatch_job(shared, &id);

        let mut guard = shared.state.lock().unwrap();
        guard.running -= 1;
        apply_outcome(shared, &mut guard, &id, outcome);
        shared.cv.notify_all();
        drop(guard);
    }
}

enum JobOutcome {
    Hit {
        ik: String,
        oh: String,
        content: ContentMap,
        store_idx: usize,
    },
    Built {
        ik: String,
        oh: String,
        content: ContentMap,
        store_idx: usize,
    },
    InProcessDone,
    GraphExpanded(Vec<JobDef>),
    Failed(String),
}

/// Runs entirely outside the state lock — store I/O, executor spawn, and
/// (per §8.1 rule 1) the two coarse `HostCallbacks` all happen here.
fn dispatch_job(shared: &Shared, id: &str) -> JobOutcome {
    let (job, completed_snapshot) = {
        let guard = shared.state.lock().unwrap();
        let job = guard
            .jobs
            .get(id)
            .cloned()
            .expect("a dispatched job must be present in state.jobs");
        (job, guard.completed.clone())
    };

    if job.graph_job {
        return match shared.callbacks.expand_graph_job(id) {
            Ok(new_jobs) => JobOutcome::GraphExpanded(new_jobs),
            Err(e) => JobOutcome::Failed(format!("expand_graph_job({id:?}) failed: {e}")),
        };
    }

    let (key_doc, ik) = match derive_key(&job, &completed_snapshot) {
        Ok(v) => v,
        Err(msg) => return JobOutcome::Failed(msg),
    };

    if matches!(job.exec_template, ExecTemplate::InProcess) {
        return match shared.callbacks.run_in_process(id, &key_doc) {
            Ok(()) => JobOutcome::InProcessDone,
            Err(e) => JobOutcome::Failed(format!("run_in_process({id:?}) failed: {e}")),
        };
    }

    match dispatch_argv_job(shared, &job, &ik, &key_doc, &completed_snapshot) {
        Ok(outcome) => outcome,
        Err(e) => JobOutcome::Failed(e.to_string()),
    }
}

fn dispatch_argv_job(
    shared: &Shared,
    job: &JobDef,
    ik: &str,
    key_doc: &Value,
    completed: &HashMap<String, CompletedInfo>,
) -> Result<JobOutcome> {
    if let Some((store_idx, manifest)) = shared.storeset.lookup(ik)? {
        return Ok(JobOutcome::Hit {
            ik: ik.to_string(),
            oh: manifest.output_hash,
            content: manifest.content,
            store_idx,
        });
    }

    let (argv_template, allow_network) = match &job.exec_template {
        ExecTemplate::Argv { argv, allow_network } => (argv, *allow_network),
        ExecTemplate::InProcess => unreachable!("InProcess handled by the caller"),
    };

    // Definition errors were already rejected at validate() time; this can
    // still legitimately block.
    let _pool_guard = shared.pools.acquire(&job.resources)?;

    let write_store = shared.storeset.write_store(job.store_target.as_deref())?;
    let staging = write_store.open_staging()?;
    let out_dir = staging.path().to_path_buf();
    let log_dir = write_store.log_dir_for(ik)?;

    let mut input_mounts = Vec::new();
    for (name, r) in &job.inputs {
        let pid = match r {
            InputRef::Job { id } | InputRef::JobSubset { id, .. } => Some(id.as_str()),
            InputRef::Leaf { .. } => None,
        };
        if let Some(pid) = pid {
            let c = completed.get(pid).ok_or_else(|| {
                Error::Other(format!(
                    "internal scheduling error: missing completed info for parent {pid:?}"
                ))
            })?;
            let source = shared.storeset.stores[c.store_idx].data_dir(&c.oh);
            input_mounts.push(Mount {
                virtual_path: format!("/ppg/in/{name}"),
                source,
            });
        }
    }
    let mut tool_mounts = Vec::new();
    for (name, tool_hash) in &job.tools {
        tool_mounts.push(Mount {
            virtual_path: format!("/ppg/tools/{name}"),
            source: std::path::PathBuf::from(tool_hash),
        });
    }

    let argv = lower_argv(argv_template, job, completed)?;
    let env = build_env(job);

    let prepared = PreparedJob {
        ik: ik.to_string(),
        argv,
        env,
        inputs: input_mounts,
        tools: tool_mounts,
        out_dir,
        log_dir,
        allow_network,
        cwd_out: true,
    };

    let start_ms = now_ms();
    let exec_result = shared.executor.run(&prepared)?;
    let end_ms = now_ms();

    if exec_result.exit_code != 0 {
        return Err(Error::JobFailed(format!(
            "job {:?} exited with code {}\n--- stderr tail ---\n{}",
            job.id,
            exec_result.exit_code,
            stderr_tail(&exec_result.stderr)
        )));
    }

    let built = BuiltInfo {
        start_ms,
        end_ms,
        host: hostname(),
        sandboxed: shared.executor.is_sandboxed(),
        ppg3_version: env!("CARGO_PKG_VERSION").to_string(),
        retain_evict: matches!(job.retain, Retain::Evict),
    };
    let job_view_name = job.view.values().next().map(|s| s.as_str());
    let outcome = write_store.publish(staging, ik, key_doc, built, job_view_name)?;

    if let Some(declared) = &job.fixed_output {
        if outcome.oh() != declared {
            return Err(Error::JobFailed(format!(
                "job {:?} declared fixed_output {declared:?} but produced {:?}",
                job.id,
                outcome.oh()
            )));
        }
    }

    if let Retain::Pin(name) = &job.retain {
        write_store.pin(name, outcome.oh())?;
    }

    let manifest = write_store.lookup(ik)?.ok_or_else(|| {
        Error::Other(format!(
            "just-published entry for input key {ik} vanished from its own store"
        ))
    })?;

    let store_idx = shared
        .storeset
        .stores
        .iter()
        .position(|s| std::ptr::eq(s, write_store))
        .ok_or_else(|| {
            Error::Other("published-to store not found in StoreSet (internal)".to_string())
        })?;

    Ok(JobOutcome::Built {
        ik: ik.to_string(),
        oh: outcome.oh().to_string(),
        content: manifest.content,
        store_idx,
    })
}

fn apply_outcome(shared: &Shared, state: &mut State, id: &str, outcome: JobOutcome) {
    match outcome {
        JobOutcome::Hit { ik, oh, content, store_idx } => {
            state.report.hits.push(id.to_string());
            state.report.job_entries.insert(id.to_string(), (ik.clone(), oh.clone()));
            if let Some(lease) = &shared.run_lease {
                let _ = lease.protect(&oh);
            }
            state.completed.insert(id.to_string(), CompletedInfo { oh, content, store_idx });
            complete_success(state, id);
        }
        JobOutcome::Built { ik, oh, content, store_idx } => {
            state.report.built.push(id.to_string());
            state.report.job_entries.insert(id.to_string(), (ik.clone(), oh.clone()));
            if let Some(lease) = &shared.run_lease {
                let _ = lease.protect(&oh);
            }
            state.completed.insert(id.to_string(), CompletedInfo { oh, content, store_idx });
            complete_success(state, id);
        }
        JobOutcome::InProcessDone => {
            state.report.built.push(id.to_string());
            complete_success(state, id);
        }
        JobOutcome::GraphExpanded(new_jobs) => {
            let pools_cap = shared.pools.capacities().clone();
            match validate_and_index(&new_jobs, &state.jobs, &pools_cap) {
                Ok(()) => {
                    for j in new_jobs {
                        insert_job(state, j);
                    }
                    state.report.built.push(id.to_string());
                    complete_success(state, id);
                }
                Err(e) => {
                    fail_job(state, id, format!("graph_job expansion invalid: {e}"));
                }
            }
        }
        JobOutcome::Failed(reason) => {
            fail_job(state, id, reason);
        }
    }
}

// ============================================================ key derivation

/// Assemble the §5 key document and derive `ik` for `job`, given the
/// already-completed parents it references. Shared by `InProcess`
/// (non-`graph_job`) and `Argv` jobs alike. Errors here are per-job
/// failures (a missing `JobSubset` file name, an internal inconsistency),
/// never a whole-run abort.
fn derive_key(
    job: &JobDef,
    completed: &HashMap<String, CompletedInfo>,
) -> std::result::Result<(Value, String), String> {
    let mut inputs_val = serde_json::Map::new();
    for (name, r) in &job.inputs {
        let v = match r {
            InputRef::Leaf { hash } => hash.clone(),
            InputRef::Job { id } => {
                let c = completed.get(id).ok_or_else(|| {
                    format!(
                        "job {:?}: parent job {id:?} has no recorded output (internal scheduling error)",
                        job.id
                    )
                })?;
                c.oh.clone()
            }
            InputRef::JobSubset { id, names } => {
                let c = completed.get(id).ok_or_else(|| {
                    format!(
                        "job {:?}: parent job {id:?} has no recorded output (internal scheduling error)",
                        job.id
                    )
                })?;
                let mut m = serde_json::Map::new();
                for n in names {
                    let entry = c.content.get(n).ok_or_else(|| {
                        format!(
                            "job {:?}: subset input {name:?} references file {n:?} which is \
                             not present in parent job {id:?}'s manifest",
                            job.id
                        )
                    })?;
                    m.insert(n.clone(), Value::String(entry.blake3.clone()));
                }
                let bytes = canon::canonicalize(&Value::Object(m)).map_err(|e| e.to_string())?;
                crate::hash::blake3_hex(&bytes)
            }
        };
        inputs_val.insert(name.clone(), Value::String(v));
    }
    let tools_val: serde_json::Map<String, Value> = job
        .tools
        .iter()
        .map(|(k, v)| (k.clone(), Value::String(v.clone())))
        .collect();
    let env_val: serde_json::Map<String, Value> = job
        .env
        .iter()
        .map(|(k, v)| (k.clone(), Value::String(v.clone())))
        .collect();
    let outputs_val: Vec<Value> = job.outputs_declared.iter().cloned().map(Value::String).collect();

    let doc = serde_json::json!({
        "ppg3_key_version": crate::KEY_VERSION,
        "job_recipe": job.recipe,
        "inputs": Value::Object(inputs_val),
        "tools": Value::Object(tools_val),
        "runtime": job.runtime,
        "env": Value::Object(env_val),
        "outputs_declared": outputs_val,
    });
    let canon_bytes = canon::canonicalize(&doc)
        .map_err(|e| format!("job {:?}: canonicalizing key document: {e}", job.id))?;
    let ik = canon::input_key(&canon_bytes)
        .map_err(|e| format!("job {:?}: computing input key: {e}", job.id))?;
    Ok((doc, ik))
}

// ========================================================= argv lowering

fn lower_argv(
    argv_template: &[String],
    job: &JobDef,
    completed: &HashMap<String, CompletedInfo>,
) -> Result<Vec<String>> {
    argv_template
        .iter()
        .map(|s| lower_token(s, job, completed))
        .collect()
}

fn lower_token(s: &str, job: &JobDef, completed: &HashMap<String, CompletedInfo>) -> Result<String> {
    let mut out = String::new();
    let mut rest = s;
    while let Some(start) = rest.find('{') {
        let Some(end_rel) = rest[start..].find('}') else {
            out.push_str(rest);
            rest = "";
            break;
        };
        let end = start + end_rel;
        out.push_str(&rest[..start]);
        let token = &rest[start + 1..end];
        match resolve_placeholder(token, job, completed)? {
            Some(v) => out.push_str(&v),
            None => {
                out.push('{');
                out.push_str(token);
                out.push('}');
            }
        }
        rest = &rest[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

/// `{in:NAME}` -> the mount root `/ppg/in/NAME`, *unless* the parent's
/// manifest has exactly one file, in which case it points directly at that
/// file (`/ppg/in/NAME/<that file>`) — a documented convenience so
/// single-file jobs don't need to know their own output's filename.
/// `{out}` -> `/ppg/out`. `{out:NAME}` -> `/ppg/out/<job.view[NAME]>`
/// (job.view is the only NAME-keyed source of output paths; `outputs_declared`
/// is a plain `Vec<String>` with no names, so `view` is what actually
/// resolves a `NAME` to a relative path — see STATUS.md for why this
/// reading was chosen over the literal but self-contradictory CONTRACT.md
/// phrasing). `{tool:NAME}` -> `/ppg/tools/NAME`.
fn resolve_placeholder(
    token: &str,
    job: &JobDef,
    completed: &HashMap<String, CompletedInfo>,
) -> Result<Option<String>> {
    if token == "out" {
        return Ok(Some("/ppg/out".to_string()));
    }
    if let Some(name) = token.strip_prefix("in:") {
        let r = job.inputs.get(name).ok_or_else(|| {
            Error::JobFailed(format!(
                "job {:?}: argv references unknown input {{in:{name}}}",
                job.id
            ))
        })?;
        return match r {
            InputRef::Leaf { .. } => Err(Error::JobFailed(format!(
                "job {:?}: {{in:{name}}} refers to a Leaf input, which has no mounted path",
                job.id
            ))),
            InputRef::Job { id } | InputRef::JobSubset { id, .. } => {
                let c = completed.get(id).ok_or_else(|| {
                    Error::Other(format!(
                        "internal scheduling error: missing completed info for parent {id:?}"
                    ))
                })?;
                if c.content.len() == 1 {
                    let only = c.content.keys().next().unwrap();
                    Ok(Some(format!("/ppg/in/{name}/{only}")))
                } else {
                    Ok(Some(format!("/ppg/in/{name}")))
                }
            }
        };
    }
    if let Some(name) = token.strip_prefix("out:") {
        let rel = job.view.get(name).ok_or_else(|| {
            Error::JobFailed(format!(
                "job {:?}: argv references {{out:{name}}} but {name:?} is not a key of job.view",
                job.id
            ))
        })?;
        return Ok(Some(format!("/ppg/out/{rel}")));
    }
    if let Some(name) = token.strip_prefix("tool:") {
        if !job.tools.contains_key(name) {
            return Err(Error::JobFailed(format!(
                "job {:?}: argv references unknown tool {{tool:{name}}}",
                job.id
            )));
        }
        return Ok(Some(format!("/ppg/tools/{name}")));
    }
    Ok(None)
}

/// §6.1: baseline env, declared `job.env` wins on conflict.
fn build_env(job: &JobDef) -> BTreeMap<String, String> {
    let mut env = BTreeMap::new();
    let path = job
        .tools
        .keys()
        .map(|n| format!("/ppg/tools/{n}/bin"))
        .collect::<Vec<_>>()
        .join(":");
    env.insert("PATH".to_string(), path);
    env.insert("HOME".to_string(), "/tmp".to_string());
    env.insert("TMPDIR".to_string(), "/tmp".to_string());
    env.insert("TZ".to_string(), "UTC".to_string());
    env.insert("LC_ALL".to_string(), "C.UTF-8".to_string());
    env.insert("SOURCE_DATE_EPOCH".to_string(), "0".to_string());
    for (k, v) in &job.env {
        env.insert(k.clone(), v.clone());
    }
    env
}

fn stderr_tail(bytes: &[u8]) -> String {
    let s = String::from_utf8_lossy(bytes);
    let lines: Vec<&str> = s.lines().collect();
    let start = lines.len().saturating_sub(20);
    lines[start..].join("\n")
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("HOST"))
        .unwrap_or_else(|_| "host".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv_job(id: &str, inputs: BTreeMap<String, InputRef>) -> JobDef {
        JobDef {
            id: id.to_string(),
            recipe: "r".to_string(),
            inputs,
            tools: BTreeMap::new(),
            runtime: serde_json::json!({}),
            env: BTreeMap::new(),
            outputs_declared: vec![],
            resources: BTreeMap::new(),
            store_target: None,
            retain: Retain::Default,
            exec_template: ExecTemplate::Argv { argv: vec!["/bin/true".to_string()], allow_network: false },
            view: BTreeMap::new(),
            fixed_output: None,
            graph_job: false,
        }
    }

    fn in_process_job(id: &str, inputs: BTreeMap<String, InputRef>, graph_job: bool) -> JobDef {
        let mut j = argv_job(id, inputs);
        j.exec_template = ExecTemplate::InProcess;
        j.graph_job = graph_job;
        j
    }

    #[test]
    fn validate_rejects_duplicate_ids() {
        let jobs = vec![argv_job("a", BTreeMap::new()), argv_job("a", BTreeMap::new())];
        assert!(matches!(
            validate_and_index(&jobs, &HashMap::new(), &BTreeMap::new()),
            Err(Error::Graph(_))
        ));
    }

    #[test]
    fn validate_rejects_unknown_dep() {
        let mut inputs = BTreeMap::new();
        inputs.insert("x".to_string(), InputRef::Job { id: "missing".to_string() });
        let jobs = vec![argv_job("a", inputs)];
        assert!(matches!(
            validate_and_index(&jobs, &HashMap::new(), &BTreeMap::new()),
            Err(Error::Graph(_))
        ));
    }

    #[test]
    fn validate_rejects_cycle() {
        let mut i_a = BTreeMap::new();
        i_a.insert("x".to_string(), InputRef::Job { id: "b".to_string() });
        let mut i_b = BTreeMap::new();
        i_b.insert("x".to_string(), InputRef::Job { id: "a".to_string() });
        let jobs = vec![argv_job("a", i_a), argv_job("b", i_b)];
        assert!(matches!(
            validate_and_index(&jobs, &HashMap::new(), &BTreeMap::new()),
            Err(Error::Graph(_))
        ));
    }

    #[test]
    fn validate_rejects_inprocess_as_job_input() {
        let producer = in_process_job("p", BTreeMap::new(), false);
        let mut inputs = BTreeMap::new();
        inputs.insert("x".to_string(), InputRef::Job { id: "p".to_string() });
        let consumer = argv_job("c", inputs);
        assert!(matches!(
            validate_and_index(&[producer, consumer], &HashMap::new(), &BTreeMap::new()),
            Err(Error::Graph(_))
        ));
    }

    #[test]
    fn validate_rejects_unknown_pool_and_over_capacity() {
        let mut j = argv_job("a", BTreeMap::new());
        j.resources.insert("cores".to_string(), 8);
        let mut caps = BTreeMap::new();
        caps.insert("cores".to_string(), 4);
        assert!(matches!(
            validate_and_index(std::slice::from_ref(&j), &HashMap::new(), &caps),
            Err(Error::Graph(_))
        ));
        let mut j2 = argv_job("a", BTreeMap::new());
        j2.resources.insert("gpu".to_string(), 1);
        assert!(matches!(
            validate_and_index(std::slice::from_ref(&j2), &HashMap::new(), &caps),
            Err(Error::Graph(_))
        ));
    }

    #[test]
    fn validate_accepts_valid_diamond() {
        let a = argv_job("a", BTreeMap::new());
        let mut i_b = BTreeMap::new();
        i_b.insert("a".to_string(), InputRef::Job { id: "a".to_string() });
        let b = argv_job("b", i_b.clone());
        let c = argv_job("c", i_b);
        let mut i_d = BTreeMap::new();
        i_d.insert("b".to_string(), InputRef::Job { id: "b".to_string() });
        i_d.insert("c".to_string(), InputRef::Job { id: "c".to_string() });
        let d = argv_job("d", i_d);
        assert!(validate_and_index(&[a, b, c, d], &HashMap::new(), &BTreeMap::new()).is_ok());
    }

    fn completed_with(id: &str, files: &[&str]) -> HashMap<String, CompletedInfo> {
        let mut content = ContentMap::new();
        for f in files {
            content.insert(
                f.to_string(),
                crate::manifest::ContentEntry {
                    blake3: format!("{f}hash").repeat(8).chars().take(64).collect(),
                    mode: "0644".to_string(),
                    size: 1,
                },
            );
        }
        let mut m = HashMap::new();
        m.insert(
            id.to_string(),
            CompletedInfo { oh: "oh".to_string(), content, store_idx: 0 },
        );
        m
    }

    #[test]
    fn lower_token_out_placeholder() {
        let job = argv_job("a", BTreeMap::new());
        let completed = HashMap::new();
        assert_eq!(lower_token("{out}/x.txt", &job, &completed).unwrap(), "/ppg/out/x.txt");
    }

    #[test]
    fn lower_token_in_placeholder_single_file_points_at_file() {
        let mut inputs = BTreeMap::new();
        inputs.insert("data".to_string(), InputRef::Job { id: "p".to_string() });
        let job = argv_job("a", inputs);
        let completed = completed_with("p", &["only.txt"]);
        assert_eq!(
            lower_token("{in:data}", &job, &completed).unwrap(),
            "/ppg/in/data/only.txt"
        );
    }

    #[test]
    fn lower_token_in_placeholder_multi_file_points_at_dir() {
        let mut inputs = BTreeMap::new();
        inputs.insert("data".to_string(), InputRef::Job { id: "p".to_string() });
        let job = argv_job("a", inputs);
        let completed = completed_with("p", &["a.txt", "b.txt"]);
        assert_eq!(lower_token("{in:data}", &job, &completed).unwrap(), "/ppg/in/data");
    }

    #[test]
    fn lower_token_tool_placeholder() {
        let mut job = argv_job("a", BTreeMap::new());
        job.tools.insert("py".to_string(), "/nix/store/xyz-py".to_string());
        assert_eq!(
            lower_token("{tool:py}/bin/python3", &job, &HashMap::new()).unwrap(),
            "/ppg/tools/py/bin/python3"
        );
    }

    #[test]
    fn lower_token_out_named_uses_view() {
        let mut job = argv_job("a", BTreeMap::new());
        job.view.insert("result".to_string(), "results/x.tsv".to_string());
        assert_eq!(
            lower_token("{out:result}", &job, &HashMap::new()).unwrap(),
            "/ppg/out/results/x.tsv"
        );
    }

    #[test]
    fn lower_token_unknown_out_named_is_job_failed() {
        let job = argv_job("a", BTreeMap::new());
        assert!(matches!(
            lower_token("{out:nope}", &job, &HashMap::new()),
            Err(Error::JobFailed(_))
        ));
    }

    #[test]
    fn build_env_baseline_then_declared_overrides() {
        let mut job = argv_job("a", BTreeMap::new());
        job.env.insert("HOME".to_string(), "/custom/home".to_string());
        job.env.insert("MY_VAR".to_string(), "1".to_string());
        let env = build_env(&job);
        assert_eq!(env.get("HOME").unwrap(), "/custom/home");
        assert_eq!(env.get("TMPDIR").unwrap(), "/tmp");
        assert_eq!(env.get("TZ").unwrap(), "UTC");
        assert_eq!(env.get("LC_ALL").unwrap(), "C.UTF-8");
        assert_eq!(env.get("SOURCE_DATE_EPOCH").unwrap(), "0");
        assert_eq!(env.get("MY_VAR").unwrap(), "1");
    }

    #[test]
    fn derive_key_produces_canonical_document() {
        let mut inputs = BTreeMap::new();
        inputs.insert("data".to_string(), InputRef::Job { id: "p".to_string() });
        inputs.insert("leaf".to_string(), InputRef::Leaf { hash: "deadbeef".to_string() });
        let job = argv_job("a", inputs);
        let completed = completed_with("p", &["only.txt"]);
        let (doc, ik) = derive_key(&job, &completed).unwrap();
        assert_eq!(ik.len(), 64);
        let bytes = canon::canonicalize(&doc).unwrap();
        canon::validate(&bytes).unwrap();
        assert_eq!(doc["inputs"]["leaf"], serde_json::json!("deadbeef"));
    }

    #[test]
    fn derive_key_subset_missing_name_is_job_failed_message() {
        let mut inputs = BTreeMap::new();
        inputs.insert(
            "data".to_string(),
            InputRef::JobSubset { id: "p".to_string(), names: vec!["missing.txt".to_string()] },
        );
        let job = argv_job("a", inputs);
        let completed = completed_with("p", &["only.txt"]);
        let err = derive_key(&job, &completed).unwrap_err();
        assert!(err.contains("missing.txt"));
    }
}
