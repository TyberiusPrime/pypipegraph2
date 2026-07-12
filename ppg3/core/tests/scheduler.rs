//! Scheduler integration tests (WP4, PPG3_DESIGN.md §12.3 "property tests on
//! the engine", CONTRACT.md "Scheduler"/"Executor"/"Store"/"StoreSet").
//!
//! These exercise `scheduler::run()` end-to-end against a real (tempdir)
//! `Store`/`StoreSet` with a `MockExecutor` standing in for a real sandbox:
//! every test is one of the §12.3 oracle properties (every miss built
//! exactly once, every hit built zero times, early cutoff on unchanged
//! output hash, parameter-flip determinism, failure propagation, abort,
//! resource serialization, ...).
//!
//! `MockExecutor` is keyed off `argv[1]` (each `JobDef` here uses
//! `argv: ["mock", "<job-id>"]`, per the WP4 assignment note — placeholders
//! are already lowered by the time `Executor::run` sees them, and there are
//! none here to lower) and writes configured `(relative-path, bytes)` pairs
//! directly into `job.out_dir`, which is already the real staging `data/`
//! directory (`Store::open_staging`) — no `NoneExecutor` staged-symlink tree
//! is needed for these tests, since we're testing scheduler orchestration,
//! not sandbox mechanics (that's WP3's `core/tests` scope).

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::Duration;

use serde_json::Value;

use ppg3_core::error::Error;
use ppg3_core::executor::{ExecResult, Executor, PreparedJob};
use ppg3_core::scheduler::{self, ExecTemplate, HostCallbacks, InputRef, JobDef, Retain};
use ppg3_core::store::Store;
use ppg3_core::storeset::StoreSet;

// ============================================================ MockExecutor

/// Test double for `Executor`: keyed by `argv[1]` (the job id). Records
/// every invocation (order + count), writes configured output files on
/// success, can be configured to fail a job, delay a job (to widen
/// concurrency-observation windows), rendezvous two jobs at a `Barrier`
/// (to force a genuine race for the determinism-violation test), or flip an
/// `AtomicBool` mid-run (for the abort test).
#[derive(Default)]
struct MockState {
    outputs: HashMap<String, Vec<(String, Vec<u8>)>>,
    exit_codes: HashMap<String, i32>,
    delays: HashMap<String, Duration>,
    barriers: HashMap<String, Arc<Barrier>>,
    abort_on: HashMap<String, Arc<AtomicBool>>,
    /// Job ids in the order `Executor::run` was actually called for them —
    /// this doubles as the "sequence counter": since the scheduler only
    /// ever calls `run` for a job after all its dependency edges are
    /// satisfied, push order here is a valid total order consistent with
    /// the dependency DAG.
    invocations: Vec<String>,
}

struct MockExecutor {
    state: Mutex<MockState>,
    active: AtomicUsize,
    max_active: AtomicUsize,
}

impl MockExecutor {
    fn new() -> Self {
        MockExecutor {
            state: Mutex::new(MockState::default()),
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
        }
    }

    fn set_output(&self, job_id: &str, files: &[(&str, &[u8])]) {
        let mut s = self.state.lock().unwrap();
        s.outputs.insert(
            job_id.to_string(),
            files.iter().map(|(p, c)| (p.to_string(), c.to_vec())).collect(),
        );
    }

    fn set_exit_code(&self, job_id: &str, code: i32) {
        self.state.lock().unwrap().exit_codes.insert(job_id.to_string(), code);
    }

    fn set_delay(&self, job_id: &str, dur: Duration) {
        self.state.lock().unwrap().delays.insert(job_id.to_string(), dur);
    }

    fn set_barrier(&self, job_id: &str, barrier: Arc<Barrier>) {
        self.state.lock().unwrap().barriers.insert(job_id.to_string(), barrier);
    }

    fn set_abort_on(&self, job_id: &str, flag: Arc<AtomicBool>) {
        self.state.lock().unwrap().abort_on.insert(job_id.to_string(), flag);
    }

    fn count(&self, job_id: &str) -> usize {
        self.state.lock().unwrap().invocations.iter().filter(|i| i.as_str() == job_id).count()
    }

    fn total_invocations(&self) -> usize {
        self.state.lock().unwrap().invocations.len()
    }

    fn invocation_order(&self) -> Vec<String> {
        self.state.lock().unwrap().invocations.clone()
    }

    fn max_concurrency(&self) -> usize {
        self.max_active.load(Ordering::SeqCst)
    }
}

impl Executor for MockExecutor {
    fn run(&self, job: &PreparedJob) -> ppg3_core::Result<ExecResult> {
        let job_id = job
            .argv
            .get(1)
            .cloned()
            .unwrap_or_else(|| panic!("MockExecutor expects argv = [\"mock\", <job-id>], got {:?}", job.argv));

        // NB: bind before the `if let` — in edition 2021 a `lock()` temporary
        // in the scrutinee lives to the end of the `if let` block, which would
        // hold the mutex across `wait()` and deadlock the other barrier party.
        let barrier = self.state.lock().unwrap().barriers.get(&job_id).cloned();
        if let Some(b) = barrier {
            b.wait();
        }

        self.state.lock().unwrap().invocations.push(job_id.clone());

        let now_active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(now_active, Ordering::SeqCst);

        // Same guard-lifetime hazard as the barrier above: bind first so the
        // lock is not held across the sleep (it would serialize every job).
        let delay = self.state.lock().unwrap().delays.get(&job_id).copied();
        if let Some(d) = delay {
            std::thread::sleep(d);
        }

        self.active.fetch_sub(1, Ordering::SeqCst);

        let exit_code = self.state.lock().unwrap().exit_codes.get(&job_id).copied().unwrap_or(0);

        if exit_code == 0 {
            let files = self.state.lock().unwrap().outputs.get(&job_id).cloned();
            if let Some(files) = files {
                for (rel, content) in files {
                    let path = job.out_dir.join(&rel);
                    if let Some(parent) = path.parent() {
                        std::fs::create_dir_all(parent).unwrap();
                    }
                    std::fs::write(&path, &content).unwrap();
                }
            }
        }

        if let Some(flag) = self.state.lock().unwrap().abort_on.get(&job_id).cloned() {
            flag.store(true, Ordering::SeqCst);
        }

        Ok(ExecResult {
            exit_code,
            stdout: Vec::new(),
            stderr: if exit_code != 0 {
                format!("mock executor: job {job_id:?} configured to exit {exit_code}").into_bytes()
            } else {
                Vec::new()
            },
        })
    }
}

// ============================================================ TestCallbacks

/// `HostCallbacks` test double. With nothing configured it behaves like a
/// "Noop" implementation: `expand_graph_job` errors loudly (naming the job)
/// rather than silently doing nothing, and `run_in_process` always succeeds
/// while recording the call for assertions.
struct TestCallbacks {
    expansions: Mutex<HashMap<String, Vec<JobDef>>>,
    run_in_process_calls: Mutex<Vec<(String, Value)>>,
}

impl TestCallbacks {
    fn new() -> Self {
        TestCallbacks {
            expansions: Mutex::new(HashMap::new()),
            run_in_process_calls: Mutex::new(Vec::new()),
        }
    }

    fn set_expansion(&self, job_id: &str, jobs: Vec<JobDef>) {
        self.expansions.lock().unwrap().insert(job_id.to_string(), jobs);
    }

    fn run_in_process_call_count(&self) -> usize {
        self.run_in_process_calls.lock().unwrap().len()
    }
}

impl HostCallbacks for TestCallbacks {
    fn expand_graph_job(&self, job_id: &str) -> ppg3_core::Result<Vec<JobDef>> {
        self.expansions
            .lock()
            .unwrap()
            .get(job_id)
            .cloned()
            .ok_or_else(|| Error::Other(format!("no expansion configured for job {job_id:?}")))
    }

    fn run_in_process(&self, job_id: &str, key_doc: &Value) -> ppg3_core::Result<()> {
        self.run_in_process_calls.lock().unwrap().push((job_id.to_string(), key_doc.clone()));
        Ok(())
    }
}

// ============================================================ job builders

fn base_job(id: &str) -> JobDef {
    JobDef {
        id: id.to_string(),
        recipe: format!("recipe-{id}"),
        inputs: BTreeMap::new(),
        tools: BTreeMap::new(),
        runtime: serde_json::json!({"python_env": null, "preload": [], "shim": "mock"}),
        env: BTreeMap::new(),
        outputs_declared: vec!["out.txt".to_string()],
        resources: BTreeMap::new(),
        store_target: None,
        retain: Retain::Default,
        exec_template: ExecTemplate::Argv { argv: vec!["mock".to_string(), id.to_string()], allow_network: false },
        view: BTreeMap::new(),
        fixed_output: None,
        graph_job: false,
    }
}

/// `deps`: `(input name, parent job id)` pairs, wired as `InputRef::Job`.
fn dep_job(id: &str, deps: &[(&str, &str)]) -> JobDef {
    let mut j = base_job(id);
    for (name, parent) in deps {
        j.inputs.insert(name.to_string(), InputRef::Job { id: parent.to_string() });
    }
    j
}

fn parallelism(workers: u64, pools: &[(&str, u64)]) -> BTreeMap<String, u64> {
    let mut m = BTreeMap::new();
    m.insert("workers".to_string(), workers);
    for (k, v) in pools {
        m.insert(k.to_string(), *v);
    }
    m
}

fn fresh_storeset() -> (tempfile::TempDir, StoreSet) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open("main", dir.path(), false).unwrap();
    (dir, StoreSet::new(vec![store]))
}

// A -> B, C -> D diamond, each with a distinct declared output file.
fn diamond_jobs() -> Vec<JobDef> {
    let mut a = base_job("a");
    a.outputs_declared = vec!["a.txt".to_string()];
    let mut b = dep_job("b", &[("a", "a")]);
    b.outputs_declared = vec!["b.txt".to_string()];
    let mut c = dep_job("c", &[("a", "a")]);
    c.outputs_declared = vec!["c.txt".to_string()];
    let mut d = dep_job("d", &[("b", "b"), ("c", "c")]);
    d.outputs_declared = vec!["d.txt".to_string()];
    vec![a, b, c, d]
}

fn configure_diamond_outputs(exec: &MockExecutor) {
    exec.set_output("a", &[("a.txt", b"A")]);
    exec.set_output("b", &[("b.txt", b"B")]);
    exec.set_output("c", &[("c.txt", b"C")]);
    exec.set_output("d", &[("d.txt", b"D")]);
}

// =================================================================== tests

/// Oracle: every miss is built exactly once; build order respects the DAG
/// (parents strictly before children); the resulting report/job_entries are
/// complete.
#[test]
fn diamond_all_miss() {
    let (_dir, storeset) = fresh_storeset();
    let exec = MockExecutor::new();
    configure_diamond_outputs(&exec);
    let callbacks = TestCallbacks::new();
    let abort = AtomicBool::new(false);

    let report =
        scheduler::run(&storeset, &exec, diamond_jobs(), &callbacks, &parallelism(4, &[]), &abort).unwrap();

    let mut built = report.built.clone();
    built.sort();
    assert_eq!(built, vec!["a".to_string(), "b".to_string(), "c".to_string(), "d".to_string()]);
    assert!(report.hits.is_empty());
    assert!(report.failed.is_empty(), "unexpected failures: {:?}", report.failed);
    assert_eq!(report.job_entries.len(), 4);

    for id in ["a", "b", "c", "d"] {
        assert_eq!(exec.count(id), 1, "job {id} should run exactly once");
    }

    let order = exec.invocation_order();
    let pos = |id: &str| order.iter().position(|x| x == id).unwrap();
    assert!(pos("a") < pos("b"), "a must run before b: {order:?}");
    assert!(pos("a") < pos("c"), "a must run before c: {order:?}");
    assert!(pos("b") < pos("d"), "b must run before d: {order:?}");
    assert!(pos("c") < pos("d"), "c must run before d: {order:?}");
}

/// Oracle: rerunning the identical graph against the same store is entirely
/// cache hits — zero executor invocations, every job in `report.hits`, and
/// the (ik, oh) pairs are bit-for-bit identical to the first run.
#[test]
fn second_run_all_hits() {
    let (_dir, storeset) = fresh_storeset();
    let exec1 = MockExecutor::new();
    configure_diamond_outputs(&exec1);
    let callbacks = TestCallbacks::new();
    let abort = AtomicBool::new(false);

    let report1 =
        scheduler::run(&storeset, &exec1, diamond_jobs(), &callbacks, &parallelism(4, &[]), &abort).unwrap();
    assert_eq!(report1.built.len(), 4);

    // Deliberately unconfigured: any invocation at all is a bug.
    let exec2 = MockExecutor::new();
    let report2 =
        scheduler::run(&storeset, &exec2, diamond_jobs(), &callbacks, &parallelism(4, &[]), &abort).unwrap();

    assert_eq!(exec2.total_invocations(), 0, "second run must not execute anything");
    let mut hits = report2.hits.clone();
    hits.sort();
    assert_eq!(hits, vec!["a".to_string(), "b".to_string(), "c".to_string(), "d".to_string()]);
    assert!(report2.built.is_empty());
    assert!(report2.failed.is_empty());
    assert_eq!(report2.job_entries, report1.job_entries);
}

/// The constructive-trace core property (§8/§5 "shallow key, early cutoff"):
/// changing a job's recipe changes *its* ik (so it rebuilds), but if the
/// rebuild produces byte-identical output content its `oh` is unchanged,
/// so anything downstream that only depends on that `oh` never rebuilds.
#[test]
fn early_cutoff() {
    let (_dir, storeset) = fresh_storeset();
    let exec1 = MockExecutor::new();
    configure_diamond_outputs(&exec1);
    let callbacks = TestCallbacks::new();
    let abort = AtomicBool::new(false);

    let report1 =
        scheduler::run(&storeset, &exec1, diamond_jobs(), &callbacks, &parallelism(4, &[]), &abort).unwrap();

    let mut jobs2 = diamond_jobs();
    for j in jobs2.iter_mut() {
        if j.id == "b" {
            j.recipe = "recipe-b-v2".to_string();
        }
    }
    let exec2 = MockExecutor::new();
    // Same content as run 1's "b" -> same oh despite the new ik.
    exec2.set_output("b", &[("b.txt", b"B")]);

    let report2 =
        scheduler::run(&storeset, &exec2, jobs2, &callbacks, &parallelism(4, &[]), &abort).unwrap();

    assert_eq!(report2.built, vec!["b".to_string()]);
    let mut hits = report2.hits.clone();
    hits.sort();
    assert_eq!(hits, vec!["a".to_string(), "c".to_string(), "d".to_string()]);

    assert_eq!(exec2.count("b"), 1);
    assert_eq!(exec2.count("a"), 0);
    assert_eq!(exec2.count("c"), 0);
    assert_eq!(exec2.count("d"), 0);

    let (ik_b1, oh_b1) = report1.job_entries.get("b").unwrap();
    let (ik_b2, oh_b2) = report2.job_entries.get("b").unwrap();
    assert_ne!(ik_b1, ik_b2, "b's recipe changed, its ik must change");
    assert_eq!(oh_b1, oh_b2, "b's content did not change, its oh must be stable");

    // D never even re-derives a different key: identical (ik, oh).
    assert_eq!(report1.job_entries.get("d"), report2.job_entries.get("d"));
}

/// Oracle (§12.3 "random parameter flip sequences ... k-th distinct
/// configuration builds nothing the (k-2)-th already built"), minimal case:
/// P1, P2, P1 - the third run is all hits.
#[test]
fn parameter_flip_k_minus_2() {
    let (_dir, storeset) = fresh_storeset();
    let callbacks = TestCallbacks::new();
    let abort = AtomicBool::new(false);

    let make = |leaf_hash: &str| {
        let mut j = base_job("a");
        j.outputs_declared = vec!["out.txt".to_string()];
        j.inputs.insert("param".to_string(), InputRef::Leaf { hash: leaf_hash.to_string() });
        vec![j]
    };

    let p1 = "1".repeat(64);
    let p2 = "2".repeat(64);

    let exec1 = MockExecutor::new();
    exec1.set_output("a", &[("out.txt", b"content")]);
    let report1 =
        scheduler::run(&storeset, &exec1, make(&p1), &callbacks, &parallelism(2, &[]), &abort).unwrap();
    assert_eq!(report1.built, vec!["a".to_string()]);
    let entry1 = report1.job_entries.get("a").unwrap().clone();

    let exec2 = MockExecutor::new();
    exec2.set_output("a", &[("out.txt", b"content")]);
    let report2 =
        scheduler::run(&storeset, &exec2, make(&p2), &callbacks, &parallelism(2, &[]), &abort).unwrap();
    assert_eq!(report2.built, vec!["a".to_string()]);
    let entry2 = report2.job_entries.get("a").unwrap().clone();
    assert_ne!(entry1.0, entry2.0, "different param -> different ik");
    assert_eq!(entry1.1, entry2.1, "identical content -> identical oh");

    // Deliberately unconfigured: p1 again must be a pure hit.
    let exec3 = MockExecutor::new();
    let report3 =
        scheduler::run(&storeset, &exec3, make(&p1), &callbacks, &parallelism(2, &[]), &abort).unwrap();
    assert_eq!(exec3.total_invocations(), 0);
    assert_eq!(report3.hits, vec!["a".to_string()]);
    assert!(report3.built.is_empty());
    assert_eq!(report3.job_entries.get("a").unwrap(), &entry1);
}

/// `InputRef::JobSubset` early cutoff: a child depending on only one of a
/// parent's declared output files must stay a HIT when the parent rebuilds
/// with that one file unchanged (even though the parent's whole-output `oh`
/// changes) - while a sibling depending on the parent's *whole* output must
/// MISS.
#[test]
fn subset_early_cutoff() {
    let (_dir, storeset) = fresh_storeset();
    let callbacks = TestCallbacks::new();
    let abort = AtomicBool::new(false);

    let make_jobs = |p_recipe: &str| {
        let mut p = base_job("p");
        p.recipe = p_recipe.to_string();
        p.outputs_declared = vec!["a.txt".to_string(), "b.txt".to_string()];

        let mut child1 = base_job("child1");
        child1.outputs_declared = vec!["out.txt".to_string()];
        child1.inputs.insert(
            "sub".to_string(),
            InputRef::JobSubset { id: "p".to_string(), names: vec!["a.txt".to_string()] },
        );

        let mut child2 = base_job("child2");
        child2.outputs_declared = vec!["out.txt".to_string()];
        child2.inputs.insert("whole".to_string(), InputRef::Job { id: "p".to_string() });

        vec![p, child1, child2]
    };

    let exec1 = MockExecutor::new();
    exec1.set_output("p", &[("a.txt", b"A1"), ("b.txt", b"B1")]);
    exec1.set_output("child1", &[("out.txt", b"child1-out")]);
    exec1.set_output("child2", &[("out.txt", b"child2-out")]);
    let report1 = scheduler::run(&storeset, &exec1, make_jobs("recipe-p-v1"), &callbacks, &parallelism(4, &[]), &abort)
        .unwrap();
    assert_eq!(report1.built.len(), 3);

    // p rebuilds (new recipe); a.txt unchanged, b.txt changed.
    let exec2 = MockExecutor::new();
    exec2.set_output("p", &[("a.txt", b"A1"), ("b.txt", b"B2")]);
    exec2.set_output("child2", &[("out.txt", b"child2-out")]);
    let report2 = scheduler::run(&storeset, &exec2, make_jobs("recipe-p-v2"), &callbacks, &parallelism(4, &[]), &abort)
        .unwrap();

    let mut built = report2.built.clone();
    built.sort();
    assert_eq!(built, vec!["child2".to_string(), "p".to_string()]);
    assert_eq!(report2.hits, vec!["child1".to_string()]);

    assert_eq!(exec2.count("p"), 1);
    assert_eq!(exec2.count("child2"), 1);
    assert_eq!(exec2.count("child1"), 0, "child1 must stay a hit (its subset input is unchanged)");

    assert_eq!(report1.job_entries.get("child1"), report2.job_entries.get("child1"));
    assert_ne!(
        report1.job_entries.get("child2"),
        report2.job_entries.get("child2"),
        "child2 depends on p's whole output, which changed"
    );
}

/// Failure policy (CONTRACT.md "Scheduler"): a failed job fails its
/// transitive dependents (reported, never dispatched); independent
/// subgraphs continue unaffected.
#[test]
fn failure_propagation() {
    let (_dir, storeset) = fresh_storeset();
    let exec = MockExecutor::new();
    exec.set_output("a", &[("a.txt", b"A")]);
    exec.set_exit_code("b", 1);
    exec.set_output("d", &[("d.txt", b"D")]);

    let a = base_job("a");
    let b = base_job("b");
    let c = dep_job("c", &[("b", "b")]);
    let d = base_job("d");

    let callbacks = TestCallbacks::new();
    let abort = AtomicBool::new(false);
    let report =
        scheduler::run(&storeset, &exec, vec![a, b, c, d], &callbacks, &parallelism(4, &[]), &abort).unwrap();

    let mut built = report.built.clone();
    built.sort();
    assert_eq!(built, vec!["a".to_string(), "d".to_string()]);

    assert!(report.failed.contains_key("b"));
    assert!(
        report.failed["b"].contains("exited with code 1"),
        "expected exit-code message, got: {}",
        report.failed["b"]
    );

    assert!(report.failed.contains_key("c"));
    assert!(
        report.failed["c"].contains("upstream failed") && report.failed["c"].contains('b'),
        "expected an upstream-failure marker naming b, got: {}",
        report.failed["c"]
    );

    assert_eq!(exec.count("c"), 0, "c must never dispatch once its only parent failed");
    assert_eq!(exec.count("b"), 1);
}

/// Every pre-dispatch validation failure (cycle, unknown dep, duplicate id,
/// over-capacity resource request) must surface as `Error::Graph` from
/// `run()` itself, before the executor is ever touched.
#[test]
fn graph_error_before_dispatch() {
    let callbacks = TestCallbacks::new();
    let abort = AtomicBool::new(false);

    {
        let (_dir, storeset) = fresh_storeset();
        let exec = MockExecutor::new();
        let a = dep_job("a", &[("x", "b")]);
        let b = dep_job("b", &[("x", "a")]);
        let err =
            scheduler::run(&storeset, &exec, vec![a, b], &callbacks, &parallelism(2, &[]), &abort).unwrap_err();
        assert!(matches!(err, Error::Graph(_)), "cycle must be Error::Graph, got {err:?}");
        assert_eq!(exec.total_invocations(), 0);
    }

    {
        let (_dir, storeset) = fresh_storeset();
        let exec = MockExecutor::new();
        let a = dep_job("a", &[("x", "missing")]);
        let err = scheduler::run(&storeset, &exec, vec![a], &callbacks, &parallelism(2, &[]), &abort).unwrap_err();
        assert!(matches!(err, Error::Graph(_)), "unknown dep must be Error::Graph, got {err:?}");
        assert_eq!(exec.total_invocations(), 0);
    }

    {
        let (_dir, storeset) = fresh_storeset();
        let exec = MockExecutor::new();
        let a1 = base_job("a");
        let a2 = base_job("a");
        let err =
            scheduler::run(&storeset, &exec, vec![a1, a2], &callbacks, &parallelism(2, &[]), &abort).unwrap_err();
        assert!(matches!(err, Error::Graph(_)), "duplicate id must be Error::Graph, got {err:?}");
        assert_eq!(exec.total_invocations(), 0);
    }

    {
        let (_dir, storeset) = fresh_storeset();
        let exec = MockExecutor::new();
        let mut a = base_job("a");
        a.resources.insert("slots".to_string(), 10);
        let err = scheduler::run(&storeset, &exec, vec![a], &callbacks, &parallelism(2, &[("slots", 1)]), &abort)
            .unwrap_err();
        assert!(matches!(err, Error::Graph(_)), "over-capacity request must be Error::Graph, got {err:?}");
        assert_eq!(exec.total_invocations(), 0);
    }
}

/// Cooperative abort (§8.1 rule 2): once the flag flips, no further job is
/// dispatched, even though its dependency edges are already satisfied.
/// Actual (verified) behavior of this implementation: a job that never got
/// to run is simply absent from every `RunReport` list (not built, not hit,
/// not failed) - see STATUS.md.
#[test]
fn abort_stops_dispatch() {
    let (_dir, storeset) = fresh_storeset();
    let exec = MockExecutor::new();
    exec.set_output("a", &[("a.txt", b"A")]);
    let abort = Arc::new(AtomicBool::new(false));
    exec.set_abort_on("a", abort.clone());

    let a = base_job("a");
    let b = dep_job("b", &[("a", "a")]);
    let c = dep_job("c", &[("b", "b")]);

    let callbacks = TestCallbacks::new();
    let report =
        scheduler::run(&storeset, &exec, vec![a, b, c], &callbacks, &parallelism(4, &[]), abort.as_ref()).unwrap();

    assert_eq!(report.built, vec!["a".to_string()]);
    assert!(report.hits.is_empty());
    assert!(report.failed.is_empty(), "b/c must not be reported as failed either: {:?}", report.failed);
    assert_eq!(exec.count("a"), 1);
    assert_eq!(exec.count("b"), 0, "b must never dispatch after abort");
    assert_eq!(exec.count("c"), 0, "c must never dispatch after abort");
    assert_eq!(report.job_entries.len(), 1);
}

/// §7.4 dynamic graphs: a `graph_job` expands into new `JobDef`s (one
/// depending on an already-present job) within the *same* `run()` call, and
/// those expanded jobs are ordinary cache-hit/miss citizens on a rerun.
#[test]
fn graph_job_expansion() {
    let (_dir, storeset) = fresh_storeset();
    let abort = AtomicBool::new(false);

    let make = || {
        let mut base = base_job("base");
        base.outputs_declared = vec!["base.txt".to_string()];

        let mut g = base_job("g");
        g.exec_template = ExecTemplate::InProcess;
        g.graph_job = true;
        g.outputs_declared = vec![];

        let mut x = base_job("x");
        x.outputs_declared = vec!["x.txt".to_string()];

        let mut y = dep_job("y", &[("base", "base")]);
        y.outputs_declared = vec!["y.txt".to_string()];

        (vec![base, g], vec![x, y])
    };

    let (jobs1, expansion1) = make();
    let exec1 = MockExecutor::new();
    exec1.set_output("base", &[("base.txt", b"BASE")]);
    exec1.set_output("x", &[("x.txt", b"X")]);
    exec1.set_output("y", &[("y.txt", b"Y")]);
    let cb1 = TestCallbacks::new();
    cb1.set_expansion("g", expansion1);

    let report1 = scheduler::run(&storeset, &exec1, jobs1, &cb1, &parallelism(4, &[]), &abort).unwrap();

    let mut built1 = report1.built.clone();
    built1.sort();
    assert_eq!(built1, vec!["base".to_string(), "g".to_string(), "x".to_string(), "y".to_string()]);
    assert_eq!(report1.job_entries.len(), 3, "g itself produces no store entry");
    assert!(!report1.job_entries.contains_key("g"));

    let (jobs2, expansion2) = make();
    let exec2 = MockExecutor::new(); // deliberately unconfigured
    let cb2 = TestCallbacks::new();
    cb2.set_expansion("g", expansion2);
    let report2 = scheduler::run(&storeset, &exec2, jobs2, &cb2, &parallelism(4, &[]), &abort).unwrap();

    assert_eq!(exec2.total_invocations(), 0, "expanded jobs must be pure hits on rerun");
    let mut hits2 = report2.hits.clone();
    hits2.sort();
    assert_eq!(hits2, vec!["base".to_string(), "x".to_string(), "y".to_string()]);
    assert_eq!(report2.built, vec!["g".to_string()], "g always re-expands (it has no store entry to cache)");
}

/// Determinism enforcement (§9): "publish-time (always on): inputs/<ik>
/// exists with different oh -> hard error." Two independent jobs are
/// engineered to derive the *same* ik (identical recipe/inputs/tools/
/// runtime/env/outputs_declared - job id is not part of the key document)
/// but produce genuinely different output content; a `Barrier` forces both
/// to pass the scheduler's store lookup (a miss, since neither has
/// published yet) before either publishes, so this is a real race, not a
/// simulated one. Exactly one publishes; the other's publish() hits
/// `Error::DeterminismViolation` at the `inputs/<ik>` symlink-resolution
/// step and is reported as that job's `failed` entry.
#[test]
fn determinism_violation_surfaces() {
    let (_dir, storeset) = fresh_storeset();
    let exec = MockExecutor::new();

    let barrier = Arc::new(Barrier::new(2));
    exec.set_barrier("detA", barrier.clone());
    exec.set_barrier("detB", barrier);
    exec.set_output("detA", &[("out.txt", b"content-A")]);
    exec.set_output("detB", &[("out.txt", b"content-B")]);

    let mut a = base_job("detA");
    a.recipe = "shared-recipe".to_string();
    a.outputs_declared = vec!["out.txt".to_string()];
    let mut b = base_job("detB");
    b.recipe = "shared-recipe".to_string();
    b.outputs_declared = vec!["out.txt".to_string()];

    let callbacks = TestCallbacks::new();
    let abort = AtomicBool::new(false);
    // At least 2 real worker threads are required for the two independent,
    // barrier-synchronized jobs to race genuinely.
    let report =
        scheduler::run(&storeset, &exec, vec![a, b], &callbacks, &parallelism(2, &[]), &abort).unwrap();

    assert_eq!(exec.count("detA"), 1);
    assert_eq!(exec.count("detB"), 1);

    assert_eq!(report.built.len(), 1, "exactly one of detA/detB should publish: {report:?}");
    assert_eq!(report.failed.len(), 1, "exactly one of detA/detB should fail with a determinism violation: {report:?}");
    let loser = report.failed.keys().next().unwrap();
    assert!(["detA", "detB"].contains(&loser.as_str()));
    let msg = &report.failed[loser];
    assert!(
        msg.to_lowercase().contains("determinism"),
        "expected a determinism-violation message, got: {msg}"
    );
}

/// §7.6 fixed-output jobs: a correct declared `fixed_output` succeeds
/// normally; a wrong one fails the job with a message naming both hashes.
#[test]
fn fixed_output_verified() {
    let (_dir, storeset) = fresh_storeset();
    let callbacks = TestCallbacks::new();
    let abort = AtomicBool::new(false);

    // Compute the correct oh independently, exactly the way the store does
    // (hash the same content under its own scratch dir), so this assertion
    // doesn't depend on the test host's umask.
    let scratch = tempfile::tempdir().unwrap();
    std::fs::write(scratch.path().join("out.txt"), b"fixed-content").unwrap();
    let content = ppg3_core::store::hash_staging_content(scratch.path()).unwrap();
    let correct_oh = ppg3_core::manifest::output_hash(&content).unwrap();

    let exec = MockExecutor::new();
    exec.set_output("f_ok", &[("out.txt", b"fixed-content")]);
    exec.set_output("f_bad", &[("out.txt", b"fixed-content")]);

    let mut f_ok = base_job("f_ok");
    f_ok.outputs_declared = vec!["out.txt".to_string()];
    f_ok.fixed_output = Some(correct_oh.clone());

    let mut f_bad = base_job("f_bad");
    f_bad.outputs_declared = vec!["out.txt".to_string()];
    let wrong_oh = "0".repeat(64);
    f_bad.fixed_output = Some(wrong_oh.clone());

    let report =
        scheduler::run(&storeset, &exec, vec![f_ok, f_bad], &callbacks, &parallelism(2, &[]), &abort).unwrap();

    assert_eq!(report.built, vec!["f_ok".to_string()]);
    assert!(report.failed.contains_key("f_bad"));
    let msg = &report.failed["f_bad"];
    assert!(msg.contains(&wrong_oh), "expected declared oh in message: {msg}");
    assert!(msg.contains(&correct_oh), "expected produced oh in message: {msg}");
    assert_eq!(report.job_entries.get("f_ok").unwrap().1, correct_oh);
}

/// §6.3 item 2 (the loader layer): a plain `InProcess` (non-`graph_job`)
/// job invokes `HostCallbacks::run_in_process` with a §5-shaped key
/// document and produces no store entry; a downstream job that lists it via
/// `InputRef::Job` (a store input) is rejected at `run()`'s pre-dispatch
/// validation with `Error::Graph` (already unit-tested at the
/// `validate_and_index` level - this asserts the same thing through the
/// public `run()` entry point).
#[test]
fn inprocess_loader() {
    let (_dir, storeset) = fresh_storeset();
    let abort = AtomicBool::new(false);

    let mut loader = base_job("loader");
    loader.exec_template = ExecTemplate::InProcess;
    loader.outputs_declared = vec![];

    let cb = TestCallbacks::new();
    let exec = MockExecutor::new();
    let report =
        scheduler::run(&storeset, &exec, vec![loader.clone()], &cb, &parallelism(2, &[]), &abort).unwrap();

    assert_eq!(report.built, vec!["loader".to_string()]);
    assert!(!report.job_entries.contains_key("loader"), "InProcess jobs produce no store entry");
    assert_eq!(cb.run_in_process_call_count(), 1);
    let calls = cb.run_in_process_calls.lock().unwrap();
    assert_eq!(calls[0].0, "loader");
    let doc = &calls[0].1;
    for key in ["ppg3_key_version", "job_recipe", "inputs", "tools", "runtime", "env", "outputs_declared"] {
        assert!(doc.get(key).is_some(), "key doc missing {key:?}: {doc}");
    }
    assert_eq!(doc["ppg3_key_version"], serde_json::json!(1));
    drop(calls);
    assert_eq!(exec.total_invocations(), 0);

    let (_dir2, storeset2) = fresh_storeset();
    let mut consumer = dep_job("consumer", &[("x", "loader")]);
    consumer.outputs_declared = vec!["out.txt".to_string()];
    let cb2 = TestCallbacks::new();
    let exec2 = MockExecutor::new();
    let err = scheduler::run(&storeset2, &exec2, vec![loader, consumer], &cb2, &parallelism(2, &[]), &abort)
        .unwrap_err();
    assert!(matches!(err, Error::Graph(_)), "InProcess job as a store input must be Error::Graph, got {err:?}");
    assert_eq!(cb2.run_in_process_call_count(), 0, "validation must fail before any dispatch");
    assert_eq!(exec2.total_invocations(), 0);
}

/// Resource pools serialize concurrent access: 3 independent jobs each
/// requiring 1 of 1 available "slots" must never overlap.
#[test]
fn resource_serialization_pool_of_one_serializes() {
    let (_dir, storeset) = fresh_storeset();
    let exec = MockExecutor::new();
    for id in ["j1", "j2", "j3"] {
        exec.set_output(id, &[("out.txt", b"done")]);
        exec.set_delay(id, Duration::from_millis(60));
    }

    let mut jobs = Vec::new();
    for id in ["j1", "j2", "j3"] {
        let mut j = base_job(id);
        j.outputs_declared = vec!["out.txt".to_string()];
        j.resources.insert("slots".to_string(), 1);
        jobs.push(j);
    }

    let callbacks = TestCallbacks::new();
    let abort = AtomicBool::new(false);
    let report =
        scheduler::run(&storeset, &exec, jobs, &callbacks, &parallelism(3, &[("slots", 1)]), &abort).unwrap();

    let mut built = report.built.clone();
    built.sort();
    assert_eq!(built, vec!["j1".to_string(), "j2".to_string(), "j3".to_string()]);
    assert_eq!(exec.max_concurrency(), 1, "a pool of 1 slot must fully serialize execution");
}

/// Same 3 jobs, but the pool has enough capacity that they need not
/// serialize - just confirms all three still complete (not asserting a
/// specific concurrency level, per the WP4 assignment note).
#[test]
fn resource_serialization_pool_of_three_allows_all_to_run() {
    let (_dir, storeset) = fresh_storeset();
    let exec = MockExecutor::new();
    for id in ["k1", "k2", "k3"] {
        exec.set_output(id, &[("out.txt", b"done")]);
        exec.set_delay(id, Duration::from_millis(30));
    }

    let mut jobs = Vec::new();
    for id in ["k1", "k2", "k3"] {
        let mut j = base_job(id);
        j.outputs_declared = vec!["out.txt".to_string()];
        j.resources.insert("slots".to_string(), 1);
        jobs.push(j);
    }

    let callbacks = TestCallbacks::new();
    let abort = AtomicBool::new(false);
    let report =
        scheduler::run(&storeset, &exec, jobs, &callbacks, &parallelism(3, &[("slots", 3)]), &abort).unwrap();

    let mut built = report.built.clone();
    built.sort();
    assert_eq!(built, vec!["k1".to_string(), "k2".to_string(), "k3".to_string()]);
    assert_eq!(exec.total_invocations(), 3);
}
