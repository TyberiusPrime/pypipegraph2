//! cargo-afl fuzzer for the PPGEvaluator state machine,
//! focused on *resumed* projects, i.e. graphs evaluated multiple times where
//! the history is carried over between runs (and only ever produced by the
//! engine itself - we never synthesize history by hand, just like real runs).
//!
//! A fuzz input decodes to:
//!   - a small DAG (2..=7 nodes, Always/Output/Ephemeral),
//!   - 2..=4 consecutive runs, each with per-node flags:
//!       fail this job, toggle its output ("code changed" / "reverted"),
//!       delete its output ("file removed on disk"),
//!       remove the node from the graph for this run ("user edited graph"),
//!     plus an optional abort after the first k jobs (ctrl-c).
//!
//! It panics (= AFL crash) on:
//!   - deadlock: evaluator not finished, nothing ready, nothing running
//!     (the 'Evaluator is not finished ...' bug in python/pypipegraph2/runner.py)
//!   - any PPGEvaluatorError::InternalError / APIError from any event
//!   - new_history() errors / panics (e.g. the 'Job was skipped but history
//!     had no entry' assert)
//! EphemeralChangedOutput is tolerated: it is defined behavior (the engine
//! fails the job internally).
//!
//! Build / run (cargo-afl from the nix store, see flake):
//!   cd fuzz
//!   cargo-afl afl build --release
//!   cargo-afl afl fuzz -i seeds -o out target/release/fuzz_history
//! Reproduce a crash:
//!   target/release/fuzz_history < out/default/crashes/id...
//! (the binary reads stdin when run outside of AFL)

use pypipegraph2::{JobKind, PPGEvaluator, PPGEvaluatorError, StrategyForTesting};
use rustc_hash::FxHashMap;
use rustc_hash::FxHashSet;

struct Reader<'a> {
    d: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(d: &'a [u8]) -> Self {
        Reader { d, pos: 0 }
    }
    /// missing bytes decode as 0 == 'nothing special happens'
    fn next(&mut self) -> u8 {
        let b = *self.d.get(self.pos).unwrap_or(&0);
        self.pos += 1;
        b
    }
}

#[derive(Debug, Clone, Copy)]
struct NodeRunFlags {
    fail: bool,
    toggle_output: bool,
    delete_output: bool,
    absent: bool,
}

#[derive(Debug, Clone)]
struct RunSpec {
    flags: Vec<NodeRunFlags>,
    /// Some((k, fail_running)): run only the first k ready jobs, then -
    /// mirroring runner.py's abort - optionally fail the next started job
    /// and abort_remaining() the rest.
    abort: Option<(usize, bool)>,
}

#[derive(Debug)]
struct Scenario {
    kinds: Vec<JobKind>,
    edges: Vec<(usize, usize)>, // (upstream, downstream), upstream < downstream
    runs: Vec<RunSpec>,
}

fn decode(data: &[u8]) -> Scenario {
    let mut r = Reader::new(data);
    let n = 2 + (r.next() % 6) as usize; // 2..=7 nodes
    let kinds: Vec<JobKind> = (0..n)
        .map(|_| match r.next() % 3 {
            0 => JobKind::Always,
            1 => JobKind::Output,
            _ => JobKind::Ephemeral,
        })
        .collect();
    let mut edges = Vec::new();
    for up in 0..n {
        for down in (up + 1)..n {
            if r.next() & 1 == 1 {
                edges.push((up, down));
            }
        }
    }
    let run_count = 2 + (r.next() % 3) as usize; // 2..=4 runs - history bugs need >= 2
    let mut runs = Vec::new();
    for run_no in 0..run_count {
        let flags = (0..n)
            .map(|_| {
                let b = r.next();
                NodeRunFlags {
                    fail: b & 1 != 0,
                    toggle_output: b & 2 != 0,
                    delete_output: b & 4 != 0,
                    // the last run always sees the full graph, so that
                    // 'job removed and later restored' is reachable
                    absent: b & 8 != 0 && run_no != run_count - 1,
                }
            })
            .collect();
        let b = r.next();
        let abort = if b & 0x80 != 0 {
            Some(((b & 0x3f) as usize, b & 0x40 != 0))
        } else {
            None
        };
        runs.push(RunSpec { flags, abort });
    }
    Scenario { kinds, edges, runs }
}

fn job_name(idx: usize) -> String {
    format!("N{}", idx)
}

fn run_scenario(s: &Scenario) {
    let n = s.kinds.len();
    let mut history: FxHashMap<String, String> = FxHashMap::default();
    let mut done: FxHashSet<String> = FxHashSet::default(); // 'outputs on disk'
    let mut output_version = vec![0u8; n]; // toggled by 'code changed'

    for run in &s.runs {
        for idx in 0..n {
            if run.flags[idx].toggle_output {
                output_version[idx] ^= 1;
            }
            if run.flags[idx].delete_output {
                done.remove(&job_name(idx));
            }
        }
        let present: Vec<bool> = run.flags.iter().map(|f| !f.absent).collect();
        if !present.iter().any(|p| *p) {
            continue;
        }

        let strat = StrategyForTesting::new();
        for k in done.iter() {
            strat.already_done.borrow_mut().insert(k.clone());
        }
        let already_done = std::rc::Rc::clone(&strat.already_done);
        let mut g = PPGEvaluator::new_with_history(history.clone(), strat);
        for idx in 0..n {
            if present[idx] {
                g.add_node(&job_name(idx), s.kinds[idx]);
            }
        }
        for (up, down) in s.edges.iter() {
            if present[*up] && present[*down] {
                g.depends_on(&job_name(*down), &job_name(*up));
            }
        }

        if let Err(e) = g.event_startup() {
            panic!(
                "event_startup failed: {:?}\nscenario: {:?}\n{}",
                e,
                s,
                g.debug_()
            );
        }

        let mut executed = 0usize;
        let mut aborted = false;
        let mut safety = 0usize;
        while !g.is_finished() {
            safety += 1;
            if safety > 10_000 {
                panic!("runaway loop\nscenario: {:?}\n{}", s, g.debug_());
            }
            let ready = g.query_ready_to_run();
            if ready.is_empty() {
                // nothing ready, nothing running (we are strictly serial),
                // not finished: this is exactly the state runner.py aborts on
                // with 'a bug in the state machine. No way forward'.
                panic!(
                    "DEADLOCK: evaluator not finished, no jobs ready, none running.\nscenario: {:?}\n{}",
                    s,
                    g.debug_()
                );
            }
            let mut ready: Vec<String> = ready.into_iter().collect();
            ready.sort(); // determinism (AFL stability)
            for job_id in ready {
                if let Some((k, fail_running)) = run.abort {
                    if executed >= k {
                        if fail_running {
                            // runner.py aborts running jobs by failing them...
                            g.event_now_running(&job_id)
                                .unwrap_or_else(|e| panic!("now_running: {:?}\n{:?}", e, s));
                            if let Err(e) = g.event_job_finished_failure(&job_id) {
                                panic!("failure event: {:?}\nscenario: {:?}", e, s);
                            }
                        }
                        // ... and event_abort()s the rest.
                        if let Err(e) = g.abort_remaining() {
                            panic!("abort_remaining: {:?}\nscenario: {:?}", e, s);
                        }
                        aborted = true;
                        break;
                    }
                }
                let idx: usize = job_id[1..].parse().unwrap();
                g.event_now_running(&job_id)
                    .unwrap_or_else(|e| panic!("now_running: {:?}\n{:?}", e, s));
                executed += 1;
                if run.flags[idx].fail {
                    if let Err(e) = g.event_job_finished_failure(&job_id) {
                        panic!("failure event: {:?}\nscenario: {:?}", e, s);
                    }
                } else {
                    let output = format!("out_{}_v{}", job_id, output_version[idx]);
                    match g.event_job_finished_success(&job_id, output) {
                        Ok(()) => {
                            already_done.borrow_mut().insert(job_id.clone());
                        }
                        Err(PPGEvaluatorError::EphemeralChangedOutput { .. }) => {
                            // defined behavior: engine fails the job internally
                        }
                        Err(e) => {
                            panic!("success event: {:?}\nscenario: {:?}\n{}", e, s, g.debug_())
                        }
                    }
                }
            }
            if aborted {
                break;
            }
            for c in g.query_ready_for_cleanup() {
                g.event_job_cleanup_done(&c)
                    .unwrap_or_else(|e| panic!("cleanup: {:?}\n{:?}", e, s));
            }
        }

        // mirrors runner.py: new_history() is taken after aborts as well
        history = match g.new_history() {
            Ok(h) => h,
            Err(e) => panic!("new_history: {:?}\nscenario: {:?}\n{}", e, s, g.debug_()),
        };
        for k in already_done.take().into_iter() {
            done.insert(k);
        }
    }
}

fn main() {
    afl::fuzz!(|data: &[u8]| {
        let scenario = decode(data);
        if std::env::var_os("PPG2_FUZZ_DUMP").is_some() {
            eprintln!("scenario: {:?}", scenario);
        }
        run_scenario(&scenario);
    });
}
