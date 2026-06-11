//! Second cargo-afl target: interleaving / concurrency fuzzer for the
//! PPGEvaluator state machine.
//!
//! Where fuzz_history executes strictly serially (start job -> finish job,
//! one at a time), this target mirrors what python/pypipegraph2/runner.py
//! actually does: a thread pool keeps *several* jobs in `Running`
//! simultaneously, they finish in arbitrary order, and cleanups happen at
//! arbitrary points in between. The fuzz input drives that schedule
//! explicitly via an action stream.
//!
//! On top of the interleaving it adds scenario dimensions fuzz_history
//! does not have:
//!   - per-run job *kind* changes (user turns an Output into an Ephemeral
//!     between sessions - legal, each run is a fresh evaluator),
//!   - per-run *edge* changes (user changed dependencies),
//!   - multi-output job names (`a:::b:::c`) whose part-composition changes
//!     between runs, driving the rename-matching machinery in new_history()
//!     (filter_if_renamed / try_finding_renamed_multi_output_job),
//!   - mid-run reconsider_all_jobs() calls (exposed to python),
//!   - cleanup actually removes the ephemeral's output from "disk"
//!     (fuzz_history leaves it present).
//!
//! And it checks *semantic* oracles, not just crashes:
//!   - DEADLOCK: not finished, nothing ready, nothing running (the
//!     runner.py "no way forward" condition),
//!   - any InternalError / APIError from any event call,
//!   - next_job_ready_to_run() consistent with query_ready_to_run(),
//!   - the run order is topological (engine's verify_order_was_topological),
//!   - CONVERGENCE: after a clean, fully successful run, an unchanged
//!     re-run must not run any Output job, and every Ephemeral it does run
//!     must have a direct downstream that also ran,
//!   - MISSING OUTPUT: in a completed run without any failure, every
//!     present Output job whose output was missing on "disk" at run start
//!     must actually have run.
//! EphemeralChangedOutput is tolerated (defined behavior; counts as a
//! failure for the oracles, and the output *is* considered written).
//!
//! Input encoding (missing bytes decode as 0 = "nothing special"):
//!
//!   byte 0            node count        2 + (b % 6)        -> 2..=7
//!   n bytes           node spec         kind = b % 3 (Always/Output/Ephemeral)
//!                                       multi-output names if b & 8
//!   n*(n-1)/2 bytes   base edges        b & 1, pair (up, down), up < down
//!   1 byte            run count         2 + (b % 3)        -> 2..=4
//!   per run:
//!     n bytes         node flags        bit0 fail, bit1 toggle output,
//!                                       bit2 delete output,
//!                                       bit3 absent (ignored on last run),
//!                                       bits4-5 kind override
//!                                         (0 keep, 1 Always, 2 Output, 3 Ephemeral),
//!                                       bits6-7 name variant (multi nodes only)
//!     ceil(pairs/8)   edge xor mask     flips base edges for this run
//!   rest              action stream, consumed across all runs as needed:
//!     op = b >> 4, sel = b & 0x0f
//!       0..=5   start ready job   sorted_ready[sel % len]   (none ready -> finish)
//!       6..=11  finish running    running[sel % len]        (none running -> start)
//!       12..=13 one cleanup       sorted_pending[sel % len] (none -> start)
//!       14      reconsider_all_jobs()  (capped at 8 per run, then -> start)
//!       15      sel == 15: abort (fail all running, abort_remaining, like
//!               runner.py on ctrl-c); otherwise -> finish
//!
//! Build / run (cargo-afl from the nix store, see fuzz/README.md):
//!   cd fuzz
//!   cargo-afl afl build --release
//!   cargo-afl afl fuzz -i seeds_interleave -o out_interleave \
//!       target/release/fuzz_interleave
//! Reproduce a crash (binary reads stdin outside of AFL):
//!   target/release/fuzz_interleave < out_interleave/default/crashes/id...
//!   PPG2_FUZZ_DUMP=1 ... to dump the decoded scenario.

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
    kind_override: Option<JobKind>,
    name_variant: u8, // 0..=3, only meaningful for multi nodes
}

#[derive(Debug, Clone)]
struct RunSpec {
    flags: Vec<NodeRunFlags>,
    /// per-run edge set: base edges xor'ed with this run's mask
    edges: Vec<(usize, usize)>, // (upstream, downstream), upstream < downstream
}

#[derive(Debug)]
struct Scenario {
    kinds: Vec<JobKind>,
    multi: Vec<bool>,
    runs: Vec<RunSpec>,
    actions: Vec<u8>,
}

fn decode(data: &[u8]) -> Scenario {
    let mut r = Reader::new(data);
    let n = 2 + (r.next() % 6) as usize; // 2..=7 nodes
    let mut kinds = Vec::with_capacity(n);
    let mut multi = Vec::with_capacity(n);
    for _ in 0..n {
        let b = r.next();
        kinds.push(match b % 3 {
            0 => JobKind::Always,
            1 => JobKind::Output,
            _ => JobKind::Ephemeral,
        });
        multi.push(b & 8 != 0);
    }
    let pairs: Vec<(usize, usize)> = (0..n)
        .flat_map(|up| ((up + 1)..n).map(move |down| (up, down)))
        .collect();
    let base_edges: Vec<bool> = pairs.iter().map(|_| r.next() & 1 == 1).collect();
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
                    kind_override: match (b >> 4) & 3 {
                        0 => None,
                        1 => Some(JobKind::Always),
                        2 => Some(JobKind::Output),
                        _ => Some(JobKind::Ephemeral),
                    },
                    name_variant: (b >> 6) & 3,
                }
            })
            .collect();
        let mask_bytes = pairs.len().div_ceil(8);
        let mut edges = Vec::new();
        let mut mask = Vec::with_capacity(mask_bytes);
        for _ in 0..mask_bytes {
            mask.push(r.next());
        }
        for (k, (up, down)) in pairs.iter().enumerate() {
            let flipped = mask[k / 8] & (1 << (k % 8)) != 0;
            if base_edges[k] ^ flipped {
                edges.push((*up, *down));
            }
        }
        runs.push(RunSpec { flags, edges });
    }
    let actions = data.get(r.pos..).unwrap_or(&[]).to_vec();
    Scenario {
        kinds,
        multi,
        runs,
        actions,
    }
}

/// job id of node `idx` in a given run. Multi-output jobs ("a:::b" names)
/// change their part composition with the name variant, modeling renamed /
/// re-grouped MultiFileGeneratingJobs.
fn job_name(idx: usize, is_multi: bool, variant: u8) -> String {
    if !is_multi {
        format!("N{}", idx)
    } else {
        match variant {
            0 => format!("N{i}_a:::N{i}_b:::N{i}_c", i = idx),
            1 => format!("N{i}_a:::N{i}_b", i = idx),
            2 => format!("N{i}_b:::N{i}_c", i = idx),
            _ => format!("N{}_z", idx),
        }
    }
}

/// what one run looked like, for comparing run R against run R-1
/// (the convergence oracle only applies to unchanged graphs)
#[derive(PartialEq, Eq, Clone, Debug, Default)]
struct GraphSig {
    present: Vec<bool>,
    kinds: Vec<JobKind>,
    names: Vec<String>,
    edges: Vec<(usize, usize)>,
}

struct RunOutcome {
    sig: GraphSig,
    aborted: bool,
    any_failure: bool, // fail events or EphemeralChangedOutput
}

fn run_scenario(s: &Scenario) {
    let n = s.kinds.len();
    let mut history: FxHashMap<String, String> = FxHashMap::default();
    let mut done: FxHashSet<String> = FxHashSet::default(); // 'outputs on disk'
    let mut output_version = vec![0u8; n]; // toggled by 'code changed'
    let mut action_pos = 0usize;
    let mut next_action = || {
        let b = *s.actions.get(action_pos).unwrap_or(&0);
        action_pos += 1;
        b
    };
    let mut prev_outcome: Option<RunOutcome> = None;

    for run in &s.runs {
        let present: Vec<bool> = run.flags.iter().map(|f| !f.absent).collect();
        let kinds: Vec<JobKind> = (0..n)
            .map(|idx| run.flags[idx].kind_override.unwrap_or(s.kinds[idx]))
            .collect();
        let names: Vec<String> = (0..n)
            .map(|idx| job_name(idx, s.multi[idx], run.flags[idx].name_variant))
            .collect();
        let name_to_idx: FxHashMap<String, usize> = names
            .iter()
            .enumerate()
            .map(|(idx, name)| (name.clone(), idx))
            .collect();
        let edges: Vec<(usize, usize)> = run
            .edges
            .iter()
            .filter(|(up, down)| present[*up] && present[*down])
            .copied()
            .collect();
        let sig = GraphSig {
            present: present.clone(),
            kinds: kinds.clone(),
            names: names.clone(),
            edges: edges.clone(),
        };

        for idx in 0..n {
            if run.flags[idx].toggle_output {
                output_version[idx] ^= 1;
            }
            if run.flags[idx].delete_output {
                done.remove(&names[idx]);
            }
        }
        if !present.iter().any(|p| *p) {
            prev_outcome = None; // graph changed in a way we don't track
            continue;
        }

        let missing_at_start: Vec<usize> = (0..n)
            .filter(|&idx| {
                present[idx] && kinds[idx] == JobKind::Output && !done.contains(&names[idx])
            })
            .collect();

        let strat = StrategyForTesting::new();
        for k in done.iter() {
            strat.already_done.borrow_mut().insert(k.clone());
        }
        let on_disk = std::rc::Rc::clone(&strat.already_done);
        let mut g = PPGEvaluator::new_with_history(history.clone(), strat);
        for idx in 0..n {
            if present[idx] {
                g.add_node(&names[idx], kinds[idx]);
            }
        }
        for (up, down) in edges.iter() {
            g.depends_on(&names[*down], &names[*up]);
        }

        if let Err(e) = g.event_startup() {
            panic!(
                "event_startup failed: {:?}\nscenario: {:?}\n{}",
                e,
                s,
                g.debug_()
            );
        }

        let mut running: Vec<String> = Vec::new();
        let mut run_order: Vec<String> = Vec::new();
        let mut executed: FxHashSet<usize> = FxHashSet::default();
        let mut any_failure = false;
        let mut aborted = false;
        let mut reconsiders_left = 8usize;
        let mut safety = 0usize;

        #[derive(Clone, Copy)]
        enum Op {
            Start,
            Finish,
            Cleanup,
            Reconsider,
            Abort,
        }

        'run: while !g.is_finished() {
            safety += 1;
            if safety > 10_000 {
                panic!("runaway loop\nscenario: {:?}\n{}", s, g.debug_());
            }
            let b = next_action();
            let sel = (b & 0x0f) as usize;
            let mut op = match b >> 4 {
                0..=5 => Op::Start,
                6..=11 => Op::Finish,
                12..=13 => Op::Cleanup,
                14 => Op::Reconsider,
                _ => {
                    if sel == 15 {
                        Op::Abort
                    } else {
                        Op::Finish
                    }
                }
            };
            // fallback chain: every iteration must either advance the engine,
            // abort, or prove the deadlock. Reconsider is capped per run so a
            // hostile action stream can't spin forever.
            loop {
                match op {
                    Op::Start => {
                        let mut ready: Vec<String> = g.query_ready_to_run().into_iter().collect();
                        // consistency oracle: next_job_ready_to_run must agree
                        // with query_ready_to_run
                        let nx = g.next_job_ready_to_run();
                        if nx.is_some() != !ready.is_empty()
                            || nx.as_ref().is_some_and(|j| !ready.contains(j))
                        {
                            panic!(
                                "next_job_ready_to_run ({:?}) inconsistent with query_ready_to_run ({:?})\nscenario: {:?}\n{}",
                                nx, ready, s, g.debug_()
                            );
                        }
                        if ready.is_empty() {
                            if running.is_empty() {
                                // nothing ready, nothing running, not finished:
                                // exactly the state runner.py aborts on with
                                // 'a bug in the state machine. No way forward'.
                                panic!(
                                    "DEADLOCK: evaluator not finished, no jobs ready, none running.\nscenario: {:?}\n{}",
                                    s,
                                    g.debug_()
                                );
                            }
                            op = Op::Finish;
                            continue;
                        }
                        ready.sort(); // determinism (AFL stability)
                        let job_id = ready[sel % ready.len()].clone();
                        g.event_now_running(&job_id).unwrap_or_else(|e| {
                            panic!("now_running: {:?}\n{:?}\n{}", e, s, g.debug_())
                        });
                        executed.insert(name_to_idx[&job_id]);
                        run_order.push(job_id.clone());
                        running.push(job_id);
                        break;
                    }
                    Op::Finish => {
                        if running.is_empty() {
                            op = Op::Start;
                            continue;
                        }
                        let job_id = running.remove(sel % running.len());
                        let idx = name_to_idx[&job_id];
                        if run.flags[idx].fail {
                            any_failure = true;
                            if let Err(e) = g.event_job_finished_failure(&job_id) {
                                panic!("failure event: {:?}\nscenario: {:?}\n{}", e, s, g.debug_());
                            }
                        } else {
                            let output = format!("out_{}_v{}", job_id, output_version[idx]);
                            match g.event_job_finished_success(&job_id, output) {
                                Ok(()) => {
                                    on_disk.borrow_mut().insert(job_id.clone());
                                }
                                Err(PPGEvaluatorError::EphemeralChangedOutput { .. }) => {
                                    // defined behavior: the engine fails the job
                                    // internally - but the job *did* write its
                                    // output before the engine noticed.
                                    any_failure = true;
                                    on_disk.borrow_mut().insert(job_id.clone());
                                }
                                Err(e) => {
                                    panic!(
                                        "success event: {:?}\nscenario: {:?}\n{}",
                                        e,
                                        s,
                                        g.debug_()
                                    )
                                }
                            }
                        }
                        break;
                    }
                    Op::Cleanup => {
                        let mut pending: Vec<String> =
                            g.query_ready_for_cleanup().into_iter().collect();
                        if pending.is_empty() {
                            op = Op::Start;
                            continue;
                        }
                        pending.sort();
                        let job_id = pending[sel % pending.len()].clone();
                        g.event_job_cleanup_done(&job_id).unwrap_or_else(|e| {
                            panic!("cleanup: {:?}\n{:?}\n{}", e, s, g.debug_())
                        });
                        // the ephemeral's output is now gone from disk
                        on_disk.borrow_mut().remove(&job_id);
                        break;
                    }
                    Op::Reconsider => {
                        if reconsiders_left == 0 {
                            op = Op::Start;
                            continue;
                        }
                        reconsiders_left -= 1;
                        if let Err(e) = g.reconsider_all_jobs() {
                            panic!(
                                "reconsider_all_jobs: {:?}\nscenario: {:?}\n{}",
                                e,
                                s,
                                g.debug_()
                            );
                        }
                        break;
                    }
                    Op::Abort => {
                        // mirrors runner.py on ctrl-c: running jobs are
                        // mock-failed, then event_abort()
                        any_failure = any_failure || !running.is_empty();
                        for job_id in running.drain(..) {
                            if let Err(e) = g.event_job_finished_failure(&job_id) {
                                panic!(
                                    "abort-failure event: {:?}\nscenario: {:?}\n{}",
                                    e,
                                    s,
                                    g.debug_()
                                );
                            }
                        }
                        if let Err(e) = g.abort_remaining() {
                            panic!("abort_remaining: {:?}\nscenario: {:?}", e, s);
                        }
                        aborted = true;
                        break 'run;
                    }
                }
            }
        }

        if !aborted {
            // drain outstanding cleanups, like the runner's scheduler loop
            // does before it can observe 'finished'
            loop {
                let mut pending: Vec<String> = g.query_ready_for_cleanup().into_iter().collect();
                if pending.is_empty() {
                    break;
                }
                pending.sort();
                for job_id in pending {
                    g.event_job_cleanup_done(&job_id)
                        .unwrap_or_else(|e| panic!("cleanup: {:?}\n{:?}", e, s));
                    on_disk.borrow_mut().remove(&job_id);
                }
            }
        }

        // order oracle: started jobs must respect the dependency order
        if !g.verify_order_was_topological(&run_order) {
            panic!(
                "run order was not topological: {:?}\nscenario: {:?}\n{}",
                run_order,
                s,
                g.debug_()
            );
        }

        // missing-output oracle: in a completed run without any failure,
        // every present Output job whose output was missing on disk at run
        // start must have run.
        if !aborted && !any_failure {
            for idx in missing_at_start {
                if !executed.contains(&idx) {
                    panic!(
                        "MISSING OUTPUT not rebuilt: {} (output absent at run start, run completed without failures, job never ran)\nscenario: {:?}\n{}",
                        names[idx],
                        s,
                        g.debug_()
                    );
                }
            }
        }

        // convergence oracle: an unchanged re-run after a clean, fully
        // successful run must not run any Output job...
        let graph_unchanged = prev_outcome.as_ref().is_some_and(|p| {
            !p.aborted
                && !p.any_failure
                && p.sig == sig
                && run
                    .flags
                    .iter()
                    .enumerate()
                    .all(|(idx, f)| !present[idx] || (!f.toggle_output && !f.delete_output))
        });
        if graph_unchanged {
            for &idx in executed.iter() {
                if kinds[idx] == JobKind::Output {
                    panic!(
                        "CONVERGENCE violated: Output job {} re-ran in an unchanged re-run after a clean successful run\nscenario: {:?}\n{}",
                        names[idx],
                        s,
                        g.debug_()
                    );
                }
            }
            // ...and every Ephemeral it does run must have been needed,
            // i.e. have a direct downstream that also ran.
            if !aborted && !any_failure {
                for &idx in executed.iter() {
                    if kinds[idx] == JobKind::Ephemeral
                        && !edges
                            .iter()
                            .any(|(up, down)| *up == idx && executed.contains(down))
                    {
                        panic!(
                            "CONVERGENCE violated: Ephemeral {} ran in an unchanged re-run but no direct downstream ran\nscenario: {:?}\n{}",
                            names[idx],
                            s,
                            g.debug_()
                        );
                    }
                }
            }
        }

        // mirrors runner.py: new_history() is taken after aborts as well
        history = match g.new_history() {
            Ok(h) => h,
            Err(e) => panic!("new_history: {:?}\nscenario: {:?}\n{}", e, s, g.debug_()),
        };
        done = on_disk.take();
        prev_outcome = Some(RunOutcome {
            sig,
            aborted,
            any_failure,
        });
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
