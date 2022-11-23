#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
use log::{debug, error, info, warn};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::types::{PyDict, PyFunction};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::thread::current; // Use log crate when building application

use petgraph::{graphmap::GraphMap, Directed, Direction};
use thiserror::Error;

use pyo3::prelude::*;

#[cfg(test)]
mod tests;

#[derive(Error, Debug)]
pub enum PPGEvaluatorError {
    #[error("API error. You're holding it wrong")]
    APIError(String),
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum JobKind {
    Always, // run always
    Output, //run if invalidated or output-not-present
    Ephemeral, // run if invalidated, or downstream jobs require them.
            // cleanups ain't jobs. Because they would not trigger ephemerals -
            // and that way lies complexity madness. Probably much easier to just have a callback
            // when a jobs' downstreams have all been finished
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum InvalidationStatus {
    //Irrelevant, // for JobKind::Always
    Invalidated,
    Validated,
    Unknown,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Finished {
    Sucessful,
    Failure,
    Skipped,
    UpstreamFailure,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum DownstreamNeedsMe {
    Yes,
    No,
    Unknown,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum RunStatus {
    Undetermined,
    RunThis,
    ReadyToRun,
    Running,
    Ran(Finished),
}

#[derive(Copy, Clone, Debug)]
enum UnfinishedDownstreams {
    Count(u32),
    Unknown,
    CleanupAlreadyHandled,
}

#[derive(Clone, Debug)]
struct Job {
    job_id: String,
    kind: JobKind,
    ran: RunStatus,
    history_output: Option<String>,
    unfinished_downstreams: UnfinishedDownstreams, // -1 = not calculated, -2: cleanup handled
}

enum StartStatus {
    NotStarted,
    Running,
    Finished,
}

pub trait PPGEvaluatorStrategy {
    fn output_already_present(&self, query: &str) -> bool;
    fn is_history_altered(
        &self,
        job_id_uptream: &str,
        job_id_downstream: &str,
        last_recorded_value: &str,
        current_value: &str,
    ) -> bool;
}

pub struct PPGEvaluator<T: PPGEvaluatorStrategy> {
    dag: GraphMap<u32, InvalidationStatus, Directed>,
    jobs: Vec<(String, Job)>,
    node_to_index: HashMap<String, u32>,
    history: HashMap<String, String>,
    strategy: T,
    already_started: StartStatus,
    jobs_ready_to_run: HashSet<String>,
    topo: Option<Vec<u32>>,
}

#[derive(Clone)]
pub struct StrategyForTesting {
    pub already_done: Rc<RefCell<HashSet<String>>>,
}

impl StrategyForTesting {
    fn new() -> Self {
        StrategyForTesting {
            already_done: Rc::new(RefCell::new(HashSet::new())),
        }
    }
}

impl PPGEvaluatorStrategy for StrategyForTesting {
    fn output_already_present(&self, query: &str) -> bool {
        self.already_done.borrow().contains(query)
    }

    fn is_history_altered(
        &self,
        job_id_upstream: &str,
        job_id_downstream: &str,
        last_recorded_value: &str,
        current_value: &str,
    ) -> bool {
        last_recorded_value != current_value
    }
}

impl<T: PPGEvaluatorStrategy> PPGEvaluator<T> {
    pub fn new(strategy: T) -> Self {
        // todo: get rid of the box for the caller at least?
        Self::new_with_history(HashMap::new(), strategy)
    }

    #[allow(clippy::type_complexity)]
    pub fn new_with_history(history: HashMap<String, String>, strategy: T) -> Self {
        let mut res = PPGEvaluator {
            dag: GraphMap::new(),
            jobs: Vec::new(),
            node_to_index: HashMap::new(),
            history,
            strategy,
            already_started: StartStatus::NotStarted,
            jobs_ready_to_run: HashSet::new(),
            topo: None,
        };
        res.add_node("<ROOT>", JobKind::Always);
        res
    }

    pub fn add_node(&mut self, job_id: &str, kind: JobKind) {
        assert_ne!(job_id, "");
        let job = Job {
            job_id: job_id.to_string(),
            kind,
            ran: match kind {
                JobKind::Always => RunStatus::RunThis,
                _ => RunStatus::Undetermined,
            },
            history_output: None,
            unfinished_downstreams: UnfinishedDownstreams::Unknown,
        };
        let idx = self.jobs.len() as u32;
        if self.node_to_index.insert(job_id.to_string(), idx).is_some() {
            panic!("Can not add a node twice to the evaluator");
        };
        self.jobs.push((job_id.to_string(), job));
        self.dag.add_node(idx);
    }

    pub fn depends_on(&mut self, downstream: &str, upstream: &str) {
        let downstream_id = self
            .node_to_index
            .get(downstream)
            .expect("Invalid downstream id");
        let upstream_id = self
            .node_to_index
            .get(upstream)
            .expect("Invalid upstream id");
        assert_ne!(downstream_id, upstream_id, "can't depend on self");
        self.dag
            .add_edge(*upstream_id, *downstream_id, InvalidationStatus::Unknown);
    }

    pub fn is_finished(&self) -> bool {
        for (job_id, job) in self.jobs.iter() {
            match job.ran {
                RunStatus::Ran(_) => {}
                _ => return false,
            }
        }
        true
    }

    fn fill_in_unfinished_downstream_counts(&mut self) {
        for (a, _, _) in self.dag.all_edges() {
            self.jobs[a as usize].1.unfinished_downstreams =
                match self.jobs[a as usize].1.unfinished_downstreams {
                    UnfinishedDownstreams::Unknown => UnfinishedDownstreams::Count(1),
                    UnfinishedDownstreams::Count(ii) => UnfinishedDownstreams::Count(ii + 1),
                    _ => panic!(),
                };
        }
    }

    pub fn event_startup(&mut self) -> Result<(), PPGEvaluatorError> {
        // this is not particulary fast.
        self.topo =
            Some(petgraph::algo::toposort(petgraph::visit::Reversed(&self.dag), None).unwrap());
        match self.already_started {
            StartStatus::Running | StartStatus::Finished => {
                return Err(PPGEvaluatorError::APIError("Can't start twice".to_string()));
            }
            _ => {}
        };
        self.already_started = StartStatus::Running;
        let mut root_edges = Vec::new();
        for a_root in self.dag.nodes().filter(|nid| {
            self.dag
                .edges_directed(*nid, Direction::Incoming)
                .next()
                .is_none()
        }) {
            if a_root != 0u32 {
                root_edges.push(a_root);
            }
        }
        for a_root in root_edges.into_iter() {
            self.dag.add_edge(0u32, a_root, InvalidationStatus::Unknown);
        }

        self.update_ephemerals_needed();
        self.jobs[0].1.ran = RunStatus::Running; //otherwise job_finished has the approriate fit
        self.jobs[0].1.history_output = Some("".to_string()); //same argument
        self.job_finished(0u32, Finished::Sucessful);

        self.fill_in_unfinished_downstream_counts();

        for j in self.jobs.iter() {
            debug!("{:?}", j);
        }
        for (a, b, x) in self.dag.all_edges() {
            debug!("{:?} {:?} {:?}", a, b, x);
        }
        Ok(())
    }

    fn job_finished(&mut self, idx: u32, outcome: Finished) {
        debug!("job finished {:?}", self.jobs[idx as usize]);
        self.jobs_ready_to_run.remove(&self.jobs[idx as usize].0);
        self.job_finished_handle_downstream(idx, outcome);
        self.job_finished_handle_upstream(idx, outcome);
        if self.is_finished() {
            self.already_started = StartStatus::Finished; // todo : find a better way to evaluate this
        }
    }

    fn job_finished_handle_upstream(&mut self, idx: u32, outcome: Finished) {
        let upstreams: Vec<_> = self
            .dag
            .neighbors_directed(idx, Direction::Incoming)
            .collect();
        for upstream_idx in upstreams {
            self.jobs[upstream_idx as usize].1.unfinished_downstreams =
                match self.jobs[upstream_idx as usize].1.unfinished_downstreams {
                    UnfinishedDownstreams::Count(0) => {
                        panic!("UnfinishedDownstreams now below 0?!")
                    }
                    UnfinishedDownstreams::Count(ii) => UnfinishedDownstreams::Count(ii - 1),
                    other => other,
                }
        }
    }

    /// update runstates with the new outcome
    fn job_finished_handle_downstream(&mut self, idx: u32, outcome: Finished) {
        let downstreams: Vec<_> = self
            .dag
            .neighbors_directed(idx, Direction::Outgoing)
            .collect();
        match (self.jobs[idx as usize].1.ran, outcome) {
            (RunStatus::Ran(_), _) => panic!("job_finished on an already ran job"),

            (RunStatus::ReadyToRun, _) => panic!("Going from ReadyToRun to finished?"),

            (_, Finished::UpstreamFailure) => {
                for ds in downstreams {
                    self.job_finished(ds, outcome);
                }
            }
            (RunStatus::RunThis, _) => panic!(
                "Going from RunThis to finished? {:?} {:?}",
                self.jobs[idx as usize], outcome
            ),
            (RunStatus::Running, Finished::Sucessful) => {
                let job_id = self.jobs[idx as usize].0.to_string();
                let history_output = &self.jobs[idx as usize]
                    .1
                    .history_output
                    .as_ref()
                    .expect("Job was finished::Sucessful, but without history_output");

                for ds in downstreams.iter() {
                    let downstream_job_id = &self.jobs[*ds as usize].0;
                    let last_history_value = self.history.get(&format!(
                        "{}!!!{}",
                        job_id.to_string(),
                        downstream_job_id.to_string()
                    )); //todo: fight alloc
                    let validation_status = if idx == 0 {
                        // is that 'link a root to every no input job' stuff really necessary?
                        InvalidationStatus::Validated
                    } else {
                        match last_history_value {
                            None => InvalidationStatus::Invalidated,
                            Some(last_history) => {
                                if self.strategy.is_history_altered(
                                    &job_id,
                                    downstream_job_id,
                                    last_history,
                                    history_output,
                                ) {
                                    InvalidationStatus::Invalidated
                                } else {
                                    InvalidationStatus::Validated
                                }
                            }
                        }
                    };
                    debug!(
                        "Last history value for {} {}: '{:?}'. Validation state -> {:?}",
                        job_id, downstream_job_id, last_history_value, validation_status
                    );

                    *self.dag.edge_weight_mut(idx, *ds).unwrap() = validation_status;
                    //todo: figure out job renaming?
                    //or declare it an external problem, and write a freaking rename tool...
                }
                for ds in downstreams {
                    self.check_ready_to_run(ds);
                }
            }
            (RunStatus::Running, Finished::Failure) => {
                for ds in downstreams {
                    self.job_finished(ds, Finished::UpstreamFailure);
                }
            }
            (RunStatus::Running, Finished::Skipped)
            | (RunStatus::Undetermined, Finished::Skipped) => {
                for ds in downstreams {
                    *self.dag.edge_weight_mut(idx, ds).unwrap() = InvalidationStatus::Validated; // a skipped job can't invalidate a downstream
                    self.check_ready_to_run(ds);
                }
            }
            (RunStatus::Undetermined, _) => {
                panic!(
                    "Should not happen {:?} {:?}",
                    self.jobs[idx as usize].1, outcome
                );
            }
        };
        self.jobs[idx as usize].1.ran = RunStatus::Ran(outcome);
    }

    fn check_ready_to_run(&mut self, idx: u32) {
        debug!("check_ready_to_run {:?}", self.jobs[idx as usize]);
        match self.jobs[idx as usize].1.ran {
            RunStatus::Ran(Finished::Skipped) => {}
            RunStatus::Running | RunStatus::Ran(_) => panic!(
                "check_ready_to_run on a done job {:?}",
                self.jobs[idx as usize]
            ),
            RunStatus::ReadyToRun => {}
            RunStatus::RunThis | RunStatus::Undetermined => {
                for (_a, _b, edge) in self.dag.edges_directed(idx, Direction::Incoming) {
                    match edge {
                        InvalidationStatus::Unknown => return,
                        InvalidationStatus::Invalidated => {
                            self.jobs[idx as usize].1.ran = RunStatus::RunThis
                        }
                        _ => {}
                    }
                }
                if self.jobs[idx as usize].1.ran == RunStatus::RunThis  //ie. invalidated above
                    ||
                        (self.jobs[idx as usize].1.kind == JobKind::Output &&
                        !self.strategy.output_already_present(&self.jobs[idx as usize].0)
                        )
                {
                    debug!("setting ready to run");
                    self.jobs[idx as usize].1.ran = RunStatus::ReadyToRun;
                    self.jobs_ready_to_run
                        .insert(self.jobs[idx as usize].0.clone());
                } else {
                    // this job has been validated, and RunStatus is Undetermined
                    //if self.output_present.contains(&self.jobs[idx as usize].0) {
                    self.job_finished(idx, Finished::Skipped)
                    /* } else {
                        self.jobs[idx as usize].1.ran = RunStatus::ReadyToRun;
                    } */
                }

                debug!("check_ready_to_run success {:?}", self.jobs[idx as usize]);
            }
        }
    }

    fn update_ephemerals_needed(&mut self) {
        let mut not_needed_counters: Vec<i64> = vec![0; self.jobs.len()];
        debug!("update_ephemerals_needed");

        //obvious improvement: cache thi
        //this is borrow checker bullshit...
        let topo = self.topo.as_ref().unwrap().clone();
        //let topo = petgraph::algo::toposort(petgraph::visit::Reversed(&self.dag), None).unwrap();
        for idx in topo {
            let idx = idx as usize;
            debug!("topo {:?} {:?}", idx, &self.jobs[idx]);
            if self.jobs[idx].1.kind == JobKind::Ephemeral {
                //this jobs not_needed_counters is up to date..
                debug!("\t not_needed_counters {}", not_needed_counters[idx]);
                if not_needed_counters[idx] < 0 {
                    // i64::min + whatever... -> this is needed.
                    //
                    if self.jobs[idx].1.ran == RunStatus::Undetermined {
                        self.jobs[idx as usize].1.ran = RunStatus::RunThis;
                    }
                } else {
                    let total = self
                        .dag
                        .neighbors_directed(idx as u32, Direction::Outgoing)
                        .count();
                    if total == not_needed_counters[idx] as usize {
                        //we know no downstream needs this
                        self.check_ready_to_run(idx as u32);
                        debug!("now setting to skipped possible {:?}", self.jobs[idx].1);
                        if self.jobs[idx].1.ran == RunStatus::ReadyToRun {
                            debug!("now setting to skipped");
                            self.job_finished(idx as u32, Finished::Skipped)
                        } else if total == 0 {
                            // has no downstreams -> don't run...
                            debug!(
                                "setting {} to skipped because of no downstreams",
                                self.jobs[idx].0
                            );
                            self.job_finished(idx as u32, Finished::Skipped)
                        }
                    }
                }
            }

            match self.jobs[idx].1.kind {
                JobKind::Always => {
                    for upstream_id in self.dag.neighbors_directed(idx as u32, Direction::Incoming)
                    {
                        not_needed_counters[upstream_id as usize] = i64::MIN;
                    }
                }
                JobKind::Output => {
                    if !self.strategy.output_already_present(&self.jobs[idx].0) {
                        for upstream_id in
                            self.dag.neighbors_directed(idx as u32, Direction::Incoming)
                        {
                            not_needed_counters[upstream_id as usize] = i64::MIN;
                        }
                    } else {
                        for upstream_id in
                            self.dag.neighbors_directed(idx as u32, Direction::Incoming)
                        {
                            not_needed_counters[upstream_id as usize] += 1;
                        }
                    }
                    //todo invalidation
                }
                JobKind::Ephemeral => match self.jobs[idx].1.ran {
                    RunStatus::RunThis => {
                        for upstream_id in
                            self.dag.neighbors_directed(idx as u32, Direction::Incoming)
                        {
                            not_needed_counters[upstream_id as usize] = i64::MIN;
                        }
                    }
                    _ => {
                        for upstream_id in
                            self.dag.neighbors_directed(idx as u32, Direction::Incoming)
                        {
                            not_needed_counters[upstream_id as usize] += 1;
                        }
                    }
                },
            }
        }

        debug!("");
    }

    pub fn event_now_running(&mut self, job_id: &str) -> Result<(), PPGEvaluatorError> {
        let idx = self.node_to_index.get(job_id).expect("Unknown job id");
        let j = &mut self.jobs[*idx as usize];
        match j.1.ran {
            RunStatus::ReadyToRun => {
                j.1.ran = RunStatus::Running;
                self.jobs_ready_to_run.remove(job_id);
                Ok(())
            }
            _ => Err(PPGEvaluatorError::APIError(format!(
                "Requested to run a job that was not ready to run! {:?}",
                j
            ))),
        }
    }

    pub fn event_job_finished_success(
        &mut self,
        job_id: &str,
        history_to_store: String,
    ) -> Result<(), PPGEvaluatorError> {
        let idx = *self.node_to_index.get(job_id).expect("Unknown job id");
        let j = &mut self.jobs[idx as usize];
        match j.1.ran {
            RunStatus::Running => {
                j.1.history_output = Some(history_to_store);
                self.job_finished(idx, Finished::Sucessful);
                Ok(())
            }
            _ => Err(PPGEvaluatorError::APIError(format!(
                "Signaled job finished/success on a job that was not running{:?}",
                j
            ))),
        }
    }

    pub fn event_job_finished_failure(&mut self, job_id: &str) -> Result<(), PPGEvaluatorError> {
        let idx = *self.node_to_index.get(job_id).expect("Unknown job id");
        let j = &mut self.jobs[idx as usize];
        match j.1.ran {
            RunStatus::Running => {
                self.job_finished(idx, Finished::Failure);
                Ok(())
            }
            _ => Err(PPGEvaluatorError::APIError(format!(
                "Signaled job finished/failure on a job that was not running{:?}",
                j
            ))),
        }
    }

    /// what jobs are ready to run *right now*
    pub fn ready_to_runs(&self) -> HashSet<String> {
        self.jobs_ready_to_run.clone()
    }

    pub fn ready_to_cleanup(&self) -> HashSet<String> {
        self.jobs[1..] // skip the root
            .iter()
            .filter_map(
                |(job_id, job)| match (job.kind, job.unfinished_downstreams) {
                    (JobKind::Ephemeral, UnfinishedDownstreams::Count(0)) => Some(job_id.clone()),
                    (_, _) => None,
                },
            )
            .collect()
    }

    pub fn failed_jobs(&self) -> HashSet<String> {
        self.jobs
            .iter()
            .filter(|(job_id, job)| job.ran == RunStatus::Ran(Finished::Failure))
            .map(|(job_id, _)| job_id.clone())
            .collect()
    }

    pub fn upstream_failed_jobs(&self) -> HashSet<String> {
        self.jobs
            .iter()
            .filter(|(job_id, job)| job.ran == RunStatus::Ran(Finished::UpstreamFailure))
            .map(|(job_id, _)| job_id.clone())
            .collect()
    }

    fn job_and_downstreams_are_ephmeral(&self, job_idx: u32) -> bool {
        if self.jobs[job_idx as usize].1.kind != JobKind::Ephemeral {
            return false;
        }
        let downstreams: Vec<_> = self
            .dag
            .neighbors_directed(job_idx, Direction::Outgoing)
            .collect();
        for ds_id in downstreams {
            if !self.job_and_downstreams_are_ephmeral(ds_id) {
                return false;
            }
        }
        return true;
    }

    pub fn new_history(&self) -> HashMap<String, String> {
        if !self.is_finished() {
            panic!("Graph wasn't finished, not handing out history..."); // todo: actually, why not, we could save history occasionally?
        }
        let mut out = self.history.clone();
        for (idx_minus_one, (job_id, job)) in self.jobs[1..].iter().enumerate() {
            let key = job_id.to_string();
            let history = match &job.history_output {
                Some(new_history) => new_history,
                None => match self.history.get(&key) {
                    Some(old_history) => old_history,
                    None => {
                        if ((&job).kind == JobKind::Ephemeral
                            && self.job_and_downstreams_are_ephmeral((idx_minus_one as u32) + 1))
                            || job.ran == RunStatus::Ran(Finished::Failure)
                            || job.ran == RunStatus::Ran(Finished::UpstreamFailure)
                        {
                            continue;
                        } else {
                            panic!(
                                "Job was skipped but history had no entry for this job: {} {:?}",
                                job_id, job.ran
                            );
                        }
                    }
                },
            };
            out.insert(key, history.to_string());
        }
        for (a, b, x) in self.dag.all_edges() {
            if a != 0 {
                // the root has no sensible history.
                let job_id_a = &self.jobs[a as usize].0;
                let job_id_b = &self.jobs[b as usize].0;
                let key = format!("{}!!!{}", job_id_a.to_string(), job_id_b.to_string());
                let history = self.jobs[a as usize].1.history_output.as_ref();
                let history = match history {
                    Some(run_history) => run_history, // we have new history.
                    None => {
                        match self.jobs[a as usize].1.ran {
                            RunStatus::Ran(Finished::Skipped)
                            //RunStatus::Ran(Finished::Skipped),

                            => {
                                match self.history.get(&key) {
                                    Some(old_run_history) => old_run_history,
                                    None => {
                                        // we had no history for this edge, for the incoming job was skipped.
                                        // But we should have a history entry for the job itself, right?
                                        let job_key = job_id_a;
                                        match self.history.get(job_key) {
                                            Some(old_run_history) => old_run_history,
                                            None => {
                                                if (&self.jobs[a as usize].1).kind
                                                    == JobKind::Ephemeral
                                                {
                                                    continue;
                                                } else {
                                                    panic!("No history for edge from this run, none from the prev run, and no upstream job history. Bug: {}, {}",
                                               job_id_a, job_id_b);
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            RunStatus::Ran(Finished::Failure) |
                            RunStatus::Ran(Finished::UpstreamFailure)
                                =>{
                                match self.history.get(&key) { //we have an old edge
                                    Some(old_run_history) => old_run_history,
                                    None => {
                                        continue;
                                    }
                                }
                            }
                            _ => {
                                panic!(
                                    "unexpected ran state when no history was present. job_id: '{}', ran state: '{:?}'",
                                    job_id_a, self.jobs[a as usize].1.ran
                                );
                            }
                        }
                    }
                };
                out.insert(key, history.to_string());
            }
        }

        out
    }
}

// simulates a complete (deterministic)
// run - jobs just register that they've been run,
// and output a 'dummy' history.
pub struct TestGraphRunner {
    pub setup_graph: Box<dyn Fn(&mut PPGEvaluator<StrategyForTesting>)>,
    pub run_counters: HashMap<String, usize>,
    pub history: HashMap<String, String>,
    pub already_done: HashSet<String>,
    pub allowed_nesting: u32,
}

impl TestGraphRunner {
    pub fn new(setup_func: Box<dyn Fn(&mut PPGEvaluator<StrategyForTesting>)>) -> Self {
        TestGraphRunner {
            setup_graph: setup_func,
            run_counters: HashMap::new(),
            history: HashMap::new(),
            already_done: HashSet::new(),
            allowed_nesting: 250,
        }
    }

    pub fn run(
        &mut self,
        jobs_to_fail: &[&str],
    ) -> Result<PPGEvaluator<StrategyForTesting>, PPGEvaluatorError> {
        let strat = StrategyForTesting::new();
        for k in self.already_done.iter() {
            strat.already_done.borrow_mut().insert(k.to_string());
        }
        let already_done2 = Rc::clone(&strat.already_done);
        let mut g = PPGEvaluator::new_with_history(self.history.clone(), strat);

        (self.setup_graph)(&mut g);
        let mut counter = self.allowed_nesting;
        g.event_startup().unwrap();
        while !g.is_finished() {
            let to_run = g.ready_to_runs();
            assert!(!to_run.is_empty());
            for job_id in to_run.iter() {
                debug!("Running {}", job_id);
                g.event_now_running(job_id)?;
                *self.run_counters.entry(job_id.clone()).or_insert(0) += 1;
                if jobs_to_fail.contains(&&job_id[..]) {
                    g.event_job_finished_failure(job_id).unwrap();
                } else {
                    g.event_job_finished_success(job_id, format!("history_{}", job_id))
                        .unwrap();
                }
                already_done2.borrow_mut().insert(job_id.clone());
            }
            counter -= 1;
            if counter == 0 {
                return Err(PPGEvaluatorError::APIError(format!(
                    "run out of room, you nested them more than {} deep?",
                    self.allowed_nesting
                )));
            }
        }
        self.history.clear();
        for (k, v) in g.new_history().iter() {
            self.history.insert(k.clone(), v.clone());
        }
        for k in already_done2.take().into_iter() {
            self.already_done.insert(k);
        }

        Ok(g)
    }
}

pub fn test_big_linear_graph(count: u32) {
    let c2 = count;
    let create_graph = move |g: &mut PPGEvaluator<StrategyForTesting>| {
        let c = c2 - 1;
        for ii in 0..c {
            g.add_node(&format!("A{}", ii), JobKind::Output);
        }
        for ii in 1..c {
            g.depends_on(&format!("A{}", ii - 1), &format!("A{}", ii));
        }
    };
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    ro.allowed_nesting = count + 1;
    let g = ro.run(&Vec::new());
    assert!(g.is_ok())
    //dbg!(g.new_history().len());
}

pub fn test_big_linear_graph_half_ephemeral(count: u32) {
    let c2 = count;
    let create_graph = move |g: &mut PPGEvaluator<StrategyForTesting>| {
        let c = c2 - 1;
        for ii in 0..c {
            g.add_node(
                &format!("A{}", ii),
                if ii % 2 == 0 {
                    JobKind::Output
                } else {
                    JobKind::Ephemeral
                },
            );
        }
        for ii in 1..c {
            g.depends_on(&format!("A{}", ii - 1), &format!("A{}", ii));
        }
    };
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    ro.allowed_nesting = count + 1;
    let g = ro.run(&Vec::new());
    assert!(g.is_ok())
    //dbg!(g.new_history().len());
}

struct StrategyForPython {
    history_altered_callback: PyObject,
}

impl PPGEvaluatorStrategy for StrategyForPython {
    fn output_already_present(&self, query: &str) -> bool {
        use std::path::PathBuf;
        let p = PathBuf::from(query);
        p.exists()
    }

    fn is_history_altered(
        &self,
        job_id_upstream: &str,
        job_id_downstream: &str,
        last_recorded_value: &str,
        current_value: &str,
    ) -> bool {
        if last_recorded_value == current_value {
            false
        } else {
            Python::with_gil(|py| {
                let res = self.history_altered_callback.call1(
                    py,
                    (
                        job_id_upstream,
                        job_id_downstream,
                        last_recorded_value,
                        current_value,
                    ),
                );
                res.expect("History comparison failed on python side")
                    .extract::<bool>(py)
                    .expect("history comparison did not return a bool")
            })
        }

        //last_recorded_value != current_value // todo
    }
}

#[pyclass(name = "PPG2Evaluator")]
pub struct PyPPG2Evaluator {
    evaluator: PPGEvaluator<StrategyForPython>, // todo
}

impl From<PPGEvaluatorError> for PyErr {
    fn from(val: PPGEvaluatorError) -> Self {
        PyValueError::new_err(val.to_string())
    }
}

#[pymethods]
impl PyPPG2Evaluator {
    #[new]
    fn __new__(
        py: Python,
        py_history: &PyDict,
        history_compare_callable: PyObject,
    ) -> Result<Self, PyErr> {
        let mut history: HashMap<String, String> = HashMap::new();
        for (k, v) in py_history.iter() {
            let ko: String = k.extract()?;
            let vo: String = v.extract()?;
            history.insert(ko, vo);
        }
        Ok(PyPPG2Evaluator {
            evaluator: PPGEvaluator::new_with_history(
                history,
                StrategyForPython {
                    history_altered_callback: history_compare_callable,
                },
            ),
        })
    }

    pub fn add_node(&mut self, job_id: &str, job_kind: &str) -> Result<(), PyErr> {
        let jk = match job_kind {
            "Output" => JobKind::Output,
            "Always" => JobKind::Always,
            "Ephemeral" => JobKind::Ephemeral,
            _ => return Err(PyTypeError::new_err("Invalid job kind")),
        };
        self.evaluator.add_node(job_id, jk);
        Ok(())
    }

    pub fn add_edge(&mut self, from: &str, to: &str) {
        self.evaluator.depends_on(from, to);
    }

    pub fn event_startup(&mut self) -> Result<(), PyErr> {
        Ok(self.evaluator.event_startup()?)
    }

    pub fn event_now_running(&mut self, job_id: &str) -> Result<(), PyErr> {
        Ok(self.evaluator.event_now_running(job_id)?)
    }

    pub fn event_job_success(&mut self, job_id: &str, new_history: &str) -> Result<(), PyErr> {
        Ok(self
            .evaluator
            .event_job_finished_success(job_id, new_history.to_string())?)
    }

    pub fn event_job_failure(&mut self, job_id: &str) -> Result<(), PyErr> {
        Ok(self.evaluator.event_job_finished_failure(job_id)?)
    }

    pub fn list_upstream_failed_jobs(&self) -> Vec<String> {
        self.evaluator.upstream_failed_jobs().into_iter().collect()
    }

    pub fn jobs_ready_to_run(&self) -> Vec<String> {
        self.evaluator.ready_to_runs().into_iter().collect()
    }

    pub fn is_finished(&self) -> bool {
        self.evaluator.is_finished()
    }

    pub fn new_history(&self) -> HashMap<String, String> {
        self.evaluator.new_history()
    }
}

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

/// A Python module implemented in Rust.
#[pymodule]
fn pypipegraph2(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add_class::<PyPPG2Evaluator>()?;
    Ok(())
}
