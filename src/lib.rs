#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
use log::{debug, error, info, warn};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::types::{PyDict, PyFunction};
use std::any;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::rc::Rc;
use std::sync::Once;
use std::thread::current; // Use log crate when building application

use petgraph::{graphmap::GraphMap, Directed, Direction};
use thiserror::Error;

use pyo3::prelude::*;

#[cfg(test)]
mod tests;

static LOGGER_INIT: Once = Once::new();

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
    ReadyToRun,
    Running,
    Ran(Finished),
}

#[derive(Copy, Clone, Debug)]
enum CleanupStatus {
    NotYet,
    Ready,
    Done,
}

#[derive(Clone, Debug)]
struct Job {
    job_id: String,
    kind: JobKind,
    ran: RunStatus,
    history_output: Option<String>,
    cleanup_status: CleanupStatus, // -1 = not calculated, -2: cleanup handled
    no_of_input_changed: bool,
}

enum StartStatus {
    NotStarted,
    Running,
    Finished,
}

enum EphmerealNeeded {
    Yes,
    No,
    Maybe,
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
        let res = PPGEvaluator {
            dag: GraphMap::new(),
            jobs: Vec::new(),
            node_to_index: HashMap::new(),
            history,
            strategy,
            already_started: StartStatus::NotStarted,
            jobs_ready_to_run: HashSet::new(),
            topo: None,
        };
        //res.add_node("<ROOT>", JobKind::Always);
        res
    }

    pub fn add_node(&mut self, job_id: &str, kind: JobKind) {
        assert_ne!(job_id, "");
        let job = Job {
            job_id: job_id.to_string(),
            kind,
            ran: RunStatus::Undetermined,
            history_output: None,
            cleanup_status: CleanupStatus::NotYet,
            no_of_input_changed: false,
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
        let key = format!("{}!!!{}", upstream, downstream);
        self.dag.add_edge(
            *upstream_id,
            *downstream_id,
            //now if we don'h have history for this edge, it's obviously invalidated, right?
            if self.history.contains_key(&key) {
                InvalidationStatus::Unknown
            } else {
                InvalidationStatus::Invalidated
            },
        );
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

    fn debug_edges(&self) {
        for (a, b, x) in self.dag.all_edges() {
            debug!(
                "{}!!!{} {:?}",
                self.jobs[a as usize].0, self.jobs[b as usize].0, x
            );
        }
    }

    pub fn event_startup(&mut self) -> Result<(), PPGEvaluatorError> {
        // this is not particulary fast.
        error!("event_startup");
        match self.already_started {
            StartStatus::Running | StartStatus::Finished => {
                return Err(PPGEvaluatorError::APIError("Can't start twice".to_string()));
            }
            _ => {}
        };
        self.already_started = StartStatus::Running;

        for j in self.jobs.iter() {
            debug!("{:?}", j);
        }
        self.debug_edges();
        //
        //the edges are already either Unknown or Invalidated (if we had no historymissing)
        self.topo =
            Some(petgraph::algo::toposort(petgraph::visit::Reversed(&self.dag), None).unwrap());
        self.count_historic_inputs();

        self.evaluate_next_steps();
        Ok(())
    }

    pub fn count_historic_inputs(&mut self) {
        let mut historic_count: HashMap<String, i32> = HashMap::new();
        for key in self.history.keys() {
            match key.split_once("!!!") {
                Some((upstream_job_id, downstream_job_id)) => {
                    *historic_count
                        .entry(downstream_job_id.to_string())
                        .or_insert(0) += 1;
                }
                None => {}
            }
        }
        let mut current_counts = HashMap::new();
        for job_id in self.node_to_index.keys() {
            current_counts.insert(job_id.clone(), 0);
        }

        for (_upstream_idx, downstream_idx, _weight) in self.dag.all_edges() {
            let downstream_job_id = &self.jobs[downstream_idx as usize].0;
            *current_counts.get_mut(downstream_job_id).unwrap() += 1;
        }
        debug!("current counts: {:?}", current_counts);
        debug!("historic counts: {:?}", &historic_count);

        for (job_id, current_count) in current_counts.iter() {
            let hist_count = match historic_count.get(job_id) {
                Some(hc) => *hc,
                None => 0,
            };
            if *current_count != hist_count {
                debug!(
                    "No of inputs changed {} current: {} historic: {}",
                    job_id, *current_count, hist_count
                );
                let job_idx = *self.node_to_index.get(job_id).unwrap();
                self.jobs[job_idx as usize].1.no_of_input_changed = true;
            } else {
                debug!(
                    "No of inputs unchanged {} {} {}",
                    job_id, hist_count, *current_count
                );
            }
        }
    }

    fn job_finished(&mut self, idx: u32, outcome: Finished) {
        debug!(
            "job finished {:?} -> outcome: {:?}",
            self.jobs[idx as usize], outcome
        );
        let old_ran_state = self.jobs[idx as usize].1.ran;
        self.jobs[idx as usize].1.ran = RunStatus::Ran(outcome);
        match (old_ran_state, outcome) {
            (RunStatus::Undetermined, Finished::Sucessful) => panic!("unexpected transition"),
            (RunStatus::Undetermined, Finished::Failure) => panic!("unexpected transition"),
            (RunStatus::Undetermined, Finished::Skipped) => {}
            (RunStatus::Undetermined, Finished::UpstreamFailure) => self.fail_downstreams(idx),
            (RunStatus::ReadyToRun, Finished::Sucessful) => panic!("unexpected transition"),
            (RunStatus::ReadyToRun, Finished::Failure) => panic!("unexpected transition"),
            (RunStatus::ReadyToRun, Finished::Skipped) => panic!("unexpected transition"),
            (RunStatus::ReadyToRun, Finished::UpstreamFailure) => panic!("unexpected transition"),
            (RunStatus::Running, Finished::Sucessful) => {}
            (RunStatus::Running, Finished::Failure) => self.fail_downstreams(idx),
            (RunStatus::Running, Finished::Skipped) => panic!("unexpected transition"),
            (RunStatus::Running, Finished::UpstreamFailure) => panic!("unexpected transition"),
            (RunStatus::Ran(_), Finished::Sucessful) => panic!("unexpected transition"),
            (RunStatus::Ran(_), Finished::Failure) => panic!("unexpected transition"),
            (RunStatus::Ran(_), Finished::Skipped) => panic!("unexpected transition"),
            (RunStatus::Ran(_), Finished::UpstreamFailure) => panic!("unexpected transition"),
        };
        for (a, b, x) in self.dag.all_edges() {
            debug!(
                "{}!!!{} {:?}",
                self.jobs[a as usize].0, self.jobs[b as usize].0, x
            );
        }

        self.evaluate_next_steps();
        if self.is_finished() {
            self.already_started = StartStatus::Finished; // todo : find a better way to evaluate this
        }
    }

    fn evaluate_next_steps(&mut self) {
        for idx in 0..self.jobs.len() {
            let (job_id, job) = &self.jobs[idx];
            debug!("Exam {} {:?}", job_id, job);
            match job.ran {
                RunStatus::ReadyToRun | RunStatus::Running => {}
                RunStatus::Undetermined => {
                    //now all I need to do is catch all the combinations...
                    let all_upstreams_done: bool = self.all_upstreams_done(idx as u32);
                    let no_of_input_changed = job.no_of_input_changed;
                    if all_upstreams_done {
                        debug!("\tall upstreams done");
                        match job.kind {
                            JobKind::Always => {
                                self.jobs[idx].1.ran = RunStatus::ReadyToRun;
                                continue;
                            }
                            JobKind::Output => {
                                let output_present = self.strategy.output_already_present(job_id);
                                let any_incoming_edge_invalidated =
                                    self.any_incoming_edge_invalidated(idx as u32);
                                if !output_present
                                    || any_incoming_edge_invalidated
                                    || no_of_input_changed
                                {
                                    debug!(
                                        "\t decided to run {} {} {}",
                                        output_present,
                                        any_incoming_edge_invalidated,
                                        no_of_input_changed
                                    );
                                    self.jobs[idx].1.ran = RunStatus::ReadyToRun;
                                    continue;
                                } else {
                                    //if output_present
                                    debug!("\t decided to skip, output was present, not invalidated {} {} {}", output_present, any_incoming_edge_invalidated, no_of_input_changed);
                                    self.job_finished(idx as u32, Finished::Skipped);
                                }
                            }
                            JobKind::Ephemeral => match self.is_ephemeral_needed(idx as u32) {
                                EphmerealNeeded::Yes => {
                                    debug!("\t decided to run, ephemeral was needed",);
                                    self.jobs[idx].1.ran = RunStatus::ReadyToRun;
                                    continue;
                                }
                                EphmerealNeeded::No => {
                                    debug!("\t decided to skip, ephemeral was not needed",);
                                    self.job_finished(idx as u32, Finished::Skipped);
                                }
                                EphmerealNeeded::Maybe => {}
                            },
                        }
                    } else {
                        debug!("\t Not all upstreams done");
                        if job.kind == JobKind::Ephemeral {
                            match self.is_ephemeral_needed(idx as u32) {
                                EphmerealNeeded::No => {
                                    debug!(
                                        "Ephemeral classified as wont't run, setting to skipped {}",
                                        &self.jobs[idx].0
                                    );
                                    self.job_finished(idx as u32, Finished::Skipped);
                                }
                                EphmerealNeeded::Yes => {}, //todo: cache?
                                EphmerealNeeded::Maybe => {}
                            }

                            /*
                            let (any_downstream_necessary, any_outgoing_invalidated) =
                                self.examine_ephmeral_downstream(idx as u32);
                            if any_downstream_necessary | any_outgoing_invalidated {
                            } else {
                                debug!(
                                    "Ephemeral classified as wont't run, setting to skipped {}",
                                    &self.jobs[idx].0
                                );
                                self.job_finished(idx as u32, Finished::Skipped);
                            }
                            */
                        }
                        //can't progress this node.
                    }
                }
                RunStatus::Ran(outcome) => match (job.kind, job.cleanup_status, outcome) {
                    (
                        JobKind::Ephemeral,
                        CleanupStatus::NotYet,
                        Finished::Sucessful | Finished::Failure,
                    ) => {
                        if self.all_downstreams_done(idx as u32) {
                            debug!("\t all downstreamns done, ready for cleanup");
                            self.jobs[idx].1.cleanup_status = CleanupStatus::Ready;
                        }
                    }
                    (
                        JobKind::Ephemeral,
                        CleanupStatus::NotYet,
                        Finished::Skipped | Finished::UpstreamFailure,
                    ) => {
                        self.jobs[idx].1.cleanup_status = CleanupStatus::Done; //technically 'Unnecessary'
                    }
                    (_, _, _) => {}
                },
            }
            debug!("\t no update");
        }
    }

    fn all_downstreams_validated(&mut self, idx: u32) -> bool {
        let downstreams: Vec<_> = self
            .dag
            .neighbors_directed(idx, Direction::Outgoing)
            .collect();
        for downstream_idx in downstreams {
            if self.any_incoming_edge_invalidated(downstream_idx) {
                return false;
            }
        }
        return true;
    }

    fn fail_downstreams(&mut self, idx: u32) {
        let downstreams: Vec<_> = self
            .dag
            .neighbors_directed(idx, Direction::Outgoing)
            .collect();
        for downstream_idx in downstreams {
            debug!(
                "Failing downstreams for {}: {}",
                &self.jobs[idx as usize].0, &self.jobs[downstream_idx as usize].0
            );
            self.job_finished(downstream_idx, Finished::UpstreamFailure)
        }
    }

    fn all_downstreams_done(&self, idx: u32) -> bool {
        let downstreams = self.dag.neighbors_directed(idx, Direction::Outgoing);
        for downstream_idx in downstreams {
            let (downstream_id, downstream_job) = &self.jobs[downstream_idx as usize];
            if downstream_job.ran == RunStatus::Ran(Finished::Skipped)
                || downstream_job.history_output.is_some()
            {
                continue;
            } else {
                return false;
            }
        }
        return true;
    }

    fn is_ephemeral_needed(&mut self, idx: u32) -> EphmerealNeeded {
        let downstreams: Vec<_> = self
            .dag
            .neighbors_directed(idx, Direction::Outgoing)
            .collect();
        if downstreams.is_empty() {
            return EphmerealNeeded::No;
        }
        let mut any_unknown = false;
        for downstream_idx in downstreams {
            let (downstream_id, downstream_job) = &self.jobs[downstream_idx as usize];
            if downstream_job.kind == JobKind::Always {
                debug!(
                    "\t\t ephemeral was needed because of {} being always",
                    downstream_id
                );
                return EphmerealNeeded::Yes;
            } else if downstream_job.kind == JobKind::Output
                && !self.strategy.output_already_present(downstream_id)
            {
                debug!(
                    "\t\t ephemeral was needed because of {} missing output",
                    downstream_id
                );
                return EphmerealNeeded::Yes;
            } else {
                /* debug!(
                    "\t\tooking at downstream {} {:?}",
                    downstream_id.clone(),
                    self.job_invalidation_status_ignoring_ephemerals(downstream_idx)
                ); */
                let downstream_kind = downstream_job.kind; 
                match self.job_invalidation_status_ignoring_ephemerals(downstream_idx) {
                    InvalidationStatus::Invalidated => return EphmerealNeeded::Yes,
                    InvalidationStatus::Validated => {
                        if downstream_kind == JobKind::Ephemeral {
                            match self.is_ephemeral_needed(downstream_idx) {
                                EphmerealNeeded::Yes => return EphmerealNeeded::Yes,
                                EphmerealNeeded::No => {}
                                EphmerealNeeded::Maybe => any_unknown = true,
                            }
                        }
                    }
                    InvalidationStatus::Unknown => {
                        any_unknown = true;
                    }
                }
            }
        }
        if any_unknown {
            EphmerealNeeded::Maybe
        } else {
            EphmerealNeeded::No
        }
    }

    fn job_invalidation_status_ignoring_ephemerals(&mut self, idx: u32) -> InvalidationStatus {
        let upstreams: Vec<_> = self
            .dag
            .neighbors_directed(idx, Direction::Incoming)
            .collect(); // todo: borrow checker...
        let job_id = &self.jobs[idx as usize].0;
        for upstream_idx in upstreams {
            if self.jobs[upstream_idx as usize].1.kind == JobKind::Ephemeral {
                continue;
            }
            match self.dag.edge_weight(upstream_idx, idx).unwrap() {
                InvalidationStatus::Invalidated => return InvalidationStatus::Invalidated,
                InvalidationStatus::Validated => {}
                InvalidationStatus::Unknown => {
                    self.update_invalidation_status(upstream_idx, idx);
                    match self.dag.edge_weight(upstream_idx, idx).unwrap() {
                        InvalidationStatus::Invalidated => return InvalidationStatus::Invalidated,
                        InvalidationStatus::Validated => {}
                        InvalidationStatus::Unknown => {
                            return InvalidationStatus::Unknown;
                        }
                    }
                }
            }
        }
        InvalidationStatus::Validated
    }

    fn update_invalidation_status(&mut self, upstream_idx: u32, downstream_idx: u32) {
        let upstream_id = &self.jobs[upstream_idx as usize].0;
        let downstream_id = &self.jobs[downstream_idx as usize].0;
        let key = format!("{}!!!{}", upstream_id, downstream_id);
        match self.history.get(&key) {
            Some(recorded_value) => {
                match self.jobs[upstream_idx as usize].1.history_output.as_ref() {
                    Some(new_value) => {
                        if self.strategy.is_history_altered(
                            upstream_id,
                            downstream_id,
                            recorded_value,
                            new_value,
                        ) {
                            *self
                                .dag
                                .edge_weight_mut(upstream_idx, downstream_idx)
                                .unwrap() = InvalidationStatus::Invalidated;
                        } else {
                        }
                        *self
                            .dag
                            .edge_weight_mut(upstream_idx, downstream_idx)
                            .unwrap() = InvalidationStatus::Validated;
                    }
                    None => {}
                }
            }

            None => {
                *self
                    .dag
                    .edge_weight_mut(upstream_idx, downstream_idx)
                    .unwrap() = InvalidationStatus::Invalidated;
            }
        }
    }

    fn has_no_downstreams(&self, idx: u32) -> bool {
        let mut downstreams = self.dag.neighbors_directed(idx, Direction::Outgoing);
        match downstreams.next() {
            Some(_) => false,
            None => true,
        }
    }

    fn any_incoming_edge_invalidated(&mut self, idx: u32) -> bool {
        let upstreams: Vec<_> = self
            .dag
            .neighbors_directed(idx, Direction::Incoming)
            .collect(); // todo: borrow checker...
        let job_id = &self.jobs[idx as usize].0;
        for upstream_idx in upstreams {
            match self.dag.edge_weight(upstream_idx, idx).unwrap() {
                InvalidationStatus::Invalidated => return true,
                InvalidationStatus::Validated => {}
                InvalidationStatus::Unknown => {
                    let upstream_id = &self.jobs[upstream_idx as usize].0;
                    let key = format!("{}!!!{}", upstream_id, job_id);
                    match self.history.get(&key) {
                        Some(recorded_value) => {
                            match self.jobs[upstream_idx as usize].1.history_output.as_ref() {
                                Some(new_value) => {
                                    if self.strategy.is_history_altered(
                                        upstream_id,
                                        job_id,
                                        recorded_value,
                                        new_value,
                                    ) {
                                        *self.dag.edge_weight_mut(upstream_idx, idx).unwrap() =
                                            InvalidationStatus::Invalidated;
                                        return true;
                                    } else {
                                    }
                                    *self.dag.edge_weight_mut(upstream_idx, idx).unwrap() =
                                        InvalidationStatus::Validated;
                                }
                                None => {}
                            }
                        }

                        None => {
                            *self.dag.edge_weight_mut(upstream_idx, idx).unwrap() =
                                InvalidationStatus::Invalidated;
                            return true;
                        }
                    }
                }
            }
        }

        false
    }

    fn all_upstreams_done(&self, idx: u32) -> bool {
        let upstreams = self.dag.neighbors_directed(idx, Direction::Incoming);
        for upstream_idx in upstreams {
            match self.jobs[upstream_idx as usize].1.ran {
                RunStatus::Undetermined => return false,
                RunStatus::ReadyToRun => return false,
                RunStatus::Running => return false,
                RunStatus::Ran(_) => {}
            }
        }
        return true;
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
    pub fn event_job_cleanup_done(&mut self, job_id: &str) -> Result<(), PPGEvaluatorError> {
        let idx = *self.node_to_index.get(job_id).expect("Unknown job id");
        let j = &mut self.jobs[idx as usize];
        match j.1.cleanup_status {
            CleanupStatus::NotYet | CleanupStatus::Done =>
                return Err(PPGEvaluatorError::APIError(format!(
                "Cleanup on a job that either was not ready for cleanup, or already cleaned up {} {:?}",
                job_id, j.1.cleanup_status
            ))),
            CleanupStatus::Ready => {
                j.1.cleanup_status = CleanupStatus::Done;
            }
        }
        Ok(())
    }

    /// what jobs are ready to run *right now*
    pub fn ready_to_runs(&self) -> HashSet<String> {
        self.jobs
            .iter()
            .filter(|(job_id, job)| job.ran == RunStatus::ReadyToRun)
            .map(|(job_id, _)| job_id.clone())
            .collect()
    }

    pub fn ready_to_cleanup(&self) -> HashSet<String> {
        self.jobs // skip the root
            .iter()
            .filter_map(|(job_id, job)| match (job.kind, job.cleanup_status) {
                (JobKind::Ephemeral, CleanupStatus::Ready) => Some(job_id.clone()),
                (_, _) => None,
            })
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
            debug!("was not ephemeral {}", &self.jobs[job_idx as usize].0);
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
        for (idx, (job_id, job)) in self.jobs.iter().enumerate() {
            let key = job_id.to_string();
            let history = match &job.history_output {
                Some(new_history) => new_history,
                None => match self.history.get(&key) {
                    Some(old_history) => old_history,
                    None => {
                        let job_and_downstreams_are_ephmeral =
                            self.job_and_downstreams_are_ephmeral(idx as u32);
                        if job_and_downstreams_are_ephmeral
                            || job.ran == RunStatus::Ran(Finished::Failure)
                            || job.ran == RunStatus::Ran(Finished::UpstreamFailure)
                        {
                            continue;
                        } else {
                            panic!(
                                "Job was skipped but history had no entry for this job: {:?} {:?}. job_and_downstreams_are_ephmeral {}",
                                job, job.ran, job_and_downstreams_are_ephmeral

                            );
                        }
                    }
                },
            };
            out.insert(key, history.to_string());
        }
        for (a, b, x) in self.dag.all_edges() {
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

        out
    }
}

fn start_logging() {
    let start_time = chrono::Utc::now();
    if !LOGGER_INIT.is_completed() {
        LOGGER_INIT.call_once(move || {
            use colored::Colorize;
            let start_time2 = start_time.clone();
            env_logger::builder()
                .format(move |buf, record| {
                    let filename = record
                        .file()
                        .unwrap_or("unknown")
                        .trim_start_matches("src/");
                    let ff = format!("{}:{}", filename, record.line().unwrap_or(0));
                    let ff = match record.level() {
                        log::Level::Error => ff.red(),
                        log::Level::Warn => ff.yellow(),
                        log::Level::Info => ff.blue(),
                        log::Level::Debug => ff.green(),
                        log::Level::Trace => ff.normal(),
                    };

                    writeln!(
                        buf,
                        "{}\t{:.4} | {}",
                        ff,
                        (chrono::Utc::now() - start_time2).num_milliseconds(),
                        //chrono::Local::now().format("%Y-%m-%dT%H:%M:%S"),
                        record.args()
                    )
                })
                .is_test(true)
                .init()
        });
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

    pub fn jobs_ready_for_cleanup(&self) -> Vec<String> {
        self.evaluator.ready_to_cleanup().into_iter().collect()
    }

    pub fn event_job_cleanup_done(&mut self, job_id: &str) -> Result<(), PyErr> {
        Ok(self.evaluator.event_job_cleanup_done(job_id)?)
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
fn enable_logging() -> PyResult<()> {
    start_logging();
    error!("hello from rust");
    Ok(())
}

/// A Python module implemented in Rust.
#[pymodule]
fn pypipegraph2(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(enable_logging, m)?)?;
    m.add_class::<PyPPG2Evaluator>()?;
    Ok(())
}
