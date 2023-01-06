#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

use log::{debug, error, info, warn};
use petgraph::visit::{IntoNeighbors, IntoNeighborsDirected};
use petgraph::{graphmap::GraphMap, Directed, Direction};
use pyo3::exceptions::asyncio::InvalidStateError;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::Rc;

use crate::{PPGEvaluatorError, PPGEvaluatorStrategy};

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum JobKind {
    Always, // run always
    Output, //run if invalidated or output-not-present
    Ephemeral, // run if invalidated, or downstream jobs require them.
            // cleanups ain't jobs. Because they would not trigger ephemerals -
            // and that way lies complexity madness. Probably much easier to just have a callback
            // when a jobs' downstreams have all been finished
}
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Outcome {
    Sucessful,
    Failure,
    Skipped,
    UpstreamFailure,
}

trait JobQueries {
    fn is_finished(&self) -> bool;
    fn is_failed(&self) -> bool;
    fn is_skipped(&self) -> bool;
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ValidationStatus {
    Unknown,
    Validated,
    Invalidated,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum JobStateAlways {
    Undetermined,
    ReadyToRun, // ie. upstreams done.
    Running,
    FinishedSuccess,
    FinishedFailure,
    FinishedUpstreamFailure,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum JobStateOutput {
    NotReady(ValidationStatus),
    ReadyToRun,
    Running,
    FinishedSuccess,
    FinishedFailure,
    FinishedUpstreamFailure,
    FinishedSkipped,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum JobStateEphemeral {
    NotReady(ValidationStatus),
    ReadyButDelayed, //ie. we have not made a decision on whether this is a go or not.
    ReadyToRun,
    Running,
    FinishedSuccessNotReadyForCleanup,
    FinishedSuccessReadyForCleanup,
    FinishedSuccessCleanedUp,
    FinishedFailure,
    FinishedUpstreamFailure,
    FinishedSkipped,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum JobState {
    Always(JobStateAlways),
    Output(JobStateOutput),
    Ephemeral(JobStateEphemeral),
}

impl JobState {
    fn is_finished(&self) -> bool {
        match self {
            JobState::Always(x) => x.is_finished(),
            JobState::Output(x) => x.is_finished(),
            JobState::Ephemeral(x) => x.is_finished(),
        }
    }
    fn is_failed(&self) -> bool {
        match self {
            JobState::Always(x) => x.is_failed(),
            JobState::Output(x) => x.is_failed(),
            JobState::Ephemeral(x) => x.is_failed(),
        }
    }
    fn is_skipped(&self) -> bool {
        match self {
            JobState::Always(x) => x.is_skipped(),
            JobState::Output(x) => x.is_skipped(),
            JobState::Ephemeral(x) => x.is_skipped(),
        }
    }
}

impl JobQueries for JobStateAlways {
    fn is_finished(&self) -> bool {
        match self {
            JobStateAlways::Undetermined => false,
            JobStateAlways::ReadyToRun => false,
            JobStateAlways::Running => false,
            JobStateAlways::FinishedSuccess => true,
            JobStateAlways::FinishedFailure => true,
            JobStateAlways::FinishedUpstreamFailure => true,
        }
    }

    fn is_failed(&self) -> bool {
        match self {
            JobStateAlways::FinishedFailure => true,
            JobStateAlways::FinishedUpstreamFailure => true,
            _ => false,
        }
    }

    fn is_skipped(&self) -> bool {
        false
    }
}
impl JobQueries for JobStateOutput {
    fn is_finished(&self) -> bool {
        match self {
            JobStateOutput::NotReady(_) => false,
            JobStateOutput::ReadyToRun => false,
            JobStateOutput::Running => false,
            JobStateOutput::FinishedSuccess => true,
            JobStateOutput::FinishedFailure => true,
            JobStateOutput::FinishedUpstreamFailure => true,
            JobStateOutput::FinishedSkipped => true,
        }
    }

    fn is_failed(&self) -> bool {
        match self {
            JobStateOutput::FinishedFailure => true,
            JobStateOutput::FinishedUpstreamFailure => true,
            _ => false,
        }
    }

    fn is_skipped(&self) -> bool {
        match self {
            JobStateOutput::FinishedSkipped => true,
            _ => false,
        }
    }
}
impl JobQueries for JobStateEphemeral {
    fn is_finished(&self) -> bool {
        match self {
            JobStateEphemeral::NotReady(_) => false,
            JobStateEphemeral::ReadyButDelayed => false,
            JobStateEphemeral::ReadyToRun => false,
            JobStateEphemeral::Running => false,
            JobStateEphemeral::FinishedSuccessNotReadyForCleanup => true,
            JobStateEphemeral::FinishedSuccessReadyForCleanup => true,
            JobStateEphemeral::FinishedSuccessCleanedUp => true,
            JobStateEphemeral::FinishedFailure => true,
            JobStateEphemeral::FinishedUpstreamFailure => true,
            JobStateEphemeral::FinishedSkipped => true,
        }
    }

    fn is_failed(&self) -> bool {
        match self {
            JobStateEphemeral::FinishedFailure => true,
            JobStateEphemeral::FinishedUpstreamFailure => true,
            _ => false,
        }
    }

    fn is_skipped(&self) -> bool {
        match self {
            JobStateEphemeral::FinishedSkipped => true,
            _ => false,
        }
    }
}

#[derive(Copy, Clone, Debug)]
enum CleanupStatus {
    NotYet,
    Ready,
    Done,
}

#[derive(Clone, Debug)]
struct NodeInfo {
    job_id: String,
    state: JobState,
    history_output: Option<String>,
}

impl NodeInfo {
    fn set_state(&mut self, new_state: JobState) {
        debug!(
            "Updating {} from {:?} to {:?}",
            self.job_id, self.state, new_state
        );
        match (self.state, new_state) {
            (JobState::Always(_), JobState::Always(_)) => {}
            (JobState::Output(_), JobState::Output(_)) => {}
            (JobState::Ephemeral(_), JobState::Ephemeral(_)) => {}
            _ => panic!("Moving a job between kinds"),
        }
        self.state = new_state;
    }
}

/// whether a Evaluator was run
enum StartStatus {
    NotStarted,
    Running,
    Finished,
}

#[derive(Debug)]
enum Signal {
    JobMustBeDone(NodeIndex),
    JobReadyToRun(NodeIndex),
    JobFinishedSkip(NodeIndex),
    JobDone(NodeIndex),
    JobFinishedSuccess(NodeIndex),
    JobFinishedFailure(NodeIndex),
    JobUpstreamFailure(NodeIndex),
    ConsiderJob(NodeIndex),
    JobCleanedUp(NodeIndex),
}

impl Signal {
    fn job_id<'a>(&self, jobs: &'a Vec<NodeInfo>) -> &'a str {
        let node_idx = *match self {
            Signal::JobMustBeDone(n) => n,
            Signal::JobReadyToRun(n) => n,
            Signal::JobFinishedSkip(n) => n,
            Signal::JobDone(n) => n,
            Signal::JobFinishedSuccess(n) => n,
            Signal::JobFinishedFailure(n) => n,
            Signal::JobUpstreamFailure(n) => n,
            Signal::ConsiderJob(n) => n,
            Signal::JobCleanedUp(n) => n,
        };
        &jobs[node_idx as usize].job_id
    }
}

type NodeIndex = usize;

type GraphType = GraphMap<NodeIndex, (), Directed>;

pub struct PPGEvaluator<T: PPGEvaluatorStrategy> {
    dag: GraphType,
    jobs: Vec<NodeInfo>,
    job_id_to_node_idx: HashMap<String, NodeIndex>,
    history: HashMap<String, String>,
    strategy: T,
    already_started: StartStatus,
    jobs_ready_to_run: HashSet<String>,
    topo: Option<Vec<NodeIndex>>,
    signals: VecDeque<Signal>,
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
            job_id_to_node_idx: HashMap::new(),
            history,
            strategy,
            already_started: StartStatus::NotStarted,
            jobs_ready_to_run: HashSet::new(),
            topo: None,
            signals: VecDeque::new(),
        };
        //res.add_node("<ROOT>", JobKind::Always);
        res
    }

    fn id_to_idx(&self, id: &str) -> NodeIndex {
        *self
            .job_id_to_node_idx
            .get(id)
            .expect("Unknown job_id passed")
    }

    pub fn add_node(&mut self, job_id: &str, kind: JobKind) {
        assert_ne!(job_id, "");
        assert!(!job_id.contains("!!!"));
        let state = match kind {
            JobKind::Always => JobState::Always(JobStateAlways::Undetermined),
            JobKind::Output => {
                JobState::Output(JobStateOutput::NotReady(ValidationStatus::Unknown))
            }
            JobKind::Ephemeral => {
                JobState::Ephemeral(JobStateEphemeral::NotReady(ValidationStatus::Unknown))
            }
        };
        let job = NodeInfo {
            job_id: job_id.to_string(),
            state,
            history_output: None,
        };
        let idx = self.jobs.len() as NodeIndex;
        if self
            .job_id_to_node_idx
            .insert(job_id.to_string(), idx)
            .is_some()
        {
            panic!("Can not add a node twice to the evaluator");
        };
        self.jobs.push(job);
        self.dag.add_node(idx);
    }

    pub fn depends_on(&mut self, downstream: &str, upstream: &str) {
        let downstream_id = self.id_to_idx(downstream);
        let upstream_id = self.id_to_idx(upstream);
        assert_ne!(downstream_id, upstream_id, "can't depend on self");
        self.dag.add_edge(upstream_id, downstream_id, ());
    }

    pub fn is_finished(&self) -> bool {
        for job in self.jobs.iter() {
            if !job.state.is_finished() {
                return false;
            }
        }
        true
    }
    pub fn debug(&self) -> String {
        let mut out = "\n".to_string();
        for job in self.jobs.iter() {
            out.push_str(&format!("{}: {:?}\n", job.job_id, job.state));
        }
        out.push('\n');
        for (upstream_idx, downstream_idx, weight) in self.dag.all_edges() {
            let upstream_id = &self.jobs[upstream_idx as usize].job_id;
            let downstream_id = &self.jobs[downstream_idx as usize].job_id;
            out.push_str(&format!(
                "({}->{}: {:?}\n",
                upstream_id, downstream_id, weight
            ));
        }
        out
    }

    pub fn verify_order_was_topological(&self, order: &Vec<String>) -> bool {
        /*
        Iterate through the edges in G. For each edge, retrieve the index of each of its vertices
            in the ordering. Compared the indices. If the origin vertex isn't earlier than the
            destination vertex, return false. If you iterate through all of the edges without
            returning false, return true.
            */
        for (upstream_idx, downstream_idx, _) in self.dag.all_edges() {
            let upstream_id = &self.jobs[upstream_idx as usize].job_id;
            let downstream_id = &self.jobs[downstream_idx as usize].job_id;
            let upstream_in_order = order.iter().position(|x| x == upstream_id);
            let downstream_in_order = order.iter().position(|x| x == downstream_id);
            match (upstream_in_order, downstream_in_order) {
                (None, None) => {}
                (None, Some(_)) => {}
                (Some(_), None) => {}
                (Some(u), Some(o)) => {
                    if u > o {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    /// what jobs are ready to run *right now*
    pub fn query_ready_to_run(&self) -> HashSet<String> {
        self.jobs_ready_to_run.clone()
    }

    pub fn query_ready_for_cleanup(&self) -> HashSet<String> {
        self.jobs
            .iter()
            .filter_map(|job| match job.state {
                JobState::Ephemeral(JobStateEphemeral::FinishedSuccessReadyForCleanup) => {
                    Some(job.job_id.clone())
                }
                _ => None,
            })
            .collect()
    }

    pub fn query_failed(&self) -> HashSet<String> {
        self.jobs
            .iter()
            .filter_map(|job| match job.state {
                JobState::Always(JobStateAlways::FinishedFailure)
                | JobState::Output(JobStateOutput::FinishedFailure)
                | JobState::Ephemeral(JobStateEphemeral::FinishedFailure) => {
                    Some(job.job_id.clone())
                }
                _ => None,
            })
            .collect()
    }

    pub fn query_upstream_failed(&self) -> HashSet<String> {
        self.jobs
            .iter()
            .filter_map(|job| match job.state {
                JobState::Always(JobStateAlways::FinishedUpstreamFailure)
                | JobState::Output(JobStateOutput::FinishedUpstreamFailure)
                | JobState::Ephemeral(JobStateEphemeral::FinishedUpstreamFailure) => {
                    Some(job.job_id.clone())
                }
                _ => None,
            })
            .collect()
    }
    fn _job_and_downstreams_are_ephmeral(&self, job_idx: NodeIndex) -> bool {
        match self.jobs[job_idx as usize].state {
            JobState::Ephemeral(_) => {}
            JobState::Always(_) | JobState::Output(_) => {
                debug!("was not ephemeral {}", &self.jobs[job_idx as usize].job_id);
                return false;
            }
        }
        let downstreams: Vec<_> = self
            .dag
            .neighbors_directed(job_idx, Direction::Outgoing)
            .collect();
        for ds_id in downstreams {
            if !self._job_and_downstreams_are_ephmeral(ds_id) {
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
        for (idx, job) in self.jobs.iter().enumerate() {
            let key = job.job_id.to_string();
            let history = match &job.history_output {
                Some(new_history) => new_history,
                None => match self.history.get(&key) {
                    Some(old_history) => old_history,
                    None => {
                        let job_and_downstreams_are_ephmeral =
                            self._job_and_downstreams_are_ephmeral(idx as NodeIndex);
                        if job_and_downstreams_are_ephmeral || job.state.is_failed() {
                            continue;
                        } else {
                            panic!(
                                "Job was skipped but history had no entry for this job: {:?} {:?}. job_and_downstreams_are_ephmeral {}",
                                job.job_id, job.state, job_and_downstreams_are_ephmeral

                            );
                        }
                    }
                },
            };
            out.insert(key, history.to_string());
        }
        for (a, b, x) in self.dag.all_edges() {
            // the root has no sensible history.
            let job_id_a = &self.jobs[a as usize].job_id;
            let job_id_b = &self.jobs[b as usize].job_id;
            let key = format!("{}!!!{}", job_id_a.to_string(), job_id_b.to_string());
            let history = self.jobs[a as usize].history_output.as_ref();
            let history = match history {
                Some(run_history) => run_history, // we have new history.
                None => {
                    match self.jobs[a as usize].state {
                        JobState::Output(JobStateOutput::FinishedSkipped)
                        | JobState::Ephemeral(JobStateEphemeral::FinishedSkipped) => {
                            match self.history.get(&key) {
                                Some(old_run_history) => old_run_history,
                                None => {
                                    // we had no history for this edge, for the incoming job was skipped.
                                    // But we should have a history entry for the job itself, right?
                                    let job_key = job_id_a;
                                    match self.history.get(job_key) {
                                        Some(old_run_history) => old_run_history,
                                        None => match (&self.jobs[a as usize]).state {
                                            JobState::Ephemeral(_) => {
                                                continue;
                                            }
                                            _ => {
                                                panic!("No history for edge from this run, none from the prev run, and no upstream job history. Bug: {}, {}",
                                               job_id_a, job_id_b);
                                            }
                                        },
                                    }
                                }
                            }
                        }
                        JobState::Always(JobStateAlways::FinishedFailure)
                        | JobState::Always(JobStateAlways::FinishedUpstreamFailure)
                        | JobState::Output(JobStateOutput::FinishedFailure)
                        | JobState::Output(JobStateOutput::FinishedUpstreamFailure)
                        | JobState::Ephemeral(JobStateEphemeral::FinishedFailure)
                        | JobState::Ephemeral(JobStateEphemeral::FinishedUpstreamFailure) => {
                            match self.history.get(&key) {
                                //we have an old edge
                                Some(old_run_history) => old_run_history,
                                None => {
                                    continue;
                                }
                            }
                        }
                        _ => {
                            panic!(
                                    "unexpected ran state when no history was present. job_id: '{}', ran state: '{:?}'",
                                    job_id_a, self.jobs[a as usize].state
                                );
                        }
                    }
                }
            };
            out.insert(key, history.to_string());
        }

        out
    }

    pub fn event_startup(&mut self) -> Result<(), PPGEvaluatorError> {
        match self.already_started {
            StartStatus::Running | StartStatus::Finished => {
                return Err(PPGEvaluatorError::APIError("Can't start twice".to_string()));
            }
            _ => {}
        };
        self.already_started = StartStatus::Running;

        // this is not particulary fast.
        self.topo =
            Some(petgraph::algo::toposort(petgraph::visit::Reversed(&self.dag), None).unwrap());
        //self.identify_changed_input_counts();
        self.identify_missing_outputs();
        self.process_signals(0); //or they're not correctly invalidated...
                                 //self.fill_in_unfinished_downstream_counts();
                                 //self.update();
        self.start_on_roots();
        self.process_signals(0);

        Ok(())
    }
    pub fn event_now_running(&mut self, job_id: &str) -> Result<(), PPGEvaluatorError> {
        let idx = self.job_id_to_node_idx.get(job_id).expect("Unknown job id");
        let j = &mut self.jobs[*idx as usize];
        match j.state {
            JobState::Always(JobStateAlways::ReadyToRun) => {
                self.jobs_ready_to_run.remove(job_id);
                j.set_state(JobState::Always(JobStateAlways::Running));
                Ok(())
            }
            JobState::Output(JobStateOutput::ReadyToRun) => {
                self.jobs_ready_to_run.remove(job_id);
                j.set_state(JobState::Output(JobStateOutput::Running));
                Ok(())
            }
            JobState::Ephemeral(JobStateEphemeral::ReadyToRun) => {
                self.jobs_ready_to_run.remove(job_id);
                j.set_state(JobState::Ephemeral(JobStateEphemeral::Running));
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
        let idx = *self.job_id_to_node_idx.get(job_id).expect("Unknown job id");
        let j = &mut self.jobs[idx as usize];
        match j.state {
            JobState::Always(JobStateAlways::Running)
            | JobState::Output(JobStateOutput::Running)
            | JobState::Ephemeral(JobStateEphemeral::Running) => {}
            _ => {
                return Err(PPGEvaluatorError::APIError(format!(
                    "Reported a job as finished taht was not running ! {:?}",
                    j
                )))
            }
        }
        j.history_output = Some(history_to_store);
        self.signals.push_back(Signal::JobFinishedSuccess(idx));
        self.process_signals(0);
        Ok(())
    }

    pub fn event_job_finished_failure(&mut self, job_id: &str) -> Result<(), PPGEvaluatorError> {
        let idx = *self.job_id_to_node_idx.get(job_id).expect("Unknown job id");
        let j = &mut self.jobs[idx as usize];
        match j.state {
            JobState::Always(JobStateAlways::Running)
            | JobState::Output(JobStateOutput::Running)
            | JobState::Ephemeral(JobStateEphemeral::Running) => {}
            _ => {
                return Err(PPGEvaluatorError::APIError(format!(
                    "Reported a job as finished taht was not running ! {:?}",
                    j
                )))
            }
        }
        self.signals.push_back(Signal::JobFinishedFailure(idx));
        self.process_signals(0);
        Ok(())
    }
    pub fn event_job_cleanup_done(&mut self, job_id: &str) -> Result<(), PPGEvaluatorError> {
        let idx = *self.job_id_to_node_idx.get(job_id).expect("Unknown job id");
        let j = &mut self.jobs[idx as usize];
        match j.state {
            JobState::Ephemeral(JobStateEphemeral::FinishedSuccessReadyForCleanup) => {
                self.signals.push_back(Signal::JobCleanedUp(idx));
                Ok(())
            }

            _ => Err(PPGEvaluatorError::APIError(format!(
                "Cleanup on a job not ready or already cleaned up! {:?}",
                j
            ))),
        }
    }

    fn add_signals(&mut self, signals: Vec<Signal>) {
        for signal in signals.into_iter() {
            debug!("Adding signal {:?}", signal);
            self.signals.push_back(signal)
        }
    }

    fn process_signals(&mut self, depth: u32) {
        debug!("Process sinals, depth {}", depth);
        let mut new_signals = Vec::new();
        for signal in self.signals.drain(..) {
            debug!("Handling signal {} {:?}", signal.job_id(&self.jobs), signal);
            match signal {
                Signal::JobMustBeDone(node_idx) => Self::signal_job_must_be_done(
                    &self.dag,
                    &mut self.jobs,
                    node_idx,
                    &mut new_signals,
                ),
                Signal::JobReadyToRun(node_idx) => {
                    let j = &mut self.jobs[node_idx as usize];
                    match j.state {
                        JobState::Always(state) => match state {
                            JobStateAlways::Undetermined => {
                                j.set_state(JobState::Always(JobStateAlways::ReadyToRun));
                            }
                            _ => panic!("unexpected"),
                        },
                        JobState::Output(state) => match state {
                            JobStateOutput::NotReady(ValidationStatus::Invalidated) => {
                                j.set_state(JobState::Output(JobStateOutput::ReadyToRun));
                            }
                            _ => panic!("unexpected"),
                        },
                        JobState::Ephemeral(state) => match state {
                            JobStateEphemeral::NotReady(ValidationStatus::Invalidated)
                            | JobStateEphemeral::ReadyButDelayed => {
                                j.set_state(JobState::Ephemeral(JobStateEphemeral::ReadyToRun));
                            }
                            _ => panic!("unexpected"),
                        },
                    }
                    self.jobs_ready_to_run.insert(j.job_id.clone());
                }
                Signal::JobFinishedSkip(node_idx) => {
                    let j = &mut self.jobs[node_idx as usize];
                    match j.state {
                        JobState::Always(_) => panic!("skipping always job is a bug"),
                        JobState::Output(state) => match state {
                            JobStateOutput::NotReady(
                                ValidationStatus::Validated | ValidationStatus::Unknown,
                            ) => {
                                j.set_state(JobState::Output(JobStateOutput::FinishedSkipped));
                            }
                            _ => panic!("unexpected: {:?}", j),
                        },
                        JobState::Ephemeral(state) => match state {
                            JobStateEphemeral::NotReady(ValidationStatus::Validated)
                            | JobStateEphemeral::ReadyButDelayed => {
                                j.set_state(JobState::Ephemeral(
                                    JobStateEphemeral::FinishedSkipped,
                                ));
                            }
                            _ => panic!("unexpected {:?}", j),
                        },
                    }
                    new_signals.push(Signal::JobDone(node_idx));
                }
                Signal::JobDone(node_idx) => {
                    // todo : call directly from JobFinished*
                    let j = &mut self.jobs[node_idx as usize];
                    if !j.state.is_finished() {
                        panic!("job_done on job not finished");
                    }
                    let downstreams = self.dag.neighbors_directed(node_idx, Direction::Outgoing);
                    for downstream_idx in downstreams {
                        debug!("ConsiderJob place 1");
                        new_signals.push(Signal::ConsiderJob(downstream_idx));
                    }
                    Self::consider_upstreams_for_cleanup(
                        &self.dag,
                        &mut self.jobs,
                        node_idx,
                        &mut new_signals,
                    );
                }
                Signal::JobFinishedSuccess(node_idx) => {
                    let j = &mut self.jobs[node_idx as usize];
                    match j.state {
                        JobState::Always(JobStateAlways::Running) => {
                            j.set_state(JobState::Always(JobStateAlways::FinishedSuccess));
                        }
                        JobState::Output(JobStateOutput::Running) => {
                            j.set_state(JobState::Output(JobStateOutput::FinishedSuccess));
                        }
                        JobState::Ephemeral(JobStateEphemeral::Running) => {
                            j.set_state(JobState::Ephemeral(
                                JobStateEphemeral::FinishedSuccessNotReadyForCleanup,
                            ));
                        }
                        _ => panic!("unexpected"),
                    }
                    new_signals.push(Signal::JobDone(node_idx));
                }
                Signal::JobFinishedFailure(node_idx) => {
                    let j = &mut self.jobs[node_idx as usize];
                    match j.state {
                        JobState::Always(JobStateAlways::Running) => {
                            j.set_state(JobState::Always(JobStateAlways::FinishedFailure));
                        }
                        JobState::Output(JobStateOutput::Running) => {
                            j.set_state(JobState::Output(JobStateOutput::FinishedFailure));
                        }
                        JobState::Ephemeral(JobStateEphemeral::Running) => {
                            j.set_state(JobState::Ephemeral(JobStateEphemeral::FinishedFailure));
                        }
                        _ => panic!("unexpected"),
                    }
                    new_signals.push(Signal::JobDone(node_idx));
                    let downstreams = self.dag.neighbors_directed(node_idx, Direction::Outgoing);
                    for downstream_idx in downstreams {
                        new_signals.push(Signal::JobUpstreamFailure(downstream_idx));
                    }
                }
                Signal::JobUpstreamFailure(node_idx) => {
                    let j = &mut self.jobs[node_idx as usize];
                    match j.state {
                        JobState::Always(JobStateAlways::Undetermined) => {
                            j.set_state(JobState::Always(JobStateAlways::FinishedUpstreamFailure));
                        }
                        JobState::Output(JobStateOutput::NotReady(_)) => {
                            j.set_state(JobState::Output(JobStateOutput::FinishedUpstreamFailure));
                        }
                        JobState::Ephemeral(JobStateEphemeral::NotReady(_)) => {
                            j.set_state(JobState::Ephemeral(
                                JobStateEphemeral::FinishedUpstreamFailure,
                            ));
                        }
                        _ => panic!("unexpected: {:?}", j),
                    }
                    new_signals.push(Signal::JobDone(node_idx));
                    let downstreams = self.dag.neighbors_directed(node_idx, Direction::Outgoing);
                    for downstream_idx in downstreams {
                        new_signals.push(Signal::JobUpstreamFailure(downstream_idx));
                    }
                }
                Signal::ConsiderJob(node_idx) => {
                    Self::signal_consider_job(
                        &self.strategy,
                        &self.dag,
                        &mut self.jobs,
                        &self.history,
                        &self.topo.as_ref().unwrap(),
                        node_idx,
                        &mut new_signals,
                    );
                }
                Signal::JobCleanedUp(node_idx) => {
                    let j = &mut self.jobs[node_idx as usize];
                    match j.state {
                        JobState::Ephemeral(JobStateEphemeral::FinishedSuccessReadyForCleanup) => {
                            j.set_state(JobState::Ephemeral(
                                JobStateEphemeral::FinishedSuccessCleanedUp,
                            ));
                        }
                        _ => panic!("unexpected"),
                    }
                }
            }
        }
        if !new_signals.is_empty() {
            debug!("New signals after process_signals - adding and processing");
            for s in new_signals.into_iter() {
                debug!("\t {:?} {}", s, s.job_id(&self.jobs));
                // we have to preempt the case that two jobs were 'JobFinished'
                // in the same loop, pushed the same ConsiderJob
                // and now we make the same conclusion twice
                self.signals.push_front(s);
            }
            //self.signals.extend(new_signals.drain(..));
        }
        if !self.signals.is_empty() {
            self.process_signals(depth + 1);
        }
    }

    fn all_upstreams_done(dag: &GraphType, jobs: &mut Vec<NodeInfo>, node_idx: NodeIndex) -> bool {
        let upstreams = dag.neighbors_directed(node_idx, Direction::Incoming);
        for upstream_idx in upstreams {
            if !jobs[upstream_idx as usize].state.is_finished() {
                return false;
            }
        }
        true
    }

    fn edge_invalidated(
        strategy: &dyn PPGEvaluatorStrategy,
        jobs: &mut Vec<NodeInfo>,
        history: &HashMap<String, String>,
        upstream_idx: NodeIndex,
        downstream_idx: NodeIndex,
    ) -> bool {
        let upstream_id = &jobs[upstream_idx].job_id;
        let downstream_id = &jobs[downstream_idx].job_id;
        let key = format!("{}!!!{}", upstream_id, downstream_id); //todo: express onlyonce
        let last_history_value = history.get(&key); //todo: fight alloc
        match last_history_value {
            Some(last_history_value) => {
                let current_value = jobs[upstream_idx]
                    .history_output
                    .as_ref()
                    .expect(&format!("No history? Unexpected {:?}", &jobs[upstream_idx]));
                strategy.is_history_altered(
                    upstream_id,
                    &downstream_id,
                    last_history_value,
                    current_value,
                )
            }

            None => true,
        }
    }

    fn update_validation_status(
        strategy: &dyn PPGEvaluatorStrategy,
        dag: &GraphType,
        jobs: &mut Vec<NodeInfo>,
        history: &HashMap<String, String>,
        node_idx: NodeIndex,
    ) -> ValidationStatus {
        let upstreams = dag.neighbors_directed(node_idx, Direction::Incoming);
        let mut all_done = true;
        let mut invalidated = false;
        for upstream_idx in upstreams {
            if jobs[upstream_idx as usize].state.is_finished() {
                if !jobs[upstream_idx as usize].state.is_skipped()
                    && Self::edge_invalidated(strategy, jobs, history, upstream_idx, node_idx)
                {
                    invalidated = true
                } else {
                    //       debug!("\t\tEdge not invalidated {} {}", jobs[upstream_idx].job_id, jobs[node_idx].job_id);
                }
            } else {
                all_done = false;
            }
        }
        if all_done {
            if invalidated {
                ValidationStatus::Invalidated
            } else {
                ValidationStatus::Validated
            }
        } else {
            if invalidated {
                ValidationStatus::Invalidated
            } else {
                ValidationStatus::Unknown
            }
        }
    }

    fn update_validation_status_but_delayed_is_validated(
        strategy: &dyn PPGEvaluatorStrategy,
        dag: &GraphType,
        jobs: &mut Vec<NodeInfo>,
        history: &HashMap<String, String>,
        node_idx: NodeIndex,
    ) -> ValidationStatus {
        let upstreams = dag.neighbors_directed(node_idx, Direction::Incoming);
        let mut all_done = true;
        let mut invalidated = false;
        for upstream_idx in upstreams {
            if jobs[upstream_idx as usize].state.is_finished() {
                if !jobs[upstream_idx as usize].state.is_skipped()
                    && Self::edge_invalidated(strategy, jobs, history, upstream_idx, node_idx)
                {
                    invalidated = true
                } else {
                    //       debug!("\t\tEdge not invalidated {} {}", jobs[upstream_idx].job_id, jobs[node_idx].job_id);
                }
            } else {
                if jobs[upstream_idx].state
                    == JobState::Ephemeral(JobStateEphemeral::ReadyButDelayed)
                {
                } else {
                    all_done = false;
                }
            }
        }
        if all_done {
            if invalidated {
                ValidationStatus::Invalidated
            } else {
                ValidationStatus::Validated
            }
        } else {
            if invalidated {
                ValidationStatus::Invalidated
            } else {
                ValidationStatus::Unknown
            }
        }
    }

    fn find_last_ephemeral(
        dag: &GraphType,
        topo: &Vec<NodeIndex>,
        node_idx: NodeIndex,
    ) -> NodeIndex {
        //todo: make this fast, maybe?
        //could store topo index on job?
        let mut max_topo_index = None;
        let mut max_topo_element = None;
        let upstreams = dag.neighbors_directed(node_idx, Direction::Incoming);
        for upstream_idx in upstreams {
            let topo_index = topo
                .iter()
                .enumerate()
                .find(|(_topo_idx, x)| **x == upstream_idx)
                .map(|(topo_idx, _)| topo_idx)
                .unwrap();
            if topo_index > max_topo_index.unwrap_or(0) {
                max_topo_index = Some(topo_index);
                max_topo_element = Some(upstream_idx);
            }
        }
        max_topo_element.unwrap()
    }
    fn check_validation_transitive_ephemerals(
        strategy: &dyn PPGEvaluatorStrategy,
        dag: &GraphType,
        jobs: &mut Vec<NodeInfo>,
        history: &HashMap<String, String>,
        topo: &Vec<NodeIndex>,
        node_idx: NodeIndex,
    ) -> ValidationStatus {
        match jobs[node_idx as usize].state {
            JobState::Ephemeral(_) => {}
            _ => panic!("should not happen"),
        }

        // you must assume that node_idx state is
        // JobStateEphemeral::ReadyButDelayed
        // even though that's not true for the 'inners'
        // that we transistively cross
        let mut all_validated = true;
        let downstreams = dag.neighbors_directed(node_idx, Direction::Outgoing);
        for downstream_idx in downstreams {
            debug!(
                "\t looking at {} {:?}",
                jobs[downstream_idx].job_id, jobs[downstream_idx].state
            );
            match jobs[downstream_idx].state {
                JobState::Always(_) => return ValidationStatus::Invalidated,
                JobState::Output(ds) => match ds {
                    JobStateOutput::NotReady(ValidationStatus::Invalidated) => {
                        return ValidationStatus::Invalidated
                    }
                    JobStateOutput::NotReady(ValidationStatus::Validated) => continue,
                    JobStateOutput::NotReady(ValidationStatus::Unknown) => {
                        let vs = Self::update_validation_status_but_delayed_is_validated(
                            strategy,
                            dag,
                            jobs,
                            history,
                            downstream_idx,
                        );
                        match vs {
                            ValidationStatus::Unknown => {
                                debug!("case last_ephemeral_for_downstream test");
                                /* if this ephemeral we're looking at is the
                                last ephemeral before the downstream_idx
                                in the topological order
                                and we're only missing ephemerals to decide
                                on downstream_idx, then we can presume it validated! */
                                let last_ephemeral_for_downstream =
                                    Self::find_last_ephemeral(dag, topo, downstream_idx);
                                if last_ephemeral_for_downstream == node_idx {
                                    //this is wrong wrong wrong wrong... you're wrong
                                    //presume validated
                                } else {
                                    all_validated = false;
                                }
                            }
                            ValidationStatus::Validated => continue,
                            ValidationStatus::Invalidated => return ValidationStatus::Invalidated,
                        };
                    }
                    _ => panic!("should not happen"),
                },
                JobState::Ephemeral(_) => {
                    match Self::check_validation_transitive_ephemerals(
                        strategy,
                        dag,
                        jobs,
                        history,
                        topo,
                        downstream_idx,
                    ) {
                        ValidationStatus::Unknown => all_validated = false,
                        ValidationStatus::Validated => {}
                        ValidationStatus::Invalidated => return ValidationStatus::Invalidated,
                    }
                }
            }
        }
        if all_validated {
            ValidationStatus::Validated
        } else {
            ValidationStatus::Unknown
        }
    }

    fn signal_consider_job(
        strategy: &dyn PPGEvaluatorStrategy,
        dag: &GraphType,
        jobs: &mut Vec<NodeInfo>,
        history: &HashMap<String, String>,
        topo: &Vec<NodeIndex>,
        node_idx: NodeIndex,
        new_signals: &mut Vec<Signal>,
    ) {
        //let j = &jobs[node_idx as usize];
        debug!(
            "considering job {}: {:?}",
            jobs[node_idx as usize].job_id, jobs[node_idx as usize].state
        );
        match jobs[node_idx as usize].state {
            JobState::Always(state) => match state {
                JobStateAlways::Undetermined => {
                    if Self::all_upstreams_done(dag, jobs, node_idx) {
                        new_signals.push(Signal::JobReadyToRun(node_idx));
                    }
                }
                _ => {}
            },
            JobState::Output(state) => match state {
                JobStateOutput::NotReady(validation_state) => {
                    match validation_state {
                        ValidationStatus::Unknown => {
                            match Self::update_validation_status(
                                strategy, dag, jobs, history, node_idx,
                            ) {
                                ValidationStatus::Unknown => {
                                    debug!("\tstill unknown validation status");
                                }
                                solid_vs => {
                                    jobs[node_idx as usize].set_state(JobState::Output(
                                        JobStateOutput::NotReady(solid_vs),
                                    ));
                                    // not the most elegant control flow, but ok.
                                    debug!("ConsiderJob place 2");
                                    new_signals.push(Signal::ConsiderJob(node_idx));
                                }
                            }
                        }
                        ValidationStatus::Validated => {
                            // can only be validated iff all_upstreams_done, right?
                            // todo remove assert
                            assert!(Self::all_upstreams_done(dag, jobs, node_idx));
                            //if Self::all_upstreams_done(dag, jobs, node_idx) {
                            debug!("JobFinishedSkip place 1");
                            new_signals.push(Signal::JobFinishedSkip(node_idx))
                            //}
                        }
                        ValidationStatus::Invalidated => {
                            if Self::all_upstreams_done(dag, jobs, node_idx) {
                                new_signals.push(Signal::JobReadyToRun(node_idx))
                            } else {
                                new_signals.push(Signal::JobMustBeDone(node_idx));
                            }
                        }
                    }
                }
                _ => {}
            },
            JobState::Ephemeral(state) => match state {
                JobStateEphemeral::NotReady(ValidationStatus::Invalidated) => {
                    if Self::all_upstreams_done(dag, jobs, node_idx) {
                        new_signals.push(Signal::JobReadyToRun(node_idx));
                    }
                }
                JobStateEphemeral::NotReady(ValidationStatus::Validated) => {
                    jobs[node_idx as usize]
                        .set_state(JobState::Ephemeral(JobStateEphemeral::ReadyButDelayed));

                    debug!("ConsiderJob place 3");
                    new_signals.push(Signal::ConsiderJob(node_idx));
                }
                JobStateEphemeral::NotReady(ValidationStatus::Unknown) => {
                    match Self::update_validation_status(strategy, dag, jobs, history, node_idx) {
                        ValidationStatus::Unknown => {
                            debug!("\tstill unknown validation status");
                        }
                        solid_vs => {
                            jobs[node_idx as usize].set_state(JobState::Ephemeral(
                                JobStateEphemeral::NotReady(solid_vs),
                            ));
                            // not the most elegant control flow, but ok.
                            //
                            debug!("ConsiderJob place 4");
                            new_signals.push(Signal::ConsiderJob(node_idx));
                        }
                    }
                }
                JobStateEphemeral::ReadyButDelayed => {
                    // this Ephemeral is validated
                    // and ready to run.
                    // but we are not sure whether it *needs* to run actually
                    // so we examine the downstreams
                    let t_vs = Self::check_validation_transitive_ephemerals(
                        strategy, dag, jobs, history, topo, node_idx,
                    );
                    debug!(
                        "\t check_validation_transitive_ephemerals {} -> {:?}",
                        jobs[node_idx as usize].job_id, t_vs
                    );

                    match t_vs {
                        ValidationStatus::Unknown => {}
                        ValidationStatus::Validated => {
                            debug!("JobFinishedSkip place 2");
                            new_signals.push(Signal::JobFinishedSkip(node_idx))
                        }
                        ValidationStatus::Invalidated => {
                            new_signals.push(Signal::JobReadyToRun(node_idx));
                        }
                    }
                    //what we need to do is check the downstreams,
                    //politely ignoring an Ephmerals along the way,
                    //ie take thier downstreams instead...
                    //and replacing their Unkown with our ReadyToRunButDelayed
                }
                _ => {}
            },
        }

        //the meat of the matter. Look at this job, and decide whether we can advance it.
    }

    fn signal_job_must_be_done(
        dag: &GraphType,
        jobs: &mut Vec<NodeInfo>,
        node_idx: NodeIndex,
        new_signals: &mut Vec<Signal>,
    ) {
        let job = &mut jobs[node_idx as usize];
        match job.state {
            JobState::Always(JobStateAlways::Undetermined) => {}
            JobState::Always(_) => panic!("unexpected"),
            JobState::Output(output_state) => match output_state {
                JobStateOutput::NotReady(_) => {
                    debug!("\t setting {} to invalidated", job.job_id);
                    job.set_state(JobState::Output(JobStateOutput::NotReady(
                        ValidationStatus::Invalidated,
                    )));
                }
                _ => {}
            },

            JobState::Ephemeral(ephemeral_state) => match ephemeral_state {
                JobStateEphemeral::NotReady(_) => {
                    debug!("\t setting {} to invalidated", job.job_id);
                    job.set_state(JobState::Ephemeral(JobStateEphemeral::NotReady(
                        ValidationStatus::Invalidated,
                    )));
                }
                JobStateEphemeral::ReadyButDelayed => {
                    new_signals.push(Signal::JobReadyToRun(node_idx));
                }
                _ => {}
            },
        };

        let upstreams = dag.neighbors_directed(node_idx, Direction::Incoming);
        for upstream_idx in upstreams {
            let upstream_job = &mut jobs[upstream_idx as usize];
            match upstream_job.state {
                JobState::Ephemeral(ephmeral_state) => match ephmeral_state {
                    JobStateEphemeral::NotReady(_) | JobStateEphemeral::ReadyButDelayed => {
                        debug!("JobMustBeDone case 1");
                        new_signals.push(Signal::JobMustBeDone(upstream_idx));
                    }
                    _ => {}
                },
                _ => {}
            }
        }
    }
    fn consider_upstreams_for_cleanup(
        dag: &GraphType,
        jobs: &mut Vec<NodeInfo>,
        node_idx: NodeIndex,
        new_signals: &mut Vec<Signal>,
    ) {
        let upstreams = dag.neighbors_directed(node_idx, Direction::Incoming);
        for upstream_idx in upstreams {
            match jobs[upstream_idx].state {
                JobState::Ephemeral(JobStateEphemeral::FinishedSuccessNotReadyForCleanup) => {
                    let mut all_downstreams_done = true;
                    let downstreams = dag.neighbors_directed(upstream_idx, Direction::Outgoing);
                    for downstream_idx in downstreams {
                        if !jobs[downstream_idx].state.is_finished() {
                            all_downstreams_done = false;
                            break;
                        }
                    }
                    if all_downstreams_done {
                        debug!("Job ready for cleanup {:?}", jobs[upstream_idx]);
                        jobs[upstream_idx].set_state(JobState::Ephemeral(
                            JobStateEphemeral::FinishedSuccessReadyForCleanup,
                        ))
                    }
                }
                _ => {}
            };
        }
    }

    fn identify_missing_outputs(&mut self) {
        let mut out_signals = Vec::new();
        for (node_idx, job) in
            self.jobs
                .iter()
                .enumerate()
                .filter(|(node_idx, job)| match job.state {
                    JobState::Always(_) => true,
                    JobState::Output(_) => {
                        if !self.strategy.output_already_present(&job.job_id) {
                            debug!("output missing on {} {}", job.job_id, node_idx);
                            true
                        } else {
                            false
                        }
                    }
                    _ => false,
                })
        {
            debug!("JobMustBeDone case 2");
            out_signals.push(Signal::JobMustBeDone(node_idx))
        }
        self.add_signals(out_signals);
    }

    fn start_on_roots(&mut self) {
        let mut out_signals = Vec::new();
        for root_idx in self.dag.nodes().filter(|node_idx| {
            !self
                .dag
                .neighbors_directed(*node_idx, Direction::Incoming)
                .next()
                .is_some()
        }) {
            let job = &self.jobs[root_idx as usize];
            debug!("root node '{}'", job.job_id);
            debug!("ConsiderJob place 5");
            out_signals.push(Signal::ConsiderJob(root_idx));
            /* match job.state {
                JobState::Always(state) => match state {
                    JobStateAlways::Undetermined => {
                        out_signals.push(Signal::JobReadyToRun(root_idx));
                    }
                    _ => panic!("unexpected"),
                },
                JobState::Output(state) => {
                    match state {
                        JobStateOutput::NotReady(validation_state) => {
                            match validation_state {
                                ValidationStatus::Invalidated => {
                                    //output was missing
                                    out_signals.push(Signal::JobReadyToRun(root_idx));
                                }
                                ValidationStatus::Unknown => {
                                    //output was not missing.
                                    out_signals.push(Signal::JobFinishedSkip(root_idx));
                                }
                                ValidationStatus::Validated => {
                                    panic!("unexpected")
                                }
                            }
                        }
                        _ => panic!("unexpected"),
                    }
                }
                JobState::Ephemeral(state) => {
                    match state {
                        JobStateEphemeral::NotReady(validation_state) => {
                            match validation_state {
                                ValidationStatus::Invalidated => {
                                    //downstream needs this.
                                    out_signals.push(Signal::JobReadyToRun(root_idx));
                                }

                                ValidationStatus::Unknown => {}
                                ValidationStatus::Validated => {
                                    panic!("I don't expect this to occur.")
                                }
                            }
                        }
                        _ => panic!("unexpected"),
                    }
                }
            } */
        }
        self.add_signals(out_signals);
    }
}
