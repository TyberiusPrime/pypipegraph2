#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

use log::{debug, error, info, warn};
use petgraph::visit::{IntoNeighbors, IntoNeighborsDirected};
use petgraph::{graphmap::GraphMap, Directed, Direction};
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
        self.process_signals(); //or they're not correctly invalidated...
                                //self.fill_in_unfinished_downstream_counts();
                                //self.update();
        self.start_on_roots();
        self.process_signals();

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
        self.process_signals();
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
        self.process_signals();
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

    fn process_signals(&mut self) {
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
            for s in new_signals.iter() {
                debug!("\t {:?} {}", s, s.job_id(&self.jobs));
            }
            self.signals.extend(new_signals.drain(..));
            self.process_signals();
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
    fn check_validation_status_ignoring_ready_but_delayed_ephemerals(
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
                match jobs[upstream_idx as usize].state {
                    JobState::Ephemeral(JobStateEphemeral::ReadyButDelayed) => {}
                    _ => {
                        all_done = false;
                    }
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

    fn signal_consider_job(
        strategy: &dyn PPGEvaluatorStrategy,
        dag: &GraphType,
        jobs: &mut Vec<NodeInfo>,
        history: &HashMap<String, String>,
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
                                    new_signals.push(Signal::ConsiderJob(node_idx));
                                }
                            }
                        }
                        ValidationStatus::Validated => {
                            // can only be validated iff all_upstreams_done, right?
                            // todo remove assert
                            assert!(Self::all_upstreams_done(dag, jobs, node_idx));
                            //if Self::all_upstreams_done(dag, jobs, node_idx) {
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
                            new_signals.push(Signal::ConsiderJob(node_idx));
                        }
                    }
                }
                JobStateEphemeral::ReadyButDelayed => {
                    // this Ephemeral is validated
                    // and ready to run.
                    // but we are not sure whether it *needs* to run actually
                    // so we examine the downstreams
                    let mut none_required_us = true;
                    let mut any_undecided = false;
                    let downstreams = dag.neighbors_directed(node_idx, Direction::Outgoing);
                    for downstream_idx in downstreams {
                        let downstream_state = jobs[downstream_idx as usize].state;
                        match downstream_state {
                            JobState::Always(_) => {
                                none_required_us = false;
                                break;
                            }
                            JobState::Output(ds) => match ds {
                                JobStateOutput::NotReady(ValidationStatus::Unknown) => {
                                    let new_vs = Self::check_validation_status_ignoring_ready_but_delayed_ephemerals(strategy, dag,jobs, history, downstream_idx);
                                    debug!(
                                        "\t\t any_undecided because of {}. new_vs: {:?}",
                                        jobs[downstream_idx].job_id, new_vs
                                    );
                                    match new_vs {
                                        ValidationStatus::Unknown => any_undecided = true,
                                        ValidationStatus::Validated => {}
                                        ValidationStatus::Invalidated => {}
                                    };
                                    //new_signals.push(Signal::ConsiderJob(downstream_idx));

                                    //break; //todo test with those in
                                }
                                JobStateOutput::NotReady(ValidationStatus::Validated) => {}
                                JobStateOutput::NotReady(ValidationStatus::Invalidated) => {
                                    debug!(
                                        "\t\t none_required_us because of {}",
                                        jobs[downstream_idx].job_id
                                    );
                                    none_required_us = false;
                                    //break;
                                }
                                JobStateOutput::ReadyToRun
                                | JobStateOutput::Running
                                | JobStateOutput::FinishedSuccess
                                | JobStateOutput::FinishedFailure
                                | JobStateOutput::FinishedUpstreamFailure
                                | JobStateOutput::FinishedSkipped => panic!("should not happen"),
                            },
                            JobState::Ephemeral(ds) => match ds {
                                JobStateEphemeral::NotReady(ValidationStatus::Unknown) => {
                                    let new_vs = Self::check_validation_status_ignoring_ready_but_delayed_ephemerals(strategy, dag,jobs, history, downstream_idx);
                                    debug!(
                                        "\t\t any_undecided because of {} new_vs {:?}",
                                        jobs[downstream_idx].job_id, new_vs
                                    );
                                    match new_vs {
                                        ValidationStatus::Unknown => any_undecided = true,
                                        ValidationStatus::Validated => {}
                                        ValidationStatus::Invalidated => {}
                                    };
                                }
                                JobStateEphemeral::NotReady(ValidationStatus::Validated) => {}
                                JobStateEphemeral::NotReady(ValidationStatus::Invalidated) => {
                                    debug!(
                                        "\t\t none_required_us because of {}",
                                        jobs[downstream_idx].job_id
                                    );
                                    none_required_us = false;
                                    //break;
                                }
                                JobStateEphemeral::ReadyButDelayed
                                | JobStateEphemeral::ReadyToRun
                                | JobStateEphemeral::Running
                                | JobStateEphemeral::FinishedSuccessNotReadyForCleanup
                                | JobStateEphemeral::FinishedSuccessReadyForCleanup
                                | JobStateEphemeral::FinishedSuccessCleanedUp
                                | JobStateEphemeral::FinishedFailure
                                | JobStateEphemeral::FinishedUpstreamFailure
                                | JobStateEphemeral::FinishedSkipped => {
                                    panic!("should not happen");
                                }
                            },
                        }
                    }
                    debug!(
                        "\t ReadyButDelayed {} result: none_required_us: {}, any_undecided: {}",
                        jobs[node_idx as usize].job_id, none_required_us, any_undecided
                    );
                    match (none_required_us, any_undecided) {
                        (_, true) => {} //can,t advance
                        (true, false) => new_signals.push(Signal::JobFinishedSkip(node_idx)),
                        (false, false) => new_signals.push(Signal::JobReadyToRun(node_idx)),
                    }
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

/*

fn job_finished(&mut self, idx: u32, outcome: Finished) {
    debug!(
        "job finished {:?} -> {:?}",
        self.jobs[idx as usize], outcome
    );
    self.jobs_ready_to_run.remove(&self.jobs[idx as usize].0);
    self.jobs[idx as usize].1.ran = RunStatus::Ran(outcome);
    self.job_finished_handle_downstream(idx, outcome);
    self.job_finished_handle_upstream(idx, outcome);
    if self.is_finished() {
        self.already_started = StartStatus::Finished; // todo : find a better way to evaluate this
    }
}

fn examine_upstream_edges(&self, node_idx: u32) -> UpstreamStatus {
    let upstreams = self.dag.neighbors_directed(node_idx, Direction::Incoming);
    let mut all_validated = true;
    for upstream_idx in upstreams {
        let e = self.dag.edge_weight(upstream_idx, node_idx).unwrap();
        /* debug!("{} -> {} {:?}", self.jobs[upstream_idx as usize].0,
        self.jobs[node_idx as usize].0,
        e); */
        match e.validation_status {
            InvalidationStatus::Unknown => {
                return UpstreamStatus::Unfinished;
            }
            InvalidationStatus::Invalidated => {
                all_validated = false;
            }
            InvalidationStatus::Validated => {}
            InvalidationStatus::Skipped => {}
            InvalidationStatus::Failed => {
                return UpstreamStatus::Failed;
            }
        }
    }
    if all_validated {
        UpstreamStatus::AllValidated
    } else {
        UpstreamStatus::AtLeastOneInvalidated
    }
}

fn examine_downstream_edges(&self, node_idx: u32) -> DownstreamStatus {
    let downstreams = self.dag.neighbors_directed(node_idx, Direction::Outgoing);
    for downstream_idx in downstreams {
        let e = self.dag.edge_weight(node_idx, downstream_idx).unwrap();
        //debug!("examine_downstream_edges for {}, {}: {:?}", self.jobs[node_idx as usize].0, self.jobs[downstream_idx as usize].0, e);

        match e.downstream_required {
            Some(true) => return DownstreamStatus::Required,
            None => return DownstreamStatus::Unknown,
            Some(false) => {}
        }
    }
    DownstreamStatus::CanBeSkipped
}

fn upstreams_finished_but_for_ephemerals(&self, node_idx: u32) -> bool {
    let upstreams = self.dag.neighbors_directed(node_idx, Direction::Incoming);
    for upstream_idx in upstreams {
        let e = self.dag.edge_weight(upstream_idx, node_idx).unwrap();
        match e.validation_status {
            InvalidationStatus::Unknown => {
                if self.jobs[upstream_idx as usize].1.kind == JobKind::Ephemeral {
                    match self.examine_downstream_edges(upstream_idx) {
                        DownstreamStatus::Required => return false,
                        DownstreamStatus::CanBeSkipped => {}
                        DownstreamStatus::Unknown => return false,
                    }
                } else {
                    return false;
                }
            }
            InvalidationStatus::Invalidated => {
                return false;
            }
            _ => {}
        }
    }
    true
}

fn update(&mut self) {
    let mut jobs_to_ready: Vec<u32> = Vec::new();
    let mut jobs_to_mark_finished: Vec<u32> = Vec::new();
    //for (node_idx, (job_id, job)) in self.jobs.iter().enumerate() {
    for node_idx in self.topo.as_ref().unwrap() {
        let node_idx = *node_idx;
        let job_id = &self.jobs[node_idx as usize].0;
        let job = &self.jobs[node_idx as usize].1;
        if !job.is_finished() && !job.is_running() {
            let ee = self.examine_upstream_edges(node_idx as u32);
            debug!("job: {}, upstream_edges: {:?}", &job_id, ee);
            match ee {
                UpstreamStatus::Unfinished => match job.kind {
                    JobKind::Always => {}
                    JobKind::Output => {
                        if self.upstreams_finished_but_for_ephemerals(node_idx)
                            && self.strategy.output_already_present(job_id)
                        {
                            jobs_to_mark_finished.push(node_idx);
                        }
                    }
                    JobKind::Ephemeral => {
                        let dd = self.examine_downstream_edges(node_idx);
                        debug!("dd2 {:?}", dd);
                        match dd {
                            DownstreamStatus::Required => {}
                            DownstreamStatus::CanBeSkipped => {
                                jobs_to_mark_finished.push(node_idx)
                            }
                            DownstreamStatus::Unknown => {}
                        }
                    }
                },
                UpstreamStatus::AllValidated => match job.kind {
                    JobKind::Always => {
                        jobs_to_ready.push(node_idx);
                    }
                    JobKind::Output => {
                        if self.strategy.output_already_present(job_id)
                            && !job.no_of_inputs_changed
                        {
                            jobs_to_mark_finished.push(node_idx);
                        } else {
                            jobs_to_ready.push(node_idx);
                        }
                    }
                    JobKind::Ephemeral => {
                        let dd = self.examine_downstream_edges(node_idx);
                        debug!("dd {:?}", dd);
                        debug!("{}", self.debug());
                        match dd {
                            DownstreamStatus::Required => jobs_to_ready.push(node_idx),
                            DownstreamStatus::CanBeSkipped => {
                                jobs_to_mark_finished.push(node_idx)
                            }
                            DownstreamStatus::Unknown => {}
                        }
                    }
                },
                UpstreamStatus::AtLeastOneInvalidated => match job.kind {
                    JobKind::Always | JobKind::Output => jobs_to_ready.push(node_idx),
                    JobKind::Ephemeral => {
                        if self.no_downstreams(node_idx) {
                            debug!("No downstreams");
                            jobs_to_mark_finished.push(node_idx)
                        } else {
                            debug!("had downstreams");
                            jobs_to_ready.push(node_idx);
                        }
                    }
                },
                UpstreamStatus::Failed => {}
            }
        }
    }
    for node_idx in jobs_to_ready {
        self.jobs[node_idx as usize].1.ran = RunStatus::ReadyToRun;
        self.jobs_ready_to_run
            .insert(self.jobs[node_idx as usize].0.clone());
    }
    for node_idx in jobs_to_mark_finished.iter() {
        self.job_finished(*node_idx, Finished::Skipped);
    }
    if !jobs_to_mark_finished.is_empty() {
        self.update();
    }
}

fn no_downstreams(&self, node_idx: u32) -> bool {
    let mut downstreams = self.dag.neighbors_directed(node_idx, Direction::Outgoing);
    match downstreams.next() {
        Some(_) => false,
        None => true,
    }
}

fn job_finished_handle_upstream(&mut self, node_idx: u32, outcome: Finished) {
    match outcome {
        Finished::Sucessful | Finished::Skipped => {
            let upstreams: Vec<_> = self
                .dag
                .neighbors_directed(node_idx, Direction::Incoming)
                .collect();
            for upstream_idx in upstreams {
                if self.jobs[upstream_idx as usize].1.kind == JobKind::Ephemeral {
                    self.jobs[upstream_idx as usize].1.unfinished_downstreams =
                        match self.jobs[upstream_idx as usize].1.unfinished_downstreams {
                            UnfinishedDownstreams::Count(x) => {
                                UnfinishedDownstreams::Count(x - 1)
                            }
                            UnfinishedDownstreams::Unknown => panic!("Should not happen"),
                            UnfinishedDownstreams::CleanupAlreadyHandled => {
                                panic!("Should not happen,")
                            }
                        }
                }
                self.dag
                    .edge_weight_mut(upstream_idx, node_idx)
                    .unwrap()
                    .downstream_required = Some(false);
            }
        }
        Finished::Failure => {}
        Finished::UpstreamFailure => {}
    }
}

fn job_finished_handle_downstream(&mut self, idx: u32, outcome: Finished) {
    let downstreams: Vec<_> = self
        .dag
        .neighbors_directed(idx, Direction::Outgoing)
        .collect();
    let job_id = self.jobs[idx as usize].0.clone();
    //.expect("Job was finished::Sucessful, but without history_output");

    for downstream_idx in downstreams {
        let e = self.dag.edge_weight_mut(idx, downstream_idx).unwrap();
        match outcome {
            Finished::Sucessful => {
                let downstream_job_id = &self.jobs[downstream_idx as usize].0;
                let key = format!("{}!!!{}", job_id, downstream_job_id);
                let last_history_value = self.history.get(&key); //todo: fight alloc
                match last_history_value {
                    None => {
                        e.validation_status = InvalidationStatus::Invalidated;
                        self.propagate_upstream_required(downstream_idx);
                    }
                    Some(last_history) => {
                        let history_output = &self.jobs[idx as usize].1.history_output.as_ref();
                        if self.strategy.is_history_altered(
                            &job_id,
                            downstream_job_id,
                            last_history,
                            history_output.expect(
                                "Job was finished::Sucessful, but without history_output",
                            ),
                        ) {
                            e.validation_status = InvalidationStatus::Invalidated;
                            self.propagate_upstream_required(downstream_idx);
                        } else {
                            e.validation_status = InvalidationStatus::Validated;
                        }
                    }
                };
            }
            Finished::Skipped => {
                e.validation_status = InvalidationStatus::Skipped;
            }
            Finished::Failure => {
                e.validation_status = InvalidationStatus::Failed;
                self.job_finished(downstream_idx, Finished::UpstreamFailure)
            }
            Finished::UpstreamFailure => {
                e.validation_status = InvalidationStatus::Failed;
                self.job_finished(downstream_idx, Finished::UpstreamFailure)
            }
        }
    }
}

fn node_has_upstreams(&self, idx: u32) -> bool {
    let mut upstreams = self.dag.neighbors_directed(idx, Direction::Incoming);
    match upstreams.next() {
        Some(_) => true,
        None => false,
    }
}

fn identify_missing_outputs(&mut self) {
    let mut propagate = Vec::new();
    for (node_idx, (job_id, job)) in self.jobs.iter().enumerate() {
        if job.no_of_inputs_changed {
            propagate.push(node_idx as u32);
        } else if job.kind == JobKind::Output {
            if !self.strategy.output_already_present(job_id) {
                propagate.push(node_idx as u32);
            }
        }
    }
    for node_idx in propagate {
        self.propagate_upstream_required(node_idx)
    }
}
fn fill_in_unfinished_downstream_counts(&mut self) {
    let mut jobs_to_mark_skipped = Vec::new();
    for (node_idx, (job_id, job)) in self.jobs.iter_mut().enumerate() {
        if job.kind == JobKind::Ephemeral {
            let ds_count = self.dag.edges(node_idx as u32).count();
            job.unfinished_downstreams = UnfinishedDownstreams::Count(ds_count as u32);
            if ds_count == 0 {
                jobs_to_mark_skipped.push(node_idx as u32);
            }
        }
    }
    for node_idx in jobs_to_mark_skipped {
        self.job_finished(node_idx, Finished::Skipped);
        self.propagate_upstream_not_required(node_idx);
    }
}

fn propagate_upstream_required(&mut self, node_idx: u32) {
    debug!("upstream required: {}", self.jobs[node_idx as usize].0);
    let upstreams: Vec<_> = self
        .dag
        .neighbors_directed(node_idx, Direction::Incoming)
        .collect();
    for upstream_idx in upstreams {
        self.dag
            .edge_weight_mut(upstream_idx, node_idx)
            .unwrap()
            .downstream_required = Some(true);
        if self.jobs[upstream_idx as usize].1.kind == JobKind::Ephemeral {
            self.propagate_upstream_required(upstream_idx);
        }
    }
}
fn propagate_upstream_not_required(&mut self, node_idx: u32) {
    let upstreams: Vec<_> = self
        .dag
        .neighbors_directed(node_idx, Direction::Incoming)
        .collect();
    for upstream_idx in upstreams {
        self.dag
            .edge_weight_mut(upstream_idx, node_idx)
            .unwrap()
            .downstream_required = Some(false);
        if self.jobs[upstream_idx as usize].1.kind == JobKind::Ephemeral {
            if self.all_downstream_edges_not_required(upstream_idx) {
                self.propagate_upstream_not_required(upstream_idx);
            }
        }
    }
}

fn all_downstream_edges_not_required(&self, node_idx: u32) -> bool {
    let downstreams = self.dag.neighbors_directed(node_idx, Direction::Outgoing);
    for downstream_idx in downstreams {
        let e = self.dag.edge_weight(node_idx, downstream_idx).unwrap();
        match e.downstream_required {
            Some(true) | None => return false,
            Some(false) => {}
        }
    }
    true
}

fn identify_changed_input_counts(&mut self) {
    let mut prev_counts: HashMap<String, u32> = HashMap::new();
    for key in self.history.keys() {
        if let Some((upstream_job_id, downstream_job_id)) = key.split_once("!!!") {
            *prev_counts
                .entry(downstream_job_id.to_string())
                .or_insert(0) += 1;
        }
    }
    for (node_idx, (job_id, job)) in self.jobs.iter_mut().enumerate() {
        let current_count = self
            .dag
            .edges_directed(node_idx as u32, Direction::Incoming)
            .count();
        let previous_count = *prev_counts.get(job_id).unwrap_or(&0);
        if current_count != previous_count as usize {
            debug!("number of inputs changed {}", job_id);
            job.no_of_inputs_changed = true;
        }
    }
}

pub fn event_startup(&mut self) -> Result<(), PPGEvaluatorError> {
    info!("startup");
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
    self.identify_changed_input_counts();
    self.identify_missing_outputs();
    self.fill_in_unfinished_downstream_counts();
    self.update();
    Ok(())
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

pub fn event_job_finished_success( &mut self, job_id: &str, history_to_store: String,) -> Result<(), PPGEvaluatorError> {
    let idx = *self.node_to_index.get(job_id).expect("Unknown job id");
    let j = &mut self.jobs[idx as usize];
    match j.1.ran {
        RunStatus::Running => {
            j.1.history_output = Some(history_to_store);
            self.job_finished(idx, Finished::Sucessful);
            self.update();
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
            self.update();
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

*/
