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
pub enum Required {
    Unknown,
    Yes,
    No,
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

macro_rules! set_node_state {
    (
        $node: expr,
        $new_state: expr
        ) => {
        debug!(
            "\tUpdating {} from {:?} to {:?}",
            $node.job_id, $node.state, $new_state
        );
        match ($node.state, $new_state) {
            (JobState::Always(_), JobState::Always(_)) => {}
            (JobState::Output(_), JobState::Output(_)) => {}
            (JobState::Ephemeral(_), JobState::Ephemeral(_)) => {}
            _ => panic!("Moving a job between kinds"),
        }
        $node.state = $new_state;
    };
}

/// whether a Evaluator was run
enum StartStatus {
    NotStarted,
    Running,
    Finished,
}

#[derive(Debug)]
struct Signal {
    kind: SignalKind,
    node_idx: NodeIndex,
}

#[derive(Debug)]
enum SignalKind {
    JobReadyToRun,
    JobFinishedSkip,
    JobDone,
    JobFinishedSuccess,
    JobFinishedFailure,
    JobUpstreamFailure,
    ConsiderJob,
    JobCleanedUp,
}

impl Signal {
    fn job_id<'a>(&self, jobs: &'a Vec<NodeInfo>) -> &'a str {
        &jobs[self.node_idx as usize].job_id
    }
}

macro_rules! NewSignal {
    // macth like arm for macro
    ($kind:expr,$node_idx:expr,$jobs:expr) => {
        // macro expand to this code
        {
            // $a and $b will be templated using the value/variable provided to macro
            let sig = Signal {
                kind: $kind,
                node_idx: $node_idx,
            };
            debug!("-> new {:?} {}", sig, $jobs[$node_idx].job_id);
            sig
        }
    };
}

macro_rules! reconsider_job {
    (
        $jobs: expr,
        $node_idx: expr,
        $new_signals: expr
    ) => {
        let mut do_add = true;
        for s in $new_signals.iter() {
            if s.node_idx == $node_idx {
                debug!(
                    "-> Not adding ConsiderJob for {} - already had {:?}",
                    $jobs[$node_idx as usize].job_id, s
                );
                do_add = false;
                break;
            }
        }
        if do_add {
            $new_signals.push(NewSignal!(SignalKind::ConsiderJob, $node_idx, $jobs));
        }
    };
}

type NodeIndex = usize;

type GraphType = GraphMap<NodeIndex, Required, Directed>;

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
        self.dag
            .add_edge(upstream_id, downstream_id, Required::Unknown);
    }

    pub fn is_finished(&self) -> bool {
        for job in self.jobs.iter() {
            if !job.state.is_finished() {
                return false;
            }
        }
        true
    }
    fn debug(dag: &GraphType, jobs: &Vec<NodeInfo>) -> String {
        let mut out = "\n".to_string();
        for job in jobs.iter() {
            out.push_str(&format!("{}: {:?}\n", job.job_id, job.state));
        }
        out.push('\n');
        for (upstream_idx, downstream_idx, weight) in dag.all_edges() {
            let upstream_id = &jobs[upstream_idx as usize].job_id;
            let downstream_id = &jobs[downstream_idx as usize].job_id;
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
    fn _job_and_downstreams_are_ephemeral(&self, job_idx: NodeIndex) -> bool {
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
            if !self._job_and_downstreams_are_ephemeral(ds_id) {
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
                            self._job_and_downstreams_are_ephemeral(idx as NodeIndex);
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
        self.topo = Some(petgraph::algo::toposort(&self.dag, None).unwrap());
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
                set_node_state!(j, JobState::Always(JobStateAlways::Running));
                Ok(())
            }
            JobState::Output(JobStateOutput::ReadyToRun) => {
                self.jobs_ready_to_run.remove(job_id);
                set_node_state!(j, JobState::Output(JobStateOutput::Running));
                Ok(())
            }
            JobState::Ephemeral(JobStateEphemeral::ReadyToRun) => {
                self.jobs_ready_to_run.remove(job_id);
                set_node_state!(j, JobState::Ephemeral(JobStateEphemeral::Running));
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
        self.signals
            .push_back(NewSignal!(SignalKind::JobFinishedSuccess, idx, self.jobs));
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
        self.signals
            .push_back(NewSignal!(SignalKind::JobFinishedFailure, idx, self.jobs));
        self.process_signals(0);
        Ok(())
    }

    pub fn event_job_cleanup_done(&mut self, job_id: &str) -> Result<(), PPGEvaluatorError> {
        let idx = *self.job_id_to_node_idx.get(job_id).expect("Unknown job id");
        let j = &mut self.jobs[idx as usize];
        match j.state {
            JobState::Ephemeral(JobStateEphemeral::FinishedSuccessReadyForCleanup) => {
                self.signals
                    .push_back(NewSignal!(SignalKind::JobCleanedUp, idx, self.jobs));
                Ok(())
            }

            _ => Err(PPGEvaluatorError::APIError(format!(
                "Cleanup on a job not ready or already cleaned up! {:?}",
                j
            ))),
        }
    }

    fn process_signals(&mut self, depth: u32) {
        debug!("");
        debug!("Process signals, depth {}", depth);
        if depth > 150 {
            panic!("Depth ConsiderJob loop. Either pathological input, or bug. Aborting to avoid stack overflow");
        }
        let mut new_signals = Vec::new();
        for signal in self.signals.drain(..) {
            debug!("");
            debug!(
                "\tHandling {:?} for {}. Current state: {:?}",
                signal,
                signal.job_id(&self.jobs),
                self.jobs[signal.node_idx as usize].state
            );
            let node_idx = signal.node_idx;
            match signal.kind {
                SignalKind::JobReadyToRun => {
                    let j = &mut self.jobs[node_idx as usize];
                    match j.state {
                        JobState::Always(state) => match state {
                            JobStateAlways::Undetermined => {
                                set_node_state!(j, JobState::Always(JobStateAlways::ReadyToRun));
                            }
                            _ => panic!("unexpected"),
                        },
                        JobState::Output(state) => match state {
                            JobStateOutput::NotReady(ValidationStatus::Invalidated) => {
                                set_node_state!(j, JobState::Output(JobStateOutput::ReadyToRun));
                            }
                            _ => panic!("unexpected"),
                        },
                        JobState::Ephemeral(state) => match state {
                            JobStateEphemeral::NotReady(ValidationStatus::Invalidated)
                            // validated happens when the job is required
                            | JobStateEphemeral::NotReady(ValidationStatus::Validated)
                            | JobStateEphemeral::ReadyButDelayed => {
                                set_node_state!(j, JobState::Ephemeral(JobStateEphemeral::ReadyToRun));
                            }
                            _ => panic!("unexpected {:?}", j),
                        },
                    }
                    self.jobs_ready_to_run.insert(j.job_id.clone());
                }
                SignalKind::JobFinishedSkip => {
                    let j = &mut self.jobs[node_idx as usize];
                    match j.state {
                        JobState::Always(_) => panic!("skipping always job is a bug"),
                        JobState::Output(state) => match state {
                            JobStateOutput::NotReady(
                                ValidationStatus::Validated | ValidationStatus::Unknown,
                            ) => {
                                set_node_state!(
                                    j,
                                    JobState::Output(JobStateOutput::FinishedSkipped)
                                );
                            }
                            _ => panic!("unexpected: {:?}", j),
                        },
                        JobState::Ephemeral(state) => match state {
                            JobStateEphemeral::NotReady(ValidationStatus::Validated)
                            | JobStateEphemeral::ReadyButDelayed => {
                                set_node_state!(
                                    j,
                                    JobState::Ephemeral(JobStateEphemeral::FinishedSkipped,)
                                );
                            }
                            JobStateEphemeral::NotReady(ValidationStatus::Invalidated) => {
                                assert!(!Self::has_downstreams(&self.dag, node_idx));
                                set_node_state!(
                                    j,
                                    JobState::Ephemeral(JobStateEphemeral::FinishedSkipped,)
                                );
                            }
                            _ => panic!("unexpected {:?}", j),
                        },
                    }
                    new_signals.push(NewSignal!(SignalKind::JobDone, node_idx, self.jobs));
                }
                SignalKind::JobDone => {
                    // todo : call directly from JobFinished*
                    let j = &mut self.jobs[node_idx as usize];
                    if !j.state.is_finished() {
                        panic!("job_done on job not finished");
                    }
                    let downstreams = self.dag.neighbors_directed(node_idx, Direction::Outgoing);
                    for downstream_idx in downstreams {
                        reconsider_job!(self.jobs, downstream_idx, new_signals);
                        assert!(!new_signals.is_empty());
                    }
                    Self::consider_upstreams_for_cleanup(
                        &self.dag,
                        &mut self.jobs,
                        node_idx,
                        &mut new_signals,
                    );
                }
                SignalKind::JobFinishedSuccess => {
                    let j = &mut self.jobs[node_idx as usize];
                    match j.state {
                        JobState::Always(JobStateAlways::Running) => {
                            set_node_state!(j, JobState::Always(JobStateAlways::FinishedSuccess));
                        }
                        JobState::Output(JobStateOutput::Running) => {
                            set_node_state!(j, JobState::Output(JobStateOutput::FinishedSuccess));
                        }
                        JobState::Ephemeral(JobStateEphemeral::Running) => {
                            set_node_state!(
                                j,
                                JobState::Ephemeral(
                                    JobStateEphemeral::FinishedSuccessNotReadyForCleanup,
                                )
                            );
                        }
                        _ => panic!("unexpected"),
                    }
                    new_signals.push(NewSignal!(SignalKind::JobDone, node_idx, self.jobs));
                }
                SignalKind::JobFinishedFailure => {
                    let j = &mut self.jobs[node_idx as usize];
                    match j.state {
                        JobState::Always(JobStateAlways::Running) => {
                            set_node_state!(j, JobState::Always(JobStateAlways::FinishedFailure));
                        }
                        JobState::Output(JobStateOutput::Running) => {
                            set_node_state!(j, JobState::Output(JobStateOutput::FinishedFailure));
                        }
                        JobState::Ephemeral(JobStateEphemeral::Running) => {
                            set_node_state!(
                                j,
                                JobState::Ephemeral(JobStateEphemeral::FinishedFailure)
                            );
                        }
                        _ => panic!("unexpected"),
                    }
                    new_signals.push(NewSignal!(SignalKind::JobDone, node_idx, self.jobs));
                    let downstreams = self.dag.neighbors_directed(node_idx, Direction::Outgoing);
                    for downstream_idx in downstreams {
                        new_signals.push(NewSignal!(
                            SignalKind::JobUpstreamFailure,
                            downstream_idx,
                            self.jobs
                        ));
                    }
                }
                SignalKind::JobUpstreamFailure => {
                    let j = &mut self.jobs[node_idx as usize];
                    match j.state {
                        JobState::Always(JobStateAlways::Undetermined) => {
                            set_node_state!(
                                j,
                                JobState::Always(JobStateAlways::FinishedUpstreamFailure)
                            );
                        }
                        JobState::Output(JobStateOutput::NotReady(_)) => {
                            set_node_state!(
                                j,
                                JobState::Output(JobStateOutput::FinishedUpstreamFailure)
                            );
                        }
                        JobState::Ephemeral(JobStateEphemeral::NotReady(_)) => {
                            set_node_state!(
                                j,
                                JobState::Ephemeral(JobStateEphemeral::FinishedUpstreamFailure,)
                            );
                        }
                        _ => panic!("unexpected: {:?}", j),
                    }
                    new_signals.push(NewSignal!(SignalKind::JobDone, node_idx, self.jobs));
                    let downstreams = self.dag.neighbors_directed(node_idx, Direction::Outgoing);
                    for downstream_idx in downstreams {
                        new_signals.push(NewSignal!(
                            SignalKind::JobUpstreamFailure,
                            downstream_idx,
                            self.jobs
                        ));
                    }
                }
                SignalKind::ConsiderJob => {
                    Self::signal_consider_job(
                        &self.strategy,
                        &mut self.dag,
                        &mut self.jobs,
                        &self.history,
                        &self.topo.as_ref().unwrap(),
                        node_idx,
                        &mut new_signals,
                    );
                }
                SignalKind::JobCleanedUp => {
                    let j = &mut self.jobs[node_idx as usize];
                    match j.state {
                        JobState::Ephemeral(JobStateEphemeral::FinishedSuccessReadyForCleanup) => {
                            set_node_state!(
                                j,
                                JobState::Ephemeral(JobStateEphemeral::FinishedSuccessCleanedUp,)
                            );
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
                self.signals.push_back(s);
            }
            //self.signals.extend(new_signals.drain(..));
        }
        if !self.signals.is_empty() {
            self.process_signals(depth + 1);
        }
        debug!("Leaving process signals, {}", depth);
    }

    fn any_downstream_required(
        dag: &GraphType,
        jobs: &mut Vec<NodeInfo>,
        node_idx: NodeIndex,
    ) -> bool {
        for (upstream_index, downstream_index, weight) in
            dag.edges_directed(node_idx, Direction::Outgoing)
        {
            match weight {
                Required::Unknown => {}
                Required::Yes => return true,
                Required::No => {}
            }
        }
        false
    }

    fn all_downstreams_validated(
        dag: &GraphType,
        jobs: &mut Vec<NodeInfo>,
        node_idx: NodeIndex,
    ) -> bool {
        let downstreams = dag.neighbors_directed(node_idx, Direction::Outgoing);
        for downstream_idx in downstreams {
            match jobs[downstream_idx as usize].state {
                JobState::Always(_) => {
                    panic!("Should have been required in the first place, I believe")
                }
                JobState::Output(JobStateOutput::NotReady(vs))
                | JobState::Ephemeral(JobStateEphemeral::NotReady(vs)) => match vs {
                    ValidationStatus::Unknown | ValidationStatus::Invalidated => return false,
                    ValidationStatus::Validated => {}
                },
                _ => panic!("should not happen"),
            }
        }
        true
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

    fn has_downstreams(dag: &GraphType, node_idx: NodeIndex) -> bool {
        let mut downstreams = dag.neighbors_directed(node_idx, Direction::Outgoing);
        downstreams.next().is_some()
    }

    fn all_upstreams_done_or_ephemeral_validated(
        dag: &GraphType,
        jobs: &mut Vec<NodeInfo>,
        node_idx: NodeIndex,
    ) -> bool {
        let upstreams = dag.neighbors_directed(node_idx, Direction::Incoming);
        for upstream_idx in upstreams {
            if !(jobs[upstream_idx as usize].state.is_finished()
                || jobs[upstream_idx as usize].state
                    == JobState::Ephemeral(JobStateEphemeral::NotReady(
                        ValidationStatus::Validated,
                    ))
                || jobs[upstream_idx as usize].state
                    == JobState::Ephemeral(JobStateEphemeral::ReadyButDelayed))
            {
                return false;
            }
        }
        true
    }

    fn edge_invalidated(
        strategy: &dyn PPGEvaluatorStrategy,
        jobs: &Vec<NodeInfo>,
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
        jobs: &Vec<NodeInfo>,
        history: &HashMap<String, String>,
        node_idx: NodeIndex,
    ) -> ValidationStatus {
        let upstreams = dag.neighbors_directed(node_idx, Direction::Incoming);
        let mut not_done = 0;
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
            } else if (jobs[upstream_idx as usize].state
                == JobState::Ephemeral(JobStateEphemeral::ReadyButDelayed))
                || (jobs[upstream_idx as usize].state
                    == JobState::Ephemeral(JobStateEphemeral::NotReady(
                        ValidationStatus::Validated,
                    )))
            // a validated ephemeral, can, even if it runs,
            // not invalidate a downstream.
            // If it produces a changed output,
            // that's a job contract failure.
            {
            } else {
                not_done += 1;
            }
        }
        if not_done == 0 {
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

    fn propagate_job_required(dag: &mut GraphType, jobs: &mut Vec<NodeInfo>, node_idx: NodeIndex) {
        let upstreams: Vec<_> = dag
            .neighbors_directed(node_idx, Direction::Incoming)
            .collect();
        for upstream_idx in upstreams {
            *dag.edge_weight_mut(upstream_idx, node_idx).unwrap() = Required::Yes;
            match jobs[upstream_idx].state {
                JobState::Always(_) | JobState::Output(_) => {}
                JobState::Ephemeral(_) => Self::propagate_job_required(dag, jobs, upstream_idx),
            }
        }
    }

    fn signal_consider_job(
        strategy: &dyn PPGEvaluatorStrategy,
        dag: &mut GraphType,
        jobs: &mut Vec<NodeInfo>,
        history: &HashMap<String, String>,
        topo: &Vec<NodeIndex>,
        node_idx: NodeIndex,
        new_signals: &mut Vec<Signal>,
    ) {
        //let j = &jobs[node_idx as usize];
        /* debug!(
            "\tconsidering job {}: {:?}",
            jobs[node_idx as usize].job_id, jobs[node_idx as usize].state
        ); */
        match jobs[node_idx as usize].state {
            JobState::Always(state) => match state {
                JobStateAlways::Undetermined => {
                    if Self::all_upstreams_done(dag, jobs, node_idx) {
                        new_signals.push(NewSignal!(SignalKind::JobReadyToRun, node_idx, jobs));
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
                                    set_node_state!(
                                        jobs[node_idx as usize],
                                        JobState::Output(JobStateOutput::NotReady(solid_vs),)
                                    );

                                    match solid_vs {
                                        ValidationStatus::Unknown => panic!("can not happen"),
                                        ValidationStatus::Validated => Self::set_upstream_edges(
                                            dag,
                                            node_idx,
                                            if solid_vs == ValidationStatus::Invalidated {
                                                Required::Yes
                                            } else {
                                                // ::Validated
                                                Required::No
                                            },
                                        ),
                                        ValidationStatus::Invalidated => {
                                            Self::propagate_job_required(dag, jobs, node_idx)
                                        }
                                    }
                                    // not the most elegant control flow, but ok.
                                    reconsider_job!(jobs, node_idx, new_signals);
                                }
                            }
                        }
                        ValidationStatus::Validated => {
                            //we can be validated either
                            //if all upstreams are done
                            //if all non-ephemeral upstreams are done
                            //and the ephemerals are validated.
                            if Self::all_upstreams_done(dag, jobs, node_idx) {
                                debug!("JobFinishedSkip place 1");
                                new_signals.push(NewSignal!(
                                    SignalKind::JobFinishedSkip,
                                    node_idx,
                                    jobs
                                ))
                            } else {
                                let upstreams =
                                    dag.neighbors_directed(node_idx, Direction::Incoming);
                                for upstream_idx in upstreams {
                                    match jobs[upstream_idx as usize].state {
                                        JobState::Ephemeral(JobStateEphemeral::ReadyButDelayed)
                                        | JobState::Ephemeral(JobStateEphemeral::NotReady(
                                            ValidationStatus::Validated,
                                        )) => {
                                            reconsider_job!(jobs, upstream_idx, new_signals);
                                        }
                                        _ => {}
                                    }
                                }
                            }
                            //}
                        }
                        ValidationStatus::Invalidated => {
                            if Self::all_upstreams_done(dag, jobs, node_idx) {
                                new_signals.push(NewSignal!(
                                    SignalKind::JobReadyToRun,
                                    node_idx,
                                    jobs
                                ))
                            } else {
                                Self::reconsider_delayed_upstreams(
                                    dag,
                                    jobs,
                                    node_idx,
                                    new_signals,
                                );
                                //debug!("{}", Self::debug(dag, jobs));
                                //new_signals.push(NewSignal!(SignalKind::JobMustBeDone,node_idx,
                                //jobs));
                            }
                        }
                    }
                }
                _ => {}
            },
            JobState::Ephemeral(state) => match state {
                JobStateEphemeral::NotReady(ValidationStatus::Invalidated) => {
                    if Self::all_upstreams_done(dag, jobs, node_idx) {
                        if Self::has_downstreams(dag, node_idx) {
                            new_signals.push(NewSignal!(SignalKind::JobReadyToRun, node_idx, jobs));
                        } else {
                            new_signals.push(NewSignal!(
                                SignalKind::JobFinishedSkip,
                                node_idx,
                                jobs
                            ));
                        }
                    }
                }
                JobStateEphemeral::NotReady(ValidationStatus::Validated) => {
                    if Self::all_upstreams_done(dag, jobs, node_idx) {
                        set_node_state!(
                            jobs[node_idx as usize],
                            JobState::Ephemeral(JobStateEphemeral::ReadyButDelayed)
                        );

                        reconsider_job!(jobs, node_idx, new_signals);
                    } else {
                        match Self::downstream_requirement_status(dag, jobs, node_idx) {
                            Required::Yes => {
                                // the Requirement will have been propagated upstream,
                                // do nothing
                            }
                            Required::Unknown => {
                                // let's try and update that in light of this node
                                // ValidationStatus::Validated
                                Self::consider_downstreams(dag, jobs, node_idx, new_signals);
                            }
                            Required::No => {
                                // I am not required. Tell
                                debug!(
                                    "\tNo downstream required {}, setting upstream edges to 'not required'",
                                    jobs[node_idx as usize].job_id
                                );
                                Self::set_upstream_edges(dag, node_idx, Required::No);
                                //
                                Self::reconsider_ephemeral_upstreams(
                                    dag,
                                    jobs,
                                    node_idx,
                                    new_signals,
                                );
                            }
                        }
                    }
                }
                JobStateEphemeral::ReadyButDelayed => {
                    {
                        if Self::any_downstream_required(dag, jobs, node_idx) {
                            debug!("\tA downstream was required");
                            new_signals.push(NewSignal!(SignalKind::JobReadyToRun, node_idx, jobs));
                        } else if Self::all_downstreams_validated(dag, jobs, node_idx) {
                            new_signals.push(NewSignal!(
                                SignalKind::JobFinishedSkip,
                                node_idx,
                                jobs
                            ));
                        } else {
                            Self::consider_downstreams(dag, jobs, node_idx, new_signals);
                        }
                    }
                    //we do signals in reverse order...
                }
                JobStateEphemeral::NotReady(ValidationStatus::Unknown) => {
                    match Self::update_validation_status(strategy, dag, jobs, history, node_idx) {
                        ValidationStatus::Unknown => {
                            debug!("\tstill unknown validation status");
                        }
                        solid_vs => {
                            set_node_state!(
                                jobs[node_idx as usize],
                                JobState::Ephemeral(JobStateEphemeral::NotReady(solid_vs),)
                            );
                            // not the most elegant control flow, but ok.
                            //
                            reconsider_job!(jobs, node_idx, new_signals);
                        }
                    }
                }
                _ => {}
            },
        }

        //the meat of the matter. Look at this job, and decide whether we can advance it.
    }

    fn downstream_requirement_status(
        dag: &GraphType,
        jobs: &mut Vec<NodeInfo>,

        node_idx: NodeIndex,
    ) -> Required {
        let downstreams = dag.neighbors_directed(node_idx, Direction::Outgoing);
        for downstream_idx in downstreams {
            match dag.edge_weight(node_idx, downstream_idx).unwrap() {
                Required::Unknown => return Required::Unknown,
                Required::Yes => return Required::Yes,
                Required::No => match jobs[downstream_idx].state {
                    JobState::Output(JobStateOutput::NotReady(ValidationStatus::Validated))
                    | JobState::Ephemeral(JobStateEphemeral::NotReady(
                        ValidationStatus::Validated,
                    )) => {}

                    JobState::Output(JobStateOutput::NotReady(ValidationStatus::Invalidated)) => {
                        return Required::Yes
                    }
                    JobState::Ephemeral(JobStateEphemeral::NotReady(
                        ValidationStatus::Invalidated,
                    )) => return Required::Yes,

                    JobState::Output(JobStateOutput::NotReady(ValidationStatus::Unknown)) => {
                        return Required::Unknown
                    }
                    JobState::Ephemeral(JobStateEphemeral::NotReady(ValidationStatus::Unknown)) => {
                        return Required::Unknown
                    }
                    _ => panic!("bug: {:?}", jobs[downstream_idx]),
                },
            }
        }
        Required::No
    }

    fn consider_downstreams(
        dag: &GraphType,
        jobs: &mut Vec<NodeInfo>,

        node_idx: NodeIndex,
        new_signals: &mut Vec<Signal>,
    ) {
        let downstreams = dag.neighbors_directed(node_idx, Direction::Outgoing);
        for downstream_idx in downstreams {
            reconsider_job!(jobs, downstream_idx, new_signals);
        }
    }

    fn reconsider_delayed_upstreams(
        dag: &GraphType,
        jobs: &mut Vec<NodeInfo>,
        node_idx: NodeIndex,
        new_signals: &mut Vec<Signal>,
    ) {
        let upstreams = dag.neighbors_directed(node_idx, Direction::Incoming);
        for upstream_idx in upstreams {
            match jobs[upstream_idx as usize].state {
                JobState::Always(_) => {}
                JobState::Output(_) => {}
                JobState::Ephemeral(state) => match state {
                    JobStateEphemeral::NotReady(_) => {
                        //new_signals.push(NewSignal!(SignalKind::ConsiderJob,upstream_idx, jobs));
                        Self::reconsider_delayed_upstreams(dag, jobs, upstream_idx, new_signals);
                    }
                    JobStateEphemeral::ReadyButDelayed => {
                        reconsider_job!(jobs, upstream_idx, new_signals);
                    }
                    _ => {}
                },
            }
        }
    }

    fn reconsider_ephemeral_upstreams(
        dag: &GraphType,
        jobs: &mut Vec<NodeInfo>,
        node_idx: NodeIndex,
        new_signals: &mut Vec<Signal>,
    ) {
        let upstreams = dag.neighbors_directed(node_idx, Direction::Incoming);
        for upstream_idx in upstreams {
            match jobs[upstream_idx as usize].state {
                JobState::Always(_) => {}
                JobState::Output(_) => {}
                JobState::Ephemeral(state) => {
                    reconsider_job!(jobs, upstream_idx, new_signals);
                }
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
                        set_node_state!(
                            jobs[upstream_idx],
                            JobState::Ephemeral(JobStateEphemeral::FinishedSuccessReadyForCleanup,)
                        );
                    }
                }
                _ => {}
            };
        }
    }

    fn set_upstream_edges(dag: &mut GraphType, node_idx: NodeIndex, weight: Required) {
        let upstreams: Vec<_> = dag
            .neighbors_directed(node_idx, Direction::Incoming)
            .collect();
        for upstream_idx in upstreams {
            *dag.edge_weight_mut(upstream_idx, node_idx).unwrap() = weight
        }
    }

    fn identify_missing_outputs(&mut self) {
        for &node_idx in self.topo.as_ref().unwrap().iter().rev() {
            let job = &mut self.jobs[node_idx as usize];
            match job.state {
                JobState::Always(_) => {
                    Self::set_upstream_edges(&mut self.dag, node_idx, Required::Yes)
                }
                JobState::Output(_) => {
                    if self.strategy.output_already_present(&job.job_id) {
                        Self::set_upstream_edges(&mut self.dag, node_idx, Required::No)
                    } else {
                        Self::set_upstream_edges(&mut self.dag, node_idx, Required::Yes);
                        set_node_state!(
                            job,
                            JobState::Output(JobStateOutput::NotReady(
                                ValidationStatus::Invalidated,
                            ))
                        );
                        //continue;
                    }
                }
                JobState::Ephemeral(_) => {
                    //we're going reverse topological, so at this point,
                    //all downstreams have declared whether they're required or not.
                    let mut any_required = false;
                    let downstreams = self.dag.neighbors_directed(node_idx, Direction::Outgoing);
                    for downstream_idx in downstreams {
                        match self.dag.edge_weight(node_idx, downstream_idx).unwrap() {
                            Required::Unknown => panic!(
                                "Should not happen {} {}",
                                downstream_idx, self.jobs[downstream_idx].job_id
                            ),
                            Required::Yes => {
                                any_required = true;
                                break;
                            }
                            Required::No => {}
                        }
                    }
                    Self::set_upstream_edges(
                        &mut self.dag,
                        node_idx,
                        if any_required {
                            Required::Yes
                        } else {
                            Required::No
                        },
                    )
                }
            }
        }
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
            out_signals.push(NewSignal!(SignalKind::ConsiderJob, root_idx, self.jobs));
        }
        for signal in out_signals.into_iter() {
            // debug!("Adding signal {:?}", signal);
            self.signals.push_back(signal)
        }
        debug!("done adding root signals\n");
    }
}
