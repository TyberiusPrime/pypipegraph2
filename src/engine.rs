#[allow(unused_imports)]
use log::{debug, error, info, warn};
use petgraph::{graphmap::GraphMap, Directed, Direction};
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet, VecDeque},
};

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

trait JobQueries {
    fn is_finished(&self) -> bool;
    fn is_failed(&self) -> bool;
    //fn is_skipped(&self) -> bool;
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
#[derive(Debug)]
pub struct EdgeInfo {
    required: Required,
    invalidated: Required,
}
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum JobStateAlways {
    Undetermined,
    ReadyToRun, // ie. upstreams done.
    Running,
    FinishedSuccess,
    FinishedFailure,
    FinishedUpstreamFailure,
    FinishedAborted,
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
    FinishedAborted,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum JobStateEphemeral {
    NotReady(ValidationStatus),
    ReadyButDelayed, //ie. we have not made a decision on whether this is a go or not.
    ReadyToRun(ValidationStatus),
    Running(ValidationStatus),
    FinishedSuccessNotReadyForCleanup,
    FinishedSuccessReadyForCleanup,
    FinishedSuccessCleanedUp,
    FinishedSuccessSkipCleanup,
    FinishedFailure,
    FinishedUpstreamFailure,
    FinishedSkipped,
    FinishedAborted,
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

    fn is_upstream_failure(&self) -> bool {
        match self {
            JobState::Always(JobStateAlways::FinishedUpstreamFailure) => true,
            JobState::Output(JobStateOutput::FinishedUpstreamFailure) => true,
            JobState::Ephemeral(JobStateEphemeral::FinishedUpstreamFailure) => true,
            _ => false,
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
            JobStateAlways::FinishedAborted => true,
        }
    }

    fn is_failed(&self) -> bool {
        matches!(
            self,
            JobStateAlways::FinishedFailure
                | JobStateAlways::FinishedUpstreamFailure
                | JobStateAlways::FinishedAborted
        )
    }

    /* fn is_skipped(&self) -> bool {
        false
    } */
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
            JobStateOutput::FinishedAborted => true,
        }
    }

    fn is_failed(&self) -> bool {
        matches!(
            self,
            JobStateOutput::FinishedFailure
                | JobStateOutput::FinishedUpstreamFailure
                | JobStateOutput::FinishedAborted
        )
    }

    /* fn is_skipped(&self) -> bool {
        matches!(self, JobStateOutput::FinishedSkipped)
    } */
}

impl JobQueries for JobStateEphemeral {
    fn is_finished(&self) -> bool {
        match self {
            JobStateEphemeral::NotReady(_) => false,
            JobStateEphemeral::ReadyButDelayed => false,
            JobStateEphemeral::ReadyToRun(_) => false,
            JobStateEphemeral::Running(_) => false,
            JobStateEphemeral::FinishedSuccessNotReadyForCleanup => true,
            JobStateEphemeral::FinishedSuccessReadyForCleanup => true,
            JobStateEphemeral::FinishedSuccessCleanedUp => true,
            JobStateEphemeral::FinishedSuccessSkipCleanup => true,
            JobStateEphemeral::FinishedFailure => true,
            JobStateEphemeral::FinishedUpstreamFailure => true,
            JobStateEphemeral::FinishedSkipped => true,
            JobStateEphemeral::FinishedAborted => true,
        }
    }

    fn is_failed(&self) -> bool {
        matches!(
            self,
            JobStateEphemeral::FinishedFailure
                | JobStateEphemeral::FinishedUpstreamFailure
                | JobStateEphemeral::FinishedAborted
        )
    }

    /* fn is_skipped(&self) -> bool {
        matches!(self, JobStateEphemeral::FinishedSkipped,)
    } */
}

#[derive(Clone, Debug)]
pub struct NodeInfo {
    job_id: String,
    state: JobState,
    history_output: Option<String>,
    last_considered_in_gen: usize,
}

impl NodeInfo {
    pub(crate) fn clone_job_id(&self) -> String {
        self.job_id.clone()
    }

    pub(crate) fn get_job_id(&self) -> &str {
        &self.job_id
    }
}

macro_rules! set_node_state {
    (
        $node: expr,
        $new_state: expr,
        $gen: expr
        ) => {
        debug!(
            "\tset_node_state {} from {:?} to {:?}",
            $node.job_id, $node.state, $new_state
        );
        match ($node.state, $new_state) {
            (JobState::Always(_), JobState::Always(_)) |
            (JobState::Output(_), JobState::Output(_)) |
            (JobState::Ephemeral(_), JobState::Ephemeral(_)) => {},

            (JobState::Always(_), _other) |
            (JobState::Ephemeral(_), _other) |
            (JobState::Output(_), _other)
             => panic!("Moving a job between kinds"), // if you encounter this from python, the
                                                       // sky must be falling
        }
        $node.state = $new_state;
        $gen.advance();
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

#[derive(Debug, PartialEq)]
enum SignalKind {
    JobReadyToRun,
    JobFinishedSkip,
    JobDone,
    JobFinishedSuccess,
    JobFinishedFailure,
    JobUpstreamFailure,
    ConsiderJob,
    JobCleanedUp,
    JobAborted,
}

impl Signal {
    fn job_id<'a>(&self, jobs: &'a [NodeInfo]) -> &'a str {
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
        $new_signals: expr,
        $gen: expr
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
            let last_considered_in_gen = $jobs[$node_idx].last_considered_in_gen;
            if $gen > last_considered_in_gen {
                $new_signals.push(NewSignal!(SignalKind::ConsiderJob, $node_idx, $jobs));
                $jobs[$node_idx].last_considered_in_gen = $gen;
            } else {
                debug!("not adding signal, was already considered in this gen");
            }
        }
    };
}

pub enum JobOutputResult {
    Done(String),
    NoSuchJob,
    NotDone,
}

pub(crate) type NodeIndex = usize;

pub(crate) type GraphType = GraphMap<NodeIndex, EdgeInfo, Directed>;

struct Generation {
    gen: usize,
}

impl Generation {
    fn get(&self) -> usize {
        self.gen
    }

    fn advance(&mut self) {
        self.gen += 1;
    }
}

pub struct PPGEvaluator<T: PPGEvaluatorStrategy> {
    dag: GraphType,
    jobs: Vec<NodeInfo>,
    job_id_to_node_idx: HashMap<String, NodeIndex>,
    history: HashMap<String, String>,
    strategy: T,
    already_started: StartStatus,
    jobs_ready_to_run: HashSet<String>,
    jobs_ready_for_cleanup: HashSet<String>,
    topo: Option<Vec<NodeIndex>>,
    signals: VecDeque<Signal>,
    gen: Generation,
}

impl<T: PPGEvaluatorStrategy> PPGEvaluator<T> {
    #[allow(dead_code)] // used for testing
    pub fn new(strategy: T) -> Self {
        // todo: get rid of the box for the caller at least?
        Self::new_with_history(HashMap::new(), strategy)
    }

    #[allow(clippy::type_complexity)]
    pub fn new_with_history(history: HashMap<String, String>, strategy: T) -> Self {
        PPGEvaluator {
            dag: GraphMap::new(),
            jobs: Vec::new(),
            job_id_to_node_idx: HashMap::new(),
            history,
            strategy,
            already_started: StartStatus::NotStarted,
            jobs_ready_to_run: HashSet::new(),
            jobs_ready_for_cleanup: HashSet::new(),
            topo: None,
            signals: VecDeque::new(),
            gen: Generation { gen: 0 },
        }
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
            last_considered_in_gen: 0,
        };
        let idx = self.jobs.len() as NodeIndex;
        if self
            .job_id_to_node_idx
            .insert(job_id.to_string(), idx)
            .is_some()
        {
            //can't get here from python.
            panic!("Can not add a node twice to the evaluator.");
        };
        self.jobs.push(job);
        self.dag.add_node(idx);
    }

    #[allow(dead_code)]
    pub fn contains_node(&self, job_id: &str) -> bool {
        self.job_id_to_node_idx.contains_key(job_id)
    }

    pub fn depends_on(&mut self, downstream: &str, upstream: &str) {
        let downstream_id = self.id_to_idx(downstream);
        let upstream_id = self.id_to_idx(upstream);
        assert_ne!(downstream_id, upstream_id, "can't depend on self");
        self.dag.add_edge(
            upstream_id,
            downstream_id,
            EdgeInfo {
                required: Required::Unknown,
                invalidated: Required::Unknown,
            },
        );
    }

    pub fn abort_remaining(&mut self) -> Result<(), PPGEvaluatorError> {
        let mut signal_failure = Vec::new();

        let mut new_signals: Vec<Signal> = Vec::new();

        for (job_idx, job) in self.jobs.iter_mut().enumerate() {
            if !job.state.is_finished() {
                //dbg!("Declaring upstream failure", job);
                signal_failure.push(job_idx);
            }
        }

        for job_idx in signal_failure {
            new_signals.push(NewSignal!(SignalKind::JobAborted, job_idx, self.jobs));
        }
        self.signals.extend(new_signals);
        self.process_signals(0)?;
        self.is_finished();
        Ok(())
    }

    pub fn is_finished(&mut self) -> bool {
        match self.already_started {
            StartStatus::NotStarted => false,
            StartStatus::Finished => true,
            StartStatus::Running => {
                for job in self.jobs.iter() {
                    if !job.state.is_finished() {
                        debug!("\tUnfinished: {} {:?}", job.job_id, job.state);
                        return false;
                    }
                }

                self.already_started = StartStatus::Finished;
                true
            }
        }
    }
    pub fn debug_is_finished(&self) -> bool {
        for job in self.jobs.iter() {
            if !job.state.is_finished() {
                debug!("Unfinished: {} {:?}", job.job_id, job.state);
            }
        }
        true
    }

    #[allow(dead_code)]
    pub fn debug_(&self) -> String {
        Self::debug(&self.dag, &self.jobs)
    }

    #[allow(dead_code)]
    pub fn reconsider_all_jobs(&mut self) -> Result<(), PPGEvaluatorError> {
        let mut new_signals: Vec<Signal> = Vec::new();
        for idx in 0..self.jobs.len() {
            if !self.jobs[idx].state.is_finished() {
                reconsider_job!(self.jobs, idx, new_signals, self.gen.get());
            }
        }
        self.signals.extend(new_signals);
        self.process_signals(0)
    }

    fn debug(dag: &GraphType, jobs: &[NodeInfo]) -> String {
        let mut out = "\nJobs: ".to_string();
        for (job_idx, job) in jobs.iter().enumerate() {
            out.push_str(&format!("{}({}): {:?}\n", job.job_id, job_idx, job.state));
        }
        out.push_str("\n\nEdges:\n");
        for (upstream_idx, downstream_idx, weight) in dag.all_edges() {
            let upstream_id = &jobs[upstream_idx as usize].job_id;
            let downstream_id = &jobs[downstream_idx as usize].job_id;
            out.push_str(&format!(
                "({}({})->{}({}): {:?}\n",
                upstream_id, upstream_idx, downstream_id, downstream_idx, weight.required
            ));
        }

        out.push_str("\n\nin code: \n");
        for (_job_idx, job) in jobs.iter().enumerate() {
            out.push_str(&format!(
                "g.add_node(\"{}\", JobKind::{});\n",
                job.job_id,
                match job.state {
                    JobState::Always(_) => "Always",
                    JobState::Ephemeral(_) => "Ephemeral",
                    JobState::Output(_) => "Output",
                }
            ));
        }
        out.push_str("let edges = vec![\n");
        for (upstream_idx, downstream_idx, _weight) in dag.all_edges() {
            out.push_str(&format!(
                "(\"{}\", \"{}\"), \n",
                jobs[downstream_idx].job_id, jobs[upstream_idx].job_id
            ));
        }
        out.push_str("];\n");
        out.push_str(
            "for (a,b) in edges {
            if g.contains_node(a) && g.contains_node(b){
                g.depends_on(a,b);
            }
        }
        ",
        );

        out
    }

    #[allow(clippy::ptr_arg)]
    #[allow(dead_code)]
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
        true
    }

    /// what jobs are ready to run *right now*
    pub fn query_ready_to_run(&self) -> HashSet<String> {
        self.jobs_ready_to_run.clone()
    }
    pub fn next_job_ready_to_run(&self) -> Option<String> {
        let next_job = self.jobs_ready_to_run.iter().next();
        next_job.cloned()
    }

    pub fn query_jobs_running(&self) -> HashSet<String> {
        self.jobs
            .iter()
            .filter_map(|job| match job.state {
                JobState::Always(JobStateAlways::Running)
                | JobState::Output(JobStateOutput::Running)
                | JobState::Ephemeral(JobStateEphemeral::Running(_)) => Some(job.job_id.clone()),
                _ => None,
            })
            .collect()
    }

    pub fn query_ready_for_cleanup(&self) -> HashSet<String> {
        self.jobs_ready_for_cleanup.clone()
    }

    #[allow(dead_code)] // used in testing
    pub fn query_failed(&self) -> HashSet<String> {
        // not worth keeping a list to prevent the scanning
        // called typically only once per graph
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
        // not worth keeping a list to prevent the scanning
        // called typically only once per graph
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
    fn _job_and_downstreams_are_ephemeral(
        dag: &GraphType,
        jobs: &[NodeInfo],
        job_idx: NodeIndex,
    ) -> bool {
        match jobs[job_idx as usize].state {
            JobState::Ephemeral(_) => {}
            JobState::Always(_) | JobState::Output(_) => {
                //    debug!("was not ephemeral {}", &self.jobs[job_idx as usize].job_id);
                return false;
            }
        }
        let downstreams: Vec<_> = dag
            .neighbors_directed(job_idx, Direction::Outgoing)
            .collect();
        for ds_id in downstreams {
            if !Self::_job_and_downstreams_are_ephemeral(dag, jobs, ds_id) {
                return false;
            }
        }
        true
    }
    ///
    /// Retrieve the 'new history' after a ppg run
    pub fn new_history(&self) -> Result<HashMap<String, String>, PPGEvaluatorError> {
        match self.already_started {
            StartStatus::Finished => {}
            _ => {
                debug!("{}", Self::debug(&self.dag, &self.jobs));
                panic!("Graph wasn't finished, not handing out history..."); // todo: actually, why not, we could save history occasionally?
            }
        }
        //our history 'keys'
        //(we can't do tuple indices because of json history-save-format.)
        //no !!! -> job output.
        //ends with !!! -> the list of named inputs
        //x!!!y -> input x for job y.

        //todo: consider splitting into multiple functions?

        // when a multi-file-generating-job get's renamed, we need to remove the old history.
        // but only then.
        let mut multi_parts_to_jobs = HashMap::new();
        for j in self.jobs.iter() {
            if j.job_id.contains(":::") {
                for part in j.job_id.split(":::") {
                    multi_parts_to_jobs.insert(part, j.job_id.clone());
                }
            } else {
                multi_parts_to_jobs.insert(&j.job_id, j.job_id.clone());
            }
        }

        let filter_if_renamed = |job_id: &str| -> bool {
            if job_id.contains(":::") {
                let last_time = multi_parts_to_jobs.get(job_id);
                match last_time {
                    Some(last_time) => last_time == job_id,
                    None => true, //not present.
                }
            } else {
                return true;
            }
        };

        let mut out = self.history.clone();
        let mut out: HashMap<_, _> = out
            .drain()
            .filter(|(k, _v)| {
                if k.contains("!!!") {
                    let (job_id_a, job_id_b) = k.split_once("!!!").unwrap();
                    if !job_id_b.is_empty() {
                        let node_idx_a = self.job_id_to_node_idx.get(job_id_a);
                        let node_idx_b = self.job_id_to_node_idx.get(job_id_b);
                        match (node_idx_a, node_idx_b) {
                            (Some(node_idx_a), Some(node_idx_b)) => {
                                self.dag.edge_weight(*node_idx_a, *node_idx_b).is_some()
                            }
                            _ => {
                                //if it's from a multi-output job that was producing different
                                //stuff before,
                                filter_if_renamed(job_id_a)
                            }
                        }
                    } else {
                        // a node uplink entry.
                        filter_if_renamed(job_id_a)
                    }
                } else {
                    // when MultiFileGeneratingJobs
                    // get renamed, we need to make sure we don't keep
                    // old history around
                    filter_if_renamed(k)
                }
            })
            .collect();

        for (idx, job) in self.jobs.iter().enumerate() {
            //step 1: record what jobs when into this one

            //step 2: record the actual output of this job
            // (are we using this anywhere?)
            // (todo: I think there's a potential to reuse it for ephemeral jobs that are needed
            // again, because their downstream failed)
            // We're not for checking-the-ephemeral-invariant-upholding,
            // for that we use the ones stored on the downstream jobs.

            let key = job.job_id.to_string();
            let job_was_success = job.history_output.is_some();
            let input_name_key = format!("{}!!!", job.job_id);
            if job_was_success {
                // if the job did not succeed, we want it to rerun!
                out.insert(
                    input_name_key,
                    self.strategy.get_input_list(idx, &self.dag, &self.jobs),
                );

                let history = match &job.history_output {
                    Some(new_history) => new_history,
                    None => match self.history.get(&key) {
                        Some(old_history) => old_history,
                        None => {
                            let job_and_downstreams_are_ephmeral =
                                Self::_job_and_downstreams_are_ephemeral(
                                    &self.dag,
                                    &self.jobs,
                                    idx as NodeIndex,
                                );
                            if job_and_downstreams_are_ephmeral || job.state.is_failed() {
                                continue;
                            } else {
                                return Err(PPGEvaluatorError::InternalError(
                                        format!(
                                "Job was skipped but history had no entry for this job: {:?} {:?}. job_and_downstreams_are_ephemeral {}",
                                job.job_id, job.state, job_and_downstreams_are_ephmeral

                            )));
                            }
                        }
                    },
                };
                out.insert(key, history.to_string());
            } else {
                // the job did not finish.
                // and by throwing away the history, we make sure it's
                // rerun on the next round.
                //
                // but if it failed because of upstream failure,
                // we need to *keep* the history.
                //
                //
                // is this not a problem on abort?
                // no abort fails the currently running jobs (external, python does that)

                // paranoia...
                //dbg!(&job);
                assert!(
                    job.state.is_failed()
                        || Self::_job_and_downstreams_are_ephemeral(&self.dag, &self.jobs, idx)
                );
                if !job.state.is_upstream_failure() {
                    out.remove(&job.job_id);
                    out.remove(&input_name_key);
                }
            }
        }

        // record the edges
        for (a, b, _weight) in self.dag.all_edges() {
            let job_id_a = &self.jobs[a as usize].job_id;
            let job_id_b = &self.jobs[b as usize].job_id;
            let key = format!("{}!!!{}", job_id_a, job_id_b);
            let history = self.jobs[a as usize].history_output.as_ref();
            let second_job_success = self.jobs[b as usize].history_output.is_some()
                || self.jobs[b as usize].state
                    == JobState::Ephemeral(JobStateEphemeral::FinishedSkipped);
            if second_job_success {
                // we do not store the history link if the second job failed.
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
                                            None => match (self.jobs[a as usize]).state {
                                                JobState::Ephemeral(_) => {
                                                    continue;
                                                }
                                                _ => {
                                                    return Err(PPGEvaluatorError::InternalError(format!("No history for edge from this run, none from the prev run, and no upstream job history. Bug: {}, {}",
                                               job_id_a, job_id_b)));
                                                }
                                            },
                                        }
                                    }
                                }
                            }
                            JobState::Always(JobStateAlways::FinishedFailure)
                            | JobState::Always(JobStateAlways::FinishedUpstreamFailure)
                            | JobState::Always(JobStateAlways::FinishedAborted)
                            | JobState::Output(JobStateOutput::FinishedFailure)
                            | JobState::Output(JobStateOutput::FinishedUpstreamFailure)
                            | JobState::Output(JobStateOutput::FinishedAborted)
                            | JobState::Ephemeral(JobStateEphemeral::FinishedFailure)
                            | JobState::Ephemeral(JobStateEphemeral::FinishedUpstreamFailure)
                            | JobState::Ephemeral(JobStateEphemeral::FinishedAborted) => {
                                match self.history.get(&key) {
                                    //we have an old edge
                                    Some(old_run_history) => old_run_history,
                                    None => {
                                        continue;
                                    }
                                }
                            }
                            _ => {
                                return Err(PPGEvaluatorError::InternalError(format!(
                                    "unexpected ran state when no history was present. job_id: '{}', ran state: '{:?}'",
                                    job_id_a, self.jobs[a as usize].state
                                )));
                            }
                        }
                    }
                };
                out.insert(key, history.to_string());
            }
        }

        Ok(out)
    }

    pub fn get_job_output(&self, job_id: &str) -> JobOutputResult {
        let job_idx = self.job_id_to_node_idx.get(job_id);
        match job_idx {
            None => JobOutputResult::NoSuchJob,
            Some(job_idx) => match &self.jobs[*job_idx as usize].history_output {
                Some(v) => JobOutputResult::Done(v.to_string()),
                None => JobOutputResult::NotDone,
            },
        }
    }

    pub fn event_startup(&mut self) -> Result<(), PPGEvaluatorError> {
        match self.already_started {
            StartStatus::Running | StartStatus::Finished => {
                return Err(PPGEvaluatorError::APIError("Can't start twice".to_string()));
            }
            _ => {}
        };
        self.already_started = StartStatus::Running;

        self.prune_leave_ephemerals();

        // this is not particulary fast.
        self.topo = Some(petgraph::algo::toposort(&self.dag, None).unwrap());
        //self.identify_changed_input_counts();
        self.identify_missing_outputs()?;
        self.process_signals(0)?; //or they're not correctly invalidated...
                                  //self.fill_in_unfinished_downstream_counts();
                                  //self.update();
        self.start_on_roots();
        self.process_signals(0)?;

        Ok(())
    }

    fn prune_leave_ephemerals(&mut self) {
        //option: speed up by looking at the edges once,
        //finding those that have no-no-ephemeral downstreams,
        let mut ephemerals: HashSet<NodeIndex> = self
            .dag
            .nodes()
            .filter(|idx| match self.jobs[*idx as usize].state {
                JobState::Ephemeral(_) => {
                    let downstreams = self.dag.neighbors_directed(*idx, Direction::Outgoing);
                    for downstream_idx in downstreams {
                        match self.jobs[downstream_idx as usize].state {
                            JobState::Ephemeral(_) => {}
                            _ => {
                                return false;
                            }
                        }
                    }
                    //only keep them if they have no downstreams, or only ephemeral downstreams
                    true
                }
                _ => false,
            })
            .collect();

        loop {
            let candidates: Vec<NodeIndex> = ephemerals
                .iter()
                .map(|x| *x)
                .filter(|idx| !Self::has_downstreams(&self.dag, *idx))
                .collect();

            for idx in candidates.iter() {
                debug!("removed leaf ephemeral {}", self.jobs[*idx].job_id);
                self.dag.remove_node(*idx);
                self.jobs[*idx].state = JobState::Ephemeral(JobStateEphemeral::FinishedSkipped);
                ephemerals.remove(idx);
            }

            if candidates.is_empty() {
                break;
            }
        }
    }

    pub fn event_now_running(&mut self, job_id: &str) -> Result<(), PPGEvaluatorError> {
        let idx = self.job_id_to_node_idx.get(job_id).expect("Unknown job id");
        let j = &mut self.jobs[*idx as usize];
        match j.state {
            JobState::Always(JobStateAlways::ReadyToRun) => {
                self.jobs_ready_to_run.remove(job_id);
                set_node_state!(j, JobState::Always(JobStateAlways::Running), self.gen);
                Ok(())
            }
            JobState::Output(JobStateOutput::ReadyToRun) => {
                self.jobs_ready_to_run.remove(job_id);
                set_node_state!(j, JobState::Output(JobStateOutput::Running), self.gen);
                Ok(())
            }
            JobState::Ephemeral(JobStateEphemeral::ReadyToRun(validation_status)) => {
                self.jobs_ready_to_run.remove(job_id);
                set_node_state!(
                    j,
                    JobState::Ephemeral(JobStateEphemeral::Running(validation_status)),
                    self.gen
                );
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
        let node_idx = *self.job_id_to_node_idx.get(job_id).expect("Unknown job id");
        let j = &self.jobs[node_idx as usize];
        match j.state {
            JobState::Always(JobStateAlways::Running)
            | JobState::Output(JobStateOutput::Running)
            | JobState::Ephemeral(JobStateEphemeral::Running(_)) => {}
            _ => {
                return Err(PPGEvaluatorError::APIError(format!(
                    "Reported a job as finished that was not running ! {:?}",
                    j
                )))
            }
        }
        if j.state == JobState::Ephemeral(JobStateEphemeral::Running(ValidationStatus::Validated)) {
            // changing your output when you were Validated is not allowed.
            // We the the job-output history, not the one on the downstreams
            // because those might be actually different, if the upstream epehemeral had run
            // and we had aborted/failed the downstreams in a previous run,
            // and this turn it's then obvs validated, but the downstreams do no longer match.
            //
            // We have to query the  downstream jobs though if it is really different
            // for history_is_different on the python side only considers those outputs
            // that are then again inputs for the downstream jobs.
            // (side-effect: if it changes in an output that's not used by any downstream,
            // this would not trigger.
            // It's an unlikely problem, and I currently don't see how to work around it.
            // witohut turning the downstream_job_id into a magic value that means 'check all the
            // outputs (we need the upstream job to know what comparison to use)
            //
            let hist_key = &j.job_id;
            if let Some(job_history) = self.history.get(hist_key) {
                // we have to check for actually altered history.
                // the timestamp may change, but the hash not...
                // any would do
                if self
                    .strategy
                    .is_history_altered(job_id, "!!!", job_history, &history_to_store)
                {
                    self.signals.push_back(NewSignal!(
                        SignalKind::JobFinishedFailure,
                        node_idx,
                        self.jobs
                    ));
                    let my_err = PPGEvaluatorError::EphemeralChangedOutput {
                        job_id: j.job_id.clone(),
                        last_history: job_history.to_string(),
                        new_history: history_to_store,
                    };
                    self.process_signals(0)?;
                    return Err(my_err);
                }
            }

            /*
            let downstreams = self.dag.neighbors_directed(node_idx, Direction::Outgoing);
            for downstream_idx in downstreams {
                debug!(
                    "Ephemeral {} finished, downstream {} is now ready to run. Downstream state was {:?}",
                    job_id, self.jobs[downstream_idx as usize].job_id
                    , self.jobs[downstream_idx as usize].state
                );
                match self.jobs[downstream_idx].state {
                    JobState::Always(_) => {}
                    JobState::Output(jo) => match jo {
                        JobStateOutput::NotReady(v) => match v {
                            ValidationStatus::Unknown => {}, //
                            ValidationStatus::Validated => {} // to the chekc.
                            ValidationStatus::Invalidated => continue,
                        },
                        JobStateOutput::FinishedSkipped => continue,
                        JobStateOutput::FinishedUpstreamFailure => continue,
                        x => panic!("Should not happen: {:?}", x),
                    },
                    JobState::Ephemeral(je) => match je {
                        JobStateEphemeral::NotReady(v) => match v {
                            ValidationStatus::Unknown => {},
                            ValidationStatus::Validated => {}
                            ValidationStatus::Invalidated => continue,
                        },
                        JobStateEphemeral::ReadyButDelayed => continue,
                        JobStateEphemeral::ReadyToRun(v) => match v {
                            ValidationStatus::Unknown => panic!("Should not happen"),
                            ValidationStatus::Validated => {}
                            ValidationStatus::Invalidated => continue,
                        },
                        JobStateEphemeral::FinishedSkipped => continue,
                        JobStateEphemeral::FinishedUpstreamFailure => continue,
                        _ => panic!("Should not happen"),
                    },
                }
                let hist_key = format!(
                    "{}!!!{}",
                    j.job_id, &self.jobs[downstream_idx as usize].job_id
                );
                if let Some(downstream_history) = self.history.get(&hist_key) {
                    // we have to check for actually altered history.
                    // the timestamp may change, but the hash not...
                    if self.strategy.is_history_altered(
                        job_id,
                        &self.jobs[downstream_idx as usize].job_id,
                        downstream_history,
                        &history_to_store,
                    ) {
                        self.signals.push_back(NewSignal!(
                            SignalKind::JobFinishedFailure,
                            node_idx,
                            self.jobs
                        ));
                        let my_err = PPGEvaluatorError::EphemeralChangedOutput {
                            job_id: j.job_id.clone(),
                            last_history: downstream_history.to_string(),
                            new_history: history_to_store,
                        };
                        self.process_signals(0)?;
                        return Err(my_err);
                    }
                }
            }
            */
        }

        let j = &mut self.jobs[node_idx as usize];
        j.history_output = Some(history_to_store);

        self.signals.push_back(NewSignal!(
            SignalKind::JobFinishedSuccess,
            node_idx,
            self.jobs
        ));
        self.process_signals(0)?;
        Ok(())
    }

    pub fn event_job_finished_failure(&mut self, job_id: &str) -> Result<(), PPGEvaluatorError> {
        let idx = *self.job_id_to_node_idx.get(job_id).expect("Unknown job id");
        let j = &mut self.jobs[idx as usize];
        match j.state {
            JobState::Always(JobStateAlways::Running)
            | JobState::Output(JobStateOutput::Running)
            | JobState::Ephemeral(JobStateEphemeral::Running(_)) => {}
            _ => {
                return Err(PPGEvaluatorError::APIError(format!(
                    "Reported a job as finished that was not running ! {:?}",
                    j
                )))
            }
        }
        self.signals
            .push_back(NewSignal!(SignalKind::JobFinishedFailure, idx, self.jobs));
        self.process_signals(0)?;
        Ok(())
    }

    pub fn event_job_cleanup_done(&mut self, job_id: &str) -> Result<(), PPGEvaluatorError> {
        let idx = *self.job_id_to_node_idx.get(job_id).expect("Unknown job id");
        let j = &mut self.jobs[idx as usize];
        match j.state {
            JobState::Ephemeral(JobStateEphemeral::FinishedSuccessReadyForCleanup) => {
                self.signals
                    .push_back(NewSignal!(SignalKind::JobCleanedUp, idx, self.jobs));
                self.process_signals(0)?;
                Ok(())
            }

            _ => Err(PPGEvaluatorError::APIError(format!(
                "Cleanup on a job not ready or already cleaned up! {:?}",
                j
            ))),
        }
    }

    fn process_signals(&mut self, depth: u32) -> Result<(), PPGEvaluatorError> {
        debug!("");
        debug!("Process signals, depth {}", depth);
        let res = self.inner_process_signals(depth);
        debug!("Leaving process signals, {}", depth);
        res
    }

    fn inner_process_signals(&mut self, depth: u32) -> Result<(), PPGEvaluatorError> {
        if depth > 1500 {
            return Err(PPGEvaluatorError::InternalError("Depth ConsiderJob loop. Either pathological input, or bug. Aborting to avoid stack overflow".to_string()));
        }
        let mut new_signals = Vec::new();
        let mut ignore_consider_signals = HashSet::new();
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
                                set_node_state!(j, JobState::Always(JobStateAlways::ReadyToRun), self.gen);
                            }
                            _ => {
                                return Err(PPGEvaluatorError::InternalError(
                                        format!("Unexpected state. Job {:?} was in state {:?} when it should have been in state Undetermined", j, j.state)
                                ))
                            }
                        },
                        JobState::Output(state) => match state {
                            JobStateOutput::NotReady(ValidationStatus::Invalidated) => {
                                set_node_state!(j, JobState::Output(JobStateOutput::ReadyToRun), self.gen);
                            }
                            _ => {
                                return Err(PPGEvaluatorError::InternalError(format!(
                                    "unexpected was 1 {:?}",
                                    j
                                )))
                            }
                        },
                        JobState::Ephemeral(state) => match state {
                            JobStateEphemeral::NotReady(ValidationStatus::Invalidated) => {
                                set_node_state!(
                                    j,
                                    JobState::Ephemeral(JobStateEphemeral::ReadyToRun(
                                        ValidationStatus::Invalidated
                                    )), self.gen
                                );
                            }
                            // validated happens when the job is required
                            JobStateEphemeral::NotReady(ValidationStatus::Validated)
                            | JobStateEphemeral::ReadyButDelayed => {
                                set_node_state!(
                                    j,
                                    JobState::Ephemeral(JobStateEphemeral::ReadyToRun(
                                        ValidationStatus::Validated
                                    )), self.gen
                                );
                            }
                            _ => {
                                return Err(PPGEvaluatorError::InternalError(format!(
                                    "unexpected was 2 {:?}",
                                    j
                                )))
                            }
                        },
                    }
                    self.jobs_ready_to_run.insert(j.job_id.clone());
                }
                SignalKind::JobFinishedSkip => {
                    let j = &mut self.jobs[node_idx as usize];
                    match j.state {
                        JobState::Always(_) => {
                            return Err(PPGEvaluatorError::InternalError(
                                "skipping always job is a bug".to_string(),
                            ))
                        }
                        JobState::Output(JobStateOutput::FinishedSkipped)
                        | JobState::Ephemeral(JobStateEphemeral::FinishedSkipped) => { // ignore,
                        }
                        JobState::Output(state) => match state {
                            JobStateOutput::NotReady(
                                ValidationStatus::Validated | ValidationStatus::Unknown,
                            ) => {
                                set_node_state!(
                                    j,
                                    JobState::Output(JobStateOutput::FinishedSkipped),
                                    self.gen
                                );
                                match self.history.get(&j.job_id) {
                                    Some(x) => j.history_output = Some(x.to_string()),
                                    None => {
                                        return Err(PPGEvaluatorError::InternalError(format!(
                                            "Skipped job, but no history was available? {:?}",
                                            j
                                        )))
                                    }
                                }
                                Self::reconsider_delayed_upstreams(
                                    &mut self.dag,
                                    &mut self.jobs,
                                    node_idx,
                                    &mut new_signals,
                                    &self.gen,
                                );
                            }
                            _ => {
                                return Err(PPGEvaluatorError::InternalError(format!(
                                    "unexpected was 3 {:?}",
                                    j
                                )))
                            }
                        },
                        JobState::Ephemeral(state) => match state {
                            JobStateEphemeral::NotReady(ValidationStatus::Validated)
                            | JobStateEphemeral::ReadyButDelayed => {
                                set_node_state!(
                                    j,
                                    JobState::Ephemeral(JobStateEphemeral::FinishedSkipped),
                                    self.gen
                                );
                                j.history_output = self.history.get(&j.job_id).cloned();
                            }
                            JobStateEphemeral::NotReady(ValidationStatus::Invalidated) => {
                                set_node_state!(
                                    j,
                                    JobState::Ephemeral(JobStateEphemeral::FinishedSkipped),
                                    self.gen
                                );
                                j.history_output = self.history.get(&j.job_id).cloned();
                                // yes the assert would be better above the state setting
                                // but the borrow checker disagrees
                                assert!(
                                    !Self::has_downstreams(&self.dag, node_idx)
                                        || Self::_job_and_downstreams_are_ephemeral(
                                            &self.dag, &self.jobs, node_idx
                                        )
                                );
                            }
                            _ => {
                                return Err(PPGEvaluatorError::InternalError(format!(
                                    "unexpected was 4 {:?}",
                                    j
                                )))
                            }
                        },
                    }
                    new_signals.push(NewSignal!(SignalKind::JobDone, node_idx, self.jobs));
                }
                SignalKind::JobDone => {
                    // todo : call directly from JobFinished*
                    let j = &mut self.jobs[node_idx as usize];
                    if !j.state.is_finished() {
                        return Err(PPGEvaluatorError::InternalError(
                            "job_done on job not finished".to_string(),
                        ));
                    }
                    let downstreams = self.dag.neighbors_directed(node_idx, Direction::Outgoing);
                    for downstream_idx in downstreams {
                        reconsider_job!(self.jobs, downstream_idx, new_signals, self.gen.get());
                        if new_signals.is_empty() {
                            debug!(
                                "reconsidering lead to no update, all current gen {}",
                                self.gen.get()
                            );
                        }
                        //assert!(!new_signals.is_empty());
                    }
                    Self::consider_upstreams_for_cleanup(
                        &self.dag,
                        &mut self.jobs,
                        &mut self.jobs_ready_for_cleanup,
                        node_idx,
                        &mut self.gen,
                    );
                }
                SignalKind::JobFinishedSuccess => {
                    let j = &mut self.jobs[node_idx as usize];
                    match j.state {
                        JobState::Always(JobStateAlways::Running) => {
                            set_node_state!(
                                j,
                                JobState::Always(JobStateAlways::FinishedSuccess),
                                self.gen
                            );
                        }
                        JobState::Output(JobStateOutput::Running) => {
                            set_node_state!(
                                j,
                                JobState::Output(JobStateOutput::FinishedSuccess),
                                self.gen
                            );
                        }
                        JobState::Ephemeral(JobStateEphemeral::Running(_)) => {
                            set_node_state!(
                                j,
                                JobState::Ephemeral(
                                    JobStateEphemeral::FinishedSuccessNotReadyForCleanup,
                                ),
                                self.gen
                            );
                        }
                        _ => {
                            return Err(PPGEvaluatorError::InternalError(format!(
                                "unexpected was 5 {:?}",
                                j
                            )))
                        }
                    }
                    new_signals.push(NewSignal!(SignalKind::JobDone, node_idx, self.jobs));
                }
                SignalKind::JobFinishedFailure => {
                    let j = &mut self.jobs[node_idx as usize];
                    match j.state {
                        JobState::Always(JobStateAlways::Running) => {
                            set_node_state!(
                                j,
                                JobState::Always(JobStateAlways::FinishedFailure),
                                self.gen
                            );
                        }
                        JobState::Output(JobStateOutput::Running) => {
                            set_node_state!(
                                j,
                                JobState::Output(JobStateOutput::FinishedFailure),
                                self.gen
                            );
                        }
                        JobState::Ephemeral(JobStateEphemeral::Running(_)) => {
                            set_node_state!(
                                j,
                                JobState::Ephemeral(JobStateEphemeral::FinishedFailure),
                                self.gen
                            );
                        }
                        _ => {
                            return Err(PPGEvaluatorError::InternalError(format!(
                                "unexpected was 6 {:?}",
                                j
                            )))
                        }
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
                    let mut propagate = true;
                    match j.state {
                        JobState::Always(JobStateAlways::FinishedUpstreamFailure)
                        | JobState::Output(JobStateOutput::FinishedUpstreamFailure)
                        | JobState::Ephemeral(JobStateEphemeral::FinishedUpstreamFailure) => {
                            //ignore - already done
                            propagate = false;
                        }
                        JobState::Always(JobStateAlways::Undetermined) => {
                            set_node_state!(
                                j,
                                JobState::Always(JobStateAlways::FinishedUpstreamFailure),
                                self.gen
                            );
                        }
                        JobState::Output(JobStateOutput::NotReady(_)) => {
                            set_node_state!(
                                j,
                                JobState::Output(JobStateOutput::FinishedUpstreamFailure),
                                self.gen
                            );
                        }
                        JobState::Ephemeral(JobStateEphemeral::NotReady(_)) => {
                            set_node_state!(
                                j,
                                JobState::Ephemeral(JobStateEphemeral::FinishedUpstreamFailure),
                                self.gen
                            );
                        }
                        JobState::Output(JobStateOutput::FinishedSkipped) => {
                            set_node_state!(
                                j,
                                JobState::Output(JobStateOutput::FinishedUpstreamFailure),
                                self.gen
                            );
                        }
                        _ => {
                            return Err(PPGEvaluatorError::InternalError(format!(
                                "unexpected was 7 {:?}",
                                j
                            )))
                        }
                    }
                    if propagate {
                        new_signals.push(NewSignal!(SignalKind::JobDone, node_idx, self.jobs));
                        let downstreams = self.dag.neighbors_directed(node_idx, Direction::Outgoing);
                        for downstream_idx in downstreams {
                            //if there's a consider signal, it's not valid anymore
                            Self::remove_consider_signals(&mut new_signals, downstream_idx);
                            ignore_consider_signals.insert(downstream_idx);

                            new_signals.push(NewSignal!(
                                SignalKind::JobUpstreamFailure,
                                downstream_idx,
                                self.jobs
                            ));
                            //debug!("Signals after: {:?}", new_signals);
                        }
                        //what about ephemeral upstreams that are now known to be unecessary?
                        Self::reconsider_ephemeral_upstreams(
                            &self.dag,
                            &mut self.jobs,
                            node_idx,
                            &mut new_signals,
                            &self.gen,
                        );
                    }
                }
                SignalKind::ConsiderJob => {
                    if ignore_consider_signals.contains(&node_idx) {
                        ignore_consider_signals.remove(&node_idx);
                        debug!(
                            "Ignored ConsiderJob for {}, was in ignore_consider_signals",
                            node_idx
                        );
                        continue;
                    }
                    Self::signal_consider_job(
                        &self.strategy,
                        &mut self.dag,
                        &mut self.jobs,
                        &self.history,
                        node_idx,
                        &mut new_signals,
                        &mut self.gen,
                        &mut ignore_consider_signals,
                    )?;
                }
                SignalKind::JobCleanedUp => {
                    let j = &mut self.jobs[node_idx as usize];
                    match j.state {
                        JobState::Ephemeral(JobStateEphemeral::FinishedSuccessReadyForCleanup) => {
                            set_node_state!(
                                j,
                                JobState::Ephemeral(JobStateEphemeral::FinishedSuccessCleanedUp),
                                self.gen
                            );
                            self.jobs_ready_for_cleanup.remove(&j.job_id);
                        }
                        _ => {
                            return Err(PPGEvaluatorError::InternalError(format!(
                                "unexpected was 8 {:?}",
                                j
                            )))
                        }
                    }
                }
                SignalKind::JobAborted => {
                    let j = &mut self.jobs[node_idx as usize];
                    if !j.state.is_finished() {
                        match j.state {
                            JobState::Ephemeral(_) => {
                                set_node_state!(
                                    j,
                                    JobState::Ephemeral(JobStateEphemeral::FinishedAborted),
                                    self.gen
                                );
                            }
                            JobState::Output(_) => {
                                set_node_state!(
                                    j,
                                    JobState::Output(JobStateOutput::FinishedAborted),
                                    self.gen
                                );
                            }
                            JobState::Always(_) => {
                                set_node_state!(
                                    j,
                                    JobState::Always(JobStateAlways::FinishedAborted),
                                    self.gen
                                );
                            }
                        }
                    }
                }
            }
        }
        if !new_signals.is_empty() {
            debug!("New signals after process_signals - adding and processing");
            for s in new_signals.into_iter() {
                debug!("\t {:?} {}", s, s.job_id(&self.jobs));
                self.signals.push_back(s);
            }
            //self.signals.extend(new_signals.drain(..));
        }
        if !self.signals.is_empty() {
            self.process_signals(depth + 1)?;
        }
        Ok(())
    }

    fn any_downstream_required(
        dag: &GraphType,
        jobs: &[NodeInfo],
        node_idx: NodeIndex,
    ) -> Result<bool, PPGEvaluatorError> {
        let res = match Self::downstream_requirement_status(dag, jobs, node_idx)? {
            Required::Unknown => false,
            Required::Yes => true,
            Required::No => false,
        };
        info!(
            "any_downstream_required {}: -> {res}",
            jobs[node_idx].job_id
        );
        Ok(res)
        /*

        for (_upstream_idx, downstream_idx, weight) in
            dag.edges_directed(node_idx, Direction::Outgoing)
        {
            info!("Looking at edge {}->{}, {weight:?}",

                  jobs[_upstream_idx].job_id,
                  jobs[downstream_idx].job_id,
                  );
            //checking the edge only checks wether this node_idx invalidated one of the
            //downstreams...
            match weight.required {
                Required::Yes => {
                    debug!("downstream required {}", jobs[downstream_idx].job_id);
                    return true;
                }
                Required::Unknown => {}
                Required::No => {}
            }
            // but the downstream might have been invalidated by another upstream.
        }
        info!("leaving any_downstream_required {} with false", jobs[node_idx].job_id);
        //why is the edge C1->B not being examined!?!?!
        dbg!(dag);
        dbg!(jobs);
        dbg!(node_idx);
        false
        */
    }

    fn all_downstreams_validated_or_upstream_failed(
        dag: &GraphType,
        jobs: &mut [NodeInfo],
        node_idx: NodeIndex,
    ) -> Result<bool, PPGEvaluatorError> {
        let downstreams = dag.neighbors_directed(node_idx, Direction::Outgoing);
        for downstream_idx in downstreams {
            match jobs[downstream_idx as usize].state {
                JobState::Always(_) => {
                    return Err(PPGEvaluatorError::InternalError(
                        "Unexpected. Should have been required in the first place, I believe"
                            .to_string(),
                    ));
                }
                JobState::Output(JobStateOutput::NotReady(vs))
                | JobState::Ephemeral(JobStateEphemeral::NotReady(vs)) => match vs {
                    ValidationStatus::Unknown | ValidationStatus::Invalidated => return Ok(false),
                    ValidationStatus::Validated => {}
                },
                JobState::Output(JobStateOutput::FinishedUpstreamFailure)
                | JobState::Ephemeral(JobStateEphemeral::FinishedUpstreamFailure)
                //unreachable - above | JobState::Always(JobStateAlways::FinishedUpstreamFailure) 
                => {}
                JobState::Output(JobStateOutput::FinishedSkipped) => {}
                _ => {
                    return Err(PPGEvaluatorError::InternalError(format!(
                        "should not happen 1272 {:?}",
                        jobs[downstream_idx as usize]
                    )));
                }
            }
        }
        Ok(true)
    }

    fn all_upstreams_done(dag: &GraphType, jobs: &mut [NodeInfo], node_idx: NodeIndex) -> bool {
        //there's no point caching this - it never get's called again if it ever returned true
        let upstreams = dag.neighbors_directed(node_idx, Direction::Incoming);
        for upstream_idx in upstreams {
            if !jobs[upstream_idx as usize].state.is_finished() {
                return false;
            }
        }
        true
    }

    fn has_upstreams(dag: &GraphType, node_idx: NodeIndex) -> bool {
        let mut upstreams = dag.neighbors_directed(node_idx, Direction::Incoming);
        upstreams.next().is_some()
    }

    fn has_downstreams(dag: &GraphType, node_idx: NodeIndex) -> bool {
        let mut downstreams = dag.neighbors_directed(node_idx, Direction::Outgoing);
        downstreams.next().is_some()
    }

    fn try_finding_renamed_multi_output_job(
        missing_upstream_id: &str,
        downstream_id: &str,
        history: &HashMap<String, String>,
    ) -> Option<String> {
        // since multi file output jobs change their names
        // but we only invalidate based on the actual job inputs
        // we need to rematch them here.
        // upstream_id is the job we have 'lost' from our set of inputs
        //
        let missing_upstream_outputs: HashSet<String> = missing_upstream_id
            .split(":::")
            .map(|x| x.to_string())
            .collect();

        let mut best = None;
        let mut best_count = 0;
        let query = format!("!!!{}", downstream_id);
        for history_entry in history.keys() {
            if history_entry.ends_with(&query) {
                let (historical_upstream_id, _downstream_idx) =
                    history_entry.split_once("!!!").unwrap(); // we know there's !!! in there and at moste once because we check the job_ids

                debug!(
                    "hunting for old name for {} - considering {}",
                    missing_upstream_id, historical_upstream_id
                );
                let historical_upstream_outputs: HashSet<String> = historical_upstream_id
                    .split(":::")
                    .map(|x| x.to_string())
                    .collect();
                let overlap = historical_upstream_outputs
                    .intersection(&missing_upstream_outputs)
                    .count();
                if overlap > best_count {
                    best_count = overlap;
                    best = Some(historical_upstream_id.to_string())
                }
            }
        }

        best
    }

    fn edge_invalidated(
        // that's a question
        dag: &mut GraphType,
        strategy: &dyn PPGEvaluatorStrategy,
        jobs: &[NodeInfo],
        history: &HashMap<String, String>,
        upstream_idx: NodeIndex,
        downstream_idx: NodeIndex,
    ) -> Result<bool, PPGEvaluatorError> {
        match dag
            .edge_weight(upstream_idx, downstream_idx)
            .unwrap()
            .invalidated
        {
            //caching this speeds up  test_big_graph_in_layers(300,30,2)
            Required::Yes => Ok(true),
            Required::No => Ok(false),
            Required::Unknown => {
                let upstream_id = &jobs[upstream_idx].job_id;
                let downstream_id = &jobs[downstream_idx].job_id;
                let key = format!("{}!!!{}", upstream_id, downstream_id); //todo: express onlyonce
                let last_history_value = history.get(&key); //todo: fight alloc
                let last_history_value: Option<Cow<_>> = match last_history_value {
                    Some(l) => Some(Cow::from(l)),
                    None => {
                        //if upstream_id.contains(":::")
                        // a 'multi file generating job' in the
                        // python portion
                        // the rust part needs to be aware of them for the history
                        // filtering anyway, so might as well do it here.
                        // note that a FG can become an MFG and visa versa,
                        // so skipping out on jobs not containging ":::"
                        // can't be done.
                        //{
                        match Self::try_finding_renamed_multi_output_job(
                            upstream_id,
                            downstream_id,
                            history,
                        ) {
                            Some(x) => {
                                debug!(
                                    "No history for {}, but found {} to use instead",
                                    upstream_id, x
                                );
                                history.get(&x).map(Cow::from)
                            }
                            None => None,
                        }
                        /* } else {
                            None
                        } */
                    }
                };
                match last_history_value {
                    Some(last_history_value) => {
                        // debug!("Had last history value: '{}'", last_history_value);
                        let current_value =
                            jobs[upstream_idx].history_output.as_ref().ok_or_else(|| {
                                PPGEvaluatorError::InternalError(format!(
                                    "No current history for job? Unexpected {:?}",
                                    &jobs[upstream_idx]
                                ))
                            })?;
                        /*
                        if *current_value != last_history_value {
                            info!(
                                "history maybe altered {}->{} {}_{}.log",
                                upstream_id, downstream_id, upstream_idx, downstream_idx
                            );
                            std::fs::write(
                                format!("historical_{}_{}.log", upstream_idx, downstream_idx),
                                last_history_value.as_bytes(),
                            )
                            .unwrap();
                            std::fs::write(
                                format!("current_{}_{}.log", upstream_idx, downstream_idx),
                                current_value.as_bytes(),
                            )
                            .unwrap();
                        }
                        */
                        if strategy.is_history_altered(
                            upstream_id,
                            downstream_id,
                            &last_history_value,
                            current_value,
                        ) {
                            dag.edge_weight_mut(upstream_idx, downstream_idx)
                                .unwrap()
                                .invalidated = Required::Yes;

                            debug!("\t\t\t had history - edge invalidated");
                            Ok(true)
                        } else {
                            dag.edge_weight_mut(upstream_idx, downstream_idx)
                                .unwrap()
                                .invalidated = Required::No;
                            debug!("\t\t\t had history - edge not invalidated");
                            Ok(false)
                        }
                    }

                    None => {
                        debug!("\t\t\t no history - edge invalidated");
                        dag.edge_weight_mut(upstream_idx, downstream_idx)
                            .unwrap()
                            .invalidated = Required::Yes;

                        Ok(true)
                    }
                }
            }
        }
    }

    fn update_validation_status(
        strategy: &dyn PPGEvaluatorStrategy,
        dag: &mut GraphType,
        jobs: &[NodeInfo],
        history: &HashMap<String, String>,
        node_idx: NodeIndex,
    ) -> Result<ValidationStatus, PPGEvaluatorError> {
        let upstreams: Vec<_> = dag
            .neighbors_directed(node_idx, Direction::Incoming)
            .collect();
        let mut not_done = 0;
        let mut invalidated = false;
        for upstream_idx in upstreams {
            if jobs[upstream_idx as usize].state.is_finished()
                || (jobs[upstream_idx as usize].state
                    == JobState::Output(JobStateOutput::NotReady(ValidationStatus::Validated)))
            {
                if jobs[upstream_idx as usize].state.is_upstream_failure() {
                    //this happens when the upstream job was UpstreamFailed,
                    //we do at this point have  JobUpstreamFailure(node_idx),
                    //but there's a ConsiderJob still in the self.signals
                    //that remove_consider_signals did not remove - since it was not in the *new*
                    //signals.
                    //return Ok(ValidationStatus::Unknown);
                }
                // !jobs[upstream_idx as usize].state.is_skipped() &&
                if Self::edge_invalidated(dag, strategy, jobs, history, upstream_idx, node_idx)? {
                    debug!(
                        "\t\tEdge invalidated {}({})-> {}({})",
                        jobs[upstream_idx].job_id, upstream_idx, jobs[node_idx].job_id, node_idx
                    );
                    invalidated = true
                } else {
                    debug!(
                        "\t\tEdge not invalidated {}({})->{}({})",
                        jobs[upstream_idx].job_id, upstream_idx, jobs[node_idx].job_id, node_idx
                    );
                }
            } else if (jobs[upstream_idx as usize].state
                == JobState::Ephemeral(JobStateEphemeral::ReadyButDelayed))
                || (jobs[upstream_idx as usize].state
                    == JobState::Ephemeral(JobStateEphemeral::NotReady(
                        ValidationStatus::Validated,
                    )))
            {
                // a validated ephemeral, can, even if it runs,
                // not invalidate a downstream.
                // If it produces a changed output,
                // that's a job contract failure.
                //
                // But! If it ran previously, produced a different output,
                // and we did not run the downstream job to completion (abort, possibly failure?)
                // Then it can invalidate the downstream job!
                //
                //TODO
                let upstream_historical_output =
                    history.get(&jobs[upstream_idx].job_id).ok_or_else(|| {
                        PPGEvaluatorError::InternalError(
                            "Should have had history for it, if it was validated?!".to_string(),
                        )
                    })?;
                let my_historical_input = history.get(&format!(
                    "{}!!!{}",
                    &jobs[upstream_idx].job_id, &jobs[node_idx].job_id
                ));
                match my_historical_input {
                    None => {
                        //no history, so certainly invalidated
                        debug!(
                            "edge invalidated: No history for {}->{}",
                            &jobs[upstream_idx].job_id, &jobs[node_idx].job_id
                        );
                        invalidated = true;
                    }
                    Some(my_historical_input) => {
                        if upstream_historical_output != my_historical_input {
                            debug!("edge invalidated by epheremeral changed in prev run: History for {}->{} changed",
                                   &jobs[upstream_idx].job_id,
                                   &jobs[node_idx].job_id);
                            invalidated = true;
                            dag.edge_weight_mut(upstream_idx, node_idx)
                                .unwrap()
                                .required = Required::Yes;
                        } else {
                            continue;
                        }
                    }
                }
            } else if jobs[upstream_idx as usize].state
                == JobState::Output(JobStateOutput::NotReady(ValidationStatus::Validated))
            {
                //neither can an output job that is validated
            } else {
                debug!(
                    "\t\t edge Counted as not done {} {}",
                    jobs[upstream_idx].job_id, jobs[node_idx].job_id
                );

                not_done += 1;
            }
        }
        debug!("\t\t not_done: {not_done}, invalidated: {invalidated}");
        if not_done == 0 {
            if invalidated {
                Ok(ValidationStatus::Invalidated)
            } else {
                Ok(ValidationStatus::Validated)
            }
        } else if invalidated {
            Ok(ValidationStatus::Invalidated)
        } else {
            Ok(ValidationStatus::Unknown)
        }
    }

    fn propagate_job_required(dag: &mut GraphType, jobs: &mut [NodeInfo], node_idx: NodeIndex) {
        let upstreams: Vec<_> = dag
            .neighbors_directed(node_idx, Direction::Incoming)
            .collect();
        for upstream_idx in upstreams {
            dag.edge_weight_mut(upstream_idx, node_idx)
                .unwrap()
                .required = Required::Yes;
            match jobs[upstream_idx].state {
                JobState::Always(_) | JobState::Output(_) => {}
                JobState::Ephemeral(_) => Self::propagate_job_required(dag, jobs, upstream_idx),
            }
        }
    }

    fn signal_consider_job(
        strategy: &dyn PPGEvaluatorStrategy,
        dag: &mut GraphType,
        jobs: &mut [NodeInfo],
        history: &HashMap<String, String>,
        node_idx: NodeIndex,
        new_signals: &mut Vec<Signal>,
        gen: &mut Generation,
        ignore_consider_signals: &mut HashSet<NodeIndex>,
    ) -> Result<(), PPGEvaluatorError> {
        //let j = &jobs[node_idx as usize];
        /* debug!(
            "\tconsidering job {}: {:?}",
            jobs[node_idx as usize].job_id, jobs[node_idx as usize].state
        ); */
        if jobs[node_idx as usize].state.is_finished() {
            debug!("Considering an already finished job -> no-op");
        }
        match jobs[node_idx as usize].state {
            JobState::Always(JobStateAlways::Undetermined) => {
                if Self::all_upstreams_done(dag, jobs, node_idx) {
                    Self::remove_consider_signals(new_signals, node_idx);
                    ignore_consider_signals.insert(node_idx);
                    new_signals.push(NewSignal!(SignalKind::JobReadyToRun, node_idx, jobs));
                }
            }
            JobState::Always(_) => {}
            JobState::Output(JobStateOutput::NotReady(validation_state)) => {
                match validation_state {
                    ValidationStatus::Unknown => {
                        match Self::update_validation_status(
                            strategy, dag, jobs, history, node_idx,
                        )? {
                            ValidationStatus::Unknown => {
                                debug!("\tstill unknown validation status");
                            }
                            solid_vs => {
                                match solid_vs {
                                    ValidationStatus::Unknown => {
                                        return Err(PPGEvaluatorError::InternalError(
                                            "can not happen".to_string(),
                                        ))
                                    }
                                    ValidationStatus::Validated => {
                                        Self::set_upstream_edges(
                                            dag,
                                            node_idx,
                                            if solid_vs == ValidationStatus::Invalidated {
                                                panic!(
                                                    "I do not see this happening from the code."
                                                );
                                                //Required::Yes
                                            } else {
                                                // ::Validated
                                                Required::No
                                            },
                                        );
                                        Self::remove_consider_signals(new_signals, node_idx);
                                        ignore_consider_signals.insert(node_idx);
                                        new_signals.push(NewSignal!(
                                            SignalKind::JobFinishedSkip,
                                            node_idx,
                                            jobs
                                        ))
                                    }
                                    ValidationStatus::Invalidated => {
                                        set_node_state!(
                                            jobs[node_idx as usize],
                                            JobState::Output(JobStateOutput::NotReady(solid_vs)),
                                            gen
                                        );

                                        Self::propagate_job_required(dag, jobs, node_idx)
                                    }
                                }
                                // not the most elegant control flow, but ok.
                                reconsider_job!(jobs, node_idx, new_signals, gen.get());

                                Self::reconsider_ephemeral_upstreams(
                                    dag,
                                    jobs,
                                    node_idx,
                                    new_signals,
                                    gen,
                                );
                            }
                        }
                    }
                    ValidationStatus::Validated => {
                        //we can be validated either
                        //if all upstreams are done
                        //if all non-ephemeral upstreams are done
                        //and the ephemerals are validated.
                        if Self::all_upstreams_done(dag, jobs, node_idx) {
                            new_signals.push(NewSignal!(
                                SignalKind::JobFinishedSkip,
                                node_idx,
                                jobs
                            ))
                        } else {
                            /*
                             */
                        }
                        //}
                    }
                    ValidationStatus::Invalidated => {
                        if Self::all_upstreams_done(dag, jobs, node_idx) {
                            Self::remove_consider_signals(new_signals, node_idx);
                            ignore_consider_signals.insert(node_idx);
                            new_signals.push(NewSignal!(SignalKind::JobReadyToRun, node_idx, jobs))
                        } else {
                            Self::reconsider_delayed_upstreams(
                                dag,
                                jobs,
                                node_idx,
                                new_signals,
                                gen,
                            );
                            //debug!("{}", Self::debug(dag, jobs));
                            //new_signals.push(NewSignal!(SignalKind::JobMustBeDone,node_idx,
                            //jobs));
                        }
                    }
                }
            }
            JobState::Output(_) => {}
            JobState::Ephemeral(state) => match state {
                JobStateEphemeral::NotReady(ValidationStatus::Invalidated) => {
                    if Self::all_upstreams_done(dag, jobs, node_idx) {
                        if Self::has_downstreams(dag, node_idx)
                            && !Self::_job_and_downstreams_are_ephemeral(dag, jobs, node_idx)
                        {
                            Self::remove_consider_signals(new_signals, node_idx);
                            ignore_consider_signals.insert(node_idx);
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
                            JobState::Ephemeral(JobStateEphemeral::ReadyButDelayed),
                            gen
                        );

                        reconsider_job!(jobs, node_idx, new_signals, gen.get());
                    } else {
                        match Self::downstream_requirement_status(dag, jobs, node_idx)? {
                            Required::Yes => {
                                // the Requirement will have been propagated upstream,
                                // do nothing
                            }
                            Required::Unknown => {
                                // let's try and update that in light of this node
                                // ValidationStatus::Validated
                                Self::consider_downstreams(dag, jobs, node_idx, new_signals, gen);
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
                                    gen,
                                );
                            }
                        }
                    }
                }
                JobStateEphemeral::ReadyButDelayed => {
                    {
                        if Self::any_downstream_required(dag, jobs, node_idx)? {
                            debug!("\tA downstream was required");
                            Self::remove_consider_signals(new_signals, node_idx);
                            ignore_consider_signals.insert(node_idx);
                            new_signals.push(NewSignal!(SignalKind::JobReadyToRun, node_idx, jobs));
                        } else if Self::all_downstreams_validated_or_upstream_failed(
                            //upstream failed on a downstream also means I'm not necessary
                            dag, jobs, node_idx,
                        )? {
                            debug!("\tAll downstreams validated");
                            Self::remove_consider_signals(new_signals, node_idx);
                            ignore_consider_signals.insert(node_idx);
                            new_signals.push(NewSignal!(
                                SignalKind::JobFinishedSkip,
                                node_idx,
                                jobs
                            ));
                        } else {
                            debug!("\tDownstreams undecided");
                            Self::consider_downstreams(dag, jobs, node_idx, new_signals, gen);
                        }
                    }
                    //we do signals in reverse order...
                }
                JobStateEphemeral::NotReady(ValidationStatus::Unknown) => {
                    match Self::update_validation_status(strategy, dag, jobs, history, node_idx)? {
                        ValidationStatus::Unknown => {
                            debug!("\tstill unknown validation status");
                        }
                        solid_vs => {
                            set_node_state!(
                                jobs[node_idx as usize],
                                JobState::Ephemeral(JobStateEphemeral::NotReady(solid_vs),),
                                gen
                            );
                            // not the most elegant control flow, but ok.
                            //
                            reconsider_job!(jobs, node_idx, new_signals, gen.get());
                            if let ValidationStatus::Invalidated = solid_vs {
                                //this happens when we did not run the ephemeral last time
                                //(aborted),
                                //but would have had to run it due to invalidation.
                                //and now it's invalidated again, but we have to tell the upstream
                                //ephemerals
                                Self::reconsider_ephemeral_upstreams(
                                    dag,
                                    jobs,
                                    node_idx,
                                    new_signals,
                                    gen,
                                );
                            }
                        }
                    }
                }
                _ => {}
            },
        }

        Ok(())
    }

    fn remove_consider_signals(new_signals: &mut Vec<Signal>, node_idx: NodeIndex) {
        new_signals.retain(|signal| {
            !(signal.kind == SignalKind::ConsiderJob && signal.node_idx == node_idx)
        });
    }

    fn downstream_requirement_status(
        dag: &GraphType,
        jobs: &[NodeInfo],

        node_idx: NodeIndex,
    ) -> Result<Required, PPGEvaluatorError> {
        let downstreams = dag.neighbors_directed(node_idx, Direction::Outgoing);
        let mut had_unknown = false;
        for downstream_idx in downstreams {
            error!(
                "downstream_requirement_status {}->{}: {:?} {:?}",
                jobs[node_idx].job_id,
                jobs[downstream_idx].job_id,
                dag.edge_weight(node_idx, downstream_idx).unwrap().required,
                jobs[downstream_idx].state
            );
            match dag.edge_weight(node_idx, downstream_idx).unwrap().required {
                Required::Unknown => return Ok(Required::Unknown),
                Required::Yes => return Ok(Required::Yes),
                Required::No => match jobs[downstream_idx].state {
                    JobState::Output(JobStateOutput::NotReady(ValidationStatus::Validated))
                    | JobState::Ephemeral(JobStateEphemeral::NotReady(
                        ValidationStatus::Validated,
                    )) => {}

                    JobState::Output(JobStateOutput::NotReady(ValidationStatus::Invalidated)) => {
                        error!("\tRequired::Yes");
                        return Ok(Required::Yes);
                    }
                    JobState::Ephemeral(JobStateEphemeral::NotReady(
                        ValidationStatus::Invalidated,
                    )) => {
                        error!("\tRequired::Yes");
                        return Ok(Required::Yes);
                    }

                    JobState::Output(JobStateOutput::NotReady(ValidationStatus::Unknown)) => {
                        error!("\tRequired::Unknown");
                        had_unknown = true;
                        //return Ok(Required::Unknown);
                    }
                    JobState::Ephemeral(JobStateEphemeral::FinishedUpstreamFailure) => {}
                    JobState::Output(JobStateOutput::FinishedSkipped)
                    | JobState::Output(JobStateOutput::FinishedUpstreamFailure) => {
                        //why would this short circuit?
                        //error!("\tRequired::No");
                        //return Ok(Required::No)
                    }
                    JobState::Ephemeral(JobStateEphemeral::NotReady(ValidationStatus::Unknown)) => {
                        error!("\tRequired::Unknown");
                        //return Ok(Required::Unknown);
                        had_unknown = true;
                    }
                    _ => {
                        return Err(PPGEvaluatorError::InternalError(format!(
                            "bug1943: {:?}",
                            jobs[downstream_idx]
                        )))
                    }
                },
            }
        }
        if had_unknown {
            Ok(Required::Unknown)
        } else {
            Ok(Required::No)
        }
    }

    fn consider_downstreams(
        dag: &GraphType,
        jobs: &mut [NodeInfo],

        node_idx: NodeIndex,
        new_signals: &mut Vec<Signal>,
        gen: &Generation,
    ) {
        let downstreams = dag.neighbors_directed(node_idx, Direction::Outgoing);
        for downstream_idx in downstreams {
            reconsider_job!(jobs, downstream_idx, new_signals, gen.get());
        }
    }

    fn reconsider_delayed_upstreams(
        dag: &GraphType,
        jobs: &mut [NodeInfo],
        node_idx: NodeIndex,
        new_signals: &mut Vec<Signal>,
        gen: &Generation,
    ) {
        let upstreams = dag.neighbors_directed(node_idx, Direction::Incoming);
        for upstream_idx in upstreams {
            match jobs[upstream_idx as usize].state {
                JobState::Always(_) => {}
                JobState::Output(_) => {}
                JobState::Ephemeral(state) => match state {
                    JobStateEphemeral::NotReady(_) => {
                        //new_signals.push(NewSignal!(SignalKind::ConsiderJob,upstream_idx, jobs));
                        Self::reconsider_delayed_upstreams(
                            dag,
                            jobs,
                            upstream_idx,
                            new_signals,
                            gen,
                        );
                    }
                    JobStateEphemeral::ReadyButDelayed => {
                        reconsider_job!(jobs, upstream_idx, new_signals, gen.get());
                    }
                    _ => {}
                },
            }
        }
    }

    fn reconsider_ephemeral_upstreams(
        dag: &GraphType,
        jobs: &mut [NodeInfo],
        node_idx: NodeIndex,
        new_signals: &mut Vec<Signal>,
        gen: &Generation,
    ) {
        let upstreams = dag.neighbors_directed(node_idx, Direction::Incoming);
        for upstream_idx in upstreams {
            match jobs[upstream_idx as usize].state {
                JobState::Always(_) => {}
                JobState::Output(_) => {}
                //actually we only need to do this
                JobState::Ephemeral(JobStateEphemeral::ReadyButDelayed)
                | JobState::Ephemeral(JobStateEphemeral::NotReady(ValidationStatus::Validated)) => {
                    reconsider_job!(jobs, upstream_idx, new_signals, gen.get());
                }
                JobState::Ephemeral(_) => {}
            }
        }
    }

    fn consider_upstreams_for_cleanup(
        dag: &GraphType,
        jobs: &mut [NodeInfo],
        jobs_ready_for_cleanup: &mut HashSet<String>,
        node_idx: NodeIndex,
        gen: &mut Generation,
    ) {
        let upstreams = dag.neighbors_directed(node_idx, Direction::Incoming);
        for upstream_idx in upstreams {
            if let JobState::Ephemeral(JobStateEphemeral::FinishedSuccessNotReadyForCleanup) =
                jobs[upstream_idx].state
            {
                let mut all_downstreams_done = true;
                let mut no_downstream_failed = true;
                let downstreams = dag.neighbors_directed(upstream_idx, Direction::Outgoing);
                for downstream_idx in downstreams {
                    if !jobs[downstream_idx].state.is_finished() {
                        all_downstreams_done = false;
                        break;
                    } else if jobs[downstream_idx].state.is_failed() {
                        no_downstream_failed = false;
                        break;
                    }
                }
                if all_downstreams_done {
                    if no_downstream_failed {
                        debug!("Job ready for cleanup {:?}", jobs[upstream_idx]);
                        set_node_state!(
                            jobs[upstream_idx],
                            JobState::Ephemeral(JobStateEphemeral::FinishedSuccessReadyForCleanup,),
                            gen
                        );
                        jobs_ready_for_cleanup.insert(jobs[upstream_idx].job_id.clone());
                    } else {
                        debug!(
                            "Job ready for cleanup {:?}, but downstreams failed -> no cleanup",
                            jobs[upstream_idx]
                        );
                        set_node_state!(
                            jobs[upstream_idx],
                            JobState::Ephemeral(JobStateEphemeral::FinishedSuccessSkipCleanup,),
                            gen
                        );
                    }
                }
            }
        }
    }

    fn set_upstream_edges(dag: &mut GraphType, node_idx: NodeIndex, weight: Required) {
        let upstreams: Vec<_> = dag
            .neighbors_directed(node_idx, Direction::Incoming)
            .collect();
        for upstream_idx in upstreams {
            (dag.edge_weight_mut(upstream_idx, node_idx).unwrap()).required = weight
        }
    }

    fn identify_missing_outputs(&mut self) -> Result<(), PPGEvaluatorError> {
        // this has to be in (inverse) topological order
        // because we need to set the required edges.
        for &node_idx in self.topo.as_ref().unwrap().iter().rev() {
            let job = &self.jobs[node_idx as usize];

            let input_name_key = format!("{}!!!", job.job_id);
            let historical_input_names = self.history.get(&input_name_key);
            let inputs_changed = match historical_input_names {
                Some(historical_input_names) => {
                    *historical_input_names
                        != self
                            .strategy
                            .get_input_list(node_idx, &self.dag, &self.jobs)
                }
                None => {
                    // not having an input job history is not itself
                    // enough reason to invalidate -
                    // they'll fail anyhow when we're looking at the individual edges
                    // and this would trigger building non-used Ephemerals
                    // but if you don't have an upstream,
                    // ande the strategy says 'already done',
                    // this is the only time we can get them invalidated
                    !Self::has_upstreams(&self.dag, node_idx)
                }
            };
            let job = &mut self.jobs[node_idx as usize];

            if inputs_changed {
                debug!("Input to job {} changed.", job.job_id);
                Self::set_upstream_edges(&mut self.dag, node_idx, Required::Yes);
                match job.state {
                    JobState::Always(JobStateAlways::Undetermined) => {
                        Self::set_upstream_edges(&mut self.dag, node_idx, Required::Yes)
                    }
                    JobState::Output(JobStateOutput::NotReady(ValidationStatus::Unknown)) => {
                        set_node_state!(
                            job,
                            JobState::Output(JobStateOutput::NotReady(
                                ValidationStatus::Invalidated,
                            )),
                            self.gen
                        );
                    }
                    JobState::Ephemeral(JobStateEphemeral::NotReady(ValidationStatus::Unknown)) => {
                        set_node_state!(
                            job,
                            JobState::Ephemeral(JobStateEphemeral::NotReady(
                                ValidationStatus::Invalidated,
                            )),
                            self.gen
                        );
                    }
                    _ => {
                        return Err(PPGEvaluatorError::InternalError(
                            "should not happen 1942".to_string(),
                        ))
                    }
                }
            } else {
                debug!("Input to job {} *un*changed.", job.job_id);
                match job.state {
                    JobState::Always(_) => {
                        Self::set_upstream_edges(&mut self.dag, node_idx, Required::Yes)
                    }
                    JobState::Output(_) => {
                        if self.strategy.output_already_present(&job.job_id) {
                            if self.history.contains_key(&job.job_id) {
                                Self::set_upstream_edges(&mut self.dag, node_idx, Required::No)
                            } else {
                                warn!(
                                    "output present, but we had no history for {}, redoing",
                                    &job.job_id
                                );
                                Self::set_upstream_edges(&mut self.dag, node_idx, Required::Yes);
                                set_node_state!(
                                    job,
                                    JobState::Output(JobStateOutput::NotReady(
                                        ValidationStatus::Invalidated,
                                    )),
                                    self.gen
                                );
                            }
                        } else {
                            Self::set_upstream_edges(&mut self.dag, node_idx, Required::Yes);
                            debug!("output was missing {}", &job.job_id);
                            set_node_state!(
                                job,
                                JobState::Output(JobStateOutput::NotReady(
                                    ValidationStatus::Invalidated,
                                )),
                                self.gen
                            );
                            //continue;
                        }
                    }
                    JobState::Ephemeral(_) => {
                        //we're going reverse topological, so at this point,
                        //all downstreams have declared whether they're required or not.
                        let mut any_required = false;
                        let downstreams =
                            self.dag.neighbors_directed(node_idx, Direction::Outgoing);
                        for downstream_idx in downstreams {
                            match self
                                .dag
                                .edge_weight(node_idx, downstream_idx)
                                .unwrap()
                                .required
                            {
                                Required::Unknown => {
                                    return Err(PPGEvaluatorError::InternalError(format!(
                                        "Should not happen {} {}",
                                        downstream_idx, self.jobs[downstream_idx].job_id
                                    )))
                                }
                                Required::Yes => {
                                    any_required = true;
                                    break;
                                }
                                Required::No => {}
                            }
                        }
                        debug!(
                            "ephemerial initial {} -> any downstreams required: {}",
                            &job.job_id, any_required
                        );
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
        Ok(())
    }

    fn start_on_roots(&mut self) {
        let mut out_signals = Vec::new();
        for root_idx in self.dag.nodes().filter(|node_idx| {
            self.dag
                .neighbors_directed(*node_idx, Direction::Incoming)
                .next()
                .is_none()
        }) {
            let job = &self.jobs[root_idx as usize];
            debug!("root node '{}'", job.job_id);
            out_signals.push(NewSignal!(SignalKind::ConsiderJob, root_idx, self.jobs));
        }
        for signal in out_signals.into_iter() {
            // debug!("Adding signal {:?}", signal);
            self.signals.push_back(signal)
        }
        debug!("done adding root signals\n");
    }
}
