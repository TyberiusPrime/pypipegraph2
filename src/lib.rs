#![allow(clippy::borrow_deref_ref)]
#[allow(unused_imports)]
use log::{debug, error, info, warn};
use pyo3::exceptions::{PyKeyError, PyTypeError, PyValueError};
use pyo3::types::PyDict;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::Path;
use std::rc::Rc;
use std::sync::Once;

use thiserror::Error;

use pyo3::prelude::*;

mod engine;
#[cfg(test)]
mod tests;

pub use engine::{JobKind, PPGEvaluator};

static LOGGER_INIT: Once = Once::new();

#[derive(Error, Debug)]
pub enum PPGEvaluatorError {
    #[error("API error. You're holding it wrong")]
    APIError(String),
    #[error("Ephemeral {job_id} was validated, but rerun for downstreams. It changed output, violating the constant input->constant output assumption. Output was \n'{last_history}' is now \n'{new_history}'. You are holding it very wrong.")]
    EphemeralChangedOutput {
        job_id: String,
        last_history: String,
        new_history: String,
    },
    #[error(
        "Internal error. Something in the pipegraph2 engine is wrong. Graph execution aborted. Msg was {0}"
    )]
    InternalError(String),
}

pub trait PPGEvaluatorStrategy {
    fn output_already_present(&self, query: &str) -> bool;
    fn is_history_altered(
        &self,
        job_id_upstream: &str,
        job_id_downstream: &str,
        last_recorded_value: &str,
        current_value: &str,
    ) -> bool;

    fn get_input_list(
        &self,
        node_idx: engine::NodeIndex,
        dag: &engine::GraphType,
        jobs: &[engine::NodeInfo],
    ) -> String;
}

#[derive(Clone, Debug)]
pub struct StrategyForTesting {
    pub already_done: Rc<RefCell<HashSet<String>>>,
}

impl StrategyForTesting {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
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
        _job_id_upstream: &str,
        _job_id_downstream: &str,
        last_recorded_value: &str,
        current_value: &str,
    ) -> bool {
        last_recorded_value != current_value
    }

    fn get_input_list(
        &self,
        node_idx: engine::NodeIndex,
        dag: &engine::GraphType,
        jobs: &[engine::NodeInfo],
    ) -> String {
        let mut names = Vec::new();
        let upstreams = dag.neighbors_directed(node_idx, petgraph::Direction::Incoming);
        for upstream_idx in upstreams {
            names.push(jobs[upstream_idx].get_job_id());
        }
        names.sort();
        names.join("\n")
    }
}

pub fn start_logging() {
    let start_time = std::time::Instant::now();
    if !LOGGER_INIT.is_completed() {
        LOGGER_INIT.call_once(move || {
            use colored::Colorize;
            let start_time2 = start_time;
            env_logger::builder()
                .filter_level(log::LevelFilter::Debug)
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
                        (std::time::Instant::now() - start_time2).as_millis(),
                        //chrono::Local::now().format("%Y-%m-%dT%H:%M:%S"),
                        record.args()
                    )
                })
                .is_test(true)
                .init()
        });
    }
}
pub fn start_logging_to_file(filename: impl AsRef<Path>) {
    let start_time = std::time::Instant::now();
    if !LOGGER_INIT.is_completed() {
        LOGGER_INIT.call_once(move || {
            let fh = std::fs::File::create(filename).expect("Could not open log file");
            let start_time2 = start_time;
            env_logger::builder()
                .filter_level(log::LevelFilter::Debug)
                .format(move |buf, record| {
                    let filename = record
                        .file()
                        .unwrap_or("unknown")
                        .trim_start_matches("src/");
                    let ff = format!(
                        "{:15}:{:5} | {:5} |",
                        filename,
                        record.line().unwrap_or(0),
                        record.level()
                    );
                    writeln!(
                        buf,
                        "{}\t{:.4}ms | {}",
                        ff,
                        (std::time::Instant::now() - start_time2).as_millis(),
                        //chrono::Local::now().format("%Y-%m-%dT%H:%M:%S"),
                        record.args()
                    )
                })
                .is_test(true)
                .target(env_logger::Target::Pipe(Box::new(fh)))
                .init()
        })
    }
}

pub struct RunError(
    pub engine::PPGEvaluator<StrategyForTesting>,
    pub PPGEvaluatorError,
);

impl std::fmt::Debug for RunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunError").field("error", &self.1).finish()
    }
}

// simulates a complete (deterministic)
// run - jobs just register that they've been run,
// and output a 'dummy' history.
pub struct TestGraphRunner {
    #[allow(clippy::type_complexity)]
    pub setup_graph: Box<dyn Fn(&mut PPGEvaluator<StrategyForTesting>)>,
    pub run_counters: HashMap<String, usize>,
    pub history: HashMap<String, String>,
    pub already_done: HashSet<String>,
    pub allowed_nesting: u32,
    pub outputs: HashMap<String, String>,
    pub run_order: Vec<String>,
    pub cleaned_up: HashSet<String>,
}

impl TestGraphRunner {
    #[allow(clippy::type_complexity)]
    pub fn new(setup_func: Box<dyn Fn(&mut PPGEvaluator<StrategyForTesting>)>) -> Self {
        TestGraphRunner {
            setup_graph: setup_func,
            run_counters: HashMap::new(),
            history: HashMap::new(),
            already_done: HashSet::new(),
            allowed_nesting: 250,
            outputs: HashMap::new(),
            run_order: Vec::new(),
            cleaned_up: HashSet::new(),
        }
    }

    pub fn debug_(&self) -> String {
        let strat = StrategyForTesting::new();
        let mut g = PPGEvaluator::new_with_history(self.history.clone(), strat);

        (self.setup_graph)(&mut g);
        g.debug_()
    }

    pub fn run(
        &mut self,
        jobs_to_fail: &[&str],
    ) -> Result<PPGEvaluator<StrategyForTesting>, RunError> {
        debug!("");
        debug!("GOGOGO ----------------------------------------------------------------");
        let strat = StrategyForTesting::new();
        for k in self.already_done.iter() {
            strat.already_done.borrow_mut().insert(k.to_string());
        }
        let already_done2 = Rc::clone(&strat.already_done);
        let mut g = PPGEvaluator::new_with_history(self.history.clone(), strat);
        self.run_order.clear();

        (self.setup_graph)(&mut g);
        //debug!("{}", g.debug_());
        let mut counter = self.allowed_nesting;
        g.event_startup().unwrap();
        while !g.is_finished() {
            let to_run = g.query_ready_to_run();
            if to_run.is_empty() {
                //  start_logging();
                // debug!("{}", g.debug_());
                //g.debug_is_finished();
            }
            if to_run.is_empty() {
                g.debug_is_finished();
            }
            assert!(!to_run.is_empty());
            for job_id in to_run.iter() {
                debug!("Running {}", job_id);
                match g.event_now_running(job_id) {
                    Ok(_) => {}
                    Err(e) => return Err(RunError(g, e.into())),
                }
                self.run_order.push(job_id.to_string());
                *self.run_counters.entry(job_id.clone()).or_insert(0) += 1;
                if jobs_to_fail.contains(&&job_id[..]) {
                    match g.event_job_finished_failure(job_id) {
                        Ok(_) => {}
                        Err(e) => return Err(RunError(g, e.into())),
                    }
                } else {
                    match g.event_job_finished_success(
                        job_id,
                        self.outputs
                            .get(job_id)
                            .unwrap_or(&format!("history_{}", job_id))
                            .to_string(),
                    ) {
                        Ok(_) => {
                            already_done2.borrow_mut().insert(job_id.clone());
                        }
                        Err(err) => match err {
                            PPGEvaluatorError::APIError(x) => panic!("api error {}", x),
                            PPGEvaluatorError::EphemeralChangedOutput { .. } => {
                                debug!("EphemeralChangedOutput error. ignoring for tests");
                            }
                            PPGEvaluatorError::InternalError(x) => panic!("internal error {}", x),
                        },
                    }
                }
            }
            counter -= 1;
            if counter == 0 {
                return Err(RunError(
                    g,
                    PPGEvaluatorError::APIError(format!(
                        "run out of room, you nested them more than {} deep?",
                        self.allowed_nesting
                    )),
                ));
            }

            for c in g.query_ready_for_cleanup() {
                g.event_job_cleanup_done(&c)
                    .expect("cleanup registering failed");
                self.cleaned_up.insert(c);
            }
        }

        self.history.clear();
        for (k, v) in {
            match g.new_history() {
                Ok(x) => x,
                Err(e) => return Err(RunError(g, e)),
            }
        }
        .iter()
        {
            self.history.insert(k.clone(), v.clone());
        }
        for k in already_done2.take().into_iter() {
            self.already_done.insert(k);
        }
        /* #[cfg(debug_assertions)]
               if !g.verify_order_was_topological(&self.run_order) {
                   panic!("Run order was not topological");
               }
        */
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

pub fn test_big_graph_in_layers(nodes_per_layer: u32, layers: u32, run_count: u32) {
    let create_graph = move |g: &mut PPGEvaluator<StrategyForTesting>| {
        for ll in 0..layers {
            if ll == 0 {
                for ii in 0..nodes_per_layer {
                    g.add_node(&format!("A{}_{}", ll, ii), JobKind::Always);
                }
            } else {
                for ii in 0..nodes_per_layer {
                    g.add_node(
                        &format!("A{}_{}", ll, ii),
                        if ii % 2 == 0 {
                            JobKind::Output
                        } else {
                            JobKind::Ephemeral
                        },
                    );
                    for yy in 0..nodes_per_layer {
                        g.depends_on(&format!("A{}_{}", ll, ii), &format!("A{}_{}", ll - 1, yy))
                    }
                }
            }
        }
    };
    let mut ro = TestGraphRunner::new(Box::new(create_graph));
    ro.allowed_nesting = layers + 1;
    for _ in 0..run_count {
        let g = ro.run(&Vec::new());
        assert!(g.is_ok())
    }
    //dbg!(g.new_history().len());
}

struct StrategyForPython {
    history_altered_callback: PyObject,
    get_job_inputs_str_callback: PyObject,
}

impl PPGEvaluatorStrategy for StrategyForPython {
    fn output_already_present(&self, query: &str) -> bool {
        use std::path::PathBuf;
        // support for multi file generating jobs
        for sub_path in query.split(":::") {
            let p = PathBuf::from(sub_path);
            if !p.exists() {
                return false;
            }
        }
        true
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

    fn get_input_list(
        &self,
        node_idx: engine::NodeIndex,
        _dag: &engine::GraphType,
        jobs: &[engine::NodeInfo],
    ) -> String {
        let job_id = jobs[node_idx as usize].clone_job_id();
        Python::with_gil(|py| {
            let res = self.get_job_inputs_str_callback.call1(py, (job_id,));
            res.expect("input_list_different failed on python side")
                .extract::<String>(py)
                .expect("input_list_changed_callback did not return a bool")
        })
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
        _py: Python,
        py_history: &Bound<PyDict>,
        history_compare_callable: PyObject,
        get_job_inputs_str_callback: PyObject,
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
                    get_job_inputs_str_callback,
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
        self.evaluator.query_upstream_failed().into_iter().collect()
    }

    pub fn jobs_ready_to_run(&self) -> Vec<String> {
        self.evaluator.query_ready_to_run().into_iter().collect()
    }
    pub fn next_job_ready_to_run(&self) -> Option<String> {
        self.evaluator.next_job_ready_to_run()
    }


    pub fn jobs_running(&self) -> Vec<String> {
        self.evaluator.query_jobs_running().into_iter().collect()
    }

    pub fn jobs_ready_for_cleanup(&self) -> Vec<String> {
        self.evaluator
            .query_ready_for_cleanup()
            .into_iter()
            .collect()
    }

    pub fn event_job_cleanup_done(&mut self, job_id: &str) -> Result<(), PyErr> {
        Ok(self.evaluator.event_job_cleanup_done(job_id)?)
    }

    pub fn is_finished(&mut self) -> bool {
        self.evaluator.is_finished()
    }

    pub fn new_history(&self) -> Result<HashMap<String, String>, PyErr> {
        Ok(self.evaluator.new_history()?)
    }

    pub fn get_job_output(&self, job_id: &str) -> Result<String, PyErr> {
        match self.evaluator.get_job_output(job_id) {
            engine::JobOutputResult::Done(v) => Ok(v),
            engine::JobOutputResult::NoSuchJob => Err(PyKeyError::new_err("Invalid job id")),
            engine::JobOutputResult::NotDone => Err(PyValueError::new_err("job not done")),
        }
    }

    pub fn debug(&self) -> String {
        self.evaluator.debug_()
    }

    pub fn debug_is_finished(&self) {
        self.evaluator.debug_is_finished();
    }

    pub fn reconsider_all_jobs(&mut self) -> Result<(), PyErr> {
        error!("Reconsidering all jobs!");
        self.evaluator.reconsider_all_jobs()?;
        Ok(())
    }

    pub fn event_abort(&mut self) -> Result<(), PyErr> {
        self.evaluator.abort_remaining()?;
        Ok(())
    }
}

/// Formats the sum of two numbers as string.
#[pyfunction]
fn enable_logging() -> PyResult<()> {
    start_logging();
    error!("hello from rust");
    Ok(())
}
/// Formats the sum of two numbers as string.
#[pyfunction]
fn enable_logging_to_file(filename: &str) -> PyResult<()> {
    start_logging_to_file(filename);
    error!("hello from rust");
    Ok(())
}

/// A Python module implemented in Rust.
#[pymodule]
fn pypipegraph2(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(enable_logging, m)?)?;
    m.add_function(wrap_pyfunction!(enable_logging_to_file, m)?)?;
    m.add_class::<PyPPG2Evaluator>()?;
    Ok(())
}
