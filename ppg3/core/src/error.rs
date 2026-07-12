//! Shared error type for ppg3-core. WP agents: extend variants as needed,
//! never stringly-type protocol errors (DeterminismViolation, CorruptStore
//! must stay structured).

use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error at {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("canonical json: {0}")]
    Canon(String),
    #[error("corrupt store: {0}")]
    CorruptStore(String),
    #[error("determinism violation for input key {ik}:\n{report}")]
    DeterminismViolation { ik: String, report: String },
    #[error("store is read-only: {0}")]
    ReadOnlyStore(String),
    #[error("graph error: {0}")]
    Graph(String),
    #[error("job failed: {0}")]
    JobFailed(String),
    #[error("{0}")]
    Other(String),
}

impl Error {
    pub fn io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Error::Io { path: path.into(), source }
    }
}
