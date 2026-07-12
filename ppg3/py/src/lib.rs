//! PyO3 bindings for ppg3-core (WP7) — placeholder, filled per ppg3/CONTRACT.md.
//! Module name: ppg3._core

use pyo3::prelude::*;

#[pymodule]
#[pyo3(name = "_core")]
fn ppg3_core(_m: &Bound<'_, PyModule>) -> PyResult<()> {
    Ok(())
}
