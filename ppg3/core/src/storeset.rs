//! Ordered multi-store lookup (WP1, PPG3_DESIGN.md §4.1, CONTRACT.md
//! "StoreSet").
//!
//! Hits are consumed in place — `StoreSet::lookup` returns the index of the
//! store that hit so callers (scheduler/sandbox) can bind-mount or link
//! directly from that store's `data_dir(oh)` rather than copying.

use crate::error::Error;
use crate::manifest::Manifest;
use crate::store::Store;

pub struct StoreSet {
    pub stores: Vec<Store>,
}

impl StoreSet {
    pub fn new(stores: Vec<Store>) -> StoreSet {
        StoreSet { stores }
    }

    /// Walk the stores in order; return the first hit along with its index.
    pub fn lookup(&self, ik: &str) -> Result<Option<(usize, Manifest)>, Error> {
        for (i, s) in self.stores.iter().enumerate() {
            if let Some(m) = s.lookup(ik)? {
                return Ok(Some((i, m)));
            }
        }
        Ok(None)
    }

    /// The store a job should publish into: `job_target` by name if given
    /// (must be writable), else the first writable store in list order.
    pub fn write_store(&self, job_target: Option<&str>) -> Result<&Store, Error> {
        if let Some(name) = job_target {
            self.stores
                .iter()
                .find(|s| s.name() == name)
                .ok_or_else(|| Error::Other(format!("no store named {name:?} configured")))
                .and_then(|s| {
                    if s.is_readonly() {
                        Err(Error::ReadOnlyStore(name.to_string()))
                    } else {
                        Ok(s)
                    }
                })
        } else {
            self.stores
                .iter()
                .find(|s| !s.is_readonly())
                .ok_or_else(|| Error::Other("no writable store configured".to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::BuiltInfo;

    fn built() -> BuiltInfo {
        BuiltInfo {
            start_ms: 0,
            end_ms: 1,
            host: "h".to_string(),
            sandboxed: false,
            ppg3_version: "0.1.0".to_string(),
            retain_evict: false,
        }
    }

    #[test]
    fn lookup_walks_in_order_and_returns_first_hit() {
        let d1 = tempfile::tempdir().unwrap();
        let d2 = tempfile::tempdir().unwrap();
        let s1 = Store::open("first", d1.path(), false).unwrap();
        let s2 = Store::open("second", d2.path(), false).unwrap();

        let ik = "1".repeat(64);
        let staging = s2.open_staging().unwrap();
        std::fs::write(staging.path().join("x"), b"hi").unwrap();
        s2.publish(staging, &ik, &serde_json::json!({}), built(), None)
            .unwrap();

        let set = StoreSet::new(vec![s1, s2]);
        let (idx, m) = set.lookup(&ik).unwrap().expect("hit in second store");
        assert_eq!(idx, 1);
        assert_eq!(m.input_key, ik);
    }

    #[test]
    fn lookup_miss_across_all_stores() {
        let d1 = tempfile::tempdir().unwrap();
        let s1 = Store::open("first", d1.path(), false).unwrap();
        let set = StoreSet::new(vec![s1]);
        assert!(set.lookup(&"9".repeat(64)).unwrap().is_none());
    }

    #[test]
    fn write_store_picks_first_writable_by_default() {
        let d1 = tempfile::tempdir().unwrap();
        let d2 = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(d1.path().join("v1")).unwrap();
        let ro = Store::open("ro", d1.path(), true).unwrap();
        let rw = Store::open("rw", d2.path(), false).unwrap();
        let set = StoreSet::new(vec![ro, rw]);
        let w = set.write_store(None).unwrap();
        assert_eq!(w.name(), "rw");
    }

    #[test]
    fn write_store_by_name_must_be_writable() {
        let d1 = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(d1.path().join("v1")).unwrap();
        let ro = Store::open("ro", d1.path(), true).unwrap();
        let set = StoreSet::new(vec![ro]);
        assert!(matches!(
            set.write_store(Some("ro")),
            Err(Error::ReadOnlyStore(_))
        ));
        assert!(set.write_store(Some("missing")).is_err());
    }
}
