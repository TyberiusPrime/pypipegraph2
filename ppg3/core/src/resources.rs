//! Named multi-unit resource pools (WP4, PPG3_DESIGN.md §8.1/§13 WP4,
//! CONTRACT.md "Scheduler" / "Resource pools").
//!
//! A real Condvar-based multi-unit semaphore. ppg2's audit finding B2 (the
//! Python `CoreLock` race — check-then-act across multiple pools without a
//! single owning lock, letting concurrent acquirers both observe capacity
//! and both proceed) must not be reproduced here: every pool's available
//! count lives behind *one* `Mutex`, and a multi-pool request is checked
//! and decremented atomically under that single lock, so there is no
//! window in which two threads can each believe they got the last unit.
//!
//! A request for a pool that doesn't exist, or for more units than the
//! pool's total capacity, is a **definition error** (`Error::Graph`) — it
//! can never be satisfied no matter how long the caller waits, so failing
//! fast (before ever touching the mutex's wait loop) is strictly better
//! than hanging forever.

use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex};

use crate::error::Error;

struct Inner {
    /// Fixed at construction: pool name -> total capacity.
    capacity: BTreeMap<String, u64>,
    /// Mutable: pool name -> currently *available* units.
    available: Mutex<BTreeMap<String, u64>>,
    cv: Condvar,
}

/// A set of named resource pools, cheaply cloneable (an `Arc` handle) so it
/// can be shared across scheduler worker threads.
#[derive(Clone)]
pub struct Pools(Arc<Inner>);

/// A held allocation. Releases its units back to the pools on `Drop` —
/// correctness never depends on the caller remembering to release
/// explicitly.
pub struct Guard {
    pools: Pools,
    amounts: BTreeMap<String, u64>,
}

impl Pools {
    pub fn new(capacity: BTreeMap<String, u64>) -> Pools {
        let available = capacity.clone();
        Pools(Arc::new(Inner {
            capacity,
            available: Mutex::new(available),
            cv: Condvar::new(),
        }))
    }

    /// Pool capacities as configured (for validating job resource requests
    /// against pools ahead of dispatch, CONTRACT.md scheduler validation
    /// step).
    pub fn capacities(&self) -> &BTreeMap<String, u64> {
        &self.0.capacity
    }

    /// A request that names an unknown pool, or asks for more than that
    /// pool's total capacity, can never succeed — a definition error,
    /// distinct from "block until available".
    fn validate(&self, requests: &BTreeMap<String, u64>) -> Result<(), Error> {
        for (name, amount) in requests {
            match self.0.capacity.get(name) {
                None => {
                    return Err(Error::Graph(format!(
                        "resource pool {name:?} is not configured"
                    )))
                }
                Some(cap) if *amount > *cap => {
                    return Err(Error::Graph(format!(
                        "request for {amount} units of pool {name:?} exceeds its capacity {cap}"
                    )))
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn can_satisfy(available: &BTreeMap<String, u64>, requests: &BTreeMap<String, u64>) -> bool {
        requests
            .iter()
            .all(|(name, amount)| available.get(name).copied().unwrap_or(0) >= *amount)
    }

    /// Block until `requests` (pool name -> units) can be satisfied
    /// atomically, then hold them until the returned `Guard` drops.
    /// `requests` may be empty (always succeeds immediately).
    pub fn acquire(&self, requests: &BTreeMap<String, u64>) -> Result<Guard, Error> {
        self.validate(requests)?;
        let mut available = self.0.available.lock().unwrap();
        loop {
            if Self::can_satisfy(&available, requests) {
                for (name, amount) in requests {
                    *available.get_mut(name).unwrap() -= amount;
                }
                return Ok(Guard {
                    pools: self.clone(),
                    amounts: requests.clone(),
                });
            }
            available = self.0.cv.wait(available).unwrap();
        }
    }

    /// Non-blocking `acquire`: `Ok(None)` means the request is well-formed
    /// but cannot be satisfied *right now*; `Err` means it can never be
    /// satisfied (definition error).
    pub fn try_acquire(&self, requests: &BTreeMap<String, u64>) -> Result<Option<Guard>, Error> {
        self.validate(requests)?;
        let mut available = self.0.available.lock().unwrap();
        if Self::can_satisfy(&available, requests) {
            for (name, amount) in requests {
                *available.get_mut(name).unwrap() -= amount;
            }
            Ok(Some(Guard {
                pools: self.clone(),
                amounts: requests.clone(),
            }))
        } else {
            Ok(None)
        }
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        if self.amounts.is_empty() {
            return;
        }
        let mut available = self.pools.0.available.lock().unwrap();
        for (name, amount) in &self.amounts {
            if let Some(slot) = available.get_mut(name) {
                *slot += amount;
            }
        }
        drop(available);
        self.pools.0.cv.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;

    fn pools_of(cap: u64) -> Pools {
        let mut m = BTreeMap::new();
        m.insert("p".to_string(), cap);
        Pools::new(m)
    }

    fn req(n: u64) -> BTreeMap<String, u64> {
        let mut m = BTreeMap::new();
        m.insert("p".to_string(), n);
        m
    }

    #[test]
    fn acquire_and_release_roundtrip() {
        let pools = pools_of(4);
        {
            let _g = pools.acquire(&req(4)).unwrap();
            // pool exhausted: try_acquire for any amount must fail to get one now
            assert!(pools.try_acquire(&req(1)).unwrap().is_none());
        }
        // released on drop
        assert!(pools.try_acquire(&req(4)).unwrap().is_some());
    }

    #[test]
    fn unknown_pool_is_definition_error() {
        let pools = pools_of(4);
        let mut r = BTreeMap::new();
        r.insert("nope".to_string(), 1);
        assert!(matches!(pools.acquire(&r), Err(Error::Graph(_))));
        assert!(matches!(pools.try_acquire(&r), Err(Error::Graph(_))));
    }

    #[test]
    fn over_capacity_request_is_definition_error() {
        let pools = pools_of(4);
        assert!(matches!(pools.acquire(&req(5)), Err(Error::Graph(_))));
    }

    #[test]
    fn empty_request_always_succeeds() {
        let pools = pools_of(0);
        let _g = pools.acquire(&BTreeMap::new()).unwrap();
    }

    #[test]
    fn multi_pool_acquisition_is_atomic() {
        let mut cap = BTreeMap::new();
        cap.insert("a".to_string(), 1);
        cap.insert("b".to_string(), 1);
        let pools = Pools::new(cap);
        let mut r_a = BTreeMap::new();
        r_a.insert("a".to_string(), 1);
        let g_a = pools.acquire(&r_a).unwrap();
        // A request needing both a and b must not partially acquire b while
        // blocked on a - try_acquire must cleanly report "not now", leaving
        // b fully available for someone who only wants b.
        let mut r_ab = BTreeMap::new();
        r_ab.insert("a".to_string(), 1);
        r_ab.insert("b".to_string(), 1);
        assert!(pools.try_acquire(&r_ab).unwrap().is_none());
        let mut r_b = BTreeMap::new();
        r_b.insert("b".to_string(), 1);
        assert!(pools.try_acquire(&r_b).unwrap().is_some());
        drop(g_a);
    }

    /// The ppg2 audit's CoreLock race (B2): concurrent acquirers of random
    /// sizes against a small pool must never observe more units in use than
    /// the pool's capacity, and every acquisition must eventually complete
    /// (no lost wakeups / stuck waiters).
    #[test]
    fn stress_never_exceeds_capacity() {
        const CAPACITY: u64 = 4;
        let pools = pools_of(CAPACITY);
        let in_use = Arc::new(AtomicU64::new(0));
        let max_observed = Arc::new(AtomicU64::new(0));

        let mut handles = Vec::new();
        for t in 0..8u64 {
            let pools = pools.clone();
            let in_use = in_use.clone();
            let max_observed = max_observed.clone();
            handles.push(thread::spawn(move || {
                // Deterministic-but-varied pseudo-random sizes without
                // pulling in a `rand` dependency.
                let mut state = 0x2545F4914F6CDD1Du64 ^ (t.wrapping_mul(0x9E3779B97F4A7C15));
                for _ in 0..200 {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    let size = 1 + (state % CAPACITY);
                    let guard = pools.acquire(&{
                        let mut m = BTreeMap::new();
                        m.insert("p".to_string(), size);
                        m
                    });
                    let guard = guard.unwrap();
                    let now = in_use.fetch_add(size, Ordering::SeqCst) + size;
                    max_observed.fetch_max(now, Ordering::SeqCst);
                    // Hold briefly to widen the window in which a race
                    // would manifest as over-capacity usage.
                    thread::yield_now();
                    in_use.fetch_sub(size, Ordering::SeqCst);
                    drop(guard);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert!(
            max_observed.load(Ordering::SeqCst) <= CAPACITY,
            "pool of capacity {CAPACITY} was over-subscribed: observed {}",
            max_observed.load(Ordering::SeqCst)
        );
        assert_eq!(in_use.load(Ordering::SeqCst), 0, "all acquisitions must have released");
    }
}
