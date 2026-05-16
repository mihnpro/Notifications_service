use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use serde::Serialize;

#[derive(Clone)]
pub struct Registry {
    inner: Arc<Mutex<HashMap<String, JobRun>>>,
}

#[derive(Clone)]
struct JobRun {
    last_ran_at: Instant,
    last_rows: u64,
    last_outcome: &'static str,
    leader: bool,
}

#[derive(Serialize)]
pub struct JobSnapshot {
    pub name: String,
    pub last_run_ms_ago: u128,
    pub last_rows: u64,
    pub last_outcome: String,
    pub leader: bool,
}

impl Registry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn record(&self, name: &str, rows: u64, outcome: &'static str, leader: bool) {
        let mut g = self.inner.lock().expect("registry mutex poisoned");
        g.insert(
            name.to_string(),
            JobRun {
                last_ran_at: Instant::now(),
                last_rows: rows,
                last_outcome: outcome,
                leader,
            },
        );
    }

    pub fn snapshot(&self) -> Vec<JobSnapshot> {
        let g = self.inner.lock().expect("registry mutex poisoned");
        g.iter()
            .map(|(name, r)| JobSnapshot {
                name: name.clone(),
                last_run_ms_ago: r.last_ran_at.elapsed().as_millis(),
                last_rows: r.last_rows,
                last_outcome: r.last_outcome.to_string(),
                leader: r.leader,
            })
            .collect()
    }
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}
