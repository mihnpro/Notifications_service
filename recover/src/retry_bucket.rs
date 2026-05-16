use std::time::Duration;

#[derive(Debug, Clone, Copy)]
pub struct Bucket {
    pub delay: Duration,
    pub label: &'static str,
}

const LABELS: [&str; 4] = ["30s", "1m", "5m", "15m"];

pub fn for_attempt(attempt_count: i32, configured_seconds: &[u64]) -> Bucket {
    let idx = (attempt_count.max(1) as usize - 1).min(LABELS.len() - 1);
    let secs = configured_seconds
        .get(idx)
        .copied()
        .unwrap_or_else(|| default_seconds(idx));
    Bucket {
        delay: Duration::from_secs(secs),
        label: LABELS[idx],
    }
}

fn default_seconds(idx: usize) -> u64 {
    match idx {
        0 => 30,
        1 => 60,
        2 => 300,
        _ => 900,
    }
}

pub fn main_routing_key(region: &str, queue_group: &str, priority: &str) -> String {
    format!("notification.{region}.{queue_group}.{priority}")
}

pub const EXCHANGE_DIRECT: &str = "notification.direct";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_map_per_attempt() {
        let cfg = [30, 60, 300, 900];
        assert_eq!(for_attempt(1, &cfg).label, "30s");
        assert_eq!(for_attempt(2, &cfg).label, "1m");
        assert_eq!(for_attempt(3, &cfg).label, "5m");
        assert_eq!(for_attempt(4, &cfg).label, "15m");
        assert_eq!(for_attempt(99, &cfg).label, "15m");
    }

    #[test]
    fn zero_attempt_clamps_to_first() {
        let cfg = [30, 60, 300, 900];
        assert_eq!(for_attempt(0, &cfg).label, "30s");
    }

    #[test]
    fn routing_key_format() {
        assert_eq!(
            main_routing_key("eu", "email", "normal"),
            "notification.eu.email.normal"
        );
    }
}
