use std::collections::VecDeque;

use serde_json::{Value, json};

#[derive(Debug)]
pub(crate) struct PnlHistory {
    capacity: usize,
    points: VecDeque<(u64, f64)>,
}

impl PnlHistory {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(2),
            points: VecDeque::new(),
        }
    }

    pub(crate) fn record(&mut self, ts_ms: u64, pnl: f64) {
        if let Some((last_ts_ms, last_pnl)) = self.points.back_mut()
            && *last_ts_ms == ts_ms
        {
            *last_pnl = pnl;
            return;
        }
        self.points.push_back((ts_ms, pnl));
        while self.points.len() > self.capacity {
            self.points.pop_front();
        }
    }

    pub(crate) fn payload(&self) -> Value {
        Value::Array(
            self.points
                .iter()
                .map(|(t, pnl)| json!({ "t": t, "pnl": pnl }))
                .collect(),
        )
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.points.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replaces_duplicate_timestamps_and_keeps_the_configured_tail() {
        let mut history = PnlHistory::new(2);
        history.record(1_000, 1.0);
        history.record(1_000, 2.0);
        history.record(2_000, 3.0);
        history.record(3_000, 4.0);

        assert_eq!(history.len(), 2);
        assert_eq!(
            history.payload(),
            json!([
                { "t": 2_000, "pnl": 3.0 },
                { "t": 3_000, "pnl": 4.0 }
            ])
        );
    }
}
