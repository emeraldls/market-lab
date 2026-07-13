use std::cmp::Ordering;

use crate::domain::types::{OrderBookLevel, OrderBookSnapshot};

#[derive(Default)]
pub struct OrderBookState {
    initialized: bool,
    exchange: String,
    symbol: String,
    timestamp_ms: u64,
    bids: Vec<OrderBookLevel>,
    asks: Vec<OrderBookLevel>,
    seq: Option<u64>,
    max_levels_per_side: usize,
}

impl OrderBookState {
    pub fn with_max_levels_per_side(max_levels_per_side: usize) -> Self {
        Self {
            max_levels_per_side,
            ..Self::default()
        }
    }

    pub fn apply_snapshot(
        &mut self,
        exchange: String,
        symbol: String,
        timestamp_ms: u64,
        bids: Vec<OrderBookLevel>,
        asks: Vec<OrderBookLevel>,
        seq: Option<u64>,
    ) {
        self.exchange = exchange;
        self.symbol = symbol;
        self.timestamp_ms = timestamp_ms;
        self.bids = bids;
        self.asks = asks;
        self.sort();
        self.clamp_levels();
        self.seq = seq;
        self.initialized = true;
    }

    pub fn apply_delta(
        &mut self,
        timestamp_ms: u64,
        bid_updates: Vec<OrderBookLevel>,
        ask_updates: Vec<OrderBookLevel>,
        seq: Option<u64>,
    ) {
        if !self.initialized {
            return;
        }

        self.timestamp_ms = timestamp_ms;
        for lvl in bid_updates {
            upsert_level(&mut self.bids, lvl, true);
        }
        for lvl in ask_updates {
            upsert_level(&mut self.asks, lvl, false);
        }
        self.sort();
        self.clamp_levels();
        self.seq = seq.or(self.seq);
    }

    pub fn snapshot(&self, depth: u16) -> Option<OrderBookSnapshot> {
        if !self.initialized || self.bids.is_empty() || self.asks.is_empty() {
            return None;
        }

        let d = depth as usize;
        Some(OrderBookSnapshot {
            exchange: self.exchange.clone(),
            symbol: self.symbol.clone(),
            timestamp_ms: self.timestamp_ms,
            bids: self.bids.iter().take(d).cloned().collect(),
            asks: self.asks.iter().take(d).cloned().collect(),
        })
    }

    fn sort(&mut self) {
        self.bids
            .sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap_or(Ordering::Equal));
        self.asks
            .sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap_or(Ordering::Equal));
    }

    fn clamp_levels(&mut self) {
        if self.max_levels_per_side == 0 {
            return;
        }
        if self.bids.len() > self.max_levels_per_side {
            self.bids.truncate(self.max_levels_per_side);
        }
        if self.asks.len() > self.max_levels_per_side {
            self.asks.truncate(self.max_levels_per_side);
        }
    }
}

fn upsert_level(levels: &mut Vec<OrderBookLevel>, update: OrderBookLevel, is_bid: bool) {
    let eps = 1e-12;
    if let Some(idx) = levels
        .iter()
        .position(|l| (l.price - update.price).abs() < eps)
    {
        if update.quantity <= 0.0 {
            levels.remove(idx);
        } else {
            levels[idx].quantity = update.quantity;
        }
        return;
    }

    if update.quantity <= 0.0 {
        return;
    }

    levels.push(update);

    if is_bid {
        levels.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap_or(Ordering::Equal));
    } else {
        levels.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap_or(Ordering::Equal));
    }
}
