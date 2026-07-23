use std::cmp::Ordering;
use std::collections::BTreeMap;

use crate::domain::types::{OrderBookLevel, OrderBookSnapshot};

#[derive(Clone, Copy, Debug)]
struct Price(f64);

impl Price {
    fn new(value: f64) -> Option<Self> {
        (value.is_finite() && value > 0.0).then_some(Self(value))
    }
}

impl PartialEq for Price {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Price {}

impl PartialOrd for Price {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Price {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// An authoritative local order book built from one full snapshot followed by
/// price-level deltas.
///
/// Both sides are stored in ascending price order. The best bid is therefore
/// the last bid and the best ask is the first ask, so top-of-book reads do not
/// allocate or sort.
#[derive(Default)]
pub struct OrderBookState {
    initialized: bool,
    exchange: String,
    symbol: String,
    timestamp_ms: u64,
    bids: BTreeMap<Price, f64>,
    asks: BTreeMap<Price, f64>,
    seq: Option<u64>,
}

impl OrderBookState {
    pub fn apply_snapshot<B, A>(
        &mut self,
        exchange: String,
        symbol: String,
        timestamp_ms: u64,
        bids: B,
        asks: A,
        seq: Option<u64>,
    ) where
        B: IntoIterator<Item = OrderBookLevel>,
        A: IntoIterator<Item = OrderBookLevel>,
    {
        let mut next_bids = BTreeMap::new();
        let mut next_asks = BTreeMap::new();
        insert_snapshot_levels(&mut next_bids, bids);
        insert_snapshot_levels(&mut next_asks, asks);

        self.exchange = exchange;
        self.symbol = symbol;
        self.timestamp_ms = timestamp_ms;
        self.bids = next_bids;
        self.asks = next_asks;
        self.seq = seq;
        self.initialized = true;
    }

    pub fn apply_delta<B, A>(
        &mut self,
        timestamp_ms: u64,
        bid_updates: B,
        ask_updates: A,
        seq: Option<u64>,
    ) where
        B: IntoIterator<Item = OrderBookLevel>,
        A: IntoIterator<Item = OrderBookLevel>,
    {
        if !self.initialized {
            return;
        }

        self.timestamp_ms = timestamp_ms;
        for level in bid_updates {
            apply_level(&mut self.bids, level);
        }
        for level in ask_updates {
            apply_level(&mut self.asks, level);
        }
        self.seq = seq.or(self.seq);
    }

    /// Clears all levels and requires a new snapshot before deltas are applied.
    pub fn reset(&mut self) {
        *self = Self::default();
    }

    pub fn best_bid_ask(&self) -> Option<(OrderBookLevel, OrderBookLevel)> {
        if !self.initialized {
            return None;
        }
        let (bid, bid_quantity) = self.bids.last_key_value()?;
        let (ask, ask_quantity) = self.asks.first_key_value()?;
        Some((
            OrderBookLevel {
                price: bid.0,
                quantity: *bid_quantity,
            },
            OrderBookLevel {
                price: ask.0,
                quantity: *ask_quantity,
            },
        ))
    }

    pub fn timestamp_ms(&self) -> u64 {
        self.timestamp_ms
    }

    pub fn snapshot(&self, depth: u16) -> Option<OrderBookSnapshot> {
        if !self.initialized || self.bids.is_empty() || self.asks.is_empty() {
            return None;
        }

        let depth = depth as usize;
        Some(OrderBookSnapshot {
            exchange: self.exchange.clone(),
            symbol: self.symbol.clone(),
            timestamp_ms: self.timestamp_ms,
            bids: self
                .bids
                .iter()
                .rev()
                .take(depth)
                .map(|(price, quantity)| OrderBookLevel {
                    price: price.0,
                    quantity: *quantity,
                })
                .collect(),
            asks: self
                .asks
                .iter()
                .take(depth)
                .map(|(price, quantity)| OrderBookLevel {
                    price: price.0,
                    quantity: *quantity,
                })
                .collect(),
        })
    }
}

fn insert_snapshot_levels(
    side: &mut BTreeMap<Price, f64>,
    levels: impl IntoIterator<Item = OrderBookLevel>,
) {
    for level in levels {
        let Some(price) = Price::new(level.price) else {
            continue;
        };
        if level.quantity.is_finite() && level.quantity > 0.0 {
            side.insert(price, level.quantity);
        }
    }
}

fn apply_level(side: &mut BTreeMap<Price, f64>, level: OrderBookLevel) {
    let Some(price) = Price::new(level.price) else {
        return;
    };
    if !level.quantity.is_finite() || level.quantity < 0.0 {
        return;
    }
    if level.quantity == 0.0 {
        side.remove(&price);
    } else {
        side.insert(price, level.quantity);
    }
}

#[cfg(test)]
mod tests {
    use std::hint::black_box;
    use std::time::Instant;

    use super::*;

    fn level(price: f64, quantity: f64) -> OrderBookLevel {
        OrderBookLevel { price, quantity }
    }

    #[test]
    fn snapshot_and_deltas_maintain_an_ordered_book() {
        let mut book = OrderBookState::default();
        book.apply_snapshot(
            "bulk".to_string(),
            "BTC/USDT".to_string(),
            10,
            vec![level(99.0, 1.0), level(100.0, 2.0), level(98.0, 3.0)],
            vec![level(102.0, 3.0), level(101.0, 2.0), level(103.0, 1.0)],
            None,
        );

        assert_eq!(
            book.best_bid_ask(),
            Some((level(100.0, 2.0), level(101.0, 2.0)))
        );

        book.apply_delta(
            11,
            vec![level(100.0, 4.0), level(100.5, 1.0)],
            vec![level(101.0, 0.0), level(100.75, 5.0)],
            None,
        );

        assert_eq!(book.timestamp_ms(), 11);
        assert_eq!(
            book.best_bid_ask(),
            Some((level(100.5, 1.0), level(100.75, 5.0)))
        );
        let snapshot = book.snapshot(3).expect("book remains initialized");
        assert_eq!(
            snapshot.bids,
            vec![level(100.5, 1.0), level(100.0, 4.0), level(99.0, 1.0)]
        );
        assert_eq!(
            snapshot.asks,
            vec![level(100.75, 5.0), level(102.0, 3.0), level(103.0, 1.0)]
        );
    }

    #[test]
    fn deleting_top_level_reveals_the_next_full_book_level() {
        let mut book = OrderBookState::default();
        book.apply_snapshot(
            "bulk".to_string(),
            "BTC/USDT".to_string(),
            1,
            vec![level(100.0, 1.0), level(99.0, 2.0), level(98.0, 3.0)],
            vec![level(101.0, 1.0), level(102.0, 2.0), level(103.0, 3.0)],
            None,
        );
        book.apply_delta(2, vec![level(100.0, 0.0)], vec![level(101.0, 0.0)], None);

        assert_eq!(
            book.best_bid_ask(),
            Some((level(99.0, 2.0), level(102.0, 2.0)))
        );
    }

    #[test]
    fn ignores_deltas_until_snapshot_and_after_reset() {
        let mut book = OrderBookState::default();
        book.apply_delta(1, vec![level(100.0, 1.0)], vec![level(101.0, 1.0)], None);
        assert_eq!(book.best_bid_ask(), None);

        book.apply_snapshot(
            "bulk".to_string(),
            "BTC/USDT".to_string(),
            2,
            vec![level(100.0, 1.0)],
            vec![level(101.0, 1.0)],
            None,
        );
        book.reset();
        book.apply_delta(3, vec![level(102.0, 1.0)], vec![level(103.0, 1.0)], None);

        assert_eq!(book.best_bid_ask(), None);
        assert!(book.snapshot(10).is_none());
    }

    #[test]
    #[ignore = "local order-book benchmark; run with --release --ignored --nocapture"]
    fn bench_orderbook_hot_paths() {
        const LEVELS: usize = 1_000;
        const UPDATES: usize = 50_000;
        const TOP_READS: usize = 1_000_000;

        let bids: Vec<_> = (0..LEVELS)
            .map(|index| level(100_000.0 - index as f64 * 0.25, 1.0))
            .collect();
        let asks: Vec<_> = (0..LEVELS)
            .map(|index| level(100_000.25 + index as f64 * 0.25, 1.0))
            .collect();
        let mut book = OrderBookState::default();
        book.apply_snapshot(
            "benchmark".to_string(),
            "BTC/USDT".to_string(),
            1,
            bids,
            asks,
            None,
        );

        let update_started = Instant::now();
        for update in 0..UPDATES {
            let index = update % (LEVELS / 2);
            book.apply_delta(
                update as u64 + 2,
                vec![level(
                    100_000.0 - index as f64 * 0.25,
                    1.0 + (update % 17) as f64,
                )],
                Vec::new(),
                None,
            );
        }
        let update_elapsed = update_started.elapsed();

        let top_started = Instant::now();
        for _ in 0..TOP_READS {
            black_box(book.best_bid_ask());
        }
        let top_elapsed = top_started.elapsed();

        eprintln!(
            "orderbook delta: {UPDATES} updates in {update_elapsed:?} ({:.1} ns/update)",
            update_elapsed.as_nanos() as f64 / UPDATES as f64
        );
        eprintln!(
            "top of book: {TOP_READS} reads in {top_elapsed:?} ({:.1} ns/read)",
            top_elapsed.as_nanos() as f64 / TOP_READS as f64
        );
    }
}
