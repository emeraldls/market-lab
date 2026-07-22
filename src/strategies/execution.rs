use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use anyhow::{Result, bail};

use crate::domain::execution::{CancelPlan, ExecutionReceipt, ExecutionVenue, Fill, TradePlan};
use crate::providers::execution::ExecutionAdapter;

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct FillProgress {
    pub filled_size: f64,
    pub fill_notional: f64,
}

impl FillProgress {
    pub fn vwap(self) -> Option<f64> {
        (self.filled_size > f64::EPSILON).then_some(self.fill_notional / self.filled_size)
    }
}

#[derive(Clone, Debug)]
struct WorkingOrder {
    order_id: String,
    price: f64,
    remaining_size: f64,
    submitted_at: Instant,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct FillFingerprint {
    order_id: String,
    slot: u64,
    ts_ms: u64,
    amount_bits: u64,
    price_bits: u64,
    maker: bool,
}

impl FillFingerprint {
    fn from_fill(fill: &Fill, order_id: &str) -> Self {
        Self {
            order_id: order_id.to_string(),
            slot: fill.slot,
            ts_ms: fill.ts_ms,
            amount_bits: fill.amount.to_bits(),
            price_bits: fill.price.to_bits(),
            maker: fill.maker,
        }
    }
}

pub struct StrategyOrderManager {
    job_id: String,
    venue: ExecutionVenue,
    account: String,
    internal_symbol: String,
    venue_symbol: String,
    order_sequence: u64,
    cancel_sequence: u64,
    owned_order_ids: HashSet<String>,
    seen_fill_counts: HashMap<FillFingerprint, usize>,
    fill_progress: FillProgress,
    working: Option<WorkingOrder>,
}

impl StrategyOrderManager {
    pub fn new(job_id: &str, parent: &TradePlan) -> Self {
        Self {
            job_id: job_id.to_string(),
            venue: parent.venue,
            account: parent.account.clone(),
            internal_symbol: parent.internal_symbol.clone(),
            venue_symbol: parent.venue_symbol.clone(),
            order_sequence: 0,
            cancel_sequence: 0,
            owned_order_ids: HashSet::new(),
            seen_fill_counts: HashMap::new(),
            fill_progress: FillProgress::default(),
            working: None,
        }
    }

    pub fn submitted_orders(&self) -> u64 {
        self.order_sequence
    }

    pub fn has_working_order(&self) -> bool {
        self.working.is_some()
    }

    pub fn working_remaining_size(&self) -> f64 {
        self.working
            .as_ref()
            .map_or(0.0, |order| order.remaining_size)
    }

    pub fn working_needs_replace(
        &self,
        price: f64,
        size: f64,
        tick_size: f64,
        lot_size: f64,
        stale_after: Duration,
    ) -> bool {
        self.working.as_ref().is_some_and(|order| {
            (order.price - price).abs() >= tick_size / 2.0
                || order.submitted_at.elapsed() >= stale_after
                || (order.remaining_size - size).abs() >= lot_size.max(order.remaining_size * 0.20)
        })
    }

    pub async fn submit(
        &mut self,
        plan: &TradePlan,
        maker_price: Option<f64>,
    ) -> Result<ExecutionReceipt> {
        if plan.venue != self.venue
            || plan.account != self.account
            || plan.internal_symbol != self.internal_symbol
            || plan.venue_symbol != self.venue_symbol
        {
            bail!("strategy order manager received a plan for another parent order");
        }
        let sequence = self.order_sequence + 1;
        let receipt = crate::runtime::submit_strategy_trade(&self.job_id, sequence, plan).await?;
        self.order_sequence = sequence;
        if let Some(order_id) = &receipt.order_id {
            self.owned_order_ids.insert(order_id.clone());
            if let Some(price) = maker_price
                && !receipt.terminal
            {
                self.working = Some(WorkingOrder {
                    order_id: order_id.clone(),
                    price,
                    remaining_size: plan.size,
                    submitted_at: Instant::now(),
                });
            }
        }
        Ok(receipt)
    }

    pub async fn cancel_working(&mut self) -> Result<bool> {
        let Some(order) = self.working.as_ref() else {
            return Ok(false);
        };
        let order_id = order.order_id.clone();
        let sequence = self.cancel_sequence + 1;
        let plan = CancelPlan {
            created_at_ms: now_ms()?,
            venue: self.venue,
            account: self.account.clone(),
            internal_symbol: self.internal_symbol.clone(),
            venue_symbol: self.venue_symbol.clone(),
            order_id: order_id.clone(),
        };
        crate::runtime::submit_strategy_cancel(&self.job_id, sequence, &plan).await?;
        self.cancel_sequence = sequence;
        if self
            .working
            .as_ref()
            .is_some_and(|working| working.order_id == order_id)
        {
            self.working = None;
        }
        Ok(true)
    }

    pub async fn reconcile(&mut self, adapter: &ExecutionAdapter) -> Result<FillProgress> {
        if let Some(current) = self.working.as_mut() {
            let open = adapter
                .open_orders(&self.account)
                .await?
                .into_iter()
                .find(|order| order.order_id == current.order_id);
            match open {
                Some(order) => current.remaining_size = order.remaining_size,
                None if current.submitted_at.elapsed() >= Duration::from_secs(2) => {
                    self.working = None;
                }
                None => {}
            }
        }

        if self.owned_order_ids.is_empty() {
            return Ok(self.fill_progress);
        }
        self.record_fills(adapter.fills(&self.account).await?);
        Ok(self.fill_progress)
    }

    pub async fn wait_for_target(
        &mut self,
        adapter: &ExecutionAdapter,
        target_size: f64,
        lot_size: f64,
        timeout: Duration,
    ) -> Result<FillProgress> {
        let deadline = Instant::now() + timeout;
        loop {
            let progress = self.reconcile(adapter).await?;
            if progress.filled_size + lot_size / 2.0 >= target_size || Instant::now() >= deadline {
                return Ok(progress);
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    fn record_fills(&mut self, fills: Vec<Fill>) {
        let mut response_counts = HashMap::<FillFingerprint, usize>::new();
        for fill in fills {
            let Some(order_id) = fill.order_id.as_deref() else {
                continue;
            };
            if !self.owned_order_ids.contains(order_id) {
                continue;
            }
            let fingerprint = FillFingerprint::from_fill(&fill, order_id);
            let response_count = response_counts.entry(fingerprint.clone()).or_default();
            *response_count += 1;
            let previously_seen = self
                .seen_fill_counts
                .get(&fingerprint)
                .copied()
                .unwrap_or_default();
            if *response_count > previously_seen {
                self.fill_progress.filled_size += fill.amount;
                self.fill_progress.fill_notional += fill.amount * fill.price;
            }
        }
        for (fingerprint, response_count) in response_counts {
            let seen = self.seen_fill_counts.entry(fingerprint).or_default();
            *seen = (*seen).max(response_count);
        }
    }
}

fn now_ms() -> Result<u64> {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| anyhow::anyhow!("system clock is before the Unix epoch"))?
        .as_millis();
    u64::try_from(millis).map_err(|_| anyhow::anyhow!("current timestamp does not fit in u64"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::execution::{OrderKind, OrderSide, PositionDirection};

    fn parent() -> TradePlan {
        TradePlan {
            created_at_ms: 1,
            venue: ExecutionVenue::Bulk,
            account: "account".to_string(),
            internal_symbol: "BTC/USDT".to_string(),
            venue_symbol: "BTC-USD".to_string(),
            direction: PositionDirection::Long,
            side: OrderSide::Buy,
            order_kind: OrderKind::Market,
            time_in_force: None,
            requested_size: Some(1.0),
            size: 1.0,
            price: None,
            reference_price: 100.0,
            requested_margin: None,
            estimated_margin: 100.0,
            estimated_exposure: 100.0,
            projected_liquidation_price: None,
            leverage: 1.0,
            reduce_only: false,
            stop_loss_price: None,
            take_profit_price: None,
        }
    }

    #[test]
    fn manager_starts_without_orders() {
        let manager = StrategyOrderManager::new("strategy_1", &parent());
        assert_eq!(manager.submitted_orders(), 0);
        assert!(!manager.has_working_order());
    }

    #[test]
    fn fill_progress_is_cumulative_and_deduplicated() {
        let mut manager = StrategyOrderManager::new("strategy_1", &parent());
        manager.owned_order_ids.insert("owned".to_string());
        let fill = Fill {
            venue: ExecutionVenue::Bulk,
            internal_symbol: "BTC/USDT".to_string(),
            venue_symbol: "BTC-USD".to_string(),
            registry_supported: true,
            side: OrderSide::Buy,
            amount: 0.25,
            price: 100.0,
            reason: "normal".to_string(),
            order_id: Some("owned".to_string()),
            maker: true,
            fee: None,
            slot: 7,
            ts_ms: 1_000,
        };

        manager.record_fills(vec![fill.clone(), fill.clone()]);
        manager.record_fills(vec![fill]);
        manager.record_fills(Vec::new());

        assert_eq!(manager.fill_progress.filled_size, 0.5);
        assert_eq!(manager.fill_progress.fill_notional, 50.0);
        assert_eq!(manager.fill_progress.vwap(), Some(100.0));
    }
}
