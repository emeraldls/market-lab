use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

use crate::domain::execution::ExecutionVenue;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MidPriceJobDefinition {
    pub venue: ExecutionVenue,
    pub symbol: String,
    /// Hard one-sided inventory limit in normalized base-asset units.
    #[serde(default)]
    pub max_inventory_size: f64,
    pub requested_margin: Option<f64>,
    #[serde(default)]
    pub max_inventory_margin: f64,
    #[serde(default)]
    pub max_inventory_exposure: f64,
    pub duration_seconds: u64,
    pub spread_bps: f64,
    #[serde(default)]
    pub refresh_seconds: f64,
    #[serde(default)]
    pub refresh_tolerance_bps: f64,
    #[serde(default)]
    pub directional_bias_percent: f64,
    pub leverage: f64,
    #[serde(default)]
    pub stop_loss_pct: Option<f64>,
}

impl MidPriceJobDefinition {
    pub fn validate(&self) -> Result<()> {
        if self.symbol.trim().is_empty() {
            bail!("mid-price bot symbol is required");
        }
        if !self.max_inventory_size.is_finite() || self.max_inventory_size <= 0.0 {
            bail!("mid-price bot maximum inventory size must be greater than zero");
        }
        if self
            .requested_margin
            .is_some_and(|margin| !margin.is_finite() || margin <= 0.0)
        {
            bail!("mid-price bot requested margin must be greater than zero");
        }
        if !self.max_inventory_margin.is_finite() || self.max_inventory_margin <= 0.0 {
            bail!("mid-price bot maximum inventory margin must be greater than zero");
        }
        if !self.max_inventory_exposure.is_finite() || self.max_inventory_exposure <= 0.0 {
            bail!("mid-price bot maximum inventory exposure must be greater than zero");
        }
        if self.duration_seconds == 0 {
            bail!("mid-price bot duration must be at least one second");
        }
        if !self.spread_bps.is_finite() || self.spread_bps < 0.0 {
            bail!("mid-price bot spread must be zero or greater");
        }
        if !self.refresh_seconds.is_finite() || self.refresh_seconds <= 0.0 {
            bail!("mid-price bot refresh time must be greater than zero seconds");
        }
        if !self.refresh_tolerance_bps.is_finite() || self.refresh_tolerance_bps < 0.0 {
            bail!("mid-price bot refresh tolerance must be zero or greater");
        }
        if !self.directional_bias_percent.is_finite()
            || !(-100.0..=100.0).contains(&self.directional_bias_percent)
        {
            bail!("mid-price bot directional bias must be between -100 and 100 percent");
        }
        if !self.leverage.is_finite() || self.leverage < 1.0 {
            bail!("mid-price bot leverage must be at least 1");
        }
        if self
            .stop_loss_pct
            .is_some_and(|percent| !percent.is_finite() || !(0.0..=100.0).contains(&percent))
        {
            bail!("mid-price bot stop loss must be between 0 and 100 percent");
        }
        let expected_margin = self.max_inventory_exposure / self.leverage;
        if (self.max_inventory_margin - expected_margin).abs()
            > 1e-8_f64.max(expected_margin.abs() * 1e-10)
        {
            bail!("mid-price bot margin, exposure, and leverage do not agree");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "name", content = "config", rename_all = "snake_case")]
pub enum BotJobDefinition {
    MidPrice(MidPriceJobDefinition),
    VolumeMid(MidPriceJobDefinition),
}

impl BotJobDefinition {
    pub fn name(&self) -> &'static str {
        match self {
            Self::MidPrice(_) => "mid-price",
            Self::VolumeMid(_) => "volume-mid",
        }
    }

    pub fn symbol(&self) -> &str {
        match self {
            Self::MidPrice(definition) | Self::VolumeMid(definition) => &definition.symbol,
        }
    }

    pub fn venue(&self) -> ExecutionVenue {
        match self {
            Self::MidPrice(definition) | Self::VolumeMid(definition) => definition.venue,
        }
    }

    pub fn leverage(&self) -> f64 {
        match self {
            Self::MidPrice(definition) | Self::VolumeMid(definition) => definition.leverage,
        }
    }

    pub fn validate(&self) -> Result<()> {
        match self {
            Self::MidPrice(definition) | Self::VolumeMid(definition) => definition.validate(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BotJobSubmission {
    pub definition: BotJobDefinition,
}

impl BotJobSubmission {
    pub fn validate(&self) -> Result<()> {
        self.definition.validate()
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BotJobStatus {
    Starting,
    Running,
    Stopping,
    Stopped,
    Completed,
    Failed,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BotPerformance {
    pub allocated_margin: f64,
    pub bought_size: f64,
    pub sold_size: f64,
    pub matched_size: f64,
    pub average_buy_price: Option<f64>,
    pub average_sell_price: Option<f64>,
    pub inventory_size: f64,
    pub average_entry_price: Option<f64>,
    pub mark_price: f64,
    pub gross_realized_pnl: f64,
    pub unrealized_pnl: f64,
    /// Signed venue fees: negative is a cost and positive is a rebate.
    pub fees: f64,
    pub fees_complete: bool,
    /// Realized plus unrealized PnL and signed fees. Funding is excluded.
    pub trading_pnl: Option<f64>,
    pub return_on_margin_pct: Option<f64>,
}

impl BotJobStatus {
    pub fn is_active(self) -> bool {
        matches!(self, Self::Starting | Self::Running | Self::Stopping)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BotJob {
    pub id: String,
    pub definition: BotJobDefinition,
    pub status: BotJobStatus,
    pub pid: Option<u32>,
    pub created_at_ms: u64,
    pub started_at_ms: Option<u64>,
    pub stopped_at_ms: Option<u64>,
    pub last_heartbeat_ms: Option<u64>,
    pub last_error: Option<String>,
    #[serde(default)]
    pub performance: Option<BotPerformance>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_mid_price_jobs_decode_for_daemon_version_negotiation() {
        let definition: BotJobDefinition = serde_json::from_value(serde_json::json!({
            "name": "mid_price",
            "config": {
                "venue": "bulk",
                "symbol": "BTC/USDT",
                "targetSizePerSide": 0.01,
                "requestedMargin": 100.0,
                "targetMargin": 50.0,
                "targetExposure": 500.0,
                "durationSeconds": 300,
                "spreadBps": 2.0,
                "directionalBiasPercent": 0.0,
                "leverage": 10.0
            }
        }))
        .expect("legacy bot definition should decode");

        let BotJobDefinition::MidPrice(definition) = definition else {
            panic!("expected a mid-price definition");
        };
        assert_eq!(definition.max_inventory_size, 0.0);
        assert_eq!(definition.max_inventory_margin, 0.0);
        assert_eq!(definition.max_inventory_exposure, 0.0);
        assert_eq!(definition.refresh_seconds, 0.0);
        assert_eq!(definition.refresh_tolerance_bps, 0.0);
        assert!(definition.validate().is_err());
    }
}
