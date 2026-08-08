use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::bots::grid::MAX_GRID_LEVELS_PER_SIDE;
use crate::domain::execution::ExecutionVenue;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MidPriceJobDefinition {
    pub venue: ExecutionVenue,
    #[serde(default = "legacy_hyperliquid_testnet")]
    pub testnet: bool,
    pub symbol: String,
    /// Total base-asset quantity allocated across both initial grid ladders.
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
    #[serde(default)]
    pub leverage: Option<f64>,
    #[serde(default)]
    pub stop_loss_pct: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<OutcomeExecutionDefinition>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GridJobDefinition {
    pub venue: ExecutionVenue,
    #[serde(default = "legacy_hyperliquid_testnet")]
    pub testnet: bool,
    pub symbol: String,
    /// Hard one-sided inventory limit in normalized base-asset units.
    pub max_inventory_size: f64,
    pub requested_margin: Option<f64>,
    pub max_inventory_margin: f64,
    pub max_inventory_exposure: f64,
    pub duration_seconds: u64,
    pub levels_per_side: u16,
    pub step_bps: f64,
    #[serde(default)]
    pub leverage: Option<f64>,
    pub stop_loss_pct: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<OutcomeExecutionDefinition>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OutcomeExecutionDefinition {
    pub outcome_id: u32,
    /// Instrument selected by the user and used as the normalized reference book.
    pub primary_symbol: String,
    /// The other binary side of the outcome market.
    pub complement_symbol: String,
    pub primary_name: String,
    pub complement_name: String,
    pub primary_market_fingerprint: String,
    pub complement_market_fingerprint: String,
    pub quote_token: String,
    /// Quote collateral split into one share of each binary side per unit.
    pub pair_size: f64,
}

impl OutcomeExecutionDefinition {
    pub fn validate(&self) -> Result<()> {
        if self.primary_symbol.trim().is_empty()
            || self.complement_symbol.trim().is_empty()
            || self.primary_symbol == self.complement_symbol
        {
            bail!("outcome execution requires distinct primary and complementary instruments");
        }
        if self.primary_name.trim().is_empty() || self.complement_name.trim().is_empty() {
            bail!("outcome execution requires names for both binary sides");
        }
        if self.primary_market_fingerprint.trim().is_empty()
            || self.complement_market_fingerprint.trim().is_empty()
        {
            bail!("outcome execution requires market fingerprints");
        }
        if self.quote_token.trim().is_empty() {
            bail!("outcome execution quote token is required");
        }
        if !self.pair_size.is_finite() || self.pair_size <= 0.0 {
            bail!("outcome execution pair size must be greater than zero");
        }
        Ok(())
    }
}

impl GridJobDefinition {
    pub fn validate(&self) -> Result<()> {
        if self.symbol.trim().is_empty() {
            bail!("grid bot symbol is required");
        }
        if !self.max_inventory_size.is_finite() || self.max_inventory_size <= 0.0 {
            bail!("grid bot maximum inventory size must be greater than zero");
        }
        if self
            .requested_margin
            .is_some_and(|margin| !margin.is_finite() || margin <= 0.0)
        {
            bail!("grid bot requested margin must be greater than zero");
        }
        if !self.max_inventory_margin.is_finite() || self.max_inventory_margin <= 0.0 {
            bail!("grid bot maximum inventory margin must be greater than zero");
        }
        if !self.max_inventory_exposure.is_finite() || self.max_inventory_exposure <= 0.0 {
            bail!("grid bot maximum inventory exposure must be greater than zero");
        }
        if self.duration_seconds == 0 {
            bail!("grid bot duration must be at least one second");
        }
        if !(1..=MAX_GRID_LEVELS_PER_SIDE).contains(&self.levels_per_side) {
            bail!("grid bot levels per side must be between 1 and {MAX_GRID_LEVELS_PER_SIDE}");
        }
        if !self.step_bps.is_finite() || self.step_bps <= 0.0 {
            bail!("grid bot step must be greater than zero");
        }
        validate_outcome_mode(self.venue, self.leverage, self.outcome.as_ref())?;
        if self
            .stop_loss_pct
            .is_some_and(|percent| !percent.is_finite() || !(0.0..=100.0).contains(&percent))
        {
            bail!("grid bot stop loss must be between 0 and 100 percent");
        }
        let expected_margin = if self.outcome.is_some() {
            self.max_inventory_exposure
        } else {
            self.max_inventory_exposure / self.leverage.context("grid bot leverage is required")?
        };
        if (self.max_inventory_margin - expected_margin).abs()
            > 1e-8_f64.max(expected_margin.abs() * 1e-10)
        {
            bail!("grid bot margin, exposure, and leverage do not agree");
        }
        Ok(())
    }
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
        validate_outcome_mode(self.venue, self.leverage, self.outcome.as_ref())?;
        if self
            .stop_loss_pct
            .is_some_and(|percent| !percent.is_finite() || !(0.0..=100.0).contains(&percent))
        {
            bail!("mid-price bot stop loss must be between 0 and 100 percent");
        }
        let expected_margin = if self.outcome.is_some() {
            self.max_inventory_exposure
        } else {
            self.max_inventory_exposure
                / self
                    .leverage
                    .context("mid-price bot leverage is required")?
        };
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
    Grid(GridJobDefinition),
    MidPrice(MidPriceJobDefinition),
    VolumeMid(MidPriceJobDefinition),
}

impl BotJobDefinition {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Grid(_) => "grid",
            Self::MidPrice(_) => "mid-price",
            Self::VolumeMid(_) => "volume-mid",
        }
    }

    pub fn symbol(&self) -> &str {
        match self {
            Self::Grid(definition) => &definition.symbol,
            Self::MidPrice(definition) | Self::VolumeMid(definition) => &definition.symbol,
        }
    }

    pub fn venue(&self) -> ExecutionVenue {
        match self {
            Self::Grid(definition) => definition.venue,
            Self::MidPrice(definition) | Self::VolumeMid(definition) => definition.venue,
        }
    }

    pub fn testnet(&self) -> bool {
        match self {
            Self::Grid(definition) => definition.testnet,
            Self::MidPrice(definition) | Self::VolumeMid(definition) => definition.testnet,
        }
    }

    pub fn leverage(&self) -> Option<f64> {
        match self {
            Self::Grid(definition) => definition.leverage,
            Self::MidPrice(definition) | Self::VolumeMid(definition) => definition.leverage,
        }
    }

    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Grid(definition) => definition.validate(),
            Self::MidPrice(definition) | Self::VolumeMid(definition) => definition.validate(),
        }
    }

    pub fn accepts_symbol(&self, symbol: &str) -> bool {
        self.outcome().map_or_else(
            || symbol == self.symbol(),
            |outcome| symbol == outcome.primary_symbol || symbol == outcome.complement_symbol,
        )
    }

    pub fn maximum_order_size(&self) -> f64 {
        if let Some(outcome) = self.outcome() {
            return outcome.pair_size;
        }
        match self {
            Self::Grid(definition) => definition.max_inventory_size,
            Self::MidPrice(definition) | Self::VolumeMid(definition) => {
                definition.max_inventory_size
            }
        }
    }

    pub fn outcome(&self) -> Option<&OutcomeExecutionDefinition> {
        match self {
            Self::Grid(definition) => definition.outcome.as_ref(),
            Self::MidPrice(definition) | Self::VolumeMid(definition) => definition.outcome.as_ref(),
        }
    }
}

fn validate_outcome_mode(
    venue: ExecutionVenue,
    leverage: Option<f64>,
    outcome: Option<&OutcomeExecutionDefinition>,
) -> Result<()> {
    match (venue, outcome) {
        (ExecutionVenue::HyperliquidOutcomes, Some(outcome)) => {
            outcome.validate()?;
            if leverage.is_some() {
                bail!("outcome execution does not use leverage");
            }
        }
        (ExecutionVenue::HyperliquidOutcomes, None) => {
            bail!("outcome execution metadata is required");
        }
        (_, Some(_)) => bail!("outcome execution metadata requires hyperliquid-outcomes"),
        (_, None) => {
            if leverage.is_none_or(|value| !value.is_finite() || value < 1.0) {
                bail!("bot leverage must be at least 1");
            }
        }
    }
    Ok(())
}

const fn legacy_hyperliquid_testnet() -> bool {
    true
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<OutcomeMakerPerformance>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OutcomeMakerPerformance {
    pub split_size: f64,
    pub merged_size: f64,
    pub primary_sold_size: f64,
    pub complement_sold_size: f64,
    pub primary_inventory: f64,
    pub complement_inventory: f64,
    pub completed_cycles: u64,
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
                "venue": "bulkf",
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

    #[test]
    fn mid_price_jobs_can_persist_outcome_execution_without_becoming_a_new_bot() {
        let definition: BotJobDefinition = serde_json::from_value(serde_json::json!({
            "name": "mid_price",
            "config": {
                "venue": "hyperliquid-outcomes",
                "testnet": false,
                "symbol": "1009:0",
                "maxInventorySize": 50.0,
                "requestedMargin": 25.0,
                "maxInventoryMargin": 25.0,
                "maxInventoryExposure": 25.0,
                "durationSeconds": 300,
                "spreadBps": 20.0,
                "refreshSeconds": 1.0,
                "refreshToleranceBps": 1.0,
                "directionalBiasPercent": 0.0,
                "stopLossPct": 5.0,
                "outcome": {
                    "outcomeId": 1009,
                    "primarySymbol": "1009:0",
                    "complementSymbol": "1009:1",
                    "primaryName": "Change",
                    "complementName": "No Change",
                    "primaryMarketFingerprint": "primary-fingerprint",
                    "complementMarketFingerprint": "complement-fingerprint",
                    "quoteToken": "USDC",
                    "pairSize": 25.0
                }
            }
        }))
        .expect("outcome definition should decode");

        definition.validate().expect("outcome job should validate");
        assert!(definition.accepts_symbol("1009:0"));
        assert!(definition.accepts_symbol("1009:1"));
        assert!(!definition.accepts_symbol("1009:2"));
        assert_eq!(definition.name(), "mid-price");
        assert_eq!(definition.maximum_order_size(), 25.0);
        assert_eq!(definition.leverage(), None);
    }
}
