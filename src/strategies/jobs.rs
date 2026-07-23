use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

use crate::domain::execution::ExecutionVenue;
use crate::strategies::oiwap::OpenInterestSource;
use crate::strategies::vwap::VolumeSource;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategySide {
    Buy,
    Sell,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TwapJobDefinition {
    pub venue: ExecutionVenue,
    #[serde(default = "legacy_hyperliquid_testnet")]
    pub testnet: bool,
    pub symbol: String,
    pub side: StrategySide,
    pub total_size: f64,
    pub requested_margin: Option<f64>,
    #[serde(default)]
    pub target_margin: f64,
    pub target_exposure: f64,
    pub duration_seconds: u64,
    pub interval_seconds: u64,
    pub leverage: f64,
    pub reduce_only: bool,
}

impl TwapJobDefinition {
    pub fn validate(&self) -> Result<()> {
        if self.symbol.trim().is_empty() {
            bail!("TWAP job symbol is required");
        }
        if !self.total_size.is_finite() || self.total_size <= 0.0 {
            bail!("TWAP job total size must be greater than zero");
        }
        if self
            .requested_margin
            .is_some_and(|margin| !margin.is_finite() || margin <= 0.0)
        {
            bail!("TWAP job requested margin must be greater than zero");
        }
        if !self.target_margin.is_finite() || self.target_margin <= 0.0 {
            bail!("TWAP job target margin must be greater than zero");
        }
        if !self.target_exposure.is_finite() || self.target_exposure <= 0.0 {
            bail!("TWAP job target exposure must be greater than zero");
        }
        if self.duration_seconds == 0 {
            bail!("TWAP job duration must be at least one second");
        }
        if self.interval_seconds == 0 {
            bail!("TWAP job interval must be at least one second");
        }
        if !self.leverage.is_finite() || self.leverage < 1.0 {
            bail!("TWAP job leverage must be at least 1");
        }
        let expected_margin = self.target_exposure / self.leverage;
        if (self.target_margin - expected_margin).abs()
            > 1e-8_f64.max(expected_margin.abs() * 1e-10)
        {
            bail!("TWAP job margin, exposure, and leverage do not agree");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VwapJobDefinition {
    pub venue: ExecutionVenue,
    #[serde(default = "legacy_hyperliquid_testnet")]
    pub testnet: bool,
    pub symbol: String,
    pub side: StrategySide,
    pub total_size: f64,
    pub requested_margin: Option<f64>,
    pub target_margin: f64,
    pub target_exposure: f64,
    pub duration_seconds: u64,
    pub volume_sources: Vec<VolumeSource>,
    pub leverage: f64,
    pub reduce_only: bool,
}

impl VwapJobDefinition {
    pub fn validate(&self) -> Result<()> {
        if self.symbol.trim().is_empty() {
            bail!("VWAP job symbol is required");
        }
        if !self.total_size.is_finite() || self.total_size <= 0.0 {
            bail!("VWAP job total size must be greater than zero");
        }
        if self
            .requested_margin
            .is_some_and(|margin| !margin.is_finite() || margin <= 0.0)
        {
            bail!("VWAP job requested margin must be greater than zero");
        }
        if !self.target_margin.is_finite() || self.target_margin <= 0.0 {
            bail!("VWAP job target margin must be greater than zero");
        }
        if !self.target_exposure.is_finite() || self.target_exposure <= 0.0 {
            bail!("VWAP job target exposure must be greater than zero");
        }
        if self.duration_seconds < 60 {
            bail!("VWAP job duration must be at least 60 seconds");
        }
        if self.volume_sources.is_empty() {
            bail!("VWAP job requires at least one volume source");
        }
        let mut exchanges = std::collections::HashSet::new();
        for source in &self.volume_sources {
            if source.exchange.trim().is_empty() || !exchanges.insert(&source.exchange) {
                bail!("VWAP job volume sources contain an empty or duplicate exchange");
            }
        }
        if !self.leverage.is_finite() || self.leverage < 1.0 {
            bail!("VWAP job leverage must be at least 1");
        }
        let expected_margin = self.target_exposure / self.leverage;
        if (self.target_margin - expected_margin).abs()
            > 1e-8_f64.max(expected_margin.abs() * 1e-10)
        {
            bail!("VWAP job margin, exposure, and leverage do not agree");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OiwapJobDefinition {
    pub venue: ExecutionVenue,
    #[serde(default = "legacy_hyperliquid_testnet")]
    pub testnet: bool,
    pub symbol: String,
    pub side: StrategySide,
    pub total_size: f64,
    pub requested_margin: Option<f64>,
    pub target_margin: f64,
    pub target_exposure: f64,
    pub duration_seconds: u64,
    pub oi_sources: Vec<OpenInterestSource>,
    pub leverage: f64,
    pub reduce_only: bool,
}

impl OiwapJobDefinition {
    pub fn validate(&self) -> Result<()> {
        if self.symbol.trim().is_empty() {
            bail!("OIWAP job symbol is required");
        }
        if !self.total_size.is_finite() || self.total_size <= 0.0 {
            bail!("OIWAP job total size must be greater than zero");
        }
        if self
            .requested_margin
            .is_some_and(|margin| !margin.is_finite() || margin <= 0.0)
        {
            bail!("OIWAP job requested margin must be greater than zero");
        }
        if !self.target_margin.is_finite() || self.target_margin <= 0.0 {
            bail!("OIWAP job target margin must be greater than zero");
        }
        if !self.target_exposure.is_finite() || self.target_exposure <= 0.0 {
            bail!("OIWAP job target exposure must be greater than zero");
        }
        if self.duration_seconds < 60 {
            bail!("OIWAP job duration must be at least 60 seconds");
        }
        if self.oi_sources.is_empty() {
            bail!("OIWAP job requires at least one OI source");
        }
        let mut exchanges = std::collections::HashSet::new();
        for source in &self.oi_sources {
            if source.exchange.trim().is_empty() || !exchanges.insert(&source.exchange) {
                bail!("OIWAP job OI sources contain an empty or duplicate exchange");
            }
        }
        if !self.leverage.is_finite() || self.leverage < 1.0 {
            bail!("OIWAP job leverage must be at least 1");
        }
        let expected_margin = self.target_exposure / self.leverage;
        if (self.target_margin - expected_margin).abs()
            > 1e-8_f64.max(expected_margin.abs() * 1e-10)
        {
            bail!("OIWAP job margin, exposure, and leverage do not agree");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "name", content = "config", rename_all = "snake_case")]
pub enum StrategyJobDefinition {
    Twap(TwapJobDefinition),
    Vwap(VwapJobDefinition),
    Oiwap(OiwapJobDefinition),
}

impl StrategyJobDefinition {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Twap(_) => "twap",
            Self::Vwap(_) => "vwap",
            Self::Oiwap(_) => "oiwap",
        }
    }

    pub fn symbol(&self) -> &str {
        match self {
            Self::Twap(definition) => &definition.symbol,
            Self::Vwap(definition) => &definition.symbol,
            Self::Oiwap(definition) => &definition.symbol,
        }
    }

    pub fn venue(&self) -> ExecutionVenue {
        match self {
            Self::Twap(definition) => definition.venue,
            Self::Vwap(definition) => definition.venue,
            Self::Oiwap(definition) => definition.venue,
        }
    }

    pub fn testnet(&self) -> bool {
        match self {
            Self::Twap(definition) => definition.testnet,
            Self::Vwap(definition) => definition.testnet,
            Self::Oiwap(definition) => definition.testnet,
        }
    }

    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Twap(definition) => definition.validate(),
            Self::Vwap(definition) => definition.validate(),
            Self::Oiwap(definition) => definition.validate(),
        }
    }
}

const fn legacy_hyperliquid_testnet() -> bool {
    true
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StrategyJobSubmission {
    pub definition: StrategyJobDefinition,
}

impl StrategyJobSubmission {
    pub fn validate(&self) -> Result<()> {
        self.definition.validate()
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyJobStatus {
    Starting,
    Running,
    Stopping,
    Stopped,
    Completed,
    Failed,
}

impl StrategyJobStatus {
    pub fn is_active(self) -> bool {
        matches!(self, Self::Starting | Self::Running | Self::Stopping)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StrategyJob {
    pub id: String,
    pub definition: StrategyJobDefinition,
    pub status: StrategyJobStatus,
    pub pid: Option<u32>,
    pub created_at_ms: u64,
    pub started_at_ms: Option<u64>,
    pub stopped_at_ms: Option<u64>,
    pub last_heartbeat_ms: Option<u64>,
    pub last_error: Option<String>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::TwapJobDefinition;

    #[test]
    fn twap_job_without_target_margin_can_be_read_from_a_running_daemon() {
        let definition: TwapJobDefinition = serde_json::from_value(json!({
            "venue": "bulk",
            "symbol": "BTC/USDT",
            "side": "buy",
            "totalSize": 0.01,
            "requestedMargin": 100.0,
            "targetExposure": 1_000.0,
            "durationSeconds": 300,
            "intervalSeconds": 60,
            "leverage": 10.0,
            "reduceOnly": false
        }))
        .expect("deserialize a TWAP job created before targetMargin was persisted");

        assert_eq!(definition.target_margin, 0.0);
    }
}
