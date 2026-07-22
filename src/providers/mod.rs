use anyhow::Result;

use crate::domain::enums::ProviderKind;
use crate::domain::requests::{InspectRequest, ReplayRequest};
use crate::domain::types::{OrderBookSnapshot, ProviderHealth, TopOfBook};

pub mod bulk;
pub mod execution;
pub mod hyperliquid;
pub mod marketlab_cloud;
pub mod mmt;

use bulk::market_data::BulkProvider;
use hyperliquid::market_data::HyperliquidProvider;
use marketlab_cloud::MarketLabProvider;
use mmt::MmtProvider;

#[allow(async_fn_in_trait)]
pub trait MarketDataProvider {
    async fn inspect(&self, req: &InspectRequest) -> Result<OrderBookSnapshot>;
    async fn replay(&self, req: &ReplayRequest) -> Result<Vec<TopOfBook>>;
    async fn health(&self) -> Result<ProviderHealth>;
}

pub enum ProviderClient {
    MarketLab,
    Mmt,
    Bulk,
    Hyperliquid,
}

impl ProviderClient {
    pub fn from_kind(kind: ProviderKind) -> Self {
        match kind {
            ProviderKind::MarketLab => Self::MarketLab,
            ProviderKind::Mmt => Self::Mmt,
            ProviderKind::Bulk => Self::Bulk,
            ProviderKind::Hyperliquid => Self::Hyperliquid,
        }
    }
}

impl MarketDataProvider for ProviderClient {
    async fn inspect(&self, req: &InspectRequest) -> Result<OrderBookSnapshot> {
        match self {
            Self::MarketLab => MarketLabProvider::inspect(req).await,
            Self::Mmt => MmtProvider::inspect(req).await,
            Self::Bulk => BulkProvider::inspect_historical().await,
            Self::Hyperliquid => HyperliquidProvider::inspect_historical().await,
        }
    }

    async fn replay(&self, req: &ReplayRequest) -> Result<Vec<TopOfBook>> {
        match self {
            Self::MarketLab => MarketLabProvider::replay(req).await,
            Self::Mmt => MmtProvider::replay(req).await,
            Self::Bulk => BulkProvider::replay_historical().await,
            Self::Hyperliquid => HyperliquidProvider::replay_historical().await,
        }
    }

    async fn health(&self) -> Result<ProviderHealth> {
        match self {
            Self::MarketLab => MarketLabProvider::health().await,
            Self::Mmt => MmtProvider::health().await,
            Self::Bulk => BulkProvider::health().await,
            Self::Hyperliquid => HyperliquidProvider::health().await,
        }
    }
}
