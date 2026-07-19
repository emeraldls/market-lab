use anyhow::{Result, bail};

use crate::domain::enums::ProviderKind;
use crate::domain::requests::{InspectRequest, ReplayRequest};
use crate::domain::types::{OrderBookSnapshot, ProviderHealth, TopOfBook};

pub mod bulk;
pub mod binance;
pub mod marketlab_cloud;
pub mod mmt;

use bulk::market_data::BulkProvider;
use binance::market_data::BinanceProvider;
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
    Binance,
    BinanceFutures,
}

impl ProviderClient {
    pub fn from_kind(kind: ProviderKind) -> Self {
        match kind {
            ProviderKind::MarketLab => Self::MarketLab,
            ProviderKind::Mmt => Self::Mmt,
            ProviderKind::Bulk => Self::Bulk,
            ProviderKind::Binance => Self::Binance,
            ProviderKind::BinanceFutures => Self::BinanceFutures,
        }
    }
}

impl MarketDataProvider for ProviderClient {
    async fn inspect(&self, req: &InspectRequest) -> Result<OrderBookSnapshot> {
        match self {
            Self::MarketLab => MarketLabProvider::inspect(req).await,
            Self::Mmt => MmtProvider::inspect(req).await,
            Self::Bulk => BulkProvider::inspect_historical().await,
            Self::Binance | Self::BinanceFutures => bail!("Binance inspect not supported"),
        }
    }

    async fn replay(&self, req: &ReplayRequest) -> Result<Vec<TopOfBook>> {
        match self {
            Self::MarketLab => MarketLabProvider::replay(req).await,
            Self::Mmt => MmtProvider::replay(req).await,
            Self::Bulk => BulkProvider::replay_historical().await,
            Self::Binance | Self::BinanceFutures => bail!("Binance replay not supported"),
        }
    }

    async fn health(&self) -> Result<ProviderHealth> {
        match self {
            Self::MarketLab => MarketLabProvider::health().await,
            Self::Mmt => MmtProvider::health().await,
            Self::Bulk => BulkProvider::health().await,
            Self::Binance => BinanceProvider::health().await,
            Self::BinanceFutures => BinanceProvider::health_futures().await,
        }
    }
}
