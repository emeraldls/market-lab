use anyhow::{Result, bail};

use crate::domain::enums::ProviderKind;
use crate::domain::requests::{InspectRequest, ReplayRequest};
use crate::domain::types::{OrderBookSnapshot, ProviderHealth, TopOfBook};

pub mod binance;
pub mod bulk;
pub mod execution;
pub mod hyperlink;
pub mod hyperliquid;
pub mod market_data;
pub mod marketlab_cloud;
pub mod mmt;

use market_data::MarketDataAdapter;
use marketlab_cloud::MarketLabProvider;
use mmt::MmtProvider;

#[allow(async_fn_in_trait)]
pub trait ProviderService {
    async fn inspect(&self, req: &InspectRequest) -> Result<OrderBookSnapshot>;
    async fn replay(&self, req: &ReplayRequest) -> Result<Vec<TopOfBook>>;
    async fn health(&self) -> Result<ProviderHealth>;
}

pub enum ProviderClient {
    MarketLab,
    Mmt,
    Direct(String),
}

impl ProviderClient {
    pub fn from_kind(kind: ProviderKind, exchange: &str) -> Result<Self> {
        Ok(match kind {
            ProviderKind::MarketLab => Self::MarketLab,
            ProviderKind::Mmt => Self::Mmt,
            ProviderKind::Direct => {
                MarketDataAdapter::for_exchange(exchange, false)?;
                Self::Direct(exchange.to_string())
            }
        })
    }
}

impl ProviderService for ProviderClient {
    async fn inspect(&self, req: &InspectRequest) -> Result<OrderBookSnapshot> {
        match self {
            Self::MarketLab => MarketLabProvider::inspect(req).await,
            Self::Mmt => MmtProvider::inspect(req).await,
            Self::Direct(exchange) => {
                bail!("{exchange} does not provide historical orderbook inspection")
            }
        }
    }

    async fn replay(&self, req: &ReplayRequest) -> Result<Vec<TopOfBook>> {
        match self {
            Self::MarketLab => MarketLabProvider::replay(req).await,
            Self::Mmt => MmtProvider::replay(req).await,
            Self::Direct(exchange) => {
                bail!("{exchange} historical orderbook replay is not implemented")
            }
        }
    }

    async fn health(&self) -> Result<ProviderHealth> {
        match self {
            Self::MarketLab => MarketLabProvider::health().await,
            Self::Mmt => MmtProvider::health().await,
            Self::Direct(exchange) => {
                MarketDataAdapter::for_exchange(exchange, false)?
                    .health()
                    .await
            }
        }
    }
}
