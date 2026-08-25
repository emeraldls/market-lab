use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProviderKind {
    MarketLab,
    Mmt,
    #[serde(
        alias = "Bulk",
        alias = "Hyperliquid",
        alias = "Binance",
        alias = "BinanceFutures"
    )]
    Direct,
}

impl ProviderKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MarketLab => "marketlab",
            Self::Mmt => "mmt",
            Self::Direct => "direct",
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum BookMode {
    Binned,
    Raw,
}
