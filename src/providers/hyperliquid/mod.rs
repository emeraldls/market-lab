pub mod client;
pub mod exchange;
pub mod execution;
pub mod market_data;
pub mod markets;
pub mod outcomes;
pub mod signing;
pub mod ws;

use serde::{Deserialize, Serialize};

pub const EXCHANGE: &str = "hyperliquidf";
pub const XYZ_EXCHANGE: &str = "hyperliquidf-xyz";
pub const XYZ_DEX: &str = "xyz";
pub const IO_EXCHANGE: &str = "hyperliquidf-io";
pub const IO_DEX: &str = "io";
pub const SPOT_EXCHANGE: &str = "hyperliquid";
pub const OUTCOMES_EXCHANGE: &str = "hyperliquid-outcomes";
pub const MAINNET_HTTP_URL: &str = "https://api.hyperliquid.xyz";
pub const MAINNET_WS_URL: &str = "wss://api.hyperliquid.xyz/ws";
pub const TESTNET_HTTP_URL: &str = "https://api.hyperliquid-testnet.xyz";
pub const TESTNET_WS_URL: &str = "wss://api.hyperliquid-testnet.xyz/ws";
pub(crate) const MARKET_ORDER_SLIPPAGE: f64 = 0.005;

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HyperliquidNetwork {
    #[default]
    Mainnet,
    Testnet,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum HyperliquidProduct {
    Spot,
    Outcome,
    Perpetual,
    XyzPerpetual,
    IoPerpetual,
}

impl HyperliquidProduct {
    pub fn from_exchange(exchange: &str) -> anyhow::Result<Self> {
        match exchange.trim().to_ascii_lowercase().as_str() {
            SPOT_EXCHANGE => Ok(Self::Spot),
            OUTCOMES_EXCHANGE => Ok(Self::Outcome),
            EXCHANGE => Ok(Self::Perpetual),
            XYZ_EXCHANGE => Ok(Self::XyzPerpetual),
            IO_EXCHANGE => Ok(Self::IoPerpetual),
            _ => anyhow::bail!(
                "Hyperliquid exchange must be `hyperliquid` (spot), `hyperliquid-outcomes` (HIP-4 outcomes), `hyperliquidf` (native perpetuals), `hyperliquidf-xyz` (XYZ perpetuals), or `hyperliquidf-io` (EntropyIO perpetuals)"
            ),
        }
    }

    pub const fn exchange(self) -> &'static str {
        match self {
            Self::Spot => SPOT_EXCHANGE,
            Self::Outcome => OUTCOMES_EXCHANGE,
            Self::Perpetual => EXCHANGE,
            Self::XyzPerpetual => XYZ_EXCHANGE,
            Self::IoPerpetual => IO_EXCHANGE,
        }
    }

    pub const fn dex(self) -> Option<&'static str> {
        match self {
            Self::XyzPerpetual => Some(XYZ_DEX),
            Self::IoPerpetual => Some(IO_DEX),
            Self::Spot | Self::Outcome | Self::Perpetual => None,
        }
    }

    pub const fn is_perpetual(self) -> bool {
        matches!(
            self,
            Self::Perpetual | Self::XyzPerpetual | Self::IoPerpetual
        )
    }

    pub const fn max_price_decimals(self) -> u8 {
        match self {
            Self::Spot | Self::Outcome => 8,
            Self::Perpetual | Self::XyzPerpetual | Self::IoPerpetual => 6,
        }
    }
}

impl HyperliquidNetwork {
    pub const fn from_testnet(testnet: bool) -> Self {
        if testnet {
            Self::Testnet
        } else {
            Self::Mainnet
        }
    }

    pub const fn http_url(self) -> &'static str {
        match self {
            Self::Mainnet => MAINNET_HTTP_URL,
            Self::Testnet => TESTNET_HTTP_URL,
        }
    }

    pub const fn ws_url(self) -> &'static str {
        match self {
            Self::Mainnet => MAINNET_WS_URL,
            Self::Testnet => TESTNET_WS_URL,
        }
    }

    pub const fn signature_source(self) -> &'static str {
        match self {
            Self::Mainnet => "a",
            Self::Testnet => "b",
        }
    }

    pub const fn approval_chain(self) -> &'static str {
        match self {
            Self::Mainnet => "Mainnet",
            Self::Testnet => "Testnet",
        }
    }

    pub const fn label(self) -> &'static str {
        match self {
            Self::Mainnet => "mainnet",
            Self::Testnet => "testnet",
        }
    }
}
