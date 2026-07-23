pub mod client;
pub mod exchange;
pub mod execution;
pub mod market_data;
pub mod markets;
pub mod signing;
pub mod ws;

use serde::{Deserialize, Serialize};

pub const EXCHANGE: &str = "hyperliquid";
pub const MAINNET_HTTP_URL: &str = "https://api.hyperliquid.xyz";
pub const MAINNET_WS_URL: &str = "wss://api.hyperliquid.xyz/ws";
pub const TESTNET_HTTP_URL: &str = "https://api.hyperliquid-testnet.xyz";
pub const TESTNET_WS_URL: &str = "wss://api.hyperliquid-testnet.xyz/ws";

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HyperliquidNetwork {
    #[default]
    Mainnet,
    Testnet,
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
