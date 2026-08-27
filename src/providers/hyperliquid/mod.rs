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
}

impl HyperliquidProduct {
    pub fn from_exchange(exchange: &str) -> anyhow::Result<Self> {
        let venue = crate::venues::VenueId::parse(exchange)?;
        Self::from_venue(venue)
    }

    pub fn from_venue(venue: crate::venues::VenueId) -> anyhow::Result<Self> {
        match venue.spec()?.market {
            crate::venues::VenueMarket::Spot => Ok(Self::Spot),
            crate::venues::VenueMarket::Outcome => Ok(Self::Outcome),
            crate::venues::VenueMarket::Perpetual
                if venue == crate::venues::VenueId::Hyperliquid =>
            {
                Ok(Self::Perpetual)
            }
            _ => anyhow::bail!("`{venue}` is not a Hyperliquid market-data venue"),
        }
    }

    pub fn exchange(&self) -> &str {
        match self {
            Self::Spot => SPOT_EXCHANGE,
            Self::Outcome => OUTCOMES_EXCHANGE,
            Self::Perpetual => EXCHANGE,
        }
    }

    pub const fn is_perpetual(self) -> bool {
        matches!(self, Self::Perpetual)
    }

    pub const fn max_price_decimals(self) -> u8 {
        match self {
            Self::Spot | Self::Outcome => 8,
            Self::Perpetual => 6,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PerpetualSymbol {
    pub canonical: String,
    pub coin: String,
    pub dex: Option<String>,
}

pub fn parse_perpetual_symbol(symbol: &str) -> anyhow::Result<PerpetualSymbol> {
    let symbol = symbol.trim();
    let (dex, coin) = match symbol.split_once(':') {
        Some((dex, coin)) => {
            if dex.is_empty()
                || coin.is_empty()
                || coin.contains(':')
                || !dex
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric() || character == '-')
            {
                anyhow::bail!(
                    "Hyperliquid HIP-3 symbol must use `dex:coin`, for example `xyz:TSLA`"
                );
            }
            (Some(dex.to_ascii_lowercase()), coin)
        }
        None => (None, symbol),
    };
    if coin.is_empty()
        || !coin
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
    {
        anyhow::bail!("invalid Hyperliquid perpetual symbol `{symbol}`");
    }
    let coin = coin.to_ascii_uppercase();
    let canonical = dex
        .as_ref()
        .map_or_else(|| coin.clone(), |dex| format!("{dex}:{coin}"));
    Ok(PerpetualSymbol {
        canonical,
        coin,
        dex,
    })
}

pub fn perpetual_dex(symbol: &str) -> anyhow::Result<Option<String>> {
    Ok(parse_perpetual_symbol(symbol)?.dex)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn perpetual_symbols_normalize_native_and_dynamic_hip3_markets() {
        let native = parse_perpetual_symbol("btc").expect("native perpetual symbol");
        assert_eq!(native.canonical, "BTC");
        assert_eq!(native.coin, "BTC");
        assert_eq!(native.dex, None);

        let hip3 = parse_perpetual_symbol("Example-Dex:coin_1").expect("HIP-3 symbol");
        assert_eq!(hip3.canonical, "example-dex:COIN_1");
        assert_eq!(hip3.coin, "COIN_1");
        assert_eq!(hip3.dex.as_deref(), Some("example-dex"));

        assert!(parse_perpetual_symbol("example-dex:").is_err());
        assert!(parse_perpetual_symbol("example.dex:COIN").is_err());
        assert!(parse_perpetual_symbol("example:COIN:EXTRA").is_err());
    }
}
