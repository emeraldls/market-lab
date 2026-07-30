use std::sync::Arc;

use anyhow::Result;

pub use crate::markets::Market as HyperliquidMarket;
pub use crate::markets::NetworkMarket as HyperliquidNetworkMarket;

use super::{HyperliquidNetwork, HyperliquidProduct};

pub fn market(symbol: &str) -> Result<Arc<HyperliquidMarket>> {
    market_for(HyperliquidProduct::Perpetual, symbol)
}

pub fn market_for(product: HyperliquidProduct, symbol: &str) -> Result<Arc<HyperliquidMarket>> {
    crate::markets::exchange_market(product.exchange(), symbol)
}

pub fn network_market(
    product: HyperliquidProduct,
    network: HyperliquidNetwork,
    symbol: &str,
) -> Result<(Arc<HyperliquidMarket>, HyperliquidNetworkMarket)> {
    let market = market_for(product, symbol)?;
    let variant = match (product, network) {
        (HyperliquidProduct::Perpetual, _) | (_, HyperliquidNetwork::Mainnet) => {
            market.network_variant("mainnet")?
        }
        (_, HyperliquidNetwork::Testnet) => market.network_variant("testnet")?,
    };
    Ok((market, variant))
}

pub fn market_for_wire(
    product: HyperliquidProduct,
    network: HyperliquidNetwork,
    wire_symbol: &str,
) -> Result<Arc<HyperliquidMarket>> {
    crate::markets::exchange_markets(product.exchange())?
        .into_iter()
        .find_map(|market| {
            let variant = match (product, network) {
                (HyperliquidProduct::Perpetual, _) | (_, HyperliquidNetwork::Mainnet) => {
                    market.network_variant("mainnet").ok()?
                }
                (_, HyperliquidNetwork::Testnet) => market.network_variant("testnet").ok()?,
            };
            (variant.provider_symbol.eq_ignore_ascii_case(wire_symbol)
                || variant.venue_symbol.eq_ignore_ascii_case(wire_symbol))
            .then_some(market)
        })
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Hyperliquid {} {} market `{wire_symbol}` is not in the local snapshot",
                network.label(),
                product.exchange()
            )
        })
}
