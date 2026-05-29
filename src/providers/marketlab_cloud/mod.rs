use anyhow::Result;

/*
    just gonna provide cloud for hyperliquid specific data, if need other data source, should use mmt
*/

use crate::domain::requests::{InspectRequest, ReplayRequest};
use crate::domain::types::{OrderBookLevel, OrderBookSnapshot, ProviderHealth, TopOfBook};

pub struct MarketLabProvider;

impl MarketLabProvider {
    pub async fn inspect(req: &InspectRequest) -> Result<OrderBookSnapshot> {
        let bids = (0..req.depth)
            .map(|i| OrderBookLevel {
                price: 100_000.0 - i as f64,
                quantity: 1.0 + i as f64,
            })
            .collect();
        let asks = (0..req.depth)
            .map(|i| OrderBookLevel {
                price: 100_001.0 + i as f64,
                quantity: 1.0 + i as f64,
            })
            .collect();

        Ok(OrderBookSnapshot {
            exchange: req.exchange.clone(),
            symbol: req.symbol.clone(),
            timestamp_ms: req.at,
            bids,
            asks,
        })
    }

    pub async fn replay(req: &ReplayRequest) -> Result<Vec<TopOfBook>> {
        let _ = (&req.to, &req.speed, &req.exchange, &req.symbol);
        Ok(vec![TopOfBook {
            timestamp_ms: req.from,
            best_bid: Some(OrderBookLevel {
                price: 100_000.0,
                quantity: 10.0,
            }),
            best_ask: Some(OrderBookLevel {
                price: 100_001.0,
                quantity: 9.5,
            }),
        }])
    }

    pub async fn health() -> Result<ProviderHealth> {
        Ok(ProviderHealth {
            provider: "marketlab".to_string(),
            status: "ok".to_string(),
            details: serde_json::json!({"mode":"stub"}),
        })
    }
}
