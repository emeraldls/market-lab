use anyhow::Result;

use crate::credentials;
use crate::domain::execution::{
    AccountSnapshot, CancelPlan, ExecutionOutcome, ExecutionReceipt, ExecutionVenue, Fill,
    OpenOrder, TradePlan, VenueCapabilities,
};
use crate::providers::bulk::execution::BulkExecutionAdapter;
use crate::providers::hyperliquid::execution::HyperliquidExecutionAdapter;
use crate::providers::hyperliquid::{HyperliquidNetwork, HyperliquidProduct};

pub enum ExecutionAdapter {
    Bulk(BulkExecutionAdapter),
    Hyperliquid(HyperliquidExecutionAdapter),
}

impl ExecutionAdapter {
    pub async fn new(venue: ExecutionVenue, testnet: bool) -> Result<Self> {
        match venue {
            ExecutionVenue::Bulk => Ok(Self::Bulk(BulkExecutionAdapter::new()?)),
            ExecutionVenue::Hyperliquid => Ok(Self::Hyperliquid(
                HyperliquidExecutionAdapter::new_for(
                    HyperliquidProduct::Perpetual,
                    HyperliquidNetwork::from_testnet(testnet),
                )
                .await?,
            )),
            ExecutionVenue::HyperliquidXyz => Ok(Self::Hyperliquid(
                HyperliquidExecutionAdapter::new_for(
                    HyperliquidProduct::XyzPerpetual,
                    HyperliquidNetwork::from_testnet(testnet),
                )
                .await?,
            )),
            ExecutionVenue::HyperliquidSpot => Ok(Self::Hyperliquid(
                HyperliquidExecutionAdapter::new_for(
                    HyperliquidProduct::Spot,
                    HyperliquidNetwork::from_testnet(testnet),
                )
                .await?,
            )),
            ExecutionVenue::HyperliquidOutcomes => Ok(Self::Hyperliquid(
                HyperliquidExecutionAdapter::new_for(
                    HyperliquidProduct::Outcome,
                    HyperliquidNetwork::from_testnet(testnet),
                )
                .await?,
            )),
        }
    }

    pub fn capabilities(venue: ExecutionVenue) -> VenueCapabilities {
        match venue {
            ExecutionVenue::Bulk => BulkExecutionAdapter::capabilities(),
            ExecutionVenue::Hyperliquid => HyperliquidExecutionAdapter::capabilities(),
            ExecutionVenue::HyperliquidXyz => {
                HyperliquidExecutionAdapter::capabilities_for(HyperliquidProduct::XyzPerpetual)
            }
            ExecutionVenue::HyperliquidSpot => {
                HyperliquidExecutionAdapter::capabilities_for(HyperliquidProduct::Spot)
            }
            ExecutionVenue::HyperliquidOutcomes => {
                HyperliquidExecutionAdapter::capabilities_for(HyperliquidProduct::Outcome)
            }
        }
    }

    pub fn configured_account(venue: ExecutionVenue) -> Result<String> {
        match venue {
            ExecutionVenue::Bulk => credentials::bulk_account(),
            ExecutionVenue::Hyperliquid => credentials::hyperliquid_account(),
            ExecutionVenue::HyperliquidXyz => credentials::hyperliquid_account(),
            ExecutionVenue::HyperliquidSpot => credentials::hyperliquid_account(),
            ExecutionVenue::HyperliquidOutcomes => credentials::hyperliquid_account(),
        }
    }

    pub async fn account_snapshot(&self, account: &str) -> Result<AccountSnapshot> {
        match self {
            Self::Bulk(adapter) => adapter.account_snapshot(account).await,
            Self::Hyperliquid(adapter) => adapter.account_snapshot(account).await,
        }
    }

    pub async fn open_orders(&self, account: &str) -> Result<Vec<OpenOrder>> {
        match self {
            Self::Bulk(adapter) => adapter.open_orders(account).await,
            Self::Hyperliquid(adapter) => adapter.open_orders(account).await,
        }
    }

    pub async fn fills(&self, account: &str) -> Result<Vec<Fill>> {
        match self {
            Self::Bulk(adapter) => adapter.fills(account).await,
            Self::Hyperliquid(adapter) => adapter.fills(account).await,
        }
    }

    pub async fn submit_trade(&self, plan: &TradePlan) -> Result<ExecutionReceipt> {
        match self {
            Self::Bulk(adapter) => {
                adapter
                    .submit_trade(credentials::active_bulk_credential()?, plan)
                    .await
            }
            Self::Hyperliquid(adapter) => adapter.submit_trade(plan).await,
        }
    }

    pub async fn cancel_order(&self, plan: &CancelPlan) -> Result<ExecutionReceipt> {
        match self {
            Self::Bulk(adapter) => {
                adapter
                    .cancel_order(
                        credentials::active_bulk_credential()?,
                        &plan.venue_symbol,
                        &plan.order_id,
                    )
                    .await
            }
            Self::Hyperliquid(adapter) => {
                adapter
                    .cancel_order(&plan.venue_symbol, &plan.order_id)
                    .await
            }
        }
    }

    pub async fn cancel_order_fast(&self, plan: &CancelPlan) -> Result<ExecutionReceipt> {
        match self {
            Self::Bulk(adapter) => {
                adapter
                    .cancel_order(
                        credentials::active_bulk_credential()?,
                        &plan.venue_symbol,
                        &plan.order_id,
                    )
                    .await
            }
            Self::Hyperliquid(adapter) => {
                adapter
                    .cancel_order_fast(&plan.venue_symbol, &plan.order_id)
                    .await
            }
        }
    }

    pub async fn submit_trades(&self, plans: &[TradePlan]) -> Result<Vec<ExecutionOutcome>> {
        match self {
            Self::Bulk(adapter) => {
                adapter
                    .submit_trades(credentials::active_bulk_credential()?, plans)
                    .await
            }
            Self::Hyperliquid(adapter) => adapter.submit_trades(plans).await,
        }
    }

    pub async fn cancel_orders(&self, plans: &[CancelPlan]) -> Result<Vec<ExecutionOutcome>> {
        match self {
            Self::Bulk(adapter) => {
                adapter
                    .cancel_orders(credentials::active_bulk_credential()?, plans)
                    .await
            }
            Self::Hyperliquid(adapter) => adapter.cancel_orders(plans).await,
        }
    }

    pub async fn cancel_orders_fast(&self, plans: &[CancelPlan]) -> Result<Vec<ExecutionOutcome>> {
        match self {
            Self::Bulk(adapter) => {
                adapter
                    .cancel_orders(credentials::active_bulk_credential()?, plans)
                    .await
            }
            Self::Hyperliquid(adapter) => adapter.cancel_orders_fast(plans).await,
        }
    }
}
