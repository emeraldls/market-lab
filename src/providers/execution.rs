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
        Self::new_for_account(venue, testnet, "main").await
    }

    pub async fn new_for_account(
        venue: ExecutionVenue,
        testnet: bool,
        account_name: &str,
    ) -> Result<Self> {
        match venue {
            ExecutionVenue::Bulk => Ok(Self::Bulk(BulkExecutionAdapter::new()?)),
            ExecutionVenue::Hyperliquid => Ok(Self::Hyperliquid(
                HyperliquidExecutionAdapter::new_for_account(
                    HyperliquidProduct::Perpetual,
                    HyperliquidNetwork::from_testnet(testnet),
                    account_name,
                )
                .await?,
            )),
            ExecutionVenue::Hyperlink => {
                if testnet {
                    anyhow::bail!("HyperLink does not support testnet");
                }
                if !account_name.trim().is_empty() && !account_name.eq_ignore_ascii_case("main") {
                    anyhow::bail!("HyperLink subaccounts are not supported");
                }
                Ok(Self::Hyperliquid(
                    HyperliquidExecutionAdapter::new_hyperlink().await?,
                ))
            }
            ExecutionVenue::HyperliquidXyz => Ok(Self::Hyperliquid(
                HyperliquidExecutionAdapter::new_for_account(
                    HyperliquidProduct::XyzPerpetual,
                    HyperliquidNetwork::from_testnet(testnet),
                    account_name,
                )
                .await?,
            )),
            ExecutionVenue::HyperliquidIo => Ok(Self::Hyperliquid(
                HyperliquidExecutionAdapter::new_for_account(
                    HyperliquidProduct::IoPerpetual,
                    HyperliquidNetwork::from_testnet(testnet),
                    account_name,
                )
                .await?,
            )),
            ExecutionVenue::HyperliquidSpot => Ok(Self::Hyperliquid(
                HyperliquidExecutionAdapter::new_for_account(
                    HyperliquidProduct::Spot,
                    HyperliquidNetwork::from_testnet(testnet),
                    account_name,
                )
                .await?,
            )),
            ExecutionVenue::HyperliquidOutcomes => Ok(Self::Hyperliquid(
                HyperliquidExecutionAdapter::new_for_account(
                    HyperliquidProduct::Outcome,
                    HyperliquidNetwork::from_testnet(testnet),
                    account_name,
                )
                .await?,
            )),
        }
    }

    pub fn capabilities(venue: ExecutionVenue) -> VenueCapabilities {
        match venue {
            ExecutionVenue::Bulk => BulkExecutionAdapter::capabilities(),
            ExecutionVenue::Hyperliquid => HyperliquidExecutionAdapter::capabilities(),
            ExecutionVenue::Hyperlink => VenueCapabilities {
                venue: ExecutionVenue::Hyperlink,
                ..HyperliquidExecutionAdapter::capabilities()
            },
            ExecutionVenue::HyperliquidXyz => {
                HyperliquidExecutionAdapter::capabilities_for(HyperliquidProduct::XyzPerpetual)
            }
            ExecutionVenue::HyperliquidIo => {
                HyperliquidExecutionAdapter::capabilities_for(HyperliquidProduct::IoPerpetual)
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
        Self::configured_account_for(venue, false, "main")
    }

    pub fn configured_account_for(
        venue: ExecutionVenue,
        testnet: bool,
        account_name: &str,
    ) -> Result<String> {
        match venue {
            ExecutionVenue::Bulk => credentials::bulk_account_for(account_name),
            ExecutionVenue::Hyperliquid
            | ExecutionVenue::HyperliquidXyz
            | ExecutionVenue::HyperliquidIo
            | ExecutionVenue::HyperliquidSpot
            | ExecutionVenue::HyperliquidOutcomes => credentials::hyperliquid_account_for(
                HyperliquidNetwork::from_testnet(testnet),
                account_name,
            ),
            ExecutionVenue::Hyperlink => {
                if testnet {
                    anyhow::bail!("HyperLink does not support testnet");
                }
                credentials::hyperlink_account_for(account_name)
            }
        }
    }

    pub fn configured_accounts(
        venue: ExecutionVenue,
        testnet: bool,
    ) -> Result<Vec<(String, String)>> {
        match venue {
            ExecutionVenue::Bulk => credentials::bulk_accounts(),
            ExecutionVenue::Hyperliquid
            | ExecutionVenue::HyperliquidXyz
            | ExecutionVenue::HyperliquidIo
            | ExecutionVenue::HyperliquidSpot
            | ExecutionVenue::HyperliquidOutcomes => {
                credentials::hyperliquid_accounts(HyperliquidNetwork::from_testnet(testnet))
            }
            ExecutionVenue::Hyperlink => {
                if testnet {
                    anyhow::bail!("HyperLink does not support testnet");
                }
                credentials::hyperlink_accounts()
            }
        }
    }

    pub async fn account_snapshot(&self, account: &str) -> Result<AccountSnapshot> {
        match self {
            Self::Bulk(adapter) => adapter.account_snapshot(account).await,
            Self::Hyperliquid(adapter) => adapter.account_snapshot(account).await,
        }
    }

    pub async fn max_leverage(&self, symbol: &str) -> Result<u32> {
        match self {
            Self::Hyperliquid(adapter) => adapter.max_leverage(symbol).await,
            Self::Bulk(_) => anyhow::bail!("BULK does not expose leverage through this adapter"),
        }
    }

    pub async fn configure_leverage(&self, symbol: &str, leverage: f64) -> Result<()> {
        match self {
            Self::Hyperliquid(adapter) => adapter.configure_leverage(symbol, leverage).await,
            Self::Bulk(_) => anyhow::bail!("BULK leverage is configured during order execution"),
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
                    .submit_trade(
                        credentials::active_bulk_credential_for_account(&plan.account)?,
                        plan,
                    )
                    .await
            }
            Self::Hyperliquid(adapter) => adapter.submit_trade(plan).await,
        }
    }

    pub async fn submit_user_outcome(
        &self,
        action: crate::providers::hyperliquid::exchange::UserOutcomeAction,
    ) -> Result<serde_json::Value> {
        match self {
            Self::Hyperliquid(adapter) => adapter.submit_user_outcome(action).await,
            Self::Bulk(_) => anyhow::bail!("user outcome actions require Hyperliquid outcomes"),
        }
    }

    pub async fn cancel_order(&self, plan: &CancelPlan) -> Result<ExecutionReceipt> {
        match self {
            Self::Bulk(adapter) => {
                adapter
                    .cancel_order(
                        credentials::active_bulk_credential_for_account(&plan.account)?,
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
                        credentials::active_bulk_credential_for_account(&plan.account)?,
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
        let Some(first) = plans.first() else {
            return Ok(Vec::new());
        };
        match self {
            Self::Bulk(adapter) => {
                adapter
                    .submit_trades(
                        credentials::active_bulk_credential_for_account(&first.account)?,
                        plans,
                    )
                    .await
            }
            Self::Hyperliquid(adapter) => adapter.submit_trades(plans).await,
        }
    }

    pub async fn cancel_orders(&self, plans: &[CancelPlan]) -> Result<Vec<ExecutionOutcome>> {
        let Some(first) = plans.first() else {
            return Ok(Vec::new());
        };
        match self {
            Self::Bulk(adapter) => {
                adapter
                    .cancel_orders(
                        credentials::active_bulk_credential_for_account(&first.account)?,
                        plans,
                    )
                    .await
            }
            Self::Hyperliquid(adapter) => adapter.cancel_orders(plans).await,
        }
    }

    pub async fn cancel_orders_fast(&self, plans: &[CancelPlan]) -> Result<Vec<ExecutionOutcome>> {
        let Some(first) = plans.first() else {
            return Ok(Vec::new());
        };
        match self {
            Self::Bulk(adapter) => {
                adapter
                    .cancel_orders(
                        credentials::active_bulk_credential_for_account(&first.account)?,
                        plans,
                    )
                    .await
            }
            Self::Hyperliquid(adapter) => adapter.cancel_orders_fast(plans).await,
        }
    }
}
