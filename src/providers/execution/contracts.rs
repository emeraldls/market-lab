use super::*;
use crate::domain::contracts::{
    ContractPlan, ContractRequest, SignedTransaction, TransactionReceipt, WalletTransaction,
};
use alloy_primitives::{Address, B256, U256};
use serde_json::Value;

/// Contract operations use the same execution registry, without inventing order-book semantics.
#[async_trait]
pub trait ContractProvider: Send + Sync {
    fn wallet_address(&self) -> Result<Address>;
    async fn state(&self) -> Result<Value>;
    async fn balances(&self, account: Address) -> Result<Value>;
    async fn quote(&self, request: &ContractRequest) -> Result<Value>;
    async fn prepare(&self, account: Address, request: &ContractRequest) -> Result<ContractPlan>;
    async fn sign(&self, call: &WalletTransaction, gas_cap: U256) -> Result<SignedTransaction>;
    async fn broadcast(&self, signed: &SignedTransaction) -> Result<()>;
    async fn receipt(&self, hash: B256) -> Result<Option<TransactionReceipt>>;
    async fn known(&self, hash: B256) -> Result<bool>;
    async fn nonce(&self, account: Address) -> Result<u64>;
}

pub async fn contract_provider(
    venue: ExecutionVenue,
    testnet: bool,
    market: Address,
) -> Result<Box<dyn ContractProvider>> {
    venue.spec()?.validate_network(testnet)?;
    execution_factory(venue).contracts(market).await
}

pub(super) struct ElysiumFactory;

#[async_trait]
impl ExecutionProviderFactory for ElysiumFactory {
    fn environment_names(&self) -> &'static [&'static str] {
        &[
            "MLAB_ELYSIUM_RPC_URL",
            "MLAB_ELYSIUM_WS_URL",
            "MLAB_ELYSIUM_POOL_FACTORY",
            "MLAB_ELYSIUM_CURVE_FACTORY",
        ]
    }

    fn capabilities(&self, venue: ExecutionVenue) -> VenueCapabilities {
        VenueCapabilities {
            venue,
            order_kinds: vec![],
            time_in_forces: vec![],
            contract_actions: true,
            reduce_only: false,
            deterministic_order_ids: false,
            delegated_agent_signing: false,
            native_protective_triggers: false,
            native_oco: false,
            native_on_fill: false,
            integer_leverage: false,
            configure_leverage_before_orders: false,
            price_encoding: crate::domain::execution::PriceEncoding::TickSize,
        }
    }

    async fn contracts(&self, market: Address) -> Result<Box<dyn ContractProvider>> {
        Ok(Box::new(
            crate::providers::elysium::contracts::ElysiumContracts::connect(market).await?,
        ))
    }

    async fn adapter(
        &self,
        _venue: ExecutionVenue,
        _market: VenueMarket,
        _testnet: bool,
        _account_name: &str,
    ) -> Result<Box<dyn ExecutionProvider>> {
        bail!("this venue uses contract operations, not order-book orders or positions")
    }

    async fn account_stream(
        &self,
        _venue: ExecutionVenue,
        _testnet: bool,
        _account: &str,
    ) -> Result<Box<dyn AccountEvents>> {
        bail!("contract execution reports transaction receipts, not private order-book events")
    }

    fn normalize_runtime_event(
        &self,
        _venue: ExecutionVenue,
        _testnet: bool,
        _account: &str,
        _raw: Value,
    ) -> Result<AccountRuntimeEvent> {
        bail!("contract execution does not emit private order-book events")
    }

    async fn connect_transport(&self, _testnet: bool) -> Result<()> {
        credentials::elysium::load()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn daemon_environment_is_explicit_and_does_not_forward_wallet_keys() {
        assert_eq!(
            daemon_environment_names(),
            vec![
                "MLAB_ELYSIUM_CURVE_FACTORY",
                "MLAB_ELYSIUM_POOL_FACTORY",
                "MLAB_ELYSIUM_RPC_URL",
                "MLAB_ELYSIUM_WS_URL"
            ]
        );
    }

    #[tokio::test]
    async fn contract_venue_routes_without_advertising_order_book_support() {
        let venue = ExecutionVenue::parse("elysium").unwrap();
        let caps = ExecutionAdapter::capabilities(venue);
        assert!(caps.contract_actions);
        assert!(caps.order_kinds.is_empty());
        assert!(!caps.delegated_agent_signing);
        assert_eq!(
            crate::markets::market_type("elysium").unwrap(),
            crate::markets::MarketType::Contract
        );
        let data =
            crate::providers::market_data::MarketDataAdapter::for_venue(venue, false).unwrap();
        assert!(data.capabilities().live_trades);
        assert!(ExecutionAdapter::new(venue, false).await.is_err());
        assert!(
            contract_provider(ExecutionVenue::Bulk, false, Address::repeat_byte(1))
                .await
                .is_err()
        );
    }
}
