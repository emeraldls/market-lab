//! Factory-scoped contract trades through the same data contract as other exchanges.
use super::trade_socket::{Notification, TradeSocket, websocket_url};
use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

use alloy_sol_types::SolEvent;
use anyhow::bail;
use async_trait::async_trait;

use super::curve::{CurveFactory, FeeMarket};
use super::*;
use crate::domain::onchain::{OnchainTrade, TradeAsset, TradeFee, TradeVolume};
use crate::domain::types::{ProviderHealth, TradeTick};
use crate::markets::{ExchangeMarkets, Market, MarketSnapshot, MarketType, ProviderType};
use crate::providers::market_data::{
    CandleEvents, MarketDataCapabilities, MarketDataProvider, OrderBookEvents,
    StreamIntegrityError, TickerEvents, TradeEvents,
};

const CURVE_FACTORY: &str = "0x43bD454E79f7fD63101377817CbD34111fE4A66A";
const CONFIRMATIONS: u64 = 12;
const BLOCK_BATCH: u64 = 128;

sol! {
    event Swap(address indexed sender, address indexed recipient, address indexed tokenIn, uint256 amountIn, uint256 amountOut, uint256 feeAmount);
    event Buy(address indexed sender, address indexed recipient, uint256 tokens, uint256 hype, uint256 fee);
    event Sell(address indexed sender, address indexed recipient, uint256 tokens, uint256 hype, uint256 fee);
}

pub struct ElysiumMarketData;

#[async_trait]
impl MarketDataProvider for ElysiumMarketData {
    async fn resolve_symbol(&self, symbol: &str) -> Result<String> {
        // Factory membership and token metadata are read on-chain in connect_trades.
        Ok(market_address(symbol)?.to_string())
    }

    fn exchange(&self) -> &str {
        "elysium"
    }
    fn label(&self) -> &str {
        "Elysium"
    }
    fn capabilities(&self) -> MarketDataCapabilities {
        MarketDataCapabilities {
            live_trades: true,
            ..Default::default()
        }
    }
    fn timeframe_from_seconds(&self, _seconds: u32) -> Result<&'static str> {
        bail!("Elysium provides trades with market state; historical candles are not available")
    }
    async fn health(&self) -> Result<ProviderHealth> {
        let client = configured_client()?.0;
        let block = client.wallet_block().await?;
        Ok(ProviderHealth {
            provider: "elysium".into(),
            status: "ok".into(),
            details: json!({"chain_id": client.chain_id, "block": block.number}),
        })
    }
    async fn connect_trades(&self, symbol: &str) -> Result<Box<dyn TradeEvents>> {
        Ok(Box::new(ElysiumTrades::connect(symbol).await?))
    }
    async fn connect_orderbook(
        &self,
        _symbol: &str,
        _depth: u16,
    ) -> Result<Box<dyn OrderBookEvents>> {
        bail!("Elysium contract markets do not have an order book")
    }
    async fn connect_candles(
        &self,
        _symbol: &str,
        _interval: &str,
    ) -> Result<Box<dyn CandleEvents>> {
        bail!("Elysium does not expose a candle stream")
    }
    async fn connect_ticker(&self, _symbol: &str) -> Result<Box<dyn TickerEvents>> {
        bail!("Elysium market state is included with trades")
    }
}

pub fn market_address(symbol: &str) -> Result<Address> {
    let normalized = symbol.trim().to_ascii_lowercase();
    let address = Address::from_str(&normalized)
        .context("Elysium symbol must be a market contract address")?;
    ensure!(
        !address.is_zero(),
        "Elysium market address must not be zero"
    );
    Ok(address)
}

pub(crate) fn configured_client() -> Result<(PoolClient, Address)> {
    let rpc = std::env::var("MLAB_ELYSIUM_RPC_URL")
        .ok()
        .map(|value| value.parse())
        .transpose()
        .context("invalid MLAB_ELYSIUM_RPC_URL")?;
    let mut client = PoolClient::new(rpc)?;
    if let Ok(factory) = std::env::var("MLAB_ELYSIUM_POOL_FACTORY") {
        client.factory = market_address(&factory)?;
    }
    let factory =
        std::env::var("MLAB_ELYSIUM_CURVE_FACTORY").unwrap_or_else(|_| CURVE_FACTORY.into());
    Ok((client, market_address(&factory)?))
}

#[derive(Clone)]
struct MarketIdentity {
    address: Address,
    curve: bool,
    base: TradeAsset,
    quote: TradeAsset,
}

impl MarketIdentity {
    async fn load(client: &PoolClient, address: Address, block: &Block) -> Result<Self> {
        let at = json!({"blockHash": block.hash, "requireCanonical": true});
        let curve = matches!(client.market, FeeMarket::Curve { .. });
        let (base, quote) = if curve {
            let token = client
                .call(address, curve::Curve::tokenCall {}, &at)
                .await?;
            (
                asset(client, token, &at).await?,
                TradeAsset {
                    address: None,
                    symbol: "HYPE".into(),
                    decimals: 18,
                },
            )
        } else {
            let values = client
                .calls(
                    &[
                        (address, Pool::token0Call {}.abi_encode()),
                        (address, Pool::token1Call {}.abi_encode()),
                    ],
                    &at,
                )
                .await?;
            (
                asset(client, decode::<Pool::token0Call>(&values[0])?, &at).await?,
                asset(client, decode::<Pool::token1Call>(&values[1])?, &at).await?,
            )
        };
        Ok(Self {
            address,
            curve,
            base,
            quote,
        })
    }

    fn kind(&self) -> &'static str {
        if self.curve { "bonding" } else { "pool" }
    }

    async fn state(&self, client: &PoolClient, block: &Block) -> Result<Value> {
        if self.curve {
            let at = json!({"blockHash": block.hash, "requireCanonical": true});
            client.curve_snapshot(self.address, block, &at).await
        } else {
            Ok(serde_json::to_value(
                client.snapshot_at(self.address, block).await?,
            )?)
        }
    }

    fn current_price(&self, state: &Value) -> Result<Option<f64>> {
        if self.curve {
            return Ok(Some(decimal_field(state, "/current_price/formatted")?));
        }
        let base = decimal_field(state, "/reserves/0/amount/formatted")?;
        let quote = decimal_field(state, "/reserves/1/amount/formatted")?;
        if base == 0.0 || quote == 0.0 {
            return Ok(None);
        }
        let price = quote / base;
        ensure!(
            price.is_finite() && price > 0.0,
            "invalid Elysium pool price"
        );
        Ok(Some(price))
    }
}

async fn asset(client: &PoolClient, address: Address, at: &Value) -> Result<TradeAsset> {
    let values = client
        .calls(
            &[
                (address, Token::symbolCall {}.abi_encode()),
                (address, Token::decimalsCall {}.abi_encode()),
            ],
            at,
        )
        .await?;
    let decimals = decode::<Token::decimalsCall>(&values[1])?;
    ensure!(decimals <= 77, "unsupported token decimals");
    Ok(TradeAsset {
        address: Some(address.to_string()),
        symbol: decode::<Token::symbolCall>(&values[0])?,
        decimals,
    })
}

fn decimal_field(value: &Value, pointer: &str) -> Result<f64> {
    let amount: f64 = value
        .pointer(pointer)
        .and_then(Value::as_str)
        .with_context(|| format!("missing Elysium state field {pointer}"))?
        .parse()?;
    ensure!(
        amount.is_finite() && amount >= 0.0,
        "invalid Elysium state amount"
    );
    Ok(amount)
}

pub async fn market_snapshot() -> Result<MarketSnapshot> {
    let (mut client, curve_factory) = configured_client()?;
    let block = client.wallet_block().await?;
    let at = json!({"blockHash": block.hash, "requireCanonical": true});
    let mut markets = Vec::new();
    for curve in [false, true] {
        if curve {
            client.factory = curve_factory;
            client.market = FeeMarket::Curve {
                factory: curve_factory,
            };
        }
        let count = if curve {
            client
                .call(client.factory, CurveFactory::marketCountCall {}, &at)
                .await?
        } else {
            client
                .call(client.factory, PoolFactory::poolCountCall {}, &at)
                .await?
        };
        let count: u64 = count.try_into().context("market count exceeds u64")?;
        for index in 0..count {
            let address = if curve {
                client
                    .call(
                        client.factory,
                        CurveFactory::marketsCall {
                            index: U256::from(index),
                        },
                        &at,
                    )
                    .await?
            } else {
                client
                    .call(
                        client.factory,
                        PoolFactory::poolsCall {
                            index: U256::from(index),
                        },
                        &at,
                    )
                    .await?
            };
            let identity = MarketIdentity::load(&client, address, &block).await?;
            markets.push(Market {
                symbol: address.to_string(),
                provider_symbol: address.to_string(),
                venue_symbol: address.to_string(),
                venue_id: None,
                aliases: Vec::new(),
                base_asset: identity.base.symbol,
                quote_asset: identity.quote.symbol,
                venue_base_asset: identity.base.address.unwrap_or_else(|| "native".into()),
                venue_quote_asset: identity.quote.address.unwrap_or_else(|| "native".into()),
                status: "active".into(),
                price_increment: None,
                size_increment: None,
                execution: None,
                network_variants: BTreeMap::new(),
            });
        }
    }
    Ok(MarketSnapshot {
        schema_version: 1,
        provider: "elysium".into(),
        provider_type: ProviderType::Standalone,
        source_url: "https://elysium.kinetiq.xyz".into(),
        fetched_at: chrono::Utc::now().to_rfc3339(),
        exchanges: vec![ExchangeMarkets {
            exchange: "elysium".into(),
            provider_exchange: None,
            name: "Elysium".into(),
            market_type: MarketType::Contract,
            markets,
        }],
    })
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(super) struct TradeLog {
    address: Address,
    topics: Vec<B256>,
    data: Bytes,
    block_number: U64,
    block_hash: B256,
    transaction_hash: B256,
    transaction_index: U64,
    log_index: U64,
    #[serde(default)]
    removed: bool,
}

#[derive(Clone, Default, Deserialize, Serialize)]
struct Volume {
    base: U256,
    quote: U256,
    trades: u64,
}

struct DecodedTrade {
    buy: bool,
    base: U256,
    quote: U256,
    fee: U256,
    fee_asset: TradeAsset,
    sender: Address,
    recipient: Address,
}

fn decode_trade(log: &TradeLog, market: &MarketIdentity) -> Result<DecodedTrade> {
    ensure!(
        log.address == market.address && !log.removed,
        "non-canonical Elysium trade log"
    );
    if !market.curve {
        let event = Swap::decode_raw_log_validate(log.topics.iter().copied(), &log.data)?;
        let base = market
            .base
            .address
            .as_deref()
            .context("missing pool base address")?
            .parse::<Address>()?;
        let quote = market
            .quote
            .address
            .as_deref()
            .context("missing pool quote address")?
            .parse::<Address>()?;
        ensure!(
            event.tokenIn == base || event.tokenIn == quote,
            "swap input is not a pool asset"
        );
        let buy = event.tokenIn == quote;
        Ok(DecodedTrade {
            buy,
            base: if buy { event.amountOut } else { event.amountIn },
            quote: if buy { event.amountIn } else { event.amountOut },
            fee: event.feeAmount,
            fee_asset: if buy {
                market.quote.clone()
            } else {
                market.base.clone()
            },
            sender: event.sender,
            recipient: event.recipient,
        })
    } else if log.topics.first() == Some(&Buy::SIGNATURE_HASH) {
        let event = Buy::decode_raw_log_validate(log.topics.iter().copied(), &log.data)?;
        Ok(DecodedTrade {
            buy: true,
            base: event.tokens,
            quote: event.hype,
            fee: event.fee,
            fee_asset: market.quote.clone(),
            sender: event.sender,
            recipient: event.recipient,
        })
    } else {
        let event = Sell::decode_raw_log_validate(log.topics.iter().copied(), &log.data)?;
        Ok(DecodedTrade {
            buy: false,
            base: event.tokens,
            quote: event.hype,
            fee: event.fee,
            fee_asset: market.quote.clone(),
            sender: event.sender,
            recipient: event.recipient,
        })
    }
}

impl Volume {
    fn add(&mut self, trade: &DecodedTrade) -> Result<()> {
        self.base = self
            .base
            .checked_add(trade.base)
            .context("base volume overflow")?;
        self.quote = self
            .quote
            .checked_add(trade.quote)
            .context("quote volume overflow")?;
        self.trades = self.trades.checked_add(1).context("trade count overflow")?;
        Ok(())
    }
}

fn quantity(raw: U256, decimals: u8) -> Result<String> {
    Ok(TokenAmount::new(raw, decimals)?.formatted)
}

fn normalize(
    log: &TradeLog,
    market: &MarketIdentity,
    block: &Block,
    state: &Value,
    volume: &mut Volume,
    from_block: u64,
    chain_id: u64,
) -> Result<TradeTick> {
    ensure!(
        log.block_hash == block.hash && log.block_number == block.number,
        "trade block changed during read"
    );
    let trade = decode_trade(log, market)?;
    ensure!(
        !trade.base.is_zero() && !trade.quote.is_zero(),
        "empty Elysium trade"
    );
    volume.add(&trade)?;
    let base_amount = quantity(trade.base, market.base.decimals)?;
    let quote_amount = quantity(trade.quote, market.quote.decimals)?;
    let size: f64 = base_amount.parse()?;
    let price = quote_amount.parse::<f64>()? / size;
    ensure!(
        size.is_finite() && size > 0.0 && price.is_finite() && price > 0.0,
        "invalid trade price or size"
    );
    let timestamp_ms = block
        .timestamp
        .to::<u64>()
        .checked_mul(1000)
        .context("timestamp overflow")?;
    Ok(TradeTick {
        exchange: "elysium".into(),
        symbol: market.address.to_string(),
        timestamp_ms,
        price,
        size,
        taker_buy: trade.buy,
        onchain: Some(Box::new(OnchainTrade {
            chain_id,
            market: market.address.to_string(),
            kind: market.kind().into(),
            block_number: block.number.to(),
            block_hash: block.hash.to_string(),
            transaction_hash: log.transaction_hash.to_string(),
            transaction_index: log.transaction_index.to(),
            log_index: log.log_index.to(),
            timestamp_ms,
            side: if trade.buy { "buy" } else { "sell" }.into(),
            sender: trade.sender.to_string(),
            recipient: trade.recipient.to_string(),
            base: market.base.clone(),
            quote: market.quote.clone(),
            base_amount,
            quote_amount,
            fee: TradeFee {
                amount: quantity(trade.fee, trade.fee_asset.decimals)?,
                asset: trade.fee_asset,
            },
            current_price: market.current_price(state)?,
            volume: TradeVolume {
                from_block,
                through_log_index: log.log_index.to(),
                trades: volume.trades,
                base: quantity(volume.base, market.base.decimals)?,
                quote: quantity(volume.quote, market.quote.decimals)?,
            },
            state: state.clone(),
        })),
    })
}

pub struct ElysiumTrades {
    client: PoolClient,
    market: MarketIdentity,
    checkpoint: Block,
    from_block: u64,
    volume: Volume,
    socket: Option<TradeSocket>,
    head: Option<Block>,
    pending: BTreeMap<(u64, u64), TradeLog>,
    // Replay through the connection/gap head, including its not-yet-confirmed blocks.
    recovery_end: u64,
    #[cfg(test)]
    backfill_reads: usize,
}

impl ElysiumTrades {
    async fn connect(symbol: &str) -> Result<Self> {
        let address = market_address(symbol)?;
        let (mut client, curve_factory) = configured_client()?;
        let latest = client.wallet_block().await?;
        let checkpoint = read_block(
            &client,
            latest.number.to::<u64>().saturating_sub(CONFIRMATIONS),
        )
        .await?;
        let at = json!({"blockHash": latest.hash, "requireCanonical": true});
        if !client
            .call(
                client.factory,
                PoolFactory::isPoolCall { pool: address },
                &at,
            )
            .await?
        {
            ensure!(
                client
                    .call(
                        curve_factory,
                        CurveFactory::isMarketCall { market: address },
                        &at
                    )
                    .await?,
                "market is not registered with the configured Marketlab pool or bonding factory"
            );
            client.factory = curve_factory;
            client.market = FeeMarket::Curve {
                factory: curve_factory,
            };
        }
        let market = MarketIdentity::load(&client, address, &latest).await?;
        let from_block = checkpoint.number.to::<u64>() + 1;
        Ok(Self {
            client,
            market,
            checkpoint,
            from_block,
            volume: Volume::default(),
            socket: None,
            head: None,
            pending: BTreeMap::new(),
            recovery_end: 0,
            #[cfg(test)]
            backfill_reads: 0,
        })
    }

    fn topics(&self) -> Vec<B256> {
        if self.market.curve {
            vec![Buy::SIGNATURE_HASH, Sell::SIGNATURE_HASH]
        } else {
            vec![Swap::SIGNATURE_HASH]
        }
    }

    async fn subscribe(&mut self) -> Result<()> {
        let configured = std::env::var("MLAB_ELYSIUM_WS_URL").ok();
        let url = websocket_url(&self.client.rpc_url, configured.as_deref())?;
        let socket = TradeSocket::connect(
            &url,
            self.client.chain_id,
            self.market.address,
            self.topics(),
        )
        .await?;
        // Subscribe first, then establish the replay boundary so startup cannot lose trades.
        let head = self.client.wallet_block().await?;
        self.recovery_end = head.number.to();
        self.head = Some(head);
        self.socket = Some(socket);
        Ok(())
    }

    fn accept_log(&mut self, log: TradeLog) -> Result<()> {
        ensure!(
            log.address == self.market.address
                && log
                    .topics
                    .first()
                    .is_some_and(|topic| self.topics().contains(topic)),
            "WebSocket returned a foreign trade log"
        );
        let number = log.block_number.to::<u64>();
        let key = (number, log.log_index.to::<u64>());
        if number <= self.checkpoint.number.to::<u64>() {
            if log.removed {
                return Err(StreamIntegrityError(
                    "Elysium confirmed trade was removed; reconcile before restarting",
                )
                .into());
            }
            // Startup replay and subscription notifications intentionally overlap.
            return Ok(());
        }
        if log.removed {
            if self
                .pending
                .get(&key)
                .is_some_and(|old| old.block_hash == log.block_hash)
            {
                self.pending.remove(&key);
            }
            self.recovery_end = self.recovery_end.max(number);
            return Ok(());
        }
        if let Some(old) = self.pending.get(&key) {
            if old == &log {
                return Ok(());
            }
            ensure!(
                old.block_hash != log.block_hash,
                "conflicting duplicate WebSocket trade logs"
            );
            self.recovery_end = self.recovery_end.max(number);
        }
        ensure!(
            self.pending.len() < 10_000 || self.pending.contains_key(&key),
            "Elysium trade buffer overflow; reconnect to recover"
        );
        self.pending.insert(key, log);
        Ok(())
    }

    fn accept_head(&mut self, head: Block) {
        if let Some(previous) = &self.head {
            if previous.number == head.number && previous.hash == head.hash {
                return;
            }
            if head.number.to::<u64>() != previous.number.to::<u64>() + 1 {
                self.recovery_end = self
                    .recovery_end
                    .max(previous.number.to())
                    .max(head.number.to());
            }
        }
        self.head = Some(head);
    }

    async fn advance(&mut self) -> Result<Vec<TradeTick>> {
        let head = self
            .head
            .as_ref()
            .context("missing Elysium subscription head")?
            .number
            .to::<u64>();
        let from = self.checkpoint.number.to::<u64>() + 1;
        let confirmed = head.saturating_sub(CONFIRMATIONS);
        if confirmed < from {
            return Ok(Vec::new());
        }
        let replay = from <= self.recovery_end;
        let to = if replay {
            confirmed.min(from + BLOCK_BATCH - 1).min(self.recovery_end)
        } else {
            // Quiet live markets only checkpoint periodically, not on every block.
            if confirmed - from + 1 < BLOCK_BATCH
                && self
                    .pending
                    .range((from, 0)..=(confirmed, u64::MAX))
                    .next()
                    .is_none()
            {
                return Ok(Vec::new());
            }
            confirmed
        };

        let previous = read_block(&self.client, self.checkpoint.number.to()).await?;
        if previous.hash != self.checkpoint.hash {
            return Err(StreamIntegrityError(
                "Elysium confirmed-block reorganization; reconcile before restarting",
            )
            .into());
        }
        let end = read_block(&self.client, to).await?;
        let mut logs: Vec<TradeLog> = if replay {
            #[cfg(test)]
            {
                self.backfill_reads += 1;
            }
            self.client.rpc("eth_getLogs", json!([{
                "address": self.market.address, "fromBlock": format!("0x{from:x}"), "toBlock": format!("0x{to:x}"),
                "topics": [self.topics()],
            }])).await?
        } else {
            self.pending
                .range((from, 0)..=(to, u64::MAX))
                .map(|(_, log)| log.clone())
                .collect()
        };
        logs.sort_by_key(|log| (log.block_number, log.transaction_index, log.log_index));
        logs.dedup();
        let mut identities = BTreeSet::new();
        let mut states = BTreeMap::new();
        let mut volume = self.volume.clone();
        let mut trades = Vec::new();
        for log in logs {
            let number = log.block_number.to::<u64>();
            ensure!(
                (from..=to).contains(&number),
                "RPC returned a log outside requested blocks"
            );
            ensure!(
                identities.insert((number, log.log_index.to::<u64>())),
                "conflicting duplicate trade logs"
            );
            if let std::collections::btree_map::Entry::Vacant(entry) = states.entry(number) {
                let block = read_block(&self.client, number).await?;
                let state = self.market.state(&self.client, &block).await?;
                entry.insert((block, state));
            }
            let (block, state) = &states[&number];
            trades.push(normalize(
                &log,
                &self.market,
                block,
                state,
                &mut volume,
                self.from_block,
                self.client.chain_id,
            )?);
        }
        // Publish and advance atomically, only after every log and state read succeeded.
        ensure!(
            read_block(&self.client, to).await?.hash == end.hash,
            "Elysium batch reorganized during read"
        );
        ensure!(
            read_block(&self.client, self.checkpoint.number.to())
                .await?
                .hash
                == self.checkpoint.hash,
            "Elysium checkpoint reorganized during read"
        );
        self.volume = volume;
        self.checkpoint = end;
        self.pending.retain(|(number, _), _| *number > to);
        Ok(trades)
    }
}

async fn read_block(client: &PoolClient, number: u64) -> Result<Block> {
    let block: Block = client
        .rpc(
            "eth_getBlockByNumber",
            json!([format!("0x{number:x}"), false]),
        )
        .await?;
    ensure!(
        block.number.to::<u64>() == number,
        "RPC returned the wrong block"
    );
    Ok(block)
}

#[async_trait]
impl TradeEvents for ElysiumTrades {
    fn checkpoint(&self) -> Option<Value> {
        Some(json!({
            "chain_id": self.client.chain_id, "market": self.market.address,
            "factory": self.client.factory, "kind": self.market.kind(),
            "block": { "number": self.checkpoint.number, "hash": self.checkpoint.hash, "timestamp": self.checkpoint.timestamp },
            "from_block": self.from_block, "volume": self.volume,
        }))
    }

    fn restore_checkpoint(&mut self, checkpoint: Value) -> Result<()> {
        ensure!(
            checkpoint["chain_id"] == self.client.chain_id
                && checkpoint["market"] == json!(self.market.address)
                && checkpoint["factory"] == json!(self.client.factory)
                && checkpoint["kind"] == self.market.kind(),
            "trade checkpoint belongs to a different market or chain"
        );
        let block: Block = serde_json::from_value(checkpoint["block"].clone())?;
        let volume: Volume = serde_json::from_value(checkpoint["volume"].clone())?;
        let from = checkpoint["from_block"]
            .as_u64()
            .context("missing trade volume start block")?;
        ensure!(
            from <= block.number.to::<u64>() + 1,
            "invalid trade volume start block"
        );
        self.checkpoint = block;
        self.volume = volume;
        self.from_block = from;
        self.socket = None;
        self.head = None;
        self.pending.clear();
        Ok(())
    }

    async fn next_trades(&mut self) -> Result<Vec<TradeTick>> {
        if self.socket.is_none() {
            self.subscribe().await?;
        }
        loop {
            let previous = self.checkpoint.number;
            let trades = self.advance().await?;
            if !trades.is_empty() {
                return Ok(trades);
            }
            if self.checkpoint.number != previous {
                continue;
            }
            match self
                .socket
                .as_mut()
                .context("missing Elysium WebSocket")?
                .next()
                .await?
            {
                Notification::Log(log) => self.accept_log(log)?,
                Notification::Head(head) => self.accept_head(head),
            }
        }
    }
}

#[cfg(test)]
#[path = "market_data_tests.rs"]
mod tests;
