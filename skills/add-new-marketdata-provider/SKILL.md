---
name: add-new-marketdata-provider
description: Add a standalone Market Lab market-data exchange through the shared provider contract, normalized streams, and market registry. Use for historical data, order books, trades, candles, tickers, open interest, funding, or statistics from a new source.
---

# Add a new market-data provider

Implement the source once behind `MarketDataProvider`. Every command, strategy, script, and backtest must consume the same normalized data without knowing the provider's wire format.

## 1. Define the supported surface

Before editing code, record which feeds the provider actually exposes:

- historical candles
- historical volume bars
- live order book
- live trades
- live candles
- live ticker
- open interest
- funding
- market statistics

Confirm endpoint URLs, subscription messages, snapshot and delta behavior, timestamps, reconnect rules, sequence identifiers, symbol format, and rate limits from primary provider documentation.

Do not advertise a capability that returns an unsupported error at runtime.

## 2. Keep provider details in one module

Place provider-specific work in `src/providers/<provider>/`:

- HTTP and WebSocket clients
- wire request and response types
- symbol conversion
- subscription construction
- snapshot and delta processing
- reconnect and resubscribe behavior
- payload error decoding

Raw payloads and provider symbols must not escape this boundary.

Do not add provider-name checks to bots, strategies, scripting, backtesting, jobs, or runtime code. Those layers select a source and consume canonical records.

## 3. Normalize markets first

Decode the provider's market catalog into `Market` and `ExecutionRules` in `src/markets/`.

Store:

- canonical Market Lab symbol
- provider wire symbol or asset ID
- aliases only when they are unambiguous
- base and quote assets
- active status
- tick size, lot size, precision, and minimum notional when supplied
- network-specific identifiers when they differ

Register the snapshot refresh in `refresh_route`. A successful refresh should support:

```bash
mlab markets --exchange <exchange> --refresh
```

Never force a universal quote asset. Normalize only what is semantically equivalent; preserve real spot quote assets and provider-specific market identity.

Hyperliquid HIP-3 perpetuals share the public exchange `hyperliquidf`. Preserve the DEX in the canonical symbol as `{dex}:{coin}` and translate that symbol to the provider's DEX-specific route only at the provider boundary.

## 4. Implement `MarketDataProvider`

Add the provider to `src/providers/market_data.rs` and implement:

- `exchange`
- `label`
- `capabilities`
- timeframe conversion
- supported historical fetch methods
- supported live connection methods

Unsupported methods must return a clear error. Do not return empty data as success.

Register a standalone provider once in `MarketDataAdapter::for_exchange`. Execution venues that reuse this provider should route through `MarketDataAdapter::for_venue` and `VenueSpec::market_data_venue`; do not duplicate the adapter.

## 5. Emit canonical records

Convert provider messages into the shared domain types:

- `OrderBookSnapshot`
- `TradeTick`
- `OhlcvCandle`
- `MarketTicker`
- the existing normalized historical record types

Use the stream traits that match the declared capabilities:

- `OrderBookEvents`
- `TradeEvents`
- `CandleEvents`
- `TickerEvents`

Wrap them with the existing venue stream adapters. Do not create a second streaming abstraction.

For each record:

- use the exchange event timestamp when available
- preserve side, price, and size semantics
- reject malformed numeric fields
- keep ordering deterministic
- use canonical symbols outside the provider module

## 6. Handle order books correctly

Follow the provider's documented model:

- **Full snapshots:** replace the in-memory book.
- **Snapshot plus deltas:** load the snapshot, apply deltas in sequence, remove zero-size levels, and resnapshot after a sequence gap.

Do not use private order updates to reconstruct a public order book. Private order updates describe the user's orders, not all market liquidity.

After reconnecting, resubscribe and rebuild state before emitting a valid book. Never continue applying deltas to a stale pre-disconnect snapshot.

## 7. Keep exchange behavior out of consumers

The following is a design failure:

```rust
if exchange == "new-exchange" {
    // special path in a bot, script, job, or backtest
}
```

The fix belongs in provider normalization, `MarketDataCapabilities`, market metadata, or the central adapter registry.

Audit before finishing:

```bash
rg -n "NEW_PROVIDER|new-provider" \
  src/bots src/strategies src/commands src/runtime src/scripting
```

Replace the placeholder with the provider's names. There should be no provider-specific data path in consumer code.

## 8. Prove the integration

Add only high-value contract tests:

- market metadata and symbol normalization
- one representative historical response per supported historical record type
- one representative WebSocket event per supported live stream type
- order-book sequence-gap recovery when deltas are used
- capability and adapter registration smoke test

Do not duplicate generic command parser or shared strategy tests for each provider.

Run:

```bash
cargo fmt --all
cargo check --all-targets
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
git diff --check
```

Smoke-test only the feeds the capability declaration enables, for example:

```bash
mlab source orderbook --exchange <exchange> --symbol BTC --depth 20 --stream
mlab source candles --exchange <exchange> --symbol BTC --timeframe 60 --stream
```

## 9. Completion criteria

The provider is complete only when:

- markets refresh into the canonical registry
- symbols resolve in both directions
- declared historical and live feeds work through `MarketDataProvider`
- reconnects restore valid stream state
- consumers require no provider-specific branches
- capability declarations match real behavior
- validation passes without suppressing warnings
