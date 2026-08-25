---
name: add-new-execution-provider
description: Add or extend a Market Lab execution venue without coupling exchange logic to bots, strategies, scripts, jobs, or runtime orchestration. Use for a new execution API, signing scheme, private account stream, authentication backend, or Hyperliquid HIP-3 DEX.
---

# Add a new execution provider

Implement the exchange once behind Market Lab's execution contracts. Existing bots, strategies, and Python scripts must pick it up through the shared venue and provider layers.

## 1. Classify the change first

Choose the smallest valid integration:

- **New Hyperliquid HIP-3 DEX:** use the existing dynamic venue name `hyperliquidf-{dex}`. Do not add a venue constant, backend variant, or factory branch for each DEX. Confirm that live metadata resolves the DEX and its markets.
- **New venue using an existing execution transport:** add a `VenueSpec` in `src/venues.rs` and reuse its existing execution, authentication, and market-data backends.
- **New execution API:** add a provider implementation, factory registration, venue specification, credentials when required, market metadata, and normalized private events.

Do not create a new provider when a venue is only an alias or a new market carried by an existing transport.

## 2. Keep exchange logic at the provider boundary

Exchange-specific behavior belongs in:

- `src/providers/<provider>/` for HTTP, WebSocket, signing, payloads, and wire errors
- `src/providers/execution.rs` for shared execution registration
- `src/venues.rs` for venue identity and backend routing
- `src/markets/` for market discovery and normalized trading rules
- `src/credentials.rs` and `src/cli/mod.rs` only when the provider needs distinct authentication

Never add exchange-name checks to:

- `src/bots/`
- `src/strategies/`
- `src/commands/bot/`
- `src/commands/strategy/`
- `src/commands/script/`
- `src/runtime/mod.rs`

If a consumer needs to know the exchange name, the abstraction is incomplete. Express the difference through capabilities, normalized market rules, or a provider method.

## 3. Implement a separate execution API

Follow this order.

### Define the wire boundary

Create or extend `src/providers/<provider>/` with:

- API endpoints and network selection
- request and response types
- symbol conversion
- request signing
- authenticated account WebSocket handling
- exchange error decoding
- market metadata decoding

Keep raw JSON and provider symbols inside this module. Return canonical Market Lab domain types to the rest of the application.

Do not silently default missing balances, fills, prices, sizes, leverage, or order status. Return a payload error that includes enough sanitized context to diagnose the provider response.

### Register the venue

Add the venue to `src/venues.rs`:

- `ExecutionBackend` only for a genuinely new transport
- `AuthBackend` only for a genuinely new credential flow
- the correct `VenueMarket` and `NetworkPolicy`
- `market_data_venue` when execution deliberately reuses another venue's public feed
- `dex` only for HIP-3 routing

Venue parsing, display names, execution routing, and market-data routing must come from `VenueSpec`. Do not duplicate venue-name helpers elsewhere.

### Implement `ExecutionProvider`

Implement the contract in `src/providers/execution.rs`:

- account snapshot
- open orders
- fills
- single trade submission and cancellation
- batch trade submission and cancellation
- provider order-ID validation

Implement optional methods only when the provider supports them, including leverage configuration, fast cancellation, gap recovery, or outcome actions.

Batch methods must preserve input order and return one result per requested item. Do not hide a partial batch failure.

### Declare capabilities truthfully

Return an accurate `VenueCapabilities` value for:

- supported order kinds and time-in-force values
- reduce-only support
- deterministic client order IDs
- delegated signing
- protective triggers, OCO, and on-fill behavior
- leverage requirements
- price encoding

Use normalized market metadata for tick size, lot size, minimum notional, precision, and maximum leverage. Do not hardcode account-dependent or changeable venue limits. Surface the exchange error when a limit is not available through metadata.

### Normalize private events

Implement the provider's account event stream and its `ExecutionProviderFactory` adapter. Convert provider events into the shared runtime updates:

- positions
- order state changes
- fills and script execution events

Handle reconnect gaps through the provider recovery hook when the exchange exposes account history. Deduplicate recovered and live fills using the exchange's stable reconciliation identifier.

Register the factory once in `execution_factory`. Do not introduce another execution dispatch table.

### Add authentication only when required

If the provider has distinct credentials:

- add its `AuthProvider` CLI value
- add setup and reauthorization in `src/credentials.rs`
- store only delegated or agent credentials when the protocol permits it
- load credentials inside `mlabd`
- redact secrets from logs, errors, job records, and generated files

If the venue reuses an existing authentication backend, do not create another credential format.

### Add canonical markets

Decode provider markets into `Market` and `ExecutionRules` in `src/markets/`. Preserve both the canonical Market Lab symbol and the provider's wire symbol or asset ID.

Add the provider to `refresh_route` when it has a standalone market catalog. Dynamic markets may be fetched live, but their resolved rules must still use the same canonical model.

## 4. Prove the integration

Add only tests that protect the provider boundary:

- market metadata and symbol normalization
- one deterministic signing fixture, when signing is new
- single and batch order response normalization
- private account event normalization
- one venue/factory registration smoke test
- one test for each genuinely unique venue rule

Do not duplicate shared bot, strategy, scripting, or Clap parser tests for every exchange.

Run:

```bash
cargo fmt --all
cargo check --all-targets
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
git diff --check
```

Audit the consumer layers before finishing:

```bash
rg -n "NEW_PROVIDER|new-provider" \
  src/bots src/strategies src/commands src/runtime src/scripting
```

Replace the placeholder with the provider's names. Results should be limited to intentional user-facing validation or tests, not execution branches.

## 5. Completion criteria

The provider is complete only when:

- a venue resolves through the central registry
- market rules and symbols normalize correctly
- account state and private events use shared domain types
- single and batch execution work through `ExecutionProvider`
- existing bots, strategies, and Python V2 can select it without provider-specific changes
- credentials and network behavior are explicit
- validation passes without suppressing warnings

Increase the daemon runtime version only when the persisted job format or CLI-daemon wire contract changes. Adding an adapter alone does not require a runtime-version bump.
