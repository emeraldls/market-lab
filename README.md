# Market Lab

Market Lab is a terminal-native market analysis toolkit.

## Run

```bash
cargo run -- --help
```

MMT market data requires an API key:

```bash
mlab auth set mmt
```

Bulk public market data is standalone: it does not require an MMT key, a Bulk
key, or `mlab auth set bulk`. The production HTTP and WebSocket endpoints are
compiled into the adapter and are not configured through environment variables.

For Bulk execution, Market Lab generates an agent wallet locally
and uses the main wallet private key once to authorize it. The main wallet key
is never stored; only the generated agent credential is saved in the OS
keychain.

```bash
mlab auth set bulk
```

Build both the CLI and its lightweight execution runtime:

```bash
cargo build --bins
```

BULK market rules are snapshotted in
`src/providers/bulk/markets.json` and embedded into the binary. Runtime symbol
lookup does not call `exchangeInfo`; the catalog records its source and fetch
time so it can be refreshed deliberately when BULK changes a market or trading
rule. Each entry stores both the BULK venue symbol (`BTC-USD`) and Market Lab's
internal symbol (`BTC/USDT`) so provider boundaries do not reconstruct mappings.

```bash
mlab markets --provider bulk
mlab markets --provider bulk --symbol BTC/USDT
```

Execution is expressed as `long`/`short` (with `buy`/`sell` aliases). Every
request is checked against the embedded market catalog before signing. A dry
run prints the complete normalized trade plan and never starts the runtime or
submits a signed transaction.

```bash
mlab trade long BTC/USDT --notional 100 --leverage 5 --dry-run
mlab trade short BTC/USDT --size 0.001 --type limit \
  --price 65000 --tif alo --dry-run
mlab trade long BTC/USDT --notional 100 --leverage 5 \
  --sl 63000 --tp 69000 --dry-run

mlab positions --venue bulk
mlab orders --venue bulk
mlab fills --venue bulk
mlab close BTC/USDT --dry-run
mlab cancel BTC/USDT <ORDER_ID> --dry-run
```

Live place/cancel requests are confirmed in the terminal unless `--yes` is
passed. They are then sent over local IPC to `mlabd`, which owns signing and
nonce sequencing for every terminal and scripted caller. The runtime is
started automatically for live execution, stays alive while idle, and stores
bounded control state plus append-only events under `~/.market-lab/execution`.

`mlabd` maintains Bulk's account WebSocket for order, fill, position, margin,
liquidation, and ADL lifecycle events. It does not poll REST on a timer. If the
connection is lost, it reconnects and performs one REST order/fill recovery for
the disconnected interval before continuing from WebSocket events. Bulk remains
the source of truth for account state; Market Lab persists only job,
idempotency, correlation, and event-delivery metadata.

```bash
mlab daemon start
mlab daemon status
mlab daemon events --limit 20
mlab daemon stop
```

Trade commands also support TOML with CLI values taking precedence:

```toml
version = 1

[market]
symbol = "BTC/USDT"

[execution]
venue = "bulk"
notional = 100
order_type = "market"
leverage = 5
dry_run = true
```

```bash
mlab trade long --config marketlab.toml
```

Scripts can consume BULK directly without `--exchange` or MMT authentication.
Live scripts support candles, order books, open-interest snapshots, total-volume
bars, and trade-derived volume delta. Historical BULK script backtests support
candles and total-volume bars; unsupported historical sources fail before any
provider request.

```bash
mlab script run examples/candle-summary.js \
  --provider bulk --symbol BTC/USDT \
  --source candles:timeframe=60

mlab script run examples/all-sources-live.js \
  --provider bulk --symbol BTC/USDT \
  --source candles:timeframe=60 \
  --source orderbook:depth=50

mlab script backtest examples/sma-cross.js \
  --provider bulk --symbol BTC/USDT \
  --from 1784052554000 --to 1784056154000 \
  --source candles:timeframe=60
```

### Detached script execution

`script run` submits an immutable copy of the script to `mlabd`, starts a
dedicated lightweight script worker, prints its job ID, and releases the
terminal immediately. Market data and execution are independent: a script can
use MMT data and Bulk execution, or Bulk for both. Omitting `--venue` leaves
`ctx.trade` and `ctx.cancel` disabled, so an analysis-only script cannot trade
accidentally.

The script snapshot, provider, exchange, source settings, parameters, symbol,
and venue are immutable deployment inputs. Running the same source with another
exchange or another `--param` value creates another independent job; restarting
a job uses its original snapshot and inputs.

```bash
# Analysis only; detached, but unable to execute.
mlab script run examples/candle-summary.js \
  --provider bulk --symbol BTC/USDT \
  --source candles:timeframe=60

# Explicitly arm Bulk execution. This example defaults to armed=false.
mlab script run examples/bulk-limit-protected.js \
  --provider bulk --symbol BTC/USDT \
  --source candles:timeframe=60 \
  --venue bulk \
  --param candles.armed=true

mlab script jobs
mlab script status <JOB_ID>
mlab script logs <JOB_ID> --follow
mlab script stop <JOB_ID>
mlab script restart <JOB_ID>
```

`ctx.trade(request)` validates the request synchronously and returns a stable
Market Lab order reference immediately. The actual signing/submission is then
serialized through `mlabd`. `key` is required and acts as the strategy's
idempotency key: retrying the exact request returns the same order, while
reusing the key with different parameters is rejected.

```js
const entry = ctx.trade({
  key: "btc-entry-v1",
  side: "long",             // long | short
  notional: 100,             // exactly one of notional | size
  leverage: 5,
  order: {
    type: "limit",           // market | limit
    price: 65000,
    tif: "gtc"               // gtc | ioc | alo
  },
  sl: 63000,
  tp: 69000
})

// entry = { id: "ord_...", key: "btc-entry-v1" }
ctx.cancel({
  key: "cancel-btc-entry-v1",
  order: entry.id             // the original trade key also works
})
```

For an entry with `sl` and/or `tp`, Market Lab signs Bulk's native on-fill
protection in the same transaction as the parent order. Supplying both creates
Bulk's native OCO range; protection becomes active after the entry fills and
one trigger cancels its sibling. Market Lab does not monitor prices locally to
emulate protective orders.

Scripts may optionally export `onExecution(ctx, event)`. Events include
`order.pending`, `order.accepted`, `order.updated`, `order.fill`,
`order.filled`, `order.cancelled`, `order.rejected`, position lifecycle events,
and account margin updates. Order-related events include the stable `orderId`,
the strategy `key`, and Bulk's `venueOrderId`. Delivery is journaled and
acknowledged only after the hook succeeds, so an unacknowledged event is
replayed after a worker restart.

```js
export function onExecution(ctx, event) {
  if (event.type === "order.rejected") {
    return { metrics: { rejected_order: event.orderId, details: event.data } }
  }
}
```

Script source records use milliseconds in `t`. Candle records always contain
`o`, `h`, `l`, `c`, `volume`, and `trades`. MMT additionally supplies its
directional `vb`, `vs`, `tb`, and `ts` fields; BULK leaves those fields absent
instead of fabricating directional volume.

Example:

```bash
mlab source candles --provider bulk --symbol BTC/USDT \
  --timeframe 60 --from 1784052554000 --to 1784056154000 --output json

mlab source volumes --provider bulk --symbol BTC/USDT \
  --timeframe 60 --from 1784052554000 --to 1784056154000 --output json

mlab source orderbook --provider bulk --symbol BTC/USDT --depth 100
mlab source oi --provider bulk --symbol BTC/USDT
mlab source funding --provider bulk --symbol BTC/USDT
mlab source stats --provider bulk --symbol BTC/USDT

mlab source candles --provider bulk --symbol BTC/USDT --timeframe 60 --stream
mlab source orderbook --provider bulk --symbol BTC/USDT --depth 100 --stream
mlab source stats --provider bulk --symbol BTC/USDT --stream
mlab source vd --provider bulk --symbol BTC/USDT --stream
```

Bulk capabilities:

| Data | Snapshot/history | Stream |
| --- | --- | --- |
| OHLCV candles | Historical | Yes |
| Total-volume bars | Historical | Yes, from candle volume |
| L2 order book | Snapshot | Yes, stateful deltas |
| Ticker/statistics | Snapshot | Yes |
| Open interest | Current snapshot | Yes |
| Funding | Current snapshot | Yes |
| Volume delta | No historical endpoint | Yes, derived from side-signed taker flow |

Bulk does not expose historical order books, historical open interest,
historical funding, or historical volume delta through the integrated public
API, so Market Lab returns explicit capability errors instead of fabricating
those datasets. Bulk's statistics endpoint currently reports rolling 24-hour
data for every accepted period.

## Time model

Market Lab uses Unix milliseconds at the application boundary. CLI `--from`
and `--to` values, envelope `ts_ms` values, and Bulk candle timestamps are
milliseconds. Providers are converted only inside their adapters: MMT receives
seconds, while Bulk response timestamps are normalized from their native unit
to milliseconds before entering Market Lab.

## Docs

- [mlab](https://marketlab.hooklytics.com)

## License

AGPL-3.0. See [LICENSE](./LICENSE).


- fix performance issues in scripting, create single runtime
- expose built in functions to scripting
