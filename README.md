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

mlab positions --venue bulk
mlab orders --venue bulk
mlab fills --venue bulk
mlab close BTC/USDT --dry-run
mlab cancel BTC/USDT <ORDER_ID> --dry-run
```

Live place/cancel requests are confirmed in the terminal unless `--yes` is
passed. They are then sent over local IPC to `mlabd`, which owns signing and
nonce sequencing for every terminal or future scripted caller. The runtime is
started automatically for live execution, stays alive while idle, and stores
bounded active state plus append-only events under `~/.market-lab/execution`.

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
