<p align="center">
  <a href="https://marketlab.sh">
    <img src="https://marketlab.sh/marketlab-mark.svg" width="72" alt="Market Lab">
  </a>
</p>

<h1 align="center">Market Lab</h1>

<p align="center">
  A local-first terminal for market data, algorithmic execution, market making, and programmable trading.
</p>

<p align="center">
  <a href="https://marketlab.sh">Website</a> ·
  <a href="https://docs.marketlab.sh">Documentation</a> ·
  <a href="https://marketlab.sh/markets">Market catalog</a> ·
  <a href="https://github.com/emeraldls/market-lab/releases">Releases</a>
</p>

<p align="center">
  <a href="https://github.com/emeraldls/market-lab/actions/workflows/ci.yml"><img src="https://github.com/emeraldls/market-lab/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://github.com/emeraldls/market-lab/releases"><img src="https://img.shields.io/github/v/release/emeraldls/market-lab" alt="Latest release"></a>
  <a href="./LICENSE"><img src="https://img.shields.io/github/license/emeraldls/market-lab" alt="AGPL-3.0 license"></a>
</p>

Market Lab brings the full trading workflow into one CLI: inspect markets, stream order books, calculate studies, place orders, run execution algorithms, deploy market-making bots, and backtest or run Python and JavaScript strategies.

Market data and execution are separate by design. A strategy can read one venue, derive a signal from another, and execute wherever you choose.

## Install

Market Lab supports macOS arm64, Linux x64, and Linux arm64.

```bash
curl -fsSL https://marketlab.sh/install.sh | bash
```

The installer asks whether `mlabd` should run natively or inside Docker. Choose directly for unattended installation:

```bash
curl -fsSL https://marketlab.sh/install.sh | bash -s -- --daemon docker
# or
curl -fsSL https://marketlab.sh/install.sh | bash -s -- --daemon native
```

Verify the installation:

```bash
mlab --version
mlab daemon backend
```

See the [installation guide](https://docs.marketlab.sh/installation) for upgrades, PATH setup, and version pinning.

## Quickstart

Public Hyperliquid market data requires no authentication.

```bash
# Install the current market snapshot.
mlab markets --exchange hyperliquidf --refresh

# Find BTC and stream its order book.
mlab markets --exchange hyperliquidf --symbol BTC
mlab source orderbook \
  --exchange hyperliquidf \
  --symbol BTC \
  --depth 20 \
  --stream
```

Authorize an execution venue only when you are ready to trade. Preview the order before submitting anything:

```bash
mlab auth set hyperliquid

mlab trade long BTC \
  --venue hyperliquidf \
  --margin 10 \
  --leverage 2 \
  --dry-run
```

Keep `--dry-run` enabled until the printed plan matches your intention.

## One CLI, the complete workflow

| Task | Command |
| --- | --- |
| Discover markets and normalized symbols | `mlab markets` |
| Read order books, candles, OI, funding, volume, and statistics | `mlab source` |
| Calculate spread, slippage, depth, imbalance, VAMP, and CVD | `mlab study` |
| Place and manage direct orders | `mlab trade`, `positions`, `orders`, `fills`, `cancel`, `close` |
| Run TWAP, VWAP, and OIWAP execution | `mlab strategy` |
| Run Grid, Mid-Price, and Volume-Mid market makers | `mlab bot` |
| Backtest and deploy Python or JavaScript | `mlab script` |
| Supervise persistent jobs | `mlab daemon` |
| Control Market Lab on another machine | `mlab remote` |

Every command supports `--help`, and commands intended for applications expose structured JSON output where applicable.

## Python strategies without framework ceremony

Python Scripting V2 uses ordinary Python. There is no Market Lab package to import and no strategy base class to inherit.

```python
SOURCE = "btc@candles@hyperliquidf:timeframe=60"

script = {
    "name": "latest-btc-close",
    "version": "2",
    "lookback": 20,
}


def on_data(ctx, history):
    candle = history.source(SOURCE, 0)
    if candle is not None:
        print(f"BTC close: {candle['c']}")
```

The literal `history.source(...)` selector declares the subscription. Use your own Python environment when the strategy needs NumPy, pandas, statsmodels, matplotlib, or another package.

```bash
mlab script backtest strategy.py \
  --from "2026-07-15 09:30:00" \
  --to "2026-07-15 16:00:00"

mlab script run strategy.py --duration 3600
mlab script logs <JOB_ID> --follow
```

Backtests use the simulator and never reach an exchange. Live jobs run through `mlabd`. Read the [Python V2 guide](https://docs.marketlab.sh/scripting-v2) before enabling execution.

JavaScript Scripting V1 remains available for existing strategies.

## Providers and venues

Market Lab uses normalized symbols and domain types while preserving the provider's real market identity at the boundary.

### Market data

| Provider | Coverage |
| --- | --- |
| MMT | Historical and live multi-venue market data |
| Binance | Standalone public Spot and USD-M perpetual data |
| BULK | Native perpetual market data on the public testnet |
| Hyperliquid | Spot, core perpetuals, HIP-3 DEXs, and outcome markets |

### Execution

| Venue | Selector | Network |
| --- | --- | --- |
| BULK perpetuals | `bulkf` | Public testnet |
| Hyperliquid Spot | `hyperliquid` | Mainnet or testnet |
| Hyperliquid core perpetuals | `hyperliquidf` | Mainnet or testnet |
| Hyperliquid HIP-3 perpetuals | `hyperliquidf-{dex}` | Mainnet or testnet |
| Hyperliquid outcomes | `hyperliquid-outcomes` | Mainnet or testnet |
| HyperLink perpetuals | `hyperlinkf` | Mainnet; access controlled by HyperLink |

Use the [market catalog](https://marketlab.sh/markets) instead of guessing symbols, collateral assets, or venue names.

## How Market Lab runs

```text
market data ──> command, bot, strategy, or script ──> mlabd ──> execution venue
                                                        │
                                                        └── jobs, orders, fills, logs
```

`mlab` is the user-facing CLI. `mlabd` owns long-running jobs and their execution state, so closing the terminal does not stop a deployment.

The daemon can run in either environment:

```bash
mlab daemon backend native
mlab daemon backend docker
```

The Docker backend keeps the daemon and managed Python runtimes inside containers. The native backend runs directly under the current operating-system user.

## Run close to the exchange

Market Lab can route commands to another installation over ordinary SSH:

```bash
mlab remote use trader@SERVER_IP
mlab bot jobs
```

Override the active target for one command:

```bash
mlab --remote local bot jobs
mlab --remote trader@OTHER_SERVER bot jobs
```

The remote `mlab` talks to its own native or Docker daemon. Commands, streamed output, and exit status travel through SSH; daemon ports and credentials are not exposed.

See [SSH transport](https://docs.marketlab.sh/transport/ssh) for setup and troubleshooting.

## Credentials and safety

- Public standalone market data does not require credentials.
- Master wallet keys are requested through hidden prompts for approval and are not stored.
- Market Lab stores delegated execution credentials under `~/.market-lab/credentials/` with owner-only permissions.
- Credentials stay on the machine that executes the command; SSH targets do not copy them.
- Only run scripts you trust. Script resource controls are reliability boundaries, not security sandboxes.

Market Lab can submit real orders. It does not guarantee profit, prevent liquidation, or make an unsafe strategy safe. Start with public data, backtests, `--dry-run`, testnet where available, and small size.

Read [Authentication](https://docs.marketlab.sh/authentication) and [Runtime safety](https://docs.marketlab.sh/scripting-v2/runtime-safety) before live deployment.

## Build from source

Market Lab is written in Rust and uses the stable toolchain.

```bash
git clone https://github.com/emeraldls/market-lab.git
cd market-lab
cargo build --release --bins
./target/release/mlab --help
```

Before opening a pull request:

```bash
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
```

Provider work must stay behind the shared provider contracts. Read the repository skills before adding a new [execution provider](./skills/add-new-execution-provider/SKILL.md) or [market-data provider](./skills/add-new-marketdata-provider/SKILL.md).

## Project status

Market Lab is under active development and remains pre-1.0. Interfaces and venue support may change between releases. Review the [release notes](https://github.com/emeraldls/market-lab/releases) before upgrading a machine that runs live jobs.

Bug reports and focused pull requests are welcome. Include the command, version, venue or provider, expected behavior, and sanitized logs when reporting an issue. Never post credentials or wallet keys.

## License

Market Lab is licensed under [AGPL-3.0](./LICENSE).
