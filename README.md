# Market Lab

A CLI-native market analysis toolkit for humans and AI agents.

Built for market makers, quants, and traders who care about market structure.

## Current Scope

- Orderbook inspection
- Live orderbook source streaming
- Studies on orderbook/volume-delta data
- JSON output for automation/agents

## Quick Start

```bash
cargo run -- --help
```

Set MMT key when using `--provider mmt`:

```bash
export MMT_API_KEY=your_key
```

## Commands (Current)

### Inspect

```bash
cargo run -- inspect --provider mmt --exchange binancef --symbol BTC/USDT --at 1779399687271 --depth 20 --output terminal
```

### Source

Orderbook snapshot:

```bash
cargo run -- source orderbook --provider mmt --exchange bybitf --symbol BTC/USDT --depth 100 --output json
```

Orderbook stream (noise controls):

```bash
cargo run -- source orderbook --provider mmt --exchange bybitf --symbol BTC/USDT --stream --depth 100 --min-size 0.1 --price-group 1 --interval-ms 1000 --buffer-size 20 --output terminal
```

Volume Delta (VD):

```bash
cargo run -- source vd --provider mmt --exchange bybitf --symbol BTC/USDT --timeframe 60 --from 1779620400 --to 1779624000 --bucket 1 --output json
```

VD stream:

```bash
cargo run -- source vd --provider mmt --exchange bybitf --symbol BTC/USDT --timeframe 60 --bucket 1 --stream --interval-ms 1000 --buffer-size 20 --output terminal
```

### Study

Slippage:

```bash
cargo run -- study slippage --provider mmt --exchange bybitf --symbol BTC/USDT --side buy --notional 100000 --depth 100 --output json
```

Imbalance:

```bash
cargo run -- study imbalance --provider mmt --exchange bybitf --symbol BTC/USDT --depth 50 --output json
```

VAMP:

```bash
cargo run -- study vamp --provider mmt --exchange bybitf --symbol BTC/USDT --depth 100 --dollar-depth 250000 --output json
```

CVD:

```bash
cargo run -- study cvd --provider mmt --exchange bybitf --symbol BTC/USDT --timeframe 3600 --from 1779620400 --to 1779624000 --bucket 1 --output json
```

CVD stream:

```bash
cargo run -- study cvd --provider mmt --exchange bybitf --symbol BTC/USDT --timeframe 3600 --bucket 1 --stream --interval-ms 1000 --buffer-size 20 --output terminal
```

### Health

```bash
cargo run -- health --provider mmt --output json
```

## Timeframe Input

`source vd` and `study cvd` accept timeframe in **seconds** and map to MMT internally.

Supported values:

- `60` (1m)
- `300` (5m)
- `900` (15m)
- `1800` (30m)
- `3600` (1h)
- `14400` (4h)
- `86400` (1d)

## License

GNU Affero General Public License v3.0 (AGPL-3.0). See [LICENSE](./LICENSE).
