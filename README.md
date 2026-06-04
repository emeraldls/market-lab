# Market Lab

Market Lab is a terminal-native market analysis toolkit.

## Run

```bash
cargo run -- --help
```

If you are using `mmt`:

```bash
export MMT_API_KEY=your_key
```

Example:

```bash
cargo run -- source orderbook \
  --provider mmt \
  --exchange bybitf \
  --symbol BTC/USDT \
  --depth 100
```

## Docs

- [mlab](https://marketlab.hooklytics.com)

## License

AGPL-3.0. See [LICENSE](./LICENSE).


- fix performance issues in scripting, create single runtime
- expose built in functions to scripting