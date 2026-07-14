# Market Lab

Market Lab is a terminal-native market analysis toolkit.

## Run

```bash
cargo run -- --help
```

If you are using `mmt`:

```bash
mlab auth set mmt
```

To connect BULK execution, Market Lab generates an agent wallet locally and
uses the main wallet private key once to authorize it. The main wallet key is
never stored; only the generated agent credential is saved in the OS keychain.

```bash
mlab auth set bulk
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
