# Telegram alerts

Create a bot with @BotFather and start a chat with it, then:

```sh
mlab alert setup
mlab alert test
mlab script run examples/alerts/telegram-smoke.py --duration 30
mlab script run examples/alerts/price-crossing.py --param price=83000
mlab script run examples/alerts/multi-exchange-sequence.py --param breakout=83000 --param spread_bps=10
```

Add `--testnet` for Bulk/Hyperliquid testnet feeds. Setup privately prompts for the token. Open its Telegram link or send the one-time code to your bot, then confirm receipt of the test message. No chat ID lookup is needed. For a group, use `mlab alert setup --chat-id -100123456789`. Use a dedicated alert bot for automatic setup.

The sequence waits for Bulk BTC to cross the breakout, then a later Hyperliquid trade must confirm the spread within 30 seconds. Both quotes must be fresh within 5 seconds. It resets after firing, expiry or stale data. Parameters control those values. No orders are placed.

`ctx.alert` returns `buffered` in live jobs and `simulated` in analysis/backtests. Buffering is not delivery confirmation. Run history records `script.alert.submission` (queued, suppressed or rejected) and `script.alert.result` (delivered or failed). The daemon retains per-job/key attempt cooldowns across restart, limits accepted messages to one per second, and never automatically retries an uncertain send. Restarting with a new job ID starts a separate cooldown; sequence state starts empty.

Tokens stay in `credentials/telegram.json` (0600), not in the script. Run `mlab alert remove` to disconnect.
