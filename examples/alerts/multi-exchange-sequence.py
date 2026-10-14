# A builder can generate the sources, parameters and steps without exposing tokens.
script = {
    "name": "btc-cross-exchange-sequence", "version": "2", "lookback": 2,
    "params": {
        "breakout": {"type": "number", "required": True},
        "spread_bps": {"type": "number", "default": 10},
        "window_seconds": {"type": "number", "default": 30},
        "fresh_seconds": {"type": "number", "default": 5},
        "cooldown_seconds": {"type": "number", "default": 300},
    },
}
latest = {}
armed_at = None
previous_bulk = None
watermark = 0
last_alert_at = None


def on_data(ctx, history):
    global armed_at, previous_bulk, watermark, last_alert_at
    bulk = history.source("btc@trades@bulkf", 0)
    hyperliquid = history.source("btc@trades@hyperliquidf", 0)
    timestamp = ctx.timestamp_ms
    if timestamp is None or timestamp < watermark:
        return
    watermark = timestamp
    current = bulk if ctx.exchange == "bulkf" else hyperliquid
    if current is None:
        return
    latest[ctx.exchange] = (timestamp, current["price"])
    window_ms = ctx.params["window_seconds"] * 1000
    fresh_ms = ctx.params["fresh_seconds"] * 1000
    if window_ms <= 0 or fresh_ms <= 0 or ctx.params["spread_bps"] < 0:
        raise ValueError("Window and freshness must be positive; spread must be non-negative")
    if armed_at is not None and timestamp - armed_at > window_ms:
        armed_at = None
    if ctx.exchange == "bulkf":
        price = current["price"]
        threshold = ctx.params["breakout"]
        if previous_bulk is not None and previous_bulk < threshold <= price:
            armed_at = timestamp
        if price < threshold:
            armed_at = None
        previous_bulk = price
    if armed_at is None or ctx.exchange != "hyperliquidf" or timestamp <= armed_at:
        return
    if any(exchange not in latest or timestamp - latest[exchange][0] > fresh_ms
           for exchange in ("bulkf", "hyperliquidf")):
        armed_at = None
        return
    bulk_price = latest["bulkf"][1]
    hl_price = latest["hyperliquidf"][1]
    if bulk_price <= 0:
        return
    spread = (hl_price / bulk_price - 1) * 10000
    if spread < ctx.params["spread_bps"]:
        return
    cooldown_ms = ctx.params["cooldown_seconds"] * 1000
    if last_alert_at is not None and timestamp - last_alert_at < cooldown_ms:
        armed_at = None
        return
    result = ctx.alert({
        "source": "telegram", "key": "btc-cross-exchange-sequence",
        "message": f"BTC breakout confirmed: Bulk ${bulk_price:,.2f}; Hyperliquid ${hl_price:,.2f} ({spread:.1f} bps higher).",
        "cooldown_seconds": int(ctx.params["cooldown_seconds"]),
    })
    last_alert_at = timestamp
    armed_at = None
    return {"metrics": {"spread_bps": spread, "alert": result["status"]}}
