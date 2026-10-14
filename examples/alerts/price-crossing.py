script = {
    "name": "btc-price-crossing", "version": "2", "lookback": 2,
    "params": {
        "price": {"type": "number", "required": True},
        "cooldown_seconds": {"type": "number", "default": 60},
    },
}
previous = None


def on_data(ctx, history):
    global previous
    trade = history.source("btc@trades@bulkf", 0)

    if trade is None:
        return

    price = trade["price"]
    threshold = ctx.params["price"]
    crossed = previous is not None and previous < threshold <= price
    previous = price
    
    if crossed:
        return {"metrics": {"alert": ctx.alert({
            "source": "telegram", "key": "btc-price-crossing",
            "message": f"Bulk BTC crossed ${threshold:,.2f}. Last: ${price:,.2f}",
            "cooldown_seconds": int(ctx.params["cooldown_seconds"]),
        })["status"]}}
