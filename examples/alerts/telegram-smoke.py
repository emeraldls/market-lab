script = {"name": "telegram-smoke", "version": "2", "lookback": 2}
sent = False


def on_data(ctx, history):
    global sent
    trade = history.source("btc@trades@bulkf", 0)
    if sent or trade is None:
        return
    result = ctx.alert({
        "source": "telegram",
        "key": "feed-connected",
        "message": f"Bulk BTC feed connected. Price: ${trade['price']:,.2f}",
        "cooldown_seconds": 3600,
    })
    sent = True
    return {"metrics": {"alert": result["status"]}}
