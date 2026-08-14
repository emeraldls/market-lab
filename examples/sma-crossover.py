import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


SOURCE = "btc@candles@binancef@mmt"

script = {
    "name": "python-sma-crossover",
    "version": "2",
    "sources": ["candles"],
    "lookback": 100,
    "params": {
        "fast_period": {"type": "number", "default": 5},
        "slow_period": {"type": "number", "default": 20},
        "margin": {"type": "number", "default": 100},
    },
}


def on_data(ctx, event, history):
    if event["source"] != SOURCE:
        return

    candles = history.source(SOURCE)
    slow_period = int(ctx.params["slow_period"])
    if len(candles) < slow_period + 1:
        return

    closes = pd.Series([candle["c"] for candle in candles])
    fast = closes.rolling(int(ctx.params["fast_period"])).mean()
    slow = closes.rolling(slow_period).mean()
    previous = np.sign(fast.iloc[-2] - slow.iloc[-2])
    current = np.sign(fast.iloc[-1] - slow.iloc[-1])

    positions = event["positions"]["open"]
    position = positions[0] if positions else None
    timestamp = candles[-1]["t"]

    if previous <= 0 and current > 0 and position is None:
        ctx.trade(
            {
                "key": f"sma-buy-{timestamp}",
                "symbol": "BTC",
                "position": "open-long",
                "margin": ctx.params["margin"],
                "order": {"type": "market"},
            }
        )
    elif previous >= 0 and current < 0 and position:
        ctx.trade(
            {
                "key": f"sma-sell-{timestamp}",
                "symbol": "BTC",
                "position": "close-long",
                "order": {"type": "market"},
            }
        )

    return {"metrics": {"fast_sma": fast.iloc[-1], "slow_sma": slow.iloc[-1]}}


def on_finish(ctx, history):
    candles = pd.DataFrame(history.source(SOURCE))
    if candles.empty:
        return

    candles["fast_sma"] = candles["c"].rolling(int(ctx.params["fast_period"])).mean()
    candles["slow_sma"] = candles["c"].rolling(int(ctx.params["slow_period"])).mean()

    candles.plot(x="t", y=["c", "fast_sma", "slow_sma"])
    plt.savefig(ctx.artifact_path("sma-crossover.png"))
    plt.close()
