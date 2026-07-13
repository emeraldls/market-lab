export const script = {
  name: "custom-sma-cross",
  version: "1",
  sources: ["candles"],
  lookback: 12,
  params: {
    candles: {
      fast: { type: "number", required: false, default: 4 },
      slow: { type: "number", required: false, default: 11 },
      notional: { type: "number", required: false, default: 1000 }
    }
  }
}

export function onData(ctx, input) {
  const fastSize = Math.trunc(ctx.params.candles.fast)
  const slowSize = Math.trunc(ctx.params.candles.slow)
  const latest = input.candles.candles[input.candles.candles.length - 1]

  if (input.candles.candles.length < slowSize + 1) {
    return {
      metrics: {
        candles: input.candles.candles.length,
        close: latest.c,
        ready: false
      },
      signal: {
        event: "warmup",
        side: "neutral",
        triggered: false,
        reason: "not enough candles"
      }
    }
  }

  const fast = ctx.study.sma(input.candles.candles, { field: "c", window: fastSize })
  const slow = ctx.study.sma(input.candles.candles, { field: "c", window: slowSize })
  const prevFast = fast.previous
  const prevSlow = slow.previous
  const currFast = fast.latest
  const currSlow = slow.latest
  const crossUp = prevFast <= prevSlow && currFast > currSlow
  const crossDown = prevFast >= prevSlow && currFast < currSlow
  const side = crossUp ? "buy" : crossDown ? "sell" : "neutral"
  const triggered = crossUp || crossDown

  return {
    metrics: {
      candles: input.candles.candles.length,
      close: latest.c,
      prev_fast: prevFast,
      prev_slow: prevSlow,
      curr_fast: currFast,
      curr_slow: currSlow
    },
    signal: {
      event: crossUp ? "cross_up" : crossDown ? "cross_down" : "no_cross",
      side,
      triggered,
      reason: triggered ? "SMA crossover" : "no crossover"
    },
    intent: triggered
      ? {
          type: "order",
          side,
          order_type: "market",
          notional: ctx.params.candles.notional
        }
      : {}
  }
}
