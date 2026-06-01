export const script = {
  name: "custom-sma-cross",
  version: "1",
  source: "candles",
  modes: ["window"],
  inputs: {
    fast: { type: "number", required: false, default: 4 },
    slow: { type: "number", required: false, default: 11 },
    notional: { type: "number", required: false, default: 1000 }
  }
}

function sma(values, size) {
  if (values.length < size) return null
  const slice = values.slice(values.length - size)
  return slice.reduce((sum, value) => sum + value, 0) / size
}

export function onData(ctx, input) {
  const fastSize = Math.trunc(ctx.inputs.fast)
  const slowSize = Math.trunc(ctx.inputs.slow)
  const closes = input.candles.map((c) => c.c)
  const latest = input.candles[input.candles.length - 1]

  if (closes.length < slowSize + 1) {
    return {
      metrics: {
        candles: closes.length,
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

  const prevCloses = closes.slice(0, closes.length - 1)
  const prevFast = sma(prevCloses, fastSize)
  const prevSlow = sma(prevCloses, slowSize)
  const currFast = sma(closes, fastSize)
  const currSlow = sma(closes, slowSize)
  const crossUp = prevFast <= prevSlow && currFast > currSlow
  const crossDown = prevFast >= prevSlow && currFast < currSlow
  const side = crossUp ? "buy" : crossDown ? "sell" : "neutral"
  const triggered = crossUp || crossDown

  return {
    metrics: {
      candles: closes.length,
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
          notional: ctx.inputs.notional
        }
      : {}
  }
}
