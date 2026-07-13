export const script = {
  name: "buy-dip-threshold",
  version: "1",
  sources: ["candles"],
  params: {
    candles: {
      drop_bps: { type: "number", required: false, default: 20 },
      notional: { type: "number", required: false, default: 1000 }
    }
  }
}

let previousStreamCandle = null

function candlesForMode(input) {
  if (input.mode === "window") {
    return input.candles.candles
  }

  if (input.mode === "stream") {
    const latest = input.candles.candle
    const candles = previousStreamCandle ? [previousStreamCandle, latest] : [latest]
    previousStreamCandle = latest
    return candles
  }

  throw new Error(`unsupported input.mode: ${input.mode}`)
}

export function onData(ctx, input) {
  const candles = candlesForMode(input)
  const latest = candles[candles.length - 1]

  if (candles.length < 2) {
    return {
      metrics: {
        mode: input.mode,
        candles: candles.length,
        close: latest.c,
        threshold_bps: ctx.params.candles.drop_bps
      },
      signal: {
        event: "warmup",
        side: "neutral",
        triggered: false,
        reason: input.mode === "stream" ? "waiting for previous candle" : "not enough candles"
      }
    }
  }

  const prev = candles[candles.length - 2]
  const moveBps = ((latest.c - prev.c) / Math.max(Math.abs(prev.c), 1)) * 10000
  const triggered = moveBps <= -ctx.params.candles.drop_bps

  return {
    metrics: {
      mode: input.mode,
      prev_close: prev.c,
      close: latest.c,
      move_bps: moveBps,
      threshold_bps: ctx.params.candles.drop_bps
    },
    signal: {
      event: triggered ? "dip" : "no_dip",
      side: triggered ? "buy" : "neutral",
      triggered,
      reason: triggered ? "close dropped below threshold" : "drop threshold not reached"
    },
    intent: triggered
      ? {
        type: "order",
        side: "buy",
        order_type: "market",
        notional: ctx.params.candles.notional
      }
      : {}
  }
}
