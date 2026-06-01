export const script = {
  name: "buy-dip-threshold",
  version: "1",
  source: "candles",
  modes: ["window"],
  inputs: {
    drop_bps: { type: "number", required: false, default: 20 },
    notional: { type: "number", required: false, default: 1000 }
  }
}

export function onData(ctx, input) {
  const candles = input.candles
  const latest = candles[candles.length - 1]

  if (candles.length < 2) {
    return {
      metrics: { candles: candles.length, close: latest.c },
      signal: { event: "warmup", side: "neutral", triggered: false }
    }
  }

  const prev = candles[candles.length - 2]
  const moveBps = ((latest.c - prev.c) / Math.max(Math.abs(prev.c), 1)) * 10000
  const triggered = moveBps <= -ctx.inputs.drop_bps

  return {
    metrics: {
      prev_close: prev.c,
      close: latest.c,
      move_bps: moveBps,
      threshold_bps: ctx.inputs.drop_bps
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
          notional: ctx.inputs.notional
        }
      : {}
  }
}
