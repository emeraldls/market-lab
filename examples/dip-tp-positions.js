export const script = {
  name: "dip-tp-positions",
  version: "1",
  sources: ["candles"],
  lookback: 100,
  params: {
    candles: {
      drop_bps: { type: "number", required: false, default: 20 },
      take_profit_bps: { type: "number", required: false, default: 50 },
      notional: { type: "number", required: false, default: 1000 }
    }
  }
}

export function onData(ctx, input) {
  if (input.mode !== "window") {
    throw new Error("dip-tp-positions only supports script backtest")
  }

  const candles = input.candles.candles
  const latest = candles[candles.length - 1]

  for (const pos of input.positions.open) {
    const target = pos.side === "long"
      ? pos.entry_price * (1 + ctx.params.candles.take_profit_bps / 10000)
      : pos.entry_price * (1 - ctx.params.candles.take_profit_bps / 10000)

    if (pos.side === "long" && latest.c >= target) {
      return {
        metrics: { close: latest.c, target, position_id: pos.id },
        signal: {
          event: "take_profit",
          side: "long",
          triggered: true,
          reason: `target hit for ${pos.id}`
        },
        intent: {
          action: "close",
          position_id: pos.id,
          reason: "take profit hit"
        }
      }
    }
  }

  if (candles.length < 2) {
    return {
      metrics: { close: latest.c, ready: false },
      signal: { event: "warmup", side: "neutral", triggered: false }
    }
  }

  const prev = candles[candles.length - 2]
  const moveBps = ((latest.c - prev.c) / Math.max(Math.abs(prev.c), 1)) * 10000
  const triggered = moveBps <= -ctx.params.candles.drop_bps

  return {
    metrics: {
      close: latest.c,
      prev_close: prev.c,
      move_bps: moveBps,
      open_positions: input.positions.open.length
    },
    signal: {
      event: triggered ? "dip" : "no_dip",
      side: triggered ? "buy" : "neutral",
      triggered,
      reason: triggered ? "dip entry" : "no dip"
    },
    intent: triggered
      ? {
          action: "open",
          side: "long",
          order_type: "market",
          notional: ctx.params.candles.notional,
          reason: "dip entry"
        }
      : {}
  }
}
