export const script = {
  name: "buy-pressure-filter",
  version: "1",
  sources: ["candles"],
  modes: ["window", "stream"],
  params: {
    candles: {
      min_vbuy: { type: "number", required: false, default: 0 },
      min_delta: { type: "number", required: false, default: 0 }
    }
  }
}

function candlesFrom(input) {
  return input.mode === "stream" ? [input.candles.candle] : input.candles.candles
}

export function onData(ctx, input) {
  const candles = candlesFrom(input)
  const filtered = candles.filter((c) => {
    return c.vb >= ctx.params.candles.min_vbuy && c.vb - c.vs >= ctx.params.candles.min_delta
  })
  const latest = candles[candles.length - 1]

  return {
    metrics: {
      candles: candles.length,
      qualifying_candles: filtered.length,
      latest_close: latest.c,
      latest_delta: latest.vb - latest.vs
    }
  }
}
