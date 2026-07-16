export const script = {
  name: "vd-summary",
  version: "1",
  sources: ["vd"],
  params: {
    vd: {
      min_delta: { type: "number", required: false, default: 0 }
    }
  }
}

export function onData(ctx, input, history) {
  if (input.mode !== "window") {
    throw new Error(`unsupported input.mode: ${input.mode}`)
  }

  const candles = input.vd.candles
  const cvd = ctx.study.cvd(candles, { bucket: input.vd.bucket })
  const latest = candles[candles.length - 1]
  const first = candles[0]
  const delta = cvd.delta
  const triggered = Math.abs(delta) >= ctx.params.vd.min_delta

  return {
    metrics: {
      mode: input.mode,
      candles: candles.length,
      first_open: first.o,
      latest_close: latest.c,
      delta,
      previous_cvd: cvd.previous,
      latest_cvd: cvd.latest,
      trades: candles.reduce((sum, candle) => sum + candle.n, 0),
      min_delta: ctx.params.vd.min_delta
    },
    signal: {
      event: triggered ? "vd_delta" : "no_vd_delta",
      side: delta > 0 ? "buy" : delta < 0 ? "sell" : "neutral",
      triggered,
      reason: triggered ? "VD delta threshold reached" : "VD delta threshold not reached"
    }
  }
}
