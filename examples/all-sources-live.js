export const script = {
  name: "all-sources-live-check",
  version: "1",
  sources: ["candles", "orderbook", "vd"],
  params: {
    candles: {
      min_body_bps: { type: "number", required: false, default: 5 }
    },
    orderbook: {
      min_imbalance: { type: "number", required: false, default: 0.15 },
      max_spread_bps: { type: "number", required: false, default: 5 },
      depth_levels: { type: "number", required: false, default: 10 }
    },
    vd: {
      min_delta: { type: "number", required: false, default: 0 }
    }
  }
}

export function onData(ctx, input) {
  if (input.mode !== "stream") {
    throw new Error("all-sources-live-check expects script run/live mode")
  }

  const candle = input.candles?.candle ?? null
  const book = input.orderbook?.snapshot ?? null
  const vd = input.vd?.candle ?? null

  if (!candle || !book || !vd) {
    return {
      metrics: {
        source: input.source,
        ready: false,
        has_candle: Boolean(candle),
        has_orderbook: Boolean(book),
        has_vd: Boolean(vd)
      },
      signal: {
        event: "warming_up",
        side: "neutral",
        triggered: false,
        reason: "waiting for all subscribed sources"
      }
    }
  }

  const spread = ctx.study.spread(book)
  const imbalance = ctx.study.imbalance(book, {
    depth: Math.trunc(ctx.params.orderbook.depth_levels)
  })
  const bodyBps = candle.o > 0 ? ((candle.c - candle.o) / candle.o) * 10000 : 0
  const cvd = ctx.study.cvd(vd, { bucket: input.vd.bucket })
  const vdDelta = cvd.delta

  const bullish =
    bodyBps >= ctx.params.candles.min_body_bps &&
    imbalance.imbalance >= ctx.params.orderbook.min_imbalance &&
    spread.spread_bps <= ctx.params.orderbook.max_spread_bps &&
    vdDelta >= ctx.params.vd.min_delta

  const bearish =
    bodyBps <= -ctx.params.candles.min_body_bps &&
    imbalance.imbalance <= -ctx.params.orderbook.min_imbalance &&
    spread.spread_bps <= ctx.params.orderbook.max_spread_bps &&
    vdDelta <= -ctx.params.vd.min_delta

  const side = bullish ? "buy" : bearish ? "sell" : "neutral"

  return {
    metrics: {
      source: input.source,
      close: candle.c,
      body_bps: bodyBps,
      spread_bps: spread.spread_bps,
      imbalance: imbalance.imbalance,
      vd_delta: vdDelta,
      latest_cvd: cvd.latest,
      vd_trades: vd.n
    },
    signal: {
      event: bullish ? "bullish_alignment" : bearish ? "bearish_alignment" : "no_alignment",
      side,
      triggered: bullish || bearish,
      reason: bullish || bearish ? "candle, orderbook, and vd aligned" : "source conditions not aligned"
    }
  }
}
