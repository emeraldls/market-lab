export const script = {
  name: "sma-crossover",
  version: "1",
  sources: ["candles", "orderbook"],
  clock: "candles",
  lookback: 9,
  params: {
    entry_amount_dollar: { type: "number", required: false, default: 100 },
    max_spread: { type: "number", required: true },
    max_slippage: { type: "number", required: true },
    leverage: { type: "number", required: false, default: 10 }
  }
}

export function onData(ctx, input, history) {
  if (input.source !== "candles@binancef@mmt") return

  const candles = history.source("candles@binancef@mmt")
  const book = history.source("orderbook@bulk", 0)
  if (candles.length < 9 || !book) return

  const fast = ctx.study.sma(candles, { window: 3 })
  const slow = ctx.study.sma(candles, { window: 8 })
  const long = fast.previous <= slow.previous && fast.latest > slow.latest
  const short = fast.previous >= slow.previous && fast.latest < slow.latest

  const side = long ? "long" : short ? "short" : null
  if (!side) return

  const spread = ctx.study.spread(book)
  const slippage = ctx.study.slippage(book, {
    side: long ? "buy" : "sell",
    notional: ctx.params.entry_amount_dollar
  })

  if (
    spread.spread_bps < 0 ||
    spread.spread_bps > ctx.params.max_spread ||
    slippage.slippage_bps > ctx.params.max_slippage
  ) return

  const candle = history.source("candles@binancef@mmt", 0)
  ctx.trade({
    key: `sma-cross-${candle.t}`,
    side,
    notional: ctx.params.entry_amount_dollar,
    order: { type: "market" },
    leverage: ctx.params.leverage
  })
}
