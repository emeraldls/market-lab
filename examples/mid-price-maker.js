export const script = {
  name: "mid-price-maker",
  version: "1",
  sources: ["orderbook"],
  lookback: 1,
  params: {
    margin: { type: "number", required: false, default: 10 },
    leverage: { type: "number", required: false, default: 1 }
  }
}

const REPRICE_INTERVAL_MS = 1000

let sequence = 0
let latest = null
const quotes = { buy: null, sell: null }

function place(ctx, side, price) {
  const ref = ctx.order({
    key: `mm-${side}-${latest.timestamp}-${++sequence}`,
    side,
    margin: ctx.params.margin,
    leverage: ctx.params.leverage,
    order: { type: "limit", price, tif: "alo" }
  })
  return { id: ref.id, side, price, cancelling: false }
}

export function onData(ctx, input, history) {
  if (input.source !== "orderbook@bulk") return

  const book = history.source("orderbook@bulk", 0)
  const bestBid = book?.bids?.[0]?.price
  const bestAsk = book?.asks?.[0]?.price
  if (!bestBid || !bestAsk || bestBid >= bestAsk) return

  const passiveBid = book.bids[1]?.price ?? bestBid
  const passiveAsk = book.asks[1]?.price ?? bestAsk

  if (latest && book.timestamp_ms - latest.timestamp < REPRICE_INTERVAL_MS) return
  latest = {
    buy: passiveBid,
    sell: passiveAsk,
    timestamp: book.timestamp_ms
  }

  for (const side of ["buy", "sell"]) {
    const quote = quotes[side]
    if (!quote) quotes[side] = place(ctx, side, latest[side])
    else if (!quote.cancelling && quote.price !== latest[side]) {
      quote.cancelling = true
      ctx.cancel({ key: `mm-cancel-${++sequence}`, order: quote.id })
    }
  }
}

export function onExecution(_ctx, event) {
  const side = event.orderId === quotes.buy?.id ? "buy" : event.orderId === quotes.sell?.id ? "sell" : null
  if (!side) return

  if (event.type === "order.cancel_failed" || event.type === "order.cancel_rejected") {
    quotes[side].cancelling = false
    return
  }

  if (!event.terminal) return
  quotes[side] = null
}
