export const script = {
  name: "bulk-limit-protected",
  version: "1",
  sources: ["candles"],
  params: {
    candles: {
      armed: { type: "boolean", required: false, default: false },
      notional: { type: "number", required: false, default: 10 },
      leverage: { type: "number", required: false, default: 2 }
    }
  }
}

let entry = null

function btcTick(price) {
  return Math.round(price * 10) / 10
}

export function onData(ctx, input) {
  const candle = input.candles.candle

  if (!ctx.params.candles.armed || entry) {
    return {
      metrics: {
        armed: ctx.params.candles.armed,
        close: candle.c,
        order_id: entry?.id ?? null
      }
    }
  }

  const limit = btcTick(candle.c * 0.995)
  entry = ctx.trade({
    key: "demo-entry-v1",
    side: "long",
    notional: ctx.params.candles.notional,
    leverage: ctx.params.candles.leverage,
    order: { type: "limit", price: limit, tif: "gtc" },
    sl: btcTick(limit * 0.98),
    tp: btcTick(limit * 1.04)
  })

  return {
    metrics: {
      close: candle.c,
      limit,
      order_id: entry.id
    }
  }
}

export function onExecution(ctx, event) {
  // `event.orderId` is Market Lab's stable ID. `event.venueOrderId` is Bulk's ID.
  // Either `entry.id` or the original key can be passed as `order` to ctx.cancel.
  if (event.orderId === entry?.id && event.type === "order.rejected") {
    entry = null
  }

  return {
    metrics: {
      event: event.type,
      order_id: event.orderId ?? null,
      venue_order_id: event.venueOrderId ?? null,
      status: event.status ?? null,
      terminal: event.terminal
    }
  }
}
