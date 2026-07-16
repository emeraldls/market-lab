export const script = {
  name: "cross-exchange-spread",
  version: "1",
  sources: ["candles"],
  modes: ["window", "stream"],
  clock: "candles",
  lookback: 2,
  params: {}
}

const BINANCE = "candles@binancef"
const OKX = "candles@okx"

function latest(input, history, selector) {
  if (input.mode === "stream") return history.source(selector, 0)
  const candles = input.sources[selector]?.candles ?? []
  return candles[candles.length - 1]
}

export function onData(ctx, input, history) {
  const binance = latest(input, history, BINANCE)
  const okx = latest(input, history, OKX)

  if (!binance || !okx) {
    return {
      metrics: {
        ready: false,
        source: input.source ?? input.clock,
      },
    }
  }

  const spread = binance.c - okx.c
  const spreadBps = okx.c === 0 ? 0 : (spread / okx.c) * 10_000

  return {
    metrics: {
      ready: true,
      binance_close: binance.c,
      okx_close: okx.c,
      spread,
      spread_bps: spreadBps,
    },
  }
}
