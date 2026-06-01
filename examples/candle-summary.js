export const script = {
  name: "candle-summary",
  version: "1",
  source: "candles",
  modes: ["window", "stream"],
  inputs: {}
}

function candlesFrom(input) {
  return input.mode === "stream" ? [input.candle] : input.candles
}

export function onData(ctx, input) {
  const candles = candlesFrom(input)
  const latest = candles[candles.length - 1]
  const first = candles[0]
  const high = Math.max(...candles.map((c) => c.h))
  const low = Math.min(...candles.map((c) => c.l))
  const buyVolume = candles.reduce((sum, c) => sum + c.vb, 0)
  const sellVolume = candles.reduce((sum, c) => sum + c.vs, 0)

  return {
    metrics: {
      candles: candles.length,
      first_close: first.c,
      latest_close: latest.c,
      high,
      low,
      buy_volume: buyVolume,
      sell_volume: sellVolume,
      volume_delta: buyVolume - sellVolume
    }
  }
}
