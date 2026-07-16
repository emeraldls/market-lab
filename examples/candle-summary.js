export const script = {
  name: "candle-summary",
  version: "1",
  sources: ["candles"],
  params: {}
}

function candlesFrom(input) {
  return input.mode === "stream" ? [input.candles.candle] : input.candles.candles
}

export function onData(ctx, input) {
  const candles = candlesFrom(input)
  const latest = candles[candles.length - 1]
  const first = candles[0]
  const high = Math.max(...candles.map((c) => c.h))
  const low = Math.min(...candles.map((c) => c.l))
  const totalVolume = candles.reduce((sum, c) => sum + c.volume, 0)
  const directionalVolume = candles.every(
    (c) => typeof c.vb === "number" && typeof c.vs === "number"
  )
  const buyVolume = directionalVolume
    ? candles.reduce((sum, c) => sum + c.vb, 0)
    : null
  const sellVolume = directionalVolume
    ? candles.reduce((sum, c) => sum + c.vs, 0)
    : null

  return {
    metrics: {
      candles: candles.length,
      first_close: first.c,
      latest_close: latest.c,
      high,
      low,
      total_volume: totalVolume,
      buy_volume: buyVolume,
      sell_volume: sellVolume,
      volume_delta: directionalVolume ? buyVolume - sellVolume : null
    }
  }
}
