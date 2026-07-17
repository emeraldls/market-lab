export const script = {
  name: "oi-volumes-check",
  version: "1",
  sources: ["oi", "volumes"],
  clock: "oi",
  params: {
    oi: {
      min_change: { type: "number", required: false, default: 0 }
    },
    volumes: {
      min_total_volume: { type: "number", required: false, default: 0 }
    }
  }
}

function latestFor(input, history, source) {
  if (input.mode === "stream") return history.source(source, 0) ?? null
  const rows = source === "volumes" ? input.volumes.profiles : input.oi.candles
  return rows[rows.length - 1]
}

function volumeStats(profile) {
  if (!profile) return { total_volume: 0, poc: null, buy_volume: 0, sell_volume: 0 }
  let totalVolume = 0
  let buyVolume = 0
  let sellVolume = 0
  let poc = null
  let pocVolume = -Infinity

  for (let idx = 0; idx < profile.p.length; idx += 1) {
    const buy = profile.b[idx] ?? 0
    const sell = profile.s[idx] ?? 0
    const total = buy + sell
    buyVolume += buy
    sellVolume += sell
    totalVolume += total
    if (total > pocVolume) {
      pocVolume = total
      poc = profile.p[idx]
    }
  }

  return { total_volume: totalVolume, poc, buy_volume: buyVolume, sell_volume: sellVolume }
}

export function onData(ctx, input, history) {
  const oi = latestFor(input, history, "oi")
  const profile = latestFor(input, history, "volumes")

  if (!oi || !profile) {
    return {
      metrics: {
        source: input.source ?? "window",
        ready: false,
        has_oi: Boolean(oi),
        has_volumes: Boolean(profile)
      },
      signal: { event: "warming_up", side: "neutral", triggered: false }
    }
  }

  const stats = volumeStats(profile)
  const oiChange = oi.c - oi.o
  const triggered =
    Math.abs(oiChange) >= ctx.params.oi.min_change &&
    stats.total_volume >= ctx.params.volumes.min_total_volume

  return {
    metrics: {
      source: input.source ?? "window",
      oi_close: oi.c,
      oi_change: oiChange,
      volume_poc: stats.poc,
      total_volume: stats.total_volume,
      buy_volume: stats.buy_volume,
      sell_volume: stats.sell_volume
    },
    signal: {
      event: triggered ? "oi_volume_activity" : "quiet",
      side: oiChange > 0 ? "buy" : oiChange < 0 ? "sell" : "neutral",
      triggered,
      reason: triggered ? "OI and volume thresholds reached" : "thresholds not reached"
    }
  }
}
