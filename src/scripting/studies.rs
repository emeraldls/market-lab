use anyhow::{Context as AnyhowContext, Result};
use rquickjs::{Ctx, Object};

pub fn attach_study_helpers<'js>(ctx: Ctx<'js>, script_ctx: &Object<'js>) -> Result<()> {
    let study_helpers: Object = ctx
        .eval(STUDY_HELPERS_JS)
        .context("failed to create ctx.study helpers")?;
    script_ctx
        .set("study", study_helpers)
        .context("failed to assign ctx.study")?;
    Ok(())
}

const STUDY_HELPERS_JS: &str = r#"
(() => {
  const assertArray = (value, name) => {
    if (!Array.isArray(value)) throw new Error(`${name} must be an array`);
  };

  const assertObject = (value, name) => {
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      throw new Error(`${name} must be an object`);
    }
  };

  const assertWindow = (window) => {
    if (!Number.isInteger(window) || window <= 0) {
      throw new Error("window must be a positive integer");
    }
  };

  const numberField = (row, field, idx) => {
    const value = row?.[field];
    if (typeof value !== "number" || !Number.isFinite(value)) {
      throw new Error(`field ${field} at index ${idx} must be a finite number`);
    }
    return value;
  };

  const valuesFrom = (rows, opts) => {
    assertArray(rows, "rows");
    assertObject(opts, "options");
    const field = opts.field || "c";
    if (typeof field !== "string" || field.length === 0) {
      throw new Error("field must be a non-empty string");
    }
    return rows.map((row, idx) => numberField(row, field, idx));
  };

  const compact = (points) => ({
    latest: points.length > 0 ? points[points.length - 1] : null,
    previous: points.length > 1 ? points[points.length - 2] : null,
    points
  });

  const sma = (rows, opts) => {
    const values = valuesFrom(rows, opts);
    const window = opts.window;
    assertWindow(window);
    const points = values.map((_, idx) => {
      if (idx + 1 < window) return null;
      let sum = 0;
      for (let i = idx + 1 - window; i <= idx; i += 1) sum += values[i];
      return sum / window;
    });
    return compact(points);
  };

  const ema = (rows, opts) => {
    const values = valuesFrom(rows, opts);
    const window = opts.window;
    assertWindow(window);
    const alpha = 2 / (window + 1);
    const points = [];
    let prev = null;
    for (let idx = 0; idx < values.length; idx += 1) {
      const value = values[idx];
      if (idx + 1 < window) {
        points.push(null);
        continue;
      }
      if (prev === null) {
        let seed = 0;
        for (let i = idx + 1 - window; i <= idx; i += 1) seed += values[i];
        prev = seed / window;
      } else {
        prev = (value - prev) * alpha + prev;
      }
      points.push(prev);
    }
    return compact(points);
  };

  const cvd = (candles) => {
    assertArray(candles, "candles");
    const points = [];
    let cumulative = 0;
    for (let idx = 0; idx < candles.length; idx += 1) {
      const candle = candles[idx];
      const buy = numberField(candle, "vb", idx);
      const sell = numberField(candle, "vs", idx);
      const delta = buy - sell;
      cumulative += delta;
      points.push({
        t: candle.t ?? null,
        delta,
        cumulative
      });
    }
    return {
      latest: points.length > 0 ? points[points.length - 1].cumulative : null,
      previous: points.length > 1 ? points[points.length - 2].cumulative : null,
      delta: cumulative,
      points
    };
  };

  const bookSides = (book) => {
    assertObject(book, "book");
    assertArray(book.bids, "book.bids");
    assertArray(book.asks, "book.asks");
    return { bids: book.bids, asks: book.asks };
  };

  const levelPrice = (level, idx, side) => numberField(level, "price", `${side}.${idx}`);
  const levelQuantity = (level, idx, side) => numberField(level, "quantity", `${side}.${idx}`);

  const positiveInt = (value, name) => {
    if (!Number.isInteger(value) || value <= 0) {
      throw new Error(`${name} must be a positive integer`);
    }
  };

  const positiveNumber = (value, name) => {
    if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) {
      throw new Error(`${name} must be a positive number`);
    }
  };

  const spread = (book) => {
    const { bids, asks } = bookSides(book);
    if (bids.length === 0) throw new Error("book.bids is empty");
    if (asks.length === 0) throw new Error("book.asks is empty");
    const best_bid = levelPrice(bids[0], 0, "bid");
    const best_ask = levelPrice(asks[0], 0, "ask");
    const spread_abs = best_ask - best_bid;
    const mid = (best_ask + best_bid) / 2;
    const spread_bps = mid > 0 ? (spread_abs / mid) * 10000 : 0;
    return { best_bid, best_ask, spread_abs, spread_bps, mid };
  };

  const depth = (book, opts = {}) => {
    const { bids, asks } = bookSides(book);
    const levels = opts.levels ?? opts.depth ?? Math.max(bids.length, asks.length);
    positiveInt(levels, "levels");
    let bid_base = 0;
    let ask_base = 0;
    let bid_quote = 0;
    let ask_quote = 0;
    for (let idx = 0; idx < Math.min(levels, bids.length); idx += 1) {
      const price = levelPrice(bids[idx], idx, "bid");
      const quantity = levelQuantity(bids[idx], idx, "bid");
      bid_base += quantity;
      bid_quote += price * quantity;
    }
    for (let idx = 0; idx < Math.min(levels, asks.length); idx += 1) {
      const price = levelPrice(asks[idx], idx, "ask");
      const quantity = levelQuantity(asks[idx], idx, "ask");
      ask_base += quantity;
      ask_quote += price * quantity;
    }
    return {
      bid_base,
      ask_base,
      bid_quote,
      ask_quote,
      total_quote: bid_quote + ask_quote
    };
  };

  const imbalance = (book, opts = {}) => {
    const { bids, asks } = bookSides(book);
    const depthValue = opts.depth ?? opts.levels ?? Math.max(bids.length, asks.length);
    positiveInt(depthValue, "depth");
    let bid_volume = 0;
    let ask_volume = 0;
    for (let idx = 0; idx < Math.min(depthValue, bids.length); idx += 1) {
      bid_volume += levelQuantity(bids[idx], idx, "bid");
    }
    for (let idx = 0; idx < Math.min(depthValue, asks.length); idx += 1) {
      ask_volume += levelQuantity(asks[idx], idx, "ask");
    }
    const denom = bid_volume + ask_volume;
    if (denom <= 0) throw new Error("empty book volumes at requested depth");
    return {
      bid_volume,
      ask_volume,
      imbalance: (bid_volume - ask_volume) / denom
    };
  };

  const sideLevels = (book, side) => {
    const { bids, asks } = bookSides(book);
    if (side === "buy") return asks;
    if (side === "sell") return bids;
    throw new Error("side must be buy or sell");
  };

  const slippage = (book, opts) => {
    assertObject(opts, "options");
    const side = opts.side;
    const notional = opts.notional;
    positiveNumber(notional, "notional");
    const levels = sideLevels(book, side);
    if (levels.length === 0) throw new Error("orderbook side is empty");

    const best_price = levelPrice(levels[0], 0, side);
    let remaining = notional;
    let total_base = 0;
    let total_quote = 0;
    let levels_consumed = 0;
    for (let idx = 0; idx < levels.length && remaining > 0; idx += 1) {
      const price = levelPrice(levels[idx], idx, side);
      const quantity = levelQuantity(levels[idx], idx, side);
      const capacity = price * quantity;
      const take_quote = Math.min(remaining, capacity);
      total_quote += take_quote;
      total_base += take_quote / price;
      remaining -= take_quote;
      levels_consumed += 1;
    }
    if (remaining > 0) throw new Error(`insufficient depth to fill notional=${notional}`);
    if (total_base <= 0) throw new Error("computed base fill is zero");

    const avg_fill_price = total_quote / total_base;
    const slippage_abs = side === "buy" ? avg_fill_price - best_price : best_price - avg_fill_price;
    const slippage_bps = best_price > 0 ? (slippage_abs / best_price) * 10000 : 0;
    return {
      avg_fill_price,
      best_price,
      slippage_abs,
      slippage_bps,
      levels_consumed
    };
  };

  const sideVwap = (levels, targetQuote, side) => {
    if (levels.length === 0) throw new Error("orderbook side is empty");
    let remaining = targetQuote;
    let total_quote = 0;
    let total_base = 0;
    let levels_consumed = 0;
    for (let idx = 0; idx < levels.length && remaining > 0; idx += 1) {
      const price = levelPrice(levels[idx], idx, side);
      const quantity = levelQuantity(levels[idx], idx, side);
      const capacity = price * quantity;
      const take_quote = Math.min(remaining, capacity);
      total_quote += take_quote;
      total_base += take_quote / price;
      remaining -= take_quote;
      levels_consumed += 1;
    }
    if (total_base <= 0) throw new Error("computed base fill is zero");
    return {
      vwap: total_quote / total_base,
      levels_consumed,
      filled_quote: total_quote
    };
  };

  const quoteCapacity = (levels, side) => {
    let total = 0;
    for (let idx = 0; idx < levels.length; idx += 1) {
      total += levelPrice(levels[idx], idx, side) * levelQuantity(levels[idx], idx, side);
    }
    return total;
  };

  const vamp = (book, opts) => {
    assertObject(opts, "options");
    const dollar_depth = opts.dollar_depth ?? opts.notional;
    positiveNumber(dollar_depth, "dollar_depth");
    const { bids, asks } = bookSides(book);
    const askFill = sideVwap(asks, dollar_depth, "ask");
    const bidFill = sideVwap(bids, dollar_depth, "bid");
    const ask_vwap = askFill.vwap;
    const bid_vwap = bidFill.vwap;
    return {
      ask_vwap,
      bid_vwap,
      vamp: (ask_vwap + bid_vwap) / 2,
      ask_levels_consumed: askFill.levels_consumed,
      bid_levels_consumed: bidFill.levels_consumed,
      max_reachable_quote_ask: quoteCapacity(asks, "ask"),
      max_reachable_quote_bid: quoteCapacity(bids, "bid"),
      complete: askFill.filled_quote >= dollar_depth && bidFill.filled_quote >= dollar_depth
    };
  };

  return Object.freeze({
    sma,
    ema,
    cvd,
    spread,
    depth,
    imbalance,
    slippage,
    vamp
  });
})()
"#;
