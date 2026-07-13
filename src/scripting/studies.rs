use anyhow::{Context as AnyhowContext, Result, bail};
use rquickjs::{Ctx, Function, Object};
use serde_json::{Value, json};

use crate::domain::enums::Side;
use crate::domain::types::{OrderBookSnapshot, VdCandle};
use crate::functions;

pub fn attach_study_helpers<'js>(ctx: Ctx<'js>, script_ctx: &Object<'js>) -> Result<()> {
    let native = Function::new(
        ctx.clone(),
        |name: String, input: String, options: String| native_study_call(&name, &input, &options),
    )
    .context("failed to create native study function")?;
    ctx.globals()
        .set("__mlab_study", native)
        .context("failed to expose native study function")?;

    let study_helpers: Object = ctx
        .eval(STUDY_HELPERS_JS)
        .context("failed to create ctx.study helpers")?;
    script_ctx
        .set("study", study_helpers)
        .context("failed to assign ctx.study")?;
    Ok(())
}

fn native_study_call(name: &str, input: &str, options: &str) -> String {
    let result = calculate(name, input, options);
    let response = match result {
        Ok(value) => json!({ "ok": true, "value": value }),
        Err(err) => json!({ "ok": false, "error": err.to_string() }),
    };
    serde_json::to_string(&response).expect("native study response must serialize")
}

fn calculate(name: &str, input: &str, options: &str) -> Result<Value> {
    let input: Value = serde_json::from_str(input).context("study input must be valid JSON")?;
    let options: Value =
        serde_json::from_str(options).context("study options must be valid JSON")?;

    match name {
        "sma" | "ema" => {
            let field = option_str(&options, "field").unwrap_or("c");
            let window = option_usize(&options, "window")?;
            let values = series_values(&input, field)?;
            let result = if name == "sma" {
                functions::sma(&values, window)?
            } else {
                functions::ema(&values, window)?
            };
            Ok(serde_json::to_value(result)?)
        }
        "cvd" => {
            let bucket = option_u8(&options, "bucket")?;
            let candles = vd_candles(input)?;
            Ok(serde_json::to_value(functions::cvd(&candles, bucket)?)?)
        }
        "spread" => Ok(serde_json::to_value(functions::spread(&book(input)?)?)?),
        "depth" => {
            let book = book(input)?;
            let levels = option_u16_alias(
                &options,
                &["levels", "depth"],
                book.bids.len().max(book.asks.len()),
            )?;
            Ok(serde_json::to_value(functions::depth(&book, levels)?)?)
        }
        "imbalance" => {
            let book = book(input)?;
            let levels = option_u16_alias(
                &options,
                &["depth", "levels"],
                book.bids.len().max(book.asks.len()),
            )?;
            Ok(serde_json::to_value(functions::imbalance(&book, levels)?)?)
        }
        "slippage" => {
            let side = match option_str(&options, "side") {
                Some("buy") => Side::Buy,
                Some("sell") => Side::Sell,
                _ => bail!("side must be buy or sell"),
            };
            let notional = option_f64(&options, "notional")?;
            Ok(serde_json::to_value(functions::slippage(
                &book(input)?,
                notional,
                side,
            )?)?)
        }
        "vamp" => {
            let dollar_depth = option_f64_alias(&options, &["dollar_depth", "notional"])?;
            Ok(serde_json::to_value(functions::vamp(
                &book(input)?,
                dollar_depth,
            )?)?)
        }
        _ => bail!("unknown study function `{name}`"),
    }
}

fn book(value: Value) -> Result<OrderBookSnapshot> {
    serde_json::from_value(value).context("book must be a valid orderbook snapshot")
}

fn vd_candles(value: Value) -> Result<Vec<VdCandle>> {
    if value.is_array() {
        serde_json::from_value(value).context("cvd rows must be MMT VD candles with {t,o,h,l,c,n}")
    } else {
        serde_json::from_value(value)
            .map(|candle| vec![candle])
            .context("cvd row must be an MMT VD candle with {t,o,h,l,c,n}")
    }
}

fn series_values(rows: &Value, field: &str) -> Result<Vec<f64>> {
    let rows = rows.as_array().context("rows must be an array")?;
    rows.iter()
        .enumerate()
        .map(|(index, row)| {
            row.get(field)
                .and_then(Value::as_f64)
                .filter(|value| value.is_finite())
                .with_context(|| format!("field {field} at index {index} must be a finite number"))
        })
        .collect()
}

fn option_str<'a>(options: &'a Value, name: &str) -> Option<&'a str> {
    options.get(name).and_then(Value::as_str)
}

fn option_usize(options: &Value, name: &str) -> Result<usize> {
    let value = options
        .get(name)
        .and_then(Value::as_u64)
        .with_context(|| format!("{name} must be a positive integer"))?;
    usize::try_from(value).context("window is too large")
}

fn option_u8(options: &Value, name: &str) -> Result<u8> {
    let value = options
        .get(name)
        .and_then(Value::as_u64)
        .with_context(|| format!("{name} must be an integer"))?;
    u8::try_from(value).with_context(|| format!("{name} is too large"))
}

fn option_f64(options: &Value, name: &str) -> Result<f64> {
    options
        .get(name)
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite())
        .with_context(|| format!("{name} must be a finite number"))
}

fn option_f64_alias(options: &Value, names: &[&str]) -> Result<f64> {
    names
        .iter()
        .find_map(|name| options.get(name).and_then(Value::as_f64))
        .filter(|value| value.is_finite())
        .with_context(|| format!("{} must be a finite number", names.join(" or ")))
}

fn option_u16_alias(options: &Value, names: &[&str], default: usize) -> Result<u16> {
    let value = names
        .iter()
        .find_map(|name| options.get(name).and_then(Value::as_u64))
        .unwrap_or(default as u64);
    if value == 0 {
        bail!("{} must be a positive integer", names.join(" or "));
    }
    u16::try_from(value).context("requested depth is too large")
}

const STUDY_HELPERS_JS: &str = r#"
(() => {
  const call = (name, input, options = {}) => {
    const response = JSON.parse(__mlab_study(name, JSON.stringify(input), JSON.stringify(options)));
    if (!response.ok) throw new Error(response.error);
    return response.value;
  };

  return Object.freeze({
    sma: (rows, options) => call("sma", rows, options),
    ema: (rows, options) => call("ema", rows, options),
    cvd: (rows, options) => call("cvd", rows, options),
    spread: (book) => call("spread", book),
    depth: (book, options = {}) => call("depth", book, options),
    imbalance: (book, options = {}) => call("imbalance", book, options),
    slippage: (book, options) => call("slippage", book, options),
    vamp: (book, options) => call("vamp", book, options)
  });
})()
"#;
