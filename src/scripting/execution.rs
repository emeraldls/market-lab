use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, bail};
use rquickjs::{Ctx, Function, Object};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::domain::execution::{OrderKind, PositionDirection, TimeInForce};

const MAX_EXECUTION_KEY_BYTES: usize = 128;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ScriptOrderKind {
    Market,
    Limit,
}

impl From<ScriptOrderKind> for OrderKind {
    fn from(value: ScriptOrderKind) -> Self {
        match value {
            ScriptOrderKind::Market => Self::Market,
            ScriptOrderKind::Limit => Self::Limit,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ScriptTimeInForce {
    Gtc,
    Ioc,
    Alo,
}

impl From<ScriptTimeInForce> for TimeInForce {
    fn from(value: ScriptTimeInForce) -> Self {
        match value {
            ScriptTimeInForce::Gtc => Self::Gtc,
            ScriptTimeInForce::Ioc => Self::Ioc,
            ScriptTimeInForce::Alo => Self::Alo,
        }
    }
}

fn default_order_kind() -> ScriptOrderKind {
    ScriptOrderKind::Market
}

fn default_tif() -> ScriptTimeInForce {
    ScriptTimeInForce::Gtc
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ScriptOrderRequest {
    #[serde(rename = "type", default = "default_order_kind")]
    pub kind: ScriptOrderKind,
    #[serde(default)]
    pub price: Option<f64>,
    #[serde(default = "default_tif")]
    pub tif: ScriptTimeInForce,
}

impl Default for ScriptOrderRequest {
    fn default() -> Self {
        Self {
            kind: ScriptOrderKind::Market,
            price: None,
            tif: ScriptTimeInForce::Gtc,
        }
    }
}

fn default_leverage() -> f64 {
    1.0
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ScriptTradeRequest {
    pub key: String,
    pub side: PositionDirection,
    #[serde(default)]
    pub size: Option<f64>,
    #[serde(default)]
    pub notional: Option<f64>,
    #[serde(default = "default_leverage")]
    pub leverage: f64,
    #[serde(default)]
    pub order: ScriptOrderRequest,
    #[serde(default)]
    pub reduce_only: bool,
    #[serde(default, alias = "stopLoss", alias = "stop_loss")]
    pub sl: Option<f64>,
    #[serde(default, alias = "takeProfit", alias = "take_profit")]
    pub tp: Option<f64>,
}

impl ScriptTradeRequest {
    pub fn validate(&self) -> Result<()> {
        validate_key(&self.key)?;
        match (self.size, self.notional) {
            (Some(_), Some(_)) => bail!("ctx.trade requires only one of size or notional"),
            (None, None) => bail!("ctx.trade requires one of size or notional"),
            _ => {}
        }
        if let Some(size) = self.size
            && (!size.is_finite() || size <= 0.0)
        {
            bail!("ctx.trade size must be > 0");
        }
        if let Some(notional) = self.notional
            && (!notional.is_finite() || notional <= 0.0)
        {
            bail!("ctx.trade notional must be > 0");
        }
        if !self.leverage.is_finite() || self.leverage < 1.0 {
            bail!("ctx.trade leverage must be at least 1");
        }
        match self.order.kind {
            ScriptOrderKind::Market if self.order.price.is_some() => {
                bail!("ctx.trade order.price is only valid for a limit order")
            }
            ScriptOrderKind::Market if self.order.tif != ScriptTimeInForce::Gtc => {
                bail!("ctx.trade order.tif is only valid for a limit order")
            }
            ScriptOrderKind::Limit => {
                let price = self
                    .order
                    .price
                    .context("ctx.trade limit orders require order.price")?;
                validate_price("ctx.trade order.price", price)?;
            }
            ScriptOrderKind::Market => {}
        }
        if let Some(price) = self.sl {
            validate_price("ctx.trade sl", price)?;
        }
        if let Some(price) = self.tp {
            validate_price("ctx.trade tp", price)?;
        }
        if self.sl.is_some() || self.tp.is_some() {
            if self.reduce_only {
                bail!("ctx.trade sl/tp cannot be attached to a reduce-only order");
            }
            if self.sl == self.tp {
                bail!("ctx.trade sl and tp must use different prices");
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ScriptCancelRequest {
    pub key: String,
    pub order: String,
}

impl ScriptCancelRequest {
    pub fn validate(&self) -> Result<()> {
        validate_key(&self.key)?;
        if self.order.trim().is_empty() {
            bail!("ctx.cancel order is required");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScriptOrderRef {
    pub id: String,
    pub key: String,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ScriptExecutionCommand {
    Trade {
        order: ScriptOrderRef,
        request: ScriptTradeRequest,
    },
    Cancel {
        request: ScriptCancelRequest,
    },
}

#[derive(Clone, Debug)]
pub struct ScriptExecutionContext {
    pub job_id: String,
    pub enabled: bool,
}

impl ScriptExecutionContext {
    pub fn disabled() -> Self {
        Self {
            job_id: "analysis".to_string(),
            enabled: false,
        }
    }
}

pub type ScriptCommandBuffer = Arc<Mutex<Vec<ScriptExecutionCommand>>>;

pub fn attach_execution_helpers<'js>(
    ctx: Ctx<'js>,
    script_ctx: &Object<'js>,
    execution: &ScriptExecutionContext,
    commands: &ScriptCommandBuffer,
) -> Result<()> {
    let job_id = execution.job_id.clone();
    let enabled = execution.enabled;
    let commands = Arc::clone(commands);
    let native = Function::new(ctx.clone(), move |operation: String, payload: String| {
        native_execution_call(&job_id, enabled, &commands, &operation, &payload)
    })
    .context("failed to create native execution function")?;
    ctx.globals()
        .set("__mlab_execution", native)
        .context("failed to expose native execution function")?;
    let helpers: Object = ctx
        .eval(EXECUTION_HELPERS_JS)
        .context("failed to create ctx execution helpers")?;
    let trade: Function = helpers.get("trade").context("failed to create ctx.trade")?;
    let cancel: Function = helpers
        .get("cancel")
        .context("failed to create ctx.cancel")?;
    script_ctx
        .set("trade", trade)
        .context("failed to assign ctx.trade")?;
    script_ctx
        .set("cancel", cancel)
        .context("failed to assign ctx.cancel")?;
    Ok(())
}

fn native_execution_call(
    job_id: &str,
    enabled: bool,
    commands: &ScriptCommandBuffer,
    operation: &str,
    payload: &str,
) -> String {
    let result = (|| -> Result<serde_json::Value> {
        if !enabled {
            bail!("script execution is disabled; deploy the script with --venue");
        }
        match operation {
            "trade" => {
                let request: ScriptTradeRequest = serde_json::from_str(payload)
                    .context("ctx.trade request must be valid JSON")?;
                request.validate()?;
                let order = ScriptOrderRef {
                    id: local_order_id(job_id, &request.key),
                    key: request.key.clone(),
                };
                commands
                    .lock()
                    .map_err(|_| anyhow::anyhow!("script execution queue lock poisoned"))?
                    .push(ScriptExecutionCommand::Trade {
                        order: order.clone(),
                        request,
                    });
                Ok(serde_json::to_value(order)?)
            }
            "cancel" => {
                let request: ScriptCancelRequest = serde_json::from_str(payload)
                    .context("ctx.cancel request must be valid JSON")?;
                request.validate()?;
                let response = json!({
                    "key": request.key,
                    "order": request.order,
                    "status": "queued"
                });
                commands
                    .lock()
                    .map_err(|_| anyhow::anyhow!("script execution queue lock poisoned"))?
                    .push(ScriptExecutionCommand::Cancel { request });
                Ok(response)
            }
            _ => bail!("unknown script execution operation `{operation}`"),
        }
    })();
    let response = match result {
        Ok(value) => json!({ "ok": true, "value": value }),
        Err(error) => json!({ "ok": false, "error": format!("{error:#}") }),
    };
    serde_json::to_string(&response).expect("native execution response must serialize")
}

pub fn local_order_id(job_id: &str, key: &str) -> String {
    // Stable FNV-1a is sufficient here: this is a local reference, not a signature.
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in job_id.bytes().chain([0]).chain(key.bytes()) {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("ord_{hash:016x}")
}

fn validate_key(key: &str) -> Result<()> {
    if key.trim().is_empty() {
        bail!("script execution key is required");
    }
    if key.len() > MAX_EXECUTION_KEY_BYTES {
        bail!("script execution key must be at most {MAX_EXECUTION_KEY_BYTES} bytes");
    }
    Ok(())
}

fn validate_price(field: &str, price: f64) -> Result<()> {
    if !price.is_finite() || price <= 0.0 {
        bail!("{field} must be > 0");
    }
    Ok(())
}

const EXECUTION_HELPERS_JS: &str = r#"
(() => {
  function call(operation, request) {
    if (!request || typeof request !== "object" || Array.isArray(request)) {
      throw new TypeError(`ctx.${operation} requires an object`);
    }
    const response = JSON.parse(__mlab_execution(operation, JSON.stringify(request)));
    if (!response.ok) throw new Error(response.error);
    return response.value;
  }
  return Object.freeze({
    trade(request) { return call("trade", request); },
    cancel(request) { return call("cancel", request); }
  });
})()
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_limit_trade_with_protection() {
        let request: ScriptTradeRequest = serde_json::from_value(json!({
            "key": "entry-1",
            "side": "long",
            "notional": 100,
            "leverage": 5,
            "order": { "type": "limit", "price": 64000, "tif": "gtc" },
            "sl": 63000,
            "tp": 67000
        }))
        .expect("decode request");
        request.validate().expect("valid request");
        assert_eq!(request.order.kind, ScriptOrderKind::Limit);
    }

    #[test]
    fn local_order_reference_is_stable() {
        assert_eq!(
            local_order_id("job_one", "entry"),
            local_order_id("job_one", "entry")
        );
        assert_ne!(
            local_order_id("job_one", "entry"),
            local_order_id("job_one", "exit")
        );
    }
}
