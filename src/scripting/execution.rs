use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, bail};
use rquickjs::{Ctx, Function, Object};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::domain::execution::{ExecutionVenue, OrderKind, PositionDirection, TimeInForce};

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

fn default_account_name() -> String {
    "main".to_string()
}

fn validate_account_name(value: &str, operation: &str) -> Result<()> {
    let value = value.trim();
    if value.is_empty() {
        bail!("{operation} account must be a non-empty string");
    }
    if value.len() > 32 {
        bail!("{operation} account must be at most 32 bytes");
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        bail!("{operation} account may contain only letters, numbers, `-`, and `_`");
    }
    Ok(())
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

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ScriptPositionOperation {
    OpenLong,
    OpenShort,
    CloseLong,
    CloseShort,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ScriptOrderSide {
    #[serde(alias = "long")]
    Buy,
    #[serde(alias = "short")]
    Sell,
}

impl ScriptOrderSide {
    pub fn order_direction(self) -> PositionDirection {
        match self {
            Self::Buy => PositionDirection::Long,
            Self::Sell => PositionDirection::Short,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "buy",
            Self::Sell => "sell",
        }
    }
}

impl ScriptPositionOperation {
    pub fn position_direction(self) -> PositionDirection {
        match self {
            Self::OpenLong | Self::CloseLong => PositionDirection::Long,
            Self::OpenShort | Self::CloseShort => PositionDirection::Short,
        }
    }

    pub fn order_direction(self) -> PositionDirection {
        match self {
            Self::OpenLong | Self::CloseShort => PositionDirection::Long,
            Self::OpenShort | Self::CloseLong => PositionDirection::Short,
        }
    }

    pub fn is_open(self) -> bool {
        matches!(self, Self::OpenLong | Self::OpenShort)
    }

    pub fn is_close(self) -> bool {
        !self.is_open()
    }

    pub fn reduce_only(self) -> bool {
        self.is_close()
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::OpenLong => "open-long",
            Self::OpenShort => "open-short",
            Self::CloseLong => "close-long",
            Self::CloseShort => "close-short",
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ScriptTradeRequest {
    pub key: String,
    pub symbol: String,
    #[serde(default = "default_account_name")]
    pub account: String,
    pub position: ScriptPositionOperation,
    #[serde(default)]
    pub size: Option<f64>,
    #[serde(default)]
    pub margin: Option<f64>,
    #[serde(default)]
    pub leverage: Option<f64>,
    #[serde(default)]
    pub order: ScriptOrderRequest,
    #[serde(default, alias = "stopLoss", alias = "stop_loss")]
    pub sl: Option<f64>,
    #[serde(default, alias = "takeProfit", alias = "take_profit")]
    pub tp: Option<f64>,
    #[serde(default, alias = "max_slippage")]
    pub max_slippage: Option<f64>,
    #[serde(default, alias = "max_slippage_bps")]
    pub max_slippage_bps: Option<f64>,
}

impl ScriptTradeRequest {
    pub fn leverage_or_default(&self) -> f64 {
        self.leverage.unwrap_or(1.0)
    }

    pub fn max_slippage_fraction(&self) -> Option<f64> {
        self.max_slippage
            .or_else(|| self.max_slippage_bps.map(|bps| bps / 10_000.0))
    }

    pub fn validate(&self) -> Result<()> {
        validate_key(&self.key)?;
        validate_account_name(&self.account, "ctx.trade")?;
        crate::scripting::inputs::normalize_script_symbol(&self.symbol)
            .context("ctx.trade symbol is invalid")?;
        if self.position.is_open() {
            match (self.size, self.margin) {
                (Some(_), Some(_)) => bail!("ctx.trade requires only one of size or margin"),
                (None, None) => bail!("ctx.trade opening operations require size or margin"),
                _ => {}
            }
        } else {
            if self.margin.is_some() {
                bail!("ctx.trade closing operations accept size, not margin");
            }
            if self.leverage.is_some() {
                bail!("ctx.trade leverage is only valid for opening operations");
            }
            if self.sl.is_some() || self.tp.is_some() {
                bail!("ctx.trade sl/tp are only valid for opening operations");
            }
        }
        if let Some(size) = self.size
            && (!size.is_finite() || size <= 0.0)
        {
            bail!("ctx.trade size must be > 0");
        }
        if let Some(margin) = self.margin
            && (!margin.is_finite() || margin <= 0.0)
        {
            bail!("ctx.trade margin must be > 0");
        }
        if let Some(leverage) = self.leverage {
            if !leverage.is_finite() || leverage < 1.0 {
                bail!("ctx.trade leverage must be at least 1");
            }
            if self
                .margin
                .is_some_and(|margin| !(margin * leverage).is_finite())
            {
                bail!("ctx.trade margin multiplied by leverage is too large");
            }
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
        if (self.sl.is_some() || self.tp.is_some()) && self.sl == self.tp {
            bail!("ctx.trade sl and tp must use different prices");
        }
        validate_max_slippage(
            "ctx.trade",
            self.order.kind,
            self.max_slippage,
            self.max_slippage_bps,
        )?;
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ScriptRawOrderRequest {
    pub key: String,
    pub symbol: String,
    #[serde(default = "default_account_name")]
    pub account: String,
    pub side: ScriptOrderSide,
    #[serde(default)]
    pub size: Option<f64>,
    #[serde(default)]
    pub margin: Option<f64>,
    #[serde(default)]
    pub leverage: Option<f64>,
    #[serde(default)]
    pub reduce_only: bool,
    #[serde(default)]
    pub order: ScriptOrderRequest,
    #[serde(default, alias = "max_slippage")]
    pub max_slippage: Option<f64>,
    #[serde(default, alias = "max_slippage_bps")]
    pub max_slippage_bps: Option<f64>,
}

impl ScriptRawOrderRequest {
    pub fn leverage_or_default(&self) -> f64 {
        self.leverage.unwrap_or(1.0)
    }

    pub fn max_slippage_fraction(&self) -> Option<f64> {
        self.max_slippage
            .or_else(|| self.max_slippage_bps.map(|bps| bps / 10_000.0))
    }

    pub fn validate(&self) -> Result<()> {
        validate_key(&self.key)?;
        validate_account_name(&self.account, "ctx.order")?;
        crate::scripting::inputs::normalize_script_symbol(&self.symbol)
            .context("ctx.order symbol is invalid")?;
        match (self.size, self.margin) {
            (Some(_), Some(_)) => bail!("ctx.order requires only one of size or margin"),
            (None, None) => bail!("ctx.order requires size or margin"),
            _ => {}
        }
        if let Some(size) = self.size
            && (!size.is_finite() || size <= 0.0)
        {
            bail!("ctx.order size must be > 0");
        }
        if let Some(margin) = self.margin
            && (!margin.is_finite() || margin <= 0.0)
        {
            bail!("ctx.order margin must be > 0");
        }
        if let Some(leverage) = self.leverage {
            if !leverage.is_finite() || leverage < 1.0 {
                bail!("ctx.order leverage must be at least 1");
            }
            if self
                .margin
                .is_some_and(|margin| !(margin * leverage).is_finite())
            {
                bail!("ctx.order margin multiplied by leverage is too large");
            }
        }
        match self.order.kind {
            ScriptOrderKind::Market if self.order.price.is_some() => {
                bail!("ctx.order order.price is only valid for a limit order")
            }
            ScriptOrderKind::Market if self.order.tif != ScriptTimeInForce::Gtc => {
                bail!("ctx.order order.tif is only valid for a limit order")
            }
            ScriptOrderKind::Limit => {
                let price = self
                    .order
                    .price
                    .context("ctx.order limit orders require order.price")?;
                validate_price("ctx.order order.price", price)?;
            }
            ScriptOrderKind::Market => {}
        }
        validate_max_slippage(
            "ctx.order",
            self.order.kind,
            self.max_slippage,
            self.max_slippage_bps,
        )?;
        Ok(())
    }
}

fn validate_max_slippage(
    operation: &str,
    order_kind: ScriptOrderKind,
    max_slippage: Option<f64>,
    max_slippage_bps: Option<f64>,
) -> Result<()> {
    if max_slippage.is_some() && max_slippage_bps.is_some() {
        bail!("{operation} accepts only one of max_slippage or max_slippage_bps");
    }
    if (max_slippage.is_some() || max_slippage_bps.is_some())
        && order_kind != ScriptOrderKind::Market
    {
        bail!("{operation} max slippage is only valid for market orders");
    }
    if let Some(value) = max_slippage
        && (!value.is_finite() || !(0.0..1.0).contains(&value))
    {
        bail!("{operation} max_slippage must be between 0 (inclusive) and 1 (exclusive)");
    }
    if let Some(value) = max_slippage_bps
        && (!value.is_finite() || !(0.0..10_000.0).contains(&value))
    {
        bail!("{operation} max_slippage_bps must be between 0 (inclusive) and 10000 (exclusive)");
    }
    Ok(())
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(untagged)]
pub enum ScriptManagedRequest {
    Trade(ScriptTradeRequest),
    Order(ScriptRawOrderRequest),
}

impl ScriptManagedRequest {
    pub fn key(&self) -> &str {
        match self {
            Self::Trade(request) => &request.key,
            Self::Order(request) => &request.key,
        }
    }

    pub fn order(&self) -> &ScriptOrderRequest {
        match self {
            Self::Trade(request) => &request.order,
            Self::Order(request) => &request.order,
        }
    }

    pub fn symbol(&self) -> &str {
        match self {
            Self::Trade(request) => &request.symbol,
            Self::Order(request) => &request.symbol,
        }
    }

    pub fn account(&self) -> &str {
        match self {
            Self::Trade(request) => &request.account,
            Self::Order(request) => &request.account,
        }
    }

    pub fn order_direction(&self) -> PositionDirection {
        match self {
            Self::Trade(request) => request.position.order_direction(),
            Self::Order(request) => request.side.order_direction(),
        }
    }

    pub fn max_slippage_fraction(&self) -> Option<f64> {
        match self {
            Self::Trade(request) => request.max_slippage_fraction(),
            Self::Order(request) => request.max_slippage_fraction(),
        }
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
        exchange: Option<ExecutionVenue>,
        request: ScriptTradeRequest,
    },
    Order {
        order: ScriptOrderRef,
        exchange: Option<ExecutionVenue>,
        request: ScriptRawOrderRequest,
    },
    Cancel {
        request: ScriptCancelRequest,
    },
}

#[derive(Clone, Debug)]
pub struct ScriptExecutionContext {
    pub job_id: String,
    pub enabled: bool,
    pub request_routed: bool,
}

impl ScriptExecutionContext {
    pub fn disabled() -> Self {
        Self {
            job_id: "analysis".to_string(),
            enabled: false,
            request_routed: false,
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
    let request_routed = execution.request_routed;
    let commands = Arc::clone(commands);
    let native = Function::new(ctx.clone(), move |operation: String, payload: String| {
        native_execution_call(
            &job_id,
            enabled,
            request_routed,
            &commands,
            &operation,
            &payload,
        )
    })
    .context("failed to create native execution function")?;
    ctx.globals()
        .set("__mlab_execution", native)
        .context("failed to expose native execution function")?;
    let helpers: Object = ctx
        .eval(EXECUTION_HELPERS_JS)
        .context("failed to create ctx execution helpers")?;
    let trade: Function = helpers.get("trade").context("failed to create ctx.trade")?;
    let order: Function = helpers.get("order").context("failed to create ctx.order")?;
    let cancel: Function = helpers
        .get("cancel")
        .context("failed to create ctx.cancel")?;
    script_ctx
        .set("trade", trade)
        .context("failed to assign ctx.trade")?;
    script_ctx
        .set("order", order)
        .context("failed to assign ctx.order")?;
    script_ctx
        .set("cancel", cancel)
        .context("failed to assign ctx.cancel")?;
    Ok(())
}

fn native_execution_call(
    job_id: &str,
    enabled: bool,
    request_routed: bool,
    commands: &ScriptCommandBuffer,
    operation: &str,
    payload: &str,
) -> String {
    let result = queue_execution_call(
        job_id,
        enabled,
        request_routed,
        commands,
        operation,
        payload,
    );
    let response = match result {
        Ok(value) => json!({ "ok": true, "value": value }),
        Err(error) => json!({ "ok": false, "error": format!("{error:#}") }),
    };
    serde_json::to_string(&response).expect("native execution response must serialize")
}

pub(crate) fn queue_execution_call(
    job_id: &str,
    enabled: bool,
    request_routed: bool,
    commands: &ScriptCommandBuffer,
    operation: &str,
    payload: &str,
) -> Result<serde_json::Value> {
    if !enabled {
        bail!("script execution is disabled; deploy the script with --venue");
    }
    match operation {
        "trade" => {
            let (exchange, request) = decode_execution_request::<ScriptTradeRequest>(
                payload,
                "ctx.trade",
                request_routed,
            )?;
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
                    exchange,
                    request,
                });
            Ok(serde_json::to_value(order)?)
        }
        "order" => {
            let (exchange, request) = decode_execution_request::<ScriptRawOrderRequest>(
                payload,
                "ctx.order",
                request_routed,
            )?;
            request.validate()?;
            let order = ScriptOrderRef {
                id: local_order_id(job_id, &request.key),
                key: request.key.clone(),
            };
            commands
                .lock()
                .map_err(|_| anyhow::anyhow!("script execution queue lock poisoned"))?
                .push(ScriptExecutionCommand::Order {
                    order: order.clone(),
                    exchange,
                    request,
                });
            Ok(serde_json::to_value(order)?)
        }
        "cancel" => {
            let request: ScriptCancelRequest =
                serde_json::from_str(payload).context("ctx.cancel request must be valid JSON")?;
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
}

fn decode_execution_request<T>(
    payload: &str,
    operation: &str,
    request_routed: bool,
) -> Result<(Option<ExecutionVenue>, T)>
where
    T: for<'de> Deserialize<'de>,
{
    let mut value: serde_json::Value = serde_json::from_str(payload)
        .with_context(|| format!("{operation} request must be valid JSON"))?;
    let exchange =
        if request_routed {
            let object = value
                .as_object_mut()
                .with_context(|| format!("{operation} request must be an object"))?;
            let raw = object
                .remove("exchange")
                .with_context(|| format!("{operation} requires exchange in Python Scripting V2"))?;
            Some(serde_json::from_value(raw).with_context(|| {
                format!("{operation} exchange is not a registered execution venue")
            })?)
        } else {
            None
        };
    let request =
        serde_json::from_value(value).with_context(|| format!("{operation} request is invalid"))?;
    Ok((exchange, request))
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
    order(request) { return call("order", request); },
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
            "symbol": "btc",
            "position": "open-long",
            "margin": 100,
            "leverage": 5,
            "order": { "type": "limit", "price": 64000, "tif": "gtc" },
            "sl": 63000,
            "tp": 67000
        }))
        .expect("decode request");
        request.validate().expect("valid request");
        assert_eq!(request.order.kind, ScriptOrderKind::Limit);
        assert_eq!(request.account, "main");
    }

    #[test]
    fn scripting_market_orders_accept_decimal_or_bps_slippage() {
        let decimal: ScriptTradeRequest = serde_json::from_value(json!({
            "key": "entry-decimal",
            "symbol": "btc",
            "position": "open-long",
            "size": 0.01,
            "max_slippage": 0.0005
        }))
        .expect("decode decimal slippage");
        let bps: ScriptRawOrderRequest = serde_json::from_value(json!({
            "key": "entry-bps",
            "symbol": "btc",
            "side": "buy",
            "size": 0.01,
            "max_slippage_bps": 5
        }))
        .expect("decode bps slippage");

        decimal.validate().expect("decimal slippage validates");
        bps.validate().expect("bps slippage validates");
        assert_eq!(decimal.max_slippage_fraction(), Some(0.0005));
        assert_eq!(bps.max_slippage_fraction(), Some(0.0005));
    }

    #[test]
    fn scripting_market_orders_reject_two_slippage_units() {
        let request: ScriptTradeRequest = serde_json::from_value(json!({
            "key": "entry-ambiguous",
            "symbol": "btc",
            "position": "open-long",
            "size": 0.01,
            "max_slippage": 0.0005,
            "max_slippage_bps": 5
        }))
        .expect("decode request");

        let error = request.validate().expect_err("units must be exclusive");
        assert!(
            error
                .to_string()
                .contains("only one of max_slippage or max_slippage_bps")
        );
    }

    #[test]
    fn limit_orders_reject_market_slippage_controls() {
        let request: ScriptRawOrderRequest = serde_json::from_value(json!({
            "key": "limit-with-slippage",
            "symbol": "btc",
            "side": "buy",
            "size": 0.01,
            "max_slippage_bps": 5,
            "order": { "type": "limit", "price": 64000, "tif": "gtc" }
        }))
        .expect("decode request");

        let error = request
            .validate()
            .expect_err("limit order must define its own price only");
        assert!(error.to_string().contains("only valid for market orders"));
    }

    #[test]
    fn named_account_is_part_of_the_managed_request() {
        let request: ScriptTradeRequest = serde_json::from_value(json!({
            "key": "entry-subaccount",
            "account": "trading-2",
            "symbol": "btc",
            "position": "open-long",
            "margin": 100,
            "leverage": 5
        }))
        .expect("decode request");
        request.validate().expect("named account is valid");
        assert_eq!(request.account, "trading-2");
    }

    #[test]
    fn rejects_legacy_notional_trade_sizing() {
        let error = serde_json::from_value::<ScriptTradeRequest>(json!({
            "key": "entry-1",
            "symbol": "btc",
            "position": "open-long",
            "notional": 100,
            "leverage": 5
        }))
        .expect_err("ctx.trade sizing must use margin or size");

        assert!(error.to_string().contains("notional"));
    }

    #[test]
    fn validates_full_position_close_without_sizing() {
        let request: ScriptTradeRequest = serde_json::from_value(json!({
            "key": "exit-1",
            "symbol": "btc",
            "position": "close-long"
        }))
        .expect("decode close request");

        request.validate().expect("valid close request");
        assert!(request.position.reduce_only());
        assert_eq!(request.position.order_direction(), PositionDirection::Short);
    }

    #[test]
    fn rejects_margin_on_position_close() {
        let request: ScriptTradeRequest = serde_json::from_value(json!({
            "key": "exit-1",
            "symbol": "btc",
            "position": "close-short",
            "margin": 100
        }))
        .expect("decode close request");

        assert!(request.validate().is_err());
    }

    #[test]
    fn raw_order_accepts_direction_aliases_and_serializes_canonical_sides() {
        let buy: ScriptRawOrderRequest = serde_json::from_value(json!({
            "key": "bid-1",
            "symbol": "btc",
            "side": "long",
            "size": 1,
            "order": { "type": "limit", "price": 99, "tif": "alo" }
        }))
        .expect("long alias decodes");
        let sell: ScriptRawOrderRequest = serde_json::from_value(json!({
            "key": "ask-1",
            "symbol": "btc",
            "side": "short",
            "margin": 100,
            "leverage": 2,
            "order": { "type": "limit", "price": 101, "tif": "alo" }
        }))
        .expect("short alias decodes");

        buy.validate().expect("buy order validates");
        sell.validate().expect("sell order validates");
        assert_eq!(buy.side, ScriptOrderSide::Buy);
        assert_eq!(sell.side, ScriptOrderSide::Sell);
        assert_eq!(serde_json::to_value(buy).unwrap()["side"], "buy");
        assert_eq!(serde_json::to_value(sell).unwrap()["side"], "sell");
    }

    #[test]
    fn managed_request_distinguishes_position_intent_from_raw_side() {
        let trade: ScriptManagedRequest = serde_json::from_value(json!({
            "key": "entry",
            "symbol": "btc",
            "position": "open-long",
            "size": 1
        }))
        .expect("trade request decodes");
        let order: ScriptManagedRequest = serde_json::from_value(json!({
            "key": "ask",
            "symbol": "btc",
            "side": "sell",
            "size": 1
        }))
        .expect("raw order decodes");

        assert!(matches!(trade, ScriptManagedRequest::Trade(_)));
        assert!(matches!(order, ScriptManagedRequest::Order(_)));
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

    #[test]
    fn python_v2_requires_and_extracts_execution_exchange() {
        let commands = Arc::new(Mutex::new(Vec::new()));
        let value = queue_execution_call(
            "python-job",
            true,
            true,
            &commands,
            "order",
            &json!({
                "key": "bid-1",
                "exchange": "hyperliquidf",
                "symbol": "btc",
                "side": "buy",
                "size": 0.1
            })
            .to_string(),
        )
        .expect("Python request should route");

        assert_eq!(value["key"], "bid-1");
        let queued = commands.lock().unwrap();
        assert!(matches!(
            queued.as_slice(),
            [ScriptExecutionCommand::Order {
                exchange: Some(ExecutionVenue::Hyperliquid),
                ..
            }]
        ));
    }

    #[test]
    fn python_v2_rejects_missing_execution_exchange() {
        let commands = Arc::new(Mutex::new(Vec::new()));
        let error = queue_execution_call(
            "python-job",
            true,
            true,
            &commands,
            "trade",
            &json!({
                "key": "entry-1",
                "symbol": "btc",
                "position": "open-long",
                "size": 0.1
            })
            .to_string(),
        )
        .expect_err("Python request without exchange must fail");

        assert!(format!("{error:#}").contains("requires exchange"));
    }

    #[test]
    fn javascript_v1_still_rejects_request_exchange() {
        let commands = Arc::new(Mutex::new(Vec::new()));
        let error = queue_execution_call(
            "javascript-job",
            true,
            false,
            &commands,
            "order",
            &json!({
                "key": "bid-1",
                "exchange": "bulkf",
                "symbol": "btc",
                "side": "buy",
                "size": 0.1
            })
            .to_string(),
        )
        .expect_err("V1 request-level exchange must remain invalid");

        assert!(format!("{error:#}").contains("unknown field `exchange`"));
    }
}
