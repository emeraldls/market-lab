use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use chrono::{NaiveDate, NaiveDateTime};
use serde::{Deserialize, Serialize};
use sha3::{Digest, Keccak256};

use super::{ExecutionRules, Market, NetworkMarket};
use crate::providers::hyperliquid::outcomes::{
    OutcomeMetadata, OutcomeSpec, OutcomeTemplate, OutcomeTemplateRole, QuestionSpec,
};
use crate::providers::hyperliquid::{HyperliquidNetwork, OUTCOMES_EXCHANGE};

pub const OUTCOME_ASSET_OFFSET: u32 = 100_000_000;
pub const OUTCOME_MIN_NOTIONAL: f64 = 10.0;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OutcomeInstrument {
    pub exchange: String,
    pub network: String,
    pub symbol: String,
    pub question_id: Option<u32>,
    pub question_name: Option<String>,
    pub question_description: Option<String>,
    pub outcome_id: u32,
    pub outcome_name: String,
    pub outcome_description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub template: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deployer: Option<crate::providers::hyperliquid::outcomes::OutcomeDeployer>,
    pub side: u8,
    pub side_name: String,
    pub quote_token: String,
    pub coin: String,
    pub token_name: String,
    pub asset_id: u32,
    pub settled: bool,
    pub metadata_fingerprint: String,
}

impl OutcomeInstrument {
    pub fn market(&self) -> Market {
        let rules = outcome_execution_rules();
        Market {
            symbol: self.symbol.clone(),
            provider_symbol: self.coin.clone(),
            venue_symbol: self.coin.clone(),
            venue_id: Some(self.asset_id),
            aliases: vec![self.coin.clone(), self.token_name.clone()],
            base_asset: self.symbol.clone(),
            quote_asset: self.quote_token.clone(),
            venue_base_asset: self.token_name.clone(),
            venue_quote_asset: self.quote_token.clone(),
            status: if self.settled { "settled" } else { "available" }.to_string(),
            price_increment: Some(rules.tick_size),
            size_increment: Some(rules.lot_size),
            execution: Some(rules),
            network_variants: BTreeMap::new(),
        }
    }
}

pub async fn instruments(network: HyperliquidNetwork) -> Result<Vec<OutcomeInstrument>> {
    let (metadata, templates) = tokio::try_join!(
        crate::providers::hyperliquid::outcomes::metadata(network),
        crate::providers::hyperliquid::outcomes::templates(network)
    )?;
    instruments_from_metadata(network, &metadata, &templates)
}

pub async fn resolve(network: HyperliquidNetwork, symbol: &str) -> Result<OutcomeInstrument> {
    let (outcome_id, side) = parse_symbol(symbol)?;
    instruments(network)
        .await?
        .into_iter()
        .find(|instrument| instrument.outcome_id == outcome_id && instrument.side == side)
        .with_context(|| {
            format!(
                "Hyperliquid {} outcome instrument `{}` is not active in outcomeMeta",
                network.label(),
                canonical_symbol(outcome_id, side)
            )
        })
}

pub async fn resolve_wire(
    network: HyperliquidNetwork,
    wire_symbol: &str,
) -> Result<OutcomeInstrument> {
    let (outcome, side) = parse_wire_symbol(wire_symbol)?;
    resolve(network, &canonical_symbol(outcome, side)).await
}

pub fn instruments_from_metadata(
    network: HyperliquidNetwork,
    metadata: &OutcomeMetadata,
    templates: &[OutcomeTemplate],
) -> Result<Vec<OutcomeInstrument>> {
    let mut parents = HashMap::<u32, &QuestionSpec>::new();
    let mut settled = HashSet::<u32>::new();
    for question in &metadata.questions {
        associate_question(&mut parents, question.fallback_outcome, question)?;
        for outcome in &question.named_outcomes {
            associate_question(&mut parents, *outcome, question)?;
        }
        for outcome in &question.settled_named_outcomes {
            associate_question(&mut parents, *outcome, question)?;
            settled.insert(*outcome);
        }
    }

    let templates = templates
        .iter()
        .map(|template| (template.id.as_str(), template))
        .collect::<HashMap<_, _>>();
    let deployers = metadata
        .deployers
        .iter()
        .map(|deployer| (deployer.venue.as_str(), deployer))
        .collect::<HashMap<_, _>>();

    let mut instruments = Vec::with_capacity(metadata.outcomes.len() * 2);
    for outcome in &metadata.outcomes {
        let parent = parents.get(&outcome.outcome).copied();
        let rendered = render_outcome(parent, outcome, &templates)?;
        let deployer = outcome
            .venue
            .as_deref()
            .map(|venue| {
                deployers.get(venue).copied().with_context(|| {
                    format!(
                        "Hyperliquid outcome {} references unknown deployer venue `{venue}`",
                        outcome.outcome
                    )
                })
            })
            .transpose()?;
        for side in 0_u8..=1 {
            let encoding = encoding(outcome.outcome, side)?;
            instruments.push(OutcomeInstrument {
                exchange: OUTCOMES_EXCHANGE.to_string(),
                network: network.label().to_string(),
                symbol: canonical_symbol(outcome.outcome, side),
                question_id: parent.map(|question| question.question),
                question_name: Some(rendered.question_name.clone()),
                question_description: rendered.question_description.clone(),
                outcome_id: outcome.outcome,
                outcome_name: rendered.outcome_name.clone(),
                outcome_description: rendered.outcome_description.clone(),
                template: rendered.template.clone(),
                deployer: deployer.cloned(),
                side,
                side_name: rendered.side_names[usize::from(side)].clone(),
                quote_token: outcome.quote_token.clone(),
                coin: format!("#{encoding}"),
                token_name: format!("+{encoding}"),
                asset_id: OUTCOME_ASSET_OFFSET
                    .checked_add(encoding)
                    .context("Hyperliquid outcome asset id exceeds u32")?,
                settled: settled.contains(&outcome.outcome),
                metadata_fingerprint: fingerprint(parent, outcome, side)?,
            });
        }
    }
    instruments.sort_by_key(|instrument| {
        (
            instrument.question_id,
            instrument.outcome_id,
            instrument.side,
        )
    });
    Ok(instruments)
}

fn associate_question<'a>(
    parents: &mut HashMap<u32, &'a QuestionSpec>,
    outcome: u32,
    question: &'a QuestionSpec,
) -> Result<()> {
    if let Some(existing) = parents.insert(outcome, question) {
        bail!(
            "Hyperliquid outcome {outcome} belongs to both question {} and {}",
            existing.question,
            question.question
        );
    }
    Ok(())
}

struct RenderedOutcome {
    question_name: String,
    question_description: Option<String>,
    outcome_name: String,
    outcome_description: String,
    side_names: [String; 2],
    template: Option<String>,
}

fn render_outcome(
    question: Option<&QuestionSpec>,
    outcome: &OutcomeSpec,
    templates: &HashMap<&str, &OutcomeTemplate>,
) -> Result<RenderedOutcome> {
    let rendered_question = question
        .map(|question| render_question(question, templates))
        .transpose()?;

    if outcome.name == "template fallback" {
        let question = question.context("template fallback is not associated with a question")?;
        if question.fallback_outcome != outcome.outcome || outcome.description != "other" {
            bail!(
                "Hyperliquid outcome {} has an invalid template fallback shape",
                outcome.outcome
            );
        }
        require_side_names(outcome, ["Yes", "No"])?;
        let (question_name, question_description) =
            rendered_question.context("template fallback omitted its question")?;
        return Ok(RenderedOutcome {
            question_name,
            question_description: Some(question_description),
            outcome_name: "Other".to_string(),
            outcome_description: outcome.description.clone(),
            side_names: ["Yes".to_string(), "No".to_string()],
            template: None,
        });
    }

    if let Some(template_id) = outcome.name.strip_prefix("template:") {
        let template = templates.get(template_id).copied().with_context(|| {
            format!(
                "Hyperliquid outcome {} references unknown template `{template_id}`",
                outcome.outcome
            )
        })?;
        let values = keyword_values(&outcome.description)?;
        let outcome_name = interpolate(template, &template.name, &values)?;
        let outcome_description = interpolate(template, &template.description, &values)?;
        let side_names = match &template.role {
            OutcomeTemplateRole::Standalone { standalone } => {
                if question.is_some() {
                    bail!(
                        "Hyperliquid outcome {} uses standalone template `{template_id}` inside a question",
                        outcome.outcome
                    );
                }
                let expected = [
                    format!("template:{}", standalone.side_names[0]),
                    format!("template:{}", standalone.side_names[1]),
                ];
                require_side_names(outcome, [&expected[0], &expected[1]])?;
                standalone.side_names.clone()
            }
            OutcomeTemplateRole::QuestionOutcome { question_outcome } => {
                let question = question.with_context(|| {
                    format!(
                        "Hyperliquid outcome {} uses question template `{template_id}` without a question",
                        outcome.outcome
                    )
                })?;
                let parent_template = question
                    .name
                    .strip_prefix("template:")
                    .context("templated question outcome belongs to a non-template question")?;
                if question_outcome.parent != parent_template {
                    bail!(
                        "Hyperliquid outcome {} template `{template_id}` expects question template `{}`, got `{parent_template}`",
                        outcome.outcome,
                        question_outcome.parent
                    );
                }
                require_side_names(outcome, ["Yes", "No"])?;
                ["Yes".to_string(), "No".to_string()]
            }
            OutcomeTemplateRole::Name(role) => bail!(
                "Hyperliquid outcome {} references template `{template_id}` with role `{role}`",
                outcome.outcome
            ),
        };
        let (question_name, question_description) = rendered_question
            .unwrap_or_else(|| (outcome_name.clone(), outcome_description.clone()));
        return Ok(RenderedOutcome {
            question_name,
            question_description: Some(question_description),
            outcome_name,
            outcome_description,
            side_names,
            template: Some(template_id.to_string()),
        });
    }

    let question_name = rendered_question.as_ref().map_or_else(
        || legacy_outcome_name(outcome),
        |(name, _)| Ok(name.clone()),
    )?;
    let question_description = rendered_question
        .map(|(_, description)| description)
        .or_else(|| Some(outcome.description.clone()));
    Ok(RenderedOutcome {
        question_name,
        question_description,
        outcome_name: outcome.name.clone(),
        outcome_description: outcome.description.clone(),
        side_names: [
            outcome.side_specs[0].name.clone(),
            outcome.side_specs[1].name.clone(),
        ],
        template: None,
    })
}

fn render_question(
    question: &QuestionSpec,
    templates: &HashMap<&str, &OutcomeTemplate>,
) -> Result<(String, String)> {
    let Some(template_id) = question.name.strip_prefix("template:") else {
        return Ok((
            legacy_question_name(question)?,
            question.description.clone(),
        ));
    };
    let template = templates.get(template_id).copied().with_context(|| {
        format!(
            "Hyperliquid question {} references unknown template `{template_id}`",
            question.question
        )
    })?;
    match &template.role {
        OutcomeTemplateRole::Name(role) if role == "question" => {}
        _ => bail!(
            "Hyperliquid question {} references non-question template `{template_id}`",
            question.question
        ),
    }
    let values = keyword_values(&question.description)?;
    Ok((
        interpolate(template, &template.name, &values)?,
        interpolate(template, &template.description, &values)?,
    ))
}

fn legacy_outcome_name(outcome: &OutcomeSpec) -> Result<String> {
    let fields = keyword_values(&outcome.description)?;
    if outcome.name == "Recurring" && fields.get("class").map(String::as_str) == Some("priceBinary")
    {
        return Ok(format!(
            "{} above {} at {}?",
            display_underlying(required_value(&fields, "underlying")?),
            display_number(required_value(&fields, "targetPrice")?),
            display_utc_time(required_value(&fields, "expiry")?)?
        ));
    }
    Ok(outcome.name.clone())
}

fn legacy_question_name(question: &QuestionSpec) -> Result<String> {
    let fields = keyword_values(&question.description)?;
    if question.name == "Recurring"
        && fields.get("class").map(String::as_str) == Some("priceBucket")
    {
        return Ok(format!(
            "{} price at {}?",
            display_underlying(required_value(&fields, "underlying")?),
            display_utc_time(required_value(&fields, "expiry")?)?
        ));
    }
    Ok(question.name.clone())
}

fn keyword_values(description: &str) -> Result<BTreeMap<String, String>> {
    if description.is_empty() {
        return Ok(BTreeMap::new());
    }
    description
        .split('|')
        .map(|entry| {
            let (key, value) = entry
                .split_once(':')
                .with_context(|| format!("outcome template value `{entry}` is missing `:`"))?;
            if key.is_empty() {
                bail!("outcome template value `{entry}` has an empty key");
            }
            Ok((key.to_string(), value.to_string()))
        })
        .try_fold(BTreeMap::new(), |mut fields, field| {
            let (key, value) = field?;
            if fields.insert(key.clone(), value).is_some() {
                bail!("outcome template description contains duplicate keyword `{key}`");
            }
            Ok(fields)
        })
}

fn required_value<'a>(values: &'a BTreeMap<String, String>, key: &str) -> Result<&'a str> {
    values
        .get(key)
        .map(String::as_str)
        .with_context(|| format!("outcome metadata omitted `{key}`"))
}

fn require_side_names(outcome: &OutcomeSpec, expected: [&str; 2]) -> Result<()> {
    if outcome.side_specs[0].name != expected[0] || outcome.side_specs[1].name != expected[1] {
        bail!(
            "Hyperliquid outcome {} side names do not match its template",
            outcome.outcome
        );
    }
    Ok(())
}

fn interpolate(
    template: &OutcomeTemplate,
    text: &str,
    values: &BTreeMap<String, String>,
) -> Result<String> {
    let expected = template
        .keywords
        .iter()
        .map(|(keyword, _)| keyword.as_str())
        .collect::<HashSet<_>>();
    let actual = values.keys().map(String::as_str).collect::<HashSet<_>>();
    if expected != actual {
        bail!(
            "Hyperliquid outcome template `{}` expected keywords [{}], got [{}]",
            template.id,
            sorted_join(expected),
            sorted_join(actual)
        );
    }

    let mut rendered = text.to_string();
    for (keyword, hint) in &template.keywords {
        let value = required_value(values, keyword)?;
        let display = display_template_value(hint, value).with_context(|| {
            format!(
                "invalid `{keyword}` value for Hyperliquid outcome template `{}`",
                template.id
            )
        })?;
        rendered = rendered.replace(&format!("{{{keyword}}}"), &display);
    }
    Ok(rendered)
}

fn sorted_join(values: HashSet<&str>) -> String {
    let mut values = values.into_iter().collect::<Vec<_>>();
    values.sort_unstable();
    values.join(", ")
}

fn display_template_value(hint: &str, value: &str) -> Result<String> {
    match hint {
        "dateTime" => display_utc_time(value),
        "date" => NaiveDate::parse_from_str(value, "%Y%m%d")
            .map(|date| date.format("%b %-d, %Y UTC").to_string())
            .context("expected UTC date YYYYMMDD"),
        "hlPerp" => Ok(display_underlying(value).to_string()),
        "uInt" => {
            value.parse::<u64>().context("expected unsigned integer")?;
            Ok(display_number(value))
        }
        "uDecimal" => {
            if value.starts_with(['+', '-'])
                || value.parse::<f64>().is_err()
                || !value
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || byte == b'.')
            {
                bail!("expected unsigned decimal");
            }
            Ok(display_number(value))
        }
        "shortString" if value.chars().count() <= 10 => Ok(value.to_string()),
        "shortString" => bail!("expected at most 10 characters"),
        "string" => Ok(value.to_string()),
        _ => bail!("unknown outcome template keyword hint `{hint}`"),
    }
}

fn display_underlying(value: &str) -> &str {
    value.rsplit(':').next().unwrap_or(value)
}

fn display_utc_time(value: &str) -> Result<String> {
    NaiveDateTime::parse_from_str(value, "%Y%m%d-%H%M")
        .map(|time| time.format("%b %-d, %H:%M UTC").to_string())
        .context("expected UTC time YYYYMMDD-HHMM")
}

fn display_number(value: &str) -> String {
    let (integer, fraction) = value.split_once('.').unwrap_or((value, ""));
    let (sign, digits) = integer
        .strip_prefix('-')
        .map_or(("", integer), |digits| ("-", digits));
    let mut grouped = String::with_capacity(value.len() + value.len() / 3);
    grouped.push_str(sign);
    for (index, character) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(character);
    }
    if !fraction.is_empty() {
        grouped.push('.');
        grouped.push_str(fraction);
    }
    grouped
}

pub fn parse_market_id(symbol: &str) -> Result<u32> {
    let value = symbol.trim();
    if value.is_empty() {
        bail!("outcome market ID cannot be empty");
    }
    if value.contains(':') || value.starts_with(['#', '+']) {
        bail!(
            "outcome bots accept the market ID without a side, e.g. 10225; side-qualified symbols are reserved for direct trading and market data"
        );
    }
    value
        .parse::<u32>()
        .context("outcome market ID must be numeric, e.g. 10225")
}

pub fn parse_symbol(symbol: &str) -> Result<(u32, u8)> {
    let value = symbol.trim();
    if value.starts_with('#') || value.starts_with('+') {
        return parse_wire_symbol(value);
    }
    let mut parts = value.split(':');
    let outcome = parts
        .next()
        .context("outcome symbol is empty")?
        .parse::<u32>()
        .context("outcome symbol must use OUTCOME_ID:SIDE, e.g. 1001:0")?;
    let side = parts
        .next()
        .context("outcome symbol must include side 0 or 1, e.g. 1001:0")?
        .parse::<u8>()
        .context("outcome side must be 0 or 1")?;
    if parts.next().is_some() || side > 1 {
        bail!("outcome symbol must use OUTCOME_ID:SIDE with side 0 or 1");
    }
    Ok((outcome, side))
}

pub fn parse_wire_symbol(symbol: &str) -> Result<(u32, u8)> {
    let encoding = symbol
        .trim()
        .strip_prefix(['#', '+'])
        .context("outcome wire symbol must begin with `#` or `+`")?
        .parse::<u32>()
        .context("outcome wire symbol contains an invalid encoding")?;
    let side = u8::try_from(encoding % 10).context("outcome side exceeds u8")?;
    if side > 1 {
        bail!("outcome encoding has invalid side {side}; only 0 and 1 exist");
    }
    Ok((encoding / 10, side))
}

pub fn canonical_symbol(outcome: u32, side: u8) -> String {
    format!("{outcome}:{side}")
}

pub fn encoding(outcome: u32, side: u8) -> Result<u32> {
    if side > 1 {
        bail!("outcome side must be 0 or 1");
    }
    outcome
        .checked_mul(10)
        .and_then(|encoding| encoding.checked_add(u32::from(side)))
        .context("Hyperliquid outcome encoding exceeds u32")
}

pub fn outcome_execution_rules() -> ExecutionRules {
    ExecutionRules {
        price_precision: 8,
        size_precision: 0,
        tick_size: 0.00000001,
        lot_size: 1.0,
        min_notional: OUTCOME_MIN_NOTIONAL,
        max_leverage: 1,
        cross_margin: false,
        order_types: vec!["MARKET".to_string(), "LIMIT".to_string()],
        time_in_forces: vec!["GTC".to_string(), "IOC".to_string(), "ALO".to_string()],
    }
}

pub fn market_from_instrument(instrument: &OutcomeInstrument) -> Arc<Market> {
    Arc::new(instrument.market())
}

pub async fn market_and_variant(
    network: HyperliquidNetwork,
    symbol: &str,
) -> Result<(Arc<Market>, NetworkMarket, OutcomeInstrument)> {
    let instrument = resolve(network, symbol).await?;
    let rules = outcome_execution_rules();
    let variant = NetworkMarket {
        provider_symbol: instrument.coin.clone(),
        venue_symbol: instrument.coin.clone(),
        venue_id: instrument.asset_id,
        venue_base_asset: instrument.token_name.clone(),
        venue_quote_asset: instrument.quote_token.clone(),
        base_token_id: None,
        quote_token_id: None,
        base_token_index: None,
        quote_token_index: None,
        price_increment: rules.tick_size,
        size_increment: rules.lot_size,
        execution: rules,
    };
    Ok((market_from_instrument(&instrument), variant, instrument))
}

fn fingerprint(question: Option<&QuestionSpec>, outcome: &OutcomeSpec, side: u8) -> Result<String> {
    let value = serde_json::to_vec(&(question, outcome, side))
        .context("failed to encode Hyperliquid outcome metadata fingerprint")?;
    Ok(hex::encode(Keccak256::digest(value)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn permissionless_fixture() -> (OutcomeMetadata, Vec<OutcomeTemplate>) {
        let metadata = serde_json::from_value(serde_json::json!({
            "outcomes": [{
                "outcome": 1210,
                "name": "template:binaryPrice",
                "description": "perp:BTC|priceDescription:BTC-USDC mark|seconds:1|threshold:100000|time:20261001-0000",
                "sideSpecs": [{"name":"template:Yes"},{"name":"template:No"}],
                "quoteToken": "USDC",
                "venue": "out",
                "deployerFeeScale": "1.0"
            }],
            "questions": [],
            "deployers": [{
                "deployer": "0x08e9c89f46dccee91bdb85c6532eb93a4c335efe",
                "venue": "out",
                "subDeployers": []
            }]
        }))
        .expect("metadata");
        let templates = serde_json::from_value(serde_json::json!([{
            "id": "binaryPrice",
            "role": {"standaloneOutcome": {"sideNames": ["Yes", "No"]}},
            "name": "{perp} above {threshold} at {time}?",
            "description": "The market resolves to Yes if {perp} is above {threshold} at {time}.",
            "keywords": [
                ["perp", "hlPerp"],
                ["priceDescription", "string"],
                ["seconds", "uInt"],
                ["threshold", "uDecimal"],
                ["time", "dateTime"]
            ]
        }]))
        .expect("templates");
        (metadata, templates)
    }

    #[test]
    fn renders_templates_and_joins_the_exact_deployer() {
        let (metadata, templates) = permissionless_fixture();
        let instruments =
            instruments_from_metadata(HyperliquidNetwork::Mainnet, &metadata, &templates)
                .expect("instruments");
        let instrument = &instruments[0];

        assert_eq!(
            instrument.question_name.as_deref(),
            Some("BTC above 100,000 at Oct 1, 00:00 UTC?")
        );
        assert_eq!(instrument.side_name, "Yes");
        assert_eq!(instrument.template.as_deref(), Some("binaryPrice"));
        assert_eq!(
            instrument
                .deployer
                .as_ref()
                .map(|value| value.venue.as_str()),
            Some("out")
        );
        assert_eq!(instrument.coin, "#12100");
        assert_eq!(instrument.asset_id, 100_012_100);
    }

    #[test]
    fn rejects_template_instances_with_a_different_keyword_set() {
        let (mut metadata, templates) = permissionless_fixture();
        metadata.outcomes[0].description =
            "perp:BTC|threshold:100000|time:20261001-0000".to_string();

        let error = instruments_from_metadata(HyperliquidNetwork::Mainnet, &metadata, &templates)
            .expect_err("missing template keywords must fail");
        assert!(error.to_string().contains("expected keywords"));
    }

    #[test]
    fn accepts_canonical_and_wire_symbols() {
        assert_eq!(parse_symbol("1001:1").expect("canonical"), (1001, 1));
        assert_eq!(parse_symbol("#10011").expect("coin"), (1001, 1));
        assert_eq!(parse_symbol("+10011").expect("token"), (1001, 1));
        assert!(parse_symbol("1001").is_err());
        assert!(parse_symbol("1001:2").is_err());
    }
}
