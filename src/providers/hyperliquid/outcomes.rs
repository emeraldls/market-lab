use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{self, IsTerminal};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use chrono::NaiveDateTime;
use dialoguer::{FuzzySelect, Select, theme::ColorfulTheme};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha3::{Digest, Keccak256};

use crate::markets::{ExecutionRules, Market, NetworkMarket};

use super::client::HyperliquidClient;
use super::{HyperliquidNetwork, OUTCOMES_EXCHANGE};

pub const OUTCOME_ASSET_OFFSET: u32 = 100_000_000;
pub const OUTCOME_MIN_NOTIONAL: f64 = 10.0;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OutcomeMetadata {
    #[serde(default)]
    pub outcomes: Vec<OutcomeSpec>,
    #[serde(default)]
    pub questions: Vec<QuestionSpec>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deployers: Vec<OutcomeDeployer>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OutcomeSpec {
    pub outcome: u32,
    pub name: String,
    pub description: String,
    pub side_specs: [OutcomeSideSpec; 2],
    pub quote_token: String,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OutcomeSideSpec {
    pub name: String,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QuestionSpec {
    pub question: u32,
    pub name: String,
    pub description: String,
    pub fallback_outcome: u32,
    #[serde(default)]
    pub named_outcomes: Vec<u32>,
    #[serde(default)]
    pub settled_named_outcomes: Vec<u32>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OutcomeDeployer {
    pub deployer: String,
    pub venue: String,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

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

    pub fn display_label(&self) -> String {
        let parent = self.question_name.as_deref().unwrap_or("Standalone");
        format!(
            "{} — {} / {} [{}]",
            clean_terminal_text(parent),
            clean_terminal_text(&self.outcome_name),
            clean_terminal_text(&self.side_name),
            self.symbol
        )
    }
}

pub async fn metadata(network: HyperliquidNetwork) -> Result<OutcomeMetadata> {
    let metadata: OutcomeMetadata = HyperliquidClient::for_network(network)?
        .info(&serde_json::json!({ "type": "outcomeMeta" }))
        .await
        .with_context(|| {
            format!(
                "failed to fetch Hyperliquid {} outcome metadata",
                network.label()
            )
        })?;
    validate_metadata(&metadata)?;
    Ok(metadata)
}

pub async fn instruments(network: HyperliquidNetwork) -> Result<Vec<OutcomeInstrument>> {
    instruments_from_metadata(network, &metadata(network).await?)
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

pub async fn select_interactive(network: HyperliquidNetwork) -> Result<OutcomeInstrument> {
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        bail!("outcome selection needs an interactive terminal; pass a symbol such as `1001:0`");
    }
    let instruments = instruments(network).await?;
    let mut outcomes = Vec::<OutcomeInstrument>::new();
    let mut seen = HashSet::<u32>::new();
    for instrument in instruments.iter().filter(|instrument| !instrument.settled) {
        if seen.insert(instrument.outcome_id) {
            outcomes.push(instrument.clone());
        }
    }
    if outcomes.is_empty() {
        bail!(
            "Hyperliquid {} currently exposes no active outcomes",
            network.label()
        );
    }
    let labels = outcomes
        .iter()
        .map(|instrument| {
            let question = instrument.question_name.as_deref().unwrap_or("Standalone");
            format!(
                "{} — {} [outcome {}]",
                clean_terminal_text(question),
                clean_terminal_text(&instrument.outcome_name),
                instrument.outcome_id
            )
        })
        .collect::<Vec<_>>();
    let theme = ColorfulTheme::default();
    let selected = FuzzySelect::with_theme(&theme)
        .with_prompt("Search or select an outcome")
        .items(&labels)
        .default(0)
        .interact_opt()?
        .context("outcome selection was cancelled")?;
    let outcome = &outcomes[selected];
    let matching = instruments
        .into_iter()
        .filter(|instrument| instrument.outcome_id == outcome.outcome_id)
        .collect::<Vec<_>>();
    let labels = matching
        .iter()
        .map(|instrument| {
            format!(
                "{} (side {})",
                clean_terminal_text(&instrument.side_name),
                instrument.side
            )
        })
        .collect::<Vec<_>>();
    let selected = Select::with_theme(&theme)
        .with_prompt(format!(
            "Select a side for {}",
            clean_terminal_text(&outcome.outcome_name)
        ))
        .items(&labels)
        .default(0)
        .interact_opt()?
        .context("outcome side selection was cancelled")?;
    Ok(matching[selected].clone())
}

pub fn instruments_from_metadata(
    network: HyperliquidNetwork,
    metadata: &OutcomeMetadata,
) -> Result<Vec<OutcomeInstrument>> {
    validate_metadata(metadata)?;
    let mut parents = HashMap::<u32, &QuestionSpec>::new();
    let mut settled = HashSet::<u32>::new();
    for question in &metadata.questions {
        parents.insert(question.fallback_outcome, question);
        for outcome in &question.named_outcomes {
            parents.insert(*outcome, question);
        }
        for outcome in &question.settled_named_outcomes {
            parents.insert(*outcome, question);
            settled.insert(*outcome);
        }
    }

    let mut instruments = Vec::with_capacity(metadata.outcomes.len() * 2);
    for outcome in &metadata.outcomes {
        let parent = parents.get(&outcome.outcome).copied();
        let question_name = readable_question_name(parent, outcome);
        for side in 0_u8..=1 {
            let encoding = encoding(outcome.outcome, side)?;
            let fingerprint = fingerprint(parent, outcome, side)?;
            instruments.push(OutcomeInstrument {
                exchange: OUTCOMES_EXCHANGE.to_string(),
                network: network.label().to_string(),
                symbol: canonical_symbol(outcome.outcome, side),
                question_id: parent.map(|question| question.question),
                question_name: Some(question_name.clone()),
                question_description: parent.map(|question| question.description.clone()),
                outcome_id: outcome.outcome,
                outcome_name: outcome.name.clone(),
                outcome_description: outcome.description.clone(),
                side,
                side_name: outcome.side_specs[usize::from(side)].name.clone(),
                quote_token: outcome.quote_token.clone(),
                coin: format!("#{encoding}"),
                token_name: format!("+{encoding}"),
                asset_id: OUTCOME_ASSET_OFFSET
                    .checked_add(encoding)
                    .context("Hyperliquid outcome asset id exceeds u32")?,
                settled: settled.contains(&outcome.outcome),
                metadata_fingerprint: fingerprint,
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

fn readable_question_name(question: Option<&QuestionSpec>, outcome: &OutcomeSpec) -> String {
    let description = question.map_or(outcome.description.as_str(), |value| {
        value.description.as_str()
    });
    if let Some(name) = structured_question_name(description) {
        return name;
    }

    question.map_or_else(|| outcome.name.clone(), |value| value.name.clone())
}

fn structured_question_name(description: &str) -> Option<String> {
    let fields = description_fields(description);

    if fields
        .get("class")
        .is_some_and(|value| *value == "priceBinary")
    {
        return price_binary_question(
            fields.get("underlying")?,
            fields.get("targetPrice")?,
            fields.get("expiry")?,
        );
    }

    if fields
        .get("class")
        .is_some_and(|value| *value == "priceBucket")
    {
        return Some(format!(
            "{} price at {}?",
            display_underlying(fields.get("underlying")?),
            display_utc_time(fields.get("expiry")?)?
        ));
    }

    if let (Some(underlying), Some(threshold), Some(time)) = (
        fields.get("perp"),
        fields.get("threshold"),
        fields.get("time"),
    ) {
        return price_binary_question(underlying, threshold, time);
    }

    if let (Some(first), Some(second), Some(time)) = (
        fields.get("participantA"),
        fields.get("participantB"),
        fields.get("scheduledStart"),
    ) {
        return Some(format!(
            "{} vs {} at {}?",
            first,
            second,
            display_utc_time(time)?
        ));
    }

    None
}

fn description_fields(description: &str) -> BTreeMap<&str, &str> {
    description
        .split('|')
        .filter_map(|part| {
            let (key, value) = part.trim().split_once(':')?;
            let value = value
                .split_once(" metadata=")
                .map_or(value, |(value, _)| value);
            (!key.is_empty() && !value.is_empty()).then_some((key, value))
        })
        .collect()
}

fn price_binary_question(underlying: &str, threshold: &str, time: &str) -> Option<String> {
    Some(format!(
        "{} above {} at {}?",
        display_underlying(underlying),
        display_number(threshold),
        display_utc_time(time)?
    ))
}

fn display_underlying(value: &str) -> &str {
    value.rsplit(':').next().unwrap_or(value)
}

fn display_utc_time(value: &str) -> Option<String> {
    NaiveDateTime::parse_from_str(value, "%Y%m%d-%H%M")
        .ok()
        .map(|time| time.format("%b %-d, %H:%M UTC").to_string())
}

fn display_number(value: &str) -> String {
    let value = numeric_prefix(value).unwrap_or_else(|| value.trim());
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

fn numeric_prefix(value: &str) -> Option<&str> {
    let value = value.trim();
    let length = value
        .char_indices()
        .take_while(|(index, character)| {
            character.is_ascii_digit() || *character == '.' || (*index == 0 && *character == '-')
        })
        .map(|(index, character)| index + character.len_utf8())
        .last()?;
    let number = &value[..length];
    number.parse::<f64>().is_ok().then_some(number)
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

pub fn clean_terminal_text(value: &str) -> String {
    value
        .chars()
        .filter(|character| !character.is_control())
        .collect::<String>()
}

fn validate_metadata(metadata: &OutcomeMetadata) -> Result<()> {
    let mut ids = HashSet::new();
    for outcome in &metadata.outcomes {
        if !ids.insert(outcome.outcome) {
            bail!(
                "Hyperliquid outcomeMeta contains duplicate outcome {}",
                outcome.outcome
            );
        }
        if outcome.quote_token.trim().is_empty() {
            bail!("Hyperliquid outcome {} omitted quoteToken", outcome.outcome);
        }
        encoding(outcome.outcome, 1)?;
    }
    Ok(())
}

fn fingerprint(question: Option<&QuestionSpec>, outcome: &OutcomeSpec, side: u8) -> Result<String> {
    let value = serde_json::to_vec(&(question, outcome, side))
        .context("failed to encode Hyperliquid outcome metadata fingerprint")?;
    Ok(hex::encode(Keccak256::digest(value)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> OutcomeMetadata {
        serde_json::from_value(serde_json::json!({
            "outcomes": [{
                "outcome": 1001,
                "name": "Above",
                "description": "BTC above threshold",
                "sideSpecs": [{"name":"Yes"},{"name":"No"}],
                "quoteToken": "USDC"
            }],
            "questions": [{
                "question": 165,
                "name": "BTC bucket",
                "description": "bucket question",
                "fallbackOutcome": 1001,
                "namedOutcomes": [],
                "settledNamedOutcomes": []
            }]
        }))
        .expect("fixture")
    }

    #[test]
    fn encodes_all_three_hyperliquid_outcome_identities() {
        let instrument = instruments_from_metadata(HyperliquidNetwork::Mainnet, &fixture())
            .expect("instruments")
            .remove(0);
        assert_eq!(instrument.symbol, "1001:0");
        assert_eq!(instrument.coin, "#10010");
        assert_eq!(instrument.token_name, "+10010");
        assert_eq!(instrument.asset_id, 100_010_010);
    }

    #[test]
    fn accepts_canonical_and_wire_symbols() {
        assert_eq!(parse_symbol("1001:1").expect("canonical"), (1001, 1));
        assert_eq!(parse_symbol("#10011").expect("coin"), (1001, 1));
        assert_eq!(parse_symbol("+10011").expect("token"), (1001, 1));
        assert!(parse_symbol("1001").is_err());
        assert!(parse_symbol("1001:2").is_err());
    }

    #[test]
    fn fingerprints_change_when_contract_metadata_changes() {
        let first =
            instruments_from_metadata(HyperliquidNetwork::Mainnet, &fixture()).expect("first");
        let mut changed = fixture();
        changed.outcomes[0].description = "different contract".to_string();
        let second =
            instruments_from_metadata(HyperliquidNetwork::Mainnet, &changed).expect("second");
        assert_ne!(
            first[0].metadata_fingerprint,
            second[0].metadata_fingerprint
        );
    }

    #[test]
    fn derives_a_readable_question_from_recurring_price_metadata() {
        let metadata: OutcomeMetadata = serde_json::from_value(serde_json::json!({
            "outcomes": [{
                "outcome": 1009,
                "name": "Recurring",
                "description": "class:priceBinary|underlying:BTC|expiry:20260806-0600|targetPrice:64315|period:1d",
                "sideSpecs": [{"name":"Yes"},{"name":"No"}],
                "quoteToken": "USDC"
            }],
            "questions": []
        }))
        .expect("metadata");

        let instruments =
            instruments_from_metadata(HyperliquidNetwork::Mainnet, &metadata).expect("instruments");

        assert_eq!(
            instruments[0].question_name.as_deref(),
            Some("BTC above 64,315 at Aug 6, 06:00 UTC?")
        );
    }

    #[test]
    fn derives_a_readable_question_from_colon_qualified_perp_metadata() {
        assert_eq!(
            structured_question_name("perp:xyz:SKHX|threshold:1180|time:20260801-1400").as_deref(),
            Some("SKHX above 1,180 at Aug 1, 14:00 UTC?")
        );
    }

    #[test]
    fn ignores_prose_appended_to_a_structured_threshold() {
        assert_eq!(
            structured_question_name(
                "perp:BTC|threshold:65000 (expires 20260807-1600) metadata=category:economics|time:20260807-1600"
            )
            .as_deref(),
            Some("BTC above 65,000 at Aug 7, 16:00 UTC?")
        );
    }

    #[test]
    fn derives_a_readable_parent_question_for_price_buckets() {
        assert_eq!(
            structured_question_name(
                "class:priceBucket|underlying:BTC|expiry:20260806-0600|priceThresholds:63028,65601|period:1d"
            )
            .as_deref(),
            Some("BTC price at Aug 6, 06:00 UTC?")
        );
    }

    #[test]
    fn keeps_human_authored_question_names() {
        let outcome = OutcomeSpec {
            outcome: 1,
            name: "June Fed rate change".to_string(),
            description: "The market resolves to Change if rates change.".to_string(),
            side_specs: [
                OutcomeSideSpec {
                    name: "Change".to_string(),
                    extra: BTreeMap::new(),
                },
                OutcomeSideSpec {
                    name: "No Change".to_string(),
                    extra: BTreeMap::new(),
                },
            ],
            quote_token: "USDC".to_string(),
            extra: BTreeMap::new(),
        };

        assert_eq!(
            readable_question_name(None, &outcome),
            "June Fed rate change"
        );
    }
}
