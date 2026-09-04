use std::collections::HashSet;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use super::client::HyperliquidClient;
use super::HyperliquidNetwork;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OutcomeMetadata {
    pub outcomes: Vec<OutcomeSpec>,
    pub questions: Vec<QuestionSpec>,
    pub deployers: Vec<OutcomeDeployer>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OutcomeSpec {
    pub outcome: u32,
    pub name: String,
    pub description: String,
    pub side_specs: [OutcomeSideSpec; 2],
    pub quote_token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub venue: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deployer_fee_scale: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OutcomeSideSpec {
    pub name: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QuestionSpec {
    pub question: u32,
    pub name: String,
    pub description: String,
    pub fallback_outcome: u32,
    pub named_outcomes: Vec<u32>,
    pub settled_named_outcomes: Vec<u32>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OutcomeDeployer {
    #[serde(rename(deserialize = "deployer"))]
    pub address: String,
    pub venue: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OutcomeTemplate {
    pub id: String,
    pub role: OutcomeTemplateRole,
    pub name: String,
    pub description: String,
    pub keywords: Vec<(String, String)>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum OutcomeTemplateRole {
    Name(String),
    Standalone {
        #[serde(rename = "standaloneOutcome")]
        standalone: StandaloneOutcomeTemplate,
    },
    QuestionOutcome {
        #[serde(rename = "questionOutcome")]
        question_outcome: QuestionOutcomeTemplate,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StandaloneOutcomeTemplate {
    pub side_names: [String; 2],
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct QuestionOutcomeTemplate {
    pub parent: String,
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

pub async fn templates(network: HyperliquidNetwork) -> Result<Vec<OutcomeTemplate>> {
    let templates: Vec<OutcomeTemplate> = HyperliquidClient::for_network(network)?
        .info(&serde_json::json!({ "type": "outcomeTemplates" }))
        .await
        .with_context(|| {
            format!(
                "failed to fetch Hyperliquid {} outcome templates",
                network.label()
            )
        })?;
    validate_templates(&templates)?;
    Ok(templates)
}

fn validate_metadata(metadata: &OutcomeMetadata) -> Result<()> {
    let mut deployer_addresses = HashSet::new();
    let mut deployer_venues = HashSet::new();
    for deployer in &metadata.deployers {
        if deployer.address.trim().is_empty() || deployer.venue.trim().is_empty() {
            bail!("Hyperliquid outcomeMeta contains an empty deployer address or venue");
        }
        if !deployer_addresses.insert(deployer.address.as_str()) {
            bail!(
                "Hyperliquid outcomeMeta contains duplicate deployer {}",
                deployer.address
            );
        }
        if !deployer_venues.insert(deployer.venue.as_str()) {
            bail!(
                "Hyperliquid outcomeMeta contains duplicate deployer venue `{}`",
                deployer.venue
            );
        }
    }

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
        if let Some(venue) = outcome.venue.as_deref()
            && !deployer_venues.contains(venue)
        {
            bail!(
                "Hyperliquid outcome {} references unknown deployer venue `{venue}`",
                outcome.outcome
            );
        }
    }
    Ok(())
}

fn validate_templates(templates: &[OutcomeTemplate]) -> Result<()> {
    let mut ids = HashSet::new();
    for template in templates {
        if template.id.is_empty() || !ids.insert(template.id.as_str()) {
            bail!(
                "Hyperliquid outcomeTemplates contains an empty or duplicate template id `{}`",
                template.id
            );
        }
        match &template.role {
            OutcomeTemplateRole::Name(role) if role == "question" => {}
            OutcomeTemplateRole::Name(role) => bail!(
                "Hyperliquid outcome template `{}` has unknown role `{role}`",
                template.id
            ),
            OutcomeTemplateRole::Standalone { standalone } => {
                if standalone.side_names.iter().any(String::is_empty) {
                    bail!(
                        "Hyperliquid outcome template `{}` has an empty side name",
                        template.id
                    );
                }
            }
            OutcomeTemplateRole::QuestionOutcome { question_outcome } => {
                if question_outcome.parent.is_empty() {
                    bail!(
                        "Hyperliquid outcome template `{}` omitted its parent template",
                        template.id
                    );
                }
            }
        }
        let mut keywords = HashSet::new();
        for (keyword, hint) in &template.keywords {
            if keyword.is_empty() || !keywords.insert(keyword.as_str()) {
                bail!(
                    "Hyperliquid outcome template `{}` contains an empty or duplicate keyword `{keyword}`",
                    template.id
                );
            }
            if !matches!(
                hint.as_str(),
                "dateTime" | "date" | "string" | "shortString" | "hlPerp" | "uInt" | "uDecimal"
            ) {
                bail!(
                    "Hyperliquid outcome template `{}` contains unknown keyword hint `{hint}`",
                    template.id
                );
            }
        }
    }
    Ok(())
}
