use std::io::{self, Write};

use anyhow::{Context, Result, bail};
use serde::Serialize;

use crate::cli::{
    OutcomeActionCommonArgs, OutcomeAmountArgs, OutcomeNegateArgs, OutcomeOptionalAmountArgs,
    OutcomeQuestionArgs, OutputFormat,
};
use crate::domain::execution::ExecutionVenue;
use crate::providers::execution::ExecutionAdapter;
use crate::providers::hyperliquid::exchange::{UserOutcomeAction, wire_number};
use crate::providers::hyperliquid::execution::HyperliquidExecutionAdapter;
use crate::providers::hyperliquid::outcomes::{OutcomeMetadata, OutcomeSpec};
use crate::providers::hyperliquid::{HyperliquidNetwork, HyperliquidProduct};

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OutcomeActionPlan {
    venue: &'static str,
    network: &'static str,
    action: &'static str,
    outcome: Option<u32>,
    question: Option<u32>,
    amount: Option<f64>,
}

pub async fn handle_split(args: OutcomeAmountArgs) -> Result<()> {
    validate_amount(args.amount)?;
    execute(
        args.common,
        OutcomeActionPlan {
            venue: "hyperliquid",
            network: "",
            action: "split",
            outcome: Some(args.outcome),
            question: None,
            amount: Some(args.amount),
        },
        UserOutcomeAction::Split {
            outcome: args.outcome,
            amount: wire_number(args.amount),
        },
    )
    .await
}

pub async fn handle_merge(args: OutcomeOptionalAmountArgs) -> Result<()> {
    if let Some(amount) = args.amount {
        validate_amount(amount)?;
    }
    execute(
        args.common,
        OutcomeActionPlan {
            venue: "hyperliquid",
            network: "",
            action: "merge",
            outcome: Some(args.outcome),
            question: None,
            amount: args.amount,
        },
        UserOutcomeAction::Merge {
            outcome: args.outcome,
            amount: args.amount.map(wire_number),
        },
    )
    .await
}

pub async fn handle_merge_question(args: OutcomeQuestionArgs) -> Result<()> {
    if let Some(amount) = args.amount {
        validate_amount(amount)?;
    }
    execute(
        args.common,
        OutcomeActionPlan {
            venue: "hyperliquid",
            network: "",
            action: "mergeQuestion",
            outcome: None,
            question: Some(args.question),
            amount: args.amount,
        },
        UserOutcomeAction::MergeQuestion {
            question: args.question,
            amount: args.amount.map(wire_number),
        },
    )
    .await
}

pub async fn handle_negate(args: OutcomeNegateArgs) -> Result<()> {
    validate_amount(args.amount)?;
    execute(
        args.common,
        OutcomeActionPlan {
            venue: "hyperliquid",
            network: "",
            action: "negate",
            outcome: Some(args.outcome),
            question: Some(args.question),
            amount: Some(args.amount),
        },
        UserOutcomeAction::Negate {
            question: args.question,
            outcome: args.outcome,
            amount: wire_number(args.amount),
        },
    )
    .await
}

async fn execute(
    common: OutcomeActionCommonArgs,
    mut plan: OutcomeActionPlan,
    action: UserOutcomeAction,
) -> Result<()> {
    common.validate()?;
    let network = HyperliquidNetwork::from_testnet(common.testnet);
    plan.network = network.label();
    validate_contract_reference(network, &plan).await?;
    render(&plan, common.output, common.dry_run)?;
    if common.dry_run {
        return Ok(());
    }
    validate_action_funds(common.testnet, &plan).await?;
    if !common.yes {
        if !matches!(common.output, OutputFormat::Terminal) {
            bail!("live outcome actions with structured output require --yes");
        }
        print!(
            "Submit this {} action to Hyperliquid {}? [y/N]: ",
            plan.action,
            network.label()
        );
        io::stdout()
            .flush()
            .context("failed to flush confirmation prompt")?;
        let mut answer = String::new();
        io::stdin()
            .read_line(&mut answer)
            .context("failed to read confirmation")?;
        if !matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes") {
            println!("cancelled; no outcome action was submitted");
            return Ok(());
        }
    }
    // Outcome metadata is dynamic. Refuse to sign if the referenced contract
    // disappeared or changed while the user was reviewing the plan.
    validate_contract_reference(network, &plan).await?;
    let adapter =
        HyperliquidExecutionAdapter::new_for(HyperliquidProduct::Outcome, network).await?;
    let response = adapter.submit_user_outcome(action).await?;
    match common.output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&response)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(&response)?),
        OutputFormat::Terminal => println!("hyperliquid: {} completed", plan.action),
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!("validated output"),
    }
    Ok(())
}

async fn validate_action_funds(testnet: bool, plan: &OutcomeActionPlan) -> Result<()> {
    let network = HyperliquidNetwork::from_testnet(testnet);
    let metadata = crate::providers::hyperliquid::outcomes::metadata(network).await?;
    let account = ExecutionAdapter::configured_account(ExecutionVenue::HyperliquidSpot)?;
    let symbol = crate::markets::outcomes::canonical_symbol(plan.outcome, 0);
    let snapshot = ExecutionAdapter::new_for_market(
        ExecutionVenue::HyperliquidSpot,
        testnet,
        "main",
        &symbol,
    )
        .await?
        .account_snapshot(&account)
        .await?;

    match plan.action {
        "split" => {
            let outcome = find_outcome(&metadata, plan.outcome)?;
            require_quote_balance(
                &snapshot,
                &outcome.quote_token,
                plan.amount.context("split plan omitted amount")?,
            )?;
        }
        "merge" => {
            let outcome = find_outcome(&metadata, plan.outcome)?;
            let available = outcome
                .side_specs
                .iter()
                .enumerate()
                .map(|(side, _)| available_outcome_side(&snapshot, outcome.outcome, side as u8))
                .fold(f64::INFINITY, f64::min);
            require_available("complete outcome pair", available, plan.amount)?;
        }
        "mergeQuestion" => {
            let question_id = plan
                .question
                .context("merge-question plan omitted question")?;
            let question = metadata
                .questions
                .iter()
                .find(|candidate| candidate.question == question_id)
                .context("validated question disappeared from metadata")?;
            let outcome_ids = std::iter::once(question.fallback_outcome)
                .chain(question.named_outcomes.iter().copied());
            let mut available = f64::INFINITY;
            for outcome_id in outcome_ids {
                let outcome = find_outcome(&metadata, Some(outcome_id))?;
                let yes_side = named_side(outcome, "yes")?;
                available =
                    available.min(available_outcome_side(&snapshot, outcome.outcome, yes_side));
            }
            require_available("complete question basket", available, plan.amount)?;
        }
        "negate" => {
            let outcome = find_outcome(&metadata, plan.outcome)?;
            let no_side = named_side(outcome, "no")?;
            require_available(
                &format!("{} No shares", outcome.outcome),
                available_outcome_side(&snapshot, outcome.outcome, no_side),
                plan.amount,
            )?;
        }
        action => bail!("unsupported outcome action `{action}`"),
    }
    Ok(())
}

fn find_outcome(metadata: &OutcomeMetadata, outcome: Option<u32>) -> Result<&OutcomeSpec> {
    let outcome = outcome.context("outcome action plan omitted outcome")?;
    metadata
        .outcomes
        .iter()
        .find(|candidate| candidate.outcome == outcome)
        .with_context(|| format!("outcome {outcome} disappeared from metadata"))
}

fn named_side(outcome: &OutcomeSpec, name: &str) -> Result<u8> {
    outcome
        .side_specs
        .iter()
        .position(|side| {
            side.name
                .strip_prefix("template:")
                .unwrap_or(&side.name)
                .eq_ignore_ascii_case(name)
        })
        .map(|side| side as u8)
        .with_context(|| {
            format!(
                "outcome {} does not expose a `{name}` side",
                outcome.outcome
            )
        })
}

fn available_outcome_side(
    snapshot: &crate::domain::execution::AccountSnapshot,
    outcome: u32,
    side: u8,
) -> f64 {
    snapshot
        .outcome_holdings
        .iter()
        .find(|holding| holding.outcome_id == outcome && holding.side == side)
        .map_or(0.0, |holding| holding.available)
}

fn require_quote_balance(
    snapshot: &crate::domain::execution::AccountSnapshot,
    quote_token: &str,
    required: f64,
) -> Result<()> {
    let available = snapshot
        .spot_balances
        .iter()
        .find(|balance| balance.asset.eq_ignore_ascii_case(quote_token))
        .map_or(0.0, |balance| balance.available);
    require_available(quote_token, available, Some(required))
}

fn require_available(label: &str, available: f64, required: Option<f64>) -> Result<()> {
    let required = required.unwrap_or({
        if available > 0.0 {
            available
        } else {
            f64::INFINITY
        }
    });
    let tolerance = 1e-12_f64.max(required.abs() * 1e-12);
    if !required.is_finite() || available + tolerance < required {
        let requested = if required.is_finite() {
            format!("{required:.8} required")
        } else {
            "a positive balance required for maximum merge".to_string()
        };
        bail!(
            "insufficient Hyperliquid outcome {label} balance: {available:.8} available, {requested}"
        );
    }
    Ok(())
}

async fn validate_contract_reference(
    network: HyperliquidNetwork,
    plan: &OutcomeActionPlan,
) -> Result<()> {
    let metadata = crate::providers::hyperliquid::outcomes::metadata(network).await?;
    if let Some(outcome) = plan.outcome
        && !metadata
            .outcomes
            .iter()
            .any(|candidate| candidate.outcome == outcome)
    {
        bail!(
            "Hyperliquid {} outcome {outcome} does not exist",
            network.label()
        );
    }
    if let Some(question) = plan.question {
        let question_spec = metadata
            .questions
            .iter()
            .find(|candidate| candidate.question == question)
            .with_context(|| {
                format!(
                    "Hyperliquid {} question {question} does not exist",
                    network.label()
                )
            })?;
        if plan.action == "negate" {
            let outcome = plan.outcome.context("negate plan omitted outcome")?;
            let belongs = question_spec.fallback_outcome == outcome
                || question_spec.named_outcomes.contains(&outcome);
            if !belongs {
                bail!("outcome {outcome} is not an active member of question {question}");
            }
        }
    }
    Ok(())
}

fn validate_amount(amount: f64) -> Result<()> {
    if !amount.is_finite() || amount <= 0.0 {
        bail!("--amount must be > 0");
    }
    Ok(())
}

fn render(plan: &OutcomeActionPlan, output: OutputFormat, dry_run: bool) -> Result<()> {
    match output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(plan)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(plan)?),
        OutputFormat::Terminal => {
            println!(
                "outcome action plan{}",
                if dry_run {
                    " (dry run — nothing will be submitted)"
                } else {
                    ""
                }
            );
            println!("  venue:    {}", plan.venue);
            println!("  network:  {}", plan.network);
            println!("  action:   {}", plan.action);
            if let Some(question) = plan.question {
                println!("  question: {question}");
            }
            if let Some(outcome) = plan.outcome {
                println!("  outcome:  {outcome}");
            }
            println!(
                "  amount:   {}",
                plan.amount
                    .map_or_else(|| "maximum".to_string(), |amount| amount.to_string())
            );
        }
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!("validated output"),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::hyperliquid::outcomes::OutcomeSideSpec;

    fn outcome() -> OutcomeSpec {
        OutcomeSpec {
            outcome: 1001,
            name: "BTC above threshold".to_string(),
            description: "fixture".to_string(),
            side_specs: [
                OutcomeSideSpec {
                    name: "Yes".to_string(),
                },
                OutcomeSideSpec {
                    name: "No".to_string(),
                },
            ],
            quote_token: "USDC".to_string(),
            venue: None,
            deployer_fee_scale: None,
        }
    }

    #[test]
    fn outcome_action_sides_are_resolved_by_metadata_name() {
        let outcome = outcome();
        assert_eq!(named_side(&outcome, "yes").expect("yes side"), 0);
        assert_eq!(named_side(&outcome, "NO").expect("no side"), 1);
        assert!(named_side(&outcome, "draw").is_err());
    }

    #[test]
    fn maximum_merge_requires_a_positive_complete_balance() {
        assert!(require_available("pair", 4.0, None).is_ok());
        assert!(require_available("pair", 0.0, None).is_err());
        assert!(require_available("pair", 4.0, Some(4.0)).is_ok());
        assert!(require_available("pair", 4.0, Some(4.1)).is_err());
    }
}
