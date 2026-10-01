//! Read-only discovery. A discovered round is not authorization to move a live job.

use anyhow::{Context, Result, bail};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use super::{OutcomeInstrument, instruments_from_metadata, keyword_values, required_value};
use crate::providers::hyperliquid::HyperliquidNetwork;
use crate::providers::hyperliquid::outcomes::{OutcomeSpec, QuestionSpec};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RecurringSeries {
    pub kind: RecurringKind,
    pub underlying: String,
    pub period_seconds: u64,
    pub quote_token: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum RecurringKind {
    PriceBinary,
    PriceBucket,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecurringOutcome {
    Binary,
    Below,
    Between,
    Above,
    Other,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RecurringRound {
    pub series: RecurringSeries,
    pub outcome: RecurringOutcome,
    pub opens_at_ms: u64,
    pub expires_at_ms: u64,
}

impl RecurringRound {
    pub(super) fn from_metadata(
        parent: Option<&QuestionSpec>,
        instrument: &OutcomeSpec,
    ) -> Result<Option<Self>> {
        // Only protocol-owned recurring markets have a guaranteed unique series.
        if instrument.venue.is_some() {
            return Ok(None);
        }
        let (description, outcome) =
            if let Some(question) = parent.filter(|question| question.name == "Recurring") {
                let role = if question.fallback_outcome == instrument.outcome {
                    RecurringOutcome::Other
                } else {
                    let fields = keyword_values(&instrument.description)?;
                    let index: usize = required_value(&fields, "index")?.parse()?;
                    if question.named_outcomes.get(index) != Some(&instrument.outcome) {
                        bail!("recurring outcome index does not match its question");
                    }
                    match index {
                        0 => RecurringOutcome::Below,
                        1 => RecurringOutcome::Between,
                        2 => RecurringOutcome::Above,
                        _ => bail!("recurring outcome bucket must be 0, 1 or 2"),
                    }
                };
                (question.description.as_str(), role)
            } else if parent.is_none() && instrument.name == "Recurring" {
                (instrument.description.as_str(), RecurringOutcome::Binary)
            } else {
                return Ok(None);
            };
        let fields = keyword_values(description)?;
        let kind = match required_value(&fields, "class")? {
            "priceBinary" if outcome == RecurringOutcome::Binary => RecurringKind::PriceBinary,
            "priceBucket" if outcome != RecurringOutcome::Binary => RecurringKind::PriceBucket,
            class => bail!("unsupported recurring outcome class `{class}`"),
        };
        let period = required_value(&fields, "period")?;
        let (count, multiplier) = if let Some(count) = period.strip_suffix('m') {
            (count, 60_u64)
        } else if let Some(count) = period
            .strip_suffix("hr")
            .or_else(|| period.strip_suffix('h'))
        {
            (count, 3_600)
        } else if let Some(count) = period.strip_suffix('d') {
            (count, 86_400)
        } else {
            bail!("unsupported recurring period `{period}`");
        };
        let period_seconds = count
            .parse::<u64>()?
            .checked_mul(multiplier)
            .filter(|seconds| *seconds > 0)
            .context("invalid recurring period")?;
        let period_ms = period_seconds
            .checked_mul(1_000)
            .context("recurring period overflow")?;
        let expiry =
            NaiveDateTime::parse_from_str(required_value(&fields, "expiry")?, "%Y%m%d-%H%M")
                .context("invalid recurring expiry")?
                .and_utc()
                .timestamp_millis();
        let expires_at_ms =
            u64::try_from(expiry).context("recurring expiry predates Unix epoch")?;
        let opens_at_ms = expires_at_ms
            .checked_sub(period_ms)
            .context("invalid recurring window")?;
        let underlying = required_value(&fields, "underlying")?;
        if underlying.is_empty() || instrument.quote_token.is_empty() {
            bail!("recurring series requires an underlying and quote token");
        }
        Ok(Some(Self {
            series: RecurringSeries {
                kind,
                underlying: underlying.to_string(),
                period_seconds,
                quote_token: instrument.quote_token.clone(),
            },
            outcome,
            opens_at_ms,
            expires_at_ms,
        }))
    }
}

/// A series and outcome role remain stable; concrete round IDs never do.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecurringSelection {
    pub network: String,
    pub series: RecurringSeries,
    pub outcome: RecurringOutcome,
    pub side: u8,
}

impl RecurringSelection {
    pub fn from_instrument(instrument: &OutcomeInstrument) -> Result<Self> {
        let round = instrument
            .recurring
            .as_ref()
            .context("market is not protocol recurring")?;
        Ok(Self {
            network: instrument.network.clone(),
            series: round.series.clone(),
            outcome: round.outcome,
            side: instrument.side,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveredRounds {
    pub current: Option<OutcomeInstrument>,
    pub next: Option<OutcomeInstrument>,
    pub observed_at_ms: u64,
    pub changes_at_ms: Option<u64>,
}

/// No template request, guessed IDs, stale-cache fallback or execution side effects.
pub async fn discover(
    network: HyperliquidNetwork,
    selection: &RecurringSelection,
) -> Result<DiscoveredRounds> {
    if selection.network != network.label() {
        bail!("recurring discovery network does not match the selected market");
    }
    let mut metadata = crate::providers::hyperliquid::outcomes::metadata(network).await?;
    let children = metadata
        .questions
        .iter()
        .filter(|question| question.name == "Recurring")
        .flat_map(|question| {
            std::iter::once(question.fallback_outcome)
                .chain(question.named_outcomes.iter().copied())
                .chain(question.settled_named_outcomes.iter().copied())
        })
        .collect::<HashSet<_>>();
    metadata.outcomes.retain(|outcome| {
        outcome.venue.is_none()
            && (outcome.name == "Recurring" || children.contains(&outcome.outcome))
    });
    let instruments = instruments_from_metadata(network, &metadata, &[])?;
    let now = u64::try_from(chrono::Utc::now().timestamp_millis())?;
    select_rounds(selection, &instruments, now)
}

pub fn select_rounds(
    selection: &RecurringSelection,
    instruments: &[OutcomeInstrument],
    now_ms: u64,
) -> Result<DiscoveredRounds> {
    if selection.side > 1 || selection.series.period_seconds == 0 {
        bail!("invalid recurring market selection");
    }
    let mut candidates = instruments
        .iter()
        .filter(|instrument| {
            !instrument.settled
                && instrument.network == selection.network
                && instrument.side == selection.side
                && instrument.recurring.as_ref().is_some_and(|round| {
                    round.series == selection.series
                        && round.outcome == selection.outcome
                        && round.expires_at_ms > now_ms
                })
        })
        .collect::<Vec<_>>();
    candidates.sort_by_key(|instrument| instrument.recurring.as_ref().unwrap().opens_at_ms);
    for pair in candidates.windows(2) {
        let previous = pair[0].recurring.as_ref().unwrap();
        let next = pair[1].recurring.as_ref().unwrap();
        if next.opens_at_ms < previous.expires_at_ms {
            bail!("ambiguous overlapping rounds for the selected recurring market");
        }
    }
    let current = candidates
        .iter()
        .find(|instrument| instrument.recurring.as_ref().unwrap().opens_at_ms <= now_ms)
        .copied()
        .cloned();
    let next = candidates
        .iter()
        .find(|instrument| instrument.recurring.as_ref().unwrap().opens_at_ms > now_ms)
        .copied()
        .cloned();
    let boundary = current
        .as_ref()
        .map(|instrument| instrument.recurring.as_ref().unwrap().expires_at_ms)
        .or_else(|| {
            next.as_ref()
                .map(|instrument| instrument.recurring.as_ref().unwrap().opens_at_ms)
        });
    Ok(DiscoveredRounds {
        current,
        next,
        observed_at_ms: now_ms,
        changes_at_ms: boundary,
    })
}

/// Subscribe first, then snapshot. Reconnect with a fresh stream after any error.
/// Expiry changes the local selection even when the exchange publishes nothing.
pub struct RecurringMarketStream {
    network: HyperliquidNetwork,
    selection: RecurringSelection,
    updates: crate::providers::hyperliquid::ws::HyperliquidOutcomeMetaStream,
    rounds: DiscoveredRounds,
    refresh_required: bool,
}

impl RecurringMarketStream {
    pub async fn connect(
        network: HyperliquidNetwork,
        selection: RecurringSelection,
    ) -> Result<Self> {
        if selection.network != network.label() {
            bail!("recurring stream network does not match the selected market");
        }
        let updates =
            crate::providers::hyperliquid::ws::HyperliquidOutcomeMetaStream::connect(network)
                .await?;
        let rounds = discover(network, &selection).await?;
        Ok(Self {
            network,
            selection,
            updates,
            rounds,
            refresh_required: false,
        })
    }

    pub fn snapshot(&self) -> &DiscoveredRounds {
        &self.rounds
    }

    pub async fn next_rounds(&mut self) -> Result<DiscoveredRounds> {
        loop {
            let boundary = self.rounds.changes_at_ms;
            let expires = async {
                if let Some(boundary) = boundary {
                    let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
                    tokio::time::sleep(std::time::Duration::from_millis(
                        boundary.saturating_sub(now),
                    ))
                    .await;
                } else {
                    std::future::pending::<()>().await;
                }
            };
            tokio::select! {
                biased;
                _ = expires => {
                    let cached = self.rounds.current.iter().chain(self.rounds.next.iter())
                        .cloned().collect::<Vec<_>>();
                    let now = u64::try_from(chrono::Utc::now().timestamp_millis())?;
                    self.rounds = select_rounds(&self.selection, &cached, now)?;
                    return Ok(self.rounds.clone());
                }
                refreshed = discover(self.network, &self.selection), if self.refresh_required => {
                    self.rounds = refreshed?;
                    self.refresh_required = false;
                    return Ok(self.rounds.clone());
                }
                updates = self.updates.next_updates(), if !self.refresh_required => {
                    if !updates?.is_empty() {
                        // Preserve invalidation if a caller cancels during the HTTP request.
                        self.refresh_required = true;
                    }
                }
            }
        }
    }
}
