//! Durable, single-wallet contract execution. Python never receives signed bytes or keys.
use super::*;
use crate::domain::contracts::{ContractPlan, ContractRequest, SignedTransaction};
use crate::providers::execution::{ContractProvider, contract_provider};
use alloy_primitives::U256;
use serde_json::{Value, json};
use std::collections::VecDeque;
use std::fs::File;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinHandle;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Submission {
    pub key: String,
    pub exchange: ExecutionVenue,
    pub request: ContractRequest,
    #[serde(default = "default_gas_cap")]
    pub max_gas_wei: U256,
}
fn default_gas_cap() -> U256 {
    U256::from(100_000_000_000_000u64)
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Status {
    Queued,
    Submitted,
    Confirmed,
    Failed,
    Cancelled,
}

#[derive(Clone, Deserialize, Serialize)]
struct Operation {
    id: String,
    job_id: String,
    submission: Submission,
    testnet: bool,
    status: Status,
    plan: Option<ContractPlan>,
    step: usize,
    pending: Option<SignedTransaction>,
    receipts: Vec<Value>,
    error: Option<String>,
}

#[derive(Clone, Deserialize, Serialize)]
struct Event {
    seq: u64,
    operation: String,
    job_id: String,
    event_type: String,
    terminal: bool,
    data: Value,
}

#[derive(Clone, Default, Deserialize, Serialize)]
struct Journal {
    operations: BTreeMap<String, Operation>,
    queue: VecDeque<String>,
    events: VecDeque<Event>,
    next_event: u64,
}

impl Journal {
    fn event(&mut self, operation: &Operation, kind: &str, terminal: bool, data: Value) {
        self.next_event += 1;
        self.events.push_back(Event {
            seq: self.next_event,
            operation: operation.id.clone(),
            job_id: operation.job_id.clone(),
            event_type: format!("contract.{kind}"),
            terminal,
            data,
        });
    }
}

struct Store {
    path: PathBuf,
    state: Mutex<Journal>,
    _lock: File,
}
impl Store {
    fn open(directory: &Path) -> Result<Self> {
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(directory.join("contract-jobs.lock"))?;
        lock.try_lock()
            .context("another daemon owns the contract journal")?;
        let path = directory.join("contract-jobs.json");
        let state = match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .context("invalid contract journal; refusing to sign")?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Journal::default(),
            Err(error) => return Err(error.into()),
        };
        Ok(Self {
            path,
            state: Mutex::new(state),
            _lock: lock,
        })
    }
    fn read(&self) -> Result<Journal> {
        Ok(self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("contract journal lock poisoned"))?
            .clone())
    }
    fn update<T>(&self, change: impl FnOnce(&mut Journal) -> Result<T>) -> Result<T> {
        let mut guard = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("contract journal lock poisoned"))?;
        let mut next = guard.clone();
        let result = change(&mut next)?;
        let temp = self.path.with_extension("tmp");
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&temp)?;
        file.write_all(&serde_json::to_vec(&next)?)?;
        file.sync_all()?;
        fs::rename(temp, &self.path)?;
        File::open(self.path.parent().context("missing journal directory")?)?.sync_all()?;
        *guard = next;
        Ok(result)
    }
    fn save(&self, operation: Operation, event: Option<(&str, bool, Value)>) -> Result<()> {
        self.update(|journal| {
            if matches!(
                operation.status,
                Status::Confirmed | Status::Failed | Status::Cancelled
            ) {
                journal.queue.retain(|id| id != &operation.id);
            }
            if let Some((kind, terminal, data)) = event {
                journal.event(&operation, kind, terminal, data);
            }
            journal.operations.insert(operation.id.clone(), operation);
            Ok(())
        })
    }
    fn authorized(&self, operation: &Operation) -> Result<()> {
        let state: RuntimeState =
            serde_json::from_slice(&fs::read(self.path.with_file_name("runtime.json"))?)?;
        authorize(&state, &operation.job_id, &operation.submission).map(|_| ())
    }
}

fn authorize(state: &RuntimeState, job_id: &str, submission: &Submission) -> Result<bool> {
    anyhow::ensure!(
        !submission.key.is_empty() && submission.key.len() <= 128,
        "contract key must be 1-128 bytes"
    );
    anyhow::ensure!(
        submission.max_gas_wei > U256::ZERO,
        "gas cap must be positive"
    );
    anyhow::ensure!(
        ExecutionAdapter::capabilities(submission.exchange).contract_actions,
        "venue does not support contract execution"
    );
    let job = state
        .script_jobs
        .get(job_id)
        .context("script job not found")?;
    anyhow::ensure!(
        matches!(
            job.status,
            ScriptJobStatus::Starting | ScriptJobStatus::Running
        ),
        "script job is not running"
    );
    anyhow::ensure!(
        job.definition.language == ScriptLanguage::PythonV2,
        "contract execution requires Python V2"
    );
    anyhow::ensure!(
        job.definition
            .execution_venues
            .contains(&submission.exchange),
        "contract exchange is not declared by this script"
    );
    anyhow::ensure!(
        crate::scripting::inputs::parse_source_configs(&job.definition.sources)?
            .values()
            .any(|source| source
                .exchange
                .eq_ignore_ascii_case(submission.exchange.market_data_id().as_str())
                && source
                    .market_symbol()
                    .eq_ignore_ascii_case(&submission.request.market.to_string())),
        "contract market is not declared by this script's sources"
    );
    submission
        .exchange
        .spec()?
        .validate_network(job.definition.testnet)?;
    Ok(job.definition.testnet)
}

pub(super) struct Service {
    store: Arc<Store>,
    task: JoinHandle<()>,
}
impl Drop for Service {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Service {
    pub(super) fn start(directory: &Path) -> Result<Self> {
        let store = Arc::new(Store::open(directory)?);
        let worker = Arc::clone(&store);
        let task = tokio::spawn(async move {
            loop {
                let result = tokio::time::timeout(Duration::from_secs(60), tick(&worker)).await;
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => eprintln!("contract execution pending: {error:#}"),
                    Err(_) => eprintln!(
                        "contract execution timed out; signed transactions remain in the journal"
                    ),
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });
        Ok(Self { store, task })
    }
    pub(super) fn enqueue(
        &self,
        state: &RuntimeState,
        job_id: &str,
        submission: Submission,
    ) -> Result<ScriptOrderRef> {
        let testnet = authorize(state, job_id, &submission)?;
        let id = local_order_id(job_id, &submission.key);
        let reference = ScriptOrderRef {
            id: id.clone(),
            key: submission.key.clone(),
        };
        self.store.update(|journal| {
            if let Some(existing) = journal.operations.get(&id) {
                anyhow::ensure!(
                    existing.job_id == job_id && existing.submission == submission,
                    "contract key was already used for a different request"
                );
                return Ok(reference);
            }
            let operation = Operation {
                id: id.clone(),
                job_id: job_id.into(),
                submission,
                testnet,
                status: Status::Queued,
                plan: None,
                step: 0,
                pending: None,
                receipts: vec![],
                error: None,
            };
            journal.event(&operation, "queued", false, json!({}));
            journal.operations.insert(id.clone(), operation);
            journal.queue.push_back(id);
            Ok(reference)
        })
    }
    pub(super) fn flush_events(
        &self,
        paths: &RuntimePaths,
        state: &mut RuntimeState,
    ) -> Result<()> {
        let snapshot = self.store.read()?;
        for event in snapshot.events {
            let operation = snapshot
                .operations
                .get(&event.operation)
                .context("contract event has no operation")?;
            let path = script_job_directory(paths, &event.job_id)?.join("events.jsonl");
            // Recover append-before-ack crashes without delivering a second copy.
            let existing = read_script_events_unbounded(&path)?;
            let job = state
                .script_jobs
                .get_mut(&event.job_id)
                .context("contract event has no script job")?;
            let latest = existing.iter().map(|e| e.seq).max().unwrap_or(0);
            job.next_event_seq = job.next_event_seq.max(latest);
            if !existing
                .iter()
                .any(|e| e.data["contractEventId"] == event.seq)
            {
                job.next_event_seq += 1;
                let output = ScriptExecutionEvent {
                    seq: job.next_event_seq,
                    job_id: event.job_id.clone(),
                    ts_ms: now_ms()?,
                    event_type: event.event_type.clone(),
                    order_id: Some(operation.id.clone()),
                    key: Some(operation.submission.key.clone()),
                    symbol: Some(operation.submission.request.market.to_string()),
                    venue: Some(operation.submission.exchange),
                    venue_order_id: None,
                    status: Some(event.event_type.trim_start_matches("contract.").into()),
                    terminal: event.terminal,
                    data: json!({"contractEventId":event.seq, "result":event.data}),
                };
                append_json_line(&path, &output)?;
            }
            File::open(&path)?.sync_all()?;
            persist_state(paths, state)?;
            self.store.update(|journal| {
                journal.events.retain(|pending| pending.seq != event.seq);
                Ok(())
            })?;
        }
        Ok(())
    }
}

fn read_script_events_unbounded(path: &Path) -> Result<Vec<ScriptExecutionEvent>> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
        Err(error) => return Err(error.into()),
    };
    text.lines()
        .map(|line| serde_json::from_str(line).context("invalid script execution event"))
        .collect()
}

async fn tick(store: &Store) -> Result<()> {
    let journal = store.read()?;
    let Some(id) = journal.queue.front() else {
        return Ok(());
    };
    let mut operation = journal
        .operations
        .get(id)
        .context("queued contract operation missing")?
        .clone();
    if operation.pending.is_none()
        && let Err(error) = store.authorized(&operation)
    {
        operation.status = Status::Cancelled;
        operation.error = Some(error.to_string());
        return store.save(
            operation,
            Some(("cancelled", true, json!({"error":error.to_string()}))),
        );
    }
    let result = async {
        let provider = contract_provider(
            operation.submission.exchange,
            operation.testnet,
            operation.submission.request.market,
        )
        .await?;
        if operation.pending.is_some() {
            return reconcile(store, provider.as_ref(), operation.clone()).await;
        }
        prepare_step(store, provider.as_ref(), &mut operation).await
    }
    .await;
    match result {
        Ok(()) => Ok(()),
        Err(error) => {
            // Never discard signed bytes on a broadcast error, timeout or ambiguous RPC result.
            if store.read()?.operations[id].pending.is_some() {
                return Err(error);
            }
            operation.status = Status::Failed;
            operation.error = Some(format!("{error:#}"));
            store.save(
                operation,
                Some(("failed", true, json!({"error":format!("{error:#}")}))),
            )
        }
    }
}

async fn prepare_step(
    store: &Store,
    provider: &dyn ContractProvider,
    operation: &mut Operation,
) -> Result<()> {
    if operation.plan.is_none() {
        operation.plan = Some(
            provider
                .prepare(provider.wallet_address()?, &operation.submission.request)
                .await?,
        );
        anyhow::ensure!(
            !operation.plan.as_ref().unwrap().transactions.is_empty(),
            "empty contract plan"
        );
        store.save(operation.clone(), None)?;
    }
    let plan = operation.plan.as_ref().context("missing contract plan")?;
    let step = plan
        .transactions
        .get(operation.step)
        .context("missing contract step")?;
    let signed = provider
        .sign(&step.transaction, operation.submission.max_gas_wei)
        .await?;
    store.authorized(operation)?;
    let data = json!({"hash":signed.hash, "step":operation.step, "action":step.action, "quote":plan.quote});
    operation.pending = Some(signed.clone());
    operation.status = Status::Submitted;
    store.save(operation.clone(), Some(("submitted", false, data)))?;
    provider.broadcast(&signed).await
}

async fn reconcile(
    store: &Store,
    provider: &dyn ContractProvider,
    mut operation: Operation,
) -> Result<()> {
    let signed = operation
        .pending
        .as_ref()
        .context("missing signed transaction")?;
    if let Some(receipt) = provider.receipt(signed.hash).await? {
        let success = receipt.status.to::<u64>() == 1;
        let receipt = serde_json::to_value(receipt)?;
        operation.receipts.push(receipt.clone());
        operation.pending = None;
        operation.step += 1;
        let complete = operation.step
            == operation
                .plan
                .as_ref()
                .context("missing prepared plan")?
                .transactions
                .len();
        operation.status = if !success {
            Status::Failed
        } else if complete {
            Status::Confirmed
        } else {
            Status::Queued
        };
        if !success {
            operation.error = Some("contract transaction reverted".into());
        }
        let kind = if !success {
            "failed"
        } else if complete {
            "confirmed"
        } else {
            "step_confirmed"
        };
        let data =
            json!({"receipt":receipt, "receipts":operation.receipts, "error":operation.error});
        return store.save(operation, Some((kind, !success || complete, data)));
    }
    if provider.known(signed.hash).await? {
        return Ok(());
    }
    anyhow::ensure!(
        provider.nonce(signed.sender).await? <= signed.nonce,
        "wallet nonce was consumed without the receipt for {}; keeping the signed transaction for recovery",
        signed.hash
    );
    provider.broadcast(signed).await
}

pub async fn submit(job_id: &str, submission: Submission) -> Result<ScriptOrderRef> {
    let response = super::request(RuntimeRequest::ScriptContract {
        job_id: job_id.into(),
        submission,
    })
    .await?;
    anyhow::ensure!(response.ok, "{}", response.message);
    serde_json::from_value(
        response
            .action_response
            .context("missing contract execution result")?,
    )
    .context("invalid contract execution result")
}

#[cfg(test)]
#[path = "contracts_tests.rs"]
mod tests;
