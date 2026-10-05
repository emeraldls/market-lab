use std::collections::{BTreeMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy_primitives::{Address, U256, utils::parse_units};
use anyhow::{Context, Result, bail, ensure};
use clap::Args;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::task::JoinHandle;

use crate::credentials;
use crate::providers::elysium::PoolClient;
use crate::providers::elysium::curve::FeeMarket;
use crate::providers::elysium::transactions::SignedFee;

use super::{RuntimeRequest, append_json_line, now_ms};

#[derive(Clone, Debug, Args, Deserialize, Serialize)]
pub struct FeePolicy {
    /// Baseline swap fee, before adding the volatility premium.
    #[arg(long, default_value_t = 30)]
    pub base_fee_bps: u16,
    /// Fee bps added per basis point of RMS price movement per sample.
    #[arg(long, default_value_t = 1.0)]
    pub sensitivity: f64,
    /// Number of price observations retained.
    #[arg(long, default_value_t = 20)]
    pub window: usize,
    /// Seconds between pool observations.
    #[arg(long, default_value_t = 30)]
    pub interval: u64,
    /// Minimum seconds between signed fee changes.
    #[arg(long, default_value_t = 300)]
    pub cooldown: u64,
    /// Ignore smaller fee differences.
    #[arg(long, default_value_t = 2)]
    pub min_change_bps: u16,
    /// Maximum native HYPE gas cost for one transaction.
    #[arg(long, default_value = "0.0001")]
    pub max_tx_gas_hype: String,
}

impl FeePolicy {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            (2..=1000).contains(&self.window),
            "window must be between 2 and 1000"
        );
        ensure!(
            (1..=86400).contains(&self.interval),
            "interval must be between 1 and 86400 seconds"
        );
        ensure!(
            (1..=86400).contains(&self.cooldown),
            "cooldown must be between 1 and 86400 seconds"
        );
        ensure!(
            self.sensitivity.is_finite() && self.sensitivity >= 0.0,
            "sensitivity must be finite and nonnegative"
        );
        ensure!(
            self.base_fee_bps <= 10_000 && self.min_change_bps > 0,
            "invalid fee policy"
        );
        ensure!(self.gas_cap()? > U256::ZERO, "gas cap must be positive");
        Ok(())
    }

    fn gas_cap(&self) -> Result<U256> {
        ensure!(
            !self.max_tx_gas_hype.starts_with('-'),
            "gas cap cannot be negative"
        );
        Ok(parse_units(&self.max_tx_gas_hype, 18)
            .context("invalid HYPE gas cap")?
            .into())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Sample {
    pub block: u64,
    pub timestamp: u64,
    pub log_price: f64,
}

/// RMS log returns, in bps per observation; token decimal scaling cancels in returns.
pub fn target_fee(
    samples: &VecDeque<Sample>,
    policy: &FeePolicy,
    min: u16,
    max: u16,
) -> Option<(u16, f64)> {
    if samples.len() < 2 {
        return None;
    }
    let squared = samples
        .iter()
        .zip(samples.iter().skip(1))
        .map(|(a, b)| ((b.log_price - a.log_price) * 10_000.0).powi(2))
        .sum::<f64>();
    let volatility_bps = (squared / (samples.len() - 1) as f64).sqrt();
    let fee = (f64::from(policy.base_fee_bps) + policy.sensitivity * volatility_bps)
        .round()
        .clamp(f64::from(min), f64::from(max)) as u16;
    Some((fee, volatility_bps))
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Running,
    Stopped,
    Failed,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PoolJob {
    pub id: String,
    pub pool: Address,
    #[serde(default)]
    pub market: FeeMarket,
    pub rpc_url: Option<Url>,
    pub policy: FeePolicy,
    pub status: JobStatus,
    pub created_at_ms: u64,
    pub last_observed_at_ms: Option<u64>,
    pub last_signed_at_ms: Option<u64>,
    pub last_error: Option<String>,
    pub current_fee_bps: Option<u16>,
    pub target_fee_bps: Option<u16>,
    pub volatility_bps: Option<f64>,
    pub last_transaction: Option<alloy_primitives::B256>,
    #[serde(default)]
    pub samples: VecDeque<Sample>,
}

impl PoolJob {
    fn public_value(mut self) -> Result<Value> {
        self.rpc_url = None;
        Ok(serde_json::to_value(self)?)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PendingFee {
    job_id: String,
    transaction: SignedFee,
}

#[derive(Clone, Default, Deserialize, Serialize)]
struct Registry {
    jobs: BTreeMap<String, PoolJob>,
    pending: Option<PendingFee>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum PoolRequest {
    Start {
        pool: Address,
        #[serde(default)]
        market: FeeMarket,
        rpc_url: Option<Url>,
        policy: FeePolicy,
    },
    Jobs,
    Stop {
        job_id: String,
    },
    Logs {
        job_id: String,
        limit: usize,
    },
}

struct Store {
    path: PathBuf,
    state: Mutex<Registry>,
    // Held for the daemon lifetime; a second process cannot sign from the same journal.
    _lock: File,
}

impl Store {
    fn read(&self) -> Result<Registry> {
        Ok(self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("pool journal lock poisoned"))?
            .clone())
    }

    fn update<T>(&self, update: impl FnOnce(&mut Registry) -> Result<T>) -> Result<T> {
        let mut guard = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("pool journal lock poisoned"))?;
        let mut next = guard.clone();
        let result = update(&mut next)?;
        let temporary = self.path.with_extension("tmp");
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&temporary)?;
        file.write_all(&serde_json::to_vec(&next)?)?;
        file.sync_all()?;
        fs::rename(&temporary, &self.path)?;
        File::open(
            self.path
                .parent()
                .context("pool journal has no directory")?,
        )?
        .sync_all()?;
        *guard = next;
        Ok(result)
    }

    fn event(&self, job_id: &str, event: &str, data: Value) -> Result<()> {
        append_json_line(
            &self.path.with_file_name("pool-events.jsonl"),
            &json!({
                "ts_ms": now_ms()?, "job_id": job_id, "event": event, "data": data
            }),
        )
    }
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
    pub(super) fn start(directory: &std::path::Path) -> Result<Self> {
        let path = directory.join("pool-jobs.json");
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(directory.join("pool-jobs.lock"))?;
        lock.try_lock()
            .context("another daemon owns the pool journal")?;
        let state: Registry = match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .context("invalid pool job journal; refusing to sign")?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Registry::default(),
            Err(error) => return Err(error.into()),
        };
        for job in state.jobs.values() {
            job.policy.validate()?;
        }
        let store = Arc::new(Store {
            path,
            state: Mutex::new(state),
            _lock: lock,
        });
        let worker = Arc::clone(&store);
        let task = tokio::spawn(async move {
            loop {
                if let Err(error) = tick(&worker).await {
                    eprintln!("pool jobs: {error:#}");
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
        Ok(Self { store, task })
    }

    pub(super) fn handle(&self, request: PoolRequest) -> Result<Value> {
        match request {
            PoolRequest::Start {
                pool,
                market,
                rpc_url,
                policy,
            } => {
                policy.validate()?;
                PoolClient::for_market(rpc_url.clone(), market.clone())?;
                credentials::pool::address()?;
                let created_at_ms = now_ms()?;
                let id = format!("pool_{created_at_ms:013x}");
                let job = self.store.update(|registry| {
                    ensure!(
                        !registry.jobs.contains_key(&id),
                        "retry submission: job ID already exists"
                    );
                    ensure!(
                            !registry
                                .jobs
                                .values()
                                .any(|job| job.pool == pool
                                    && matches!(job.status, JobStatus::Running)),
                            "a fee job is already running for this pool"
                        );
                    let job = PoolJob {
                        id: id.clone(),
                        pool,
                        market,
                        rpc_url,
                        policy,
                        status: JobStatus::Running,
                        created_at_ms,
                        last_observed_at_ms: None,
                        last_signed_at_ms: None,
                        last_error: None,
                        current_fee_bps: None,
                        target_fee_bps: None,
                        volatility_bps: None,
                        last_transaction: None,
                        samples: VecDeque::new(),
                    };
                    registry.jobs.insert(id.clone(), job.clone());
                    Ok(job)
                })?;
                job.public_value()
            }
            PoolRequest::Jobs => {
                let registry = self.store.read()?;
                let jobs = registry
                    .jobs
                    .into_values()
                    .map(PoolJob::public_value)
                    .collect::<Result<Vec<_>>>()?;
                let pending = registry.pending.map(|pending| {
                    let tx = pending.transaction;
                    json!({
                        "job_id": pending.job_id,
                        "transaction": {
                            "hash": tx.hash, "sender": tx.sender, "nonce": tx.nonce,
                            "pool": tx.pool, "fee_bps": tx.fee_bps,
                            "max_gas_cost_wei": tx.max_gas_cost_wei
                        }
                    })
                });
                Ok(json!({ "jobs": jobs, "pending": pending }))
            }
            PoolRequest::Stop { job_id } => {
                let job = self.store.update(|registry| {
                    let job = registry
                        .jobs
                        .get_mut(&job_id)
                        .context("pool job not found")?;
                    job.status = JobStatus::Stopped;
                    Ok(job.clone())
                })?;
                job.public_value()
            }
            PoolRequest::Logs { job_id, limit } => {
                ensure!(
                    (1..=1000).contains(&limit),
                    "log limit must be between 1 and 1000"
                );
                ensure!(
                    self.store.read()?.jobs.contains_key(&job_id),
                    "pool job not found"
                );
                let source =
                    match fs::read_to_string(self.store.path.with_file_name("pool-events.jsonl")) {
                        Ok(source) => source,
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
                        Err(error) => return Err(error.into()),
                    };
                let mut events = Vec::new();
                for line in source.lines().rev() {
                    let value: Value = serde_json::from_str(line)?;
                    if value["job_id"] == job_id {
                        events.push(value);
                    }
                    if events.len() == limit {
                        break;
                    }
                }
                events.reverse();
                Ok(json!(events))
            }
        }
    }
}

pub async fn request(request: PoolRequest) -> Result<Value> {
    super::ensure_running().await?;
    let response = super::request(RuntimeRequest::Pool { request }).await?;
    ensure!(response.ok, "{}", response.message);
    response
        .action_response
        .context("daemon omitted pool result")
}

async fn tick(store: &Store) -> Result<()> {
    let registry = store.read()?;
    if let Some(pending) = registry.pending {
        let job = registry
            .jobs
            .get(&pending.job_id)
            .context("pending fee has no job")?;
        let client = PoolClient::for_market(job.rpc_url.clone(), job.market.clone())?;
        let result =
            tokio::time::timeout(Duration::from_secs(60), reconcile(store, &client, &pending))
                .await
                .context("pending pool transaction check timed out")
                .and_then(|result| result);
        if let Err(error) = result {
            record_error(store, &pending.job_id, &error, false)?;
        }
        return Ok(());
    }
    // ponytail: one signing lane per operator; shard by operator if pool count warrants it.
    for job in registry.jobs.values() {
        if !matches!(job.status, JobStatus::Running) {
            continue;
        }
        let now = now_ms()?;
        if job
            .last_observed_at_ms
            .is_some_and(|last| now.saturating_sub(last) < job.policy.interval * 1000)
        {
            continue;
        }
        let result = tokio::time::timeout(Duration::from_secs(60), observe_job(store, job))
            .await
            .context("pool observation timed out")
            .and_then(|result| result);
        if let Err(error) = result {
            record_error(store, &job.id, &error, true)?;
        }
        if store.read()?.pending.is_some() {
            break;
        }
    }
    Ok(())
}

fn record_error(store: &Store, id: &str, error: &anyhow::Error, fail: bool) -> Result<()> {
    let message = format!("{error:#}");
    let changed = store.update(|registry| {
        let job = registry.jobs.get_mut(id).context("pool job disappeared")?;
        let changed = job.last_error.as_ref() != Some(&message);
        job.last_error = Some(message.clone());
        if fail && matches!(job.status, JobStatus::Running) {
            job.status = JobStatus::Failed;
        }
        Ok(changed)
    })?;
    if changed {
        store.event(id, "error", json!({ "message": message }))?;
    }
    Ok(())
}

async fn observe_job(store: &Store, original: &PoolJob) -> Result<()> {
    let client = PoolClient::for_market(original.rpc_url.clone(), original.market.clone())?;
    let observation = client.observe(original.pool).await?;
    let signer = credentials::pool::load()?;
    ensure!(
        observation.operator == signer.address(),
        "grant this operator permission with the pool manager wallet first"
    );
    ensure!(
        (observation.min_fee_bps..=observation.max_fee_bps).contains(&original.policy.base_fee_bps),
        "base fee is outside the pool bounds"
    );
    let now = now_ms()?;
    ensure!(
        now / 1000
            <= observation
                .timestamp
                .saturating_add(original.policy.interval.max(60) * 2),
        "RPC pool data is stale"
    );
    let log_price = observation.log_price()?;
    let proposal = store.update(|registry| {
        let job = registry
            .jobs
            .get_mut(&original.id)
            .context("pool job disappeared")?;
        if !matches!(job.status, JobStatus::Running) {
            return Ok(None);
        }
        job.last_observed_at_ms = Some(now);
        job.current_fee_bps = Some(observation.fee_bps);
        job.last_error = None;
        if let Some(last) = job.samples.back() {
            if observation.block <= last.block {
                return Ok(None);
            }
            if observation.timestamp.saturating_sub(last.timestamp) > job.policy.interval * 3 {
                job.samples.clear();
            }
        }
        job.samples.push_back(Sample {
            block: observation.block,
            timestamp: observation.timestamp,
            log_price,
        });
        while job.samples.len() > job.policy.window {
            job.samples.pop_front();
        }
        let Some((target, volatility)) = target_fee(
            &job.samples,
            &job.policy,
            observation.min_fee_bps,
            observation.max_fee_bps,
        ) else {
            return Ok(None);
        };
        job.target_fee_bps = Some(target);
        job.volatility_bps = Some(volatility);
        let cooling = job
            .last_signed_at_ms
            .is_some_and(|last| now.saturating_sub(last) < job.policy.cooldown * 1000);
        Ok(
            (!cooling && observation.fee_bps.abs_diff(target) >= job.policy.min_change_bps)
                .then_some(target),
        )
    })?;
    store.event(&original.id, "observed", json!({ "block": observation.block, "fee_bps": observation.fee_bps, "proposed_fee_bps": proposal }))?;
    let Some(fee) = proposal else {
        return Ok(());
    };
    let transaction = client
        .prepare_fee(original.pool, fee, &signer, original.policy.gas_cap()?)
        .await?;
    let committed = store.update(|registry| {
        let job = registry
            .jobs
            .get_mut(&original.id)
            .context("pool job disappeared")?;
        if !matches!(job.status, JobStatus::Running) {
            return Ok(false);
        }
        ensure!(
            registry.pending.is_none(),
            "operator already has a pending transaction"
        );
        job.last_signed_at_ms = Some(now_ms()?);
        job.last_transaction = Some(transaction.hash);
        registry.pending = Some(PendingFee {
            job_id: original.id.clone(),
            transaction: transaction.clone(),
        });
        Ok(true)
    })?;
    if committed {
        store.event(
            &original.id,
            "prepared",
            json!({ "hash": transaction.hash, "fee_bps": fee, "nonce": transaction.nonce }),
        )?;
        // Once committed, uncertainty is resolved using the same signed bytes, never a new nonce.
        if let Err(error) = client.broadcast_fee(&transaction).await {
            record_error(store, &original.id, &error, false)?;
        }
    }
    Ok(())
}

async fn reconcile(store: &Store, client: &PoolClient, pending: &PendingFee) -> Result<()> {
    let transaction = &pending.transaction;
    if let Some(receipt) = client.fee_receipt(transaction.hash).await? {
        store.event(&pending.job_id, "receipt", serde_json::to_value(&receipt)?)?;
        store.update(|registry| {
            let job = registry
                .jobs
                .get_mut(&pending.job_id)
                .context("pool job disappeared")?;
            if receipt.status.to::<u64>() != 1 {
                job.status = JobStatus::Failed;
                job.last_error = Some(format!("fee transaction {} reverted", transaction.hash));
            } else {
                job.current_fee_bps = Some(transaction.fee_bps);
                job.last_error = None;
            }
            registry.pending = None;
            Ok(())
        })?;
        return Ok(());
    }
    if client.fee_known(transaction.hash).await? {
        return Ok(());
    }
    if client.operator_nonce(transaction.sender).await? > transaction.nonce {
        bail!(
            "operator nonce was consumed without a known receipt for {}; refusing another fee transaction",
            transaction.hash
        );
    }
    client.broadcast_fee(transaction).await
}
