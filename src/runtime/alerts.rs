//! Tokens stay in mlabd. Hooks enqueue bounded, non-trading notification requests.
use super::*;
use crate::alerts::{AlertRequest, TelegramClient};
use crate::credentials::telegram;
use crate::scripting::language::ScriptLanguage;
use anyhow::ensure;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use tokio::task::JoinHandle;

const JOURNAL: &str = "alert-cooldowns.json";
const MAX_KEYS: usize = 4096;
const QUEUE_CAPACITY: usize = 64;

#[derive(Default, Deserialize, Serialize)]
struct Cooldowns {
    until: BTreeMap<String, u64>,
    next_send_ms: u64,
}

impl Cooldowns {
    fn reserve(&mut self, job_id: &str, request: &AlertRequest, now: u64) -> Result<bool> {
        self.until.retain(|_, until| *until > now);
        let key = format!("{job_id}:{}", request.key);
        if self.until.get(&key).is_some_and(|until| *until > now) {
            return Ok(false);
        }
        ensure!(
            now >= self.next_send_ms,
            "Telegram alert rate limit: at most one accepted message per second"
        );
        ensure!(
            self.until.len() < MAX_KEYS,
            "Telegram alert cooldown capacity reached"
        );
        self.until
            .insert(key, now.saturating_add(request.cooldown_seconds * 1000));
        self.next_send_ms = now.saturating_add(1000);
        Ok(true)
    }
}

pub(super) struct Service {
    sender: mpsc::Sender<(String, AlertRequest)>,
    worker: JoinHandle<()>,
    cooldowns: Cooldowns,
    directory: PathBuf,
    credential_directory: PathBuf,
}

impl Service {
    pub(super) fn start(directory: &Path) -> Result<Self> {
        Self::start_with_client(directory, &telegram::directory()?, TelegramClient::new()?)
    }

    fn start_with_client(
        directory: &Path,
        credential_directory: &Path,
        client: TelegramClient,
    ) -> Result<Self> {
        let cooldowns: Cooldowns =
            match credentials::read_credential_at(directory, JOURNAL, "alert cooldown")? {
                Some(encoded) => {
                    serde_json::from_str(&encoded).context("invalid alert cooldown journal")?
                }
                None => Cooldowns::default(),
            };
        ensure!(
            cooldowns.until.len() <= MAX_KEYS,
            "alert cooldown journal exceeds its key limit"
        );
        let (sender, mut receiver) = mpsc::channel::<(String, AlertRequest)>(QUEUE_CAPACITY);
        let worker_credentials = credential_directory.to_path_buf();
        let worker_jobs = directory.join("jobs");
        let worker = tokio::spawn(async move {
            while let Some((job_id, request)) = receiver.recv().await {
                let result = async {
                    let credential = telegram::load_at(&worker_credentials)?
                        .context("Telegram configuration was removed; message not sent")?;
                    client.send(&credential, &request.message).await
                }
                .await;
                let value = match result {
                    Ok(message_id) => json!({"status":"delivered", "message_id":message_id}),
                    Err(error) => json!({"status":"failed", "error":format!("{error:#}")}),
                };
                let event = json!({"type":"script.alert.result", "source":"telegram", "key":request.key, "ts_ms":now_ms().unwrap_or(0), "result":value});
                if let Err(error) =
                    append_json_line(&worker_jobs.join(&job_id).join("output.jsonl"), &event)
                {
                    eprintln!("could not record Telegram alert result: {error:#}");
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        });
        Ok(Self {
            sender,
            worker,
            cooldowns,
            directory: directory.into(),
            credential_directory: credential_directory.into(),
        })
    }

    pub(super) fn enqueue(
        &mut self,
        state: &RuntimeState,
        job_id: &str,
        request: AlertRequest,
    ) -> Result<Value> {
        request.validate()?;
        ensure!(
            !job_id.is_empty()
                && job_id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_'),
            "invalid alert script job ID"
        );
        let job = state
            .script_jobs
            .get(job_id)
            .context("alert script job does not exist")?;
        ensure!(
            matches!(
                job.status,
                ScriptJobStatus::Starting | ScriptJobStatus::Running
            ),
            "alert script job is not active"
        );
        ensure!(
            job.definition.language == ScriptLanguage::PythonV2,
            "alerts require Python V2"
        );
        ensure!(
            telegram::load_at(&self.credential_directory)?.is_some(),
            "Telegram is not configured; run `mlab alert setup`"
        );
        let permit = self
            .sender
            .try_reserve()
            .context("Telegram alert queue is full; message not queued")?;
        let before = serde_json::to_vec(&self.cooldowns)?;
        if !self.cooldowns.reserve(job_id, &request, now_ms()?)? {
            return Ok(json!({"status":"suppressed", "key":request.key}));
        }
        // Record attempts before sending. Ambiguous network failures never trigger automatic retries.
        if let Err(error) = credentials::write_credential_at(
            &self.directory,
            JOURNAL,
            &serde_json::to_vec(&self.cooldowns)?,
            "alert cooldown",
        ) {
            self.cooldowns = serde_json::from_slice(&before)?;
            return Err(error);
        }
        let result = json!({"status":"queued", "key":request.key});
        permit.send((job_id.into(), request));
        Ok(result)
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

pub async fn submit(job_id: &str, alert: AlertRequest) -> Result<Value> {
    let response = super::request(RuntimeRequest::ScriptAlert {
        job_id: job_id.into(),
        alert,
    })
    .await?;
    ensure!(response.ok, "{}", response.message);
    response
        .action_response
        .context("daemon omitted alert result")
}
