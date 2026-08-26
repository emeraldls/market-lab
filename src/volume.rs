use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use rand_core::{OsRng, RngCore};
use serde::{Deserialize, Serialize};
use sha3::{Digest, Keccak256};
use tokio::sync::mpsc;

use crate::domain::execution::ExecutionVenue;
use crate::providers::marketlab_cloud::MarketLabCloudClient;
use crate::venues::VenueMarket;

pub const VOLUME_SCHEMA_VERSION: u8 = 1;
const CHANNEL_CAPACITY: usize = 2_048;
const BATCH_SIZE: usize = 100;
const RECENT_EVENT_LIMIT: usize = 10_000;
const EXPORT_INTERVAL_SECS: u64 = 10;

/// Public fill-volume contract consumed by the Market Lab metrics service.
///
/// Wallet, order, trade, price, and quantity identifiers deliberately never
/// leave the daemon. Only the USD-equivalent notional required for aggregate
/// usage metrics is exported.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FillVolumeEvent {
    pub event_id: String,
    pub venue: String,
    pub network: String,
    pub market: String,
    pub symbol: String,
    pub notional_usd: String,
    pub maker: bool,
    pub filled_at_ms: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FillVolumeBatch {
    pub schema_version: u8,
    pub installation_id: String,
    pub client_version: String,
    pub sent_at_ms: u64,
    pub fills: Vec<FillVolumeEvent>,
}

/// Internal input. Sensitive provider identifiers are used only to derive an
/// opaque event id inside the daemon and are never serialized into a batch.
pub struct FillVolumeInput {
    pub venue: ExecutionVenue,
    pub testnet: bool,
    pub account: String,
    pub order_id: String,
    pub trade_id: Option<String>,
    pub symbol: String,
    pub amount: f64,
    pub price: f64,
    pub maker: bool,
    pub filled_at_ms: u64,
}

#[derive(Clone, Default)]
pub struct VolumeExporter {
    sender: Option<mpsc::Sender<FillVolumeInput>>,
}

impl VolumeExporter {
    /// Starts the durable exporter when an ingestion URL was embedded at build
    /// time or supplied through `MLAB_VOLUME_INGEST_URL` for development.
    pub fn start(market_lab_home: &Path) -> Result<Self> {
        let Some(cloud) = MarketLabCloudClient::configured()? else {
            return Ok(Self::default());
        };
        let directory = market_lab_home.join("telemetry");
        secure_directory(&directory)?;
        let identity = load_or_create_identity(&directory.join("identity.json"))?;
        let store_path = directory.join("volume.json");
        let store = load_store(&store_path)?;
        let (sender, receiver) = mpsc::channel(CHANNEL_CAPACITY);
        tokio::spawn(run_exporter(cloud, identity, store_path, store, receiver));
        Ok(Self {
            sender: Some(sender),
        })
    }

    /// Enqueues without awaiting disk or network I/O. Failure affects metrics
    /// only and must never delay or reject an exchange operation.
    pub fn record(&self, input: FillVolumeInput) {
        let Some(sender) = &self.sender else {
            return;
        };
        if let Err(error) = sender.try_send(input) {
            eprintln!("volume telemetry warning: fill queue unavailable: {error}");
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.sender.is_some()
    }
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct InstallationIdentity {
    installation_id: String,
    secret: String,
}

#[derive(Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct VolumeStore {
    #[serde(default)]
    pending: Vec<FillVolumeEvent>,
    #[serde(default)]
    recent_event_ids: Vec<String>,
}

async fn run_exporter(
    cloud: MarketLabCloudClient,
    identity: InstallationIdentity,
    store_path: PathBuf,
    mut store: VolumeStore,
    mut receiver: mpsc::Receiver<FillVolumeInput>,
) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(EXPORT_INTERVAL_SECS));
    loop {
        tokio::select! {
            input = receiver.recv() => {
                let Some(input) = input else {
                    let _ = export_pending(&cloud, &identity, &store_path, &mut store).await;
                    return;
                };
                match build_event(&identity, input) {
                    Ok(Some(event)) => {
                        let known = store.pending.iter().any(|pending| pending.event_id == event.event_id)
                            || store.recent_event_ids.iter().any(|event_id| event_id == &event.event_id);
                        if !known {
                            store.pending.push(event);
                            if let Err(error) = persist_store(&store_path, &store) {
                                eprintln!("volume telemetry warning: failed to persist fill: {error:#}");
                            }
                        }
                        if store.pending.len() >= BATCH_SIZE
                            && let Err(error) = export_pending(
                                &cloud,
                                &identity,
                                &store_path,
                                &mut store,
                            ).await
                        {
                            eprintln!("volume telemetry warning: export failed: {error:#}");
                        }
                    }
                    Ok(None) => {}
                    Err(error) => eprintln!("volume telemetry warning: ignored invalid fill: {error:#}"),
                }
            }
            _ = interval.tick() => {
                if let Err(error) = export_pending(
                    &cloud,
                    &identity,
                    &store_path,
                    &mut store,
                ).await
                {
                    eprintln!("volume telemetry warning: export failed: {error:#}");
                }
            }
        }
    }
}

fn build_event(
    identity: &InstallationIdentity,
    input: FillVolumeInput,
) -> Result<Option<FillVolumeEvent>> {
    let spec = input.venue.spec()?;
    let network = spec.network_label(input.testnet);
    // Restore mainnet-only volume by uncommenting: if network != "mainnet" { return Ok(None); }
    if !input.amount.is_finite()
        || input.amount <= 0.0
        || !input.price.is_finite()
        || input.price <= 0.0
    {
        bail!("fill amount and price must be finite and positive");
    }
    let notional = input.amount.abs() * input.price;
    if !notional.is_finite() {
        bail!("fill notional is not finite");
    }
    let provider_fill_id = input.trade_id.as_deref().unwrap_or(&input.order_id);
    let fallback = format!(
        "{}:{}:{}",
        input.filled_at_ms,
        input.amount.to_bits(),
        input.price.to_bits()
    );
    let mut digest = Keccak256::new();
    digest.update(identity.secret.as_bytes());
    digest.update([0]);
    digest.update(input.venue.as_str().as_bytes());
    digest.update([0]);
    digest.update(network.as_bytes());
    digest.update([0]);
    digest.update(input.account.as_bytes());
    digest.update([0]);
    digest.update(input.order_id.as_bytes());
    digest.update([0]);
    digest.update(provider_fill_id.as_bytes());
    if input.trade_id.is_none() {
        digest.update([0]);
        digest.update(fallback.as_bytes());
    }

    Ok(Some(FillVolumeEvent {
        event_id: hex::encode(digest.finalize()),
        venue: input.venue.to_string(),
        network: network.to_string(),
        market: market_name(spec.market).to_string(),
        symbol: input.symbol.to_ascii_uppercase(),
        notional_usd: format!("{notional:.8}"),
        maker: input.maker,
        filled_at_ms: input.filled_at_ms,
    }))
}

async fn export_pending(
    cloud: &MarketLabCloudClient,
    identity: &InstallationIdentity,
    store_path: &Path,
    store: &mut VolumeStore,
) -> Result<()> {
    if store.pending.is_empty() {
        return Ok(());
    }
    let count = store.pending.len().min(BATCH_SIZE);
    let batch = FillVolumeBatch {
        schema_version: VOLUME_SCHEMA_VERSION,
        installation_id: identity.installation_id.clone(),
        client_version: env!("CARGO_PKG_VERSION").to_string(),
        sent_at_ms: now_ms()?,
        fills: store.pending[..count].to_vec(),
    };
    cloud.ingest_volume(&batch).await?;
    eprintln!("volume telemetry: exported {count} fill(s)");

    let delivered = store.pending.drain(..count).collect::<Vec<_>>();
    store
        .recent_event_ids
        .extend(delivered.into_iter().map(|event| event.event_id));
    if store.recent_event_ids.len() > RECENT_EVENT_LIMIT {
        let remove = store.recent_event_ids.len() - RECENT_EVENT_LIMIT;
        store.recent_event_ids.drain(..remove);
    }
    persist_store(store_path, store)
}

fn market_name(market: VenueMarket) -> &'static str {
    match market {
        VenueMarket::Perpetual => "perpetual",
        VenueMarket::Hip3 => "hip3",
        VenueMarket::Spot => "spot",
        VenueMarket::Outcome => "outcome",
    }
}

fn load_or_create_identity(path: &Path) -> Result<InstallationIdentity> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .with_context(|| format!("failed to parse {}", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut id = [0_u8; 16];
            let mut secret = [0_u8; 32];
            OsRng.fill_bytes(&mut id);
            OsRng.fill_bytes(&mut secret);
            let identity = InstallationIdentity {
                installation_id: hex::encode(id),
                secret: hex::encode(secret),
            };
            secure_write_json(path, &identity)?;
            Ok(identity)
        }
        Err(error) => Err(error).with_context(|| format!("failed to read {}", path.display())),
    }
}

fn load_store(path: &Path) -> Result<VolumeStore> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .with_context(|| format!("failed to parse {}", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(VolumeStore::default()),
        Err(error) => Err(error).with_context(|| format!("failed to read {}", path.display())),
    }
}

fn persist_store(path: &Path, store: &VolumeStore) -> Result<()> {
    secure_write_json(path, store)
}

fn secure_directory(path: &Path) -> Result<()> {
    fs::create_dir_all(path).with_context(|| format!("failed to create {}", path.display()))?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("failed to secure {}", path.display()))
}

fn secure_write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let temporary = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(value).context("failed to encode telemetry state")?;
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .open(&temporary)
        .with_context(|| format!("failed to write {}", temporary.display()))?;
    file.write_all(&bytes)
        .with_context(|| format!("failed to write {}", temporary.display()))?;
    file.sync_all()
        .with_context(|| format!("failed to sync {}", temporary.display()))?;
    fs::rename(&temporary, path)
        .with_context(|| format!("failed to replace {}", path.display()))?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("failed to secure {}", path.display()))
}

fn now_ms() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_millis()
        .try_into()
        .context("timestamp does not fit u64")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> InstallationIdentity {
        InstallationIdentity {
            installation_id: "installation".to_string(),
            secret: "secret".to_string(),
        }
    }

    #[test]
    fn exported_fill_omits_private_execution_identifiers() {
        let event = build_event(
            &identity(),
            FillVolumeInput {
                venue: ExecutionVenue::Hyperliquid,
                testnet: false,
                account: "0xprivate".to_string(),
                order_id: "1234".to_string(),
                trade_id: Some("5678".to_string()),
                symbol: "btc".to_string(),
                amount: 0.01,
                price: 65_000.0,
                maker: true,
                filled_at_ms: 1_780_000_000_000,
            },
        )
        .expect("event builds")
        .expect("mainnet event");
        let json = serde_json::to_string(&event).expect("event serializes");
        assert_eq!(event.notional_usd, "650.00000000");
        assert!(!json.contains("0xprivate"));
        assert!(!json.contains("1234"));
        assert!(!json.contains("5678"));
    }

    #[test]
    fn event_identity_is_stable_for_live_and_recovered_copies() {
        let input = || FillVolumeInput {
            venue: ExecutionVenue::Hyperliquid,
            testnet: false,
            account: "account".to_string(),
            order_id: "order".to_string(),
            trade_id: Some("trade".to_string()),
            symbol: "ETH".to_string(),
            amount: 1.25,
            price: 2_000.0,
            maker: false,
            filled_at_ms: 123,
        };
        let first = build_event(&identity(), input()).unwrap().unwrap();
        let recovered = build_event(&identity(), input()).unwrap().unwrap();
        assert_eq!(first.event_id, recovered.event_id);
    }

    #[test]
    fn testnet_fills_are_temporarily_reported_for_volume_testing() {
        let event = build_event(
            &identity(),
            FillVolumeInput {
                venue: ExecutionVenue::Hyperliquid,
                testnet: true,
                account: "account".to_string(),
                order_id: "order".to_string(),
                trade_id: None,
                symbol: "BTC".to_string(),
                amount: 1.0,
                price: 1.0,
                maker: false,
                filled_at_ms: 1,
            },
        )
        .expect("valid fill");
        assert_eq!(event.expect("testnet event").network, "testnet");
    }

    #[test]
    fn batch_contract_uses_camel_case_wire_fields() {
        let batch = FillVolumeBatch {
            schema_version: 1,
            installation_id: "install".to_string(),
            client_version: "0.1.2".to_string(),
            sent_at_ms: 123,
            fills: Vec::new(),
        };
        let json = serde_json::to_value(batch).expect("batch serializes");
        assert_eq!(json["schemaVersion"], 1);
        assert_eq!(json["installationId"], "install");
        assert_eq!(json["clientVersion"], "0.1.2");
        assert_eq!(json["sentAtMs"], 123);
    }
}
