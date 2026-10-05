use super::*;
use crate::domain::contracts::{
    ContractAction, ContractStep, TransactionReceipt, WalletTransaction,
};
use alloy_primitives::{Address, B256, Bytes, U64, U128};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

struct Fixture {
    paths: RuntimePaths,
    state: RuntimeState,
}
impl Fixture {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!(
            "mlab-contract-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(directory.join("jobs/script_test")).unwrap();
        let paths = RuntimePaths {
            socket: directory.join("socket"),
            state: directory.join("runtime.json"),
            events: directory.join("events.jsonl"),
            log: directory.join("log"),
            jobs: directory.join("jobs"),
            directory,
        };
        let mut state: RuntimeState = serde_json::from_value(json!({"version":RUNTIME_STATE_VERSION, "pid":1, "started_at_ms":0, "tracked_orders":{}})).unwrap();
        state.script_jobs.insert(
            "script_test".into(),
            ScriptJob {
                id: "script_test".into(),
                status: ScriptJobStatus::Running,
                pid: Some(1),
                created_at_ms: 0,
                started_at_ms: Some(0),
                stopped_at_ms: None,
                last_heartbeat_ms: None,
                last_error: None,
                next_event_seq: 0,
                worker_event_cursor: 0,
                definition: ScriptJobDefinition {
                    script_name: "contract".into(),
                    original_path: "contract.py".into(),
                    snapshot_path: PathBuf::from("contract.py"),
                    language: ScriptLanguage::PythonV2,
                    python_runtime: None,
                    providers: vec![],
                    exchanges: vec!["elysium".into()],
                    execution_venues: vec![ExecutionVenue::Elysium],
                    sources: vec![format!("{}@trades@elysium", Address::repeat_byte(1))],
                    params: vec![],
                    venue: None,
                    testnet: false,
                    duration_seconds: None,
                    verbose: false,
                },
            },
        );
        persist_state(&paths, &state).unwrap();
        Self { paths, state }
    }
    fn service(&self) -> Service {
        Service {
            store: Arc::new(Store::open(&self.paths.directory).unwrap()),
            task: tokio::spawn(std::future::pending()),
        }
    }
    fn submission(&self) -> Submission {
        Submission {
            key: "buy-1".into(),
            exchange: ExecutionVenue::Elysium,
            max_gas_wei: default_gas_cap(),
            request: ContractRequest {
                market: Address::repeat_byte(1),
                slippage_bps: 50,
                action: ContractAction::Swap {
                    token_in: "native".into(),
                    amount: "0.1".into(),
                },
            },
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.paths.directory).unwrap();
    }
}

struct Provider {
    path: PathBuf,
    confirmed: AtomicBool,
    fail_broadcast: AtomicBool,
    consumed: AtomicBool,
    signs: AtomicUsize,
    broadcasts: AtomicUsize,
}
impl Provider {
    fn new(f: &Fixture) -> Self {
        Self {
            path: f.paths.directory.join("contract-jobs.json"),
            confirmed: AtomicBool::new(false),
            fail_broadcast: AtomicBool::new(true),
            consumed: AtomicBool::new(false),
            signs: AtomicUsize::new(0),
            broadcasts: AtomicUsize::new(0),
        }
    }
}
#[async_trait::async_trait]
impl ContractProvider for Provider {
    fn wallet_address(&self) -> Result<Address> {
        Ok(Address::repeat_byte(2))
    }
    async fn state(&self) -> Result<Value> {
        Ok(json!({}))
    }
    async fn balances(&self, _: Address) -> Result<Value> {
        Ok(json!({}))
    }
    async fn quote(&self, _: &ContractRequest) -> Result<Value> {
        Ok(json!({}))
    }
    async fn prepare(&self, account: Address, request: &ContractRequest) -> Result<ContractPlan> {
        Ok(ContractPlan {
            market: request.market,
            account,
            chain_id: 99801,
            quote: json!({}),
            transactions: vec![ContractStep {
                action: "swap".into(),
                transaction: WalletTransaction {
                    chain_id: U64::from(99801),
                    from: account,
                    to: request.market,
                    data: Bytes::new(),
                    value: U256::from(1),
                },
            }],
        })
    }
    async fn sign(&self, call: &WalletTransaction, _: U256) -> Result<SignedTransaction> {
        self.signs.fetch_add(1, Ordering::SeqCst);
        Ok(SignedTransaction {
            chain_id: 99801,
            hash: B256::repeat_byte(3),
            raw: Bytes::from_static(b"signed"),
            sender: call.from,
            nonce: 4,
            to: call.to,
            value: call.value,
            max_gas_cost_wei: U256::from(1),
        })
    }
    async fn broadcast(&self, signed: &SignedTransaction) -> Result<()> {
        // A broadcast must never occur before exact signed bytes exist on disk.
        let journal: Journal = serde_json::from_slice(&fs::read(&self.path)?).unwrap();
        let pending = journal
            .operations
            .values()
            .find_map(|op| op.pending.as_ref())
            .unwrap();
        assert_eq!(pending.raw, signed.raw);
        assert_eq!(pending.hash, signed.hash);
        self.broadcasts.fetch_add(1, Ordering::SeqCst);
        if self.fail_broadcast.load(Ordering::SeqCst) {
            bail!("simulated lost RPC response");
        }
        Ok(())
    }
    async fn receipt(&self, hash: B256) -> Result<Option<TransactionReceipt>> {
        Ok(self
            .confirmed
            .load(Ordering::SeqCst)
            .then_some(TransactionReceipt {
                transaction_hash: hash,
                block_hash: B256::repeat_byte(8),
                block_number: U64::from(9),
                status: U64::from(1),
                gas_used: U64::from(1),
                effective_gas_price: U128::from(1),
            }))
    }
    async fn known(&self, _: B256) -> Result<bool> {
        Ok(false)
    }
    async fn nonce(&self, _: Address) -> Result<u64> {
        Ok(if self.consumed.load(Ordering::SeqCst) {
            5
        } else {
            4
        })
    }
}

#[tokio::test]
async fn signed_operations_survive_restart_and_rebroadcast_without_resigning() {
    let fixture = Fixture::new();
    let service = fixture.service();
    let submission = fixture.submission();
    let reference = service
        .enqueue(&fixture.state, "script_test", submission.clone())
        .unwrap();
    assert_eq!(
        service
            .enqueue(&fixture.state, "script_test", submission.clone())
            .unwrap(),
        reference
    );
    let mut different = submission;
    different.request.slippage_bps = 100;
    assert!(
        service
            .enqueue(&fixture.state, "script_test", different)
            .is_err()
    );
    let provider = Provider::new(&fixture);
    let mut op = service.store.read().unwrap().operations[&reference.id].clone();
    assert!(
        prepare_step(&service.store, &provider, &mut op)
            .await
            .is_err()
    );
    assert_eq!(provider.signs.load(Ordering::SeqCst), 1);
    drop(service);
    let service = fixture.service();
    let persisted = service.store.read().unwrap().operations[&reference.id].clone();
    assert!(persisted.pending.is_some());
    provider.fail_broadcast.store(false, Ordering::SeqCst);
    reconcile(&service.store, &provider, persisted.clone())
        .await
        .unwrap();
    assert_eq!(provider.signs.load(Ordering::SeqCst), 1);
    assert_eq!(provider.broadcasts.load(Ordering::SeqCst), 2);
    provider.consumed.store(true, Ordering::SeqCst);
    assert!(
        reconcile(&service.store, &provider, persisted.clone())
            .await
            .is_err()
    );
    assert!(
        service.store.read().unwrap().operations[&reference.id]
            .pending
            .is_some()
    );
    provider.confirmed.store(true, Ordering::SeqCst);
    reconcile(&service.store, &provider, persisted)
        .await
        .unwrap();
    let journal = service.store.read().unwrap();
    assert_eq!(journal.operations[&reference.id].status, Status::Confirmed);
    assert!(journal.queue.is_empty());
    assert!(journal.events.back().unwrap().terminal);
}

#[tokio::test]
async fn event_delivery_recovers_append_before_ack_and_stopped_jobs_cannot_sign() {
    let mut fixture = Fixture::new();
    let service = fixture.service();
    let reference = service
        .enqueue(&fixture.state, "script_test", fixture.submission())
        .unwrap();
    let events = service.store.read().unwrap().events;
    service
        .flush_events(&fixture.paths, &mut fixture.state)
        .unwrap();
    assert_eq!(fixture.state.script_jobs["script_test"].next_event_seq, 1);
    service
        .store
        .update(|journal| {
            journal.events = events;
            Ok(())
        })
        .unwrap();
    fixture
        .state
        .script_jobs
        .get_mut("script_test")
        .unwrap()
        .next_event_seq = 0;
    service
        .flush_events(&fixture.paths, &mut fixture.state)
        .unwrap();
    let log =
        read_script_events_unbounded(&fixture.paths.jobs.join("script_test/events.jsonl")).unwrap();
    assert_eq!(log.len(), 1);
    assert_eq!(fixture.state.script_jobs["script_test"].next_event_seq, 1);
    fixture
        .state
        .script_jobs
        .get_mut("script_test")
        .unwrap()
        .status = ScriptJobStatus::Stopping;
    persist_state(&fixture.paths, &fixture.state).unwrap();
    let op = service.store.read().unwrap().operations[&reference.id].clone();
    assert!(service.store.authorized(&op).is_err());
    assert!(
        service
            .enqueue(&fixture.state, "script_test", fixture.submission())
            .is_err()
    );
}

#[tokio::test]
async fn authorization_requires_matching_python_job_venue_and_source() {
    let mut fixture = Fixture::new();
    let service = fixture.service();
    let mut submission = fixture.submission();
    submission.request.market = Address::repeat_byte(9);
    assert!(
        service
            .enqueue(&fixture.state, "script_test", submission)
            .is_err()
    );
    fixture
        .state
        .script_jobs
        .get_mut("script_test")
        .unwrap()
        .definition
        .execution_venues
        .clear();
    assert!(
        service
            .enqueue(&fixture.state, "script_test", fixture.submission())
            .is_err()
    );
}
