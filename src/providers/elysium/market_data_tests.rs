use super::*;
use crate::scripting::inputs::{parse_source_configs, validate_source_configs_for_run};
use crate::scripting::manifest::ScriptManifest;
use crate::scripting::market_data::ScriptTrade;
use alloy_primitives::address;

fn identity(curve: bool) -> MarketIdentity {
    MarketIdentity {
        address: address!("59675174f1700677e608f86016f1cea764e1abfa"),
        curve,
        base: TradeAsset {
            address: Some(Address::repeat_byte(1).to_string()),
            symbol: "ABC".into(),
            decimals: 6,
        },
        quote: TradeAsset {
            address: if curve {
                None
            } else {
                Some(Address::repeat_byte(2).to_string())
            },
            symbol: "HYPE".into(),
            decimals: 18,
        },
    }
}

fn block() -> Block {
    Block {
        number: U64::from(120),
        hash: B256::repeat_byte(3),
        timestamp: U64::from(1700000000),
    }
}

fn log<E: SolEvent>(event: E) -> TradeLog {
    let data = event.encode_log_data();
    TradeLog {
        address: identity(false).address,
        topics: data.topics().to_vec(),
        data: data.data,
        block_number: block().number,
        block_hash: block().hash,
        transaction_hash: B256::repeat_byte(4),
        transaction_index: U64::ZERO,
        log_index: U64::from(2),
        removed: false,
    }
}

fn buy_log() -> TradeLog {
    log(Buy {
        sender: Address::repeat_byte(5),
        recipient: Address::repeat_byte(6),
        tokens: U256::from(2_000_001),
        hype: U256::from(1_010_000_000_000_000_001_u64),
        fee: U256::from(10_000_000_000_000_000_u64),
    })
}

#[test]
fn bonding_trades_preserve_exact_amounts_fees_price_state_and_volume() {
    let market = identity(true);
    let state = json!({"current_price": {"formatted":"0.75"}, "sold":{"formatted":"2.000001"}});
    let mut volume = Volume::default();
    let first = normalize(
        &buy_log(),
        &market,
        &block(),
        &state,
        &mut volume,
        100,
        99801,
    )
    .unwrap();
    let payload = serde_json::to_value(ScriptTrade::from_tick(&first)).unwrap();
    assert_eq!(payload["onchain"]["base_amount"], "2.000001");
    assert_eq!(payload["onchain"]["quote_amount"], "1.010000000000000001");
    assert_eq!(payload["onchain"]["fee"]["amount"], "0.01");
    assert_eq!(payload["onchain"]["current_price"], 0.75);
    assert_ne!(payload["price"], payload["onchain"]["current_price"]);
    assert_eq!(payload["onchain"]["state"], state);
    assert_eq!(payload["onchain"]["side"], "buy");
    assert_eq!(payload["onchain"]["volume"]["from_block"], 100);

    let mut sale = log(Sell {
        sender: Address::repeat_byte(6),
        recipient: Address::repeat_byte(6),
        tokens: U256::from(1_000_000),
        hype: U256::from(495_000_000_000_000_000_u64),
        fee: U256::from(5_000_000_000_000_000_u64),
    });
    sale.log_index = U64::from(3);
    let second = normalize(&sale, &market, &block(), &state, &mut volume, 100, 99801).unwrap();
    let data = second.onchain.unwrap();
    assert_eq!(data.side, "sell");
    assert_eq!(data.quote_amount, "0.495"); // Event contains payout, not payout + fee.
    assert_eq!(data.volume.base, "3.000001");
    assert_eq!(data.volume.quote, "1.505000000000000001");
    assert_eq!(data.volume.trades, 2);
    assert_eq!(data.volume.through_log_index, 3);
}

#[test]
fn pool_trades_orient_side_and_fee_in_the_actual_input_asset() {
    let market = identity(false);
    let state = json!({"reserves": [{"amount":{"formatted":"10"}}, {"amount":{"formatted":"20"}}]});
    for buy in [false, true] {
        let event = Swap {
            sender: Address::repeat_byte(5),
            recipient: Address::repeat_byte(6),
            tokenIn: if buy {
                Address::repeat_byte(2)
            } else {
                Address::repeat_byte(1)
            },
            amountIn: if buy {
                U256::from(2_000_000_000_000_000_000_u64)
            } else {
                U256::from(1_000_000)
            },
            amountOut: if buy {
                U256::from(1_000_000)
            } else {
                U256::from(2_000_000_000_000_000_000_u64)
            },
            feeAmount: if buy {
                U256::from(6_000_000_000_000_000_u64)
            } else {
                U256::from(3_000)
            },
        };
        let trade = normalize(
            &log(event),
            &market,
            &block(),
            &state,
            &mut Volume::default(),
            100,
            99801,
        )
        .unwrap();
        assert_eq!(trade.price, 2.0);
        assert_eq!(trade.size, 1.0);
        assert_eq!(trade.taker_buy, buy);
        let data = trade.onchain.unwrap();
        assert_eq!(data.base_amount, "1");
        assert_eq!(data.quote_amount, "2");
        assert_eq!(data.current_price, Some(2.0));
        assert_eq!(
            data.fee.asset,
            if buy {
                market.quote.clone()
            } else {
                market.base.clone()
            }
        );
        assert_eq!(data.fee.amount, if buy { "0.006" } else { "0.003" });
    }
}

#[test]
fn rejects_reverted_removed_foreign_or_mismatched_block_logs() {
    let state = json!({"current_price":{"formatted":"1"}});
    let mut event = buy_log();
    event.removed = true;
    assert!(decode_trade(&event, &identity(true)).is_err());
    event.removed = false;
    event.address = Address::ZERO;
    assert!(decode_trade(&event, &identity(true)).is_err());
    event = buy_log();
    event.block_hash = B256::ZERO;
    assert!(
        normalize(
            &event,
            &identity(true),
            &block(),
            &state,
            &mut Volume::default(),
            100,
            99801
        )
        .is_err()
    );
    let empty_state =
        json!({"reserves": [{"amount":{"formatted":"0"}}, {"amount":{"formatted":"0"}}]});
    assert_eq!(identity(false).current_price(&empty_state).unwrap(), None);
}

#[test]
fn elysium_sources_are_factory_market_addresses_and_trade_capabilities_only() {
    let address = identity(true).address.to_string();
    let config = parse_source_configs(&[format!("{address}@trades@elysium")]).unwrap();
    let manifest: ScriptManifest =
        serde_json::from_value(json!({"name":"market-watch", "version":"2"})).unwrap();
    validate_source_configs_for_run(&manifest, &config).unwrap();
    assert!(parse_source_configs(&["GURT@trades@elysium".into()]).is_err());
    assert!(parse_source_configs(&[format!("{}@trades@elysium", Address::ZERO)]).is_err());
    let capabilities = ElysiumMarketData.capabilities();
    assert!(capabilities.live_trades);
    assert!(
        !capabilities.live_orderbook
            && !capabilities.historical_candles
            && !capabilities.live_ticker
    );
}

fn test_stream() -> ElysiumTrades {
    ElysiumTrades {
        client: PoolClient::new(None).unwrap(),
        market: identity(false),
        checkpoint: block(),
        from_block: 100,
        volume: Volume {
            base: U256::from(123),
            quote: U256::from(456),
            trades: 7,
        },
        socket: None,
        head: None,
        pending: BTreeMap::new(),
        recovery_end: 0,
        backfill_reads: 0,
    }
}

#[test]
fn checkpoint_round_trip_preserves_volume_and_rejects_other_market() {
    let first = test_stream();
    let mut next = test_stream();
    next.volume = Volume::default();
    next.restore_checkpoint(first.checkpoint().unwrap())
        .unwrap();
    assert_eq!(next.volume.trades, 7);
    assert_eq!(next.volume.base, U256::from(123));
    assert_eq!(next.from_block, 100);
    let mut invalid = first.checkpoint().unwrap();
    invalid["market"] = json!(Address::ZERO);
    assert!(next.restore_checkpoint(invalid).is_err());
}

#[tokio::test]
#[ignore = "run scripts/check-elysium-stream.sh against its isolated Anvil instance"]
async fn local_contract_trade_stream() {
    async fn cast(args: &[&str]) -> String {
        let rpc = std::env::var("MLAB_ELYSIUM_RPC_URL").unwrap();
        assert!(rpc.starts_with("http://127.0.0.1:"));
        let output = tokio::process::Command::new("cast")
            .args(args)
            .args(["--rpc-url", &rpc])
            .output()
            .await
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().into()
    }
    async fn send(args: &[&str]) {
        let key = std::env::var("ELYSIUM_SMOKE_KEY").unwrap();
        let mut command = vec!["send", "--private-key", &key];
        command.extend_from_slice(args);
        cast(&command).await;
    }
    let version = cast(&["rpc", "web3_clientVersion"]).await;
    assert!(version.to_lowercase().contains("anvil"));
    let pool = std::env::var("ELYSIUM_SMOKE_POOL").unwrap();
    let curve = std::env::var("ELYSIUM_SMOKE_CURVE").unwrap();
    let token = std::env::var("ELYSIUM_SMOKE_TOKEN").unwrap();
    let owner = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266";
    let snapshot = market_snapshot().await.unwrap();
    assert_eq!(snapshot.exchanges[0].markets.len(), 2);
    let mut pool_stream = ElysiumTrades::connect(&pool).await.unwrap();
    let mut curve_stream = ElysiumTrades::connect(&curve).await.unwrap();
    assert!(ElysiumTrades::connect(&token).await.is_err());
    pool_stream.subscribe().await.unwrap();
    curve_stream.subscribe().await.unwrap();

    send(&[
        &pool,
        "swapExactInput(address,uint256,uint256,address,uint256)",
        &token,
        "1000000000000000000",
        "1",
        owner,
        "9999999999",
    ])
    .await;
    send(&[
        &curve,
        "buyHype(uint256,address,uint256)",
        "1",
        owner,
        "9999999999",
        "--value",
        "1000000000000000000",
    ])
    .await;
    for _ in 0..12 {
        cast(&["rpc", "anvil_mine", "1"]).await;
    }
    let pools = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        pool_stream.next_trades(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(pools.len(), 1);
    assert_eq!(pools[0].onchain.as_ref().unwrap().kind, "pool");
    let buys = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        curve_stream.next_trades(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(buys.len(), 1);
    assert_eq!(buys[0].onchain.as_ref().unwrap().side, "buy");
    assert!(curve_stream.advance().await.unwrap().is_empty());

    // Once caught up, another live trade must come from the socket, not eth_getLogs.
    let reads = curve_stream.backfill_reads;
    send(&[
        &curve,
        "buyHype(uint256,address,uint256)",
        "1",
        owner,
        "9999999999",
        "--value",
        "10000000000000000",
    ])
    .await;
    for _ in 0..12 {
        cast(&["rpc", "anvil_mine", "1"]).await;
    }
    let live = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        curve_stream.next_trades(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(live.len(), 1);
    assert_eq!(
        curve_stream.backfill_reads, reads,
        "healthy live feed must not fetch trade logs over HTTP"
    );
    assert_eq!(live[0].onchain.as_ref().unwrap().volume.trades, 2);

    let cursor = curve_stream.checkpoint().unwrap();
    send(&[
        &token,
        "approve(address,uint256)",
        &curve,
        "1000000000000000000",
    ])
    .await;
    send(&[
        &curve,
        "sell(uint256,uint256,address,uint256)",
        "1000000000000000000",
        "1",
        owner,
        "9999999999",
    ])
    .await;
    cast(&["rpc", "anvil_mine", "12"]).await;
    let mut resumed = ElysiumTrades::connect(&curve).await.unwrap();
    resumed.restore_checkpoint(cursor).unwrap();
    let sells = tokio::time::timeout(std::time::Duration::from_secs(10), resumed.next_trades())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(sells.len(), 1);
    let sale = sells[0].onchain.as_ref().unwrap();
    assert_eq!(sale.side, "sell");
    assert_eq!(sale.base_amount, "1");
    assert_eq!(sale.volume.trades, 3);
    assert!(resumed.advance().await.unwrap().is_empty());

    super::super::contracts_tests::local_contract_operations(
        pool.parse().unwrap(),
        curve.parse().unwrap(),
        token.parse().unwrap(),
    )
    .await;

    // Failed RPC reads must leave the checkpoint and accumulated volume untouched.
    // Give advance a new confirmed block without needing a socket read in these fault checks.
    resumed.accept_head(resumed.client.wallet_block().await.unwrap());
    cast(&["rpc", "anvil_mine", "12"]).await;
    resumed.accept_head(resumed.client.wallet_block().await.unwrap());
    let before = resumed.checkpoint().unwrap();
    let rpc = resumed.client.rpc_url.clone();
    resumed.client.rpc_url = "http://127.0.0.1:1".parse().unwrap();
    assert!(resumed.advance().await.is_err());
    assert_eq!(resumed.checkpoint().unwrap(), before);
    resumed.client.rpc_url = rpc;
    resumed.checkpoint.hash = B256::ZERO;
    assert!(
        resumed
            .advance()
            .await
            .unwrap_err()
            .is::<StreamIntegrityError>()
    );
}

#[test]
fn python_receives_enriched_trades_without_a_second_source() {
    use crate::scripting::engine::Script;
    use crate::scripting::execution::ScriptExecutionContext;
    use crate::scripting::language::PythonRuntime;
    let path = std::env::temp_dir().join(format!("mlab-elysium-trades-{}.py", std::process::id()));
    std::fs::write(&path, include_str!("../../../examples/elysium-trades.py")).unwrap();
    if PythonRuntime::resolve(&path, None).is_err() {
        std::fs::remove_file(path).unwrap();
        eprintln!("Python unavailable: skipping bridge test");
        return;
    }
    let script = Script::load(&path).unwrap();
    let selector = format!("{}@trades@elysium", identity(true).address).to_lowercase();
    assert_eq!(
        script.source_declarations(),
        std::slice::from_ref(&selector)
    );
    let session = script
        .start_session_with_execution_and_sources(
            &json!({}),
            ScriptExecutionContext::disabled(),
            Some(std::slice::from_ref(&selector)),
        )
        .unwrap();
    let trade = normalize(
        &buy_log(),
        &identity(true),
        &block(),
        &json!({"current_price":{"formatted":"0.75"}}),
        &mut Volume::default(),
        100,
        99801,
    )
    .unwrap();
    let result = session.run_event(json!({
        "source": selector, "source_type": "trades", "provider": "elysium", "exchange":"elysium",
        "symbol": identity(true).address.to_string(), "data":{"record":ScriptTrade::from_tick(&trade)},
    })).unwrap();
    assert_eq!(result.output.metrics["current_price"], 0.75);
    assert_eq!(
        result.output.metrics["quote_volume"],
        "1.010000000000000001"
    );
    assert_eq!(result.output.metrics["fee"], "0.01");
    assert_eq!(result.output.metrics["side"], "buy");
    drop(session);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn subscription_buffer_deduplicates_and_recovers_unconfirmed_reorgs() {
    let mut stream = test_stream();
    stream.market = identity(true);
    stream.checkpoint.number = U64::from(100);
    let event = buy_log();
    stream.accept_log(event.clone()).unwrap();
    stream.accept_log(event.clone()).unwrap();
    assert_eq!(stream.pending.len(), 1);
    let mut removed = event.clone();
    removed.removed = true;
    stream.accept_log(removed).unwrap();
    assert!(stream.pending.is_empty());
    assert_eq!(stream.recovery_end, 120);
    let mut replacement = event.clone();
    replacement.block_hash = B256::repeat_byte(9);
    stream.accept_log(replacement).unwrap();
    let mut old_removal = event;
    old_removal.removed = true;
    stream.accept_log(old_removal.clone()).unwrap();
    assert_eq!(
        stream.pending.len(),
        1,
        "old fork removal cannot erase its replacement"
    );
    stream.checkpoint.number = U64::from(120);
    assert!(
        stream
            .accept_log(old_removal)
            .unwrap_err()
            .is::<StreamIntegrityError>()
    );
}

#[test]
fn head_gaps_replay_unconfirmed_blocks_without_regular_polling() {
    let mut stream = test_stream();
    stream.accept_head(block());
    let mut next = block();
    next.number = U64::from(121);
    stream.accept_head(next);
    assert_eq!(stream.recovery_end, 0);
    let mut gap = block();
    gap.number = U64::from(130);
    stream.accept_head(gap);
    assert_eq!(stream.recovery_end, 130);
    assert!(stream.recovery_end > 130 - CONFIRMATIONS);
    stream.accept_head(block()); // Reorg back below the previous head.
    assert_eq!(stream.recovery_end, 130);
}

#[test]
fn subscription_buffer_rejects_conflicts_foreign_logs_and_overflow() {
    let mut stream = test_stream();
    stream.market = identity(true);
    stream.checkpoint.number = U64::from(100);
    let mut event = buy_log();
    stream.accept_log(event.clone()).unwrap();
    event.transaction_hash = B256::repeat_byte(8);
    assert!(stream.accept_log(event).is_err());
    let mut foreign = buy_log();
    foreign.address = Address::ZERO;
    assert!(stream.accept_log(foreign).is_err());
    for index in 0..10_000 {
        let mut event = buy_log();
        event.log_index = U64::from(index);
        stream.accept_log(event).unwrap();
    }
    let mut overflow = buy_log();
    overflow.log_index = U64::from(10_001);
    assert!(stream.accept_log(overflow).is_err());
    assert_eq!(stream.pending.len(), 10_000);
}
