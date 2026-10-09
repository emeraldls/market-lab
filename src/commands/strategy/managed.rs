use crate::cli::{
    CliSide, OutputFormat, RunManagedArgs, TradeArgs, TradeOrderKind, TradeTimeInForce,
};
use crate::commands::execution::build_trade_plan;
use crate::domain::execution::{CancelPlan, OrderKind, PositionDirection, TimeInForce, TradePlan};
use crate::providers::execution::ExecutionAdapter;
use crate::providers::market_data::VenueTradesStream;
use crate::strategies::jobs::{
    StrategyJob, StrategyJobDefinition, StrategyJobSubmission, StrategySide, TwapJobDefinition,
};
use crate::strategies::managed::{ExecutionStyle, MAX_ORDERS, ManagedDefinition, pov_credit};
use anyhow::{Context, Result, bail};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
fn direction(side: StrategySide) -> PositionDirection {
    match side {
        StrategySide::Buy => PositionDirection::Long,
        StrategySide::Sell => PositionDirection::Short,
    }
}
fn trade_args(
    d: &ManagedDefinition,
    size: Option<f64>,
    margin: Option<f64>,
    price: f64,
) -> TradeArgs {
    TradeArgs {
        symbol: d.base.symbol.clone(),
        symbol_flag: None,
        config: None,
        venue: d.base.venue,
        testnet: d.base.testnet,
        size,
        margin,
        order_kind: TradeOrderKind::Limit,
        price: Some(price),
        tif: if d.style == ExecutionStyle::Pov {
            TradeTimeInForce::Ioc
        } else {
            TradeTimeInForce::Gtc
        },
        leverage: Some(d.base.leverage),
        reduce_only: d.base.reduce_only,
        sl: None,
        tp: None,
        dry_run: true,
        yes: false,
        output: OutputFormat::Json,
    }
}

pub async fn handle(args: RunManagedArgs, style: ExecutionStyle) -> Result<()> {
    args.base.validate()?;
    if args.base.interval != 60 {
        bail!("--interval is not used by this strategy");
    }
    let mut d = ManagedDefinition {
        base: TwapJobDefinition {
            venue: args.base.venue,
            testnet: args.base.testnet,
            symbol: args.base.symbol.clone(),
            side: match args.base.side {
                CliSide::Buy => StrategySide::Buy,
                CliSide::Sell => StrategySide::Sell,
            },
            total_size: args.base.size.unwrap_or(args.display_size.unwrap_or(1.0)),
            requested_margin: args.base.margin,
            target_margin: 1.0,
            target_exposure: args.base.leverage,
            leverage: args.base.leverage,
            duration_seconds: args.base.duration,
            interval_seconds: 1,
            reduce_only: args.base.reduce_only,
        },
        style,
        limit_price: args.limit_price,
        participation: args.participation,
        display_size: args.display_size,
        start_price: args.start_price,
        end_price: args.end_price,
        levels: args.levels,
    };
    d.validate()?;
    let price = d.limit_price.or(d.end_price).context("price is required")?;
    let parent = build_trade_plan(
        &trade_args(&d, args.base.size, args.base.margin, price),
        direction(d.base.side),
    )
    .await?;
    d.base.total_size = parent.size;
    d.base.symbol = parent.internal_symbol.clone();
    d.base.target_margin = parent.estimated_margin;
    d.base.target_exposure = parent.estimated_exposure;
    let market =
        crate::markets::exchange_market(d.base.venue.market_data_id().as_str(), &d.base.symbol)?;
    let rules = market.execution_rules()?;
    let slices = d.slices(rules.lot_size, rules.tick_size, rules.min_notional)?;
    let view = json!({"type":"strategy.plan","strategy":style.name(),"venue":d.base.venue,"symbol":d.base.symbol,"side":d.base.side,"totalSize":parent.size,"estimatedMargin":parent.estimated_margin,"estimatedExposure":parent.estimated_exposure,"referencePrice":parent.reference_price,"durationSecs":d.base.duration_seconds,"leverage":d.base.leverage,"reduceOnly":d.base.reduce_only,"feasible":true,"limitPrice":d.limit_price,"participation":d.participation,"displaySize":d.display_size,"startPrice":d.start_price,"endPrice":d.end_price,"levels":d.levels,"childOrders":if style==ExecutionStyle::Pov {None} else {Some(slices.len())},"orders":if style==ExecutionStyle::Pov {vec![]} else {slices}});
    if args.base.dry_run {
        println!("{}", serde_json::to_string(&view)?);
        return Ok(());
    }
    if !args.base.yes {
        bail!("preview with --dry-run, then pass --yes to place live strategy orders");
    }
    let definition = match style {
        ExecutionStyle::Pov => StrategyJobDefinition::Pov(d),
        ExecutionStyle::Iceberg => StrategyJobDefinition::Iceberg(d),
        ExecutionStyle::Scale => StrategyJobDefinition::Scale(d),
    };
    let job = crate::runtime::submit_strategy_job(StrategyJobSubmission { definition }).await?;
    super::vwap::render_submission(&job, args.base.output)
}

pub fn validate_child(d: &ManagedDefinition, sequence: u64, plan: &TradePlan) -> Result<()> {
    d.validate()?;
    if sequence == 0
        || sequence > MAX_ORDERS
        || plan.testnet != d.base.testnet
        || plan.order_kind != OrderKind::Limit
        || plan.price.is_none()
    {
        bail!("invalid managed strategy child");
    }
    let market =
        crate::markets::exchange_market(d.base.venue.market_data_id().as_str(), &d.base.symbol)?;
    let rules = market.execution_rules()?;
    let slices = d.slices(rules.lot_size, rules.tick_size, rules.min_notional)?;
    let expected = if d.style == ExecutionStyle::Pov {
        &slices[0]
    } else {
        slices
            .get((sequence - 1) as usize)
            .context("child exceeds strategy schedule")?
    };
    if (plan.price.unwrap() - expected.price).abs() > rules.tick_size * 1e-7
        || (d.style != ExecutionStyle::Pov
            && (plan.size - expected.size).abs() > rules.lot_size * 1e-7)
        || plan.time_in_force
            != Some(if d.style == ExecutionStyle::Pov {
                TimeInForce::Ioc
            } else {
                TimeInForce::Gtc
            })
    {
        bail!("child differs from approved strategy price, size or time in force");
    }
    Ok(())
}

struct TrackedOrder {
    size: f64,
    filled: f64,
}

fn reconcile_fills(
    orders: &mut HashMap<String, TrackedOrder>,
    fills: Vec<crate::domain::execution::Fill>,
) {
    let mut totals = HashMap::<String, f64>::new();
    let mut seen = HashSet::new();
    for fill in fills {
        let Some(id) = fill.order_id else { continue };
        if !orders.contains_key(&id) || !fill.amount.is_finite() || fill.amount <= 0.0 {
            continue;
        }
        if let Some(trade_id) = fill.trade_id {
            if !seen.insert((id.clone(), trade_id)) {
                continue;
            }
        }
        *totals.entry(id).or_default() += fill.amount;
    }
    // Recent-fill windows may shrink; never treat disappearance as a fill or add snapshots twice.
    for (id, total) in totals {
        let order = orders.get_mut(&id).unwrap();
        order.filled = order.filled.max(total).min(order.size);
    }
}

pub async fn handle_worker_job(job_id: &str, job: StrategyJob) -> Result<()> {
    let d = match job.definition {
        StrategyJobDefinition::Pov(d)
        | StrategyJobDefinition::Iceberg(d)
        | StrategyJobDefinition::Scale(d) => d,
        _ => bail!("wrong strategy worker"),
    };
    let pid = std::process::id();
    crate::runtime::strategy_worker_started(job_id, pid).await?;
    let result = run_worker(job_id, &d).await;
    let error = result.as_ref().err().map(|e| format!("{e:#}"));
    if let Some(error) = &error {
        let _ = crate::runtime::append_strategy_output(
            job_id,
            &json!({"type":"strategy.run.failed","strategy":d.style.name(),"error":error}),
        );
    }
    crate::runtime::strategy_worker_finished(job_id, pid, error).await?;
    result
}

async fn run_worker(job_id: &str, d: &ManagedDefinition) -> Result<()> {
    // A restart must never replenish an order using a lost in-memory fill ledger.
    if crate::runtime::strategy_output_after(job_id, 0)?
        .1
        .iter()
        .any(|v| v["type"] == "strategy.managed.started")
    {
        bail!("interrupted strategy requires reconciliation; automatic resubmission is disabled");
    }
    let adapter =
        ExecutionAdapter::new_for_market(d.base.venue, d.base.testnet, "main", &d.base.symbol)
            .await?;
    let price = d.limit_price.or(d.end_price).context("missing price")?;
    let parent = build_trade_plan(
        &trade_args(d, Some(d.base.total_size), None, price),
        direction(d.base.side),
    )
    .await?;
    let market =
        crate::markets::exchange_market(d.base.venue.market_data_id().as_str(), &d.base.symbol)?;
    let rules = market.execution_rules()?;
    let slices = d.slices(rules.lot_size, rules.tick_size, rules.min_notional)?;
    let mut stream = if d.style == ExecutionStyle::Pov {
        Some(VenueTradesStream::connect(d.base.venue, &d.base.symbol, d.base.testnet).await?)
    } else {
        None
    };
    crate::runtime::append_strategy_output(
        job_id,
        &json!({"type":"strategy.managed.started","strategy":d.style.name(),"ts_ms":now_ms()}),
    )?;
    let started = Instant::now();
    let mut watermark = now_ms();
    let mut volume = 0.0;
    let mut submitted = 0.0;
    let mut orders = HashMap::<String, TrackedOrder>::new();
    let mut sequence = 0u64;
    let mut reported_filled = -1.0;
    let mut tick = tokio::time::interval(Duration::from_secs(2));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let result:Result<&str>=async {
      loop {
        tokio::select! {
          trades=async { match stream.as_mut(){Some(s)=>s.next_trades().await,None=>std::future::pending().await} } => {
            let trades=trades?;let old=watermark;
            for trade in trades { if trade.timestamp_ms>old && trade.timestamp_ms<=now_ms()+1000 && trade.size.is_finite() && trade.size>0.0 { volume+=trade.size;watermark=watermark.max(trade.timestamp_ms); } }
            if !volume.is_finite(){bail!("invalid live volume");}
          }
          _=terminate.recv()=>return Ok("stopped"),
          _=tokio::signal::ctrl_c()=>return Ok("stopped"),
          _=tick.tick()=>{
            crate::runtime::strategy_worker_heartbeat(job_id,std::process::id()).await?;
            if !orders.is_empty() {
              let fills=adapter.fills(&parent.account).await?;
              reconcile_fills(&mut orders, fills);
            }
            let filled=orders.values().map(|o|o.filled).sum::<f64>();
            if filled != reported_filled {
              crate::runtime::append_strategy_output(job_id,&json!({"type":"strategy.progress","strategy":d.style.name(),"filledSize":filled,"submittedSize":submitted,"ts_ms":now_ms()}))?;
              reported_filled=filled;
            }
            if filled+rules.lot_size/2.0>=d.base.total_size {return Ok("completed");}
            if started.elapsed().as_secs()>=d.base.duration_seconds {return Ok("partial");}
            for _ in 0..if d.style==ExecutionStyle::Scale {slices.len()} else {1} {
            if started.elapsed().as_secs()>=d.base.duration_seconds {break;}
            let slice=match d.style {
              ExecutionStyle::Scale=>slices.get(sequence as usize).cloned(),
              ExecutionStyle::Iceberg=>if submitted-filled<rules.lot_size/2.0 {slices.get(sequence as usize).cloned()}else{None},
              ExecutionStyle::Pov=>{let size=pov_credit(volume,d.participation.unwrap(),submitted,d.base.total_size,rules.lot_size);if size>=rules.lot_size && size*price>=rules.min_notional {Some(crate::strategies::managed::Slice{size,price})}else{None}},
            };
            let Some(slice)=slice else{break;};
            if sequence>=MAX_ORDERS {bail!("maximum child-order count reached");}
            crate::runtime::strategy_worker_heartbeat(job_id,std::process::id()).await?;
            let plan=build_trade_plan(&trade_args(d,Some(slice.size),None,slice.price),direction(d.base.side)).await?;
            let receipt=crate::runtime::submit_strategy_trade(job_id,sequence+1,&plan).await?;
            sequence+=1;submitted+=plan.size;
            let id=receipt.order_id.clone().context("venue did not return an order id; stopped to prevent duplicate execution")?;
            orders.insert(id.clone(),TrackedOrder{size:plan.size,filled:0.0});
            let filled=receipt.filled_size.unwrap_or(0.0);
            if !filled.is_finite() || filled<0.0 || filled>plan.size+rules.lot_size/2.0 {bail!("venue returned invalid fill size");}
            orders.insert(id,TrackedOrder{size:plan.size,filled});
            crate::runtime::append_strategy_output(job_id,&json!({"type":"strategy.child_order","action":"limit","strategy":d.style.name(),"sequence":sequence,"size":plan.size,"price":slice.price,"orderId":receipt.order_id,"status":receipt.status,"filledSize":orders.values().map(|o|o.filled).sum::<f64>(),"ts_ms":now_ms()}))?;
            }
          }
        }
      }
    }.await;
    // Cancel only this job's remaining orders, including on stream or execution failure.
    let cleanup: Result<()> = async {
        let open = adapter.open_orders(&parent.account).await?;
        let mut cancellation = 0;
        for order in open {
            if orders.contains_key(&order.order_id) {
                cancellation += 1;
                crate::runtime::submit_strategy_cancel(
                    job_id,
                    cancellation,
                    &CancelPlan {
                        created_at_ms: now_ms(),
                        venue: d.base.venue,
                        testnet: d.base.testnet,
                        account: parent.account.clone(),
                        internal_symbol: parent.internal_symbol.clone(),
                        venue_symbol: parent.venue_symbol.clone(),
                        order_id: order.order_id,
                    },
                )
                .await?;
            }
        }
        for attempt in 0..3 {
            let remaining = adapter.open_orders(&parent.account).await?;
            if !remaining.iter().any(|o| orders.contains_key(&o.order_id)) {
                reconcile_fills(&mut orders, adapter.fills(&parent.account).await?);
                return Ok(());
            }
            if attempt < 2 {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
        bail!("strategy orders remain open after cancellation")
    }
    .await;
    let filled = orders.values().map(|o| o.filled).sum::<f64>();
    crate::runtime::append_strategy_output(
        job_id,
        &json!({"type":"strategy.run.finished","strategy":d.style.name(),"status":result.as_ref().map_or("failed",|s|*s),"targetSize":d.base.total_size,"filledSize":filled,"submittedSize":submitted,"elapsedMs":started.elapsed().as_millis(),"ts_ms":now_ms()}),
    )?;
    cleanup.context("could not confirm strategy order cleanup; inspect open orders")?;
    match result? {
        "partial" => bail!(
            "deadline reached with {filled} of {} filled; remaining orders cancelled",
            d.base.total_size
        ),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::execution::{ExecutionVenue, Fill, OrderSide};
    fn fill(id: &str, amount: f64) -> Fill {
        Fill {
            venue: ExecutionVenue::Bulk,
            internal_symbol: "BTC".into(),
            venue_symbol: "BTC".into(),
            registry_supported: true,
            side: OrderSide::Buy,
            amount,
            price: 100.0,
            reason: String::new(),
            order_id: Some("owned".into()),
            trade_id: Some(id.into()),
            maker: true,
            fee: None,
            fee_asset: None,
            slot: 0,
            ts_ms: 1,
        }
    }
    #[test]
    fn fill_snapshots_do_not_double_count_or_regress() {
        let mut orders = HashMap::from([(
            "owned".into(),
            TrackedOrder {
                size: 1.0,
                filled: 0.0,
            },
        )]);
        reconcile_fills(&mut orders, vec![fill("a", 0.2), fill("a", 0.2)]);
        assert_eq!(orders["owned"].filled, 0.2);
        reconcile_fills(&mut orders, vec![fill("a", 0.2), fill("b", 0.3)]);
        assert_eq!(orders["owned"].filled, 0.5);
        reconcile_fills(&mut orders, vec![fill("b", 0.3)]);
        assert_eq!(orders["owned"].filled, 0.5);
        let mut foreign = fill("c", 10.0);
        foreign.order_id = Some("another-job".into());
        reconcile_fills(&mut orders, vec![foreign, fill("bad", f64::NAN)]);
        assert_eq!(orders["owned"].filled, 0.5);
    }
}
