use std::collections::VecDeque;
use std::time::Duration;

use anyhow::{Context, Result, bail};

use crate::cli::{OutputFormat, SourceVolumesArgs};
use crate::domain::enums::ProviderKind;
use crate::domain::types::VolumeProfile;
use crate::providers::mmt::MmtProvider;
use crate::providers::mmt::utils::normalize_symbol_for_mmt;
use crate::providers::mmt::ws_client::MmtWsClient;

use super::common::{SourceEnvelope, SourceMeta, render_terminal};

pub async fn handle(args: SourceVolumesArgs) -> Result<()> {
    args.validate()?;
    if !matches!(args.provider.into(), ProviderKind::Mmt) {
        bail!("source volumes currently supports only --provider mmt");
    }

    if args.stream {
        if matches!(args.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("stream mode currently supports only --output terminal|json|jsonl");
        }
        return stream_volumes(args).await;
    }

    let series = MmtProvider::volumes(
        &args.exchange,
        &args.symbol,
        args.mmt_tf()?,
        args.from
            .ok_or_else(|| anyhow::anyhow!("--from is required when not streaming"))?,
        args.to
            .ok_or_else(|| anyhow::anyhow!("--to is required when not streaming"))?,
    )
    .await?;

    let ts_ms = series.data.last().map(|p| p.t * 1000).unwrap_or(0);
    let env = SourceEnvelope {
        r#type: "source.volumes.series".to_string(),
        version: "1",
        provider: "mmt",
        exchange: series.exchange.clone(),
        symbol: series.symbol.clone(),
        ts_ms,
        stream: false,
        data: series,
        meta: SourceMeta {
            depth: None,
            min_size: None,
            max_size: None,
            price_group: None,
            interval_ms: None,
            timeframe: Some(args.mmt_tf()?.to_string()),
            bucket: None,
            from: args.from,
            to: args.to,
        },
    };

    match args.output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&env)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(&env)?),
        OutputFormat::Terminal => {
            println!(
                "{} volumes tf={} points={} from={} to={}",
                env.symbol,
                env.meta.timeframe.clone().unwrap_or_default(),
                env.data.points,
                env.meta.from.unwrap_or(0),
                env.meta.to.unwrap_or(0)
            );
        }
        OutputFormat::Csv | OutputFormat::Parquet => {
            println!("TODO source volumes export: {:?}", args.output)
        }
    }

    Ok(())
}

async fn stream_volumes(args: SourceVolumesArgs) -> Result<()> {
    let ws = MmtWsClient::shared().await?;
    ws.subscribe(serde_json::json!({
        "type": "subscribe",
        "channel": "volumes",
        "exchange": args.exchange.to_lowercase(),
        "symbol": normalize_symbol_for_mmt(&args.symbol)?,
        "tf": args.mmt_tf()?,
    }))
    .await
    .context("failed to subscribe to volumes channel")?;

    let mut ticker = tokio::time::interval(Duration::from_millis(args.interval_ms));
    let mut latest: Option<VolumeProfile> = None;
    let mut buf: VecDeque<String> = VecDeque::with_capacity(args.buffer_size as usize);

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                eprintln!("\nstream stopped");
                break;
            }
            msg = ws.next_json() => {
                let Some(value) = msg? else { bail!("websocket closed by server"); };
                if let Some(profile) = parse_volumes_message(value)? {
                    latest = Some(profile);
                }
            }
            _ = ticker.tick() => {
                let Some(p) = latest.as_ref() else { continue; };
                let env = SourceEnvelope {
                    r#type: "source.volumes.stream".to_string(),
                    version: "1",
                    provider: "mmt",
                    exchange: args.exchange.to_lowercase(),
                    symbol: args.symbol.to_uppercase(),
                    ts_ms: p.t * 1000,
                    stream: true,
                    data: p.clone(),
                    meta: SourceMeta {
                        depth: None,
                        min_size: None,
                        max_size: None,
                        price_group: Some(p.pg),
                        interval_ms: Some(args.interval_ms),
                        timeframe: Some(args.mmt_tf()?.to_string()),
                        bucket: None,
                        from: None,
                        to: None,
                    },
                };

                match args.output {
                    OutputFormat::Json | OutputFormat::Jsonl => println!("{}", serde_json::to_string(&env)?),
                    OutputFormat::Terminal => {
                        let total_buy: f64 = p.b.iter().sum();
                        let total_sell: f64 = p.s.iter().sum();
                        let line = format!(
                            "t={} levels={} pg={} buy={} sell={}",
                            p.t, p.p.len(), p.pg, total_buy, total_sell
                        );
                        if buf.len() >= args.buffer_size as usize { buf.pop_front(); }
                        buf.push_back(line);
                        render_terminal("market-lab source volumes stream", &buf)?;
                    }
                    OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
                }
            }
        }
    }

    Ok(())
}

fn parse_volumes_message(value: serde_json::Value) -> Result<Option<VolumeProfile>> {
    if value.is_null() || value.get("type").and_then(|x| x.as_str()) == Some("subscribed") {
        return Ok(None);
    }
    if value.get("type").and_then(|x| x.as_str()) != Some("data") {
        return Ok(None);
    }
    if value.get("channel").and_then(|x| x.as_str()) != Some("volumes") {
        return Ok(None);
    }
    let payload = value.get("data").context("volumes payload missing data")?;
    let profile: VolumeProfile =
        serde_json::from_value(payload.clone()).context("invalid volumes profile shape")?;
    Ok(Some(profile))
}
