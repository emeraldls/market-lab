use std::collections::VecDeque;
use std::io::{self, Write};

use anyhow::Result;
use serde::Serialize;

use crate::providers::mmt::ws::MmtDepthStream;

pub struct StreamRunConfig {
    pub exchange: String,
    pub symbol: String,
    pub depth: u16,
    pub buffer_size: u16,
    pub output: crate::cli::OutputFormat,
}

pub async fn run_mmt_realtime<F, G, T>(
    cfg: StreamRunConfig,
    mut calc: F,
    mut to_terminal_line: G,
) -> Result<()>
where
    F: FnMut(&crate::domain::types::OrderBookSnapshot) -> Result<T>,
    G: FnMut(&T) -> String,
    T: Serialize,
{
    let state_cap = (cfg.depth as usize).saturating_mul(10).clamp(100, 10_000);
    let mut stream =
        MmtDepthStream::connect(&cfg.exchange, &cfg.symbol, cfg.depth, state_cap).await?;
    let mut buf: VecDeque<String> = VecDeque::with_capacity(cfg.buffer_size as usize);

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                eprintln!("\nstream stopped");
                break;
            }
            snap = stream.next_snapshot() => {
                let snap = snap?;
                let out = calc(&snap)?;
                match cfg.output {
                    crate::cli::OutputFormat::Json => {
                        println!("{}", serde_json::to_string(&out)?);
                    }
                    crate::cli::OutputFormat::Terminal => {
                        let line = to_terminal_line(&out);
                        if buf.len() >= cfg.buffer_size as usize {
                            buf.pop_front();
                        }
                        buf.push_back(line);
                        render_terminal(&buf)?;
                    }
                    crate::cli::OutputFormat::Csv | crate::cli::OutputFormat::Parquet => unreachable!(),
                }
            }
        }
    }

    Ok(())
}

fn render_terminal(buf: &VecDeque<String>) -> Result<()> {
    print!("\x1B[2J\x1B[H");
    println!("market-lab realtime (latest {} updates)", buf.len());
    println!("----------------------------------------");
    for line in buf {
        println!("{}", line);
    }
    io::stdout().flush()?;
    Ok(())
}
