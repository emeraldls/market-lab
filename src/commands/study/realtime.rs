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

pub async fn run_mmt_realtime<F, G, J, T>(
    cfg: StreamRunConfig,
    mut calc: F,
    mut to_terminal_line: G,
    mut to_json: J,
) -> Result<()>
where
    F: FnMut(&crate::domain::types::OrderBookSnapshot) -> Result<T>,
    G: FnMut(&T) -> String,
    J: FnMut(&T, crate::cli::OutputFormat) -> Result<String>,
    T: Serialize,
{
    let mut stream = MmtDepthStream::connect(&cfg.exchange, &cfg.symbol, cfg.depth).await?;
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
                    crate::cli::OutputFormat::Json | crate::cli::OutputFormat::Jsonl => {
                        println!("{}", to_json(&out, cfg.output)?);
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
