use std::path::Path;

use anyhow::{Context, Result, bail};
use base64::Engine as _;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

use crate::commands::script::backtest::fetch_notebook_source;
use crate::commands::source::common::format_utc_ms;
use crate::scripting::studies::calculate_value;

pub(crate) const BOOTSTRAP_SOURCE: &str = include_str!("notebook_bootstrap.py");

const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;
const MAX_STUDY_PAYLOAD_BYTES: usize = 256 * 1024 * 1024;
const RESULT_CHUNK_BYTES: usize = 192 * 1024;
const SOURCE_CHUNK_BYTES: usize = 256 * 1024;

pub(crate) async fn serve(listener: UnixListener, token: String) -> Result<()> {
    loop {
        let (stream, _) = listener
            .accept()
            .await
            .context("failed to accept a notebook bridge connection")?;
        let token = token.clone();
        tokio::spawn(async move {
            if let Err(error) = serve_connection(stream, &token).await {
                eprintln!("notebook bridge warning: {error:#}");
            }
        });
    }
}

async fn serve_connection(stream: UnixStream, token: &str) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let request = match read_frame(&mut reader).await {
        Ok(request) => request,
        Err(error) => {
            let _ = write_error(&mut writer, &error.to_string()).await;
            return Err(error);
        }
    };
    if let Err(error) = verify_token(&request, token) {
        let _ = write_error(&mut writer, &error.to_string()).await;
        return Err(error);
    }

    let result = match request.get("type").and_then(Value::as_str) {
        Some("history") => handle_history(&request, &mut writer).await,
        Some("source") => handle_source(&request, &mut reader, &mut writer).await,
        Some("study_start") => handle_study(&request, &mut reader, &mut writer, token).await,
        Some(other) => Err(anyhow::anyhow!(
            "unknown Market Lab notebook request `{other}`"
        )),
        None => Err(anyhow::anyhow!(
            "Market Lab notebook request has no message type"
        )),
    };
    if let Err(error) = result {
        write_error(&mut writer, &format!("{error:#}")).await?;
    }
    Ok(())
}

async fn handle_history<W>(request: &Value, writer: &mut W) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let from_raw = request
        .get("from")
        .and_then(Value::as_str)
        .context("mlab.history from_ must be a UTC date string")?;
    let to_raw = request
        .get("to")
        .and_then(Value::as_str)
        .context("mlab.history to must be a UTC date string")?;
    let from = crate::cli::parse_datetime_ms(from_raw).map_err(anyhow::Error::msg)?;
    let to = crate::cli::parse_datetime_ms(to_raw).map_err(anyhow::Error::msg)?;
    if from >= to {
        bail!("mlab.history from_ must be earlier than to");
    }
    write_result(
        writer,
        &json!({
            "fromMs": from,
            "toMs": to,
            "fromUtc": format_utc_ms(from),
            "toUtc": format_utc_ms(to),
        }),
    )
    .await
}

async fn handle_source<R, W>(request: &Value, reader: &mut R, writer: &mut W) -> Result<()>
where
    R: tokio::io::AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let selector = request
        .get("selector")
        .and_then(Value::as_str)
        .filter(|selector| !selector.trim().is_empty())
        .context("history.source selector must be a non-empty string")?;
    let from = request
        .get("fromMs")
        .and_then(Value::as_u64)
        .context("history.source request omitted fromMs")?;
    let to = request
        .get("toMs")
        .and_then(Value::as_u64)
        .context("history.source request omitted toMs")?;
    validate_range(from, to)?;

    let records = tokio::select! {
        result = fetch_notebook_source(selector, from, to) => result?,
        result = wait_for_disconnect(reader) => {
            result?;
            bail!("notebook history request was cancelled");
        }
    };
    write_frame(
        writer,
        &json!({ "type": "source_start", "count": records.len() }),
    )
    .await?;
    write_source_chunks(writer, records).await?;
    write_frame(writer, &json!({ "type": "source_end" })).await
}

async fn handle_study<R, W>(
    request: &Value,
    reader: &mut R,
    writer: &mut W,
    token: &str,
) -> Result<()>
where
    R: tokio::io::AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let name = request
        .get("name")
        .and_then(Value::as_str)
        .filter(|name| !name.trim().is_empty())
        .context("mlab.study request omitted its function name")?
        .to_string();
    let mut payload = Vec::new();
    loop {
        let frame = read_frame(reader).await?;
        verify_token(&frame, token)?;
        match frame.get("type").and_then(Value::as_str) {
            Some("study_chunk") => {
                let encoded = frame
                    .get("data")
                    .and_then(Value::as_str)
                    .context("mlab.study chunk omitted data")?;
                let decoded = base64::engine::general_purpose::STANDARD
                    .decode(encoded)
                    .context("mlab.study chunk is not valid base64")?;
                if payload.len().saturating_add(decoded.len()) > MAX_STUDY_PAYLOAD_BYTES {
                    bail!(
                        "mlab.study input exceeds the {} MiB notebook limit",
                        MAX_STUDY_PAYLOAD_BYTES / (1024 * 1024)
                    );
                }
                payload.extend_from_slice(&decoded);
            }
            Some("study_end") => break,
            Some(other) => bail!("unexpected mlab.study frame `{other}`"),
            None => bail!("mlab.study frame has no message type"),
        }
    }
    let payload: Value =
        serde_json::from_slice(&payload).context("mlab.study input is not valid JSON")?;
    let input = payload
        .get("input")
        .cloned()
        .context("mlab.study input is missing")?;
    let options = payload.get("options").cloned().unwrap_or_else(|| json!({}));
    let calculation = tokio::task::spawn_blocking(move || calculate_value(&name, input, options));
    let result = tokio::select! {
        result = calculation => result.context("Market Lab notebook study task failed")??,
        result = wait_for_disconnect(reader) => {
            result?;
            bail!("notebook study request was cancelled");
        }
    };
    write_result(writer, &result).await
}

async fn wait_for_disconnect<R>(reader: &mut R) -> Result<()>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    let buffered = reader
        .fill_buf()
        .await
        .context("failed while monitoring a notebook request")?;
    if buffered.is_empty() {
        return Ok(());
    }
    bail!("notebook client sent data after completing its request")
}

fn verify_token(frame: &Value, expected: &str) -> Result<()> {
    let provided = frame
        .get("token")
        .and_then(Value::as_str)
        .context("Market Lab notebook request omitted its session token")?;
    if provided != expected {
        bail!("Market Lab notebook session token is invalid");
    }
    Ok(())
}

fn validate_range(from: u64, to: u64) -> Result<()> {
    if !(10_000_000_000..10_000_000_000_000).contains(&from)
        || !(10_000_000_000..10_000_000_000_000).contains(&to)
    {
        bail!("notebook history range must use millisecond timestamps");
    }
    if from >= to {
        bail!("notebook history start must be earlier than its end");
    }
    Ok(())
}

async fn read_frame<R>(reader: &mut R) -> Result<Value>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    let mut payload = Vec::new();
    let read = reader
        .take((MAX_FRAME_BYTES + 1) as u64)
        .read_until(b'\n', &mut payload)
        .await
        .context("failed to read a Market Lab notebook frame")?;
    if read == 0 {
        bail!("Market Lab notebook bridge connection closed");
    }
    if payload.len() > MAX_FRAME_BYTES || !payload.ends_with(b"\n") {
        bail!("Market Lab notebook request frame exceeds the 4 MiB limit");
    }
    payload.pop();
    serde_json::from_slice(&payload).context("Market Lab notebook request is not valid JSON")
}

async fn write_frame<W>(writer: &mut W, value: &Value) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let payload = serde_json::to_vec(value).context("failed to encode notebook response")?;
    if payload.len() > MAX_FRAME_BYTES {
        bail!("Market Lab notebook response frame exceeds the 4 MiB limit");
    }
    writer
        .write_all(&payload)
        .await
        .context("failed to write notebook response")?;
    writer
        .write_all(b"\n")
        .await
        .context("failed to finish notebook response frame")?;
    writer
        .flush()
        .await
        .context("failed to flush notebook response")
}

async fn write_source_chunks<W>(writer: &mut W, records: Vec<Value>) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut chunk = Vec::new();
    let mut bytes = 0_usize;
    for record in records {
        let record_bytes = serde_json::to_vec(&record)
            .context("failed to measure a notebook source record")?
            .len();
        if !chunk.is_empty() && bytes.saturating_add(record_bytes) > SOURCE_CHUNK_BYTES {
            write_frame(writer, &json!({ "type": "source_chunk", "records": chunk })).await?;
            chunk = Vec::new();
            bytes = 0;
        }
        bytes = bytes.saturating_add(record_bytes);
        chunk.push(record);
    }
    if !chunk.is_empty() {
        write_frame(writer, &json!({ "type": "source_chunk", "records": chunk })).await?;
    }
    Ok(())
}

async fn write_result<W>(writer: &mut W, value: &Value) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let encoded = serde_json::to_vec(value).context("failed to encode notebook result")?;
    write_frame(writer, &json!({ "type": "result_start" })).await?;
    for chunk in encoded.chunks(RESULT_CHUNK_BYTES) {
        write_frame(
            writer,
            &json!({
                "type": "result_chunk",
                "data": base64::engine::general_purpose::STANDARD.encode(chunk),
            }),
        )
        .await?;
    }
    write_frame(writer, &json!({ "type": "result_end" })).await
}

async fn write_error<W>(writer: &mut W, message: &str) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_frame(writer, &json!({ "type": "error", "message": message })).await
}

pub(crate) fn write_bootstrap(path: &Path) -> Result<()> {
    std::fs::write(path, BOOTSTRAP_SOURCE)
        .with_context(|| format!("failed to write {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand_core::RngCore;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn validates_notebook_ranges() {
        assert!(validate_range(1_785_542_400_000, 1_787_184_000_000).is_ok());
        assert!(validate_range(1_787_184_000_000, 1_785_542_400_000).is_err());
        assert!(validate_range(1, 2).is_err());
    }

    #[test]
    fn bootstrap_injects_mlab_and_keeps_context_out_of_research() {
        assert!(BOOTSTRAP_SOURCE.contains("ipython.push({\"mlab\""));
        assert!(!BOOTSTRAP_SOURCE.contains("def context("));
    }

    #[tokio::test]
    async fn python_bridge_creates_history_and_runs_native_studies() {
        let root = Path::new("/tmp").join(format!(
            "mlab-nb-{}-{}",
            std::process::id(),
            rand_core::OsRng.next_u64()
        ));
        fs::create_dir_all(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let bootstrap = root.join("marketlab_notebook.py");
        write_bootstrap(&bootstrap).unwrap();
        let socket = root.join("bridge.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let token = "notebook-test-token".to_string();
        let server = tokio::spawn(serve(listener, token.clone()));
        let program = r#"
from marketlab_notebook import MarketLabNotebook
mlab = MarketLabNotebook(__import__('os').environ['MLAB_NOTEBOOK_SOCKET'], __import__('os').environ['MLAB_NOTEBOOK_TOKEN'])
history = mlab.history(from_='2026-08-01', to='2026-08-02')
study = mlab.study.sma([{'c': 1.0}, {'c': 3.0}], {'window': 2})
assert history.from_ms == 1785542400000
assert 'sources=0' in repr(history)
assert study['latest'] == 2.0
print('ok')
"#;
        let output = match tokio::process::Command::new("python3")
            .arg("-c")
            .arg(program)
            .env("PYTHONPATH", &root)
            .env("MLAB_NOTEBOOK_SOCKET", &socket)
            .env("MLAB_NOTEBOOK_TOKEN", &token)
            .output()
            .await
        {
            Ok(output) => output,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                server.abort();
                let _ = fs::remove_dir_all(&root);
                return;
            }
            Err(error) => panic!("failed to run Python bridge test: {error}"),
        };
        server.abort();
        let _ = fs::remove_dir_all(&root);
        assert!(
            output.status.success(),
            "Python bridge failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "ok");
    }
}
