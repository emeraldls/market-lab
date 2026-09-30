use std::path::Path;

use anyhow::{Context, Result, bail};
use sha3::{Digest, Sha3_256};

use crate::cli::ScriptBacktestArgs;
use crate::scripting::{jobs::MAX_SCRIPT_SOURCE_BYTES, sandbox};

const POLICY_VERSION: &str = "python-cloud-2";

// ponytail: release images must be immutable; bind an image digest before allowing independently updated Python environments.
fn fingerprint(source: &str, params: &[String]) -> Result<String> {
    Ok(hex::encode(Sha3_256::digest(serde_json::to_vec(&(
        POLICY_VERSION,
        env!("CARGO_PKG_VERSION"),
        source,
        params,
    ))?)))
}

pub fn require_approval(source: &str, params: &[String]) -> Result<()> {
    if !sandbox::required()? {
        return Ok(());
    }
    let fingerprint = fingerprint(source, params)?;
    let path = crate::daemon::market_lab_home()?
        .join("prechecks")
        .join(format!("{fingerprint}.json"));
    let data = std::fs::read(&path).context(
        "This script and its parameters need a successful sandbox precheck before deployment",
    )?;
    let receipt: serde_json::Value = serde_json::from_slice(&data)?;
    if receipt["fingerprint"] != fingerprint || receipt["status"] != "passed" {
        bail!("invalid script precheck receipt");
    }
    Ok(())
}

pub async fn handle(mut args: ScriptBacktestArgs) -> Result<()> {
    if !sandbox::required()? {
        bail!(
            "script precheck requires MLAB_PYTHON_SANDBOX=required; unrestricted prechecks are not accepted"
        );
    }
    sandbox::require_linux()?;
    args.validate()?;
    if Path::new(&args.script).extension().and_then(|s| s.to_str()) != Some("py") {
        bail!("Cloud precheck accepts .py scripts only");
    }
    if args.to - args.from > 24 * 60 * 60 * 1000 {
        bail!("precheck range must not exceed 24 hours; choose a short representative window");
    }
    let source = std::fs::read_to_string(&args.script)?;
    if source.len() > MAX_SCRIPT_SOURCE_BYTES {
        bail!("script exceeds 1 MiB");
    }
    let fingerprint = fingerprint(&source, &args.param)?;
    let directory = crate::daemon::market_lab_home()?.join("prechecks");
    std::fs::create_dir_all(&directory)?;
    let receipt = directory.join(format!("{fingerprint}.json"));
    // A failed recheck must not leave an earlier pass usable.
    match std::fs::remove_file(&receipt) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let snapshot = sandbox::Workspace::new()?;
    let path = snapshot.0.join("strategy.py");
    std::fs::write(&path, &source)?;
    args.script = path.to_string_lossy().into_owned();
    super::backtest::handle_precheck(args).await?;
    use std::io::Write;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))?;
    let staged = directory.join(format!(
        "{}.tmp",
        snapshot.0.file_name().unwrap().to_string_lossy()
    ));
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&staged)?;
    file.write_all(&serde_json::to_vec(&serde_json::json!({
        "status": "passed", "fingerprint": fingerprint,
        "policy": POLICY_VERSION, "mlab_version": env!("CARGO_PKG_VERSION"),
    }))?)?;
    file.sync_all()?;
    std::fs::rename(staged, receipt)?;
    Ok(())
}
