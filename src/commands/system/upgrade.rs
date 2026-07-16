use std::env;
use std::ffi::OsStr;
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use flate2::read::GzDecoder;
use semver::Version;
use serde::Deserialize;
use tar::Archive;

use crate::cli::{OutputFormat, UpgradeArgs};
use crate::domain::types::UpgradeStatus;

const REPO: &str = "emeraldls/market-lab";
const APP_NAME: &str = "mlab";
const DAEMON_NAME: &str = "mlabd";

#[derive(Debug, Deserialize)]
struct GithubRelease {
    tag_name: String,
}

pub async fn handle(args: UpgradeArgs) -> Result<()> {
    if matches!(
        args.output,
        OutputFormat::Csv | OutputFormat::Parquet | OutputFormat::Jsonl
    ) && !args.check
    {
        bail!("upgrade install mode supports only --output terminal|json");
    }

    let current_version = env!("CARGO_PKG_VERSION").to_string();
    let target = detect_target()?;
    let release = fetch_latest_release().await?;
    let latest_version = normalize_tag(&release.tag_name)?;
    let asset_url = asset_url(&latest_version, &target);
    let up_to_date = is_latest(&current_version, &latest_version)?;

    if args.check {
        let status = UpgradeStatus {
            app: APP_NAME.to_string(),
            current_version,
            latest_version,
            target,
            up_to_date,
            updated: false,
            asset_url,
        };
        return render(&status, args.output);
    }

    if up_to_date {
        let status = UpgradeStatus {
            app: APP_NAME.to_string(),
            current_version,
            latest_version,
            target,
            up_to_date: true,
            updated: false,
            asset_url,
        };
        return render(&status, OutputFormat::Terminal);
    }

    self_update(&asset_url).await?;

    let status = UpgradeStatus {
        app: APP_NAME.to_string(),
        current_version,
        latest_version,
        target,
        up_to_date: false,
        updated: true,
        asset_url,
    };

    render(&status, OutputFormat::Terminal)
}

async fn fetch_latest_release() -> Result<GithubRelease> {
    let client = reqwest::Client::builder()
        .user_agent(format!("{APP_NAME}/{}", env!("CARGO_PKG_VERSION")))
        .build()?;

    let resp = client
        .get(format!(
            "https://api.github.com/repos/{REPO}/releases/latest"
        ))
        .send()
        .await?;

    let status = resp.status();
    let body = resp.text().await?;
    if !status.is_success() {
        bail!(
            "GitHub releases/latest returned HTTP {} body={}",
            status,
            body
        );
    }

    Ok(serde_json::from_str(&body)?)
}

fn detect_target() -> Result<String> {
    let arch = match env::consts::ARCH {
        "x86_64" => "x86_64",
        "aarch64" => "aarch64",
        "arm64" => "aarch64",
        other => bail!("unsupported architecture: {}", other),
    };

    let os = match env::consts::OS {
        "macos" => "apple-darwin",
        "linux" => "unknown-linux-gnu",
        other => bail!("unsupported operating system: {}", other),
    };

    Ok(format!("{arch}-{os}"))
}

fn normalize_tag(tag: &str) -> Result<String> {
    Ok(tag
        .strip_prefix('v')
        .ok_or_else(|| anyhow::anyhow!("unexpected release tag format: {}", tag))?
        .to_string())
}

fn asset_url(version: &str, target: &str) -> String {
    format!("https://github.com/{REPO}/releases/download/v{version}/mlab-{version}-{target}.tar.gz")
}

fn is_latest(current: &str, latest: &str) -> Result<bool> {
    Ok(Version::parse(current)? >= Version::parse(latest)?)
}

async fn self_update(url: &str) -> Result<()> {
    let client = reqwest::Client::builder()
        .user_agent(format!("{APP_NAME}/{}", env!("CARGO_PKG_VERSION")))
        .build()?;

    let resp = client.get(url).send().await?;
    let status = resp.status();
    let bytes = resp.bytes().await?;
    if !status.is_success() {
        let body = String::from_utf8_lossy(&bytes);
        bail!(
            "release asset download returned HTTP {} body={}",
            status,
            body
        );
    }

    let current_exe = env::current_exe().context("failed to resolve current executable path")?;
    let parent = current_exe
        .parent()
        .ok_or_else(|| anyhow::anyhow!("current executable has no parent directory"))?;
    let daemon_exe = parent.join(DAEMON_NAME);
    let cli_temp_path = temp_binary_path(parent, APP_NAME);
    let daemon_temp_path = temp_binary_path(parent, DAEMON_NAME);

    extract_binary(&bytes, APP_NAME, &cli_temp_path)?;
    extract_binary(&bytes, DAEMON_NAME, &daemon_temp_path)?;
    fs::rename(&daemon_temp_path, &daemon_exe).with_context(|| {
        format!(
            "failed to replace runtime executable {}",
            daemon_exe.display()
        )
    })?;
    fs::rename(&cli_temp_path, &current_exe).with_context(|| {
        format!(
            "failed to replace current executable {}",
            current_exe.display()
        )
    })?;

    Ok(())
}

fn extract_binary(archive_bytes: &[u8], binary_name: &str, destination: &Path) -> Result<()> {
    let cursor = Cursor::new(archive_bytes);
    let gz = GzDecoder::new(cursor);
    let mut archive = Archive::new(gz);

    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?;
        if path.file_name() == Some(OsStr::new(binary_name)) {
            let mut out = fs::File::create(destination)?;
            std::io::copy(&mut entry, &mut out)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(destination, fs::Permissions::from_mode(0o755))?;
            }
            return Ok(());
        }
    }

    bail!("downloaded archive did not contain {binary_name}")
}

fn temp_binary_path(parent: &Path, binary_name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    parent.join(format!(".{binary_name}.upgrade.{nanos}"))
}

fn render(status: &UpgradeStatus, output: OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Terminal => {
            if status.updated {
                println!(
                    "updated {} from {} to {} ({})",
                    status.app, status.current_version, status.latest_version, status.target
                );
            } else if status.up_to_date {
                println!(
                    "{} {} is already the latest version ({})",
                    status.app, status.current_version, status.target
                );
            } else {
                println!(
                    "{} current={} latest={} target={}",
                    status.app, status.current_version, status.latest_version, status.target
                );
            }
        }
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(status)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(status)?),
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_release_tag() {
        assert_eq!(normalize_tag("v0.0.1").unwrap(), "0.0.1");
    }

    #[test]
    fn compares_versions() {
        assert!(is_latest("0.0.2", "0.0.1").unwrap());
        assert!(!is_latest("0.0.1", "0.0.2").unwrap());
    }

    #[test]
    fn builds_expected_asset_url() {
        assert_eq!(
            asset_url("0.0.1", "aarch64-apple-darwin"),
            "https://github.com/emeraldls/market-lab/releases/download/v0.0.1/mlab-0.0.1-aarch64-apple-darwin.tar.gz"
        );
    }

    #[test]
    fn extracts_cli_and_daemon_from_tarball() {
        use flate2::Compression;
        use flate2::write::GzEncoder;
        use std::io::Read;
        use tar::Builder;

        let mut tar_buf = Vec::new();
        {
            let gz = GzEncoder::new(&mut tar_buf, Compression::default());
            let mut builder = Builder::new(gz);
            for (name, contents) in [
                ("mlab", b"test-cli".as_slice()),
                ("mlabd", b"test-daemon".as_slice()),
            ] {
                let mut header = tar::Header::new_gnu();
                header.set_size(contents.len() as u64);
                header.set_mode(0o755);
                header.set_cksum();
                builder
                    .append_data(
                        &mut header,
                        format!("mlab-0.0.1-aarch64-apple-darwin/{name}"),
                        contents,
                    )
                    .unwrap();
            }
            builder.finish().unwrap();
        }

        let cli_dest = env::temp_dir().join(format!("mlab-test-{}", std::process::id()));
        let daemon_dest = env::temp_dir().join(format!("mlabd-test-{}", std::process::id()));
        extract_binary(&tar_buf, APP_NAME, &cli_dest).unwrap();
        extract_binary(&tar_buf, DAEMON_NAME, &daemon_dest).unwrap();

        let mut cli = String::new();
        fs::File::open(&cli_dest)
            .unwrap()
            .read_to_string(&mut cli)
            .unwrap();
        let mut daemon = String::new();
        fs::File::open(&daemon_dest)
            .unwrap()
            .read_to_string(&mut daemon)
            .unwrap();
        assert_eq!(cli, "test-cli");
        assert_eq!(daemon, "test-daemon");
        let _ = fs::remove_file(cli_dest);
        let _ = fs::remove_file(daemon_dest);
    }
}
