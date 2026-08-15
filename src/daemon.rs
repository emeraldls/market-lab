use std::fs;
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use rand_core::{OsRng, RngCore};
use serde::{Deserialize, Serialize};

pub const CONFIG_VERSION: u8 = 1;
pub const DOCKER_CONTAINER_PORT: u16 = 47_831;
pub const DEFAULT_DOCKER_CONTAINER: &str = "marketlab-mlabd";
pub const DOCKER_IMAGE_REPOSITORY: &str = "ghcr.io/emeraldls/market-lab-daemon";
const CONFIG_FILE: &str = "daemon.json";
const TOKEN_FILE: &str = "daemon.token";

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DaemonBackend {
    #[default]
    Native,
    Docker,
}

impl DaemonBackend {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Docker => "docker",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DockerDaemonConfig {
    pub image: String,
    pub container: String,
    pub host: String,
    pub port: u16,
}

impl Default for DockerDaemonConfig {
    fn default() -> Self {
        Self {
            image: docker_image_for_version(env!("CARGO_PKG_VERSION")),
            container: DEFAULT_DOCKER_CONTAINER.to_string(),
            host: "127.0.0.1".to_string(),
            port: DOCKER_CONTAINER_PORT,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DaemonConfig {
    pub version: u8,
    pub backend: DaemonBackend,
    #[serde(default)]
    pub docker: DockerDaemonConfig,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            version: CONFIG_VERSION,
            backend: DaemonBackend::Native,
            docker: DockerDaemonConfig::default(),
        }
    }
}

impl DaemonConfig {
    pub fn docker_for_version(version: &str) -> Self {
        Self {
            backend: DaemonBackend::Docker,
            docker: DockerDaemonConfig {
                image: docker_image_for_version(version),
                ..DockerDaemonConfig::default()
            },
            ..Self::default()
        }
    }
}

pub fn docker_image_for_version(version: &str) -> String {
    format!("{DOCKER_IMAGE_REPOSITORY}:v{version}")
}

pub fn market_lab_home() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("MLAB_HOME") {
        return Ok(PathBuf::from(path));
    }
    let home = std::env::var_os("HOME").context("HOME is required for Market Lab")?;
    Ok(PathBuf::from(home).join(".market-lab"))
}

pub fn config_path() -> Result<PathBuf> {
    Ok(market_lab_home()?.join(CONFIG_FILE))
}

pub fn token_path() -> Result<PathBuf> {
    Ok(market_lab_home()?.join(TOKEN_FILE))
}

pub fn load() -> Result<DaemonConfig> {
    load_from(&config_path()?)
}

fn load_from(path: &Path) -> Result<DaemonConfig> {
    let source = match fs::read_to_string(path) {
        Ok(source) => source,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(DaemonConfig::default());
        }
        Err(error) => {
            return Err(error).with_context(|| format!("failed to read {}", path.display()));
        }
    };
    let config: DaemonConfig = serde_json::from_str(&source)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    if config.version != CONFIG_VERSION {
        bail!(
            "unsupported daemon configuration version {} in {}",
            config.version,
            path.display()
        );
    }
    if config.docker.host != "127.0.0.1" && config.docker.host != "::1" {
        bail!("Docker daemon endpoint must be bound to a loopback address");
    }
    if config.docker.port == 0 {
        bail!("Docker daemon endpoint port must be greater than zero");
    }
    Ok(config)
}

pub fn save(config: &DaemonConfig) -> Result<()> {
    save_to(&config_path()?, config)
}

fn save_to(path: &Path, config: &DaemonConfig) -> Result<()> {
    let parent = path
        .parent()
        .context("daemon configuration path has no parent")?;
    fs::create_dir_all(parent).with_context(|| format!("failed to create {}", parent.display()))?;
    fs::set_permissions(parent, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("failed to secure {}", parent.display()))?;
    let temporary = path.with_extension("json.tmp");
    let encoded = serde_json::to_vec_pretty(config).context("failed to encode daemon config")?;
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .open(&temporary)
        .with_context(|| format!("failed to create {}", temporary.display()))?;
    file.write_all(&encoded)
        .with_context(|| format!("failed to write {}", temporary.display()))?;
    file.sync_all()
        .with_context(|| format!("failed to sync {}", temporary.display()))?;
    fs::rename(&temporary, path)
        .with_context(|| format!("failed to replace {}", path.display()))?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("failed to secure {}", path.display()))
}

pub fn ensure_token() -> Result<String> {
    let path = token_path()?;
    match read_token_from(&path) {
        Ok(token) => return Ok(token),
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound) => {}
        Err(error) => return Err(error),
    }
    let mut bytes = [0_u8; 32];
    OsRng.fill_bytes(&mut bytes);
    let token = hex::encode(bytes);
    write_token_to(&path, &token)?;
    Ok(token)
}

pub fn read_token() -> Result<String> {
    read_token_from(&token_path()?)
}

fn read_token_from(path: &Path) -> Result<String> {
    let token =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let token = token.trim();
    if token.len() != 64 || !token.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!(
            "{} contains an invalid daemon authentication token",
            path.display()
        );
    }
    Ok(token.to_string())
}

fn write_token_to(path: &Path, token: &str) -> Result<()> {
    let parent = path.parent().context("daemon token path has no parent")?;
    fs::create_dir_all(parent).with_context(|| format!("failed to create {}", parent.display()))?;
    fs::set_permissions(parent, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("failed to secure {}", parent.display()))?;
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(path)
        .with_context(|| format!("failed to create {}", path.display()))?;
    file.write_all(token.as_bytes())
        .with_context(|| format!("failed to write {}", path.display()))?;
    file.write_all(b"\n")
        .with_context(|| format!("failed to write {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("failed to sync {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temporary_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "market-lab-daemon-{name}-{}-{}",
            std::process::id(),
            rand_core::OsRng.next_u64()
        ))
    }

    #[test]
    fn missing_config_preserves_native_behavior() {
        let path = temporary_path("missing").join(CONFIG_FILE);
        assert_eq!(load_from(&path).unwrap(), DaemonConfig::default());
    }

    #[test]
    fn config_round_trips_with_private_permissions() {
        let directory = temporary_path("roundtrip");
        let path = directory.join(CONFIG_FILE);
        let config = DaemonConfig::docker_for_version("9.8.7");
        save_to(&path, &config).unwrap();
        assert_eq!(load_from(&path).unwrap(), config);
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn token_validation_rejects_short_or_non_hex_values() {
        let directory = temporary_path("token");
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join(TOKEN_FILE);
        fs::write(&path, "not-a-token\n").unwrap();
        assert!(read_token_from(&path).is_err());
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn release_versions_map_to_immutable_image_tags() {
        assert_eq!(
            docker_image_for_version("0.0.8"),
            "ghcr.io/emeraldls/market-lab-daemon:v0.0.8"
        );
    }
}
