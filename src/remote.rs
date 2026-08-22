use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs;
use std::io::Write as _;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::mpsc;

use crate::daemon::{self, DaemonBackend};
use crate::scripting::environment::PythonEnvironmentSnapshot;
use crate::scripting::language::{PythonRuntime, ScriptLanguage};

pub const CONFIG_VERSION: u8 = 1;
pub const TRANSPORT_VERSION: u8 = 1;
const CONFIG_FILE: &str = "remotes.json";
const SSH_CONNECT_TIMEOUT_SECONDS: u64 = 10;
const HANDSHAKE_TIMEOUT_SECONDS: u64 = 15;
const TRANSPORT_CHILD_ENV: &str = "MLAB_TRANSPORT_CHILD";
const MAX_SCRIPT_BYTES: usize = 2 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoteProfile {
    pub ssh: String,
    pub mlab: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoteConfig {
    pub version: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active: Option<String>,
    #[serde(default)]
    pub remotes: BTreeMap<String, RemoteProfile>,
}

impl Default for RemoteConfig {
    fn default() -> Self {
        Self {
            version: CONFIG_VERSION,
            active: None,
            remotes: BTreeMap::new(),
        }
    }
}

#[derive(Debug)]
pub enum InvocationRoute {
    Local(Vec<OsString>),
    Remote {
        name: String,
        profile: RemoteProfile,
        args: Vec<String>,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoteHandshake {
    pub transport_version: u8,
    pub marketlab_version: String,
    pub runtime_version: u8,
    pub daemon_backend: DaemonBackend,
    pub os: String,
    pub arch: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum TransportRequest {
    Hello {
        transport_version: u8,
        marketlab_version: String,
    },
    Execute {
        id: String,
        args: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        script: Option<RemoteScriptBundle>,
    },
    Close,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct RemoteScriptBundle {
    argument_index: usize,
    language: ScriptLanguage,
    source_base64: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    python: Option<RemotePythonEnvironment>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct RemotePythonEnvironment {
    python_request: String,
    requirements: String,
    fingerprint: String,
    package_count: usize,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum TransportFrame {
    Hello { handshake: RemoteHandshake },
    Stdout { id: String, data_base64: String },
    Stderr { id: String, data_base64: String },
    Exit { id: String, code: i32 },
    Closed,
    Error { message: String },
}

pub fn config_path() -> Result<PathBuf> {
    Ok(daemon::market_lab_home()?.join(CONFIG_FILE))
}

pub fn is_transport_child() -> bool {
    std::env::var_os(TRANSPORT_CHILD_ENV).is_some()
}

pub fn load() -> Result<RemoteConfig> {
    load_from(&config_path()?)
}

fn load_from(path: &Path) -> Result<RemoteConfig> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(RemoteConfig::default());
        }
        Err(error) => {
            return Err(error).with_context(|| format!("failed to inspect {}", path.display()));
        }
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!(
            "remote configuration {} must be a regular file",
            path.display()
        );
    }
    if metadata.uid() != unsafe { libc::geteuid() } {
        bail!(
            "remote configuration {} is owned by another user",
            path.display()
        );
    }
    if metadata.permissions().mode() & 0o077 != 0 {
        bail!(
            "remote configuration {} has unsafe permissions; expected 0600",
            path.display()
        );
    }
    let source =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let config: RemoteConfig = serde_json::from_str(&source)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    validate_config(&config, path)?;
    Ok(config)
}

pub fn save(config: &RemoteConfig) -> Result<()> {
    save_to(&config_path()?, config)
}

fn save_to(path: &Path, config: &RemoteConfig) -> Result<()> {
    validate_config(config, path)?;
    let parent = path
        .parent()
        .context("remote configuration path has no parent")?;
    fs::create_dir_all(parent).with_context(|| format!("failed to create {}", parent.display()))?;
    fs::set_permissions(parent, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("failed to secure {}", parent.display()))?;
    let temporary = path.with_extension(format!("json.tmp.{}", std::process::id()));
    let encoded = serde_json::to_vec_pretty(config).context("failed to encode remote config")?;
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&temporary)
        .with_context(|| format!("failed to create {}", temporary.display()))?;
    let result = (|| -> Result<()> {
        file.write_all(&encoded)
            .with_context(|| format!("failed to write {}", temporary.display()))?;
        file.sync_all()
            .with_context(|| format!("failed to sync {}", temporary.display()))?;
        fs::rename(&temporary, path)
            .with_context(|| format!("failed to replace {}", path.display()))?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to secure {}", path.display()))
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

pub fn validate_name(name: &str) -> Result<()> {
    if name == "local" {
        bail!("`local` is reserved for the local MarketLab installation");
    }
    if name.is_empty()
        || name.len() > 64
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        bail!("remote name must contain only letters, numbers, '.', '-', or '_'");
    }
    Ok(())
}

fn direct_profile(ssh: &str) -> Result<RemoteProfile> {
    let profile = RemoteProfile {
        ssh: ssh.to_string(),
        mlab: "mlab".to_string(),
    };
    validate_profile(&profile)?;
    Ok(profile)
}

pub fn resolve_target(config: &RemoteConfig, target: &str) -> Result<RemoteProfile> {
    if let Some(profile) = config.remotes.get(target) {
        return Ok(profile.clone());
    }
    direct_profile(target)
}

pub fn remember_target(config: &mut RemoteConfig, target: &str) -> Result<RemoteProfile> {
    let profile = resolve_target(config, target)?;
    config
        .remotes
        .entry(target.to_string())
        .or_insert_with(|| profile.clone());
    Ok(profile)
}

pub fn validate_profile(profile: &RemoteProfile) -> Result<()> {
    if profile.ssh.is_empty()
        || profile.ssh.starts_with('-')
        || profile
            .ssh
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
    {
        bail!("SSH destination must be one user@host or hostname value");
    }
    if profile.mlab.is_empty()
        || profile.mlab.starts_with('-')
        || !profile.mlab.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'.' | b'_' | b'-' | b'~')
        })
    {
        bail!("remote mlab executable must be one shell-safe command name or path");
    }
    Ok(())
}

fn validate_config(config: &RemoteConfig, path: &Path) -> Result<()> {
    if config.version != CONFIG_VERSION {
        bail!(
            "unsupported remote configuration version {} in {}",
            config.version,
            path.display()
        );
    }
    for (name, profile) in &config.remotes {
        if validate_name(name).is_err() && name != &profile.ssh {
            bail!("remote target key `{name}` is neither a saved name nor its SSH destination");
        }
        validate_profile(profile)?;
    }
    if let Some(active) = &config.active
        && !config.remotes.contains_key(active)
    {
        bail!("active remote `{active}` is not configured");
    }
    Ok(())
}

pub fn route_invocation(args: impl IntoIterator<Item = OsString>) -> Result<InvocationRoute> {
    let args = args.into_iter().collect::<Vec<_>>();
    if std::env::var_os(TRANSPORT_CHILD_ENV).is_some() {
        return Ok(InvocationRoute::Local(args));
    }
    let (stripped, requested) = strip_remote_selection(args)?;
    let command = stripped
        .get(1)
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    if matches!(command, "remote" | "transport") {
        if requested.as_deref().is_some_and(|name| name != "local") {
            bail!("--remote cannot be used with the `{command}` command");
        }
        return Ok(InvocationRoute::Local(stripped));
    }
    let config = load()?;
    let selected = match requested.as_deref() {
        Some("local") => None,
        Some(name) => Some(name.to_string()),
        None => config.active.clone(),
    };
    let Some(name) = selected else {
        return Ok(InvocationRoute::Local(stripped));
    };
    let profile = resolve_target(&config, &name)?;
    let forwarded = stripped
        .into_iter()
        .skip(1)
        .map(|arg| {
            arg.into_string()
                .map_err(|_| anyhow::anyhow!("remote command arguments must be valid UTF-8"))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(InvocationRoute::Remote {
        name,
        profile,
        args: forwarded,
    })
}

fn strip_remote_selection(args: Vec<OsString>) -> Result<(Vec<OsString>, Option<String>)> {
    let mut stripped = Vec::with_capacity(args.len());
    let mut requested = None;
    let mut iter = args.into_iter();
    if let Some(program) = iter.next() {
        stripped.push(program);
    }
    while let Some(arg) = iter.next() {
        let value = arg.to_string_lossy();
        if value == "--remote" {
            let name = iter
                .next()
                .context("--remote requires `user@host`, a saved target, or `local`")?
                .into_string()
                .map_err(|_| anyhow::anyhow!("--remote must be valid UTF-8"))?;
            if requested.replace(name).is_some() {
                bail!("--remote may only be specified once");
            }
        } else if let Some(name) = value.strip_prefix("--remote=") {
            if name.is_empty() {
                bail!("--remote requires `user@host`, a saved target, or `local`");
            }
            if requested.replace(name.to_string()).is_some() {
                bail!("--remote may only be specified once");
            }
        } else {
            stripped.push(arg);
        }
    }
    Ok((stripped, requested))
}

pub async fn execute(name: &str, profile: &RemoteProfile, args: Vec<String>) -> Result<i32> {
    if args.is_empty() {
        bail!("a MarketLab command is required for remote execution");
    }
    let mut connection = connect(profile).await?;
    validate_handshake(name, &connection.handshake)?;
    let id = format!("remote-{}", std::process::id());
    let (args, script) = prepare_remote_command(args)?;
    connection
        .send(&TransportRequest::Execute {
            id: id.clone(),
            args,
            script,
        })
        .await?;

    let mut stdout = tokio::io::stdout();
    let mut stderr = tokio::io::stderr();
    let exit_code = loop {
        match connection.read_frame().await? {
            TransportFrame::Stdout {
                id: frame_id,
                data_base64,
            } if frame_id == id => {
                stdout.write_all(&decode_output(&data_base64)?).await?;
                stdout.flush().await?;
            }
            TransportFrame::Stderr {
                id: frame_id,
                data_base64,
            } if frame_id == id => {
                stderr.write_all(&decode_output(&data_base64)?).await?;
                stderr.flush().await?;
            }
            TransportFrame::Exit { id: frame_id, code } if frame_id == id => break code,
            TransportFrame::Error { message } => bail!("remote transport failed: {message}"),
            other => bail!("remote `{name}` returned an unexpected transport frame: {other:?}"),
        }
    };
    connection.finish().await?;
    Ok(exit_code)
}

pub async fn test(name: &str, profile: &RemoteProfile) -> Result<RemoteHandshake> {
    let mut connection = connect(profile).await?;
    validate_handshake(name, &connection.handshake)?;
    connection.send(&TransportRequest::Close).await?;
    match connection.read_frame().await? {
        TransportFrame::Closed => {}
        TransportFrame::Error { message } => bail!("remote transport failed: {message}"),
        other => bail!("remote `{name}` returned an unexpected transport frame: {other:?}"),
    }
    let handshake = connection.handshake.clone();
    connection.finish().await?;
    Ok(handshake)
}

fn validate_handshake(name: &str, handshake: &RemoteHandshake) -> Result<()> {
    if handshake.transport_version != TRANSPORT_VERSION {
        bail!(
            "remote `{name}` transport version {} does not match local version {}; upgrade MarketLab on both machines",
            handshake.transport_version,
            TRANSPORT_VERSION
        );
    }
    if handshake.marketlab_version != env!("CARGO_PKG_VERSION") {
        bail!(
            "remote `{name}` runs mlab {} but this machine runs {}; install the same MarketLab version on both machines",
            handshake.marketlab_version,
            env!("CARGO_PKG_VERSION")
        );
    }
    Ok(())
}

fn prepare_remote_command(
    mut args: Vec<String>,
) -> Result<(Vec<String>, Option<RemoteScriptBundle>)> {
    remove_option_with_value(&mut args, "--config")?;
    if !matches!(args.as_slice(), [command, mode, ..] if command == "script" && matches!(mode.as_str(), "run" | "backtest"))
    {
        return Ok((args, None));
    }
    let script_index = script_argument_index(&args)?;
    let script_path = PathBuf::from(&args[script_index]);
    let language = ScriptLanguage::from_path(&script_path)?;
    let source = fs::read(&script_path)
        .with_context(|| format!("failed to read local script {}", script_path.display()))?;
    if source.len() > MAX_SCRIPT_BYTES {
        bail!(
            "remote script {} is {} bytes; the transport limit is {} bytes",
            script_path.display(),
            source.len(),
            MAX_SCRIPT_BYTES
        );
    }
    let python = if language == ScriptLanguage::PythonV2 {
        let requested = option_value(&args, "--python")?.map(PathBuf::from);
        let runtime = PythonRuntime::resolve(&script_path, requested.as_deref())?;
        let snapshot = PythonEnvironmentSnapshot::capture(&runtime)
            .context("failed to capture the Python environment for remote deployment")?;
        Some(RemotePythonEnvironment {
            python_request: snapshot.python_request,
            requirements: snapshot.requirements,
            fingerprint: snapshot.managed.fingerprint,
            package_count: snapshot.managed.package_count,
        })
    } else {
        None
    };
    remove_option_with_value(&mut args, "--python")?;
    let argument_index = script_argument_index(&args)?;
    Ok((
        args,
        Some(RemoteScriptBundle {
            argument_index,
            language,
            source_base64: base64::engine::general_purpose::STANDARD.encode(source),
            python,
        }),
    ))
}

fn option_value(args: &[String], option: &str) -> Result<Option<String>> {
    let mut found = None;
    let mut index = 0;
    while index < args.len() {
        if args[index] == option {
            let value = args
                .get(index + 1)
                .with_context(|| format!("{option} requires a value"))?
                .clone();
            if found.replace(value).is_some() {
                bail!("{option} may only be specified once");
            }
            index += 2;
        } else if let Some(value) = args[index].strip_prefix(&format!("{option}=")) {
            if value.is_empty() {
                bail!("{option} requires a value");
            }
            if found.replace(value.to_string()).is_some() {
                bail!("{option} may only be specified once");
            }
            index += 1;
        } else {
            index += 1;
        }
    }
    Ok(found)
}

fn remove_option_with_value(args: &mut Vec<String>, option: &str) -> Result<()> {
    let mut cleaned = Vec::with_capacity(args.len());
    let mut index = 0;
    while index < args.len() {
        if args[index] == option {
            if args.get(index + 1).is_none() {
                bail!("{option} requires a value");
            }
            index += 2;
        } else if args[index].starts_with(&format!("{option}=")) {
            index += 1;
        } else {
            cleaned.push(args[index].clone());
            index += 1;
        }
    }
    *args = cleaned;
    Ok(())
}

fn script_argument_index(args: &[String]) -> Result<usize> {
    const VALUE_OPTIONS: &[&str] = &[
        "--config",
        "--python",
        "--venue",
        "--from",
        "--to",
        "--source",
        "--param",
        "--duration",
        "--output",
    ];
    let mut index = 2;
    while index < args.len() {
        let argument = &args[index];
        if VALUE_OPTIONS.contains(&argument.as_str()) {
            if args.get(index + 1).is_none() {
                bail!("{argument} requires a value");
            }
            index += 2;
        } else if VALUE_OPTIONS
            .iter()
            .any(|option| argument.starts_with(&format!("{option}=")))
            || matches!(argument.as_str(), "--testnet" | "--verbose")
        {
            index += 1;
        } else if argument.starts_with('-') {
            bail!("cannot deploy remote script with unrecognized option `{argument}`");
        } else {
            return Ok(index);
        }
    }
    bail!("script path is required for remote execution")
}

struct StagedCommand {
    args: Vec<String>,
    _staging: Option<StagingDirectory>,
}

struct StagingDirectory(PathBuf);

impl Drop for StagingDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

async fn stage_remote_command(
    mut args: Vec<String>,
    script: Option<RemoteScriptBundle>,
) -> Result<StagedCommand> {
    let Some(script) = script else {
        return Ok(StagedCommand {
            args,
            _staging: None,
        });
    };
    let argument = args
        .get_mut(script.argument_index)
        .context("remote script argument index is invalid")?;
    let source = decode_output(&script.source_base64)?;
    if source.len() > MAX_SCRIPT_BYTES {
        bail!("remote script exceeds the {MAX_SCRIPT_BYTES} byte transport limit");
    }
    let root = daemon::market_lab_home()?.join("transport");
    secure_directory(&root)?;
    let directory = root.join(format!(
        "command-{}-{}",
        std::process::id(),
        random_suffix()
    ));
    secure_directory(&directory)?;
    let staging = StagingDirectory(directory.clone());
    let script_path = directory.join(script.language.snapshot_file_name());
    write_secure_file(&script_path, &source)?;
    *argument = script_path.display().to_string();
    if let Some(python) = script.python {
        let interpreter = prepare_remote_python_environment(&python).await?;
        args.push("--python".to_string());
        args.push(interpreter.display().to_string());
    }
    Ok(StagedCommand {
        args,
        _staging: Some(staging),
    })
}

async fn prepare_remote_python_environment(
    environment: &RemotePythonEnvironment,
) -> Result<PathBuf> {
    validate_python_environment(environment)?;
    let root = daemon::market_lab_home()?.join("remote-python-runtimes");
    secure_directory(&root)?;
    let directory = root.join(&environment.fingerprint);
    let interpreter = directory.join(".venv/bin/python");
    let marker = directory.join("runtime.json");
    if interpreter.is_file() && marker.is_file() {
        return Ok(interpreter);
    }
    let lock_path = root.join(format!("{}.lock", environment.fingerprint));
    let lock = acquire_runtime_lock(&lock_path, &interpreter, &marker).await?;
    if interpreter.is_file() && marker.is_file() {
        drop(lock);
        return Ok(interpreter);
    }
    if directory.exists() {
        fs::remove_dir_all(&directory)
            .with_context(|| format!("failed to replace {}", directory.display()))?;
    }
    secure_directory(&directory)?;
    let requirements = directory.join("requirements.lock");
    write_secure_file(&requirements, environment.requirements.as_bytes())?;
    let result = create_remote_python_environment(environment, &directory, &requirements).await;
    if let Err(error) = result {
        let _ = fs::remove_dir_all(&directory);
        return Err(error);
    }
    let marker_value = serde_json::json!({
        "version": 1,
        "fingerprint": environment.fingerprint,
        "pythonRequest": environment.python_request,
        "packageCount": environment.package_count,
        "interpreter": interpreter,
    });
    write_secure_file(
        &marker,
        &serde_json::to_vec_pretty(&marker_value)
            .context("failed to encode remote Python runtime marker")?,
    )?;
    drop(lock);
    Ok(interpreter)
}

fn validate_python_environment(environment: &RemotePythonEnvironment) -> Result<()> {
    if environment.fingerprint.len() != 64
        || !environment
            .fingerprint
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        bail!("remote Python environment fingerprint is invalid");
    }
    let (major, minor) = environment
        .python_request
        .split_once('.')
        .context("remote Python version must be major.minor")?;
    if major != "3" || minor.parse::<u16>().is_err() {
        bail!("remote Python version is invalid");
    }
    if environment.requirements.len() > 96 * 1024
        || environment.requirements.lines().count() != environment.package_count
    {
        bail!("remote Python dependency snapshot is invalid");
    }
    Ok(())
}

struct RuntimeLock(PathBuf);

impl Drop for RuntimeLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

async fn acquire_runtime_lock(
    path: &Path,
    interpreter: &Path,
    marker: &Path,
) -> Result<Option<RuntimeLock>> {
    for _ in 0..120 {
        match fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(path)
        {
            Ok(_) => return Ok(Some(RuntimeLock(path.to_path_buf()))),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                if interpreter.is_file() && marker.is_file() {
                    return Ok(None);
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("failed to lock remote Python runtime {}", path.display())
                });
            }
        }
    }
    bail!(
        "timed out waiting for remote Python runtime {}; remove stale lock {} if no deployment is active",
        interpreter.display(),
        path.display()
    )
}

async fn create_remote_python_environment(
    environment: &RemotePythonEnvironment,
    directory: &Path,
    requirements: &Path,
) -> Result<()> {
    let venv = directory.join(".venv");
    let interpreter = venv.join("bin/python");
    let uv_available = Command::new("uv")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .is_ok_and(|status| status.success());
    if uv_available {
        command_success(
            Command::new("uv")
                .args(["venv", "--python", &environment.python_request])
                .arg(&venv),
            "create the remote Python environment with uv",
        )
        .await?;
        if environment.package_count > 0 {
            command_success(
                Command::new("uv")
                    .args(["pip", "install", "--python"])
                    .arg(&interpreter)
                    .arg("--requirements")
                    .arg(requirements),
                "install remote Python dependencies with uv",
            )
            .await?;
        }
        return Ok(());
    }

    let python = find_remote_python(&environment.python_request).await?;
    command_success(
        Command::new(&python).args(["-m", "venv"]).arg(&venv),
        "create the remote Python environment",
    )
    .await?;
    if environment.package_count > 0 {
        command_success(
            Command::new(&interpreter)
                .args(["-m", "pip", "install", "--requirement"])
                .arg(requirements),
            "install remote Python dependencies",
        )
        .await?;
    }
    Ok(())
}

async fn find_remote_python(request: &str) -> Result<String> {
    for candidate in [format!("python{request}"), "python3".to_string()] {
        let output = Command::new(&candidate)
            .args([
                "-c",
                &format!(
                    "import sys; raise SystemExit(0 if sys.version_info[:2] == ({}, {}) else 1)",
                    request.split('.').next().unwrap_or("3"),
                    request.split('.').nth(1).unwrap_or("0")
                ),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await;
        if output.is_ok_and(|status| status.success()) {
            return Ok(candidate);
        }
    }
    bail!("remote host does not provide Python {request}; install it or install uv")
}

async fn command_success(command: &mut Command, action: &str) -> Result<()> {
    let output = command
        .output()
        .await
        .with_context(|| format!("failed to {action}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        bail!(
            "failed to {action}{}",
            if stderr.is_empty() {
                String::new()
            } else {
                format!(": {stderr}")
            }
        );
    }
    Ok(())
}

fn secure_directory(path: &Path) -> Result<()> {
    fs::create_dir_all(path).with_context(|| format!("failed to create {}", path.display()))?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("failed to secure {}", path.display()))
}

fn write_secure_file(path: &Path, contents: &[u8]) -> Result<()> {
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .open(path)
        .with_context(|| format!("failed to create {}", path.display()))?;
    file.write_all(contents)
        .with_context(|| format!("failed to write {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("failed to sync {}", path.display()))
}

fn random_suffix() -> String {
    use rand_core::RngCore as _;

    format!("{:016x}", rand_core::OsRng.next_u64())
}

struct SshConnection {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    stderr_task: tokio::task::JoinHandle<Result<()>>,
    handshake: RemoteHandshake,
}

impl SshConnection {
    async fn send(&mut self, request: &TransportRequest) -> Result<()> {
        write_json_line(&mut self.stdin, request)
            .await
            .context("failed to write to the remote MarketLab transport")
    }

    async fn read_frame(&mut self) -> Result<TransportFrame> {
        read_json_line(&mut self.stdout)
            .await
            .context("failed to read from the remote MarketLab transport")
    }

    async fn finish(mut self) -> Result<()> {
        self.stdin.shutdown().await.ok();
        let status = self
            .child
            .wait()
            .await
            .context("failed to wait for SSH transport")?;
        self.stderr_task
            .await
            .context("SSH diagnostic task failed")??;
        if !status.success() {
            bail!("SSH transport exited with {status}");
        }
        Ok(())
    }
}

async fn connect(profile: &RemoteProfile) -> Result<SshConnection> {
    validate_profile(profile)?;
    let remote_command = remote_transport_command(profile)?;
    let mut child = Command::new("ssh")
        .args([
            "-T",
            "-o",
            &format!("ConnectTimeout={SSH_CONNECT_TIMEOUT_SECONDS}"),
            "-o",
            "ServerAliveInterval=15",
            "-o",
            "ServerAliveCountMax=3",
        ])
        .arg(&profile.ssh)
        .arg(remote_command)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("failed to start OpenSSH for {}", profile.ssh))?;
    let mut stdin = child.stdin.take().context("SSH stdin was not captured")?;
    let stdout = child.stdout.take().context("SSH stdout was not captured")?;
    let mut ssh_stderr = child.stderr.take().context("SSH stderr was not captured")?;
    let stderr_task = tokio::spawn(async move {
        let mut stderr = tokio::io::stderr();
        tokio::io::copy(&mut ssh_stderr, &mut stderr)
            .await
            .context("failed to forward SSH diagnostics")?;
        Ok(())
    });
    write_json_line(
        &mut stdin,
        &TransportRequest::Hello {
            transport_version: TRANSPORT_VERSION,
            marketlab_version: env!("CARGO_PKG_VERSION").to_string(),
        },
    )
    .await
    .context("failed to start the remote MarketLab handshake")?;
    let mut stdout = BufReader::new(stdout);
    let frame = tokio::time::timeout(
        Duration::from_secs(HANDSHAKE_TIMEOUT_SECONDS),
        read_json_line::<_, TransportFrame>(&mut stdout),
    )
    .await
    .context("remote MarketLab handshake timed out")??;
    let handshake = match frame {
        TransportFrame::Hello { handshake } => handshake,
        TransportFrame::Error { message } => {
            bail!("remote transport rejected the handshake: {message}")
        }
        other => bail!("remote MarketLab returned an invalid handshake frame: {other:?}"),
    };
    Ok(SshConnection {
        child,
        stdin,
        stdout,
        stderr_task,
        handshake,
    })
}

fn remote_transport_command(profile: &RemoteProfile) -> Result<String> {
    validate_profile(profile)?;
    let missing = serde_json::to_string(&TransportFrame::Error {
        message: format!(
            "remote mlab `{}` was not found in the non-interactive SSH environment; MarketLab checks ~/.local/bin and /usr/local/bin automatically",
            profile.mlab
        ),
    })?;
    let unsupported = serde_json::to_string(&TransportFrame::Error {
        message: "the remote mlab build does not support SSH transport; install the same MarketLab build on both machines"
            .to_string(),
    })?;
    Ok(format!(
        "PATH=\"$HOME/.local/bin:/usr/local/bin:$PATH\"; export PATH; \
         if ! command -v {mlab} >/dev/null 2>&1; then printf '%s\\n' '{missing}'; exit 127; fi; \
         if ! {mlab} transport --help >/dev/null 2>&1; then printf '%s\\n' '{unsupported}'; exit 64; fi; \
         exec {mlab} transport serve",
        mlab = profile.mlab,
    ))
}

pub async fn serve() -> Result<()> {
    let mut input = BufReader::new(tokio::io::stdin());
    let mut output = tokio::io::stdout();
    let hello: TransportRequest = read_json_line(&mut input)
        .await
        .context("transport handshake was not received")?;
    let (client_transport, _client_version) = match hello {
        TransportRequest::Hello {
            transport_version,
            marketlab_version,
        } => (transport_version, marketlab_version),
        _ => {
            write_json_line(
                &mut output,
                &TransportFrame::Error {
                    message: "first transport frame must be hello".to_string(),
                },
            )
            .await?;
            return Ok(());
        }
    };
    let config = daemon::load()?;
    let handshake = RemoteHandshake {
        transport_version: TRANSPORT_VERSION,
        marketlab_version: env!("CARGO_PKG_VERSION").to_string(),
        runtime_version: crate::runtime::RUNTIME_VERSION,
        daemon_backend: config.backend,
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
    };
    write_json_line(&mut output, &TransportFrame::Hello { handshake }).await?;
    if client_transport != TRANSPORT_VERSION {
        return Ok(());
    }
    match read_json_line::<_, TransportRequest>(&mut input).await? {
        TransportRequest::Execute { id, args, script } => {
            if let Err(error) = execute_local(&mut output, id, args, script).await {
                write_json_line(
                    &mut output,
                    &TransportFrame::Error {
                        message: format!("{error:#}"),
                    },
                )
                .await?;
            }
            Ok(())
        }
        TransportRequest::Close => write_json_line(&mut output, &TransportFrame::Closed).await,
        TransportRequest::Hello { .. } => {
            write_json_line(
                &mut output,
                &TransportFrame::Error {
                    message: "transport handshake was already completed".to_string(),
                },
            )
            .await
        }
    }
}

enum ChildChunk {
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
}

async fn execute_local<W>(
    output: &mut W,
    id: String,
    args: Vec<String>,
    script: Option<RemoteScriptBundle>,
) -> Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    if args.is_empty() {
        return write_json_line(
            output,
            &TransportFrame::Error {
                message: "remote command is empty".to_string(),
            },
        )
        .await;
    }
    let staged = stage_remote_command(args, script).await?;
    let executable = std::env::current_exe().context("failed to locate remote mlab executable")?;
    let mut child = Command::new(executable)
        .args(&staged.args)
        .env(TRANSPORT_CHILD_ENV, "1")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .context("failed to execute remote mlab command")?;
    let stdout = child
        .stdout
        .take()
        .context("remote command stdout was not captured")?;
    let stderr = child
        .stderr
        .take()
        .context("remote command stderr was not captured")?;
    let (sender, mut receiver) = mpsc::channel::<ChildChunk>(32);
    let stdout_task = spawn_chunk_reader(stdout, sender.clone(), true);
    let stderr_task = spawn_chunk_reader(stderr, sender.clone(), false);
    drop(sender);
    let mut wait = Box::pin(child.wait());
    let mut exit_status = None;
    loop {
        if exit_status.is_some() && receiver.is_closed() && receiver.is_empty() {
            break;
        }
        tokio::select! {
            status = &mut wait, if exit_status.is_none() => {
                exit_status = Some(status.context("failed to wait for remote mlab command")?);
            }
            chunk = receiver.recv() => {
                if let Some(chunk) = chunk {
                    let frame = match chunk {
                        ChildChunk::Stdout(bytes) => TransportFrame::Stdout {
                            id: id.clone(),
                            data_base64: base64::engine::general_purpose::STANDARD.encode(bytes),
                        },
                        ChildChunk::Stderr(bytes) => TransportFrame::Stderr {
                            id: id.clone(),
                            data_base64: base64::engine::general_purpose::STANDARD.encode(bytes),
                        },
                    };
                    write_json_line(output, &frame).await?;
                }
            }
        }
    }
    stdout_task.await.context("remote stdout task failed")??;
    stderr_task.await.context("remote stderr task failed")??;
    let status = exit_status.context("remote command exited without a status")?;
    write_json_line(
        output,
        &TransportFrame::Exit {
            id,
            code: status.code().unwrap_or(1),
        },
    )
    .await
}

fn spawn_chunk_reader<R>(
    mut reader: R,
    sender: mpsc::Sender<ChildChunk>,
    stdout: bool,
) -> tokio::task::JoinHandle<Result<()>>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut buffer = vec![0_u8; 8 * 1024];
        loop {
            let bytes = reader.read(&mut buffer).await?;
            if bytes == 0 {
                return Ok(());
            }
            let chunk = if stdout {
                ChildChunk::Stdout(buffer[..bytes].to_vec())
            } else {
                ChildChunk::Stderr(buffer[..bytes].to_vec())
            };
            if sender.send(chunk).await.is_err() {
                return Ok(());
            }
        }
    })
}

async fn write_json_line<W, T>(writer: &mut W, value: &T) -> Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
    T: Serialize,
{
    let mut encoded = serde_json::to_vec(value).context("failed to encode transport frame")?;
    encoded.push(b'\n');
    writer.write_all(&encoded).await?;
    writer.flush().await?;
    Ok(())
}

fn decode_output(encoded: &str) -> Result<Vec<u8>> {
    base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .context("remote transport returned invalid base64 output")
}

async fn read_json_line<R, T>(reader: &mut R) -> Result<T>
where
    R: tokio::io::AsyncBufRead + Unpin,
    T: for<'de> Deserialize<'de>,
{
    let mut line = String::new();
    let bytes = reader.read_line(&mut line).await?;
    if bytes == 0 {
        bail!("SSH connection closed before the transport response completed");
    }
    serde_json::from_str(&line).with_context(|| {
        format!(
            "invalid transport frame `{}`; ensure the remote shell does not print startup text",
            line.trim()
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_directory(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "marketlab-remote-{name}-{}-{}",
            std::process::id(),
            rand_core::OsRng.next_u64()
        ))
    }

    use rand_core::RngCore as _;

    #[test]
    fn remote_config_round_trips_with_owner_only_permissions() {
        let directory = test_directory("roundtrip");
        fs::create_dir(&directory).expect("test directory should be created");
        let path = directory.join("remotes.json");
        let config = RemoteConfig {
            active: Some("tokyo".to_string()),
            remotes: BTreeMap::from([(
                "tokyo".to_string(),
                RemoteProfile {
                    ssh: "marketlab-tokyo".to_string(),
                    mlab: "/usr/local/bin/mlab".to_string(),
                },
            )]),
            ..RemoteConfig::default()
        };
        save_to(&path, &config).expect("config should save");
        assert_eq!(load_from(&path).expect("config should load"), config);
        let mode = fs::metadata(&path)
            .expect("config metadata")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
        fs::remove_file(path).expect("test config should be removed");
        fs::remove_dir(directory).expect("test directory should be removed");
    }

    #[test]
    fn remote_profiles_reject_ssh_and_command_injection() {
        for profile in [
            RemoteProfile {
                ssh: "-oProxyCommand=evil".to_string(),
                mlab: "mlab".to_string(),
            },
            RemoteProfile {
                ssh: "safe".to_string(),
                mlab: "mlab;evil".to_string(),
            },
            RemoteProfile {
                ssh: "user@host extra".to_string(),
                mlab: "mlab".to_string(),
            },
        ] {
            assert!(validate_profile(&profile).is_err());
        }
    }

    #[test]
    fn direct_ssh_targets_need_no_saved_profile() {
        let config = RemoteConfig::default();
        let profile = resolve_target(&config, "trader@203.0.113.10")
            .expect("direct SSH destination should resolve");
        assert_eq!(profile.ssh, "trader@203.0.113.10");
        assert_eq!(profile.mlab, "mlab");
        assert!(
            config.remotes.is_empty(),
            "one-command overrides are not saved"
        );
    }

    #[test]
    fn ssh_transport_resolves_official_install_locations_and_preflights_support() {
        let profile = RemoteProfile {
            ssh: "trader@203.0.113.10".to_string(),
            mlab: "mlab".to_string(),
        };
        let command = remote_transport_command(&profile)
            .expect("remote transport command should be constructed");
        assert!(command.contains("$HOME/.local/bin:/usr/local/bin:$PATH"));
        assert!(command.contains("command -v mlab"));
        assert!(command.contains("mlab transport --help"));
        assert!(command.contains("exec mlab transport serve"));
    }

    #[test]
    fn default_direct_ssh_targets_are_remembered() {
        let mut config = RemoteConfig::default();
        let profile = remember_target(&mut config, "trader@203.0.113.10")
            .expect("direct SSH destination should be remembered");
        config.active = Some("trader@203.0.113.10".to_string());
        validate_config(&config, Path::new("remotes.json"))
            .expect("direct destination should be a valid active target");
        assert_eq!(config.remotes.get("trader@203.0.113.10"), Some(&profile));
    }

    #[test]
    fn remote_selector_is_removed_before_forwarding() {
        let (args, selected) = strip_remote_selection(
            ["mlab", "bot", "jobs", "--remote=tokyo", "--output", "json"]
                .into_iter()
                .map(OsString::from)
                .collect(),
        )
        .expect("selector should parse");
        assert_eq!(selected.as_deref(), Some("tokyo"));
        assert_eq!(
            args,
            ["mlab", "bot", "jobs", "--output", "json"]
                .into_iter()
                .map(OsString::from)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn transport_frames_are_newline_safe_json() {
        let frame = TransportFrame::Stdout {
            id: "request-1".to_string(),
            data_base64: base64::engine::general_purpose::STANDARD.encode("first\nsecond\n"),
        };
        let encoded = serde_json::to_string(&frame).expect("frame should encode");
        assert_eq!(encoded.lines().count(), 1);
        let decoded: TransportFrame = serde_json::from_str(&encoded).expect("frame should decode");
        assert!(matches!(
            decoded,
            TransportFrame::Stdout { data_base64, .. }
                if decode_output(&data_base64).expect("output should decode") == b"first\nsecond\n"
        ));
    }

    #[test]
    fn remote_script_is_bundled_and_local_only_paths_are_removed() {
        let directory = test_directory("script-bundle");
        fs::create_dir(&directory).expect("test directory should be created");
        let script = directory.join("strategy.js");
        fs::write(
            &script,
            "export const script = { version: '1', name: 'remote' };\n",
        )
        .expect("test script should be written");
        let config = directory.join("marketlab.toml");
        let args = vec![
            "script".to_string(),
            "run".to_string(),
            "--config".to_string(),
            config.display().to_string(),
            script.display().to_string(),
            "--source".to_string(),
            "btc@candles@hyperliquidf".to_string(),
        ];
        let (args, bundle) = prepare_remote_command(args).expect("script should bundle");
        let bundle = bundle.expect("script bundle should be present");
        assert!(!args.iter().any(|argument| argument == "--config"));
        assert_eq!(args[bundle.argument_index], script.display().to_string());
        assert_eq!(bundle.language, ScriptLanguage::JavaScriptV1);
        assert_eq!(
            decode_output(&bundle.source_base64).expect("source should decode"),
            fs::read(&script).expect("test script should read")
        );
        fs::remove_dir_all(directory).expect("test directory should be removed");
    }
}
