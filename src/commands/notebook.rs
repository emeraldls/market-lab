use std::env;
use std::ffi::OsString;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use rand_core::{OsRng, RngCore};
use serde_json::json;
use tokio::net::UnixListener;
use tokio::process::Command;

use crate::cli::NotebookArgs;
use crate::daemon;
use crate::scripting::language::PythonRuntime;
use crate::scripting::notebook::{self, write_bootstrap};

const SESSION_DIRECTORY: &str = "run/notebooks";
const SOCKET_FILE: &str = "bridge.sock";

pub async fn handle(args: NotebookArgs) -> Result<()> {
    let current_dir = env::current_dir().context("failed to resolve the notebook directory")?;
    let runtime = PythonRuntime::resolve(&current_dir.join("notebook.py"), args.python.as_deref())?;
    let jupyter = resolve_notebook_dependencies(&runtime, &current_dir).await?;

    let session = NotebookSession::create(&runtime)?;
    let listener = UnixListener::bind(&session.socket_path).with_context(|| {
        format!(
            "failed to bind notebook bridge {}",
            session.socket_path.display()
        )
    })?;
    secure_file(&session.socket_path, 0o600)?;

    let mut server = tokio::spawn(notebook::serve(listener, session.token.clone()));
    let mut command = Command::new(&jupyter.program);
    if args.no_browser {
        command.arg("--no-browser");
    }
    command
        .arg(format!("--ServerApp.root_dir={}", current_dir.display()))
        .arg("--MappingKernelManager.default_kernel_name=python3")
        .env("JUPYTER_PATH", &session.jupyter_path)
        .current_dir(&current_dir)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    command.as_std_mut().process_group(0);

    eprintln!("starting Market Lab notebook research session");
    eprintln!(
        "  python:  {} ({})",
        runtime.interpreter.display(),
        runtime.version
    );
    eprintln!("  folder:  {}", current_dir.display());
    eprintln!("  kernel:  Market Lab ({})", runtime.version);
    eprintln!("  mode:    research");

    let mut child = command
        .spawn()
        .with_context(|| format!("failed to start {}", jupyter.display))?;
    let status = tokio::select! {
        result = child.wait() => Some(result.context("failed while waiting for Jupyter")?),
        result = &mut server => {
            let result = result.context("notebook bridge task failed")?;
            if let Err(error) = result {
                let _ = stop_jupyter(&mut child).await;
                return Err(error).context("Market Lab notebook bridge stopped");
            }
            None
        }
        result = tokio::signal::ctrl_c() => {
            result.context("failed to listen for notebook shutdown")?;
            stop_jupyter(&mut child).await?;
            None
        }
    };
    server.abort();
    if let Some(status) = status
        && !status.success()
    {
        bail!("Jupyter exited with status {status}");
    }
    Ok(())
}

async fn stop_jupyter(child: &mut tokio::process::Child) -> Result<()> {
    let Some(pid) = child.id() else {
        return Ok(());
    };
    let result = unsafe { libc::kill(-(pid as i32), libc::SIGTERM) };
    if result != 0 {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ESRCH) {
            return Err(error).context("failed to stop the Jupyter process group");
        }
    }
    match tokio::time::timeout(Duration::from_secs(5), child.wait()).await {
        Ok(result) => {
            result.context("failed to wait for Jupyter shutdown")?;
        }
        Err(_) => {
            child.kill().await.context("failed to terminate Jupyter")?;
            child
                .wait()
                .await
                .context("failed to wait for Jupyter termination")?;
        }
    }
    Ok(())
}

struct JupyterCommand {
    program: PathBuf,
    display: String,
}

async fn resolve_notebook_dependencies(
    runtime: &PythonRuntime,
    project_directory: &Path,
) -> Result<JupyterCommand> {
    let mut missing = Vec::new();
    if !python_module_available(runtime, "ipykernel").await? {
        missing.push("ipykernel");
    }
    let jupyter = find_jupyter(runtime);
    if jupyter.is_none() {
        missing.push("jupyterlab");
    }
    if !missing.is_empty() {
        bail!(
            "Python notebook dependencies are missing: {}\nInstall them with:\n{}",
            missing.join(", "),
            package_install_command(project_directory, &missing)
        );
    }
    Ok(jupyter.expect("JupyterLab was checked above"))
}

fn find_jupyter(runtime: &PythonRuntime) -> Option<JupyterCommand> {
    if let Some(directory) = runtime.interpreter.parent() {
        if let Some(command) = jupyter_command_at(directory.join("jupyter-lab")) {
            return Some(command);
        }
    }
    if let Some(program) = find_on_path("jupyter-lab") {
        return jupyter_command_at(program);
    }
    None
}

fn jupyter_command_at(program: PathBuf) -> Option<JupyterCommand> {
    if !program.is_file() {
        return None;
    }
    Some(JupyterCommand {
        display: program.display().to_string(),
        program,
    })
}

async fn python_module_available(runtime: &PythonRuntime, module: &str) -> Result<bool> {
    let output = Command::new(&runtime.interpreter)
        .args([
            "-c",
            &format!(
                "import importlib.util; raise SystemExit(0 if importlib.util.find_spec({module:?}) else 1)"
            ),
        ])
        .output()
        .await
        .with_context(|| {
            format!(
                "failed to inspect Python environment {}",
                runtime.interpreter.display()
            )
        })?;
    Ok(output.status.success())
}

fn package_install_command(project_directory: &Path, packages: &[&str]) -> String {
    if uses_uv(project_directory) {
        format!("uv add {}", packages.join(" "))
    } else {
        format!("pip install {}", packages.join(" "))
    }
}

fn uses_uv(project_directory: &Path) -> bool {
    for directory in project_directory.ancestors() {
        if directory.join("uv.lock").is_file() {
            return true;
        }
        if directory.join("pyproject.toml").is_file()
            || directory.join("requirements.txt").is_file()
        {
            return false;
        }
    }
    false
}

struct NotebookSession {
    root: PathBuf,
    socket_path: PathBuf,
    token: String,
    jupyter_path: OsString,
}

impl NotebookSession {
    fn create(runtime: &PythonRuntime) -> Result<Self> {
        let session_id = random_hex(12);
        let token = random_hex(32);
        let root = daemon::market_lab_home()?
            .join(SESSION_DIRECTORY)
            .join(&session_id);
        let bootstrap = root.join("bootstrap");
        let ipython = root.join("ipython/profile_default");
        let jupyter = root.join("jupyter");
        let kernel = jupyter.join("kernels/python3");
        for directory in [&root, &bootstrap, &ipython, &kernel] {
            fs::create_dir_all(directory)
                .with_context(|| format!("failed to create {}", directory.display()))?;
            secure_file(directory, 0o700)?;
        }

        let bootstrap_path = bootstrap.join("marketlab_notebook.py");
        write_bootstrap(&bootstrap_path)?;
        secure_file(&bootstrap_path, 0o600)?;

        let ipython_config = ipython.join("ipython_config.py");
        fs::write(
            &ipython_config,
            "c.InteractiveShellApp.extensions = ['marketlab_notebook']\n",
        )
        .with_context(|| format!("failed to write {}", ipython_config.display()))?;
        secure_file(&ipython_config, 0o600)?;

        let socket_path = root.join(SOCKET_FILE);
        let python_path = joined_path(&bootstrap, env::var_os("PYTHONPATH").as_deref())?;
        let kernel_spec = json!({
            "argv": [
                runtime.interpreter.to_string_lossy(),
                "-m",
                "ipykernel_launcher",
                "-f",
                "{connection_file}",
            ],
            "display_name": format!("Market Lab ({})", runtime.version),
            "language": "python",
            "env": {
                "PYTHONPATH": python_path.to_string_lossy(),
                "IPYTHONDIR": root.join("ipython").to_string_lossy(),
                "MLAB_NOTEBOOK_SOCKET": socket_path.to_string_lossy(),
                "MLAB_NOTEBOOK_TOKEN": token,
            },
            "metadata": {
                "marketlab": {
                    "mode": "research",
                    "version": env!("CARGO_PKG_VERSION"),
                }
            }
        });
        let kernel_path = kernel.join("kernel.json");
        fs::write(&kernel_path, serde_json::to_vec_pretty(&kernel_spec)?)
            .with_context(|| format!("failed to write {}", kernel_path.display()))?;
        secure_file(&kernel_path, 0o600)?;

        let jupyter_path = joined_path(&jupyter, env::var_os("JUPYTER_PATH").as_deref())?;
        Ok(Self {
            root,
            socket_path,
            token,
            jupyter_path,
        })
    }
}

impl Drop for NotebookSession {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.root)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            eprintln!(
                "warning: failed to remove notebook session {}: {error}",
                self.root.display()
            );
        }
    }
}

fn joined_path(primary: &Path, existing: Option<&std::ffi::OsStr>) -> Result<OsString> {
    let mut paths = vec![primary.to_path_buf()];
    if let Some(existing) = existing {
        paths.extend(env::split_paths(existing));
    }
    env::join_paths(paths).context("failed to build notebook search path")
}

fn secure_file(path: &Path, mode: u32) -> Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(mode))
        .with_context(|| format!("failed to secure {}", path.display()))
}

fn random_hex(bytes: usize) -> String {
    let mut value = vec![0_u8; bytes];
    OsRng.fill_bytes(&mut value);
    hex::encode(value)
}

fn find_on_path(program: &str) -> Option<PathBuf> {
    let path = env::var_os("PATH")?;
    env::split_paths(&path)
        .map(|directory| directory.join(program))
        .find(|candidate| candidate.is_file())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn joins_notebook_paths_without_losing_existing_entries() {
        let separator = if cfg!(windows) { ";" } else { ":" };
        let existing = OsString::from(format!("/one{separator}/two"));
        let joined = joined_path(Path::new("/marketlab"), Some(&existing)).unwrap();
        let paths = env::split_paths(&joined).collect::<Vec<_>>();
        assert_eq!(paths[0], PathBuf::from("/marketlab"));
        assert_eq!(paths[1], PathBuf::from("/one"));
        assert_eq!(paths[2], PathBuf::from("/two"));
    }

    #[test]
    fn random_session_values_have_the_requested_entropy_length() {
        assert_eq!(random_hex(12).len(), 24);
        assert_eq!(random_hex(32).len(), 64);
    }
}
