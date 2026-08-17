use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScriptLanguage {
    #[default]
    JavaScriptV1,
    PythonV2,
}

impl ScriptLanguage {
    pub fn from_path(path: &Path) -> Result<Self> {
        match path.extension().and_then(|extension| extension.to_str()) {
            Some("js" | "mjs") => Ok(Self::JavaScriptV1),
            Some("py") => Ok(Self::PythonV2),
            Some(extension) => {
                bail!("unsupported script extension `.{extension}`; expected .js, .mjs, or .py")
            }
            None => bail!("script path must end in .js, .mjs, or .py"),
        }
    }

    pub const fn manifest_version(self) -> &'static str {
        match self {
            Self::JavaScriptV1 => "1",
            Self::PythonV2 => "2",
        }
    }

    pub const fn snapshot_file_name(self) -> &'static str {
        match self {
            Self::JavaScriptV1 => "strategy.js",
            Self::PythonV2 => "strategy.py",
        }
    }

    pub const fn engine_name(self) -> &'static str {
        match self {
            Self::JavaScriptV1 => "quickjs",
            Self::PythonV2 => "python",
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PythonRuntime {
    pub interpreter: PathBuf,
    pub version: String,
}

impl PythonRuntime {
    pub fn resolve(script_path: &Path, requested: Option<&Path>) -> Result<Self> {
        let interpreter = if let Some(requested) = requested {
            resolve_requested_interpreter(requested)?
        } else if let Some(project_python) = adjacent_virtualenv_python(script_path) {
            project_python
        } else {
            find_on_path("python3")
                .or_else(|| find_on_path("python"))
                .context(
                    "Python scripting requires Python 3; pass --python or create .venv beside the script",
                )?
        };
        Self::inspect(interpreter)
    }

    pub fn inspect(interpreter: PathBuf) -> Result<Self> {
        let interpreter = if interpreter.is_absolute() {
            interpreter
        } else {
            env::current_dir()
                .context("failed to resolve the current directory for the Python interpreter")?
                .join(interpreter)
        };
        let output = Command::new(&interpreter)
            .args([
                "-c",
                "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}')",
            ])
            .output()
            .with_context(|| format!("failed to start Python at {}", interpreter.display()))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            bail!(
                "Python interpreter {} failed its startup check{}",
                interpreter.display(),
                if stderr.is_empty() {
                    String::new()
                } else {
                    format!(": {stderr}")
                }
            );
        }
        let version = String::from_utf8(output.stdout)
            .context("Python returned a non-UTF-8 version")?
            .trim()
            .to_string();
        let mut parts = version.split('.');
        let major = parts.next().and_then(|part| part.parse::<u32>().ok());
        let minor = parts.next().and_then(|part| part.parse::<u32>().ok());
        if major != Some(3) || minor.is_none_or(|minor| minor < 9) {
            bail!("Python scripting requires Python 3.9 or newer; found {version}");
        }
        Ok(Self {
            interpreter,
            version,
        })
    }
}

fn resolve_requested_interpreter(requested: &Path) -> Result<PathBuf> {
    if requested.components().count() > 1 || requested.is_absolute() {
        if requested.is_file() {
            return Ok(requested.to_path_buf());
        }
        bail!("Python interpreter {} does not exist", requested.display());
    }
    find_on_path(requested.to_string_lossy().as_ref())
        .with_context(|| format!("Python interpreter `{}` was not found", requested.display()))
}

fn adjacent_virtualenv_python(script_path: &Path) -> Option<PathBuf> {
    let directory = script_path.parent().unwrap_or_else(|| Path::new("."));
    [
        directory.join(".venv").join("bin").join("python"),
        directory.join(".venv").join("Scripts").join("python.exe"),
    ]
    .into_iter()
    .find(|candidate| candidate.is_file())
}

fn find_on_path(program: &str) -> Option<PathBuf> {
    let path = env::var_os("PATH")?;
    env::split_paths(&path)
        .flat_map(|directory| {
            #[cfg(windows)]
            {
                vec![
                    directory.join(program),
                    directory.join(format!("{program}.exe")),
                ]
            }
            #[cfg(not(windows))]
            {
                vec![directory.join(program)]
            }
        })
        .find(|candidate| candidate.is_file())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn language_is_selected_by_extension() {
        assert_eq!(
            ScriptLanguage::from_path(Path::new("alpha.js")).unwrap(),
            ScriptLanguage::JavaScriptV1
        );
        assert_eq!(
            ScriptLanguage::from_path(Path::new("alpha.py")).unwrap(),
            ScriptLanguage::PythonV2
        );
        assert!(ScriptLanguage::from_path(Path::new("alpha.txt")).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn python_runtime_preserves_virtual_environment_symlink() {
        use std::fs;
        use std::os::unix::fs::{PermissionsExt, symlink};

        let directory = std::env::temp_dir().join(format!(
            "mlab-python-runtime-symlink-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&directory);
        fs::create_dir_all(&directory).unwrap();
        let interpreter = directory.join("fake-python");
        fs::write(&interpreter, "#!/bin/sh\nprintf '3.11.9\\n'\n").unwrap();
        fs::set_permissions(&interpreter, fs::Permissions::from_mode(0o700)).unwrap();

        let virtualenv_bin = directory.join(".venv/bin");
        fs::create_dir_all(&virtualenv_bin).unwrap();
        let virtualenv_python = virtualenv_bin.join("python");
        symlink("../../fake-python", &virtualenv_python).unwrap();

        let runtime = PythonRuntime::inspect(virtualenv_python.clone()).unwrap();
        assert_eq!(runtime.interpreter, virtualenv_python);
        assert_eq!(runtime.version, "3.11.9");
        fs::remove_dir_all(directory).unwrap();
    }
}
