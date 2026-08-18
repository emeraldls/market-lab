use std::collections::BTreeMap;
use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use sha3::{Digest, Keccak256};

use super::language::{ManagedPythonRuntime, PythonRuntime};

const SNAPSHOT_SCHEMA: &str = "marketlab-python-runtime-v1";
const MAX_REQUIREMENTS_BYTES: usize = 96 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PythonEnvironmentSnapshot {
    pub python_request: String,
    pub requirements: String,
    pub managed: ManagedPythonRuntime,
}

impl PythonEnvironmentSnapshot {
    pub fn capture(runtime: &PythonRuntime) -> Result<Self> {
        let python_request = python_minor(&runtime.version)?;
        let requirements = match find_on_path("uv") {
            Some(uv) => capture_with_uv(&uv, &runtime.interpreter).or_else(|uv_error| {
                capture_with_python(&runtime.interpreter).with_context(|| {
                    format!(
                        "uv could not inspect {}; the Python metadata fallback also failed: {uv_error:#}",
                        runtime.interpreter.display()
                    )
                })
            })?,
            None => capture_with_python(&runtime.interpreter)?,
        };
        let requirements = normalize_requirements(&requirements)?;
        if requirements.len() > MAX_REQUIREMENTS_BYTES {
            bail!(
                "the selected Python environment produces a {} byte dependency snapshot; the limit is {} bytes",
                requirements.len(),
                MAX_REQUIREMENTS_BYTES
            );
        }
        let package_count = requirements.lines().count();
        let fingerprint = runtime_fingerprint(&python_request, &requirements);
        Ok(Self {
            python_request,
            requirements,
            managed: ManagedPythonRuntime {
                fingerprint,
                package_count,
            },
        })
    }
}

fn runtime_fingerprint(python_request: &str, requirements: &str) -> String {
    let mut digest = Keccak256::new();
    digest.update(SNAPSHOT_SCHEMA.as_bytes());
    digest.update([0]);
    digest.update(env!("CARGO_PKG_VERSION").as_bytes());
    digest.update([0]);
    digest.update(std::env::consts::ARCH.as_bytes());
    digest.update([0]);
    digest.update(python_request.as_bytes());
    digest.update([0]);
    digest.update(requirements.as_bytes());
    hex::encode(digest.finalize())
}

fn capture_with_uv(uv: &Path, interpreter: &Path) -> Result<String> {
    let output = Command::new(uv)
        .args(["pip", "freeze", "--python"])
        .arg(interpreter)
        .output()
        .with_context(|| format!("failed to start uv at {}", uv.display()))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        bail!(
            "uv failed to inspect {}{}",
            interpreter.display(),
            if stderr.is_empty() {
                String::new()
            } else {
                format!(": {stderr}")
            }
        );
    }
    String::from_utf8(output.stdout).context("uv returned non-UTF-8 dependency data")
}

#[derive(Deserialize)]
struct InstalledDistribution {
    name: String,
    version: String,
    direct: bool,
}

fn capture_with_python(interpreter: &Path) -> Result<String> {
    const INSPECT: &str = r#"
import importlib.metadata as metadata
import json

rows = []
for distribution in metadata.distributions():
    name = distribution.metadata.get("Name")
    if not name:
        continue
    direct = distribution.read_text("direct_url.json") is not None
    rows.append({"name": name, "version": distribution.version, "direct": direct})
print(json.dumps(rows, separators=(",", ":")))
"#;
    let output = Command::new(interpreter)
        .args(["-c", INSPECT])
        .output()
        .with_context(|| format!("failed to inspect packages with {}", interpreter.display()))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        bail!(
            "Python package inspection failed for {}{}",
            interpreter.display(),
            if stderr.is_empty() {
                String::new()
            } else {
                format!(": {stderr}")
            }
        );
    }
    let distributions: Vec<InstalledDistribution> = serde_json::from_slice(&output.stdout)
        .context("Python returned malformed package metadata")?;
    let mut requirements = String::new();
    for distribution in distributions {
        if distribution.direct {
            bail!(
                "Python dependency `{}` is installed from a local, editable, or direct URL source; managed Docker runtimes currently require packages published to an index",
                distribution.name
            );
        }
        requirements.push_str(&distribution.name);
        requirements.push_str("==");
        requirements.push_str(&distribution.version);
        requirements.push('\n');
    }
    Ok(requirements)
}

fn normalize_requirements(source: &str) -> Result<String> {
    let mut packages = BTreeMap::<String, String>::new();
    for raw in source.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with("-e ")
            || line.starts_with("--editable")
            || line.contains(" @ ")
            || line.starts_with("file:")
        {
            bail!(
                "Python dependency `{line}` is local, editable, or URL-based; managed Docker runtimes currently require index packages pinned as name==version"
            );
        }
        let (name, version) = line.split_once("==").with_context(|| {
            format!(
                "Python dependency `{line}` is not reproducibly pinned; managed Docker runtimes require name==version"
            )
        })?;
        let normalized = normalize_package_name(name)?;
        if matches!(normalized.as_str(), "pip" | "setuptools" | "wheel") {
            continue;
        }
        if version.is_empty()
            || version.chars().any(char::is_whitespace)
            || version.chars().any(char::is_control)
        {
            bail!("Python dependency `{line}` has an invalid version");
        }
        let requirement = format!("{}=={version}", name.trim());
        if let Some(previous) = packages.insert(normalized, requirement.clone())
            && previous != requirement
        {
            bail!(
                "Python environment contains conflicting requirements `{previous}` and `{requirement}`"
            );
        }
    }
    let mut output = packages.into_values().collect::<Vec<_>>().join("\n");
    if !output.is_empty() {
        output.push('\n');
    }
    Ok(output)
}

fn normalize_package_name(name: &str) -> Result<String> {
    let name = name.trim();
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        bail!("Python dependency name `{name}` is invalid");
    }
    Ok(name
        .chars()
        .map(|character| match character {
            '.' | '_' => '-',
            other => other.to_ascii_lowercase(),
        })
        .collect())
}

fn python_minor(version: &str) -> Result<String> {
    let mut parts = version.split('.');
    let major = parts
        .next()
        .context("Python version is missing its major component")?;
    let minor = parts
        .next()
        .context("Python version is missing its minor component")?;
    if major != "3" || minor.parse::<u32>().is_err() {
        bail!("unsupported Python version `{version}`");
    }
    Ok(format!("{major}.{minor}"))
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
    fn requirements_are_sorted_normalized_and_drop_bootstrap_tools() {
        let requirements =
            normalize_requirements("pandas==2.3.1\nNumPy==2.2.6\npip==25.1\nsetuptools==80.0\n")
                .unwrap();
        assert_eq!(requirements, "NumPy==2.2.6\npandas==2.3.1\n");
    }

    #[test]
    fn nonportable_requirements_are_rejected() {
        let error = normalize_requirements("local-lib @ file:///tmp/local-lib\n").unwrap_err();
        assert!(format!("{error:#}").contains("local, editable, or URL-based"));
    }

    #[test]
    fn python_minor_ignores_patch_version() {
        assert_eq!(python_minor("3.12.8").unwrap(), "3.12");
    }

    #[test]
    fn runtime_fingerprint_changes_only_with_runtime_inputs() {
        let first = runtime_fingerprint("3.12", "numpy==2.2.6\n");
        assert_eq!(first, runtime_fingerprint("3.12", "numpy==2.2.6\n"));
        assert_ne!(first, runtime_fingerprint("3.11", "numpy==2.2.6\n"));
        assert_ne!(first, runtime_fingerprint("3.12", "numpy==2.3.0\n"));
    }
}
