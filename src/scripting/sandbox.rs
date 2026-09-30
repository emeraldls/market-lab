//! Cloud restrictions are applied by the OS before any user Python is imported.
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Result, bail};

pub const POLICY_ENV: &str = "MLAB_PYTHON_SANDBOX";
pub const INTERPRETER: &str = "/usr/bin/python3";
pub const WORKER_COMMAND: &str = "python-sandbox-worker";
// NumPy/SciPy map native numerical libraries in addition to Python's heap.
pub const MEMORY_BYTES: usize = 512 * 1024 * 1024;

pub fn required() -> Result<bool> {
    match std::env::var(POLICY_ENV) {
        Err(std::env::VarError::NotPresent) => Ok(false),
        Ok(value) if value == "required" => Ok(true),
        _ => bail!("{POLICY_ENV} must be `required` or unset"),
    }
}

pub fn require_linux() -> Result<()> {
    if !cfg!(target_os = "linux") {
        bail!("Cloud Python sandbox requires Linux; it never runs unrestricted");
    }
    Ok(())
}

pub fn validate_interpreter(path: &Path) -> Result<()> {
    require_linux()?;
    if path != Path::new(INTERPRETER) {
        bail!("Cloud scripts use the image's {INTERPRETER}; custom interpreters are not allowed");
    }
    Ok(())
}

pub fn command(script: &Path, mode: &str, workspace: &Path) -> Result<Command> {
    require_linux()?;
    let mut command = Command::new(std::env::current_exe()?.with_file_name("mlabd"));
    command
        .arg(WORKER_COMMAND)
        .arg(script.canonicalize()?)
        .arg(mode)
        .arg(workspace.canonicalize()?)
        .env_clear();
    Ok(command)
}

// This entry point runs before Tokio starts. Only trusted Rust executes before restriction.
pub fn worker(args: impl Iterator<Item = std::ffi::OsString>) -> Result<()> {
    let args = args.collect::<Vec<_>>();
    if args.len() != 3 {
        bail!("sandbox worker requires script, mode and workspace");
    }
    require_linux()?;
    #[cfg(target_os = "linux")]
    return linux::run(Path::new(&args[0]), &args[1], Path::new(&args[2]));
    #[cfg(not(target_os = "linux"))]
    unreachable!()
}

/// Private scratch space. Cloud containers mount /tmp as a size-limited tmpfs.
pub struct Workspace(pub PathBuf);

impl Workspace {
    pub fn new() -> Result<Self> {
        require_linux()?;
        #[cfg(target_os = "linux")]
        linux::check_tmpfs()?;
        use rand_core::{OsRng, RngCore};
        use std::os::unix::fs::DirBuilderExt;
        let mut nonce = [0u8; 16];
        OsRng.fill_bytes(&mut nonce);
        let path = PathBuf::from("/tmp").join(format!("mlab-python-{}", hex::encode(nonce)));
        std::fs::DirBuilder::new().mode(0o700).create(&path)?;
        Ok(Self(path))
    }

    pub fn export(&self, destination: &Path) -> Result<()> {
        // No symlinks or special files are exported from untrusted code.
        fn copy(from: &Path, to: &Path, bytes: &mut u64, entries: &mut usize) -> Result<()> {
            for entry in std::fs::read_dir(from)? {
                let entry = entry?;
                *entries += 1;
                if *entries > 1024 {
                    bail!("Python artifacts exceed 1024 entries");
                }
                let metadata = entry.path().symlink_metadata()?;
                if metadata.is_symlink() {
                    bail!("Python artifacts cannot contain symlinks");
                }
                let target = to.join(entry.file_name());
                if metadata.is_dir() {
                    std::fs::create_dir_all(&target)?;
                    copy(&entry.path(), &target, bytes, entries)?;
                } else if metadata.is_file() {
                    *bytes = bytes.saturating_add(metadata.len());
                    if *bytes > 64 * 1024 * 1024 {
                        bail!("Python artifacts exceed 64 MiB");
                    }
                    std::fs::copy(entry.path(), target)?;
                } else {
                    bail!("Python artifacts must be regular files");
                }
            }
            Ok(())
        }
        let artifacts = self.0.join("artifacts");
        if artifacts.exists() {
            copy(&artifacts, destination, &mut 0, &mut 0)?;
        }
        Ok(())
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use anyhow::Context;
    use landlock::{
        ABI, Access, AccessFs, CompatLevel, Compatible, PathBeneath, PathFd, Ruleset, RulesetAttr,
        RulesetCreatedAttr, RulesetStatus, Scope,
    };
    use seccompiler::{
        BpfProgram, SeccompAction, SeccompCmpArgLen, SeccompCmpOp, SeccompCondition, SeccompFilter,
        SeccompRule,
    };
    use std::collections::BTreeMap;
    use std::os::unix::process::CommandExt;

    pub(super) fn check_tmpfs() -> Result<()> {
        let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
        if unsafe { libc::statfs(c"/tmp".as_ptr(), stat.as_mut_ptr()) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let stat = unsafe { stat.assume_init() };
        if stat.f_type != libc::TMPFS_MAGIC
            || (stat.f_blocks as u128) * (stat.f_bsize as u128) > 256 * 1024 * 1024
        {
            bail!("Cloud Python requires /tmp on a tmpfs limited to at most 256 MiB");
        }
        Ok(())
    }

    pub(super) fn run(script: &Path, mode: &std::ffi::OsStr, workspace: &Path) -> Result<()> {
        check_tmpfs()?;
        let script = script.canonicalize()?;
        let workspace = workspace.canonicalize()?;
        if workspace.parent() != Some(Path::new("/tmp"))
            || !workspace
                .file_name()
                .is_some_and(|s| s.to_string_lossy().starts_with("mlab-python-"))
        {
            bail!("invalid Python sandbox workspace");
        }
        let mut command = Command::new(INTERPRETER);
        command
            .args(["-I", "-B", "-u", "-c", super::super::python::PYTHON_RUNNER])
            .arg(&script)
            .arg(mode)
            .env_clear()
            .current_dir(&workspace)
            .env("HOME", &workspace)
            .env("TMPDIR", &workspace)
            .env("MPLCONFIGDIR", workspace.join("matplotlib"))
            .env("MPLBACKEND", "Agg")
            .env("OPENBLAS_NUM_THREADS", "1")
            .env("OMP_NUM_THREADS", "1")
            .env("JOBLIB_MULTIPROCESSING", "0")
            .env("MKL_NUM_THREADS", "1");

        // ABI 6 also isolates signals and abstract Unix sockets from the daemon.
        let abi = ABI::V6;
        let read = AccessFs::from_read(abi);
        let mut ruleset = Ruleset::default()
            .set_compatibility(CompatLevel::HardRequirement)
            .handle_access(AccessFs::from_all(abi))?
            .scope(Scope::AbstractUnixSocket | Scope::Signal)?
            .create()
            .context(
                "Linux kernel must support Landlock ABI 6 (Linux 6.12+) with Landlock enabled",
            )?;
        for path in ["/usr/lib", "/usr/local/lib", "/lib", "/lib64"] {
            if Path::new(path).exists() {
                ruleset = ruleset.add_rule(PathBeneath::new(PathFd::new(path)?, read))?;
            }
        }
        for path in [PathBuf::from(INTERPRETER).canonicalize()?, script] {
            ruleset = ruleset.add_rule(PathBeneath::new(
                PathFd::new(path)?,
                AccessFs::ReadFile | AccessFs::Execute,
            ))?;
        }
        // Debian's standard sitecustomize is a symlink outside /usr/lib.
        let sitecustomize = Path::new("/usr/lib/python3.11/sitecustomize.py");
        if sitecustomize.exists() {
            ruleset = ruleset.add_rule(PathBeneath::new(
                PathFd::new(sitecustomize.canonicalize()?)?,
                AccessFs::ReadFile,
            ))?;
        }
        if Path::new("/etc/ld.so.cache").exists() {
            ruleset = ruleset.add_rule(PathBeneath::new(
                PathFd::new("/etc/ld.so.cache")?,
                AccessFs::ReadFile,
            ))?;
        }
        ruleset = ruleset
            .add_rule(PathBeneath::new(
                PathFd::new("/dev/urandom")?,
                AccessFs::ReadFile,
            ))?
            .add_rule(PathBeneath::new(
                PathFd::new("/dev/null")?,
                AccessFs::ReadFile | AccessFs::WriteFile,
            ))?
            .add_rule(PathBeneath::new(
                PathFd::new(&workspace)?,
                AccessFs::ReadFile
                    | AccessFs::ReadDir
                    | AccessFs::WriteFile
                    | AccessFs::MakeReg
                    | AccessFs::MakeDir
                    | AccessFs::RemoveFile
                    | AccessFs::RemoveDir
                    | AccessFs::Truncate,
            ))?;
        let status = ruleset.restrict_self()?;
        if status.ruleset != RulesetStatus::FullyEnforced || !status.no_new_privs {
            bail!("Python sandbox was not fully enforced");
        }
        for (resource, value) in [
            (libc::RLIMIT_AS, MEMORY_BYTES as u64),
            (libc::RLIMIT_FSIZE, 16 * 1024 * 1024),
            (libc::RLIMIT_NOFILE, 64),
            (libc::RLIMIT_CORE, 0),
        ] {
            let limit = libc::rlimit {
                rlim_cur: value,
                rlim_max: value,
            };
            if unsafe { libc::setrlimit(resource, &limit) } != 0 {
                return Err(std::io::Error::last_os_error().into());
            }
        }
        // Do not expose parent descriptors, even if a caller forgot CLOEXEC.
        if unsafe {
            libc::syscall(
                libc::SYS_close_range,
                3u32,
                u32::MAX,
                libc::CLOSE_RANGE_CLOEXEC,
            )
        } != 0
        {
            return Err(std::io::Error::last_os_error().into());
        }
        // Deliberately single-threaded initially: no clone/fork, networking, ptrace,
        // process signalling, namespace changes, io_uring, or mount operations.
        let mut calls = vec![
            libc::SYS_read,
            libc::SYS_write,
            libc::SYS_readv,
            libc::SYS_writev,
            libc::SYS_close,
            libc::SYS_close_range,
            libc::SYS_lseek,
            libc::SYS_pread64,
            libc::SYS_pwrite64,
            libc::SYS_openat,
            libc::SYS_fstat,
            libc::SYS_statfs,
            libc::SYS_fstatfs,
            libc::SYS_statx,
            libc::SYS_getdents64,
            libc::SYS_readlinkat,
            libc::SYS_faccessat,
            libc::SYS_faccessat2,
            libc::SYS_fcntl,
            libc::SYS_mmap,
            libc::SYS_mprotect,
            libc::SYS_munmap,
            libc::SYS_mremap,
            libc::SYS_madvise,
            libc::SYS_brk,
            libc::SYS_rt_sigaction,
            libc::SYS_rt_sigprocmask,
            libc::SYS_rt_sigreturn,
            libc::SYS_sigaltstack,
            libc::SYS_futex,
            libc::SYS_set_tid_address,
            libc::SYS_set_robust_list,
            libc::SYS_rseq,
            libc::SYS_sched_yield,
            libc::SYS_sched_getaffinity,
            libc::SYS_clock_gettime,
            libc::SYS_clock_getres,
            libc::SYS_clock_nanosleep,
            libc::SYS_nanosleep,
            libc::SYS_gettimeofday,
            libc::SYS_getpid,
            libc::SYS_getppid,
            libc::SYS_gettid,
            libc::SYS_getuid,
            libc::SYS_geteuid,
            libc::SYS_getgid,
            libc::SYS_getegid,
            libc::SYS_getrandom,
            libc::SYS_uname,
            libc::SYS_prlimit64,
            libc::SYS_getrusage,
            libc::SYS_sysinfo,
            libc::SYS_getcwd,
            libc::SYS_chdir,
            libc::SYS_mkdirat,
            libc::SYS_unlinkat,
            libc::SYS_renameat,
            libc::SYS_renameat2,
            libc::SYS_ftruncate,
            libc::SYS_fsync,
            libc::SYS_fdatasync,
            libc::SYS_umask,
            libc::SYS_dup,
            libc::SYS_dup3,
            libc::SYS_ppoll,
            libc::SYS_pselect6,
            libc::SYS_execve,
            libc::SYS_exit,
            libc::SYS_exit_group,
        ];
        #[cfg(target_arch = "x86_64")]
        calls.extend([
            libc::SYS_arch_prctl,
            libc::SYS_open,
            libc::SYS_stat,
            libc::SYS_lstat,
            libc::SYS_newfstatat,
            libc::SYS_access,
            libc::SYS_readlink,
            libc::SYS_mkdir,
            libc::SYS_unlink,
            libc::SYS_rename,
            libc::SYS_dup2,
            libc::SYS_poll,
            libc::SYS_select,
            libc::SYS_time,
        ]);
        #[cfg(target_arch = "aarch64")]
        calls.push(libc::SYS_newfstatat);
        let mut rules = calls
            .into_iter()
            .map(|call| (call, vec![]))
            .collect::<BTreeMap<_, _>>();
        // prlimit must never change the daemon's limits through a sibling PID.
        rules.insert(
            libc::SYS_prlimit64,
            vec![SeccompRule::new(vec![SeccompCondition::new(
                0,
                SeccompCmpArgLen::Qword,
                SeccompCmpOp::Eq,
                0,
            )?])?],
        );
        let filter: BpfProgram = SeccompFilter::new(
            rules,
            SeccompAction::Errno(libc::EPERM as u32),
            SeccompAction::Allow,
            std::env::consts::ARCH.try_into()?,
        )?
        .try_into()?;
        seccompiler::apply_filter(&filter)?;
        Err(command.exec()).context("failed to exec sandboxed Python")
    }
}
