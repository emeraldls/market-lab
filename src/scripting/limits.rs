use serde::Serialize;

pub const SCRIPT_HEAP_BYTES: usize = 32 * 1024 * 1024;
pub const SCRIPT_STACK_BYTES: usize = 1024 * 1024;
pub const SCRIPT_HOOK_TIMEOUT_MS: u64 = 250;
pub const SCRIPT_PYTHON_HOOK_TIMEOUT_MS: u64 = 5_000;
pub const SCRIPT_PYTHON_STARTUP_TIMEOUT_MS: u64 = 30_000;
pub const SCRIPT_PYTHON_FINISH_TIMEOUT_MS: u64 = 60_000;
pub const SCRIPT_PYTHON_MEMORY_BYTES: usize = 2 * 1024 * 1024 * 1024;
pub const SCRIPT_PYTHON_MAX_PROCESSES: usize = 32;
pub const SCRIPT_PYTHON_PROTOCOL_BYTES: usize = 8 * 1024 * 1024;
pub const SCRIPT_PYTHON_LOG_BYTES: usize = 64 * 1024 * 1024;
pub const SCRIPT_DEFAULT_LOOKBACK_CANDLES: usize = 5_000;
pub const SCRIPT_MAX_LOOKBACK_CANDLES: usize = 5_000;

#[derive(Debug, Clone, Copy, Serialize)]
pub struct ScriptRuntimeLimits {
    pub heap_bytes: usize,
    pub stack_bytes: usize,
    pub hook_timeout_ms: u64,
    pub startup_timeout_ms: u64,
    pub finish_timeout_ms: u64,
    pub process_memory_bytes: usize,
    pub max_processes: usize,
    pub protocol_message_bytes: usize,
    pub log_bytes: usize,
}

pub fn default_limits() -> ScriptRuntimeLimits {
    ScriptRuntimeLimits {
        heap_bytes: SCRIPT_HEAP_BYTES,
        stack_bytes: SCRIPT_STACK_BYTES,
        hook_timeout_ms: SCRIPT_HOOK_TIMEOUT_MS,
        startup_timeout_ms: 0,
        finish_timeout_ms: 0,
        process_memory_bytes: 0,
        max_processes: 0,
        protocol_message_bytes: 0,
        log_bytes: 0,
    }
}

pub fn python_limits() -> ScriptRuntimeLimits {
    ScriptRuntimeLimits {
        heap_bytes: 0,
        stack_bytes: 0,
        hook_timeout_ms: SCRIPT_PYTHON_HOOK_TIMEOUT_MS,
        startup_timeout_ms: SCRIPT_PYTHON_STARTUP_TIMEOUT_MS,
        finish_timeout_ms: SCRIPT_PYTHON_FINISH_TIMEOUT_MS,
        process_memory_bytes: SCRIPT_PYTHON_MEMORY_BYTES,
        max_processes: SCRIPT_PYTHON_MAX_PROCESSES,
        protocol_message_bytes: SCRIPT_PYTHON_PROTOCOL_BYTES,
        log_bytes: SCRIPT_PYTHON_LOG_BYTES,
    }
}
