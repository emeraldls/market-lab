use serde::Serialize;

pub const SCRIPT_HEAP_BYTES: usize = 32 * 1024 * 1024;
pub const SCRIPT_STACK_BYTES: usize = 1024 * 1024;
pub const SCRIPT_HOOK_TIMEOUT_MS: u64 = 250;
pub const SCRIPT_DEFAULT_LOOKBACK_CANDLES: usize = 5_000;
pub const SCRIPT_MAX_LOOKBACK_CANDLES: usize = 5_000;

#[derive(Debug, Clone, Copy, Serialize)]
pub struct ScriptRuntimeLimits {
    pub heap_bytes: usize,
    pub stack_bytes: usize,
    pub hook_timeout_ms: u64,
}

pub fn default_limits() -> ScriptRuntimeLimits {
    ScriptRuntimeLimits {
        heap_bytes: SCRIPT_HEAP_BYTES,
        stack_bytes: SCRIPT_STACK_BYTES,
        hook_timeout_ms: SCRIPT_HOOK_TIMEOUT_MS,
    }
}
