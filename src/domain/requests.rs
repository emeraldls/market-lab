use super::enums::{BookMode, ProviderKind, Side};

#[derive(Clone, Debug)]
pub struct InspectRequest {
    pub provider: ProviderKind,
    pub exchange: String,
    pub symbol: String,
    pub at: u64,
    pub depth: u16,
    pub book_mode: BookMode,
}

#[derive(Clone, Debug)]
pub struct ReplayRequest {
    pub provider: ProviderKind,
    pub exchange: String,
    pub symbol: String,
    pub from: u64,
    pub to: u64,
    pub speed: u32,
}

#[derive(Clone, Debug)]
pub struct SlippageRequest {
    pub provider: ProviderKind,
    pub exchange: String,
    pub symbol: String,
    pub side: Side,
    pub notional: f64,
    pub depth: u16,
    pub book_mode: BookMode,
    pub stream: bool,
    pub buffer_size: u16,
}

#[derive(Clone, Debug)]
pub struct ImbalanceRequest {
    pub provider: ProviderKind,
    pub exchange: String,
    pub symbol: String,
    pub depth: u16,
    pub book_mode: BookMode,
    pub stream: bool,
    pub buffer_size: u16,
}

#[derive(Clone, Debug)]
pub struct VampRequest {
    pub provider: ProviderKind,
    pub exchange: String,
    pub symbol: String,
    pub depth: u16,
    pub dollar_depth: f64,
    pub book_mode: BookMode,
    pub stream: bool,
    pub buffer_size: u16,
}

#[derive(Clone, Debug)]
pub struct SpreadRequest {
    pub provider: ProviderKind,
    pub exchange: String,
    pub symbol: String,
    pub depth: u16,
    pub book_mode: BookMode,
    pub stream: bool,
    pub buffer_size: u16,
}

#[derive(Clone, Debug)]
pub struct DepthRequest {
    pub provider: ProviderKind,
    pub exchange: String,
    pub symbol: String,
    pub levels: u16,
    pub book_mode: BookMode,
    pub stream: bool,
    pub buffer_size: u16,
}
