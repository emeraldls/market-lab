use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum ProviderKind {
    MarketLab,
    Mmt,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum BookMode {
    Binned,
    Raw,
}
