use std::fmt;
use std::str::FromStr;

use anyhow::{Result, bail};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

const MAX_VENUE_ID_LEN: usize = 63;
const LEGACY_HIP3_PREFIX: &str = "hyperliquidf-";

/// Stable, serializable identity for an execution venue.
///
/// Venue IDs are deliberately data rather than enum variants. This keeps job
/// records and command plumbing independent from the set of registered venues.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct VenueId {
    bytes: [u8; MAX_VENUE_ID_LEN],
    len: u8,
}

impl VenueId {
    #[allow(non_upper_case_globals)]
    pub const Bulk: Self = Self::from_static("bulkf");
    #[allow(non_upper_case_globals)]
    pub const Hyperliquid: Self = Self::from_static("hyperliquidf");
    #[allow(non_upper_case_globals)]
    pub const Hyperlink: Self = Self::from_static("hyperlinkf");
    #[allow(non_upper_case_globals)]
    pub const HyperliquidSpot: Self = Self::from_static("hyperliquid");
    #[allow(non_upper_case_globals)]
    pub const HyperliquidOutcomes: Self = Self::from_static("hyperliquid-outcomes");

    const fn from_static(value: &str) -> Self {
        let source = value.as_bytes();
        assert!(!source.is_empty() && source.len() <= MAX_VENUE_ID_LEN);

        let mut bytes = [0; MAX_VENUE_ID_LEN];
        let mut index = 0;
        while index < source.len() {
            bytes[index] = source[index];
            index += 1;
        }
        Self {
            bytes,
            len: source.len() as u8,
        }
    }

    pub fn parse(value: &str) -> std::result::Result<Self, VenueIdError> {
        let normalized = value.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            return Err(VenueIdError::new("venue cannot be empty"));
        }
        if normalized.len() > MAX_VENUE_ID_LEN {
            return Err(VenueIdError::new(format!(
                "venue must be at most {MAX_VENUE_ID_LEN} bytes"
            )));
        }
        if !normalized
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            return Err(VenueIdError::new(
                "venue may contain only lowercase letters, digits, and hyphens",
            ));
        }

        let mut bytes = [0; MAX_VENUE_ID_LEN];
        bytes[..normalized.len()].copy_from_slice(normalized.as_bytes());
        let venue = Self {
            bytes,
            len: normalized.len() as u8,
        };
        resolve(venue).map_err(|error| VenueIdError::new(error.to_string()))?;
        Ok(venue)
    }

    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.bytes[..usize::from(self.len)])
            .expect("VenueId only contains validated ASCII")
    }

    pub fn spec(self) -> Result<VenueSpec> {
        resolve(self)
    }

    pub fn is_outcome(self) -> bool {
        self.spec()
            .is_ok_and(|spec| spec.market == VenueMarket::Outcome)
    }

    pub fn is_spot(self) -> bool {
        self.spec()
            .is_ok_and(|spec| spec.market == VenueMarket::Spot)
    }

    pub fn is_perpetual(self) -> bool {
        self.spec().is_ok_and(|spec| spec.market.is_perpetual())
    }

    pub fn is_hyperliquid(self) -> bool {
        self.spec()
            .is_ok_and(|spec| spec.execution == ExecutionBackend::Hyperliquid)
    }

    pub fn market_data_id(self) -> Self {
        self.registered().market_data_venue
    }

    pub fn execution_backend(self) -> ExecutionBackend {
        self.registered().execution
    }

    pub fn auth_backend(self) -> AuthBackend {
        self.registered().auth
    }

    pub fn market(self) -> VenueMarket {
        self.registered().market
    }

    pub fn label(self) -> String {
        self.registered().label()
    }

    pub fn network_label(self, testnet: bool) -> String {
        format!(
            "{} {}",
            self.label(),
            self.registered().network_label(testnet)
        )
    }

    fn registered(self) -> VenueSpec {
        resolve(self).expect("VenueId is validated when it is constructed")
    }
}

impl fmt::Debug for VenueId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("VenueId")
            .field(&self.as_str())
            .finish()
    }
}

impl fmt::Display for VenueId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for VenueId {
    type Err = VenueIdError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl Serialize for VenueId {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for VenueId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).map_err(de::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VenueIdError(String);

impl VenueIdError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for VenueIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for VenueIdError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionBackend {
    Bulk,
    Hyperliquid,
    Hyperlink,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthBackend {
    Bulk,
    Hyperliquid,
    Hyperlink,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VenueMarket {
    Perpetual,
    Spot,
    Outcome,
}

impl VenueMarket {
    pub const fn is_perpetual(self) -> bool {
        matches!(self, Self::Perpetual)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NetworkPolicy {
    /// The venue is always the public BULK testnet; `--testnet` is unnecessary.
    TestnetOnly,
    /// The venue exposes only mainnet.
    MainnetOnly,
    /// The venue supports both networks and obeys `--testnet`.
    Selectable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VenueSpec {
    pub id: VenueId,
    pub execution: ExecutionBackend,
    pub auth: AuthBackend,
    pub market: VenueMarket,
    pub network: NetworkPolicy,
    /// Market-data identity used when execution and price discovery differ.
    pub market_data_venue: VenueId,
}

impl VenueSpec {
    pub fn label(self) -> String {
        match self.id {
            VenueId::Bulk => "BULK".to_string(),
            VenueId::Hyperliquid => "Hyperliquid".to_string(),
            VenueId::Hyperlink => "HyperLink".to_string(),
            VenueId::HyperliquidSpot => "Hyperliquid Spot".to_string(),
            VenueId::HyperliquidOutcomes => "Hyperliquid Outcomes".to_string(),
            _ => self.id.to_string(),
        }
    }

    pub fn network_label(self, testnet: bool) -> &'static str {
        match self.network {
            NetworkPolicy::TestnetOnly => "testnet",
            NetworkPolicy::MainnetOnly => "mainnet",
            NetworkPolicy::Selectable if testnet => "testnet",
            NetworkPolicy::Selectable => "mainnet",
        }
    }

    pub fn validate_network(self, testnet: bool) -> Result<()> {
        match (self.network, testnet) {
            (NetworkPolicy::MainnetOnly, true) => {
                bail!("--testnet is not supported by {}", self.label())
            }
            (NetworkPolicy::TestnetOnly, true) => {
                bail!("--testnet is only valid with a Hyperliquid venue")
            }
            _ => Ok(()),
        }
    }
}

const BULK: VenueSpec = VenueSpec {
    id: VenueId::Bulk,
    execution: ExecutionBackend::Bulk,
    auth: AuthBackend::Bulk,
    market: VenueMarket::Perpetual,
    network: NetworkPolicy::TestnetOnly,
    market_data_venue: VenueId::Bulk,
};

const HYPERLIQUID: VenueSpec = VenueSpec {
    id: VenueId::Hyperliquid,
    execution: ExecutionBackend::Hyperliquid,
    auth: AuthBackend::Hyperliquid,
    market: VenueMarket::Perpetual,
    network: NetworkPolicy::Selectable,
    market_data_venue: VenueId::Hyperliquid,
};

const HYPERLINK: VenueSpec = VenueSpec {
    id: VenueId::Hyperlink,
    execution: ExecutionBackend::Hyperlink,
    auth: AuthBackend::Hyperlink,
    market: VenueMarket::Perpetual,
    network: NetworkPolicy::MainnetOnly,
    market_data_venue: VenueId::Hyperliquid,
};

const HYPERLIQUID_SPOT: VenueSpec = VenueSpec {
    id: VenueId::HyperliquidSpot,
    execution: ExecutionBackend::Hyperliquid,
    auth: AuthBackend::Hyperliquid,
    market: VenueMarket::Spot,
    network: NetworkPolicy::Selectable,
    market_data_venue: VenueId::HyperliquidSpot,
};

const HYPERLIQUID_OUTCOMES: VenueSpec = VenueSpec {
    id: VenueId::HyperliquidOutcomes,
    execution: ExecutionBackend::Hyperliquid,
    auth: AuthBackend::Hyperliquid,
    market: VenueMarket::Outcome,
    network: NetworkPolicy::Selectable,
    market_data_venue: VenueId::HyperliquidOutcomes,
};

pub const BUILTIN_VENUES: &[VenueSpec] = &[
    BULK,
    HYPERLIQUID,
    HYPERLINK,
    HYPERLIQUID_SPOT,
    HYPERLIQUID_OUTCOMES,
];

pub fn resolve(venue: VenueId) -> Result<VenueSpec> {
    if let Some(spec) = BUILTIN_VENUES.iter().find(|spec| spec.id == venue) {
        return Ok(*spec);
    }

    if let Some(dex) = venue.as_str().strip_prefix(LEGACY_HIP3_PREFIX) {
        let example = if dex.is_empty() {
            "xyz:TSLA".to_string()
        } else {
            format!("{dex}:COIN")
        };
        bail!(
            "venue `{venue}` was removed; use venue `hyperliquidf` with a DEX-qualified symbol such as `{example}`"
        );
    }

    bail!("unsupported execution venue `{venue}`")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_execution_and_market_data_as_separate_concerns() {
        let spec = resolve(VenueId::Hyperlink).expect("HyperLink is registered");
        assert_eq!(spec.execution, ExecutionBackend::Hyperlink);
        assert_eq!(spec.market_data_venue, VenueId::Hyperliquid);
    }

    #[test]
    fn legacy_hip3_venues_explain_the_symbol_migration() {
        let error = VenueId::parse("hyperliquidf-xyz").expect_err("legacy venue must fail");
        assert!(error.to_string().contains("xyz:COIN"));
        assert!(error.to_string().contains("venue `hyperliquidf`"));
    }
}
