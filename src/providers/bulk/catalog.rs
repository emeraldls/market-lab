use std::collections::HashSet;
use std::sync::OnceLock;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

const CATALOG_SCHEMA_VERSION: u8 = 2;
const CATALOG_JSON: &str = include_str!("markets.json");

static CATALOG: OnceLock<BulkMarketCatalog> = OnceLock::new();

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BulkMarketCatalog {
    pub schema_version: u8,
    pub provider: String,
    pub source_url: String,
    pub fetched_at: String,
    pub markets: Vec<BulkMarket>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BulkMarket {
    pub symbol: String,
    pub internal_symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub status: String,
    pub price_precision: u8,
    pub size_precision: u8,
    pub tick_size: f64,
    pub lot_size: f64,
    pub min_notional: f64,
    pub max_leverage: u16,
    pub order_types: Vec<String>,
    pub time_in_forces: Vec<String>,
}

impl BulkMarketCatalog {
    pub fn find(&self, symbol: &str) -> Result<Option<&BulkMarket>> {
        let (base, quote) = symbol_parts(symbol)?;
        let venue_candidate = format!("{base}-{quote}");
        let internal_candidate = format!("{base}/{quote}");
        Ok(self.markets.iter().find(|market| {
            market.symbol == venue_candidate || market.internal_symbol == internal_candidate
        }))
    }

    fn validate(&self) -> Result<()> {
        if self.schema_version != CATALOG_SCHEMA_VERSION {
            bail!(
                "unsupported BULK market catalog schema version {}",
                self.schema_version
            );
        }
        if self.provider != "bulk" {
            bail!(
                "BULK market catalog has unexpected provider `{}`",
                self.provider
            );
        }
        if self.source_url.trim().is_empty() || self.fetched_at.trim().is_empty() {
            bail!("BULK market catalog is missing snapshot provenance");
        }
        if self.markets.is_empty() {
            bail!("BULK market catalog contains no markets");
        }

        let mut venue_symbols = HashSet::with_capacity(self.markets.len());
        let mut internal_symbols = HashSet::with_capacity(self.markets.len());
        for market in &self.markets {
            market.validate()?;
            if !venue_symbols.insert(&market.symbol) {
                bail!("BULK market catalog contains duplicate `{}`", market.symbol);
            }
            if !internal_symbols.insert(&market.internal_symbol) {
                bail!(
                    "BULK market catalog contains duplicate internal symbol `{}`",
                    market.internal_symbol
                );
            }
        }
        Ok(())
    }
}

impl BulkMarket {
    pub fn is_trading(&self) -> bool {
        self.status == "TRADING"
    }

    pub fn supports_order_type(&self, order_type: &str) -> bool {
        self.order_types
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(order_type))
    }

    fn validate(&self) -> Result<()> {
        let expected_symbol = format!("{}-{}", self.base_asset, self.quote_asset);
        let internal_quote = if self.quote_asset == "USD" {
            "USDT"
        } else {
            &self.quote_asset
        };
        let expected_internal_symbol = format!("{}/{internal_quote}", self.base_asset);
        if self.symbol != expected_symbol || venue_symbol(&self.symbol)? != self.symbol {
            bail!("invalid BULK market symbol `{}`", self.symbol);
        }
        if self.internal_symbol != expected_internal_symbol {
            bail!(
                "BULK market `{}` has invalid internal symbol `{}`; expected `{expected_internal_symbol}`",
                self.symbol,
                self.internal_symbol
            );
        }
        if !self.tick_size.is_finite() || self.tick_size <= 0.0 {
            bail!("BULK market `{}` has an invalid tick size", self.symbol);
        }
        if !self.lot_size.is_finite() || self.lot_size <= 0.0 {
            bail!("BULK market `{}` has an invalid lot size", self.symbol);
        }
        if !self.min_notional.is_finite() || self.min_notional <= 0.0 {
            bail!(
                "BULK market `{}` has an invalid minimum notional",
                self.symbol
            );
        }
        if self.max_leverage == 0 || self.order_types.is_empty() {
            bail!("BULK market `{}` has incomplete trading rules", self.symbol);
        }
        Ok(())
    }
}

pub fn market_catalog() -> Result<&'static BulkMarketCatalog> {
    if let Some(catalog) = CATALOG.get() {
        return Ok(catalog);
    }

    let catalog: BulkMarketCatalog =
        serde_json::from_str(CATALOG_JSON).context("embedded BULK market catalog is malformed")?;
    catalog.validate()?;
    let _ = CATALOG.set(catalog);

    CATALOG
        .get()
        .context("failed to initialize the BULK market catalog")
}

pub fn market(symbol: &str) -> Result<&'static BulkMarket> {
    let catalog = market_catalog()?;
    catalog.find(symbol)?.with_context(|| {
        let supported = catalog
            .markets
            .iter()
            .map(|market| market.symbol.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        format!("BULK does not support `{symbol}` in the local catalog; supported: {supported}")
    })
}

fn venue_symbol(symbol: &str) -> Result<String> {
    let (base, quote) = symbol_parts(symbol)?;
    Ok(format!("{base}-{quote}"))
}

fn symbol_parts(symbol: &str) -> Result<(String, String)> {
    let normalized = symbol.trim().to_ascii_uppercase().replace('/', "-");
    let mut parts = normalized.split('-');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(base), Some(quote), None) if !base.is_empty() && !quote.is_empty() => {
            Ok((base.to_string(), quote.to_string()))
        }
        _ => bail!("symbol must look like BASE/QUOTE or BASE-QUOTE, e.g. BTC/USDT"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_catalog_is_valid_and_complete() {
        let catalog = market_catalog().expect("catalog loads");

        assert_eq!(catalog.schema_version, 2);
        assert_eq!(catalog.provider, "bulk");
        assert_eq!(catalog.markets.len(), 11);
        assert!(catalog.markets.iter().all(BulkMarket::is_trading));
    }

    #[test]
    fn resolves_terminal_and_bulk_symbol_formats() {
        let terminal = market("btc/usdt").expect("internal symbol resolves");
        let usd_alias = market("btc/usd").expect("USD alias resolves");
        let native = market("BTC-USD").expect("native symbol resolves");

        assert_eq!(terminal.symbol, "BTC-USD");
        assert_eq!(terminal.internal_symbol, "BTC/USDT");
        assert_eq!(terminal.symbol, usd_alias.symbol);
        assert_eq!(terminal.symbol, native.symbol);
        assert_eq!(terminal.lot_size, 0.000001);
        assert_eq!(terminal.size_precision, 6);
        assert_eq!(terminal.min_notional, 1.0);
        assert!(terminal.supports_order_type("market"));
    }

    #[test]
    fn rejects_unknown_and_malformed_symbols() {
        assert!(market("BTC/USDC").is_err());
        assert!(market("BTC").is_err());
    }
}
