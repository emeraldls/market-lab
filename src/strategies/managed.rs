use crate::strategies::jobs::{StrategySide, TwapJobDefinition};
use crate::strategies::twap::TwapSchedule;
use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

pub const MAX_ORDERS: u64 = 10_000;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStyle {
    Pov,
    Iceberg,
    Scale,
}
impl ExecutionStyle {
    pub fn name(self) -> &'static str {
        match self {
            Self::Pov => "pov",
            Self::Iceberg => "iceberg",
            Self::Scale => "scale",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagedDefinition {
    #[serde(flatten)]
    pub base: TwapJobDefinition,
    pub style: ExecutionStyle,
    pub limit_price: Option<f64>,
    pub participation: Option<f64>,
    pub display_size: Option<f64>,
    pub start_price: Option<f64>,
    pub end_price: Option<f64>,
    pub levels: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Slice {
    pub size: f64,
    pub price: f64,
}

impl ManagedDefinition {
    pub fn validate(&self) -> Result<()> {
        self.base.validate()?;
        for value in [
            self.limit_price,
            self.participation,
            self.display_size,
            self.start_price,
            self.end_price,
        ]
        .into_iter()
        .flatten()
        {
            if !value.is_finite() || value <= 0.0 {
                bail!("strategy prices, sizes and participation must be finite and positive");
            }
        }
        match self.style {
            ExecutionStyle::Pov => {
                if self.limit_price.is_none() || self.participation.is_none_or(|p| p > 100.0) {
                    bail!("POV requires --limit-price and --participation in (0, 100]");
                }
                if self.display_size.is_some()
                    || self.start_price.is_some()
                    || self.end_price.is_some()
                    || self.levels.is_some()
                {
                    bail!("POV does not accept iceberg or scale settings");
                }
            }
            ExecutionStyle::Iceberg => {
                if self.limit_price.is_none()
                    || self.display_size.is_none_or(|s| s > self.base.total_size)
                {
                    bail!(
                        "Iceberg requires --limit-price and --display-size no greater than total size"
                    );
                }
                if self.participation.is_some()
                    || self.start_price.is_some()
                    || self.end_price.is_some()
                    || self.levels.is_some()
                {
                    bail!("Iceberg does not accept POV or scale settings");
                }
            }
            ExecutionStyle::Scale => {
                if self
                    .start_price
                    .zip(self.end_price)
                    .is_none_or(|(a, b)| a >= b)
                    || self.levels.is_none_or(|n| !(2..=100).contains(&n))
                {
                    bail!(
                        "Scale requires --start-price < --end-price and --levels between 2 and 100"
                    );
                }
                if self.limit_price.is_some()
                    || self.participation.is_some()
                    || self.display_size.is_some()
                {
                    bail!("Scale does not accept POV or iceberg settings");
                }
            }
        }
        Ok(())
    }

    pub fn slices(&self, lot: f64, tick: f64, minimum: f64) -> Result<Vec<Slice>> {
        self.validate()?;
        if !tick.is_finite() || tick <= 0.0 {
            bail!("invalid market tick size");
        }
        for price in [self.limit_price, self.start_price, self.end_price]
            .into_iter()
            .flatten()
        {
            let ticks = price / tick;
            if (ticks - ticks.round()).abs() > 1e-7_f64.max(ticks.abs() * 1e-12) {
                bail!("price {price} must align to tick size {tick}");
            }
        }
        let count = match self.style {
            ExecutionStyle::Pov => 1,
            ExecutionStyle::Iceberg => {
                (self.base.total_size / self.display_size.unwrap()).ceil() as u64
            }
            ExecutionStyle::Scale => self.levels.unwrap(),
        };
        if count == 0 || count > MAX_ORDERS {
            bail!("strategy may contain at most {MAX_ORDERS} child orders");
        }
        let low = self.start_price.or(self.limit_price).unwrap();
        let schedule = TwapSchedule::build(self.base.total_size, lot, low, minimum, count, 1)?;
        let mut slices = Vec::new();
        for (index, child) in schedule.children.iter().enumerate() {
            let price = if self.style == ExecutionStyle::Scale {
                let high = self.end_price.unwrap();
                ((low + (high - low) * index as f64 / (count - 1) as f64) / tick).round() * tick
            } else {
                low
            };
            if slices.last().is_some_and(|s: &Slice| {
                self.style == ExecutionStyle::Scale && (s.price - price).abs() < tick / 2.0
            }) {
                bail!("scale range is too narrow for this number of levels");
            }
            if self.style == ExecutionStyle::Iceberg
                && child.size > self.display_size.unwrap() + lot * 1e-8
            {
                bail!("display size must fit the market lot size");
            }
            slices.push(Slice {
                size: child.size,
                price,
            });
        }
        if self.style == ExecutionStyle::Scale && self.base.side == StrategySide::Buy {
            slices.reverse();
        }
        Ok(slices)
    }
}

pub fn pov_credit(volume: f64, participation: f64, submitted: f64, total: f64, lot: f64) -> f64 {
    ((volume * participation / 100.0 - submitted)
        .max(0.0)
        .min((total - submitted).max(0.0))
        / lot)
        .floor()
        * lot
}

#[cfg(test)]
mod tests {
    use super::*;
    fn definition(style: ExecutionStyle) -> ManagedDefinition {
        ManagedDefinition {
            base: TwapJobDefinition {
                venue: crate::domain::execution::ExecutionVenue::Bulk,
                testnet: true,
                symbol: "BTC".into(),
                side: StrategySide::Buy,
                total_size: 1.0,
                requested_margin: None,
                target_margin: 100.0,
                target_exposure: 100.0,
                duration_seconds: 60,
                interval_seconds: 1,
                leverage: 1.0,
                reduce_only: false,
            },
            style,
            limit_price: Some(100.0),
            participation: None,
            display_size: Some(0.3),
            start_price: None,
            end_price: None,
            levels: None,
        }
    }
    #[test]
    fn iceberg_preserves_total_and_display_cap() {
        let d = definition(ExecutionStyle::Iceberg);
        let s = d.slices(0.01, 1.0, 1.0).unwrap();
        assert_eq!(s.len(), 4);
        assert!(s.iter().all(|s| s.size <= 0.3));
        assert!((s.iter().map(|s| s.size).sum::<f64>() - 1.0).abs() < 1e-10);
    }
    #[test]
    fn scale_orders_cover_range_and_reject_collapsed_ticks() {
        let mut d = definition(ExecutionStyle::Scale);
        d.limit_price = None;
        d.display_size = None;
        d.start_price = Some(90.0);
        d.end_price = Some(100.0);
        d.levels = Some(3);
        let s = d.slices(0.01, 1.0, 1.0).unwrap();
        assert_eq!(
            s.iter().map(|s| s.price).collect::<Vec<_>>(),
            vec![100.0, 95.0, 90.0]
        );
        d.levels = Some(20);
        assert!(d.slices(0.01, 1.0, 1.0).is_err());
    }
    #[test]
    fn rejects_invalid_and_irrelevant_options() {
        let mut d = definition(ExecutionStyle::Iceberg);
        d.participation = Some(5.0);
        assert!(d.validate().is_err());
        d.participation = None;
        d.limit_price = Some(f64::NAN);
        assert!(d.validate().is_err());
    }
    #[test]
    fn pov_never_exceeds_participation_or_target() {
        assert_eq!(pov_credit(100.0, 5.0, 3.0, 10.0, 1.0), 2.0);
        assert_eq!(pov_credit(1000.0, 5.0, 9.0, 10.0, 1.0), 1.0);
        assert_eq!(pov_credit(10.0, 5.0, 1.0, 10.0, 1.0), 0.0);
    }
}
