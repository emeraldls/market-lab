use anyhow::{Result, bail};

const MAX_CHILD_ORDERS: u64 = 100_000;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TwapChild {
    pub sequence: u64,
    pub offset_secs: u64,
    pub size: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TwapSchedule {
    pub total_size: f64,
    pub duration_secs: u64,
    pub interval_secs: u64,
    pub children: Vec<TwapChild>,
}

impl TwapSchedule {
    pub fn build(
        total_size: f64,
        lot_size: f64,
        reference_price: f64,
        min_notional: f64,
        duration_secs: u64,
        interval_secs: u64,
    ) -> Result<Self> {
        if !total_size.is_finite() || total_size <= 0.0 {
            bail!("TWAP total size must be greater than zero");
        }
        if !lot_size.is_finite() || lot_size <= 0.0 {
            bail!("TWAP lot size must be greater than zero");
        }
        if !reference_price.is_finite() || reference_price <= 0.0 {
            bail!("TWAP reference price must be greater than zero");
        }
        if !min_notional.is_finite() || min_notional < 0.0 {
            bail!("TWAP minimum notional cannot be negative");
        }
        if duration_secs == 0 {
            bail!("TWAP duration must be at least one second");
        }
        if interval_secs == 0 {
            bail!("TWAP interval must be at least one second");
        }

        let requested_children = duration_secs.div_ceil(interval_secs);
        if requested_children > MAX_CHILD_ORDERS {
            bail!(
                "TWAP schedule would create {requested_children} child orders; increase --interval"
            );
        }

        let raw_lots = total_size / lot_size;
        let rounded_lots = raw_lots.round();
        let alignment_tolerance = 1e-8_f64.max(raw_lots.abs() * 1e-12);
        if (raw_lots - rounded_lots).abs() > alignment_tolerance {
            bail!("TWAP total size {total_size} is not aligned to lot size {lot_size}");
        }
        if rounded_lots > u64::MAX as f64 {
            bail!("TWAP total size is too large");
        }
        let total_lots = rounded_lots as u64;
        if total_lots < requested_children {
            bail!(
                "TWAP needs {requested_children} child orders but the target contains only {total_lots} lots; increase --interval or size"
            );
        }

        let base_lots = total_lots / requested_children;
        let extra_lots = total_lots % requested_children;
        let mut children = Vec::with_capacity(requested_children as usize);
        for index in 0..requested_children {
            let extras_before = index * extra_lots / requested_children;
            let extras_after = (index + 1) * extra_lots / requested_children;
            let lots = base_lots + (extras_after - extras_before);
            let size = lots as f64 * lot_size;
            let estimated_notional = size * reference_price;
            if estimated_notional + f64::EPSILON < min_notional {
                bail!(
                    "TWAP child order {} has estimated notional {estimated_notional:.8}, below the market minimum {min_notional}; increase --interval or total size",
                    index + 1
                );
            }
            children.push(TwapChild {
                sequence: index + 1,
                offset_secs: index * interval_secs,
                size,
            });
        }

        Ok(Self {
            total_size,
            duration_secs,
            interval_secs,
            children,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_all_lots_without_losing_the_remainder() {
        let schedule =
            TwapSchedule::build(1.03, 0.01, 100.0, 1.0, 30, 10).expect("schedule should build");
        let sizes = schedule
            .children
            .iter()
            .map(|child| child.size)
            .collect::<Vec<_>>();

        assert_eq!(schedule.children.len(), 3);
        assert_eq!(sizes, vec![0.34, 0.34, 0.35000000000000003]);
        assert!((sizes.iter().sum::<f64>() - 1.03).abs() < 1e-12);
        assert_eq!(schedule.children[0].offset_secs, 0);
        assert_eq!(schedule.children[2].offset_secs, 20);
    }

    #[test]
    fn rejects_children_below_market_minimum() {
        let error = TwapSchedule::build(0.003, 0.001, 1_000.0, 5.0, 30, 10)
            .expect_err("each child would be below the minimum");

        assert!(error.to_string().contains("below the market minimum"));
    }

    #[test]
    fn rejects_more_children_than_available_lots() {
        let error = TwapSchedule::build(0.02, 0.01, 100.0, 0.0, 30, 10)
            .expect_err("there are only two lots for three children");

        assert!(error.to_string().contains("target contains only 2 lots"));
    }
}
