//! Bollinger Band mean reversion: buy the lower band, sell the middle.
//!
//! The bands are `sma ± k · sigma`, so the cache stores the SMA and the
//! standard deviation once per period and the evaluator applies `k` per bar.
//! That keeps a two-dimensional sweep (period × deviations) on a
//! one-dimensional [`PeriodCache`].

use crate::data::Bars;
use crate::indicators::ma::sma;
use crate::indicators::rolling::rolling_stddev;
use crate::indicators::{BarsF64, PeriodCache};
use crate::precision::BacktestFloat;
use crate::strategy::params::ParamSpec;
use crate::strategy::{ConfigurableStrategy, Signal, Strategy};
use std::cmp::Ordering;
use std::ops::RangeInclusive;

pub struct BollingerReversion;

pub struct BollingerCache<T> {
    pub middle: PeriodCache<T>,
    pub sigma: PeriodCache<T>,
}

#[derive(Clone, Debug)]
pub struct BollingerConfig {
    pub period: RangeInclusive<usize>,
    /// Band width in tenths of a standard deviation, so the sweep key stays an
    /// integer. `20` means 2.0 sigma.
    pub deviation_tenths: RangeInclusive<usize>,
}

impl Default for BollingerConfig {
    fn default() -> Self {
        Self {
            period: 10..=40,
            deviation_tenths: 15..=30,
        }
    }
}

impl Strategy for BollingerReversion {
    /// `(period, deviation_tenths)`.
    type Params = (usize, usize);
    type Cache<T: BacktestFloat> = BollingerCache<T>;
    type Config = BollingerConfig;
    const NAME: &'static str = "bb_reversion";

    fn build_cache<T: BacktestFloat>(
        bars: Bars<'_, T>,
        cfg: &Self::Config,
    ) -> anyhow::Result<Self::Cache<T>> {
        let source = BarsF64::from_bars(bars);
        let (min, max) = (*cfg.period.start(), *cfg.period.end());
        Ok(BollingerCache {
            middle: PeriodCache::build(&source, min, max, |bars, period| sma(&bars.close, period)),
            sigma: PeriodCache::build(&source, min, max, |bars, period| {
                rolling_stddev(&bars.close, period)
            }),
        })
    }

    fn enumerate_params(cfg: &Self::Config) -> Vec<Self::Params> {
        cfg.period
            .clone()
            .flat_map(|period| {
                cfg.deviation_tenths
                    .clone()
                    .map(move |tenths| (period, tenths))
            })
            .collect()
    }

    fn evaluator<'a, T: BacktestFloat>(
        bars: Bars<'a, T>,
        cache: &'a Self::Cache<T>,
        params: Self::Params,
    ) -> impl Fn(usize) -> Signal + 'a {
        let (period, tenths) = params;
        let middle = cache
            .middle
            .get(period)
            .unwrap_or_else(|| panic!("Bollinger cache missing period {period}"));
        let sigma = cache
            .sigma
            .get(period)
            .unwrap_or_else(|| panic!("Bollinger sigma cache missing period {period}"));
        let close = bars.close;
        let deviations = T::from_f64(tenths as f64 / 10.0);
        move |bar_index| {
            let centre = middle[bar_index];
            let spread = sigma[bar_index] * deviations;
            let price = close[bar_index];
            if price < centre - spread {
                // Stretched below the lower band: fade the move.
                Signal::EnterLong
            } else if price > centre {
                // Reverted to the mean — the trade is done.
                Signal::ExitLong
            } else {
                Signal::Hold
            }
        }
    }

    fn param_summary((period, tenths): Self::Params) -> String {
        format!("period={period},deviations={:.1}", tenths as f64 / 10.0)
    }

    fn tie_break(left: Self::Params, right: Self::Params) -> Ordering {
        left.cmp(&right)
    }
}

impl ConfigurableStrategy for BollingerReversion {
    const DESCRIPTION: &'static str = "Buy below the lower Bollinger band, exit at the mean";
    const PARAMETERS: &'static [(&'static str, &'static str)] = &[
        ("period", "Bollinger period (default 10..40)"),
        (
            "deviation_tenths",
            "band width in tenths of a sigma (default 15..30, i.e. 1.5..3.0)",
        ),
    ];

    fn config_from(spec: &ParamSpec) -> anyhow::Result<Self::Config> {
        spec.reject_unknown(&Self::parameter_names())?;
        let defaults = BollingerConfig::default();
        Ok(BollingerConfig {
            period: spec.range("period", defaults.period)?,
            deviation_tenths: spec.range("deviation_tenths", defaults.deviation_tenths)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::OwnedBars;

    fn cache_of(middle: Vec<f32>, sigma: Vec<f32>) -> BollingerCache<f32> {
        BollingerCache {
            middle: PeriodCache::<f32>::from_series(3, vec![middle]),
            sigma: PeriodCache::<f32>::from_series(3, vec![sigma]),
        }
    }

    #[test]
    fn evaluator_buys_below_the_lower_band_and_exits_at_the_mean() {
        // Middle 100, sigma 5, deviations 2.0 → lower band at 90.
        let prices = OwnedBars::from_close(vec![89.0_f32, 95.0, 101.0]);
        let cache = cache_of(vec![100.0; 3], vec![5.0; 3]);
        let evaluator = BollingerReversion::evaluator::<f32>(prices.bars(), &cache, (3, 20));
        assert_eq!(evaluator(0), Signal::EnterLong, "89 < 90");
        assert_eq!(evaluator(1), Signal::Hold, "between the band and the mean");
        assert_eq!(evaluator(2), Signal::ExitLong, "101 > 100");
    }

    #[test]
    fn wider_bands_require_a_deeper_stretch_to_trigger() {
        let prices = OwnedBars::from_close(vec![89.0_f32]);
        let cache = cache_of(vec![100.0], vec![5.0]);
        // 2.0 sigma → lower band 90, so 89 triggers.
        let tight = BollingerReversion::evaluator::<f32>(prices.bars(), &cache, (3, 20));
        assert_eq!(tight(0), Signal::EnterLong);
        // 3.0 sigma → lower band 85, so 89 does not.
        let wide = BollingerReversion::evaluator::<f32>(prices.bars(), &cache, (3, 30));
        assert_eq!(wide(0), Signal::Hold);
    }

    #[test]
    fn param_summary_renders_tenths_as_a_decimal() {
        assert_eq!(
            BollingerReversion::param_summary((20, 25)),
            "period=20,deviations=2.5"
        );
    }

    #[test]
    fn enumerate_params_covers_the_grid() {
        let cfg = BollingerConfig {
            period: 10..=11,
            deviation_tenths: 20..=21,
        };
        assert_eq!(
            BollingerReversion::enumerate_params(&cfg),
            vec![(10, 20), (10, 21), (11, 20), (11, 21)]
        );
    }
}
