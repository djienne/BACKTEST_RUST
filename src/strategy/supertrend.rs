//! Supertrend following: long while the trend is up, flat while it is down.
//!
//! Supertrend's bands ratchet, so each `(period, multiplier)` pair has its own
//! path-dependent series and cannot share one with its neighbours. That makes
//! this the [`SeriesCache`] case rather than the
//! [`PeriodCache`](crate::indicators::PeriodCache) case — see the note on
//! [`SeriesCache`] for why the distinction matters for memory.

use crate::data::Bars;
use crate::indicators::volatility::supertrend;
use crate::indicators::{BarsF64, SeriesCache};
use crate::precision::BacktestFloat;
use crate::strategy::params::ParamSpec;
use crate::strategy::{ConfigurableStrategy, Signal, Strategy};
use std::cmp::Ordering;
use std::ops::RangeInclusive;

pub struct SupertrendFollow;

#[derive(Clone, Debug)]
pub struct SupertrendConfig {
    pub period: RangeInclusive<usize>,
    /// Band width in tenths of an ATR, so the sweep key stays an integer.
    pub multiplier_tenths: RangeInclusive<usize>,
}

impl Default for SupertrendConfig {
    fn default() -> Self {
        Self {
            period: 7..=21,
            multiplier_tenths: 15..=40,
        }
    }
}

impl SupertrendConfig {
    /// The parameter grid, in the order [`SeriesCache`] indexes it. Both
    /// `build_cache` and `enumerate_params` go through this so the cache index
    /// and the parameter tuple can never fall out of step.
    fn grid(&self) -> Vec<(usize, usize)> {
        self.period
            .clone()
            .flat_map(|period| {
                self.multiplier_tenths
                    .clone()
                    .map(move |tenths| (period, tenths))
            })
            .collect()
    }
}

impl Strategy for SupertrendFollow {
    /// `(index into the cache, period, multiplier_tenths)`.
    ///
    /// The index is carried in the tuple because a `SeriesCache` is keyed by
    /// position, and recomputing that position from the parameters in the hot
    /// loop would be a division per bar.
    type Params = (usize, usize, usize);
    type Cache<T: BacktestFloat> = SeriesCache<T>;
    type Config = SupertrendConfig;
    const NAME: &'static str = "supertrend";

    fn build_cache<T: BacktestFloat>(
        bars: Bars<'_, T>,
        cfg: &Self::Config,
    ) -> anyhow::Result<Self::Cache<T>> {
        let source = BarsF64::from_bars(bars);
        let grid = cfg.grid();
        SeriesCache::build(&source, &grid, |bars, (period, tenths)| {
            supertrend(bars, period, tenths as f64 / 10.0).direction
        })
    }

    fn enumerate_params(cfg: &Self::Config) -> Vec<Self::Params> {
        cfg.grid()
            .into_iter()
            .enumerate()
            .map(|(index, (period, tenths))| (index, period, tenths))
            .collect()
    }

    fn evaluator<'a, T: BacktestFloat>(
        _bars: Bars<'a, T>,
        cache: &'a Self::Cache<T>,
        params: Self::Params,
    ) -> impl Fn(usize) -> Signal + 'a {
        let (index, period, tenths) = params;
        let direction = cache.get(index).unwrap_or_else(|| {
            panic!("supertrend cache missing entry {index} (period={period}, tenths={tenths})")
        });
        move |bar_index| {
            let value = direction[bar_index];
            if value > T::ZERO {
                Signal::EnterLong
            } else if value < T::ZERO {
                Signal::ExitLong
            } else {
                // NaN during warmup.
                Signal::Hold
            }
        }
    }

    fn param_summary((_, period, tenths): Self::Params, _cfg: &Self::Config) -> String {
        format!("period={period},multiplier={:.1}", tenths as f64 / 10.0)
    }

    /// Compares on `(period, multiplier)` rather than the cache index, so the
    /// tie-break stays meaningful if the grid order ever changes.
    fn tie_break(left: Self::Params, right: Self::Params) -> Ordering {
        (left.1, left.2).cmp(&(right.1, right.2))
    }
}

impl ConfigurableStrategy for SupertrendFollow {
    const DESCRIPTION: &'static str =
        "Hold while Supertrend points up, stand aside while it points down";
    const PARAMETERS: &'static [(&'static str, &'static str)] = &[
        ("period", "ATR period (default 7..21)"),
        (
            "multiplier_tenths",
            "band width in tenths of an ATR (default 15..40, i.e. 1.5..4.0)",
        ),
    ];

    fn config_from(spec: &ParamSpec) -> anyhow::Result<Self::Config> {
        spec.reject_unknown(&Self::parameter_names())?;
        let defaults = SupertrendConfig::default();
        Ok(SupertrendConfig {
            period: spec.range("period", defaults.period)?,
            multiplier_tenths: spec.range("multiplier_tenths", defaults.multiplier_tenths)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::OwnedBars;

    fn trending_bars(len: usize, slope: f32) -> OwnedBars<f32> {
        let closes: Vec<f32> = (0..len).map(|i| 100.0 + slope * i as f32).collect();
        OwnedBars::ohlc(
            closes.clone(),
            closes.iter().map(|v| v + 1.0).collect(),
            closes.iter().map(|v| v - 1.0).collect(),
            closes,
        )
    }

    #[test]
    fn enumerate_params_indexes_match_the_cache_layout() {
        // The invariant that makes the index-in-params trick safe.
        let cfg = SupertrendConfig {
            period: 7..=9,
            multiplier_tenths: 20..=21,
        };
        let grid = cfg.grid();
        for (index, period, tenths) in SupertrendFollow::enumerate_params(&cfg) {
            assert_eq!(grid[index], (period, tenths), "index {index} must line up");
        }
    }

    #[test]
    fn build_cache_produces_one_series_per_parameter_tuple() {
        let prices = trending_bars(120, 1.0);
        let cfg = SupertrendConfig {
            period: 7..=8,
            multiplier_tenths: 20..=21,
        };
        let cache = SupertrendFollow::build_cache::<f32>(prices.bars(), &cfg).unwrap();
        let params = SupertrendFollow::enumerate_params(&cfg);
        assert_eq!(params.len(), 4);
        for (index, ..) in &params {
            assert!(cache.get(*index).is_some(), "missing entry {index}");
        }
        assert!(cache.get(params.len()).is_none(), "and no more than that");
    }

    #[test]
    fn evaluator_is_long_in_an_uptrend_and_flat_in_a_downtrend() {
        let rising = trending_bars(120, 1.0);
        let cfg = SupertrendConfig {
            period: 10..=10,
            multiplier_tenths: 30..=30,
        };
        let cache = SupertrendFollow::build_cache::<f32>(rising.bars(), &cfg).unwrap();
        let evaluator = SupertrendFollow::evaluator::<f32>(rising.bars(), &cache, (0, 10, 30));
        assert_eq!(evaluator(119), Signal::EnterLong);

        let falling = trending_bars(120, -1.0);
        let cache = SupertrendFollow::build_cache::<f32>(falling.bars(), &cfg).unwrap();
        let evaluator = SupertrendFollow::evaluator::<f32>(falling.bars(), &cache, (0, 10, 30));
        assert_eq!(evaluator(119), Signal::ExitLong);
    }

    #[test]
    fn build_cache_refuses_a_sweep_that_would_not_fit_in_memory() {
        // A grid this wide over a long series is exactly the mistake the
        // SeriesCache budget exists to catch.
        let prices = trending_bars(200_000, 0.01);
        let cfg = SupertrendConfig {
            period: 2..=400,
            multiplier_tenths: 10..=60,
        };
        let error = SupertrendFollow::build_cache::<f32>(prices.bars(), &cfg)
            .expect_err("should refuse rather than exhaust memory");
        let message = format!("{error:#}");
        assert!(message.contains("--param"), "should say how: {message}");
    }
}
