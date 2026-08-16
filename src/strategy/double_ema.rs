//! Two-EMA crossover strategy: long when fast EMA > slow EMA, flat otherwise.
//!
//! Indicator values become valid one bar after each EMA's period; before
//! that, the EMA is `NaN` and every comparison against `NaN` is false, so the
//! evaluator naturally emits `Hold` during warmup without an explicit guard.

use crate::backtest::ema_parameter_pairs;
use crate::data::Bars;
use crate::indicators::ma::ema;
use crate::indicators::{BarsF64, PeriodCache};
use crate::precision::BacktestFloat;
use crate::strategy::params::ParamSpec;
use crate::strategy::{ConfigurableStrategy, Signal, Strategy};
use std::cmp::Ordering;

pub struct DoubleEmaCrossover;

#[derive(Clone, Debug)]
pub struct DoubleEmaConfig {
    pub fast_period_min: usize,
    pub slow_period_min: usize,
    pub max_period: usize,
}

impl Default for DoubleEmaConfig {
    fn default() -> Self {
        Self {
            fast_period_min: 5,
            slow_period_min: 6,
            max_period: 600,
        }
    }
}

impl Strategy for DoubleEmaCrossover {
    type Params = (usize, usize);
    type Cache<T: BacktestFloat> = PeriodCache<T>;
    type Config = DoubleEmaConfig;
    const NAME: &'static str = "double_ema";

    fn build_cache<T: BacktestFloat>(
        bars: Bars<'_, T>,
        cfg: &Self::Config,
    ) -> anyhow::Result<Self::Cache<T>> {
        let source = BarsF64::from_bars(bars);
        Ok(PeriodCache::build(
            &source,
            cfg.fast_period_min,
            cfg.max_period,
            |bars, period| ema(&bars.close, period),
        ))
    }

    fn enumerate_params(cfg: &Self::Config) -> Vec<Self::Params> {
        ema_parameter_pairs(cfg.fast_period_min, cfg.slow_period_min, cfg.max_period)
    }

    fn evaluator<'a, T: BacktestFloat>(
        _bars: Bars<'a, T>,
        cache: &'a Self::Cache<T>,
        params: Self::Params,
    ) -> impl Fn(usize) -> Signal + 'a {
        let (fast_period, slow_period) = params;
        let fast = cache
            .get(fast_period)
            .unwrap_or_else(|| panic!("EMA cache missing fast period {fast_period}"));
        let slow = cache
            .get(slow_period)
            .unwrap_or_else(|| panic!("EMA cache missing slow period {slow_period}"));
        move |bar_index| {
            let fast_value = fast[bar_index];
            let slow_value = slow[bar_index];
            if fast_value > slow_value {
                Signal::EnterLong
            } else if fast_value < slow_value {
                Signal::ExitLong
            } else {
                // Equal or NaN — no transition.
                Signal::Hold
            }
        }
    }

    fn param_summary((fast, slow): Self::Params) -> String {
        format!("fast={fast},slow={slow}")
    }

    /// Smaller `(fast, slow)` lex-tuple wins on ties, so a sweep is
    /// reproducible regardless of how rayon happened to schedule it.
    fn tie_break(left: Self::Params, right: Self::Params) -> Ordering {
        left.cmp(&right)
    }
}

impl ConfigurableStrategy for DoubleEmaCrossover {
    const DESCRIPTION: &'static str = "Hold while the fast EMA is above the slow EMA";
    const PARAMETERS: &'static [(&'static str, &'static str)] = &[
        ("fast_min", "smallest fast period (default 5)"),
        ("slow_min", "smallest slow period (default 6)"),
        ("max_period", "largest period of either EMA (default 600)"),
    ];

    fn config_from(spec: &ParamSpec) -> anyhow::Result<Self::Config> {
        spec.reject_unknown(&Self::parameter_names())?;
        let defaults = DoubleEmaConfig::default();
        let config = DoubleEmaConfig {
            fast_period_min: spec.scalar("fast_min", defaults.fast_period_min)?,
            slow_period_min: spec.scalar("slow_min", defaults.slow_period_min)?,
            max_period: spec.scalar("max_period", defaults.max_period)?,
        };
        if config.fast_period_min == 0 || config.slow_period_min == 0 {
            anyhow::bail!("EMA periods must be at least 1");
        }
        if config.max_period < config.slow_period_min {
            anyhow::bail!(
                "max_period ({}) is below slow_min ({}), leaving nothing to sweep",
                config.max_period,
                config.slow_period_min
            );
        }
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::OwnedBars;

    #[test]
    fn enumerate_params_matches_search_space() {
        let cfg = DoubleEmaConfig {
            fast_period_min: 5,
            slow_period_min: 6,
            max_period: 7,
        };
        assert_eq!(
            DoubleEmaCrossover::enumerate_params(&cfg),
            vec![(5, 6), (5, 7), (6, 7)]
        );
    }

    #[test]
    fn evaluator_emits_enter_long_when_fast_crosses_above_slow() {
        let prices = OwnedBars::from_close(vec![1.0_f32; 4]);
        let cache = PeriodCache::<f32>::from_series(
            1,
            vec![vec![f32::NAN, 2.0, 1.0, 1.0], vec![f32::NAN, 1.0, 2.0, 1.0]],
        );
        let evaluator = DoubleEmaCrossover::evaluator::<f32>(prices.bars(), &cache, (1, 2));
        // Index 0: NaN comparisons false in both directions → Hold (warmup).
        assert_eq!(evaluator(0), Signal::Hold);
        // Index 1: fast=2 > slow=1 → EnterLong.
        assert_eq!(evaluator(1), Signal::EnterLong);
        // Index 2: fast=1 < slow=2 → ExitLong.
        assert_eq!(evaluator(2), Signal::ExitLong);
        // Index 3: fast=1 == slow=1 → Hold.
        assert_eq!(evaluator(3), Signal::Hold);
    }

    #[test]
    fn build_cache_covers_every_period_the_sweep_will_ask_for() {
        let prices = OwnedBars::from_close((0..100).map(|i| 100.0 + i as f32).collect());
        let cfg = DoubleEmaConfig {
            fast_period_min: 5,
            slow_period_min: 6,
            max_period: 12,
        };
        let cache = DoubleEmaCrossover::build_cache::<f32>(prices.bars(), &cfg).unwrap();
        for (fast, slow) in DoubleEmaCrossover::enumerate_params(&cfg) {
            assert!(cache.get(fast).is_some(), "missing fast period {fast}");
            assert!(cache.get(slow).is_some(), "missing slow period {slow}");
        }
    }

    #[test]
    fn config_from_rejects_a_search_space_with_nothing_in_it() {
        let spec = ParamSpec::parse(["slow_min=50", "max_period=20"]).unwrap();
        assert!(DoubleEmaCrossover::config_from(&spec).is_err());
    }
}
