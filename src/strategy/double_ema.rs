//! Two-EMA crossover strategy: long when fast EMA > slow EMA, flat otherwise.
//!
//! Indicator values become valid one bar after each EMA's period; before
//! that, the EMA is `NaN` and `NaN > NaN` is `false`, so `desired_position`
//! naturally returns `Flat` during warmup without an explicit guard.

use crate::backtest::ema_parameter_pairs;
use crate::precision::BacktestFloat;
use crate::strategy::{Signal, Strategy};
use crate::ta_wrapper::EMAStore;
use std::cmp::Ordering;

pub struct DoubleEmaCrossover;

#[derive(Clone, Debug)]
pub struct DoubleEmaConfig {
    pub fast_period_min: usize,
    pub slow_period_min: usize,
    pub max_period: usize,
}

impl Strategy for DoubleEmaCrossover {
    type Params = (usize, usize);
    type Cache<T: BacktestFloat> = EMAStore<T>;
    type Config = DoubleEmaConfig;
    const NAME: &'static str = "double_ema";

    fn build_cache<T: BacktestFloat>(
        _open: &[T],
        close: &[T],
        cfg: &Self::Config,
    ) -> Self::Cache<T> {
        EMAStore::new(close, cfg.fast_period_min, cfg.max_period)
    }

    fn enumerate_params(cfg: &Self::Config) -> Vec<Self::Params> {
        ema_parameter_pairs(cfg.fast_period_min, cfg.slow_period_min, cfg.max_period)
    }

    fn evaluator<'a, T: BacktestFloat>(
        cache: &'a Self::Cache<T>,
        params: Self::Params,
    ) -> impl Fn(usize) -> Signal + 'a {
        let (fast_period, slow_period) = params;
        let fast = cache
            .get_ema(fast_period)
            .unwrap_or_else(|| panic!("EMA store missing fast period {fast_period}"));
        let slow = cache
            .get_ema(slow_period)
            .unwrap_or_else(|| panic!("EMA store missing slow period {slow_period}"));
        move |bar_index| {
            let fast_value = fast[bar_index];
            let slow_value = slow[bar_index];
            if fast_value > slow_value {
                Signal::EnterLong
            } else if fast_value < slow_value {
                Signal::ExitLong
            } else {
                // Equal or NaN — preserves the pre-refactor "no transition" path.
                Signal::Hold
            }
        }
    }

    fn param_summary((fast, slow): Self::Params) -> String {
        format!("fast={fast},slow={slow}")
    }

    /// Smaller `(fast, slow)` lex-tuple wins on ties — preserves the
    /// pre-refactor behavior so determinism checks against `main` hold.
    fn tie_break(left: Self::Params, right: Self::Params) -> Ordering {
        left.cmp(&right)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let cache = EMAStore::<f32>::from_series(
            1,
            vec![vec![f32::NAN, 2.0, 1.0, 1.0], vec![f32::NAN, 1.0, 2.0, 1.0]],
        );
        let evaluator = DoubleEmaCrossover::evaluator::<f32>(&cache, (1, 2));
        // Index 0: NaN comparisons false in both directions → Hold (warmup).
        assert_eq!(evaluator(0), Signal::Hold);
        // Index 1: fast=2 > slow=1 → EnterLong.
        assert_eq!(evaluator(1), Signal::EnterLong);
        // Index 2: fast=1 < slow=2 → ExitLong.
        assert_eq!(evaluator(2), Signal::ExitLong);
        // Index 3: fast=1 == slow=1 → Hold.
        assert_eq!(evaluator(3), Signal::Hold);
    }
}
