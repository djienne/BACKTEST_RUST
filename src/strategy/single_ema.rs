//! Price-vs-EMA threshold: long when close > EMA, flat otherwise.
//!
//! This is a seam-validating stub — not wired into the CLI. Its purpose is
//! to prove the `Strategy` trait works for a strategy with a 1-parameter
//! sweep that combines a precomputed indicator with a raw price column.

use crate::data::Bars;
use crate::indicators::ma::ema;
use crate::indicators::{BarsF64, PeriodCache};
use crate::precision::BacktestFloat;
use crate::strategy::{Signal, Strategy};
use std::cmp::Ordering;

pub struct PriceVsEma;

#[derive(Clone, Debug)]
pub struct PriceVsEmaConfig {
    pub period_min: usize,
    pub period_max: usize,
}

impl Strategy for PriceVsEma {
    type Params = usize;
    type Cache<T: BacktestFloat> = PeriodCache<T>;
    type Config = PriceVsEmaConfig;
    const NAME: &'static str = "price_vs_ema";

    fn build_cache<T: BacktestFloat>(bars: Bars<'_, T>, cfg: &Self::Config) -> Self::Cache<T> {
        let source = BarsF64::from_bars(bars);
        PeriodCache::build(&source, cfg.period_min, cfg.period_max, |bars, period| {
            ema(&bars.close, period)
        })
    }

    fn enumerate_params(cfg: &Self::Config) -> Vec<Self::Params> {
        (cfg.period_min..=cfg.period_max).collect()
    }

    fn evaluator<'a, T: BacktestFloat>(
        bars: Bars<'a, T>,
        cache: &'a Self::Cache<T>,
        params: Self::Params,
    ) -> impl Fn(usize) -> Signal + 'a {
        let ema = cache
            .get(params)
            .unwrap_or_else(|| panic!("EMA cache missing period {params}"));
        let close = bars.close;
        move |bar_index| {
            let close_value = close[bar_index];
            let ema_value = ema[bar_index];
            if close_value > ema_value {
                Signal::EnterLong
            } else if close_value < ema_value {
                Signal::ExitLong
            } else {
                Signal::Hold
            }
        }
    }

    fn param_summary(period: Self::Params) -> String {
        format!("period={period}")
    }

    fn tie_break(left: Self::Params, right: Self::Params) -> Ordering {
        left.cmp(&right)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::OwnedBars;

    #[test]
    fn evaluator_enters_long_when_close_above_ema() {
        let prices = OwnedBars::from_close(vec![100.0_f32, 110.0, 120.0]);
        let cache = PeriodCache::<f32>::from_series(1, vec![vec![100.0, 105.0, 115.0]]);
        let evaluator = PriceVsEma::evaluator::<f32>(prices.bars(), &cache, 1);
        assert_eq!(evaluator(0), Signal::Hold); // 100 == 100
        assert_eq!(evaluator(1), Signal::EnterLong); // 110 > 105
        assert_eq!(evaluator(2), Signal::EnterLong); // 120 > 115
    }

    #[test]
    fn evaluator_exits_when_close_falls_below_ema() {
        let prices = OwnedBars::from_close(vec![110.0_f32, 100.0]);
        let cache = PeriodCache::<f32>::from_series(1, vec![vec![105.0, 105.0]]);
        let evaluator = PriceVsEma::evaluator::<f32>(prices.bars(), &cache, 1);
        assert_eq!(evaluator(0), Signal::EnterLong);
        assert_eq!(evaluator(1), Signal::ExitLong);
    }

    #[test]
    fn enumerate_params_yields_inclusive_range() {
        let cfg = PriceVsEmaConfig {
            period_min: 5,
            period_max: 8,
        };
        assert_eq!(PriceVsEma::enumerate_params(&cfg), vec![5, 6, 7, 8]);
    }
}
