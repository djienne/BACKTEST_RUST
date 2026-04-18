//! Price-vs-EMA threshold: long when close > EMA, flat otherwise.
//!
//! This is a seam-validating stub — not wired into the CLI. Its purpose is
//! to prove the `Strategy` trait works for a strategy with a 1-parameter
//! sweep and a cache that combines the shared `EMAStore` with strategy-
//! private state (the `close` series).

use crate::precision::BacktestFloat;
use crate::strategy::{Signal, Strategy};
use crate::ta_wrapper::EMAStore;
use std::cmp::Ordering;

pub struct PriceVsEma;

#[derive(Clone, Debug)]
pub struct PriceVsEmaConfig {
    pub period_min: usize,
    pub period_max: usize,
}

pub struct PriceVsEmaCache<T> {
    pub ema: EMAStore<T>,
    pub close: Vec<T>,
}

impl Strategy for PriceVsEma {
    type Params = usize;
    type Cache<T: BacktestFloat> = PriceVsEmaCache<T>;
    type Config = PriceVsEmaConfig;
    const NAME: &'static str = "price_vs_ema";

    fn build_cache<T: BacktestFloat>(
        _open: &[T],
        close: &[T],
        cfg: &Self::Config,
    ) -> Self::Cache<T> {
        PriceVsEmaCache {
            ema: EMAStore::new(close, cfg.period_min, cfg.period_max),
            close: close.to_vec(),
        }
    }

    fn enumerate_params(cfg: &Self::Config) -> Vec<Self::Params> {
        (cfg.period_min..=cfg.period_max).collect()
    }

    fn evaluator<'a, T: BacktestFloat>(
        cache: &'a Self::Cache<T>,
        params: Self::Params,
    ) -> impl Fn(usize) -> Signal + 'a {
        let ema = cache
            .ema
            .get_ema(params)
            .unwrap_or_else(|| panic!("EMA store missing period {params}"));
        let close = cache.close.as_slice();
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

    fn cache_with(close: Vec<f32>, ema: Vec<f32>) -> PriceVsEmaCache<f32> {
        PriceVsEmaCache {
            ema: EMAStore::<f32>::from_series(1, vec![ema]),
            close,
        }
    }

    #[test]
    fn evaluator_enters_long_when_close_above_ema() {
        let cache = cache_with(vec![100.0, 110.0, 120.0], vec![100.0, 105.0, 115.0]);
        let evaluator = PriceVsEma::evaluator::<f32>(&cache, 1);
        assert_eq!(evaluator(0), Signal::Hold); // 100 == 100
        assert_eq!(evaluator(1), Signal::EnterLong); // 110 > 105
        assert_eq!(evaluator(2), Signal::EnterLong); // 120 > 115
    }

    #[test]
    fn evaluator_exits_when_close_falls_below_ema() {
        let cache = cache_with(vec![110.0, 100.0], vec![105.0, 105.0]);
        let evaluator = PriceVsEma::evaluator::<f32>(&cache, 1);
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
