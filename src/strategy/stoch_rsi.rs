//! Stochastic RSI mean reversion.
//!
//! Same shape as [`crate::strategy::rsi_reversion`], but on the twitchier
//! StochRSI `%K`. The RSI period and the smoothing lengths are fixed scalars;
//! the stochastic lookback and the thresholds are what get swept.

use crate::data::Bars;
use crate::indicators::momentum::stochastic_rsi;
use crate::indicators::{BarsF64, PeriodCache};
use crate::precision::BacktestFloat;
use crate::strategy::params::ParamSpec;
use crate::strategy::{ConfigurableStrategy, Signal, Strategy};
use std::cmp::Ordering;
use std::ops::RangeInclusive;

pub struct StochRsi;

#[derive(Clone, Debug)]
pub struct StochRsiConfig {
    pub rsi_period: usize,
    pub k_smooth: usize,
    pub d_smooth: usize,
    pub stoch_period: RangeInclusive<usize>,
    pub oversold: RangeInclusive<usize>,
    pub overbought: RangeInclusive<usize>,
}

impl Default for StochRsiConfig {
    fn default() -> Self {
        Self {
            rsi_period: 14,
            k_smooth: 3,
            d_smooth: 3,
            stoch_period: 5..=30,
            oversold: 10..=25,
            overbought: 75..=90,
        }
    }
}

impl Strategy for StochRsi {
    /// `(stoch_period, oversold, overbought)`.
    type Params = (usize, usize, usize);
    type Cache<T: BacktestFloat> = PeriodCache<T>;
    type Config = StochRsiConfig;
    const NAME: &'static str = "stoch_rsi";

    fn build_cache<T: BacktestFloat>(
        bars: Bars<'_, T>,
        cfg: &Self::Config,
    ) -> anyhow::Result<Self::Cache<T>> {
        let source = BarsF64::from_bars(bars);
        let (rsi_period, k_smooth, d_smooth) = (cfg.rsi_period, cfg.k_smooth, cfg.d_smooth);
        Ok(PeriodCache::build(
            &source,
            *cfg.stoch_period.start(),
            *cfg.stoch_period.end(),
            move |bars, period| {
                stochastic_rsi(&bars.close, rsi_period, period, k_smooth, d_smooth).k
            },
        ))
    }

    fn enumerate_params(cfg: &Self::Config) -> Vec<Self::Params> {
        let mut params = Vec::new();
        for period in cfg.stoch_period.clone() {
            for oversold in cfg.oversold.clone() {
                for overbought in cfg.overbought.clone() {
                    if overbought > oversold {
                        params.push((period, oversold, overbought));
                    }
                }
            }
        }
        params
    }

    fn evaluator<'a, T: BacktestFloat>(
        _bars: Bars<'a, T>,
        cache: &'a Self::Cache<T>,
        params: Self::Params,
    ) -> impl Fn(usize) -> Signal + 'a {
        let (period, oversold, overbought) = params;
        let values = cache
            .get(period)
            .unwrap_or_else(|| panic!("StochRSI cache missing period {period}"));
        let low = T::from_usize(oversold);
        let high = T::from_usize(overbought);
        move |bar_index| {
            let value = values[bar_index];
            if value < low {
                Signal::EnterLong
            } else if value > high {
                Signal::ExitLong
            } else {
                Signal::Hold
            }
        }
    }

    fn param_summary((period, oversold, overbought): Self::Params) -> String {
        format!("stoch_period={period},oversold={oversold},overbought={overbought}")
    }

    fn tie_break(left: Self::Params, right: Self::Params) -> Ordering {
        left.cmp(&right)
    }
}

impl ConfigurableStrategy for StochRsi {
    const DESCRIPTION: &'static str = "Buy oversold / sell overbought on the StochRSI %K line";
    const PARAMETERS: &'static [(&'static str, &'static str)] = &[
        (
            "rsi_period",
            "RSI period feeding the stochastic (default 14)",
        ),
        ("k_smooth", "%K smoothing (default 3)"),
        ("d_smooth", "%D smoothing (default 3)"),
        ("stoch_period", "stochastic lookback (default 5..30)"),
        ("oversold", "entry threshold (default 10..25)"),
        ("overbought", "exit threshold (default 75..90)"),
    ];

    fn config_from(spec: &ParamSpec) -> anyhow::Result<Self::Config> {
        spec.reject_unknown(&Self::parameter_names())?;
        let defaults = StochRsiConfig::default();
        Ok(StochRsiConfig {
            rsi_period: spec.scalar("rsi_period", defaults.rsi_period)?,
            k_smooth: spec.scalar("k_smooth", defaults.k_smooth)?,
            d_smooth: spec.scalar("d_smooth", defaults.d_smooth)?,
            stoch_period: spec.range("stoch_period", defaults.stoch_period)?,
            oversold: spec.range("oversold", defaults.oversold)?,
            overbought: spec.range("overbought", defaults.overbought)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::OwnedBars;

    #[test]
    fn build_cache_covers_the_swept_stochastic_periods() {
        let closes: Vec<f32> = (0..300)
            .map(|i| 100.0 + ((i as f32) * 0.2).sin() * 10.0)
            .collect();
        let prices = OwnedBars::from_close(closes);
        let cfg = StochRsiConfig {
            stoch_period: 5..=8,
            ..StochRsiConfig::default()
        };
        let cache = StochRsi::build_cache::<f32>(prices.bars(), &cfg).unwrap();
        for period in 5..=8 {
            let series = cache.get(period).expect("period in range");
            assert!(
                series.iter().any(|v| v.is_finite()),
                "period {period} produced nothing but warmup"
            );
        }
    }

    #[test]
    fn evaluator_reads_the_thresholds() {
        let prices = OwnedBars::from_close(vec![100.0_f32; 3]);
        let cache = PeriodCache::<f32>::from_series(5, vec![vec![5.0, 50.0, 95.0]]);
        let evaluator = StochRsi::evaluator::<f32>(prices.bars(), &cache, (5, 20, 80));
        assert_eq!(evaluator(0), Signal::EnterLong);
        assert_eq!(evaluator(1), Signal::Hold);
        assert_eq!(evaluator(2), Signal::ExitLong);
    }

    #[test]
    fn config_from_reads_scalars_and_ranges() {
        let spec = ParamSpec::parse(["rsi_period=21", "stoch_period=10..12"]).unwrap();
        let cfg = StochRsi::config_from(&spec).unwrap();
        assert_eq!(cfg.rsi_period, 21);
        assert_eq!(cfg.stoch_period, 10..=12);
        assert_eq!(cfg.k_smooth, 3, "untouched default");
    }
}
