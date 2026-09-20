//! RSI mean reversion: buy oversold, sell overbought.
//!
//! Every parameter tuple with the same RSI period shares one RSI series, so
//! the whole sweep costs one [`PeriodCache`] over the period range.

use crate::data::Bars;
use crate::indicators::momentum::rsi;
use crate::indicators::{BarsF64, PeriodCache};
use crate::precision::BacktestFloat;
use crate::strategy::params::ParamSpec;
use crate::strategy::{ConfigurableStrategy, Signal, Strategy};
use std::cmp::Ordering;
use std::ops::RangeInclusive;

pub struct RsiReversion;

#[derive(Clone, Debug)]
pub struct RsiReversionConfig {
    pub period: RangeInclusive<usize>,
    pub oversold: RangeInclusive<usize>,
    pub overbought: RangeInclusive<usize>,
}

impl Default for RsiReversionConfig {
    fn default() -> Self {
        Self {
            period: 5..=30,
            oversold: 20..=40,
            overbought: 60..=80,
        }
    }
}

impl Strategy for RsiReversion {
    /// `(period, oversold, overbought)`.
    type Params = (usize, usize, usize);
    type Cache<T: BacktestFloat> = PeriodCache<T>;
    type Config = RsiReversionConfig;
    const NAME: &'static str = "rsi_reversion";

    fn build_cache<T: BacktestFloat>(
        bars: Bars<'_, T>,
        cfg: &Self::Config,
    ) -> anyhow::Result<Self::Cache<T>> {
        let source = BarsF64::from_bars(bars);
        Ok(PeriodCache::build(
            &source,
            *cfg.period.start(),
            *cfg.period.end(),
            |bars, period| rsi(&bars.close, period),
        ))
    }

    fn enumerate_params(cfg: &Self::Config) -> Vec<Self::Params> {
        let mut params = Vec::new();
        for period in cfg.period.clone() {
            for oversold in cfg.oversold.clone() {
                for overbought in cfg.overbought.clone() {
                    // A band that crosses over itself would emit both signals
                    // at once; skip rather than let the engine arbitrate.
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
            .unwrap_or_else(|| panic!("RSI cache missing period {period}"));
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

    fn param_summary((period, oversold, overbought): Self::Params, _cfg: &Self::Config) -> String {
        format!("period={period},oversold={oversold},overbought={overbought}")
    }

    fn tie_break(left: Self::Params, right: Self::Params) -> Ordering {
        left.cmp(&right)
    }
}

impl ConfigurableStrategy for RsiReversion {
    const DESCRIPTION: &'static str = "Buy when RSI is oversold, sell when overbought";
    const PARAMETERS: &'static [(&'static str, &'static str)] = &[
        ("period", "RSI period (default 5..30)"),
        ("oversold", "entry threshold (default 20..40)"),
        ("overbought", "exit threshold (default 60..80)"),
    ];

    fn config_from(spec: &ParamSpec) -> anyhow::Result<Self::Config> {
        spec.reject_unknown(&Self::parameter_names())?;
        let defaults = RsiReversionConfig::default();
        Ok(RsiReversionConfig {
            period: spec.range("period", defaults.period)?,
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
    fn enumerate_params_skips_inverted_bands() {
        let cfg = RsiReversionConfig {
            period: 5..=5,
            oversold: 30..=50,
            overbought: 40..=45,
        };
        let params = RsiReversion::enumerate_params(&cfg);
        assert!(
            params.iter().all(|(_, low, high)| high > low),
            "an overbought level below the oversold one is not a band"
        );
        assert!(!params.is_empty(), "the valid combinations must survive");
    }

    #[test]
    fn evaluator_enters_below_the_band_and_exits_above_it() {
        let prices = OwnedBars::from_close(vec![100.0_f32; 4]);
        let cache = PeriodCache::<f32>::from_series(2, vec![vec![f32::NAN, 15.0, 50.0, 85.0]]);
        let evaluator = RsiReversion::evaluator::<f32>(prices.bars(), &cache, (2, 30, 70));
        assert_eq!(evaluator(0), Signal::Hold, "warmup");
        assert_eq!(evaluator(1), Signal::EnterLong, "15 < 30");
        assert_eq!(evaluator(2), Signal::Hold, "inside the band");
        assert_eq!(evaluator(3), Signal::ExitLong, "85 > 70");
    }

    #[test]
    fn config_from_applies_defaults_and_overrides() {
        let cfg = RsiReversion::config_from(&ParamSpec::default()).unwrap();
        assert_eq!(cfg.period, 5..=30);

        let spec = ParamSpec::parse(["period=14", "oversold=25..35"]).unwrap();
        let cfg = RsiReversion::config_from(&spec).unwrap();
        assert_eq!(cfg.period, 14..=14);
        assert_eq!(cfg.oversold, 25..=35);
        assert_eq!(cfg.overbought, 60..=80, "untouched default");
    }

    #[test]
    fn config_from_rejects_an_unknown_parameter() {
        let spec = ParamSpec::parse(["oversld=30"]).unwrap();
        assert!(RsiReversion::config_from(&spec).is_err());
    }
}
