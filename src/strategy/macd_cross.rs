//! MACD signal-line crossover: long while the histogram is positive.
//!
//! The histogram depends on all three periods at once — the signal line is an
//! EMA of the fast/slow difference — so like Supertrend this needs one series
//! per parameter tuple. The default grid is deliberately narrow; widen it with
//! `--param` and the [`SeriesCache`] budget will tell you when it is too much.

use crate::data::Bars;
use crate::indicators::momentum::macd;
use crate::indicators::{BarsF64, SeriesCache};
use crate::precision::BacktestFloat;
use crate::strategy::params::ParamSpec;
use crate::strategy::{ConfigurableStrategy, Signal, Strategy};
use std::cmp::Ordering;
use std::ops::RangeInclusive;

pub struct MacdCross;

#[derive(Clone, Debug)]
pub struct MacdConfig {
    pub fast: RangeInclusive<usize>,
    pub slow: RangeInclusive<usize>,
    pub signal: RangeInclusive<usize>,
}

impl Default for MacdConfig {
    fn default() -> Self {
        Self {
            fast: 8..=16,
            slow: 20..=32,
            signal: 7..=12,
        }
    }
}

impl MacdConfig {
    /// The parameter grid in cache order. Combinations where the "fast" period
    /// is not actually faster are dropped: they invert the indicator's meaning
    /// rather than exploring it.
    fn grid(&self) -> Vec<(usize, usize, usize)> {
        let mut grid = Vec::new();
        for fast in self.fast.clone() {
            for slow in self.slow.clone() {
                if slow <= fast {
                    continue;
                }
                for signal in self.signal.clone() {
                    grid.push((fast, slow, signal));
                }
            }
        }
        grid
    }
}

impl Strategy for MacdCross {
    /// `(index into the cache, fast, slow, signal)` — see
    /// [`crate::strategy::supertrend`] for why the index rides along.
    type Params = (usize, usize, usize, usize);
    type Cache<T: BacktestFloat> = SeriesCache<T>;
    type Config = MacdConfig;
    const NAME: &'static str = "macd_cross";

    fn build_cache<T: BacktestFloat>(
        bars: Bars<'_, T>,
        cfg: &Self::Config,
    ) -> anyhow::Result<Self::Cache<T>> {
        let source = BarsF64::from_bars(bars);
        let grid = cfg.grid();
        SeriesCache::build(&source, &grid, |bars, (fast, slow, signal)| {
            macd(&bars.close, fast, slow, signal).histogram
        })
    }

    fn enumerate_params(cfg: &Self::Config) -> Vec<Self::Params> {
        cfg.grid()
            .into_iter()
            .enumerate()
            .map(|(index, (fast, slow, signal))| (index, fast, slow, signal))
            .collect()
    }

    fn evaluator<'a, T: BacktestFloat>(
        _bars: Bars<'a, T>,
        cache: &'a Self::Cache<T>,
        params: Self::Params,
    ) -> impl Fn(usize) -> Signal + 'a {
        let (index, ..) = params;
        let histogram = cache
            .get(index)
            .unwrap_or_else(|| panic!("MACD cache missing entry {index}"));
        move |bar_index| {
            let value = histogram[bar_index];
            if value > T::ZERO {
                Signal::EnterLong
            } else if value < T::ZERO {
                Signal::ExitLong
            } else {
                Signal::Hold
            }
        }
    }

    fn param_summary((_, fast, slow, signal): Self::Params) -> String {
        format!("fast={fast},slow={slow},signal={signal}")
    }

    fn tie_break(left: Self::Params, right: Self::Params) -> Ordering {
        (left.1, left.2, left.3).cmp(&(right.1, right.2, right.3))
    }
}

impl ConfigurableStrategy for MacdCross {
    const DESCRIPTION: &'static str = "Hold while the MACD histogram is above zero";
    const PARAMETERS: &'static [(&'static str, &'static str)] = &[
        ("fast", "fast EMA period (default 8..16)"),
        ("slow", "slow EMA period (default 20..32)"),
        ("signal", "signal EMA period (default 7..12)"),
    ];

    fn config_from(spec: &ParamSpec) -> anyhow::Result<Self::Config> {
        spec.reject_unknown(&Self::parameter_names())?;
        let defaults = MacdConfig::default();
        Ok(MacdConfig {
            fast: spec.range("fast", defaults.fast)?,
            slow: spec.range("slow", defaults.slow)?,
            signal: spec.range("signal", defaults.signal)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::OwnedBars;

    #[test]
    fn grid_drops_combinations_where_fast_is_not_faster() {
        let cfg = MacdConfig {
            fast: 10..=30,
            slow: 12..=20,
            signal: 9..=9,
        };
        let grid = cfg.grid();
        assert!(!grid.is_empty());
        assert!(
            grid.iter().all(|(fast, slow, _)| slow > fast),
            "a slow period at or below the fast one inverts the indicator"
        );
    }

    #[test]
    fn enumerate_params_indexes_match_the_cache_layout() {
        let cfg = MacdConfig {
            fast: 8..=9,
            slow: 20..=21,
            signal: 7..=8,
        };
        let grid = cfg.grid();
        for (index, fast, slow, signal) in MacdCross::enumerate_params(&cfg) {
            assert_eq!(grid[index], (fast, slow, signal));
        }
    }

    #[test]
    fn evaluator_follows_the_sign_of_the_histogram() {
        let prices = OwnedBars::from_close(vec![100.0_f32; 3]);
        let cache = SeriesCache::<f32>::build(
            &crate::indicators::BarsF64::from_bars(prices.bars()),
            &[0usize],
            |_, _| vec![f64::NAN, 0.5, -0.5],
        )
        .unwrap();
        let evaluator = MacdCross::evaluator::<f32>(prices.bars(), &cache, (0, 12, 26, 9));
        assert_eq!(evaluator(0), Signal::Hold, "warmup");
        assert_eq!(evaluator(1), Signal::EnterLong);
        assert_eq!(evaluator(2), Signal::ExitLong);
    }

    #[test]
    fn config_from_reads_ranges() {
        let spec = ParamSpec::parse(["fast=12", "slow=26", "signal=9"]).unwrap();
        let cfg = MacdCross::config_from(&spec).unwrap();
        assert_eq!(MacdCross::enumerate_params(&cfg).len(), 1, "fully pinned");
    }
}
