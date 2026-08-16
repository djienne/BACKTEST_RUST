//! Technical indicators.
//!
//! # Conventions
//!
//! Every indicator returns a series the **same length as its input**, with
//! `NaN` in the leading positions where it is not yet defined. The engine
//! relies on this: `NaN > x` and `NaN < x` are both false, so a strategy
//! comparing indicator values emits `Hold` throughout the warmup without
//! needing an explicit guard.
//!
//! # Precision
//!
//! Indicator math runs in `f64` and the result is stored in the build's
//! precision `T`. Indicators are computed once per run while the sweep reads
//! them hundreds of thousands of times, so the arithmetic width is free and
//! the storage width is what matters — which is exactly what the `f32`/`f64`
//! features select. [`BarsF64`] does the widening once for the whole run.
//!
//! # Adding an indicator
//!
//! 1. Write one function in the matching submodule:
//!    `fn my_indicator(bars: &BarsF64, period: usize) -> Vec<f64>`, or
//!    `fn my_indicator(values: &[f64], period: usize) -> Vec<f64>` if it only
//!    needs one column. Prefix the undefined warmup with `NaN`.
//! 2. Add a test pinning it against hand-computed values.
//!
//! That is the whole contract — [`PeriodCache`] can then precompute it across
//! a whole period range for a sweep with no further plumbing.

pub mod ma;
pub mod momentum;
pub mod rolling;
pub mod trend;
pub mod volatility;

use crate::data::Bars;
use crate::precision::BacktestFloat;
use rayon::prelude::*;

/// Market columns widened to `f64` once per run, so indicators never repeat
/// the conversion per period.
#[derive(Debug, Clone, Default)]
pub struct BarsF64 {
    pub open: Vec<f64>,
    pub high: Vec<f64>,
    pub low: Vec<f64>,
    pub close: Vec<f64>,
    pub volume: Vec<f64>,
}

impl BarsF64 {
    pub fn from_bars<T: BacktestFloat>(bars: Bars<'_, T>) -> Self {
        let widen = |values: &[T]| values.iter().map(|v| v.to_f64()).collect();
        Self {
            open: widen(bars.open),
            high: widen(bars.high),
            low: widen(bars.low),
            close: widen(bars.close),
            volume: widen(bars.volume),
        }
    }

    pub fn len(&self) -> usize {
        self.close.len()
    }

    pub fn is_empty(&self) -> bool {
        self.close.is_empty()
    }

    /// Typical price `(high + low + close) / 3`, the input to CCI and friends.
    pub fn typical_price(&self) -> Vec<f64> {
        (0..self.len())
            .map(|i| (self.high[i] + self.low[i] + self.close[i]) / 3.0)
            .collect()
    }

    /// Midpoint `(high + low) / 2`, the centre line for Supertrend.
    pub fn median_price(&self) -> Vec<f64> {
        (0..self.len())
            .map(|i| (self.high[i] + self.low[i]) / 2.0)
            .collect()
    }
}

/// Index of the first non-`NaN` value, or `None` if there is none.
pub fn first_valid(values: &[f64]) -> Option<usize> {
    values.iter().position(|v| v.is_finite())
}

/// The finite tail of a `NaN`-prefixed series, with the offset it starts at.
///
/// Every indicator here begins by calling this, which is what makes them
/// composable: HMA is a WMA of a difference of WMAs, StochRSI is a stochastic
/// of an RSI, a MACD signal line is an EMA of a MACD line. Without it each
/// stage would see its input's warmup as data and propagate `NaN` forever.
///
/// The assumption is that a series is `NaN` up to some index and finite after
/// it — true of every indicator in this module.
pub fn valid_tail(values: &[f64]) -> Option<(usize, &[f64])> {
    first_valid(values).map(|start| (start, &values[start..]))
}

/// A series of `NaN` the same length as `values`, the starting point of every
/// indicator implementation here.
pub fn nan_series(len: usize) -> Vec<f64> {
    vec![f64::NAN; len]
}

/// One indicator precomputed across a contiguous range of periods.
///
/// Backing storage is a single flat buffer with stride `len` rather than a
/// `Vec<Vec<T>>`: a sweep over 600 periods is one allocation instead of 600,
/// and the sweep is memory-bandwidth-bound, so contiguity is worth having.
pub struct PeriodCache<T> {
    period_min: usize,
    period_max: usize,
    len: usize,
    buffer: Vec<T>,
}

impl<T: BacktestFloat> PeriodCache<T> {
    /// Compute `indicator` for every period in `period_min..=period_max`, in
    /// parallel, and store the results in the active precision.
    pub fn build<F>(bars: &BarsF64, period_min: usize, period_max: usize, indicator: F) -> Self
    where
        F: Fn(&BarsF64, usize) -> Vec<f64> + Sync,
    {
        let len = bars.len();
        let count = period_max.saturating_sub(period_min) + 1;
        if period_max < period_min || len == 0 {
            return Self {
                period_min,
                period_max: period_min.saturating_sub(1),
                len,
                buffer: Vec::new(),
            };
        }

        let mut buffer = vec![T::NAN; len * count];
        buffer
            .par_chunks_mut(len)
            .enumerate()
            .for_each(|(index, slot)| {
                let series = indicator(bars, period_min + index);
                debug_assert_eq!(series.len(), len, "indicator changed the series length");
                for (target, value) in slot.iter_mut().zip(series.iter()) {
                    *target = T::from_f64(*value);
                }
            });

        Self {
            period_min,
            period_max,
            len,
            buffer,
        }
    }

    /// The precomputed series for `period`, or `None` if it is outside the
    /// range supplied at construction.
    pub fn get(&self, period: usize) -> Option<&[T]> {
        if period < self.period_min || period > self.period_max {
            return None;
        }
        let start = (period - self.period_min) * self.len;
        self.buffer.get(start..start + self.len)
    }

    pub fn periods(&self) -> std::ops::RangeInclusive<usize> {
        self.period_min..=self.period_max
    }

    /// Build a cache directly from precomputed series, assigning periods
    /// `period_min, period_min + 1, ...` in order. Test-only: lets a unit test
    /// pin exact indicator values without going through the real calculation.
    #[cfg(test)]
    pub fn from_series(period_min: usize, series: Vec<Vec<T>>) -> Self {
        let len = series.first().map_or(0, Vec::len);
        assert!(
            series.iter().all(|s| s.len() == len),
            "all series must be the same length"
        );
        Self {
            period_min,
            period_max: period_min + series.len().saturating_sub(1),
            len,
            buffer: series.into_iter().flatten().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::OwnedBars;

    #[test]
    fn period_cache_returns_each_period_and_rejects_the_rest() {
        let owned = OwnedBars::from_close(vec![1.0_f32, 2.0, 3.0, 4.0]);
        let bars = BarsF64::from_bars(owned.bars());
        // A stand-in indicator: every value is the period, so the slice a
        // caller gets back identifies which period it came from.
        let cache =
            PeriodCache::<f32>::build(&bars, 2, 5, |b, period| vec![period as f64; b.len()]);

        assert!(cache.get(1).is_none(), "below the range");
        assert!(cache.get(6).is_none(), "above the range");
        for period in 2..=5 {
            let series = cache.get(period).expect("in range");
            assert_eq!(series, vec![period as f32; 4], "period {period}");
        }
        assert_eq!(cache.periods(), 2..=5);
    }

    #[test]
    fn period_cache_survives_an_empty_or_inverted_range() {
        let owned = OwnedBars::from_close(vec![1.0_f32, 2.0]);
        let bars = BarsF64::from_bars(owned.bars());
        let inverted = PeriodCache::<f32>::build(&bars, 10, 2, |b, _| vec![0.0; b.len()]);
        assert!(inverted.get(2).is_none());
        assert!(inverted.get(10).is_none());

        let empty = BarsF64::default();
        let no_bars = PeriodCache::<f32>::build(&empty, 1, 3, |b, _| vec![0.0; b.len()]);
        assert!(no_bars.get(1).is_none());
    }

    #[test]
    fn bars_f64_derives_typical_and_median_prices() {
        let owned = OwnedBars::ohlc(
            vec![1.0_f32, 1.0],
            vec![12.0, 22.0],
            vec![6.0, 8.0],
            vec![9.0, 12.0],
        );
        let bars = BarsF64::from_bars(owned.bars());
        assert_eq!(bars.typical_price(), vec![9.0, 14.0]);
        assert_eq!(bars.median_price(), vec![9.0, 15.0]);
    }

    #[test]
    fn first_valid_skips_a_nan_warmup() {
        assert_eq!(first_valid(&[f64::NAN, f64::NAN, 1.0, 2.0]), Some(2));
        assert_eq!(first_valid(&[1.0]), Some(0));
        assert_eq!(first_valid(&[f64::NAN]), None);
        assert_eq!(first_valid(&[]), None);
    }
}
