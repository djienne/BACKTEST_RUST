//! Volatility and channel indicators.

use super::ma::{ema, rma, sma};
use super::rolling::{rolling_max, rolling_min, rolling_stddev};
use super::{nan_series, BarsF64};

/// A three-line channel: an upper band, a centre line and a lower band.
#[derive(Debug, Clone, PartialEq)]
pub struct Channel {
    pub upper: Vec<f64>,
    pub middle: Vec<f64>,
    pub lower: Vec<f64>,
}

/// True range: the largest of the bar's own span, and its gap up or down from
/// the previous close. The gap terms are what make it larger than `high - low`
/// across an overnight jump.
pub fn true_range(bars: &BarsF64) -> Vec<f64> {
    let len = bars.len();
    let mut out = nan_series(len);
    if len == 0 {
        return out;
    }
    out[0] = bars.high[0] - bars.low[0];
    for (index, slot) in out.iter_mut().enumerate().skip(1) {
        let previous_close = bars.close[index - 1];
        let span = bars.high[index] - bars.low[index];
        let gap_up = (bars.high[index] - previous_close).abs();
        let gap_down = (bars.low[index] - previous_close).abs();
        *slot = span.max(gap_up).max(gap_down);
    }
    out
}

/// Average True Range — Wilder's smoothing of [`true_range`], never negative.
pub fn atr(bars: &BarsF64, period: usize) -> Vec<f64> {
    rma(&true_range(bars), period)
}

/// Bollinger Bands: an SMA with bands `deviations` population standard
/// deviations away.
pub fn bollinger(values: &[f64], period: usize, deviations: f64) -> Channel {
    let middle = sma(values, period);
    let sigma = rolling_stddev(values, period);
    let mut upper = nan_series(values.len());
    let mut lower = nan_series(values.len());
    for index in 0..values.len() {
        if middle[index].is_finite() && sigma[index].is_finite() {
            upper[index] = middle[index] + deviations * sigma[index];
            lower[index] = middle[index] - deviations * sigma[index];
        }
    }
    Channel {
        upper,
        middle,
        lower,
    }
}

/// Keltner Channels: an EMA centre with bands set by ATR rather than by
/// standard deviation, which makes them react to range rather than to
/// close-to-close dispersion.
pub fn keltner(bars: &BarsF64, ema_period: usize, atr_period: usize, multiplier: f64) -> Channel {
    let middle = ema(&bars.close, ema_period);
    let range = atr(bars, atr_period);
    let mut upper = nan_series(bars.len());
    let mut lower = nan_series(bars.len());
    for index in 0..bars.len() {
        if middle[index].is_finite() && range[index].is_finite() {
            upper[index] = middle[index] + multiplier * range[index];
            lower[index] = middle[index] - multiplier * range[index];
        }
    }
    Channel {
        upper,
        middle,
        lower,
    }
}

/// Donchian Channels: the highest high and lowest low of the last `period`
/// bars, with their midpoint.
pub fn donchian(bars: &BarsF64, period: usize) -> Channel {
    let upper = rolling_max(&bars.high, period);
    let lower = rolling_min(&bars.low, period);
    let mut middle = nan_series(bars.len());
    for index in 0..bars.len() {
        if upper[index].is_finite() && lower[index].is_finite() {
            middle[index] = (upper[index] + lower[index]) / 2.0;
        }
    }
    Channel {
        upper,
        middle,
        lower,
    }
}

/// Supertrend: an ATR-width band that flips sides when price closes through it.
#[derive(Debug, Clone, PartialEq)]
pub struct Supertrend {
    /// The active band — support while the trend is up, resistance while down.
    pub line: Vec<f64>,
    /// `+1` in an uptrend, `-1` in a downtrend, `NaN` during warmup.
    pub direction: Vec<f64>,
}

pub fn supertrend(bars: &BarsF64, period: usize, multiplier: f64) -> Supertrend {
    let len = bars.len();
    let mut line = nan_series(len);
    let mut direction = nan_series(len);
    let range = atr(bars, period);
    let midpoint = bars.median_price();

    let Some(start) = super::first_valid(&range) else {
        return Supertrend { line, direction };
    };

    // The bands ratchet: an upper band only moves down (and a lower band only
    // up) while the trend holds, which is what stops the line whipsawing on
    // every tick of volatility.
    let mut final_upper = midpoint[start] + multiplier * range[start];
    let mut final_lower = midpoint[start] - multiplier * range[start];
    let mut trend_up = bars.close[start] >= final_upper;
    line[start] = if trend_up { final_lower } else { final_upper };
    direction[start] = if trend_up { 1.0 } else { -1.0 };

    for index in start + 1..len {
        let basic_upper = midpoint[index] + multiplier * range[index];
        let basic_lower = midpoint[index] - multiplier * range[index];
        let previous_close = bars.close[index - 1];

        final_upper = if basic_upper < final_upper || previous_close > final_upper {
            basic_upper
        } else {
            final_upper
        };
        final_lower = if basic_lower > final_lower || previous_close < final_lower {
            basic_lower
        } else {
            final_lower
        };

        let close = bars.close[index];
        if trend_up && close < final_lower {
            trend_up = false;
        } else if !trend_up && close > final_upper {
            trend_up = true;
        }

        line[index] = if trend_up { final_lower } else { final_upper };
        direction[index] = if trend_up { 1.0 } else { -1.0 };
    }

    Supertrend { line, direction }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::OwnedBars;

    fn warmup(values: &[f64]) -> usize {
        values.iter().take_while(|v| v.is_nan()).count()
    }

    fn bars_from(high: Vec<f32>, low: Vec<f32>, close: Vec<f32>) -> BarsF64 {
        let owned = OwnedBars::ohlc(close.clone(), high, low, close);
        BarsF64::from_bars(owned.bars())
    }

    #[test]
    fn true_range_uses_the_gap_from_the_previous_close() {
        // Bar 1 gaps up hard: its own span is 2, but the gap from the prior
        // close of 10 is 10 — the gap has to win.
        let bars = bars_from(vec![11.0, 20.0], vec![9.0, 18.0], vec![10.0, 19.0]);
        let out = true_range(&bars);
        assert_eq!(out[0], 2.0, "first bar has no previous close: high - low");
        assert_eq!(out[1], 10.0, "high 20 vs previous close 10");
    }

    #[test]
    fn true_range_is_never_negative() {
        let highs: Vec<f32> = (0..100).map(|i| 100.0 + (i % 11) as f32).collect();
        let lows: Vec<f32> = (0..100).map(|i| 90.0 + (i % 7) as f32).collect();
        let closes: Vec<f32> = (0..100).map(|i| 95.0 + (i % 5) as f32).collect();
        let bars = bars_from(highs, lows, closes);
        assert!(true_range(&bars).iter().all(|v| *v >= 0.0));
    }

    #[test]
    fn atr_of_a_constant_range_equals_that_range() {
        // Every bar spans exactly 4 with no gaps, so the average is 4.
        let closes: Vec<f32> = vec![100.0; 40];
        let highs: Vec<f32> = vec![102.0; 40];
        let lows: Vec<f32> = vec![98.0; 40];
        let bars = bars_from(highs, lows, closes);
        let out = atr(&bars, 14);
        assert_eq!(warmup(&out), 13);
        assert!((out[39] - 4.0).abs() < 1e-9, "got {}", out[39]);
    }

    #[test]
    fn atr_is_never_negative() {
        let highs: Vec<f32> = (0..200)
            .map(|i| 100.0 + ((i as f32) * 0.5).sin() * 5.0 + 1.0)
            .collect();
        let lows: Vec<f32> = (0..200)
            .map(|i| 100.0 + ((i as f32) * 0.5).sin() * 5.0 - 1.0)
            .collect();
        let closes: Vec<f32> = (0..200)
            .map(|i| 100.0 + ((i as f32) * 0.5).sin() * 5.0)
            .collect();
        let bars = bars_from(highs, lows, closes);
        assert!(atr(&bars, 14)
            .iter()
            .filter(|v| v.is_finite())
            .all(|v| *v >= 0.0));
    }

    #[test]
    fn bollinger_middle_band_is_the_sma_and_bands_are_symmetric() {
        let values: Vec<f64> = (0..80)
            .map(|i| 100.0 + ((i as f64) * 0.4).sin() * 10.0)
            .collect();
        let out = bollinger(&values, 20, 2.0);
        let reference = sma(&values, 20);
        for (index, &middle) in out.middle.iter().enumerate() {
            if middle.is_finite() {
                assert!((middle - reference[index]).abs() < 1e-12);
                let above = out.upper[index] - middle;
                let below = middle - out.lower[index];
                assert!((above - below).abs() < 1e-9, "bands must be symmetric");
                assert!(above >= 0.0, "upper band cannot sit below the middle");
            }
        }
    }

    #[test]
    fn bollinger_bands_collapse_onto_a_flat_series() {
        let out = bollinger(&[50.0; 40], 20, 2.0);
        assert!((out.upper[39] - 50.0).abs() < 1e-12);
        assert!((out.lower[39] - 50.0).abs() < 1e-12);
    }

    #[test]
    fn keltner_bands_widen_with_the_true_range() {
        let closes: Vec<f32> = vec![100.0; 60];
        let narrow = bars_from(vec![101.0; 60], vec![99.0; 60], closes.clone());
        let wide = bars_from(vec![110.0; 60], vec![90.0; 60], closes);
        let narrow_width = {
            let c = keltner(&narrow, 20, 10, 2.0);
            c.upper[59] - c.lower[59]
        };
        let wide_width = {
            let c = keltner(&wide, 20, 10, 2.0);
            c.upper[59] - c.lower[59]
        };
        assert!(
            wide_width > narrow_width * 5.0,
            "{wide_width} should dwarf {narrow_width}"
        );
    }

    #[test]
    fn donchian_brackets_the_recent_range() {
        let bars = bars_from(
            vec![10.0, 12.0, 11.0, 15.0],
            vec![5.0, 6.0, 3.0, 7.0],
            vec![8.0, 9.0, 7.0, 12.0],
        );
        let out = donchian(&bars, 3);
        assert_eq!(warmup(&out.upper), 2);
        assert_eq!(out.upper[2], 12.0);
        assert_eq!(out.lower[2], 3.0);
        assert_eq!(out.middle[2], 7.5);
        assert_eq!(out.upper[3], 15.0);
        assert_eq!(out.lower[3], 3.0);
    }

    #[test]
    fn supertrend_follows_a_sustained_move_in_each_direction() {
        let rising: Vec<f32> = (0..80).map(|i| 100.0 + i as f32).collect();
        let bars = bars_from(
            rising.iter().map(|v| v + 1.0).collect(),
            rising.iter().map(|v| v - 1.0).collect(),
            rising.clone(),
        );
        let up = supertrend(&bars, 10, 3.0);
        assert_eq!(up.direction[79], 1.0, "a steady climb is an uptrend");
        assert!(
            up.line[79] < rising[79] as f64,
            "the line trails below price in an uptrend"
        );

        let falling: Vec<f32> = (0..80).map(|i| 200.0 - i as f32).collect();
        let bars = bars_from(
            falling.iter().map(|v| v + 1.0).collect(),
            falling.iter().map(|v| v - 1.0).collect(),
            falling.clone(),
        );
        let down = supertrend(&bars, 10, 3.0);
        assert_eq!(down.direction[79], -1.0, "and a steady fall a downtrend");
        assert!(
            down.line[79] > falling[79] as f64,
            "the line sits above price in a downtrend"
        );
    }

    #[test]
    fn supertrend_direction_is_only_ever_plus_or_minus_one() {
        let closes: Vec<f32> = (0..300)
            .map(|i| 100.0 + ((i as f32) * 0.2).sin() * 20.0)
            .collect();
        let bars = bars_from(
            closes.iter().map(|v| v + 2.0).collect(),
            closes.iter().map(|v| v - 2.0).collect(),
            closes,
        );
        let out = supertrend(&bars, 14, 3.0);
        for value in out.direction.iter().filter(|v| v.is_finite()) {
            assert!(*value == 1.0 || *value == -1.0, "got {value}");
        }
    }

    #[test]
    fn volatility_indicators_survive_empty_input() {
        let bars = BarsF64::default();
        assert!(true_range(&bars).is_empty());
        assert!(atr(&bars, 14).is_empty());
        assert!(donchian(&bars, 14).upper.is_empty());
        assert!(supertrend(&bars, 14, 3.0).line.is_empty());
        assert!(bollinger(&[], 20, 2.0).middle.is_empty());
    }
}
