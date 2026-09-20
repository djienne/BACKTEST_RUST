//! Momentum and oscillator indicators.

use super::ma::{ema, rma, sma};
use super::rolling::{rolling_max, rolling_mean_abs_dev, rolling_min};
use super::{nan_series, BarsF64};

/// Value a bounded oscillator reports when its window has no range at all
/// (a perfectly flat `period` bars). The midpoint is the honest reading:
/// price is neither at the top nor the bottom of a range that does not exist.
const FLAT_WINDOW_MIDPOINT: f64 = 50.0;

/// Wilder's Relative Strength Index, in `[0, 100]`.
///
/// Uses [`rma`] for the average gain and loss, per Wilder's original
/// definition — an EMA of the same period gives visibly different numbers.
pub fn rsi(values: &[f64], period: usize) -> Vec<f64> {
    let len = values.len();
    let mut out = nan_series(len);
    if period == 0 || len < 2 {
        return out;
    }

    // Bar-to-bar gains and losses, both non-negative. Index 0 has no prior
    // bar, so it stays NaN and the averages start one bar later.
    let mut gains = nan_series(len);
    let mut losses = nan_series(len);
    for index in 1..len {
        let change = values[index] - values[index - 1];
        if change.is_finite() {
            gains[index] = change.max(0.0);
            losses[index] = (-change).max(0.0);
        }
    }

    let avg_gain = rma(&gains, period);
    let avg_loss = rma(&losses, period);
    for index in 0..len {
        let (gain, loss) = (avg_gain[index], avg_loss[index]);
        if !gain.is_finite() || !loss.is_finite() {
            continue;
        }
        out[index] = if loss == 0.0 {
            // No downside in the window at all: RSI saturates. (Both zero
            // means a flat window, which is conventionally reported as 100 by
            // the same formula's limit; callers treating >70 as overbought
            // should pair RSI with a volatility filter regardless.)
            100.0
        } else {
            let rs = gain / loss;
            100.0 - 100.0 / (1.0 + rs)
        };
    }
    out
}

/// A `%K` / `%D` oscillator pair.
#[derive(Debug, Clone, PartialEq)]
pub struct Oscillator {
    pub k: Vec<f64>,
    pub d: Vec<f64>,
}

/// Stochastic oscillator: where the close sits within the recent high/low
/// range. `%K` is smoothed by `k_smooth` bars, `%D` is an SMA of `%K`.
pub fn stochastic(bars: &BarsF64, period: usize, k_smooth: usize, d_smooth: usize) -> Oscillator {
    let highest = rolling_max(&bars.high, period);
    let lowest = rolling_min(&bars.low, period);
    let raw = position_in_range(&bars.close, &highest, &lowest);
    bounded_oscillator(&raw, k_smooth, d_smooth)
}

/// Stochastic RSI: the stochastic formula applied to the RSI series rather
/// than to price, which makes it far twitchier than either parent.
pub fn stochastic_rsi(
    values: &[f64],
    rsi_period: usize,
    stoch_period: usize,
    k_smooth: usize,
    d_smooth: usize,
) -> Oscillator {
    let rsi_values = rsi(values, rsi_period);
    let highest = rolling_max(&rsi_values, stoch_period);
    let lowest = rolling_min(&rsi_values, stoch_period);
    let raw = position_in_range(&rsi_values, &highest, &lowest);
    bounded_oscillator(&raw, k_smooth, d_smooth)
}

/// Smooth a `[0, 100]` series into a `%K` / `%D` pair, keeping both inside the
/// bound.
///
/// The clamp is not belt-and-braces: [`sma`] carries a rolling sum, and after
/// enough add/subtract cycles that sum can sit an ulp above the true total, so
/// an average of values that are all exactly 100 can come back as 100.000...4.
/// Callers are told the range is `[0, 100]`, so it has to actually be.
fn bounded_oscillator(raw: &[f64], k_smooth: usize, d_smooth: usize) -> Oscillator {
    let k = clamp_percent(&smooth_or_passthrough(raw, k_smooth));
    let d = clamp_percent(&smooth_or_passthrough(&k, d_smooth));
    Oscillator { k, d }
}

fn clamp_percent(values: &[f64]) -> Vec<f64> {
    values
        .iter()
        .map(|v| {
            if v.is_finite() {
                v.clamp(0.0, 100.0)
            } else {
                *v
            }
        })
        .collect()
}

/// `100 * (value - low) / (high - low)`, elementwise, guarding a zero range.
fn position_in_range(values: &[f64], highest: &[f64], lowest: &[f64]) -> Vec<f64> {
    let mut out = nan_series(values.len());
    for index in 0..values.len() {
        let (value, high, low) = (values[index], highest[index], lowest[index]);
        if !value.is_finite() || !high.is_finite() || !low.is_finite() {
            continue;
        }
        let range = high - low;
        out[index] = if range <= 0.0 {
            FLAT_WINDOW_MIDPOINT
        } else {
            // Clamped: when the value *is* the window's extreme, the division
            // can land a few ulps outside the range, and callers are entitled
            // to trust the documented [0, 100] bound.
            (100.0 * (value - low) / range).clamp(0.0, 100.0)
        };
    }
    out
}

/// SMA smoothing, or the series untouched when the smoothing length is 1 —
/// a 1-bar average is the identity, and skipping it avoids a wasted pass.
fn smooth_or_passthrough(values: &[f64], period: usize) -> Vec<f64> {
    if period <= 1 {
        values.to_vec()
    } else {
        sma(values, period)
    }
}

/// Moving Average Convergence Divergence: the gap between a fast and a slow
/// EMA, its own EMA (the signal line), and the difference between them.
#[derive(Debug, Clone, PartialEq)]
pub struct Macd {
    pub macd: Vec<f64>,
    pub signal: Vec<f64>,
    pub histogram: Vec<f64>,
}

pub fn macd(values: &[f64], fast: usize, slow: usize, signal_period: usize) -> Macd {
    let fast_ema = ema(values, fast);
    let slow_ema = ema(values, slow);
    let mut line = nan_series(values.len());
    for index in 0..values.len() {
        if fast_ema[index].is_finite() && slow_ema[index].is_finite() {
            line[index] = fast_ema[index] - slow_ema[index];
        }
    }
    let signal = ema(&line, signal_period);
    let mut histogram = nan_series(values.len());
    for index in 0..values.len() {
        if line[index].is_finite() && signal[index].is_finite() {
            histogram[index] = line[index] - signal[index];
        }
    }
    Macd {
        macd: line,
        signal,
        histogram,
    }
}

/// Rate of change, as a percentage of the value `period` bars ago.
pub fn roc(values: &[f64], period: usize) -> Vec<f64> {
    let mut out = nan_series(values.len());
    if period == 0 {
        return out;
    }
    for index in period..values.len() {
        let previous = values[index - period];
        if previous != 0.0 {
            out[index] = (values[index] - previous) / previous * 100.0;
        }
    }
    out
}

/// Commodity Channel Index.
///
/// The `0.015` scaling is Lambert's original constant, chosen so that roughly
/// 70–80% of readings land in `[-100, 100]`.
///
/// Cost is O(n · period) because the mean absolute deviation has no rolling
/// form — see [`rolling_mean_abs_dev`]. Suitable for a fixed period, not for a
/// wide sweep.
pub fn cci(bars: &BarsF64, period: usize) -> Vec<f64> {
    const LAMBERT: f64 = 0.015;
    let typical = bars.typical_price();
    let mean = sma(&typical, period);
    let deviation = rolling_mean_abs_dev(&typical, period);
    let mut out = nan_series(typical.len());
    for index in 0..typical.len() {
        if !mean[index].is_finite() || !deviation[index].is_finite() || deviation[index] == 0.0 {
            continue;
        }
        out[index] = (typical[index] - mean[index]) / (LAMBERT * deviation[index]);
    }
    out
}

/// Williams %R, in `[-100, 0]`: the mirror image of the stochastic `%K`.
pub fn williams_r(bars: &BarsF64, period: usize) -> Vec<f64> {
    let highest = rolling_max(&bars.high, period);
    let lowest = rolling_min(&bars.low, period);
    let mut out = nan_series(bars.len());
    for (index, slot) in out.iter_mut().enumerate() {
        let (high, low) = (highest[index], lowest[index]);
        if !high.is_finite() || !low.is_finite() {
            continue;
        }
        let range = high - low;
        *slot = if range <= 0.0 {
            -FLAT_WINDOW_MIDPOINT
        } else {
            (-100.0 * (high - bars.close[index]) / range).clamp(-100.0, 0.0)
        };
    }
    out
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
    fn rsi_saturates_at_100_for_an_unbroken_climb() {
        let values: Vec<f64> = (0..40).map(|i| 100.0 + i as f64).collect();
        let out = rsi(&values, 14);
        assert!((out[39] - 100.0).abs() < 1e-9, "got {}", out[39]);
    }

    #[test]
    fn rsi_bottoms_out_for_an_unbroken_fall() {
        let values: Vec<f64> = (0..40).map(|i| 100.0 - i as f64).collect();
        let out = rsi(&values, 14);
        assert!(out[39] < 1e-9, "got {}", out[39]);
    }

    #[test]
    fn rsi_of_a_symmetric_zigzag_settles_into_a_two_point_cycle() {
        // A +1/-1 alternation does *not* converge to 50: Wilder's averages
        // reach a limit cycle that swaps phase with the price, and RSI is read
        // at a point rather than averaged over the pair.
        //
        // With alpha = 1/p, let (G, L) be the averages just after a gain bar.
        // The next (loss) bar swaps their roles, so in steady state:
        //     G = L + (1 - L)/p        L = G * (p - 1)/p
        // which for p = 14 gives G = 14/27, L = 13/27, and therefore
        //     RSI after a gain bar = 100 * 14/27 = 51.851...
        //     RSI after a loss bar = 100 * 13/27 = 48.148...
        let values: Vec<f64> = (0..600)
            .map(|i| if i % 2 == 0 { 100.0 } else { 101.0 })
            .collect();
        let out = rsi(&values, 14);
        assert!(
            (out[599] - 1400.0 / 27.0).abs() < 1e-9,
            "gain bar: got {}, expected {}",
            out[599],
            1400.0 / 27.0
        );
        assert!(
            (out[598] - 1300.0 / 27.0).abs() < 1e-9,
            "loss bar: got {}, expected {}",
            out[598],
            1300.0 / 27.0
        );
        // The pair does straddle the midpoint, which is the intuition the
        // naive "it should be 50" reading is reaching for.
        assert!((0.5 * (out[598] + out[599]) - 50.0).abs() < 1e-9);
    }

    #[test]
    fn rsi_stays_inside_its_bounds_on_noisy_input() {
        let values: Vec<f64> = (0..500)
            .map(|i| 100.0 + ((i as f64) * 0.9).sin() * 20.0 + ((i % 7) as f64))
            .collect();
        let out = rsi(&values, 14);
        assert_eq!(warmup(&out), 14, "one differencing bar + Wilder's seed");
        for (index, value) in out.iter().enumerate().skip(14) {
            assert!(
                (0.0..=100.0).contains(value),
                "rsi[{index}] = {value} escaped [0, 100]"
            );
        }
    }

    #[test]
    fn rsi_handles_degenerate_input() {
        assert!(rsi(&[1.0, 2.0], 0).iter().all(|v| v.is_nan()));
        assert!(rsi(&[1.0], 14).iter().all(|v| v.is_nan()));
        assert!(rsi(&[], 14).is_empty());
    }

    #[test]
    fn rsi_skips_undefined_prefix_without_inventing_zero_changes() {
        let prices = [100.0, 90.0, 100.0, 90.0, 100.0, 110.0];
        let expected = rsi(&prices, 3);
        let mut prefixed = vec![f64::NAN; 2];
        prefixed.extend(prices);
        let actual = rsi(&prefixed, 3);
        assert!(actual[..5].iter().all(|v| v.is_nan()));
        for i in 3..prices.len() {
            assert_eq!(actual[i + 2], expected[i]);
        }
        assert!((actual[5] - 100.0 / 3.0).abs() < 1e-12);
    }

    #[test]
    fn stochastic_reports_position_within_the_range() {
        // Close at the very top of the window → 100; at the bottom → 0.
        let bars = bars_from(
            vec![10.0, 12.0, 14.0],
            vec![5.0, 6.0, 4.0],
            vec![9.0, 12.0, 4.0],
        );
        let out = stochastic(&bars, 3, 1, 1);
        // Window highs 14, lows 4 → close 4 sits at the bottom.
        assert!((out.k[2] - 0.0).abs() < 1e-9, "got {}", out.k[2]);
    }

    #[test]
    fn stochastic_reports_the_midpoint_for_a_flat_window() {
        let bars = bars_from(vec![5.0; 6], vec![5.0; 6], vec![5.0; 6]);
        let out = stochastic(&bars, 3, 1, 1);
        assert!(
            out.k[5] == FLAT_WINDOW_MIDPOINT,
            "a zero-width range must not divide by zero: {}",
            out.k[5]
        );
    }

    #[test]
    fn stochastic_stays_inside_its_bounds() {
        let highs: Vec<f32> = (0..200)
            .map(|i| 100.0 + ((i as f32) * 0.4).sin() * 10.0 + 2.0)
            .collect();
        let lows: Vec<f32> = (0..200)
            .map(|i| 100.0 + ((i as f32) * 0.4).sin() * 10.0 - 2.0)
            .collect();
        let closes: Vec<f32> = (0..200)
            .map(|i| 100.0 + ((i as f32) * 0.4).sin() * 10.0)
            .collect();
        let bars = bars_from(highs, lows, closes);
        let out = stochastic(&bars, 14, 3, 3);
        for value in out.k.iter().chain(out.d.iter()).filter(|v| v.is_finite()) {
            assert!((0.0..=100.0).contains(value), "escaped bounds: {value}");
        }
    }

    #[test]
    fn stochastic_rsi_stays_inside_its_bounds_and_reaches_both_ends() {
        let values: Vec<f64> = (0..400)
            .map(|i| 100.0 + ((i as f64) * 0.25).sin() * 15.0)
            .collect();
        let out = stochastic_rsi(&values, 14, 14, 3, 3);
        let finite: Vec<f64> = out.k.iter().copied().filter(|v| v.is_finite()).collect();
        assert!(!finite.is_empty(), "should produce values");
        for value in &finite {
            assert!((0.0..=100.0).contains(value), "escaped bounds: {value}");
        }
        // On a clean oscillation the RSI sweeps its whole recent range, so
        // StochRSI should touch both extremes rather than hovering mid-band.
        assert!(finite.iter().cloned().fold(f64::MAX, f64::min) < 5.0);
        assert!(finite.iter().cloned().fold(f64::MIN, f64::max) > 95.0);
    }

    #[test]
    fn macd_line_is_the_gap_between_its_emas_and_the_histogram_closes_the_loop() {
        let values: Vec<f64> = (0..200)
            .map(|i| 100.0 + ((i as f64) * 0.1).sin() * 8.0)
            .collect();
        let out = macd(&values, 12, 26, 9);
        let fast = ema(&values, 12);
        let slow = ema(&values, 26);
        for index in 0..values.len() {
            if out.macd[index].is_finite() {
                assert!((out.macd[index] - (fast[index] - slow[index])).abs() < 1e-12);
            }
            if out.histogram[index].is_finite() {
                assert!(
                    (out.histogram[index] - (out.macd[index] - out.signal[index])).abs() < 1e-12
                );
            }
        }
        assert!(out.signal.iter().any(|v| v.is_finite()), "signal defined");
    }

    #[test]
    fn roc_reports_percentage_change() {
        let values = [100.0, 110.0, 90.0];
        let out = roc(&values, 1);
        assert_eq!(warmup(&out), 1);
        assert!((out[1] - 10.0).abs() < 1e-12, "+10%");
        assert!(
            (out[2] - (-18.181818181818183)).abs() < 1e-9,
            "got {}",
            out[2]
        );
    }

    #[test]
    fn roc_skips_a_zero_denominator() {
        let out = roc(&[0.0, 5.0], 1);
        assert!(out[1].is_nan(), "no percentage change from zero");
    }

    #[test]
    fn cci_is_positive_above_the_mean_and_negative_below() {
        let rising: Vec<f32> = (0..60).map(|i| 100.0 + i as f32).collect();
        let bars = bars_from(rising.clone(), rising.clone(), rising.clone());
        let out = cci(&bars, 20);
        assert!(
            out[59] > 0.0,
            "a climb sits above its own mean: {}",
            out[59]
        );

        let falling: Vec<f32> = (0..60).map(|i| 100.0 - i as f32).collect();
        let bars = bars_from(falling.clone(), falling.clone(), falling.clone());
        let out = cci(&bars, 20);
        assert!(out[59] < 0.0, "and a fall below it: {}", out[59]);
    }

    #[test]
    fn williams_r_mirrors_the_stochastic() {
        let highs: Vec<f32> = (0..100)
            .map(|i| 100.0 + ((i as f32) * 0.3).sin() * 10.0 + 1.0)
            .collect();
        let lows: Vec<f32> = (0..100)
            .map(|i| 100.0 + ((i as f32) * 0.3).sin() * 10.0 - 1.0)
            .collect();
        let closes: Vec<f32> = (0..100)
            .map(|i| 100.0 + ((i as f32) * 0.3).sin() * 10.0)
            .collect();
        let bars = bars_from(highs, lows, closes);
        let stoch = stochastic(&bars, 14, 1, 1);
        let wr = williams_r(&bars, 14);
        for (index, (&percent_r, &percent_k)) in wr.iter().zip(stoch.k.iter()).enumerate() {
            if percent_r.is_finite() && percent_k.is_finite() {
                // %R = %K - 100, by construction.
                assert!(
                    (percent_r - (percent_k - 100.0)).abs() < 1e-9,
                    "index {index}: %R {percent_r} vs %K {percent_k}"
                );
            }
        }
    }

    #[test]
    fn williams_r_stays_within_its_bounds() {
        let highs: Vec<f32> = (0..80).map(|i| 50.0 + (i % 9) as f32).collect();
        let lows: Vec<f32> = (0..80).map(|i| 40.0 + (i % 5) as f32).collect();
        let closes: Vec<f32> = (0..80).map(|i| 45.0 + (i % 7) as f32).collect();
        let bars = bars_from(highs, lows, closes);
        for value in williams_r(&bars, 14).iter().filter(|v| v.is_finite()) {
            assert!((-100.0..=0.0).contains(value), "escaped bounds: {value}");
        }
    }
}
