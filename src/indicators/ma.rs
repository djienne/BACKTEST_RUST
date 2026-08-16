//! Moving averages.
//!
//! All of these are O(n) in the series length regardless of period: the
//! windowed ones carry a running sum, the recursive ones are one multiply-add
//! per bar. That is what lets a sweep precompute several hundred periods
//! without the cost growing quadratically.

use super::{nan_series, valid_tail};

/// Simple moving average. Defined `period - 1` bars after the input's own
/// warmup ends.
pub fn sma(values: &[f64], period: usize) -> Vec<f64> {
    let mut out = nan_series(values.len());
    if period == 0 {
        return out;
    }
    let Some((start, tail)) = valid_tail(values) else {
        return out;
    };
    if tail.len() < period {
        return out;
    }
    let mut sum = 0.0;
    for (offset, &value) in tail.iter().enumerate() {
        sum += value;
        if offset >= period {
            sum -= tail[offset - period];
        }
        if offset + 1 >= period {
            out[start + offset] = sum / period as f64;
        }
    }
    out
}

/// Exponential moving average, `alpha = 2 / (period + 1)`, seeded with the
/// first value of the series.
///
/// A leading `NaN` run is skipped rather than propagated, so an EMA can be
/// taken of another indicator's output (a MACD signal line, a smoothed
/// stochastic) without the warmup swallowing the whole series.
pub fn ema(values: &[f64], period: usize) -> Vec<f64> {
    let mut out = nan_series(values.len());
    if period == 0 {
        return out;
    }
    let Some((start, tail)) = valid_tail(values) else {
        return out;
    };
    let alpha = 2.0 / (period as f64 + 1.0);
    let mut current = tail[0];
    for (offset, &value) in tail.iter().enumerate() {
        if offset > 0 {
            current = alpha * value + (1.0 - alpha) * current;
        }
        if offset + 1 >= period {
            out[start + offset] = current;
        }
    }
    out
}

/// Wilder's smoothing (`alpha = 1 / period`), seeded with the SMA of the first
/// window. This is the average behind RSI, ATR and ADX — it is *not* the same
/// as an EMA of the same period, and using one for the other is the classic
/// way to get indicator values that disagree with every charting package.
pub fn rma(values: &[f64], period: usize) -> Vec<f64> {
    let mut out = nan_series(values.len());
    if period == 0 {
        return out;
    }
    let Some((start, tail)) = valid_tail(values) else {
        return out;
    };
    if tail.len() < period {
        return out;
    }

    let mut current = tail[..period].iter().sum::<f64>() / period as f64;
    out[start + period - 1] = current;
    for (offset, &value) in tail.iter().enumerate().skip(period) {
        current += (value - current) / period as f64;
        out[start + offset] = current;
    }
    out
}

/// Linearly weighted moving average: the most recent bar carries weight
/// `period`, the oldest weight 1.
pub fn wma(values: &[f64], period: usize) -> Vec<f64> {
    let mut out = nan_series(values.len());
    if period == 0 {
        return out;
    }
    let Some((start, tail)) = valid_tail(values) else {
        return out;
    };
    if tail.len() < period {
        return out;
    }
    let denominator = (period * (period + 1) / 2) as f64;

    // Seed the first window explicitly, then maintain it with the standard
    // recurrence: numerator += period * new - (rolling sum before the push).
    let mut numerator: f64 = tail[..period]
        .iter()
        .enumerate()
        .map(|(i, v)| (i + 1) as f64 * v)
        .sum();
    let mut window_sum: f64 = tail[..period].iter().sum();
    out[start + period - 1] = numerator / denominator;

    for offset in period..tail.len() {
        let entering = tail[offset];
        let leaving = tail[offset - period];
        numerator += period as f64 * entering - window_sum;
        window_sum += entering - leaving;
        out[start + offset] = numerator / denominator;
    }
    out
}

/// Double EMA: `2 * ema - ema(ema)`. Reacts faster than an EMA of the same
/// period at the cost of more overshoot.
pub fn dema(values: &[f64], period: usize) -> Vec<f64> {
    let first = ema(values, period);
    let second = ema(&first, period);
    combine(&[(2.0, &first), (-1.0, &second)])
}

/// Triple EMA: `3 * ema - 3 * ema(ema) + ema(ema(ema))`.
pub fn tema(values: &[f64], period: usize) -> Vec<f64> {
    let first = ema(values, period);
    let second = ema(&first, period);
    let third = ema(&second, period);
    combine(&[(3.0, &first), (-3.0, &second), (1.0, &third)])
}

/// Hull moving average: `wma(2 * wma(n/2) - wma(n), sqrt(n))`. Much smoother
/// than a WMA of the same period with far less lag.
pub fn hma(values: &[f64], period: usize) -> Vec<f64> {
    if period < 2 {
        return nan_series(values.len());
    }
    let half = (period / 2).max(1);
    let root = (period as f64).sqrt().round().max(1.0) as usize;
    let fast = wma(values, half);
    let slow = wma(values, period);
    let raw = combine(&[(2.0, &fast), (-1.0, &slow)]);
    wma(&raw, root)
}

/// Weighted sum of aligned series. Any position where a contributor is `NaN`
/// stays `NaN`, which keeps the composite's warmup equal to the longest
/// warmup among its parts.
fn combine(terms: &[(f64, &Vec<f64>)]) -> Vec<f64> {
    let len = terms.first().map_or(0, |(_, series)| series.len());
    let mut out = nan_series(len);
    for index in 0..len {
        let mut total = 0.0;
        let mut defined = true;
        for (weight, series) in terms {
            let value = series[index];
            if !value.is_finite() {
                defined = false;
                break;
            }
            total += weight * value;
        }
        if defined {
            out[index] = total;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Count of leading `NaN`s — the indicator's warmup.
    fn warmup(values: &[f64]) -> usize {
        values.iter().take_while(|v| v.is_nan()).count()
    }

    fn assert_close(got: f64, want: f64, what: &str) {
        assert!((got - want).abs() < 1e-9, "{what}: got {got}, want {want}");
    }

    #[test]
    fn sma_averages_the_trailing_window() {
        let values = [1.0, 2.0, 3.0, 4.0, 5.0];
        let out = sma(&values, 3);
        assert_eq!(warmup(&out), 2, "defined from index period-1");
        assert_close(out[2], 2.0, "(1+2+3)/3");
        assert_close(out[3], 3.0, "(2+3+4)/3");
        assert_close(out[4], 4.0, "(3+4+5)/3");
    }

    #[test]
    fn sma_of_a_constant_series_is_that_constant() {
        let out = sma(&[7.0; 50], 10);
        assert!(out[10..].iter().all(|v| (v - 7.0).abs() < 1e-12));
    }

    #[test]
    fn sma_handles_degenerate_periods() {
        assert!(sma(&[1.0, 2.0], 0).iter().all(|v| v.is_nan()));
        assert!(sma(&[1.0, 2.0], 5).iter().all(|v| v.is_nan()));
        assert_eq!(sma(&[1.0, 2.0], 1), vec![1.0, 2.0], "period 1 is identity");
    }

    #[test]
    fn ema_period_one_matches_the_input() {
        assert_eq!(ema(&[1.0, 2.0, 3.0], 1), vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn ema_matches_a_hand_computed_recurrence() {
        // alpha = 2/(3+1) = 0.5, seeded at 10.
        let values = [10.0, 20.0, 30.0];
        let out = ema(&values, 3);
        assert_eq!(warmup(&out), 2);
        // e1 = 0.5*20 + 0.5*10 = 15; e2 = 0.5*30 + 0.5*15 = 22.5
        assert_close(out[2], 22.5, "ema[2]");
    }

    #[test]
    fn ema_starts_after_a_nan_warmup_instead_of_propagating_it() {
        let values = [f64::NAN, f64::NAN, 10.0, 20.0, 30.0];
        let out = ema(&values, 3);
        assert_eq!(warmup(&out), 4, "two NaN + two bars of its own warmup");
        assert_close(out[4], 22.5, "same recurrence, shifted");
    }

    #[test]
    fn rma_seeds_with_the_first_window_average() {
        // Wilder: seed = mean of first 3 = 2, then += (x - prev)/3.
        let values = [1.0, 2.0, 3.0, 7.0];
        let out = rma(&values, 3);
        assert_eq!(warmup(&out), 2);
        assert_close(out[2], 2.0, "seed");
        assert_close(out[3], 2.0 + (7.0 - 2.0) / 3.0, "one smoothing step");
    }

    #[test]
    fn rma_and_ema_of_the_same_period_differ() {
        // The distinction that trips people up: Wilder's period-14 average is
        // an EMA with alpha 1/14, not 2/15.
        let values: Vec<f64> = (1..=60).map(|i| i as f64).collect();
        let wilder = rma(&values, 14);
        let exponential = ema(&values, 14);
        assert!((wilder[59] - exponential[59]).abs() > 1.0);
    }

    #[test]
    fn wma_weights_the_most_recent_bar_hardest() {
        // (1*1 + 2*2 + 3*3) / 6 = 14/6
        let out = wma(&[1.0, 2.0, 3.0, 4.0], 3);
        assert_eq!(warmup(&out), 2);
        assert_close(out[2], 14.0 / 6.0, "first window");
        // (1*2 + 2*3 + 3*4) / 6 = 20/6
        assert_close(out[3], 20.0 / 6.0, "recurrence keeps the weighting");
    }

    #[test]
    fn wma_recurrence_matches_a_direct_computation() {
        let values: Vec<f64> = (0..80)
            .map(|i| ((i as f64) * 0.7).sin() * 50.0 + 100.0)
            .collect();
        let period = 9;
        let fast = wma(&values, period);
        for index in period - 1..values.len() {
            let direct: f64 = values[index + 1 - period..=index]
                .iter()
                .enumerate()
                .map(|(i, v)| (i + 1) as f64 * v)
                .sum::<f64>()
                / (period * (period + 1) / 2) as f64;
            assert!(
                (fast[index] - direct).abs() < 1e-9,
                "index {index}: recurrence {} vs direct {direct}",
                fast[index]
            );
        }
    }

    #[test]
    fn dema_and_tema_lead_a_plain_ema_on_a_trend() {
        // On a steady rise every average lags the price; the multi-stage ones
        // must lag less. Long enough for the nested EMAs to settle — while
        // they are still converging from their seed, TEMA can overshoot.
        let values: Vec<f64> = (0..400).map(|i| 100.0 + i as f64).collect();
        let last = values.len() - 1;
        let price = values[last];
        let plain = ema(&values, 20)[last];
        let double = dema(&values, 20)[last];
        let triple = tema(&values, 20)[last];
        assert!(double > plain, "dema {double} should lead ema {plain}");
        assert!(triple > double, "tema {triple} should lead dema {double}");
        // Both are built to have no lag on a linear trend, so once settled
        // they sit on the price itself.
        assert!(
            (triple - price).abs() < 1e-6,
            "tema {triple} should converge to the price {price}"
        );
        assert!(plain < price, "a plain EMA still lags: {plain} vs {price}");
    }

    #[test]
    fn hma_lags_a_trend_far_less_than_the_averages_it_is_built_from() {
        // The Hull average's selling point. On a ramp of slope `s`, an SMA(n)
        // lags s(n-1)/2 and a WMA(n) lags s(n-1)/3; the Hull construction
        // cancels most of that, leaving a fraction of a bar rather than zero —
        // for n = 16, s = 2 the residual is 4/3.
        let slope = 2.0;
        let values: Vec<f64> = (0..120).map(|i| 10.0 + slope * i as f64).collect();
        let last = values.len() - 1;
        let price = values[last];

        let hull_lag = price - hma(&values, 16)[last];
        let weighted_lag = price - wma(&values, 16)[last];
        let simple_lag = price - sma(&values, 16)[last];

        assert!(
            (hull_lag - 4.0 / 3.0).abs() < 1e-9,
            "hull lag {hull_lag}, expected 4/3"
        );
        assert!((weighted_lag - slope * 5.0).abs() < 1e-9, "wma lag");
        assert!((simple_lag - slope * 7.5).abs() < 1e-9, "sma lag");
        assert!(
            hull_lag < weighted_lag && weighted_lag < simple_lag,
            "hull {hull_lag} < wma {weighted_lag} < sma {simple_lag}"
        );
    }

    #[test]
    fn every_average_returns_the_input_length() {
        let values: Vec<f64> = (0..40).map(|i| i as f64).collect();
        for out in [
            sma(&values, 5),
            ema(&values, 5),
            rma(&values, 5),
            wma(&values, 5),
            dema(&values, 5),
            tema(&values, 5),
            hma(&values, 5),
        ] {
            assert_eq!(out.len(), values.len());
        }
    }

    #[test]
    fn averages_survive_empty_input() {
        for out in [
            sma(&[], 5),
            ema(&[], 5),
            rma(&[], 5),
            wma(&[], 5),
            dema(&[], 5),
            tema(&[], 5),
            hma(&[], 5),
        ] {
            assert!(out.is_empty());
        }
    }
}
