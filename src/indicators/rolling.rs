//! Rolling-window statistics shared by several indicators.
//!
//! `rolling_min`/`rolling_max` use a monotonic deque, so they are O(n) rather
//! than O(n·period). That matters: Stochastic, Donchian and Williams %R all
//! rest on them, and a sweep evaluates them across hundreds of periods.

use super::{nan_series, valid_tail};
use std::collections::VecDeque;

/// Lowest value in each trailing window of `period` bars.
pub fn rolling_min(values: &[f64], period: usize) -> Vec<f64> {
    rolling_extreme(values, period, Extreme::Min)
}

/// Highest value in each trailing window of `period` bars.
pub fn rolling_max(values: &[f64], period: usize) -> Vec<f64> {
    rolling_extreme(values, period, Extreme::Max)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Extreme {
    Min,
    Max,
}

fn rolling_extreme(values: &[f64], period: usize, kind: Extreme) -> Vec<f64> {
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

    // Indices whose values are candidates for the window's extreme, kept in
    // monotonic order. The front is always the current answer.
    let mut candidates: VecDeque<usize> = VecDeque::with_capacity(period);
    for (offset, &value) in tail.iter().enumerate() {
        while candidates
            .front()
            .is_some_and(|&oldest| offset >= period && oldest + period <= offset)
        {
            candidates.pop_front();
        }
        while let Some(&last) = candidates.back() {
            let dominated = match kind {
                Extreme::Min => tail[last] >= value,
                Extreme::Max => tail[last] <= value,
            };
            if dominated {
                candidates.pop_back();
            } else {
                break;
            }
        }
        candidates.push_back(offset);
        if offset + 1 >= period {
            out[start + offset] = tail[candidates[0]];
        }
    }
    out
}

/// Population standard deviation over each trailing window — the convention
/// Bollinger Bands use. Carries running sums, so it is O(n).
pub fn rolling_stddev(values: &[f64], period: usize) -> Vec<f64> {
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
    let n = period as f64;
    let mut sum = 0.0;
    let mut sum_sq = 0.0;
    for (offset, &value) in tail.iter().enumerate() {
        sum += value;
        sum_sq += value * value;
        if offset >= period {
            let leaving = tail[offset - period];
            sum -= leaving;
            sum_sq -= leaving * leaving;
        }
        if offset + 1 >= period {
            let mean = sum / n;
            // Clamped at zero: with a constant window the subtraction can land
            // a few ulps below it, and a negative variance would produce NaN.
            out[start + offset] = (sum_sq / n - mean * mean).max(0.0).sqrt();
        }
    }
    out
}

/// Mean absolute deviation from the window's own mean — the denominator of CCI.
///
/// Unlike the statistics above this has no O(n) rolling form, so it costs
/// O(n · period). Fine for a fixed CCI period; deliberately *not* something to
/// sweep across hundreds of periods.
pub fn rolling_mean_abs_dev(values: &[f64], period: usize) -> Vec<f64> {
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
    let n = period as f64;
    for offset in period - 1..tail.len() {
        let window = &tail[offset + 1 - period..=offset];
        let mean = window.iter().sum::<f64>() / n;
        out[start + offset] = window.iter().map(|v| (v - mean).abs()).sum::<f64>() / n;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn warmup(values: &[f64]) -> usize {
        values.iter().take_while(|v| v.is_nan()).count()
    }

    #[test]
    fn rolling_min_and_max_track_the_window() {
        let values = [5.0, 1.0, 3.0, 9.0, 2.0, 8.0];
        let mins = rolling_min(&values, 3);
        let maxs = rolling_max(&values, 3);
        assert_eq!(warmup(&mins), 2);
        assert_eq!(&mins[2..], &[1.0, 1.0, 2.0, 2.0]);
        assert_eq!(&maxs[2..], &[5.0, 9.0, 9.0, 9.0]);
    }

    #[test]
    fn rolling_extremes_match_a_brute_force_scan() {
        // The deque bookkeeping is the easy thing to get subtly wrong, so
        // check it against the obvious implementation over noisy input.
        let values: Vec<f64> = (0..300)
            .map(|i| ((i as f64) * 1.7).sin() * 100.0 + ((i % 13) as f64))
            .collect();
        for period in [1, 2, 3, 7, 30, 299, 300] {
            let mins = rolling_min(&values, period);
            let maxs = rolling_max(&values, period);
            for index in period - 1..values.len() {
                let window = &values[index + 1 - period..=index];
                let want_min = window.iter().copied().fold(f64::INFINITY, f64::min);
                let want_max = window.iter().copied().fold(f64::NEG_INFINITY, f64::max);
                assert_eq!(mins[index], want_min, "min period={period} index={index}");
                assert_eq!(maxs[index], want_max, "max period={period} index={index}");
            }
        }
    }

    #[test]
    fn rolling_extremes_handle_degenerate_periods() {
        assert!(rolling_min(&[1.0, 2.0], 0).iter().all(|v| v.is_nan()));
        assert!(rolling_max(&[1.0, 2.0], 9).iter().all(|v| v.is_nan()));
        assert_eq!(rolling_min(&[1.0, 2.0], 1), vec![1.0, 2.0]);
    }

    #[test]
    fn rolling_stddev_of_a_constant_window_is_zero() {
        let out = rolling_stddev(&[4.0; 20], 5);
        assert!(
            out[4..].iter().all(|v| *v == 0.0),
            "a flat window has no deviation, and must not go negative: {out:?}"
        );
    }

    #[test]
    fn rolling_stddev_matches_a_hand_computation() {
        // Population sd of [2,4,4,4,5,5,7,9] is exactly 2.
        let values = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let out = rolling_stddev(&values, 8);
        assert!((out[7] - 2.0).abs() < 1e-12, "got {}", out[7]);
    }

    #[test]
    fn rolling_stddev_matches_a_brute_force_scan() {
        let values: Vec<f64> = (0..200).map(|i| ((i as f64) * 0.3).cos() * 30.0).collect();
        let period = 14;
        let fast = rolling_stddev(&values, period);
        for index in period - 1..values.len() {
            let window = &values[index + 1 - period..=index];
            let mean = window.iter().sum::<f64>() / period as f64;
            let want =
                (window.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / period as f64).sqrt();
            assert!(
                (fast[index] - want).abs() < 1e-9,
                "index {index}: {} vs {want}",
                fast[index]
            );
        }
    }

    #[test]
    fn mean_abs_dev_matches_a_hand_computation() {
        // Window [1,2,3,4]: mean 2.5, deviations 1.5,0.5,0.5,1.5 → mean 1.0.
        let out = rolling_mean_abs_dev(&[1.0, 2.0, 3.0, 4.0], 4);
        assert!((out[3] - 1.0).abs() < 1e-12, "got {}", out[3]);
    }
}
