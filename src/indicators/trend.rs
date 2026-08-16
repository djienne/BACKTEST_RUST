//! Trend-strength and trend-following indicators.

use super::ma::rma;
use super::volatility::true_range;
use super::{nan_series, BarsF64};

/// Wilder's Directional Movement system.
///
/// `plus_di` and `minus_di` measure how much of the recent range was directional
/// up versus down; `adx` measures how *decisively* one leads the other, without
/// saying which. A high ADX with `minus_di` on top is a strong downtrend.
#[derive(Debug, Clone, PartialEq)]
pub struct DirectionalIndex {
    pub plus_di: Vec<f64>,
    pub minus_di: Vec<f64>,
    pub adx: Vec<f64>,
}

pub fn directional_index(bars: &BarsF64, period: usize) -> DirectionalIndex {
    let len = bars.len();
    let mut plus_di = nan_series(len);
    let mut minus_di = nan_series(len);
    if period == 0 || len < 2 {
        return DirectionalIndex {
            adx: nan_series(len),
            plus_di,
            minus_di,
        };
    }

    // Directional movement: only the larger of the two moves counts, and only
    // when it is positive — an inside bar contributes nothing either way.
    let mut plus_dm = nan_series(len);
    let mut minus_dm = nan_series(len);
    for index in 1..len {
        let up_move = bars.high[index] - bars.high[index - 1];
        let down_move = bars.low[index - 1] - bars.low[index];
        plus_dm[index] = if up_move > down_move && up_move > 0.0 {
            up_move
        } else {
            0.0
        };
        minus_dm[index] = if down_move > up_move && down_move > 0.0 {
            down_move
        } else {
            0.0
        };
    }

    // Drop bar 0's true range so all three series share one warmup.
    let mut range = true_range(bars);
    range[0] = f64::NAN;

    let smoothed_range = rma(&range, period);
    let smoothed_plus = rma(&plus_dm, period);
    let smoothed_minus = rma(&minus_dm, period);

    let mut dx = nan_series(len);
    for index in 0..len {
        let tr = smoothed_range[index];
        if !tr.is_finite() {
            // Still in warmup.
            continue;
        }
        // A market with literally no range carries no directional information.
        // Emitting zeros rather than leaving a NaN hole matters: the ADX below
        // is an RMA of this series, and a gap in the middle would poison every
        // value after it.
        let (plus, minus) = if tr <= 0.0 {
            (0.0, 0.0)
        } else {
            (
                100.0 * smoothed_plus[index] / tr,
                100.0 * smoothed_minus[index] / tr,
            )
        };
        if !plus.is_finite() || !minus.is_finite() {
            continue;
        }
        plus_di[index] = plus;
        minus_di[index] = minus;
        let total = plus + minus;
        dx[index] = if total > 0.0 {
            100.0 * (plus - minus).abs() / total
        } else {
            0.0
        };
    }

    DirectionalIndex {
        adx: rma(&dx, period),
        plus_di,
        minus_di,
    }
}

/// Parabolic SAR — a stop-and-reverse trailing level.
///
/// `step` is how fast the acceleration factor grows with each new extreme, and
/// `max_step` caps it. Wilder's defaults are 0.02 and 0.2.
pub fn parabolic_sar(bars: &BarsF64, step: f64, max_step: f64) -> Vec<f64> {
    let len = bars.len();
    let mut out = nan_series(len);
    if len < 2 || step <= 0.0 || max_step <= 0.0 {
        return out;
    }

    // Seed from the first two bars: rising highs mean start long.
    let mut rising = bars.close[1] >= bars.close[0];
    let mut extreme = if rising { bars.high[1] } else { bars.low[1] };
    let mut sar = if rising { bars.low[0] } else { bars.high[0] };
    let mut acceleration = step;
    out[1] = sar;

    // Indexed rather than iterated: each step reads two *previous* bars and
    // carries four pieces of state forward, which a zip would only obscure.
    #[allow(clippy::needless_range_loop)]
    for index in 2..len {
        sar += acceleration * (extreme - sar);

        // The SAR may never intrude into the last two bars' range, or it would
        // stop a position out on a bar that never actually traded through it.
        if rising {
            sar = sar.min(bars.low[index - 1]).min(bars.low[index - 2]);
        } else {
            sar = sar.max(bars.high[index - 1]).max(bars.high[index - 2]);
        }

        let reversed = if rising {
            bars.low[index] < sar
        } else {
            bars.high[index] > sar
        };

        if reversed {
            // Flip: the old extreme becomes the new stop, and acceleration
            // restarts.
            sar = extreme;
            rising = !rising;
            acceleration = step;
            extreme = if rising {
                bars.high[index]
            } else {
                bars.low[index]
            };
        } else {
            let new_extreme = if rising {
                bars.high[index].max(extreme)
            } else {
                bars.low[index].min(extreme)
            };
            if new_extreme != extreme {
                extreme = new_extreme;
                acceleration = (acceleration + step).min(max_step);
            }
        }
        out[index] = sar;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::OwnedBars;

    fn bars_from(high: Vec<f32>, low: Vec<f32>, close: Vec<f32>) -> BarsF64 {
        let owned = OwnedBars::ohlc(close.clone(), high, low, close);
        BarsF64::from_bars(owned.bars())
    }

    fn trending(len: usize, slope: f32) -> BarsF64 {
        let closes: Vec<f32> = (0..len).map(|i| 100.0 + slope * i as f32).collect();
        bars_from(
            closes.iter().map(|v| v + 1.0).collect(),
            closes.iter().map(|v| v - 1.0).collect(),
            closes,
        )
    }

    #[test]
    fn plus_di_leads_in_an_uptrend_and_minus_di_in_a_downtrend() {
        let up = directional_index(&trending(100, 1.0), 14);
        assert!(
            up.plus_di[99] > up.minus_di[99],
            "+DI {} should lead -DI {}",
            up.plus_di[99],
            up.minus_di[99]
        );

        let down = directional_index(&trending(100, -1.0), 14);
        assert!(
            down.minus_di[99] > down.plus_di[99],
            "-DI {} should lead +DI {}",
            down.minus_di[99],
            down.plus_di[99]
        );
    }

    #[test]
    fn adx_is_high_for_a_clean_trend_and_low_for_chop() {
        let trend = directional_index(&trending(200, 1.0), 14);

        // A sawtooth with no net direction.
        let closes: Vec<f32> = (0..200)
            .map(|i| 100.0 + if i % 2 == 0 { 0.0 } else { 1.0 })
            .collect();
        let chop = directional_index(
            &bars_from(
                closes.iter().map(|v| v + 1.0).collect(),
                closes.iter().map(|v| v - 1.0).collect(),
                closes,
            ),
            14,
        );

        assert!(
            trend.adx[199] > 50.0,
            "a clean trend should register: {}",
            trend.adx[199]
        );
        assert!(
            chop.adx[199] < trend.adx[199],
            "chop {} must score below trend {}",
            chop.adx[199],
            trend.adx[199]
        );
    }

    #[test]
    fn directional_values_stay_within_their_bounds() {
        let closes: Vec<f32> = (0..300)
            .map(|i| 100.0 + ((i as f32) * 0.3).sin() * 15.0)
            .collect();
        let bars = bars_from(
            closes.iter().map(|v| v + 2.0).collect(),
            closes.iter().map(|v| v - 2.0).collect(),
            closes,
        );
        let out = directional_index(&bars, 14);
        for series in [&out.plus_di, &out.minus_di, &out.adx] {
            for value in series.iter().filter(|v| v.is_finite()) {
                assert!((0.0..=100.0).contains(value), "escaped bounds: {value}");
            }
        }
    }

    #[test]
    fn directional_index_survives_degenerate_input() {
        let empty = BarsF64::default();
        assert!(directional_index(&empty, 14).adx.is_empty());
        let two = trending(2, 1.0);
        assert!(directional_index(&two, 0).adx.iter().all(|v| v.is_nan()));
    }

    #[test]
    fn parabolic_sar_trails_below_a_rise_and_above_a_fall() {
        let up = trending(60, 1.0);
        let sar = parabolic_sar(&up, 0.02, 0.2);
        assert!(
            sar[59] < up.close[59],
            "in an uptrend the stop trails below price: {} vs {}",
            sar[59],
            up.close[59]
        );

        let down = trending(60, -1.0);
        let sar = parabolic_sar(&down, 0.02, 0.2);
        assert!(
            sar[59] > down.close[59],
            "and above it in a downtrend: {} vs {}",
            sar[59],
            down.close[59]
        );
    }

    #[test]
    fn parabolic_sar_reverses_when_the_trend_does() {
        // Up for 40 bars then down for 40: the stop must end up on the other
        // side of price.
        let closes: Vec<f32> = (0..80)
            .map(|i| {
                if i < 40 {
                    100.0 + i as f32
                } else {
                    140.0 - (i - 40) as f32
                }
            })
            .collect();
        let bars = bars_from(
            closes.iter().map(|v| v + 1.0).collect(),
            closes.iter().map(|v| v - 1.0).collect(),
            closes.clone(),
        );
        let sar = parabolic_sar(&bars, 0.02, 0.2);
        assert!(sar[39] < closes[39] as f64, "still long at the peak");
        assert!(sar[79] > closes[79] as f64, "flipped short by the end");
    }

    #[test]
    fn parabolic_sar_survives_degenerate_input() {
        assert!(parabolic_sar(&BarsF64::default(), 0.02, 0.2).is_empty());
        let bars = trending(10, 1.0);
        assert!(parabolic_sar(&bars, 0.0, 0.2).iter().all(|v| v.is_nan()));
    }
}
