use crate::precision::BacktestFloat;

pub fn max_drawdown<T: BacktestFloat>(portfolio_values: &[T]) -> f64 {
    if portfolio_values.is_empty() {
        return 0.0;
    }

    let mut max_drawdown = T::ZERO;
    let mut peak = portfolio_values[0];

    for &value in portfolio_values {
        if value > peak {
            peak = value;
        }
        let drawdown = if peak > T::ZERO {
            (peak - value) / peak
        } else {
            T::ZERO
        };
        if drawdown > max_drawdown {
            max_drawdown = drawdown;
        }
    }

    max_drawdown.to_f64() * 100.0
}

/// Annualized Sharpe ratio of a per-period return series.
///
/// `risk_free_rate` is a **per-period** rate, in the same units as `returns`.
/// Annualization scales the per-period ratio by `sqrt(periods_per_year)`.
pub fn sharpe_ratio<T: BacktestFloat>(
    returns: &[T],
    risk_free_rate: T,
    periods_per_year: usize,
) -> f64 {
    if returns.len() < 2 || periods_per_year == 0 {
        return 0.0;
    }

    let count = T::from_usize(returns.len());
    let mean_return = returns
        .iter()
        .copied()
        .fold(T::ZERO, |acc, value| acc + value)
        / count;
    // Sample variance (Bessel-corrected).
    let variance = returns
        .iter()
        .copied()
        .map(|value| {
            let delta = value - mean_return;
            delta * delta
        })
        .fold(T::ZERO, |acc, value| acc + value)
        / T::from_usize(returns.len() - 1);

    annualize_sharpe(
        (mean_return - risk_free_rate).to_f64(),
        variance.to_f64(),
        periods_per_year,
    )
}

/// Shared tail of every Sharpe computation: guard the degenerate cases, then
/// scale the per-period ratio to a yearly one. Split out so the streaming
/// accumulator in the sweep and the slice form above cannot drift apart.
pub fn annualize_sharpe(mean_excess_return: f64, variance: f64, periods_per_year: usize) -> f64 {
    let std_dev = variance.sqrt();
    if !std_dev.is_finite() || std_dev <= 0.0 || !mean_excess_return.is_finite() {
        return 0.0;
    }
    mean_excess_return / std_dev * (periods_per_year as f64).sqrt()
}

/// Streaming equity-curve statistics.
///
/// The sweep evaluates one of these per parameter tuple — hundreds of
/// thousands of times over hundreds of thousands of bars — so materializing
/// the equity curve and the return series just to reduce them to a handful of
/// scalars is the dominant allocation in the program. Everything here is a
/// running scalar instead.
///
/// Accumulation is in `f64` regardless of the build's price precision: these
/// are a few registers, not arrays, so the width costs nothing in memory
/// traffic, and it keeps an `f32` sweep from accumulating visible error in the
/// variance over a hundred thousand bars.
#[derive(Debug, Clone)]
pub struct EquityStats {
    peak: f64,
    max_drawdown: f64,
    prev_value: f64,
    last_value: f64,
    returns: usize,
    sum: f64,
    sum_sq: f64,
    downside_sum_sq: f64,
}

impl EquityStats {
    pub fn new(starting_capital: f64) -> Self {
        Self {
            peak: starting_capital,
            max_drawdown: 0.0,
            prev_value: starting_capital,
            last_value: starting_capital,
            returns: 0,
            sum: 0.0,
            sum_sq: 0.0,
            downside_sum_sq: 0.0,
        }
    }

    /// Record one bar's mark-to-market portfolio value.
    ///
    /// `downside_target` is the per-bar return below which a move counts
    /// against the Sortino ratio — normally the risk-free rate.
    pub fn push(&mut self, value: f64, downside_target: f64) {
        if value > self.peak {
            self.peak = value;
        }
        if self.peak > 0.0 {
            let drawdown = (self.peak - value) / self.peak;
            if drawdown > self.max_drawdown {
                self.max_drawdown = drawdown;
            }
        }
        // A zeroed portfolio has no meaningful return; skipping it matches the
        // slice-based path this replaced.
        if self.prev_value > 0.0 {
            let ret = (value - self.prev_value) / self.prev_value;
            self.returns += 1;
            self.sum += ret;
            self.sum_sq += ret * ret;
            let shortfall = (ret - downside_target).min(0.0);
            self.downside_sum_sq += shortfall * shortfall;
        }
        self.prev_value = value;
        self.last_value = value;
    }

    pub fn final_value(&self) -> f64 {
        self.last_value
    }

    /// Maximum peak-to-trough decline, as a percentage.
    pub fn max_drawdown_pct(&self) -> f64 {
        self.max_drawdown * 100.0
    }

    fn mean(&self) -> f64 {
        self.sum / self.returns as f64
    }

    /// Sample (Bessel-corrected) variance of the per-bar returns.
    ///
    /// Computed from the running sums rather than a second pass. In `f64`,
    /// with per-bar returns whose mean is orders of magnitude below their RMS,
    /// the cancellation in `sum_sq - sum^2/n` costs a handful of digits out of
    /// sixteen. The `max(0.0)` is a floor for the degenerate constant-return
    /// case, where that subtraction can land just below zero.
    fn variance(&self) -> f64 {
        if self.returns < 2 {
            return 0.0;
        }
        let n = self.returns as f64;
        ((self.sum_sq - self.sum * self.sum / n) / (n - 1.0)).max(0.0)
    }

    pub fn sharpe(&self, risk_free_rate: f64, periods_per_year: usize) -> f64 {
        if self.returns < 2 || periods_per_year == 0 {
            return 0.0;
        }
        annualize_sharpe(
            self.mean() - risk_free_rate,
            self.variance(),
            periods_per_year,
        )
    }

    /// Like Sharpe but penalising only downside deviation.
    ///
    /// A run with no bar below the target has zero downside deviation, which
    /// makes the ratio genuinely infinite. Reporting `inf` says that; reusing
    /// Sharpe's zero-variance guard would report `0` and make a flawless run
    /// look like the worst one in the sweep. A run that simply never traded
    /// also has zero downside, but its excess return is zero too, so it still
    /// reports `0`. Sortino is reported, never ranked on — `prefer` keys off
    /// Sharpe, so an infinity here cannot win a sweep.
    pub fn sortino(&self, risk_free_rate: f64, periods_per_year: usize) -> f64 {
        if self.returns < 2 || periods_per_year == 0 {
            return 0.0;
        }
        let mean_excess = self.mean() - risk_free_rate;
        let downside_variance = self.downside_sum_sq / self.returns as f64;
        if downside_variance <= 0.0 {
            return if mean_excess > 0.0 {
                f64::INFINITY
            } else {
                0.0
            };
        }
        annualize_sharpe(mean_excess, downside_variance, periods_per_year)
    }

    /// Compound annual growth rate, as a percentage.
    pub fn cagr_pct(&self, starting_capital: f64, bars: usize, periods_per_year: usize) -> f64 {
        if periods_per_year == 0 || bars == 0 || starting_capital <= 0.0 || self.last_value <= 0.0 {
            return 0.0;
        }
        let years = bars as f64 / periods_per_year as f64;
        if years <= 0.0 {
            return 0.0;
        }
        ((self.last_value / starting_capital).powf(1.0 / years) - 1.0) * 100.0
    }
}

/// Return-per-unit-of-drawdown. Both inputs are percentages, so the ratio is
/// unitless. Zero when there was no drawdown to divide by.
pub fn calmar_ratio(cagr_pct: f64, max_drawdown_pct: f64) -> f64 {
    if max_drawdown_pct <= 0.0 || !cagr_pct.is_finite() {
        return 0.0;
    }
    cagr_pct / max_drawdown_pct
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_drawdown_is_zero_for_monotonic_series() {
        assert_eq!(max_drawdown(&[1.0_f32, 2.0, 3.0]), 0.0);
    }

    #[test]
    fn max_drawdown_handles_dip_and_partial_recovery() {
        let values = [100.0_f32, 50.0, 75.0, 25.0];
        let dd = max_drawdown(&values);
        assert!((dd - 75.0).abs() < 1e-3);
    }

    #[test]
    fn max_drawdown_is_safe_for_empty_inputs() {
        assert_eq!(max_drawdown::<f32>(&[]), 0.0);
    }

    #[test]
    fn sharpe_is_zero_for_a_single_return() {
        assert_eq!(sharpe_ratio(&[0.05_f32], 0.0, 365), 0.0);
    }

    #[test]
    fn sharpe_is_zero_for_an_idle_portfolio() {
        // A portfolio that does not change produces exactly-zero returns each
        // bar — that is what the backtest emits when sitting in cash through a
        // flat price series. Variance collapses to exactly 0 and the guard in
        // `sharpe_ratio` kicks in. (Using a non-zero literal like 0.01 would
        // not work because 0.01 isn't exactly representable in binary float;
        // rounding noise dominates the variance.)
        let returns = vec![0.0_f64; 50];
        assert_eq!(sharpe_ratio(&returns, 0.0, 365), 0.0);
    }

    #[test]
    fn sharpe_matches_manual_calculation() {
        // Symmetric returns => mean = 0 => sharpe = 0.
        let r = [0.01_f64, -0.01, 0.01, -0.01];
        assert!(sharpe_ratio(&r, 0.0, 4).abs() < 1e-12);

        // Constant returns => std = 0 => sharpe = 0 (guard).
        let r2 = [0.01_f64; 4];
        assert_eq!(sharpe_ratio(&r2, 0.0, 4), 0.0);

        // Mixed: mean = 0.01, sample std = sqrt(4 * 0.0001 / 3) ~= 0.0115470.
        // annualized_std = std / sqrt(252) ~= 0.000727
        // sharpe = 0.01 / 0.000727 ~= 13.7477.
        let r3 = [0.02_f64, 0.0, 0.02, 0.0];
        let s = sharpe_ratio(&r3, 0.0, 252);
        assert!((s - 13.747727).abs() < 1e-2, "sharpe = {s}");
    }

    #[test]
    fn sharpe_subtracts_risk_free_rate() {
        let r = [0.02_f64, 0.0, 0.02, 0.0];
        let s_no_rf = sharpe_ratio(&r, 0.0, 252);
        let s_rf = sharpe_ratio(&r, 0.005, 252);
        assert!(s_no_rf > s_rf, "{s_no_rf} should exceed {s_rf}");
    }

    #[test]
    fn annualize_sharpe_scales_by_the_root_of_the_period_count() {
        // Doubling the periods per year multiplies the ratio by sqrt(2).
        let one = annualize_sharpe(0.01, 0.0001, 252);
        let two = annualize_sharpe(0.01, 0.0001, 504);
        assert!((two / one - 2.0_f64.sqrt()).abs() < 1e-12);
        assert!((one - 0.01 / 0.01 * 252.0_f64.sqrt()).abs() < 1e-12);
    }

    #[test]
    fn annualize_sharpe_guards_degenerate_inputs() {
        assert_eq!(annualize_sharpe(0.01, 0.0, 252), 0.0, "zero variance");
        assert_eq!(annualize_sharpe(0.01, f64::NAN, 252), 0.0);
        assert_eq!(annualize_sharpe(f64::NAN, 0.0001, 252), 0.0);
        assert_eq!(annualize_sharpe(0.01, -1.0, 252), 0.0, "negative variance");
    }

    /// Drive `EquityStats` over an equity curve and return it, alongside the
    /// return series the old slice-based path would have built.
    fn stats_over(curve: &[f64]) -> (EquityStats, Vec<f64>) {
        let mut stats = EquityStats::new(curve[0]);
        let mut returns = Vec::new();
        let mut prev = curve[0];
        for &value in &curve[1..] {
            stats.push(value, 0.0);
            if prev > 0.0 {
                returns.push((value - prev) / prev);
            }
            prev = value;
        }
        (stats, returns)
    }

    #[test]
    fn streaming_stats_agree_with_the_slice_implementations() {
        // A curve with a real peak, a real trough and a partial recovery.
        let curve: Vec<f64> = (0..500)
            .map(|i| {
                let t = i as f64 / 40.0;
                1000.0 * (1.0 + 0.30 * t.sin() + 0.002 * i as f64)
            })
            .collect();
        let (stats, returns) = stats_over(&curve);

        assert!(
            (stats.max_drawdown_pct() - max_drawdown(&curve)).abs() < 1e-9,
            "streaming drawdown {} vs slice {}",
            stats.max_drawdown_pct(),
            max_drawdown(&curve)
        );
        let slice_sharpe = sharpe_ratio(&returns, 0.0, 365);
        assert!(
            (stats.sharpe(0.0, 365) - slice_sharpe).abs() < 1e-9,
            "streaming sharpe {} vs slice {slice_sharpe}",
            stats.sharpe(0.0, 365)
        );
        assert_eq!(stats.final_value(), *curve.last().unwrap());
    }

    #[test]
    fn streaming_stats_handle_a_flat_curve() {
        let (stats, _) = stats_over(&[1000.0; 20]);
        assert_eq!(stats.max_drawdown_pct(), 0.0);
        assert_eq!(stats.sharpe(0.0, 365), 0.0, "zero variance is guarded");
        assert_eq!(stats.sortino(0.0, 365), 0.0);
        assert_eq!(stats.final_value(), 1000.0);
    }

    #[test]
    fn sortino_ignores_upside_volatility() {
        // Two curves reaching the same place; `gentle` never gives back more
        // than `harsh` does. Sortino must prefer the shallower drawdowns even
        // though both have similar total volatility.
        let gentle: Vec<f64> = (0..60)
            .map(|i| 1000.0 * 1.01_f64.powi(i) * if i % 3 == 0 { 0.995 } else { 1.0 })
            .collect();
        let harsh: Vec<f64> = (0..60)
            .map(|i| 1000.0 * 1.01_f64.powi(i) * if i % 3 == 0 { 0.95 } else { 1.0 })
            .collect();
        let (gentle_stats, _) = stats_over(&gentle);
        let (harsh_stats, _) = stats_over(&harsh);
        assert!(
            gentle_stats.sortino(0.0, 365) > harsh_stats.sortino(0.0, 365),
            "shallow dips {} should beat deep ones {}",
            gentle_stats.sortino(0.0, 365),
            harsh_stats.sortino(0.0, 365)
        );
    }

    #[test]
    fn sortino_is_infinite_only_when_a_profitable_run_never_dips() {
        // Growth by exactly 1.5x per bar, so every return is exactly 0.5 in
        // binary and the variance is exactly zero — no rounding noise to
        // muddy which branch is under test. (Kept to 20 bars so 1000 * 1.5^i
        // stays exactly representable.)
        let flawless: Vec<f64> = (0..20).map(|i| 1000.0 * 1.5_f64.powi(i)).collect();
        let (stats, _) = stats_over(&flawless);
        assert_eq!(stats.sortino(0.0, 365), f64::INFINITY, "no bar ever dipped");
        // Sharpe keeps the conservative guard, because it *is* the ranking key
        // and an infinity there would win every sweep outright.
        assert_eq!(stats.sharpe(0.0, 365), 0.0);

        // A strategy that never traded also has zero downside, but zero excess
        // return with it — that must not read as flawless.
        let (idle, _) = stats_over(&[1000.0; 40]);
        assert_eq!(idle.sortino(0.0, 365), 0.0);
    }

    #[test]
    fn cagr_recovers_a_known_annual_growth_rate() {
        // Exactly one year of daily bars, doubling over the year.
        let mut stats = EquityStats::new(1000.0);
        for i in 1..=365 {
            stats.push(1000.0 * 2.0_f64.powf(i as f64 / 365.0), 0.0);
        }
        assert!(
            (stats.cagr_pct(1000.0, 365, 365) - 100.0).abs() < 1e-6,
            "cagr = {}",
            stats.cagr_pct(1000.0, 365, 365)
        );
    }

    #[test]
    fn cagr_and_calmar_guard_degenerate_inputs() {
        let stats = EquityStats::new(1000.0);
        assert_eq!(stats.cagr_pct(1000.0, 0, 365), 0.0, "no bars");
        assert_eq!(stats.cagr_pct(0.0, 365, 365), 0.0, "no capital");
        assert_eq!(stats.cagr_pct(1000.0, 365, 0), 0.0, "no periods");
        assert_eq!(calmar_ratio(50.0, 0.0), 0.0, "no drawdown to divide by");
        assert_eq!(calmar_ratio(f64::NAN, 10.0), 0.0);
        assert!((calmar_ratio(50.0, 25.0) - 2.0).abs() < 1e-12);
    }

    #[test]
    fn streaming_stats_survive_a_wiped_out_portfolio() {
        let (stats, _) = stats_over(&[1000.0, 500.0, 0.0, 0.0, 0.0]);
        assert!((stats.max_drawdown_pct() - 100.0).abs() < 1e-9);
        assert_eq!(stats.final_value(), 0.0);
        assert!(stats.sharpe(0.0, 365).is_finite());
        assert_eq!(
            stats.cagr_pct(1000.0, 4, 365),
            0.0,
            "no positive final value"
        );
    }
}
