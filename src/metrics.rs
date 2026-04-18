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
    let variance = returns
        .iter()
        .copied()
        .map(|value| {
            let delta = value - mean_return;
            delta * delta
        })
        .fold(T::ZERO, |acc, value| acc + value)
        / T::from_usize(returns.len() - 1);
    let annualized_std_dev = variance.sqrt() / T::from_usize(periods_per_year).sqrt();

    if !annualized_std_dev.is_finite() || annualized_std_dev <= T::ZERO {
        0.0
    } else {
        ((mean_return - risk_free_rate) / annualized_std_dev).to_f64()
    }
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
}
