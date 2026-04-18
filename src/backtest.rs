use crate::data::CandleSeries;
use crate::exchange::Level;
use crate::metrics::{max_drawdown, sharpe_ratio};
use crate::precision::{BacktestFloat, Float, Precision, ACTIVE_PRECISION};
use crate::ta_wrapper;
use anyhow::Context;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::borrow::Cow;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionModel {
    NextOpen,
}

impl std::fmt::Display for ExecutionModel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutionModel::NextOpen => f.write_str("next_open"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RunConfig {
    /// Trading pair, e.g. "BTC-USDT". `Cow` so the default literal stays
    /// borrowed (no allocation) while CLI input lives as `Cow::Owned`.
    pub pair: Cow<'static, str>,
    pub level: Level,
    pub threads: usize,
    pub fast_period_min: usize,
    pub slow_period_min: usize,
    pub max_period: usize,
    pub starting_capital: f32,
    pub fee_rate: f32,
    pub execution_model: ExecutionModel,
    pub show_progress: bool,
    pub progress_step: usize,
    pub download_start: u64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BacktestMetrics {
    pub final_value: f64,
    pub max_drawdown: f64,
    pub sharpe_ratio: f64,
}

#[derive(Clone, Copy, Debug)]
pub struct NumericBacktestConfig<T> {
    pub periods_per_year: usize,
    pub starting_capital: T,
    pub fee_rate: T,
    pub execution_model: ExecutionModel,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SweepResult {
    pub metrics: BacktestMetrics,
    pub fast_period: usize,
    pub slow_period: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct PrecisionRun {
    pub precision: Precision,
    pub best: SweepResult,
    pub duration: Duration,
}

pub fn periods_per_year(level: Level) -> usize {
    match level {
        Level::Minute1 => 24 * 60 * 365,
        Level::Minute3 => 24 * 20 * 365,
        Level::Minute5 => 24 * 12 * 365,
        Level::Minute15 => 24 * 4 * 365,
        Level::Minute30 => 24 * 2 * 365,
        Level::Hour1 => 24 * 365,
        Level::Hour2 => 12 * 365,
        Level::Hour4 => 6 * 365,
        Level::Hour6 => 4 * 365,
        Level::Hour12 => 2 * 365,
        Level::Day1 => 365,
        Level::Day3 => 365 / 3,
        Level::Week1 => 52,
        Level::Month1 => 12,
    }
}

pub fn ema_parameter_pairs(
    fast_period_min: usize,
    slow_period_min: usize,
    max_period: usize,
) -> Vec<(usize, usize)> {
    let mut pairs = Vec::new();
    for fast_period in fast_period_min..=max_period {
        let slow_start = slow_period_min.max(fast_period + 1);
        for slow_period in slow_start..=max_period {
            pairs.push((fast_period, slow_period));
        }
    }
    pairs
}

pub fn prefer_sweep_result(left: SweepResult, right: SweepResult) -> SweepResult {
    let left_finite = left.metrics.sharpe_ratio.is_finite();
    let right_finite = right.metrics.sharpe_ratio.is_finite();
    match (left_finite, right_finite) {
        (true, false) => return left,
        (false, true) => return right,
        _ => {}
    }
    if right.metrics.sharpe_ratio > left.metrics.sharpe_ratio {
        right
    } else if right.metrics.sharpe_ratio < left.metrics.sharpe_ratio {
        left
    } else if right.metrics.final_value > left.metrics.final_value {
        right
    } else if right.metrics.final_value < left.metrics.final_value {
        left
    } else if (right.fast_period, right.slow_period) < (left.fast_period, left.slow_period) {
        right
    } else {
        left
    }
}

fn numeric_backtest_config<T: BacktestFloat>(config: &RunConfig) -> NumericBacktestConfig<T> {
    NumericBacktestConfig {
        periods_per_year: periods_per_year(config.level),
        starting_capital: T::from_f32(config.starting_capital),
        fee_rate: T::from_f32(config.fee_rate),
        execution_model: config.execution_model,
    }
}

pub fn backtest_double_ema<T: BacktestFloat>(
    open_prices: &[T],
    close_prices: &[T],
    ema1: &[T],
    ema2: &[T],
    config: NumericBacktestConfig<T>,
) -> BacktestMetrics {
    if close_prices.is_empty()
        || open_prices.len() != close_prices.len()
        || close_prices.len() != ema1.len()
        || close_prices.len() != ema2.len()
    {
        return BacktestMetrics {
            final_value: config.starting_capital.to_f64(),
            max_drawdown: 0.0,
            sharpe_ratio: 0.0,
        };
    }
    let mut usdt: T = config.starting_capital;
    let mut asset_quantity: T = T::ZERO;
    let mut in_position = false;
    let mut previous_value: T = usdt;

    let n = close_prices.len();
    let mut portfolio_values = Vec::with_capacity(n);
    portfolio_values.push(usdt);
    let mut returns = Vec::with_capacity(n - 1);

    // 4-way zip over the four input slices: trade/mark prices come from
    // [1..], EMA signals from [..n-1]. The slice creation is bounds-checked
    // once; the iterator chain lowers to four pointer-bumps with no
    // per-element check inside the hot loop.
    let trades = match config.execution_model {
        ExecutionModel::NextOpen => &open_prices[1..],
    };
    let marks = &close_prices[1..];
    let prev_fast = &ema1[..n - 1];
    let prev_slow = &ema2[..n - 1];

    for (((&trade_price, &mark_price), &previous_fast), &previous_slow) in trades
        .iter()
        .zip(marks.iter())
        .zip(prev_fast.iter())
        .zip(prev_slow.iter())
    {
        let signal_valid = previous_fast.is_finite() && previous_slow.is_finite();
        if signal_valid && !in_position && previous_fast > previous_slow {
            asset_quantity = usdt / trade_price * (T::ONE - config.fee_rate);
            usdt = T::ZERO;
            in_position = true;
        } else if signal_valid && in_position && previous_fast < previous_slow {
            usdt = asset_quantity * trade_price * (T::ONE - config.fee_rate);
            asset_quantity = T::ZERO;
            in_position = false;
        }

        let current_value = if in_position {
            asset_quantity * mark_price
        } else {
            usdt
        };
        portfolio_values.push(current_value);
        if previous_value > T::ZERO {
            returns.push((current_value - previous_value) / previous_value);
        }
        previous_value = current_value;
    }

    let final_value = portfolio_values.last().copied().unwrap().to_f64();
    let max_drawdown = max_drawdown(&portfolio_values);
    let sharpe = sharpe_ratio(&returns, T::ZERO, config.periods_per_year);

    let metrics = BacktestMetrics {
        final_value,
        max_drawdown,
        sharpe_ratio: sharpe,
    };
    debug_assert!(
        metrics.sharpe_ratio.is_finite(),
        "non-finite sharpe leaked from backtest"
    );
    metrics
}

fn run_precision_sweep_impl<T: BacktestFloat>(
    pool: &rayon::ThreadPool,
    config: &RunConfig,
    open_prices: &[T],
    close_prices: &[T],
    show_progress: bool,
    parameter_pairs: &[(usize, usize)],
) -> anyhow::Result<PrecisionRun> {
    println!("Calculating all indicators for {}...", ACTIVE_PRECISION);
    let ema_store =
        ta_wrapper::EMAStore::<T>::new(close_prices, config.fast_period_min, config.max_period);
    let backtest_config = numeric_backtest_config::<T>(config);

    println!("Calculated all indicators for {}.", ACTIVE_PRECISION);
    if show_progress {
        println!(
            "Running all backtests on {} threads with {}...",
            config.threads, ACTIVE_PRECISION
        );
    }

    let total_iterations = parameter_pairs.len();
    let progress_counter = AtomicUsize::new(0);
    let start = Instant::now();
    let best = pool
        .install(|| {
            parameter_pairs
                .par_iter()
                .fold(
                    || None::<SweepResult>,
                    |acc, &(fast_period, slow_period)| {
                        let fast_ema = ema_store.get_ema(fast_period).unwrap_or_else(|| {
                            panic!("EMA store missing period {fast_period}")
                        });
                        let slow_ema = ema_store.get_ema(slow_period).unwrap_or_else(|| {
                            panic!("EMA store missing period {slow_period}")
                        });
                        let metrics = backtest_double_ema(
                            open_prices,
                            close_prices,
                            fast_ema,
                            slow_ema,
                            backtest_config,
                        );

                        let count = progress_counter.fetch_add(1, Ordering::Relaxed) + 1;
                        if show_progress
                            && (count.is_multiple_of(config.progress_step)
                                || count == total_iterations)
                        {
                            let percentage =
                                (count as f32 / total_iterations as f32) * 100.0;
                            println!(
                                "  ...Progress: {:6}/{:6} iterations completed {:5.1}%.",
                                count, total_iterations, percentage
                            );
                        }

                        let candidate = SweepResult {
                            metrics,
                            fast_period,
                            slow_period,
                        };
                        Some(match acc {
                            Some(prev) => prefer_sweep_result(prev, candidate),
                            None => candidate,
                        })
                    },
                )
                .reduce(
                    || None::<SweepResult>,
                    |a, b| match (a, b) {
                        (Some(x), Some(y)) => Some(prefer_sweep_result(x, y)),
                        (Some(x), None) | (None, Some(x)) => Some(x),
                        (None, None) => None,
                    },
                )
        })
        .with_context(|| "EMA parameter search space is empty")?;

    Ok(PrecisionRun {
        precision: ACTIVE_PRECISION,
        best,
        duration: start.elapsed(),
    })
}

#[cfg(all(feature = "f32", not(feature = "f64")))]
fn to_active_floats(prices: &[f32]) -> Vec<Float> {
    prices.to_vec()
}

#[cfg(all(feature = "f64", not(feature = "f32")))]
fn to_active_floats(prices: &[f32]) -> Vec<Float> {
    prices.iter().copied().map(f64::from).collect()
}

pub fn run(config: &RunConfig, market: &CandleSeries) -> anyhow::Result<PrecisionRun> {
    let pool = ThreadPoolBuilder::new()
        .num_threads(config.threads)
        .build()
        .context("failed to construct rayon thread pool")?;

    let parameter_pairs = ema_parameter_pairs(
        config.fast_period_min,
        config.slow_period_min,
        config.max_period,
    );

    let open = to_active_floats(&market.open_prices);
    let close = to_active_floats(&market.close_prices);

    run_precision_sweep_impl::<Float>(
        &pool,
        config,
        &open,
        &close,
        config.show_progress,
        &parameter_pairs,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backtest_waits_for_the_next_bar_after_a_signal() {
        let open_prices = vec![100.0, 100.0, 200.0];
        let close_prices = vec![100.0, 100.0, 100.0];
        let ema_fast = vec![f32::NAN, 2.0, 2.0];
        let ema_slow = vec![f32::NAN, 1.0, 1.0];

        let metrics = backtest_double_ema(
            &open_prices,
            &close_prices,
            &ema_fast,
            &ema_slow,
            NumericBacktestConfig {
                periods_per_year: periods_per_year(Level::Hour4),
                starting_capital: 1000.0_f32,
                fee_rate: 0.0015_f32,
                execution_model: ExecutionModel::NextOpen,
            },
        );

        assert!((metrics.final_value - 499.25).abs() < 1e-3);
    }

    #[test]
    fn backtest_enters_then_exits_on_crossover_reversal() {
        // 6 bars, all open=close=100. EMAs cross above slow at i=2 (signal
        // from EMA[1]=(2,1)) → enter at open[2]. They cross below slow at
        // i=5 (signal from EMA[4]=(1,2)) → exit at open[5].
        let open_prices = vec![100.0_f32; 6];
        let close_prices = vec![100.0_f32; 6];
        let ema_fast = vec![f32::NAN, 2.0, 2.0, 2.0, 1.0, 1.0];
        let ema_slow = vec![f32::NAN, 1.0, 1.0, 1.0, 2.0, 2.0];

        let metrics = backtest_double_ema(
            &open_prices,
            &close_prices,
            &ema_fast,
            &ema_slow,
            NumericBacktestConfig {
                periods_per_year: periods_per_year(Level::Hour4),
                starting_capital: 1000.0_f32,
                fee_rate: 0.0015_f32,
                execution_model: ExecutionModel::NextOpen,
            },
        );

        // Round-trip cost: 1000 * (1 - fee)^2 = 1000 * 0.9985 * 0.9985.
        let expected = 1000.0_f64 * (1.0 - 0.0015_f64).powi(2);
        assert!(
            (metrics.final_value - expected).abs() < 1e-3,
            "final_value = {}, expected ≈ {}",
            metrics.final_value,
            expected
        );
    }

    #[test]
    fn backtest_supports_f64_precision() {
        let open_prices = vec![100.0_f64, 100.0, 200.0];
        let close_prices = vec![100.0_f64, 100.0, 100.0];
        let ema_fast = vec![f64::NAN, 2.0, 2.0];
        let ema_slow = vec![f64::NAN, 1.0, 1.0];

        let metrics = backtest_double_ema(
            &open_prices,
            &close_prices,
            &ema_fast,
            &ema_slow,
            NumericBacktestConfig {
                periods_per_year: periods_per_year(Level::Hour4),
                starting_capital: 1000.0_f64,
                fee_rate: 0.0015_f64,
                execution_model: ExecutionModel::NextOpen,
            },
        );

        assert!((metrics.final_value - 499.25).abs() < 1e-6);
    }

    #[test]
    fn periods_per_year_matches_the_selected_level() {
        assert_eq!(periods_per_year(Level::Hour4), 6 * 365);
        assert_eq!(periods_per_year(Level::Minute15), 24 * 4 * 365);
        assert_eq!(periods_per_year(Level::Month1), 12);
    }

    #[test]
    fn ema_parameter_pairs_match_the_expected_search_space() {
        assert_eq!(ema_parameter_pairs(5, 6, 7), vec![(5, 6), (5, 7), (6, 7)]);
    }

    #[test]
    fn prefer_sweep_result_uses_sharpe_then_final_value_then_periods() {
        let first = SweepResult {
            metrics: BacktestMetrics {
                final_value: 1000.0,
                max_drawdown: 10.0,
                sharpe_ratio: 1.0,
            },
            fast_period: 10,
            slow_period: 20,
        };
        let second = SweepResult {
            metrics: BacktestMetrics {
                final_value: 1010.0,
                max_drawdown: 12.0,
                sharpe_ratio: 1.0,
            },
            fast_period: 12,
            slow_period: 24,
        };
        assert_eq!(prefer_sweep_result(first, second), second);
    }

    #[test]
    fn prefer_sweep_result_prefers_finite_sharpe_over_nan() {
        let finite = SweepResult {
            metrics: BacktestMetrics {
                final_value: 100.0,
                max_drawdown: 0.0,
                sharpe_ratio: -10.0,
            },
            fast_period: 1,
            slow_period: 2,
        };
        let nan = SweepResult {
            metrics: BacktestMetrics {
                final_value: 100.0,
                max_drawdown: 0.0,
                sharpe_ratio: f64::NAN,
            },
            fast_period: 3,
            slow_period: 4,
        };
        assert_eq!(prefer_sweep_result(finite, nan), finite);
        assert_eq!(prefer_sweep_result(nan, finite), finite);
    }
}
