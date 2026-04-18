mod exchange;
mod precision;
mod ta_wrapper;
mod utils;

use anyhow::Context as _;
use chrono::prelude::*; // This crate provides easy-to-use date and time functions
use chrono::Utc;
use exchange::Level;
use precision::{BacktestFloat, Precision};
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExecutionModel {
    NextOpen,
}

impl std::fmt::Display for ExecutionModel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutionModel::NextOpen => f.write_str("next_open"),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct RunConfig {
    pair: &'static str,
    level: Level,
    threads: usize,
    fast_period_min: usize,
    slow_period_min: usize,
    max_period: usize,
    starting_capital: f32,
    fee_rate: f32,
    execution_model: ExecutionModel,
    precision: Precision,
    compare_precisions: bool,
    show_progress: bool,
    progress_step: usize,
    download_start: u64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct BacktestMetrics {
    final_value: f64,
    max_drawdown: f64,
    sharpe_ratio: f64,
}

#[derive(Clone, Copy, Debug)]
struct NumericBacktestConfig<T> {
    periods_per_year: usize,
    starting_capital: T,
    fee_rate: T,
    execution_model: ExecutionModel,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct SweepResult {
    metrics: BacktestMetrics,
    fast_period: usize,
    slow_period: usize,
}

#[derive(Clone, Copy, Debug)]
struct PrecisionRun {
    precision: Precision,
    best: SweepResult,
    duration: std::time::Duration,
}

fn default_download_start() -> u64 {
    chrono::NaiveDate::from_ymd_opt(2019, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_millis() as u64
}

fn default_run_config() -> RunConfig {
    RunConfig {
        pair: "BTC-USDT",
        level: Level::Hour4,
        threads: 12,
        fast_period_min: 5,
        slow_period_min: 6,
        max_period: 600,
        starting_capital: 1000.0,
        fee_rate: 0.0015,
        execution_model: ExecutionModel::NextOpen,
        precision: Precision::F32,
        compare_precisions: false,
        show_progress: true,
        progress_step: 10_000,
        download_start: default_download_start(),
    }
}

fn parse_env_bool(value: &str) -> anyhow::Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => anyhow::bail!("invalid boolean value '{value}'"),
    }
}

fn load_run_config() -> anyhow::Result<RunConfig> {
    let mut config = default_run_config();

    if let Ok(value) = std::env::var("BACKTEST_PRECISION") {
        config.precision = value.parse().map_err(anyhow::Error::msg)?;
    }
    if let Ok(value) = std::env::var("BACKTEST_COMPARE_PRECISIONS") {
        config.compare_precisions = parse_env_bool(&value)?;
    }
    if let Ok(value) = std::env::var("BACKTEST_SHOW_PROGRESS") {
        config.show_progress = parse_env_bool(&value)?;
    }

    Ok(config)
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let start = Instant::now();
    let config = load_run_config()?;

    let data_file = utils::data_file_path(config.pair, &config.level);
    if let Err(error) =
        utils::download_dump_k_lines_to_json(config.pair, config.level, config.download_start..)
            .await
    {
        if !data_file.is_file() {
            return Err(error).with_context(|| {
                format!(
                    "failed to download market data and no cached file is available at {}",
                    data_file.display()
                )
            });
        }

        eprintln!(
            "Download failed ({error:#}). Falling back to cached data at {}.",
            data_file.display()
        );
    }

    // Load data

    println!("Doing: {:} {:}", config.pair, config.level);

    let market = utils::load_data_file(config.pair, &config.level)?;

    if let Some(&first_timestamp) = market.timestamps.first() {
        let first_date = Utc
            .timestamp_millis_opt(i64::try_from(first_timestamp)?)
            .single()
            .with_context(|| format!("invalid first timestamp: {first_timestamp}"))?;
        println!("First timestamp : {}", first_date.format("%Y-%m-%d"));
    }
    if let Some(&last_timestamp) = market.timestamps.last() {
        let last_date = Utc
            .timestamp_millis_opt(i64::try_from(last_timestamp)?)
            .single()
            .with_context(|| format!("invalid last timestamp: {last_timestamp}"))?;
        println!("Last  timestamp : {}", last_date.format("%Y-%m-%d"));
    }

    // Calculate all indicators
    let pool = ThreadPoolBuilder::new()
        .num_threads(config.threads)
        .build()
        .unwrap();

    let selected_run = if config.compare_precisions {
        let f32_run = run_precision_sweep(&pool, &config, &market, Precision::F32, false)?;
        let f64_run = run_precision_sweep(&pool, &config, &market, Precision::F64, false)?;
        print_precision_comparison(f32_run, f64_run);

        if config.precision == Precision::F64 {
            f64_run
        } else {
            f32_run
        }
    } else {
        run_precision_sweep(
            &pool,
            &config,
            &market,
            config.precision,
            config.show_progress,
        )?
    };

    println!("Done");
    println!("Precision: {}", selected_run.precision);
    println!(
        "Best result: sharpe: {:.6}, max_dd: {:.4}, Period1: {}, Period2: {}",
        selected_run.best.metrics.sharpe_ratio,
        selected_run.best.metrics.max_drawdown,
        selected_run.best.fast_period,
        selected_run.best.slow_period
    );
    println!(
        "Final portfolio value: {:.3}$",
        selected_run.best.metrics.final_value
    );
    println!(
        "Sweep duration: {:.3}s",
        selected_run.duration.as_secs_f64()
    );

    let ohlcv_file = format!("{}_{}", config.pair, config.level);
    let precision = selected_run.precision.to_string();
    utils::write_to_file(
        &utils::results_file_path(config.pair, &config.level),
        &utils::ResultRow {
            ohlcv_file: &ohlcv_file,
            precision: &precision,
            duration_ms: selected_run.duration.as_secs_f64() * 1000.0,
            port_value: selected_run.best.metrics.final_value,
            max_dd: selected_run.best.metrics.max_drawdown,
            sharpe_ratio: selected_run.best.metrics.sharpe_ratio,
            period1: selected_run.best.fast_period,
            period2: selected_run.best.slow_period,
        },
    )?;

    let duration = start.elapsed();
    println!("Time elapsed: {:?}", duration);

    Ok(())
}

///////////////////////////////////////////////////////////////////////////////////////////////////////

fn periods_per_year(level: Level) -> usize {
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

///////////////////////////////////////////////////////////////////////////////////////////////////////

fn ema_parameter_pairs(
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

fn prefer_sweep_result(left: SweepResult, right: SweepResult) -> SweepResult {
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

fn execution_price<T: BacktestFloat>(
    execution_model: ExecutionModel,
    open_prices: &[T],
    close_prices: &[T],
    index: usize,
) -> T {
    match execution_model {
        ExecutionModel::NextOpen => open_prices
            .get(index)
            .copied()
            .unwrap_or_else(|| close_prices[index]),
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////////

fn generic_max_drawdown<T: BacktestFloat>(portfolio_values: &[T]) -> f64 {
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

fn generic_sharpe_ratio<T: BacktestFloat>(returns: &[T], periods_per_year: usize) -> f64 {
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
        (mean_return / annualized_std_dev).to_f64()
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

fn backtest_double_ema<T: BacktestFloat>(
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
    let mut in_position = false; // Flag to track if we are currently holding the asset

    let mut portfolio_values = vec![usdt]; // Track portfolio values over time
    let mut returns = vec![]; // To calculate the Sharpe Ratio
    for i in 1..close_prices.len() {
        let execution_price = execution_price(config.execution_model, open_prices, close_prices, i);
        let mark_price = close_prices[i];
        let previous_value = portfolio_values.last().copied().unwrap();
        let previous_fast = ema1[i - 1];
        let previous_slow = ema2[i - 1];

        if previous_fast.is_finite()
            && previous_slow.is_finite()
            && !in_position
            && previous_fast > previous_slow
        {
            asset_quantity = usdt / execution_price * (T::ONE - config.fee_rate);
            usdt = T::ZERO;
            in_position = true;
        } else if previous_fast.is_finite()
            && previous_slow.is_finite()
            && in_position
            && previous_fast < previous_slow
        {
            usdt = asset_quantity * execution_price * (T::ONE - config.fee_rate);
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
    }

    let final_value = portfolio_values.last().copied().unwrap().to_f64();
    let max_drawdown = generic_max_drawdown(&portfolio_values);
    let sharpe_ratio = generic_sharpe_ratio(&returns, config.periods_per_year);

    BacktestMetrics {
        final_value,
        max_drawdown,
        sharpe_ratio,
    }
}

fn run_precision_sweep(
    pool: &rayon::ThreadPool,
    config: &RunConfig,
    market: &utils::CandleSeries,
    precision: Precision,
    show_progress: bool,
) -> anyhow::Result<PrecisionRun> {
    let parameter_pairs = ema_parameter_pairs(
        config.fast_period_min,
        config.slow_period_min,
        config.max_period,
    );

    match precision {
        Precision::F32 => run_precision_sweep_impl(
            pool,
            config,
            &market.open_prices,
            &market.close_prices,
            precision,
            show_progress,
            &parameter_pairs,
        ),
        Precision::F64 => {
            let open_prices = market
                .open_prices
                .iter()
                .copied()
                .map(f64::from)
                .collect::<Vec<_>>();
            let close_prices = market
                .close_prices
                .iter()
                .copied()
                .map(f64::from)
                .collect::<Vec<_>>();
            run_precision_sweep_impl(
                pool,
                config,
                &open_prices,
                &close_prices,
                precision,
                show_progress,
                &parameter_pairs,
            )
        }
    }
}

fn run_precision_sweep_impl<T: BacktestFloat>(
    pool: &rayon::ThreadPool,
    config: &RunConfig,
    open_prices: &[T],
    close_prices: &[T],
    precision: Precision,
    show_progress: bool,
    parameter_pairs: &[(usize, usize)],
) -> anyhow::Result<PrecisionRun> {
    println!("Calculating all indicators for {}...", precision);
    let ema_store =
        ta_wrapper::EMAStore::<T>::new(close_prices, config.fast_period_min, config.max_period);
    let backtest_config = numeric_backtest_config::<T>(config);

    println!("Calculated all indicators for {}.", precision);
    if show_progress {
        println!(
            "Running all backtests on {} threads with {}...",
            config.threads, precision
        );
    }

    let total_iterations = parameter_pairs.len();
    let progress_counter = AtomicUsize::new(0);
    let start = Instant::now();
    let best = pool
        .install(|| {
            parameter_pairs
                .par_iter()
                .map(|&(fast_period, slow_period)| {
                    let metrics = backtest_double_ema(
                        open_prices,
                        close_prices,
                        ema_store.get_ema(fast_period),
                        ema_store.get_ema(slow_period),
                        backtest_config,
                    );

                    let count = progress_counter.fetch_add(1, Ordering::Relaxed) + 1;
                    if show_progress
                        && (count.is_multiple_of(config.progress_step) || count == total_iterations)
                    {
                        let percentage = (count as f32 / total_iterations as f32) * 100.0;
                        println!(
                            "  ...Progress: {:6}/{:6} iterations completed {:5.1}%.",
                            count, total_iterations, percentage
                        );
                    }

                    SweepResult {
                        metrics,
                        fast_period,
                        slow_period,
                    }
                })
                .reduce_with(prefer_sweep_result)
        })
        .with_context(|| "EMA parameter search space is empty")?;

    Ok(PrecisionRun {
        precision,
        best,
        duration: start.elapsed(),
    })
}

fn print_precision_comparison(f32_run: PrecisionRun, f64_run: PrecisionRun) {
    println!("Precision comparison on identical input candles:");
    println!(
        "  {} -> {:.3}s | sharpe {:.6} | final ${:.3} | max_dd {:.4}% | periods ({}, {})",
        f32_run.precision,
        f32_run.duration.as_secs_f64(),
        f32_run.best.metrics.sharpe_ratio,
        f32_run.best.metrics.final_value,
        f32_run.best.metrics.max_drawdown,
        f32_run.best.fast_period,
        f32_run.best.slow_period
    );
    println!(
        "  {} -> {:.3}s | sharpe {:.6} | final ${:.3} | max_dd {:.4}% | periods ({}, {})",
        f64_run.precision,
        f64_run.duration.as_secs_f64(),
        f64_run.best.metrics.sharpe_ratio,
        f64_run.best.metrics.final_value,
        f64_run.best.metrics.max_drawdown,
        f64_run.best.fast_period,
        f64_run.best.slow_period
    );
    println!(
        "  delta -> duration {:+.3}s | final {:+.6} | sharpe {:+.6}",
        f64_run.duration.as_secs_f64() - f32_run.duration.as_secs_f64(),
        f64_run.best.metrics.final_value - f32_run.best.metrics.final_value,
        f64_run.best.metrics.sharpe_ratio - f32_run.best.metrics.sharpe_ratio
    );
}

///////////////////////////////////////////////////////////////////////////////////////////////////////

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
    fn parse_env_bool_understands_common_values() {
        assert!(parse_env_bool("true").unwrap());
        assert!(parse_env_bool("1").unwrap());
        assert!(!parse_env_bool("false").unwrap());
        assert!(!parse_env_bool("0").unwrap());
    }
}
