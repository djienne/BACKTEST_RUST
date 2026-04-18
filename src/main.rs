mod exchange;
mod ta_wrapper;
mod utils;

use anyhow::Context as _;
use chrono::prelude::*; // This crate provides easy-to-use date and time functions
use chrono::Utc;
use exchange::Level;
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
    progress_step: usize,
    download_start: u64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct BacktestMetrics {
    final_value: f32,
    max_drawdown: f32,
    sharpe_ratio: f32,
}

#[derive(Clone, Copy, Debug)]
struct BacktestConfig {
    periods_per_year: usize,
    starting_capital: f32,
    fee_rate: f32,
    execution_model: ExecutionModel,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct SweepResult {
    metrics: BacktestMetrics,
    fast_period: usize,
    slow_period: usize,
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
        progress_step: 10_000,
        download_start: default_download_start(),
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let start = Instant::now();
    let config = default_run_config();

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
    println!("Calculating all indicators...");

    let ema_store = ta_wrapper::EMAStore::new(
        &market.close_prices,
        config.fast_period_min,
        config.max_period,
    );
    let backtest_config = BacktestConfig {
        periods_per_year: periods_per_year(config.level),
        starting_capital: config.starting_capital,
        fee_rate: config.fee_rate,
        execution_model: config.execution_model,
    };
    let parameter_pairs = ema_parameter_pairs(
        config.fast_period_min,
        config.slow_period_min,
        config.max_period,
    );

    println!("Calculated all indicators.");

    // Run backtests

    println!("Running all backtests on {} threads...", config.threads);

    // Configure and use a custom thread pool
    let pool = ThreadPoolBuilder::new()
        .num_threads(config.threads)
        .build()
        .unwrap();

    let total_iterations = parameter_pairs.len();
    let progress_counter = AtomicUsize::new(0);
    let best = pool
        .install(|| {
            parameter_pairs
                .par_iter()
                .map(|&(fast_period, slow_period)| {
                    let metrics = backtest_double_ema(
                        &market.open_prices,
                        &market.close_prices,
                        ema_store.get_ema(fast_period),
                        ema_store.get_ema(slow_period),
                        backtest_config,
                    );

                    let count = progress_counter.fetch_add(1, Ordering::Relaxed) + 1;
                    if count.is_multiple_of(config.progress_step) || count == total_iterations {
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

    println!("Done");
    println!(
        "Best result: sharpe: {:.3}, max_dd: {:.2}, Period1: {}, Period2: {}",
        best.metrics.sharpe_ratio, best.metrics.max_drawdown, best.fast_period, best.slow_period
    );
    println!("Final portfolio value: {:.1}$", best.metrics.final_value);

    utils::write_to_file(
        &utils::results_file_path(config.pair, &config.level),
        format!("{}_{}", config.pair, config.level).as_str(),
        best.metrics.final_value,
        best.metrics.max_drawdown,
        best.metrics.sharpe_ratio,
        best.fast_period,
        best.slow_period,
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

fn execution_price(
    execution_model: ExecutionModel,
    open_prices: &[f32],
    close_prices: &[f32],
    index: usize,
) -> f32 {
    match execution_model {
        ExecutionModel::NextOpen => open_prices
            .get(index)
            .copied()
            .unwrap_or_else(|| close_prices[index]),
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////////

fn backtest_double_ema(
    open_prices: &[f32],
    close_prices: &[f32],
    ema1: &[f32],
    ema2: &[f32],
    config: BacktestConfig,
) -> BacktestMetrics {
    if close_prices.is_empty()
        || open_prices.len() != close_prices.len()
        || close_prices.len() != ema1.len()
        || close_prices.len() != ema2.len()
    {
        return BacktestMetrics {
            final_value: config.starting_capital,
            max_drawdown: 0.0,
            sharpe_ratio: 0.0,
        };
    }
    let mut usdt: f32 = config.starting_capital;
    let mut asset_quantity: f32 = 0.0; // Quantity of the asset we own
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
            asset_quantity = usdt / execution_price * (1.0 - config.fee_rate);
            usdt = 0.0;
            in_position = true;
        } else if previous_fast.is_finite()
            && previous_slow.is_finite()
            && in_position
            && previous_fast < previous_slow
        {
            usdt = asset_quantity * execution_price * (1.0 - config.fee_rate);
            asset_quantity = 0.0;
            in_position = false;
        }

        let current_value = if in_position {
            asset_quantity * mark_price
        } else {
            usdt
        };
        portfolio_values.push(current_value);
        if previous_value > 0.0 {
            returns.push((current_value - previous_value) / previous_value);
        }
    }

    let final_value = *portfolio_values.last().unwrap();
    let max_drawdown = utils::calculate_max_drawdown(&portfolio_values);
    let sharpe_ratio = utils::calculate_sharpe_ratio(&returns, 0.0, config.periods_per_year);

    BacktestMetrics {
        final_value,
        max_drawdown,
        sharpe_ratio,
    }
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
            BacktestConfig {
                periods_per_year: periods_per_year(Level::Hour4),
                starting_capital: 1000.0,
                fee_rate: 0.0015,
                execution_model: ExecutionModel::NextOpen,
            },
        );

        assert!((metrics.final_value - 499.25).abs() < 1e-3);
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
}
