mod auto_trading;
use anyhow::Context as _;
use chrono::prelude::*; // This crate provides easy-to-use date and time functions
use chrono::Utc;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
mod ta_wrapper;
mod utils;
use auto_trading::*;

const PAIR: &str = "BTC-USDT";
const LEVEL: Level = Level::Hour4; // for indicators and entry/exit signals
const NB_THREADS: usize = 12;

///////////////////////////////////////////////////////////////////////////////////////////////////////
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let start = Instant::now();

    let data_file = utils::data_file_path(PAIR, &LEVEL);
    if let Err(error) = utils::download_dump_k_lines_to_json(PAIR, LEVEL, 1546300800000..).await {
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

    println!("Doing: {:} {:}", PAIR, LEVEL);

    let (timestamps, close_prices) = utils::load_data_file(PAIR, &LEVEL)?;

    if let Some(&first_timestamp) = timestamps.first() {
        let first_date = Utc
            .timestamp_millis_opt(first_timestamp)
            .single()
            .with_context(|| format!("invalid first timestamp: {first_timestamp}"))?;
        println!("First timestamp : {}", first_date.format("%Y-%m-%d"));
    }
    if let Some(&last_timestamp) = timestamps.last() {
        let last_date = Utc
            .timestamp_millis_opt(last_timestamp)
            .single()
            .with_context(|| format!("invalid last timestamp: {last_timestamp}"))?;
        println!("Last  timestamp : {}", last_date.format("%Y-%m-%d"));
    }

    // Calculate all indicators
    println!("Calculating all indicators...");

    let ema_store = Arc::new(ta_wrapper::EMAStore::new(&close_prices, 3, 600));
    let close_prices = Arc::new(close_prices);
    let periods_per_year = periods_per_year(LEVEL);

    println!("Calculated all indicators.");

    // Run backtests

    println!("Running all backtests on {} threads...", NB_THREADS);

    // Prepare for collecting results
    let best_results = Arc::new(Mutex::new((0.0, 0.0, f32::NEG_INFINITY, 0, 0)));

    // Configure and use a custom thread pool
    let pool = ThreadPoolBuilder::new()
        .num_threads(NB_THREADS)
        .build()
        .unwrap();

    let total_iterations = (5..=600)
        .into_iter()
        .map(|p1| (p1 + 1..=600).count())
        .sum::<usize>();
    let progress_counter = Arc::new(AtomicUsize::new(0));

    pool.install(|| {
        (5..=600).into_par_iter().for_each(|period1| {
            for period2 in 6..=600 {
                if period2 > period1 {
                    let ema1 = ema_store.get_ema(period1);
                    let ema2 = ema_store.get_ema(period2);

                    let (port_value, max_drawdown, sharpe_ratio) =
                        backtest_double_ema(close_prices.as_ref(), ema1, ema2, periods_per_year);

                    let mut best = best_results.lock().unwrap();
                    if sharpe_ratio > best.2 {
                        *best = (port_value, max_drawdown, sharpe_ratio, period1, period2);
                    }

                    // Progress update
                    let count = progress_counter.fetch_add(1, Ordering::SeqCst) + 1;
                    if count % 10000 == 0 || count == total_iterations {
                        let percentage = (count as f32 / total_iterations as f32) * 100.0;
                        println!(
                            "  ...Progress: {:6}/{:6} iterations completed {:5.1}%.",
                            count, total_iterations, percentage
                        );
                    }
                }
            }
        });
    });

    println!("Done");
    let best = best_results.lock().unwrap();
    println!(
        "Best result: sharpe: {:.3}, max_dd: {:.2}, Period1: {}, Period2: {}",
        best.2, best.1, best.3, best.4
    );
    println!("Final portfolio value: {:.1}$", best.0);

    utils::write_to_file(
        format!("{}_{}", PAIR, LEVEL).as_str(),
        best.0,
        best.1,
        best.2,
        best.3,
        best.4,
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

fn backtest_double_ema(
    close_prices: &[f32],
    ema1: &[f32],
    ema2: &[f32],
    periods_per_year: usize,
) -> (f32, f32, f32) {
    if close_prices.is_empty()
        || close_prices.len() != ema1.len()
        || close_prices.len() != ema2.len()
    {
        return (1000.0, 0.0, 0.0);
    }

    let mut usdt: f32 = 1000.0; // Starting with 1000 USDT
    let mut asset_quantity: f32 = 0.0; // Quantity of the asset we own
    let mut in_position = false; // Flag to track if we are currently holding the asset

    let mut portfolio_values = vec![usdt]; // Track portfolio values over time
    let mut returns = vec![]; // To calculate the Sharpe Ratio
    let fee: f32 = 0.15; // %, buy and sell fees

    // Iterate through close_prices to decide when to buy/sell
    for i in 1..close_prices.len() {
        let current_price = close_prices[i];
        let previous_value = portfolio_values.last().copied().unwrap();
        let previous_fast = ema1[i - 1];
        let previous_slow = ema2[i - 1];

        if previous_fast.is_finite()
            && previous_slow.is_finite()
            && !in_position
            && previous_fast > previous_slow
        {
            // Enter position: buy as much as possible with all USDT
            asset_quantity = usdt / current_price * (1.0 - fee / 100.0);
            usdt = 0.0;
            in_position = true;
            //println!("Buying at price {}: new asset quantity {}", current_price, asset_quantity);
        } else if previous_fast.is_finite()
            && previous_slow.is_finite()
            && in_position
            && previous_fast < previous_slow
        {
            // Exit position: sell all of the asset
            usdt = asset_quantity * current_price * (1.0 - fee / 100.0);
            asset_quantity = 0.0;
            in_position = false;
            //println!("Selling at price {}: new USDT amount {}", current_price, usdt);
        }

        let current_value = if in_position {
            asset_quantity * current_price
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
    let sharpe_ratio = utils::calculate_sharpe_ratio(&returns, 0.0, periods_per_year);

    (final_value, max_drawdown, sharpe_ratio)
}

///////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backtest_waits_for_the_next_bar_after_a_signal() {
        let close_prices = vec![100.0, 200.0, 100.0];
        let ema_fast = vec![f32::NAN, 2.0, 2.0];
        let ema_slow = vec![f32::NAN, 1.0, 1.0];

        let (final_value, _, _) = backtest_double_ema(
            &close_prices,
            &ema_fast,
            &ema_slow,
            periods_per_year(Level::Hour4),
        );

        assert!((final_value - 998.5).abs() < 1e-3);
    }

    #[test]
    fn periods_per_year_matches_the_selected_level() {
        assert_eq!(periods_per_year(Level::Hour4), 6 * 365);
        assert_eq!(periods_per_year(Level::Minute15), 24 * 4 * 365);
        assert_eq!(periods_per_year(Level::Month1), 12);
    }
}
