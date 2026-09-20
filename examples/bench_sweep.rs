//! Repeatable sweep timing, for before/after comparisons of engine changes.
//!
//! Reads a cached market without downloading. Gaps are rejected; an optional
//! final argument selects a start in unix-ms. Reports sweep time and throughput.
//!
//! ```text
//! # Committed fixture's contiguous tail; max period, threads, pair, level, since:
//! cargo run --release --example bench_sweep -- 200 4 BTC-USDT 4h 1582128000000
//! cargo run --release --example bench_sweep -- 200 4 ETH-USDT 1h
//! ```

use backtest_rust::backtest::{run, EngineConfig, ExecutionModel};
use backtest_rust::data::{load_data_file, DataPaths};
use backtest_rust::exchange::Level;
use backtest_rust::strategy::double_ema::{DoubleEmaConfig, DoubleEmaCrossover};
use backtest_rust::strategy::Strategy as _;
use std::borrow::Cow;
use std::str::FromStr;

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let max_period: usize = args.first().map_or(Ok(200), |v| v.parse())?;
    let threads: usize = args.get(1).map_or(Ok(1), |v| v.parse())?;
    let pair = args
        .get(2)
        .cloned()
        .unwrap_or_else(|| "BTC-USDT".to_string());
    let level = args
        .get(3)
        .map_or(Ok(Level::Hour4), |v| Level::from_str(v))
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let paths = DataPaths::default();
    let mut market = load_data_file(&paths, &pair, &level)?;
    if let Some(since) = args.get(4) {
        market.retain_since(since.parse()?)?;
    }

    let engine = EngineConfig {
        pair: Cow::Owned(pair.clone()),
        level,
        threads,
        starting_capital: 1000.0,
        fee_rate: 0.0015,
        risk_free_rate: 0.0,
        execution_model: ExecutionModel::NextOpen,
        show_progress: false,
        progress_step: usize::MAX,
        download_start: 0,
        split: None,
    };
    let strategy = DoubleEmaConfig {
        fast_period_min: 5,
        slow_period_min: 6,
        max_period,
    };

    let combinations = DoubleEmaCrossover::enumerate_params(&strategy).len();
    println!(
        "{pair} {level}: {} candles, {combinations} parameter pairs, {threads} thread(s)",
        market.len(),
    );

    let result = run::<DoubleEmaCrossover>(&engine, &strategy, &market)?;
    let seconds = result.duration.as_secs_f64();
    println!(
        "sweep {seconds:.3}s | {:.0} backtests/s | {:.1} M bar-evaluations/s | best {:?} sharpe {:.6}",
        combinations as f64 / seconds,
        (combinations as f64 * market.len() as f64) / seconds / 1e6,
        result.best.params,
        result.best.metrics.sharpe_ratio,
    );
    Ok(())
}
