use backtest_rust::backtest::{run, EngineConfig, ExecutionModel};
use backtest_rust::data::CandleSeries;
use backtest_rust::exchange::Level;
use backtest_rust::precision::ACTIVE_PRECISION;
use backtest_rust::strategy::double_ema::{DoubleEmaConfig, DoubleEmaCrossover};
use std::borrow::Cow;

fn synthetic_market(n: usize) -> CandleSeries {
    let mut timestamps = Vec::with_capacity(n);
    let mut open_prices = Vec::with_capacity(n);
    let mut close_prices = Vec::with_capacity(n);
    for i in 0..n {
        timestamps.push(i as u64 * 60_000);
        let phase = (i as f32 / 30.0).sin();
        let close = 100.0 + 20.0 * phase;
        // Open shifted by a small constant so swapping `open_prices` and
        // `close_prices` in the execution path would observably change
        // the computed best result.
        open_prices.push(close + 0.5);
        close_prices.push(close);
    }
    CandleSeries {
        timestamps,
        open_prices,
        close_prices,
    }
}

fn small_engine() -> EngineConfig {
    EngineConfig {
        pair: Cow::Borrowed("TEST-USDT"),
        level: Level::Hour1,
        threads: 2,
        starting_capital: 1000.0,
        fee_rate: 0.0015,
        execution_model: ExecutionModel::NextOpen,
        show_progress: false,
        progress_step: 100,
        download_start: 0,
    }
}

fn small_strategy() -> DoubleEmaConfig {
    DoubleEmaConfig {
        fast_period_min: 3,
        slow_period_min: 4,
        max_period: 8,
    }
}

#[test]
fn run_is_deterministic_on_synthetic_market() {
    let market = synthetic_market(200);
    let r1 = run::<DoubleEmaCrossover>(&small_engine(), &small_strategy(), &market).unwrap();
    let r2 = run::<DoubleEmaCrossover>(&small_engine(), &small_strategy(), &market).unwrap();

    assert_eq!(r1.precision, ACTIVE_PRECISION);
    assert_eq!(r1.best.params, r2.best.params);
    assert!((r1.best.metrics.sharpe_ratio - r2.best.metrics.sharpe_ratio).abs() < 1e-9);
}
