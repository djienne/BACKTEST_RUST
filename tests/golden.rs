//! Regression over the explicitly selected uninterrupted tail of the fixture.
//!
//! The full fixture has known gaps and must be rejected. Recorded values here
//! detect behavioral changes; hand-computed accounting tests provide the
//! independent check on the simulation, rather than treating old output as truth.

use backtest_rust::backtest::{run, EngineConfig, ExecutionModel};
use backtest_rust::data::{load_data_file, DataPaths};
use backtest_rust::exchange::Level;
use backtest_rust::strategy::double_ema::{DoubleEmaConfig, DoubleEmaCrossover};
use std::borrow::Cow;

/// The selected tail starts immediately after the last known gap.
const FIRST_CONTIGUOUS_TIMESTAMP: u64 = 1_582_128_000_000;
// f64: final=9500.632369, sharpe=1.100367925, max_dd=53.727312.
// f32: final=9500.611328, sharpe=1.100367042, max_dd=53.727324.
const EXPECTED_PARAMS: (usize, usize) = (40, 58);
const EXPECTED_FINAL_VALUE: f64 = 9_500.632_369;
const EXPECTED_SHARPE: f64 = 1.100_367_925;
const EXPECTED_MAX_DD: f64 = 53.727_312;
const EXPECTED_CANDLES: usize = 15_982;

fn engine() -> EngineConfig {
    EngineConfig {
        pair: Cow::Borrowed("BTC-USDT"),
        level: Level::Hour4,
        threads: 4,
        starting_capital: 1000.0,
        fee_rate: 0.0015,
        risk_free_rate: 0.0,
        execution_model: ExecutionModel::NextOpen,
        show_progress: false,
        progress_step: 100_000,
        download_start: 0,
        split: None,
    }
}

fn strategy() -> DoubleEmaConfig {
    DoubleEmaConfig {
        fast_period_min: 5,
        slow_period_min: 6,
        max_period: 60,
    }
}

#[test]
fn double_ema_sweep_matches_the_recorded_baseline() {
    let mut market = load_data_file(&DataPaths::default(), "BTC-USDT", &Level::Hour4)
        .expect("fixture data should load");
    assert_eq!(market.len(), EXPECTED_CANDLES);
    let error = run::<DoubleEmaCrossover>(&engine(), &strategy(), &market).unwrap_err();
    assert!(format!("{error:#}").contains("gap"));
    market.retain_since(FIRST_CONTIGUOUS_TIMESTAMP).unwrap();
    assert_eq!(market.timestamps[0], FIRST_CONTIGUOUS_TIMESTAMP);
    assert_eq!(market.len(), 13_499);
    let selected = run::<DoubleEmaCrossover>(&engine(), &strategy(), &market).unwrap();
    let m = selected.best.metrics;

    println!(
        "GOLDEN params={:?} final_value={:.6} sharpe={:.9} max_dd={:.6} candles={}",
        selected.best.params,
        m.final_value,
        m.sharpe_ratio,
        m.max_drawdown,
        market.close_prices.len(),
    );

    assert_eq!(selected.best.params, EXPECTED_PARAMS, "winning parameters");
    // Relative 1e-4 on the equity: a genuinely different trade sequence moves
    // this by percent, not by parts-per-ten-thousand.
    assert!(
        (m.final_value - EXPECTED_FINAL_VALUE).abs() <= EXPECTED_FINAL_VALUE.abs() * 1e-4,
        "final_value = {}, expected ≈ {EXPECTED_FINAL_VALUE}",
        m.final_value
    );
    assert!(
        (m.sharpe_ratio - EXPECTED_SHARPE).abs() <= 2e-3,
        "sharpe = {}, expected ≈ {EXPECTED_SHARPE}",
        m.sharpe_ratio
    );
    assert!(
        (m.max_drawdown - EXPECTED_MAX_DD).abs() <= 1e-2,
        "max_dd = {}, expected ≈ {EXPECTED_MAX_DD}",
        m.max_drawdown
    );
}
