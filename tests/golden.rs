//! Golden regression: pins the `double_ema` sweep result on the committed
//! `dataKLines/BTC-USDT-4h.feather` fixture.
//!
//! This is the safety net for engine refactors. The winning parameters must
//! never change; the metrics may drift in the last digits when the reduction
//! order or accumulation scheme changes (e.g. two-pass variance → Welford), so
//! they are compared with a precision-dependent tolerance rather than exactly.

use backtest_rust::backtest::{run, EngineConfig, ExecutionModel};
use backtest_rust::data::{load_data_file, DataPaths};
use backtest_rust::exchange::Level;
use backtest_rust::strategy::double_ema::{DoubleEmaConfig, DoubleEmaCrossover};
use std::borrow::Cow;

/// Recorded from the pre-refactor engine (commit 7f305e7), `f64` build. See the
/// module docs before touching. The `f32` build agrees on the parameters and
/// lands within the tolerances below:
///   f32: final=23468.886719 sharpe=1.235991836 max_dd=53.727335
///   f64: final=23468.932676 sharpe=1.235911914 max_dd=53.727312
const EXPECTED_PARAMS: (usize, usize) = (39, 59);
const EXPECTED_FINAL_VALUE: f64 = 23_468.932_676;
const EXPECTED_SHARPE: f64 = 1.235_911_914;
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
    let market = load_data_file(&DataPaths::default(), "BTC-USDT", &Level::Hour4)
        .expect("fixture data should load");
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

    assert_eq!(
        market.close_prices.len(),
        EXPECTED_CANDLES,
        "fixture changed size — re-record the baseline deliberately, don't relax it"
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
