use backtest_rust::backtest::{run, ExecutionModel, RunConfig};
use backtest_rust::data::CandleSeries;
use backtest_rust::exchange::Level;
use backtest_rust::precision::Precision;

fn synthetic_market(n: usize) -> CandleSeries {
    let mut timestamps = Vec::with_capacity(n);
    let mut open_prices = Vec::with_capacity(n);
    let mut close_prices = Vec::with_capacity(n);
    for i in 0..n {
        timestamps.push(i as u64 * 60_000);
        let phase = (i as f32 / 30.0).sin();
        let price = 100.0 + 20.0 * phase;
        open_prices.push(price);
        close_prices.push(price);
    }
    CandleSeries {
        timestamps,
        open_prices,
        close_prices,
    }
}

fn small_config() -> RunConfig {
    RunConfig {
        pair: "TEST-USDT",
        level: Level::Hour1,
        threads: 2,
        fast_period_min: 3,
        slow_period_min: 4,
        max_period: 8,
        starting_capital: 1000.0,
        fee_rate: 0.0015,
        execution_model: ExecutionModel::NextOpen,
        precision: Precision::F32,
        compare_precisions: false,
        show_progress: false,
        progress_step: 100,
        download_start: 0,
    }
}

#[test]
fn run_is_deterministic_on_synthetic_market() {
    let market = synthetic_market(200);
    let report1 = run(small_config(), &market).unwrap();
    let report2 = run(small_config(), &market).unwrap();

    assert_eq!(report1.selected.precision, Precision::F32);
    assert_eq!(
        report1.selected.best.fast_period,
        report2.selected.best.fast_period
    );
    assert_eq!(
        report1.selected.best.slow_period,
        report2.selected.best.slow_period
    );
    assert!(
        (report1.selected.best.metrics.sharpe_ratio
            - report2.selected.best.metrics.sharpe_ratio)
            .abs()
            < 1e-9
    );
}

#[test]
fn run_in_compare_mode_returns_both_runs() {
    let market = synthetic_market(200);
    let mut config = small_config();
    config.compare_precisions = true;

    let report = run(config, &market).unwrap();
    let (f32_run, f64_run) = report.comparison.expect("comparison should be present");
    assert_eq!(f32_run.precision, Precision::F32);
    assert_eq!(f64_run.precision, Precision::F64);
    // Same parameter pair should win across precisions on a smooth synthetic series.
    assert_eq!(f32_run.best.fast_period, f64_run.best.fast_period);
    assert_eq!(f32_run.best.slow_period, f64_run.best.slow_period);
}
