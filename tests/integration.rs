use backtest_rust::backtest::{run, EngineConfig, ExecutionModel};
use backtest_rust::data::CandleSeries;
use backtest_rust::exchange::Level;
use backtest_rust::precision::ACTIVE_PRECISION;
use backtest_rust::strategy::double_ema::{DoubleEmaConfig, DoubleEmaCrossover};
use std::borrow::Cow;

fn synthetic_market(n: usize) -> CandleSeries {
    let mut market = CandleSeries {
        timestamps: Vec::with_capacity(n),
        open_prices: Vec::with_capacity(n),
        high_prices: Vec::with_capacity(n),
        low_prices: Vec::with_capacity(n),
        close_prices: Vec::with_capacity(n),
        volumes: Vec::with_capacity(n),
    };
    for i in 0..n {
        market.timestamps.push(i as u64 * 60_000);
        let phase = (i as f32 / 30.0).sin();
        let close = 100.0 + 20.0 * phase;
        // Open shifted by a small constant so swapping `open_prices` and
        // `close_prices` in the execution path would observably change
        // the computed best result.
        market.open_prices.push(close + 0.5);
        market.high_prices.push(close + 1.0);
        market.low_prices.push(close - 1.0);
        market.close_prices.push(close);
        market.volumes.push(1_000.0 + i as f32);
    }
    market
}

fn small_engine() -> EngineConfig {
    EngineConfig {
        pair: Cow::Borrowed("TEST-USDT"),
        level: Level::Hour1,
        threads: 2,
        starting_capital: 1000.0,
        fee_rate: 0.0015,
        risk_free_rate: 0.0,
        execution_model: ExecutionModel::NextOpen,
        show_progress: false,
        progress_step: 100,
        download_start: 0,
        split: None,
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
    assert!(r1.out_of_sample.is_none(), "no split was requested");
}

#[test]
fn split_reports_a_separate_out_of_sample_result() {
    let market = synthetic_market(400);
    let mut engine = small_engine();
    engine.split = Some(0.7);

    let result = run::<DoubleEmaCrossover>(&engine, &small_strategy(), &market).unwrap();
    let out_of_sample = result
        .out_of_sample
        .expect("a split run must report held-out metrics");

    // The two segments are scored independently, so their equity curves start
    // from the same capital but end in different places.
    assert_ne!(
        out_of_sample.final_value, result.best.metrics.final_value,
        "held-out metrics must not be a copy of the in-sample ones"
    );

    // The winner is chosen on the in-sample segment alone: re-running without
    // the split searches a longer series and may well pick something else.
    let full = run::<DoubleEmaCrossover>(&small_engine(), &small_strategy(), &market).unwrap();
    assert!(
        full.out_of_sample.is_none(),
        "the unsplit run has nothing held out"
    );
}

#[test]
fn run_rejects_a_ragged_market() {
    let mut market = synthetic_market(50);
    market.high_prices.pop();
    let error = run::<DoubleEmaCrossover>(&small_engine(), &small_strategy(), &market)
        .expect_err("a ragged series must not be silently truncated");
    assert!(
        format!("{error:#}").contains("columns disagree"),
        "unexpected error: {error:#}"
    );
}
