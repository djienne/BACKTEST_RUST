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
        market.timestamps.push(i as u64 * 3_600_000);
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
    let mut engine = small_engine();
    engine.threads = 1;
    let r1 = run::<DoubleEmaCrossover>(&engine, &small_strategy(), &market).unwrap();
    engine.threads = 4;
    let r2 = run::<DoubleEmaCrossover>(&engine, &small_strategy(), &market).unwrap();

    assert_eq!(r1.precision, ACTIVE_PRECISION);
    assert_eq!(r1.best, r2.best);
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

    // Changing only future/held-out prices must not influence training.
    let mut changed = market.clone();
    for i in 280..changed.len() {
        let price = 80.0 + ((i - 280) % 3) as f32 * 20.0;
        changed.open_prices[i] = price;
        changed.close_prices[i] = price;
        changed.high_prices[i] = price + 1.0;
        changed.low_prices[i] = price - 1.0;
    }
    let perturbed = run::<DoubleEmaCrossover>(&engine, &small_strategy(), &changed).unwrap();
    assert_eq!(
        perturbed.best, result.best,
        "holdout leaked into optimization"
    );
    assert_ne!(perturbed.out_of_sample, result.out_of_sample);
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

#[test]
fn run_rejects_bad_prices_and_missing_time() {
    let engine = small_engine();
    for price in [0.0, -1.0, f32::NAN, f32::INFINITY] {
        let mut market = synthetic_market(50);
        market.open_prices[20] = price;
        assert!(run::<DoubleEmaCrossover>(&engine, &small_strategy(), &market).is_err());
    }
    let mut market = synthetic_market(50);
    market.low_prices[20] = market.high_prices[20] + 1.0;
    assert!(run::<DoubleEmaCrossover>(&engine, &small_strategy(), &market).is_err());
    let mut market = synthetic_market(50);
    market.timestamps[20..]
        .iter_mut()
        .for_each(|t| *t += 3_600_000);
    assert!(format!(
        "{:#}",
        run::<DoubleEmaCrossover>(&engine, &small_strategy(), &market).unwrap_err()
    )
    .contains("gap"));
}

#[test]
fn selecting_a_start_keeps_all_columns_aligned() {
    let original = synthetic_market(50);
    let mut selected = original.clone();
    selected.retain_since(original.timestamps[20] + 1).unwrap();
    assert_eq!(selected.timestamps, original.timestamps[21..]);
    assert_eq!(selected.open_prices, original.open_prices[21..]);
    assert_eq!(selected.close_prices, original.close_prices[21..]);
    assert_eq!(selected.high_prices, original.high_prices[21..]);
    assert_eq!(selected.low_prices, original.low_prices[21..]);
    assert_eq!(selected.volumes, original.volumes[21..]);
    run::<DoubleEmaCrossover>(&small_engine(), &small_strategy(), &selected).unwrap();
    selected.retain_since(u64::MAX).unwrap();
    assert!(run::<DoubleEmaCrossover>(&small_engine(), &small_strategy(), &selected).is_err());
}

#[test]
fn cli_honors_since_and_never_saves_an_invalid_holdout() {
    use backtest_rust::{data::DataPaths, exchange::K, feather};
    use std::{
        fs,
        process::Command,
        time::{SystemTime, UNIX_EPOCH},
    };

    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let root = std::env::temp_dir().join(format!("backtest_cli_{}", now.as_nanos()));
    let paths = DataPaths::new(root.join("data"), root.join("results"));
    fs::create_dir_all(paths.klines_dir()).unwrap();
    let last = (now.as_millis() as u64 / 60_000 - 1) * 60_000;
    let rows: Vec<K> = (0..30)
        .map(|i| {
            let price = 100.0 + (i as f32 * 0.5).sin();
            K {
                time: last - (29 - i) * 60_000,
                open: price,
                high: price,
                low: price,
                close: price,
                volume: 100.0,
            }
        })
        .collect();
    let cache = paths.feather("TEST-USDT", &Level::Minute1);
    feather::write(&cache, &rows).unwrap();
    let run_cli = |split| {
        Command::new(env!("CARGO_BIN_EXE_BACKTEST_RUST"))
            .env("BACKTEST_FORCE_DOWNLOAD", "0")
            .env("BACKTEST_SHOW_PROGRESS", "0")
            .args([
                "--pair",
                "TEST-USDT",
                "--level",
                "1m",
                "--strategy",
                "price_vs_ema",
                "--param",
                "period=2",
                "--since",
                &rows[10].time.to_string(),
                "--split",
                split,
                "--data-dir",
                paths.klines_dir().to_str().unwrap(),
                "--results-dir",
                root.join("results").to_str().unwrap(),
            ])
            .output()
            .unwrap()
    };
    let output = run_cli("0.7");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let saved = fs::read_to_string(paths.results("TEST-USDT", &Level::Minute1)).unwrap();
    let mut lines = saved.lines();
    let names: Vec<_> = lines.next().unwrap().split(',').collect();
    let fields: Vec<_> = lines.next().unwrap().split(',').collect();
    let field = |name| fields[names.iter().position(|v| *v == name).unwrap()];
    assert_eq!(field("FirstTimeMs"), rows[10].time.to_string());
    assert_eq!(field("LastTimeMs"), last.to_string());
    assert_eq!(field("Candles"), "20");
    assert_eq!(field("SplitIndex"), "14");
    assert_eq!(
        feather::read(&cache).unwrap(),
        rows,
        "selection must not truncate the cache"
    );
    let failed = run_cli("0.99");
    assert!(!failed.status.success());
    assert!(String::from_utf8_lossy(&failed.stderr).contains("at least 2"));
    assert_eq!(
        fs::read_to_string(paths.results("TEST-USDT", &Level::Minute1)).unwrap(),
        saved
    );
    fs::remove_dir_all(root).unwrap();
}
