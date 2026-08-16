use crate::data::{Bars, CandleSeries, MarketArrays};
use crate::exchange::Level;
use crate::metrics::{calmar_ratio, EquityStats};
use crate::precision::{BacktestFloat, Float, Precision, ACTIVE_PRECISION};
use crate::strategy::{Position, Signal, Strategy};
use anyhow::Context;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionModel {
    NextOpen,
}

impl std::fmt::Display for ExecutionModel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutionModel::NextOpen => f.write_str("next_open"),
        }
    }
}

/// Strategy-independent engine configuration. Anything strategy-specific
/// (e.g. EMA period bounds for `DoubleEmaCrossover`) lives in the strategy's
/// own `Config` type and is passed alongside.
#[derive(Clone, Debug)]
pub struct EngineConfig {
    /// Trading pair, e.g. "BTC-USDT". `Cow` so the default literal stays
    /// borrowed (no allocation) while CLI input lives as `Cow::Owned`.
    pub pair: Cow<'static, str>,
    pub level: Level,
    pub threads: usize,
    pub starting_capital: f32,
    pub fee_rate: f32,
    /// Per-**bar** risk-free rate subtracted from returns before the Sharpe
    /// ratio is computed. Usually 0.
    pub risk_free_rate: f32,
    pub execution_model: ExecutionModel,
    pub show_progress: bool,
    pub progress_step: usize,
    pub download_start: u64,
    /// Fraction of the series to optimize on, leaving the rest held out.
    /// `None` sweeps the whole series — the winner is then an in-sample
    /// result by construction, which is what this option exists to expose.
    pub split: Option<f32>,
}

/// How the bar index space is divided between optimization and validation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SampleSplit {
    pub in_sample: std::ops::Range<usize>,
    /// `None` when the whole series is optimized on.
    pub out_of_sample: Option<std::ops::Range<usize>>,
}

/// Split `bars` bars at `fraction`. Returns the whole range unsplit when the
/// fraction is absent, out of range, or would leave either side too short to
/// backtest (a segment needs at least two bars to produce one return).
pub fn sample_split(bars: usize, fraction: Option<f32>) -> SampleSplit {
    const MIN_SEGMENT: usize = 2;
    let whole = SampleSplit {
        in_sample: 0..bars,
        out_of_sample: None,
    };
    let Some(fraction) = fraction else {
        return whole;
    };
    if !(0.0..=1.0).contains(&fraction) {
        return whole;
    }
    let boundary = (bars as f32 * fraction) as usize;
    if boundary < MIN_SEGMENT || bars.saturating_sub(boundary) < MIN_SEGMENT {
        return whole;
    }
    SampleSplit {
        in_sample: 0..boundary,
        out_of_sample: Some(boundary..bars),
    }
}

/// Everything one backtest reports. Only `sharpe_ratio` and `final_value` take
/// part in ranking (see `prefer`); the rest are for the operator reading the
/// result, and exist because "best Sharpe" alone hides whether a strategy
/// traded twice or twenty thousand times.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BacktestMetrics {
    pub final_value: f64,
    /// Deepest peak-to-trough decline, as a percentage.
    pub max_drawdown: f64,
    pub sharpe_ratio: f64,
    /// Downside-only counterpart to Sharpe. May be infinite — see
    /// `metrics::EquityStats::sortino`.
    pub sortino_ratio: f64,
    /// CAGR divided by max drawdown.
    pub calmar_ratio: f64,
    /// Compound annual growth rate, as a percentage.
    pub cagr: f64,
    /// Completed round trips. A position still open at the last bar is not
    /// counted, here or in `win_rate`.
    pub trades: usize,
    /// Share of completed round trips that made money, as a percentage.
    pub win_rate: f64,
    /// Share of bars spent holding the asset, as a percentage. A strategy with
    /// a great Sharpe and 2% exposure is a different animal from one at 90%.
    pub exposure: f64,
}

impl BacktestMetrics {
    /// The result of doing nothing: no trades, no drawdown, capital intact.
    pub fn idle(starting_capital: f64) -> Self {
        Self {
            final_value: starting_capital,
            max_drawdown: 0.0,
            sharpe_ratio: 0.0,
            sortino_ratio: 0.0,
            calmar_ratio: 0.0,
            cagr: 0.0,
            trades: 0,
            win_rate: 0.0,
            exposure: 0.0,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct NumericBacktestConfig<T> {
    pub periods_per_year: usize,
    pub starting_capital: T,
    pub fee_rate: T,
    pub risk_free_rate: T,
    pub execution_model: ExecutionModel,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SweepResult<P> {
    pub metrics: BacktestMetrics,
    pub params: P,
}

#[derive(Clone, Copy, Debug)]
pub struct PrecisionRun<P> {
    pub precision: Precision,
    /// Winner of the sweep, scored over the optimization segment.
    pub best: SweepResult<P>,
    /// The same parameters re-scored on the held-out tail, when a split was
    /// requested. This is the number worth trusting; `best` is fitted to its
    /// own data by construction.
    pub out_of_sample: Option<BacktestMetrics>,
    pub duration: Duration,
}

pub fn periods_per_year(level: Level) -> usize {
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

/// Lex-ordered (fast, slow) pairs with `slow > fast`. Kept as a free helper
/// so `DoubleEmaCrossover::enumerate_params` can delegate and the unit test
/// stays here.
pub fn ema_parameter_pairs(
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

fn prefer<S: Strategy>(
    left: SweepResult<S::Params>,
    right: SweepResult<S::Params>,
) -> SweepResult<S::Params> {
    let left_finite = left.metrics.sharpe_ratio.is_finite();
    let right_finite = right.metrics.sharpe_ratio.is_finite();
    match (left_finite, right_finite) {
        (true, false) => return left,
        (false, true) => return right,
        _ => {}
    }
    if right.metrics.sharpe_ratio > left.metrics.sharpe_ratio {
        return right;
    }
    if right.metrics.sharpe_ratio < left.metrics.sharpe_ratio {
        return left;
    }
    if right.metrics.final_value > left.metrics.final_value {
        return right;
    }
    if right.metrics.final_value < left.metrics.final_value {
        return left;
    }
    match S::tie_break(left.params, right.params) {
        Ordering::Less | Ordering::Equal => left,
        Ordering::Greater => right,
    }
}

fn numeric_backtest_config<T: BacktestFloat>(engine: &EngineConfig) -> NumericBacktestConfig<T> {
    NumericBacktestConfig {
        periods_per_year: periods_per_year(engine.level),
        starting_capital: T::from_f32(engine.starting_capital),
        fee_rate: T::from_f32(engine.fee_rate),
        risk_free_rate: T::from_f32(engine.risk_free_rate),
        execution_model: engine.execution_model,
    }
}

/// Run one backtest for one parameter tuple. Generic over the strategy
/// (so `S::evaluator` is monomorphized + inlined into the hot loop) and
/// over the float precision.
pub fn run_one<S: Strategy, T: BacktestFloat>(
    bars: Bars<'_, T>,
    cache: &S::Cache<T>,
    params: S::Params,
    cfg: NumericBacktestConfig<T>,
) -> BacktestMetrics {
    run_one_range::<S, T>(bars, cache, params, cfg, 0..bars.len())
}

/// [`run_one`] restricted to a slice of the bar index space.
///
/// The indicator cache is always built over the *whole* series, so an
/// out-of-sample segment starting at bar `k` sees indicators with their full
/// warmup behind them rather than a fresh, invalid one. That is why the range
/// is a parameter here instead of the caller simply passing shorter slices.
pub fn run_one_range<S: Strategy, T: BacktestFloat>(
    bars: Bars<'_, T>,
    cache: &S::Cache<T>,
    params: S::Params,
    cfg: NumericBacktestConfig<T>,
    range: std::ops::Range<usize>,
) -> BacktestMetrics {
    let (open, close) = (bars.open, bars.close);
    // `run` rejects mismatched inputs up front; the assert keeps that contract
    // visible here, and the clamp keeps a release build safely truncating
    // rather than indexing past an end.
    debug_assert_eq!(open.len(), close.len(), "open/close length mismatch");
    let limit = open.len().min(close.len());
    let start = range.start.min(limit);
    let end = range.end.min(limit);
    let starting_capital = cfg.starting_capital.to_f64();
    if end.saturating_sub(start) < 2 {
        return BacktestMetrics::idle(starting_capital);
    }

    let evaluator = S::evaluator::<T>(bars, cache, params);
    let risk_free = cfg.risk_free_rate.to_f64();

    let mut usdt: T = cfg.starting_capital;
    let mut qty: T = T::ZERO;
    let mut current = Position::Flat;
    let mut stats = EquityStats::new(starting_capital);

    let mut trades = 0usize;
    let mut wins = 0usize;
    let mut bars_in_market = 0usize;
    let mut entry_value = starting_capital;

    // Pre-slice the trade and mark price views so the per-bar access goes
    // through bounds-check-free iterators. The strategy's `evaluator` reads
    // absolute bar indices, hence `start + offset` below.
    let trade_prices = match cfg.execution_model {
        ExecutionModel::NextOpen => &open[start + 1..end],
    };
    let marks = &close[start + 1..end];

    for (offset, (&trade_price, &mark_price)) in trade_prices.iter().zip(marks.iter()).enumerate() {
        let signal = evaluator(start + offset);
        match (current, signal) {
            (Position::Flat, Signal::EnterLong) => {
                entry_value = usdt.to_f64();
                qty = usdt / trade_price * (T::ONE - cfg.fee_rate);
                usdt = T::ZERO;
                current = Position::Long;
            }
            (Position::Long, Signal::ExitLong) => {
                usdt = qty * trade_price * (T::ONE - cfg.fee_rate);
                qty = T::ZERO;
                current = Position::Flat;
                trades += 1;
                if usdt.to_f64() > entry_value {
                    wins += 1;
                }
            }
            _ => {}
        }

        let value = match current {
            Position::Long => {
                bars_in_market += 1;
                qty * mark_price
            }
            Position::Flat => usdt,
        };
        stats.push(value.to_f64(), risk_free);
    }

    let simulated_bars = marks.len();
    let cagr = stats.cagr_pct(starting_capital, simulated_bars, cfg.periods_per_year);
    let max_drawdown = stats.max_drawdown_pct();
    let metrics = BacktestMetrics {
        final_value: stats.final_value(),
        max_drawdown,
        sharpe_ratio: stats.sharpe(risk_free, cfg.periods_per_year),
        sortino_ratio: stats.sortino(risk_free, cfg.periods_per_year),
        calmar_ratio: calmar_ratio(cagr, max_drawdown),
        cagr,
        trades,
        win_rate: percentage(wins, trades),
        exposure: percentage(bars_in_market, simulated_bars),
    };
    debug_assert!(
        metrics.sharpe_ratio.is_finite(),
        "non-finite sharpe leaked from backtest"
    );
    metrics
}

fn percentage(part: usize, whole: usize) -> f64 {
    if whole == 0 {
        return 0.0;
    }
    part as f64 / whole as f64 * 100.0
}

fn run_precision_sweep_impl<S: Strategy, T: BacktestFloat>(
    pool: &rayon::ThreadPool,
    engine: &EngineConfig,
    strategy_config: &S::Config,
    bars: Bars<'_, T>,
    parameter_set: &[S::Params],
) -> anyhow::Result<PrecisionRun<S::Params>> {
    report(
        engine,
        format_args!("Calculating all indicators for {ACTIVE_PRECISION}..."),
    );
    let cache = S::build_cache::<T>(bars, strategy_config)
        .with_context(|| format!("strategy '{}' could not build its indicators", S::NAME))?;
    let backtest_config = numeric_backtest_config::<T>(engine);
    let split = sample_split(bars.len(), engine.split);

    report(
        engine,
        format_args!("Calculated all indicators for {ACTIVE_PRECISION}."),
    );
    if let Some(out) = &split.out_of_sample {
        report(
            engine,
            format_args!(
                "Optimizing on bars 0..{} and holding out {}..{} for validation.",
                split.in_sample.end, out.start, out.end
            ),
        );
    }
    report(
        engine,
        format_args!(
            "Running all backtests on {} threads with {ACTIVE_PRECISION}...",
            engine.threads
        ),
    );

    let total_iterations = parameter_set.len();
    let progress_counter = AtomicUsize::new(0);
    let start = Instant::now();
    let best = pool
        .install(|| {
            parameter_set
                .par_iter()
                .fold(
                    || None::<SweepResult<S::Params>>,
                    |acc, &params| {
                        let metrics = run_one_range::<S, T>(
                            bars,
                            &cache,
                            params,
                            backtest_config,
                            split.in_sample.clone(),
                        );

                        let count = progress_counter.fetch_add(1, AtomicOrdering::Relaxed) + 1;
                        if engine.show_progress
                            && (count.is_multiple_of(engine.progress_step)
                                || count == total_iterations)
                        {
                            let percentage = (count as f32 / total_iterations as f32) * 100.0;
                            println!(
                                "  ...Progress: {:6}/{:6} iterations completed {:5.1}%.",
                                count, total_iterations, percentage
                            );
                        }

                        let candidate = SweepResult { metrics, params };
                        Some(match acc {
                            Some(prev) => prefer::<S>(prev, candidate),
                            None => candidate,
                        })
                    },
                )
                .reduce(
                    || None::<SweepResult<S::Params>>,
                    |a, b| match (a, b) {
                        (Some(x), Some(y)) => Some(prefer::<S>(x, y)),
                        (Some(x), None) | (None, Some(x)) => Some(x),
                        (None, None) => None,
                    },
                )
        })
        .with_context(|| format!("strategy '{}' produced an empty parameter sweep", S::NAME))?;
    let duration = start.elapsed();

    // Re-score the winner on the held-out tail. One extra backtest, after the
    // sweep, so it stays out of the measured duration.
    let out_of_sample = split
        .out_of_sample
        .map(|range| run_one_range::<S, T>(bars, &cache, best.params, backtest_config, range));

    Ok(PrecisionRun {
        precision: ACTIVE_PRECISION,
        best,
        out_of_sample,
        duration,
    })
}

/// Engine-side progress reporting, gated on `show_progress` in one place
/// rather than at each `println!`.
fn report(engine: &EngineConfig, message: std::fmt::Arguments<'_>) {
    if engine.show_progress {
        println!("{message}");
    }
}

pub fn run<S: Strategy>(
    engine: &EngineConfig,
    strategy_config: &S::Config,
    market: &CandleSeries,
) -> anyhow::Result<PrecisionRun<S::Params>> {
    // A ragged series means the loader or the caller is broken. Failing here
    // is the whole point: silently backtesting the shorter prefix would return
    // a plausible-looking result for data that does not exist.
    if !market.is_rectangular() {
        anyhow::bail!(
            "malformed market data: columns disagree in length \
             (timestamps={}, open={}, high={}, low={}, close={}, volume={})",
            market.timestamps.len(),
            market.open_prices.len(),
            market.high_prices.len(),
            market.low_prices.len(),
            market.close_prices.len(),
            market.volumes.len(),
        );
    }
    if market.len() < 2 {
        anyhow::bail!(
            "not enough candles to backtest: {} (need at least 2)",
            market.len()
        );
    }

    let pool = ThreadPoolBuilder::new()
        .num_threads(engine.threads)
        .build()
        .context("failed to construct rayon thread pool")?;

    let parameter_set = S::enumerate_params(strategy_config);
    let arrays = MarketArrays::<Float>::from_series(market);

    run_precision_sweep_impl::<S, Float>(
        &pool,
        engine,
        strategy_config,
        arrays.bars(),
        &parameter_set,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::OwnedBars;
    use crate::indicators::PeriodCache;
    use crate::strategy::double_ema::DoubleEmaCrossover;

    /// Bars with independent open and close columns — the execution model
    /// fills at the *next* open and marks at that bar's close, so the two must
    /// be distinguishable for these tests to mean anything.
    fn bars_of<T: BacktestFloat>(open: Vec<T>, close: Vec<T>) -> OwnedBars<T> {
        OwnedBars::ohlc(open, close.clone(), close.clone(), close)
    }

    /// A strategy that replays a canned signal per bar. Lets engine tests pin
    /// position transitions, trade counting and segment ranges without an
    /// indicator in the way.
    struct SignalScript;

    impl Strategy for SignalScript {
        type Params = ();
        type Cache<T: BacktestFloat> = Vec<Signal>;
        type Config = Vec<Signal>;
        const NAME: &'static str = "signal_script";

        fn build_cache<T: BacktestFloat>(
            _: Bars<'_, T>,
            cfg: &Self::Config,
        ) -> anyhow::Result<Self::Cache<T>> {
            Ok(cfg.clone())
        }

        fn enumerate_params(_: &Self::Config) -> Vec<Self::Params> {
            vec![()]
        }

        fn evaluator<'a, T: BacktestFloat>(
            _: Bars<'a, T>,
            cache: &'a Self::Cache<T>,
            _: (),
        ) -> impl Fn(usize) -> Signal + 'a {
            // Bars past the end of the script simply hold.
            move |i| cache.get(i).copied().unwrap_or(Signal::Hold)
        }

        fn param_summary(_: ()) -> String {
            String::new()
        }

        fn tie_break(_: (), _: ()) -> Ordering {
            Ordering::Equal
        }
    }

    fn num_cfg<T: BacktestFloat>(starting: f32) -> NumericBacktestConfig<T> {
        NumericBacktestConfig {
            periods_per_year: periods_per_year(Level::Hour4),
            starting_capital: T::from_f32(starting),
            fee_rate: T::from_f32(0.0015),
            risk_free_rate: T::ZERO,
            execution_model: ExecutionModel::NextOpen,
        }
    }

    #[test]
    fn run_one_waits_for_the_next_bar_after_a_signal() {
        let open_prices = vec![100.0_f32, 100.0, 200.0];
        let close_prices = vec![100.0_f32, 100.0, 100.0];
        let ema_fast = vec![f32::NAN, 2.0, 2.0];
        let ema_slow = vec![f32::NAN, 1.0, 1.0];
        let cache = PeriodCache::<f32>::from_series(1, vec![ema_fast, ema_slow]);
        let prices = bars_of(open_prices, close_prices);

        let metrics = run_one::<DoubleEmaCrossover, f32>(
            prices.bars(),
            &cache,
            (1, 2),
            num_cfg::<f32>(1000.0),
        );

        assert!((metrics.final_value - 499.25).abs() < 1e-3);
    }

    #[test]
    fn run_one_enters_then_exits_on_crossover_reversal() {
        let open_prices = vec![100.0_f32; 6];
        let close_prices = vec![100.0_f32; 6];
        let ema_fast = vec![f32::NAN, 2.0, 2.0, 2.0, 1.0, 1.0];
        let ema_slow = vec![f32::NAN, 1.0, 1.0, 1.0, 2.0, 2.0];
        let cache = PeriodCache::<f32>::from_series(1, vec![ema_fast, ema_slow]);
        let prices = bars_of(open_prices, close_prices);

        let metrics = run_one::<DoubleEmaCrossover, f32>(
            prices.bars(),
            &cache,
            (1, 2),
            num_cfg::<f32>(1000.0),
        );

        let expected = 1000.0_f64 * (1.0 - 0.0015_f64).powi(2);
        assert!(
            (metrics.final_value - expected).abs() < 1e-3,
            "final_value = {}, expected ≈ {}",
            metrics.final_value,
            expected
        );
    }

    #[test]
    fn run_one_supports_f64_precision() {
        let open_prices = vec![100.0_f64, 100.0, 200.0];
        let close_prices = vec![100.0_f64, 100.0, 100.0];
        let ema_fast = vec![f64::NAN, 2.0, 2.0];
        let ema_slow = vec![f64::NAN, 1.0, 1.0];
        let cache = PeriodCache::<f64>::from_series(1, vec![ema_fast, ema_slow]);
        let prices = bars_of(open_prices, close_prices);

        let metrics = run_one::<DoubleEmaCrossover, f64>(
            prices.bars(),
            &cache,
            (1, 2),
            num_cfg::<f64>(1000.0),
        );

        assert!((metrics.final_value - 499.25).abs() < 1e-6);
    }

    /// Engine-level test using `SignalScript` with a fixed signal sequence.
    /// Exercises the position-transition match independently of any indicator.
    #[test]
    fn run_one_engine_transitions_match_test_strategy_signals() {
        // 4 bars; engine reads signals at bar_index = 0..=2 (n - 1 = 3 iters).
        // i=0: Hold → no transition. value = usdt = 1000.
        // i=1: EnterLong → enter at open[2]=200. qty = 1000/200*0.9985 = 4.9925.
        //      value = qty * close[2] = 4.9925 * 200 = 998.5.
        // i=2: Hold → no transition. value = qty * close[3] = 4.9925 * 100 = 499.25.
        let open_prices = vec![100.0_f32, 100.0, 200.0, 100.0];
        let close_prices = vec![100.0_f32, 100.0, 200.0, 100.0];
        let cache: Vec<Signal> = vec![Signal::Hold, Signal::EnterLong, Signal::Hold];
        let prices = bars_of(open_prices, close_prices);

        let metrics =
            run_one::<SignalScript, f32>(prices.bars(), &cache, (), num_cfg::<f32>(1000.0));

        assert!(
            (metrics.final_value - 499.25).abs() < 1e-3,
            "final = {}",
            metrics.final_value
        );
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

    fn sweep<P: Copy>(params: P, sharpe: f64, final_value: f64) -> SweepResult<P> {
        SweepResult {
            metrics: BacktestMetrics {
                final_value,
                sharpe_ratio: sharpe,
                ..BacktestMetrics::idle(final_value)
            },
            params,
        }
    }

    #[test]
    fn prefer_uses_sharpe_then_final_value_then_tie_break() {
        // Same sharpe + same final_value → tie_break decides. DoubleEmaCrossover
        // tie_break returns natural ordering of (fast, slow); Less means left wins.
        let small = sweep::<(usize, usize)>((10, 20), 1.0, 1000.0);
        let large = sweep::<(usize, usize)>((12, 24), 1.0, 1000.0);
        assert_eq!(prefer::<DoubleEmaCrossover>(small, large), small);
        assert_eq!(prefer::<DoubleEmaCrossover>(large, small), small);

        // Higher sharpe wins regardless of tie_break.
        let lo = sweep::<(usize, usize)>((10, 20), 0.5, 1000.0);
        let hi = sweep::<(usize, usize)>((12, 24), 1.0, 900.0);
        assert_eq!(prefer::<DoubleEmaCrossover>(lo, hi), hi);
    }

    #[test]
    fn sample_split_divides_the_bar_range() {
        let split = sample_split(100, Some(0.7));
        assert_eq!(split.in_sample, 0..70);
        assert_eq!(split.out_of_sample, Some(70..100));
    }

    #[test]
    fn sample_split_falls_back_to_the_whole_range_when_a_side_would_be_too_short() {
        for (bars, fraction) in [
            (100, None),
            (100, Some(0.0)),
            (100, Some(1.0)),
            (100, Some(1.5)),  // out of range
            (100, Some(-0.5)), // out of range
            (100, Some(0.01)), // in-sample would be 1 bar
            (100, Some(0.99)), // out-of-sample would be 1 bar
            (3, Some(0.5)),    // too few bars to divide at all
        ] {
            let split = sample_split(bars, fraction);
            assert_eq!(
                split.in_sample,
                0..bars,
                "bars={bars} fraction={fraction:?}"
            );
            assert_eq!(
                split.out_of_sample, None,
                "bars={bars} fraction={fraction:?}"
            );
        }
    }

    #[test]
    fn run_one_range_scores_only_the_requested_segment() {
        // Flat for the first half, then a steady climb. A strategy that is
        // always long must show no gain in the first segment and a gain in
        // the second.
        let mut close = vec![100.0_f32; 10];
        close.extend((1..=10).map(|i| 100.0 + 10.0 * i as f32));
        let prices = bars_of(close.clone(), close.clone());
        let always_long: Vec<Signal> = vec![Signal::EnterLong; close.len()];

        let first = run_one_range::<SignalScript, f32>(
            prices.bars(),
            &always_long,
            (),
            num_cfg::<f32>(1000.0),
            0..10,
        );
        let second = run_one_range::<SignalScript, f32>(
            prices.bars(),
            &always_long,
            (),
            num_cfg::<f32>(1000.0),
            10..20,
        );

        assert!(
            (first.final_value - 998.5).abs() < 1e-2,
            "flat segment keeps capital minus one entry fee, got {}",
            first.final_value
        );
        assert!(
            second.final_value > 1400.0,
            "climbing segment should gain, got {}",
            second.final_value
        );
    }

    #[test]
    fn run_one_reports_trades_exposure_and_win_rate() {
        // Enter, ride a rise, exit; then enter, ride a fall, exit.
        let close = vec![100.0_f32, 100.0, 200.0, 200.0, 200.0, 100.0, 100.0, 100.0];
        let prices = bars_of(close.clone(), close.clone());
        let signals = vec![
            Signal::EnterLong, // fill at bar 1
            Signal::Hold,
            Signal::ExitLong, // fill at bar 3, after the rise
            Signal::EnterLong,
            Signal::Hold,
            Signal::ExitLong, // fill at bar 6, after the fall
            Signal::Hold,
        ];

        let m = run_one_range::<SignalScript, f32>(
            prices.bars(),
            &signals,
            (),
            num_cfg::<f32>(1000.0),
            0..close.len(),
        );

        assert_eq!(m.trades, 2, "two completed round trips");
        assert!((m.win_rate - 50.0).abs() < 1e-6, "one up, one down");
        assert!(m.exposure > 0.0 && m.exposure < 100.0, "partly invested");
        assert!(m.max_drawdown > 0.0, "the second trade lost money");
    }

    #[test]
    fn an_open_position_at_the_last_bar_is_not_counted_as_a_trade() {
        let close = vec![100.0_f32, 100.0, 110.0, 120.0];
        let prices = bars_of(close.clone(), close.clone());
        let signals = vec![Signal::EnterLong, Signal::Hold, Signal::Hold];

        let m = run_one_range::<SignalScript, f32>(
            prices.bars(),
            &signals,
            (),
            num_cfg::<f32>(1000.0),
            0..close.len(),
        );
        assert_eq!(m.trades, 0, "never exited, so nothing completed");
        assert_eq!(m.win_rate, 0.0);
        assert!(m.final_value > 1000.0, "but the equity still marked up");
    }

    #[test]
    fn prefer_prefers_finite_sharpe_over_nan() {
        let finite = sweep::<(usize, usize)>((1, 2), -10.0, 100.0);
        let nan = sweep::<(usize, usize)>((3, 4), f64::NAN, 100.0);
        assert_eq!(prefer::<DoubleEmaCrossover>(finite, nan), finite);
        assert_eq!(prefer::<DoubleEmaCrossover>(nan, finite), finite);
    }
}
