use crate::data::CandleSeries;
use crate::exchange::Level;
use crate::metrics::{max_drawdown, sharpe_ratio};
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
    pub execution_model: ExecutionModel,
    pub show_progress: bool,
    pub progress_step: usize,
    pub download_start: u64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BacktestMetrics {
    pub final_value: f64,
    pub max_drawdown: f64,
    pub sharpe_ratio: f64,
}

#[derive(Clone, Copy, Debug)]
pub struct NumericBacktestConfig<T> {
    pub periods_per_year: usize,
    pub starting_capital: T,
    pub fee_rate: T,
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
    pub best: SweepResult<P>,
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

fn prefer<S: Strategy>(left: SweepResult<S::Params>, right: SweepResult<S::Params>)
    -> SweepResult<S::Params>
{
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

fn numeric_backtest_config<T: BacktestFloat>(
    engine: &EngineConfig,
) -> NumericBacktestConfig<T> {
    NumericBacktestConfig {
        periods_per_year: periods_per_year(engine.level),
        starting_capital: T::from_f32(engine.starting_capital),
        fee_rate: T::from_f32(engine.fee_rate),
        execution_model: engine.execution_model,
    }
}

/// Run one backtest for one parameter tuple. Generic over the strategy
/// (so `S::evaluator` is monomorphized + inlined into the hot loop) and
/// over the float precision.
pub fn run_one<S: Strategy, T: BacktestFloat>(
    open: &[T],
    close: &[T],
    cache: &S::Cache<T>,
    params: S::Params,
    cfg: NumericBacktestConfig<T>,
) -> BacktestMetrics {
    if close.is_empty() || open.len() != close.len() {
        return BacktestMetrics {
            final_value: cfg.starting_capital.to_f64(),
            max_drawdown: 0.0,
            sharpe_ratio: 0.0,
        };
    }

    let n = close.len();
    let evaluator = S::evaluator::<T>(cache, params);

    let mut usdt: T = cfg.starting_capital;
    let mut qty: T = T::ZERO;
    let mut current = Position::Flat;
    let mut prev_value: T = usdt;

    let mut portfolio_values = Vec::with_capacity(n);
    portfolio_values.push(usdt);
    let mut returns = Vec::with_capacity(n - 1);

    // Pre-slice the trade and mark price views so the per-bar access goes
    // through bounds-check-free iterators. The strategy's `evaluator` reads
    // `bar_index` (the lookback) directly from its hoisted slices.
    let trades = match cfg.execution_model {
        ExecutionModel::NextOpen => &open[1..],
    };
    let marks = &close[1..];

    for (bar_index, (&trade_price, &mark_price)) in
        trades.iter().zip(marks.iter()).enumerate()
    {
        let signal = evaluator(bar_index);
        match (current, signal) {
            (Position::Flat, Signal::EnterLong) => {
                qty = usdt / trade_price * (T::ONE - cfg.fee_rate);
                usdt = T::ZERO;
                current = Position::Long;
            }
            (Position::Long, Signal::ExitLong) => {
                usdt = qty * trade_price * (T::ONE - cfg.fee_rate);
                qty = T::ZERO;
                current = Position::Flat;
            }
            _ => {}
        }

        let value = match current {
            Position::Long => qty * mark_price,
            Position::Flat => usdt,
        };
        portfolio_values.push(value);
        if prev_value > T::ZERO {
            returns.push((value - prev_value) / prev_value);
        }
        prev_value = value;
    }

    let final_value = portfolio_values.last().copied().unwrap().to_f64();
    let max_drawdown = max_drawdown(&portfolio_values);
    let sharpe = sharpe_ratio(&returns, T::ZERO, cfg.periods_per_year);

    let metrics = BacktestMetrics {
        final_value,
        max_drawdown,
        sharpe_ratio: sharpe,
    };
    debug_assert!(
        metrics.sharpe_ratio.is_finite(),
        "non-finite sharpe leaked from backtest"
    );
    metrics
}

fn run_precision_sweep_impl<S: Strategy, T: BacktestFloat>(
    pool: &rayon::ThreadPool,
    engine: &EngineConfig,
    strategy_config: &S::Config,
    open: &[T],
    close: &[T],
    parameter_set: &[S::Params],
) -> anyhow::Result<PrecisionRun<S::Params>> {
    println!("Calculating all indicators for {}...", ACTIVE_PRECISION);
    let cache = S::build_cache::<T>(open, close, strategy_config);
    let backtest_config = numeric_backtest_config::<T>(engine);

    println!("Calculated all indicators for {}.", ACTIVE_PRECISION);
    if engine.show_progress {
        println!(
            "Running all backtests on {} threads with {}...",
            engine.threads, ACTIVE_PRECISION
        );
    }

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
                        let metrics = run_one::<S, T>(open, close, &cache, params, backtest_config);

                        let count = progress_counter.fetch_add(1, AtomicOrdering::Relaxed) + 1;
                        if engine.show_progress
                            && (count.is_multiple_of(engine.progress_step)
                                || count == total_iterations)
                        {
                            let percentage =
                                (count as f32 / total_iterations as f32) * 100.0;
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
        .with_context(|| {
            format!("strategy '{}' produced an empty parameter sweep", S::NAME)
        })?;

    Ok(PrecisionRun {
        precision: ACTIVE_PRECISION,
        best,
        duration: start.elapsed(),
    })
}

#[cfg(all(feature = "f32", not(feature = "f64")))]
fn to_active_floats(prices: &[f32]) -> Vec<Float> {
    prices.to_vec()
}

#[cfg(all(feature = "f64", not(feature = "f32")))]
fn to_active_floats(prices: &[f32]) -> Vec<Float> {
    prices.iter().copied().map(f64::from).collect()
}

pub fn run<S: Strategy>(
    engine: &EngineConfig,
    strategy_config: &S::Config,
    market: &CandleSeries,
) -> anyhow::Result<PrecisionRun<S::Params>> {
    let pool = ThreadPoolBuilder::new()
        .num_threads(engine.threads)
        .build()
        .context("failed to construct rayon thread pool")?;

    let parameter_set = S::enumerate_params(strategy_config);

    let open = to_active_floats(&market.open_prices);
    let close = to_active_floats(&market.close_prices);

    run_precision_sweep_impl::<S, Float>(
        &pool,
        engine,
        strategy_config,
        &open,
        &close,
        &parameter_set,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategy::double_ema::DoubleEmaCrossover;
    use crate::ta_wrapper::EMAStore;

    fn num_cfg<T: BacktestFloat>(starting: f32) -> NumericBacktestConfig<T> {
        NumericBacktestConfig {
            periods_per_year: periods_per_year(Level::Hour4),
            starting_capital: T::from_f32(starting),
            fee_rate: T::from_f32(0.0015),
            execution_model: ExecutionModel::NextOpen,
        }
    }

    #[test]
    fn run_one_waits_for_the_next_bar_after_a_signal() {
        let open_prices = vec![100.0_f32, 100.0, 200.0];
        let close_prices = vec![100.0_f32, 100.0, 100.0];
        let ema_fast = vec![f32::NAN, 2.0, 2.0];
        let ema_slow = vec![f32::NAN, 1.0, 1.0];
        let cache = EMAStore::<f32>::from_series(1, vec![ema_fast, ema_slow]);

        let metrics = run_one::<DoubleEmaCrossover, f32>(
            &open_prices,
            &close_prices,
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
        let cache = EMAStore::<f32>::from_series(1, vec![ema_fast, ema_slow]);

        let metrics = run_one::<DoubleEmaCrossover, f32>(
            &open_prices,
            &close_prices,
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
        let cache = EMAStore::<f64>::from_series(1, vec![ema_fast, ema_slow]);

        let metrics = run_one::<DoubleEmaCrossover, f64>(
            &open_prices,
            &close_prices,
            &cache,
            (1, 2),
            num_cfg::<f64>(1000.0),
        );

        assert!((metrics.final_value - 499.25).abs() < 1e-6);
    }

    /// Engine-level test using a `TestStrategy` with a fixed signal sequence.
    /// Exercises the position-transition match independently of any indicator.
    #[test]
    fn run_one_engine_transitions_match_test_strategy_signals() {
        struct TestStrategy;
        impl Strategy for TestStrategy {
            type Params = ();
            type Cache<T: BacktestFloat> = Vec<Signal>;
            type Config = Vec<Signal>;
            const NAME: &'static str = "test";

            fn build_cache<T: BacktestFloat>(_: &[T], _: &[T], cfg: &Self::Config)
                -> Self::Cache<T>
            { cfg.clone() }

            fn enumerate_params(_: &Self::Config) -> Vec<Self::Params> { vec![()] }

            fn evaluator<'a, T: BacktestFloat>(cache: &'a Self::Cache<T>, _: ())
                -> impl Fn(usize) -> Signal + 'a
            { move |i| cache[i] }

            fn param_summary(_: ()) -> String { String::new() }
            fn tie_break(_: (), _: ()) -> Ordering { Ordering::Equal }
        }

        // 4 bars; engine reads signals at bar_index = 0..=2 (n - 1 = 3 iters).
        // i=0: Hold → no transition. value = usdt = 1000.
        // i=1: EnterLong → enter at open[2]=200. qty = 1000/200*0.9985 = 4.9925.
        //      value = qty * close[2] = 4.9925 * 200 = 998.5.
        // i=2: Hold → no transition. value = qty * close[3] = 4.9925 * 100 = 499.25.
        let open_prices = vec![100.0_f32, 100.0, 200.0, 100.0];
        let close_prices = vec![100.0_f32, 100.0, 200.0, 100.0];
        let cache: Vec<Signal> = vec![Signal::Hold, Signal::EnterLong, Signal::Hold];

        let metrics = run_one::<TestStrategy, f32>(
            &open_prices,
            &close_prices,
            &cache,
            (),
            num_cfg::<f32>(1000.0),
        );

        assert!((metrics.final_value - 499.25).abs() < 1e-3,
            "final = {}", metrics.final_value);
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
            metrics: BacktestMetrics { final_value, max_drawdown: 0.0, sharpe_ratio: sharpe },
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
    fn prefer_prefers_finite_sharpe_over_nan() {
        let finite = sweep::<(usize, usize)>((1, 2), -10.0, 100.0);
        let nan = sweep::<(usize, usize)>((3, 4), f64::NAN, 100.0);
        assert_eq!(prefer::<DoubleEmaCrossover>(finite, nan), finite);
        assert_eq!(prefer::<DoubleEmaCrossover>(nan, finite), finite);
    }
}
