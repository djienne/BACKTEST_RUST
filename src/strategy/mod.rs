//! Strategy trait + supporting types.
//!
//! The engine in `crate::backtest` is generic over `Strategy`. A strategy
//! owns its parameter space, its indicator cache, and a per-bar evaluator
//! closure that emits a `Signal` for each bar.
//!
//! Strategies are zero-sized marker types. The engine calls associated
//! functions (no `&self`); monomorphization collapses the trait dispatch
//! to direct calls in the hot loop.

use crate::data::Bars;
use crate::precision::BacktestFloat;
use crate::strategy::params::ParamSpec;
use std::cmp::Ordering;

pub mod bb_reversion;
pub mod double_ema;
pub mod macd_cross;
pub mod params;
pub mod registry;
pub mod rsi_reversion;
pub mod single_ema;
pub mod stoch_rsi;
pub mod supertrend;

/// Engine-tracked position state.
///
/// `Short` is intentionally omitted; adding it is one variant + new arms in
/// the engine's transition match.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Position {
    Flat,
    Long,
}

/// What the strategy wants to do this bar.
///
/// `Hold` means "no change" — the engine ignores it and keeps the current
/// position. `EnterLong` is only acted on when currently `Flat`; `ExitLong`
/// only when currently `Long`. Equal/NaN indicator values map to `Hold`,
/// preserving the pre-refactor behavior of the EMA crossover engine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Signal {
    Hold,
    EnterLong,
    ExitLong,
}

pub trait Strategy {
    type Params: Copy + Send + Sync + 'static + std::fmt::Debug + PartialEq;
    type Cache<T: BacktestFloat>: Send + Sync;
    type Config: Send + Sync;

    const NAME: &'static str;

    /// Precompute every indicator the parameter sweep will read. Called once
    /// per run; the result is shared read-only across all worker threads.
    ///
    /// Fallible so a strategy can refuse a configuration it cannot serve —
    /// notably one whose cache would not fit in memory (see
    /// `indicators::SeriesCache`) — rather than dying mid-sweep.
    fn build_cache<T: BacktestFloat>(
        bars: Bars<'_, T>,
        cfg: &Self::Config,
    ) -> anyhow::Result<Self::Cache<T>>;

    fn enumerate_params(cfg: &Self::Config) -> Vec<Self::Params>;

    /// Build a per-bar evaluator closure with strategy-private lookups
    /// (e.g. slice-by-period) hoisted out of the hot loop. The engine
    /// calls the returned closure once per bar.
    ///
    /// Raw price columns arrive via `bars`, so a cache only ever holds
    /// *derived* series — no strategy needs to copy the close series to read
    /// it back later.
    fn evaluator<'a, T: BacktestFloat>(
        bars: Bars<'a, T>,
        cache: &'a Self::Cache<T>,
        params: Self::Params,
    ) -> impl Fn(usize) -> Signal + 'a;

    /// Human-readable serialization for CSV/println.
    fn param_summary(params: Self::Params) -> String;

    /// Tie-break used by the sweep when sharpe + final_value are equal.
    /// Returning `Less` means `left` wins; `Greater` means `right` wins;
    /// `Equal` means the engine picks `left` deterministically.
    fn tie_break(left: Self::Params, right: Self::Params) -> Ordering;
}

/// A strategy the CLI can name and configure.
///
/// Splitting this from [`Strategy`] keeps the engine's contract free of
/// anything CLI-shaped: a strategy used only from Rust needs `Strategy` alone.
pub trait ConfigurableStrategy: Strategy {
    /// One line for `list-strategies`.
    const DESCRIPTION: &'static str;
    /// The `--param` names this strategy reads, with their defaults.
    const PARAMETERS: &'static [(&'static str, &'static str)];

    /// Build the strategy's config from `--param` entries, applying defaults.
    /// Implementations should call [`ParamSpec::reject_unknown`] so a typo is
    /// reported rather than ignored.
    fn config_from(spec: &ParamSpec) -> anyhow::Result<Self::Config>;

    /// The `--param` names, for the unknown-name check and for help text.
    fn parameter_names() -> Vec<&'static str> {
        Self::PARAMETERS.iter().map(|(name, _)| *name).collect()
    }
}
