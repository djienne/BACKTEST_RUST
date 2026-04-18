//! Strategy trait + supporting types.
//!
//! The engine in `crate::backtest` is generic over `Strategy`. A strategy
//! owns its parameter space, its indicator cache, and a per-bar evaluator
//! closure that emits a `Signal` for each bar.
//!
//! Strategies are zero-sized marker types. The engine calls associated
//! functions (no `&self`); monomorphization collapses the trait dispatch
//! to direct calls in the hot loop.

use crate::precision::BacktestFloat;
use std::cmp::Ordering;

pub mod double_ema;
pub mod single_ema;

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

    fn build_cache<T: BacktestFloat>(
        open: &[T],
        close: &[T],
        cfg: &Self::Config,
    ) -> Self::Cache<T>;

    fn enumerate_params(cfg: &Self::Config) -> Vec<Self::Params>;

    /// Build a per-bar evaluator closure with strategy-private lookups
    /// (e.g. slice-by-period) hoisted out of the hot loop. The engine
    /// calls the returned closure once per bar.
    fn evaluator<'a, T: BacktestFloat>(
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
