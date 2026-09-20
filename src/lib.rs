//! A parameter-sweep backtester for Binance spot candles.
//!
//! # How a run is put together
//!
//! ```text
//! download → feather cache → CandleSeries → MarketArrays → Bars<'_, T>
//!                                                             │
//!                        Strategy::build_cache ───────────────┤
//!                        (PeriodCache / SeriesCache)          │
//!                                                             ▼
//!                        backtest::run  ──►  one run_one per parameter tuple
//!                                                             │
//!                                        prefer(): best by Sharpe  ──►  PrecisionRun
//! ```
//!
//! [`exchange`] and [`download`] fetch and cache candles; [`feather`] is the
//! on-disk format. [`data`] turns a cache into a [`data::CandleSeries`] and
//! hands out [`data::Bars`] views. [`indicators`] precomputes indicator series
//! across a parameter range. [`strategy`] turns those into per-bar signals,
//! [`backtest`] simulates them, [`metrics`] scores the equity curve, and
//! [`output`] appends the winner to a CSV.
//!
//! # Execution model
//!
//! The signal is read from bar *i*, the fill happens at `open[i + 1]`, and the
//! position is marked at `close[i + 1]`. There is no look-ahead. Fees are
//! charged on both sides; there is no slippage or minimum-notional model.
//! Positions are long or flat — no shorting, no leverage, no stop-losses.
//! The engine rejects invalid prices and gaps. The CLI first attempts to fetch
//! missing candles, applies an explicit `--since` to the in-memory window, then
//! runs the sweep. Requested holdouts are never silently disabled.
//!
//! # Precision
//!
//! The `f32` (default) and `f64` features select the **storage** precision of
//! the indicator cache and the equity path, exactly one of which must be
//! enabled. Indicator arithmetic always runs in `f64`: it happens once per run
//! while the sweep reads the stored arrays hundreds of thousands of times, so
//! the storage width is what governs cost.
//!
//! # Extending
//!
//! - **An indicator** is one function plus one test — see the [`indicators`]
//!   module docs.
//! - **A strategy** is one trait impl plus one line in the registry — see the
//!   [`strategy::registry`] module docs.

pub mod backtest;
pub mod data;
pub mod download;
pub mod exchange;
pub mod feather;
pub mod indicators;
pub mod metrics;
pub mod output;
pub mod precision;
pub mod strategy;
