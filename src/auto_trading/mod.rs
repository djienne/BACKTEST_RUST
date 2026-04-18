//! Generic backtesting framework and exchange adapters shared with the active binary.
//! The repository's executable parameter sweep still lives in `src/main.rs`.
#![cfg_attr(test, allow(dead_code))]
#![allow(clippy::too_many_arguments)]

#[cfg(test)]
mod backtester;
mod base;
mod exchange;
#[cfg(test)]
mod match_engine;
mod util;

#[cfg(test)]
pub use base::*;
#[cfg(not(test))]
pub use base::{Level, TimeRange, K};
#[cfg(test)]
pub use exchange::*;
#[cfg(not(test))]
pub use exchange::{Binance, Exchange};
#[cfg(test)]
pub use match_engine::*;
#[cfg(test)]
pub use util::*;
#[cfg(not(test))]
pub use util::{get_k_range, product_mapping, time_to_string};
