//! Generic backtesting framework and exchange adapters shared with the active binary.
//! The repository's executable parameter sweep still lives in `src/main.rs`.
#![allow(dead_code)]
#![allow(clippy::too_many_arguments)]

mod backtester;
mod base;
mod exchange;
mod match_engine;
mod util;

#[allow(unused_imports)]
pub use backtester::*;
pub use base::*;
pub use exchange::*;
pub use match_engine::*;
pub use util::*;
