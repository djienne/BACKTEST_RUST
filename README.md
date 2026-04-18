# backtest_rust

`backtest_rust` is a Rust backtester that downloads or reuses Binance candlestick data, brute-forces double-EMA parameter pairs for one market and timeframe, and reports the best result by Sharpe ratio.

The current default run is hardcoded to:

- Pair: `BTC-USDT`
- Timeframe: `4h`
- Threads: `12`
- EMA search space: fast `5..=600`, slow `6..=600`, with `slow > fast`
- Starting capital: `1000 USDT`
- Trading fee: `0.15%` on buys and sells

## Strategy summary

This is a simple spot-style EMA crossover backtest:

- It precomputes EMA values for the configured close-price series.
- It enters a full position when the previous bar's fast EMA is above the slow EMA.
- It exits the full position when the previous bar's fast EMA is below the slow EMA.
- It evaluates every allowed EMA pair and keeps the best result by Sharpe ratio.

The current data download starts at `2019-01-01T00:00:00Z`.

## Prerequisites

- Rust with Cargo installed.
- Internet access for the first run, unless the needed candle file is already present in `dataKLines/`.
- If you build for the `x86_64-pc-windows-gnu` target, this repo expects the `x86_64-w64-mingw32-gcc` linker because that target is configured in `Cargo.toml`.
- If you use a different Windows toolchain such as MSVC, you may need to adjust or remove that target-specific linker setting.

## Quick start

Run the backtest with:

```bash
cargo run --release
```

What happens during a normal run:

- The binary looks for `dataKLines/BTC-USDT-4h.json`.
- If the file is missing or older than 2 days, it tries to download fresh Binance candles.
- If the download fails but the cache file already exists, it falls back to the cached file.
- It prints the candle date range, computes indicators, runs the EMA search, and reports the best result.
- It overwrites `results.csv` in the repository root with the latest best-result row.

Useful commands:

```bash
cargo test
```

## Files this program touches

- `dataKLines/BTC-USDT-4h.json`: cached market data for the current default configuration.
- `results.csv`: the latest backtest result written by the binary.

## Configuration

The current binary is configured in source code rather than via CLI flags or a config file.

Values you are most likely to change live in `src/main.rs`:

- `PAIR`: trading pair to backtest
- `LEVEL`: candle timeframe
- `NB_THREADS`: Rayon thread count for the parameter sweep
- Download start timestamp: passed to `download_dump_k_lines_to_json(...)`
- EMA search range: currently hardcoded in the loops and EMA store setup
- Starting capital and fee: currently hardcoded in `backtest_double_ema(...)`

## Notes on numeric precision

- The active code path uses `f32` for indicator and portfolio calculations.
- This README does not claim benchmark numbers for `f32` versus `f64`, because the current repository does not include reproducible benchmark results.
- `backtest_rust_f16.zip` is best treated as an archived experiment, not part of the current build or documented runtime path.
