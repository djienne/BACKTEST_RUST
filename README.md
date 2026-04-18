# backtest_rust

`backtest_rust` is a Rust backtester that downloads or reuses Binance candlestick data, brute-forces double-EMA parameter pairs for one market and timeframe, and reports the best result by Sharpe ratio.

Two backtesting layers exist in the repository:

- `src/main.rs` is the active executable path used by the binary.
- `src/auto_trading/` is a more generic framework that is currently exercised through tests and shared utilities, but it is not the entry point for the brute-force sweep.

The current default run is hardcoded to:

- Pair: `BTC-USDT`
- Timeframe: `4h`
- Threads: `12`
- EMA search space: fast `5..=600`, slow `6..=600`, with `slow > fast`
- Starting capital: `1000 USDT`
- Trading fee: `0.15%` on buys and sells
- Execution model: signal from the previous close, execution at the next bar open

## Strategy summary

This is a simple spot-style EMA crossover backtest:

- It precomputes EMA values for the configured close-price series.
- It enters a full position when the previous bar's fast EMA is above the slow EMA.
- It exits the full position when the previous bar's fast EMA is below the slow EMA.
- It executes trades at the next bar open and marks portfolio value at the bar close.
- It evaluates every allowed EMA pair and keeps the best result by Sharpe ratio.

The current data download starts at `2019-01-01T00:00:00Z`.

## Prerequisites

- Rust with Cargo installed.
- Internet access for the first run, unless the needed candle file is already present in `dataKLines/`.

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
- It appends the latest best-result row to `results/BTC-USDT-4h.csv`.

Useful commands:

```bash
cargo test
```

## Files this program touches

- `dataKLines/BTC-USDT-4h.json`: cached market data for the current default configuration.
- `results/BTC-USDT-4h.csv`: appended run history for the current default configuration.

## Configuration

The current binary is configured in source code rather than via CLI flags or a config file.

Values you are most likely to change live in `src/main.rs`:

- `default_run_config()`: pair, timeframe, Rayon thread count, EMA search range, starting capital, fee, and execution model

## Notes on numeric precision

- The active code path uses `f32` for indicator and portfolio calculations.
- This README does not claim benchmark numbers for `f32` versus `f64`, because the current repository does not include reproducible benchmark results.
- `backtest_rust_f16.zip` is best treated as an archived experiment, not part of the current build or documented runtime path.
