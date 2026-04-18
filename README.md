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
- Precision: `f32` by default for the expensive sweep and EMA storage

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
  The CSV includes the selected precision and the measured sweep duration in milliseconds.

Useful commands:

```bash
cargo test
```

Precision switching is available through environment variables:

```bash
# Run the expensive sweep in f64 instead of f32.
$env:BACKTEST_PRECISION = "f64"
cargo run --release

# Compare f32 and f64 on the same cached candle input.
$env:BACKTEST_COMPARE_PRECISIONS = "1"
$env:BACKTEST_SHOW_PROGRESS = "0"
cargo run --release
```

## Files this program touches

- `dataKLines/BTC-USDT-4h.json`: cached market data for the current default configuration.
- `results/BTC-USDT-4h.csv`: appended run history for the current default configuration.

## Configuration

The current binary is configured in source code rather than via CLI flags or a config file.

Values you are most likely to change live in `src/main.rs`:

- `default_run_config()`: pair, timeframe, Rayon thread count, EMA search range, starting capital, fee, and execution model
- `BACKTEST_PRECISION`: optional env override for `f32` or `f64`
- `BACKTEST_COMPARE_PRECISIONS`: optional env override to benchmark both precisions on the same backtest
- `BACKTEST_SHOW_PROGRESS`: optional env override to suppress progress logging during comparisons

## Notes on numeric precision

- The expensive EMA sweep is generic over `f32` and `f64`.
- `f32` mode keeps the hot-loop data arrays and EMA storage in `f32`, which uses less memory and was faster on the default run below.
- `f64` mode widens the hot-loop data arrays and EMA storage to `f64`, which is useful when you want less cumulative rounding drift in the sweep.
- The candle input values are the same between both runs; only the arithmetic/storage precision differs inside the expensive loop.

Quick comparison from `cargo run --release` with:

- default `BTC-USDT` / `4h` sweep
- same cached candle file
- progress logging disabled

Observed on this machine:

| Precision | Duration | Best periods | Final value | Sharpe | Max drawdown |
| --- | ---: | --- | ---: | ---: | ---: |
| `f32` | `1.948s` | `(29, 148)` | `45259.496` | `1.440853` | `45.6798%` |
| `f64` | `2.694s` | `(29, 148)` | `45282.640` | `1.441065` | `45.5372%` |

Quick comment:

- In this repo's current default run, `f64` was about `38%` slower than `f32`.
- The winning EMA pair stayed the same.
- The result difference was small but non-zero: about `+23.144 USDT` final value and `+0.000211` Sharpe for `f64`.
- Treat these numbers as machine- and dataset-specific, not universal benchmark claims.

- `backtest_rust_f16.zip` is best treated as an archived experiment, not part of the current build or documented runtime path.
