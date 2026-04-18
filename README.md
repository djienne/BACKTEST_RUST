# backtest_rust

`backtest_rust` is a Rust backtester that downloads or reuses Binance candlestick data, brute-forces double-EMA parameter pairs for one market and timeframe, and reports the best result by Sharpe ratio.

The codebase is split into small modules:

- `src/main.rs` is the binary entry point: reads env vars, downloads/loads candles, calls `backtest::run`, prints + writes results.
- `src/backtest.rs` owns the sweep, backtest loop, and the `RunConfig`/`RunReport` types.
- `src/exchange.rs`, `src/download.rs`, `src/data.rs`, `src/output.rs`, `src/metrics.rs`, `src/ta_wrapper.rs`, `src/precision.rs` are focused single-responsibility modules.
- `src/lib.rs` re-exports the modules so `tests/integration.rs` can drive `run()` end-to-end against synthetic data without touching the network.

The current default run is hardcoded to:

- Pair: `BTC-USDT`
- Timeframe: `15m`
- Threads: `1` (single-threaded for repeatable benchmarking)
- EMA search space: fast `5..=600`, slow `6..=600`, with `slow > fast`
- Starting capital: `1000 USDT`
- Trading fee: `0.15%` on buys and sells
- Execution model: signal from the previous close, execution at the next bar open
- Precision: `f32` by default for the expensive sweep and EMA storage (compile-time switch)

## Strategy summary

This is a simple spot-style EMA crossover backtest:

- It precomputes EMA values for the configured close-price series.
- It enters a full position when the previous bar's fast EMA is above the slow EMA.
- It exits the full position when the previous bar's fast EMA is below the slow EMA.
- It executes trades at the next bar open and marks portfolio value at the bar close.
- It evaluates every allowed EMA pair and keeps the best result by Sharpe ratio.

The current data download defaults to starting at `2019-01-01T00:00:00Z`; this can be overridden at the command line via `--since` (see [Downloading historical data](#downloading-historical-data)).

## Prerequisites

- Rust with Cargo installed.
- Internet access for the first run, unless the needed candle file is already present in `dataKLines/`.
- Only Binance **spot** products are supported. `*-SWAP` / futures symbols are rejected with a clear error.

## Quick start

Run the backtest with the default `f32` precision:

```bash
cargo run --release
```

Run it with `f64` precision instead (compile-time selection):

```bash
cargo run --release --no-default-features --features f64
```

The two features (`f32` and `f64`) are mutually exclusive; the build fails with a clear error if both are enabled or neither is.

What happens during a normal run:

- The binary looks for `dataKLines/BTC-USDT-15m.json`.
- Freshness is measured from the **last candle timestamp inside the file**, not the file's mtime. If that last candle is more than two days old (or the file is missing), Binance is queried for a fresh pull. Pass `--force` or set `BACKTEST_FORCE_DOWNLOAD=1` to skip this check.
- If the download fails but the cache file already exists, it falls back to the cached file.
- The cache is normalized on load: candles are sorted ascending by timestamp and any duplicate timestamps are dropped. If the file needed fixing, it is rewritten in place and a one-line note is printed to stderr.
- It prints the candle date range, computes indicators, runs the EMA search, and reports the best result.
- It appends the latest best-result row to `results/BTC-USDT-15m.csv`.
  The CSV includes the active precision (`f32` or `f64`) and the measured sweep duration in milliseconds.

Useful commands:

```bash
cargo test
cargo test --no-default-features --features f64
```

The optional `BACKTEST_SHOW_PROGRESS` env var (`0`/`1`) toggles the per-iteration progress log without rebuilding. `BACKTEST_FORCE_DOWNLOAD=1` is an env-level shortcut for `--force`.

## Downloading historical data

To pull fresh kline data **without running the sweep**, use the `download` subcommand:

```bash
cargo run --release -- download
```

This always re-downloads (bypasses the freshness guard), writes `dataKLines/<pair>-<level>.json`, then exits.

Override the start of the window with `--since`. Either a calendar date or a unix-milliseconds timestamp is accepted:

```bash
cargo run --release -- download --since 2017-08-17
cargo run --release -- download --since 1502928000000
```

**Maximum available history for BTC-USDT.** Binance's BTC/USDT spot pair started trading on **2017-08-17**, so passing `--since 2017-08-17` (or anything earlier — the API clamps to its own earliest candle) yields the full history Binance will serve for that symbol. Other pairs may have later launch dates; Binance will simply return the earliest candles it has. Note: a full 15-minute history from 2017 is roughly 300k candles and a multi-megabyte JSON file, and the pagination loop adds a small inter-page delay, so an initial full pull takes on the order of a minute.

Force a re-download inside the normal sweep run with `--force` (or `BACKTEST_FORCE_DOWNLOAD=1`):

```bash
cargo run --release -- --force
BACKTEST_FORCE_DOWNLOAD=1 cargo run --release
```

Full CLI reference:

```bash
cargo run --release -- --help
```

## Files this program touches

- `dataKLines/BTC-USDT-15m.json`: cached market data for the current default configuration. Rewritten in place if the loader detects out-of-order or duplicate timestamps.
- `results/BTC-USDT-15m.csv`: appended run history for the current default configuration.

## Configuration

The current binary is configured in source code rather than via CLI flags or a config file.

Values you are most likely to change live in `src/main.rs`:

- `default_run_config()`: pair, timeframe, Rayon thread count, EMA search range, starting capital, fee, and execution model
- Precision is selected at compile time via the `f32` / `f64` cargo features (default: `f32`)
- `BACKTEST_SHOW_PROGRESS`: optional env override to suppress progress logging

## Notes on numeric precision

- The expensive EMA sweep is generic over `f32` and `f64`, but each binary build monomorphizes only the active precision (no dead code, no runtime dispatch).
- `f32` mode keeps the hot-loop data arrays and EMA storage in `f32`, which uses less memory.
- `f64` mode widens the hot-loop data arrays and EMA storage to `f64`, which is useful when you want less cumulative rounding drift in the sweep.
- The candle input values are the same between both builds; only the arithmetic/storage precision differs inside the expensive loop.
- To compare precisions, build and run twice — once per feature — and compare the printed `Sweep duration` and best result.

## Optimization

The release profile is tuned for maximum throughput:

- `opt-level = 3`
- `lto = "fat"` — whole-program inlining across crates
- `codegen-units = 1` — single codegen unit for fewer optimization boundaries
- `strip = true` — drop debug symbols from the release binary
- `.cargo/config.toml` sets `rustflags = ["-C", "target-cpu=native"]` so the build uses the host CPU's full instruction set (e.g., AVX2/AVX-512 if present). Resulting binaries are not portable to older CPUs — rebuild on the target machine.

Sample run on the cached `BTC-USDT 15m` data (2019-01-01 → 2024-05-09, ~187k candles), single-threaded, built with the optimization profile above:

| Precision | Build command | Sweep duration | Best periods | Final value | Sharpe | Max drawdown |
| --- | --- | ---: | --- | ---: | ---: | ---: |
| `f32` | `cargo run --release` | `124.681s` | `(243, 249)` | `14829.247` | `1.296941` | `68.36%` |
| `f64` | `cargo run --release --no-default-features --features f64` | `220.298s` | `(207, 290)` | `14516.753` | `1.290675` | `68.56%` |

On this dataset f64 is roughly 1.77× slower than f32 — the sweep is dominated by memory bandwidth (the EMA store grows to ~hundreds of MB), so widening the data words doubles the working set and pushes more traffic through the cache hierarchy. The two precisions select different EMA pairs because the search space is huge and many pairs have very close Sharpe ratios; small rounding differences are enough to tip the ranking.

Treat these numbers as machine- and dataset-specific, not universal benchmark claims.

- `backtest_rust_f16.zip` is best treated as an archived experiment, not part of the current build or documented runtime path.
