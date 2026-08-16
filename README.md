# backtest_rust

A Rust backtester that downloads Binance candles, brute-forces a strategy's
parameter space over them, and reports the best result by Sharpe ratio.

```bash
cargo run --release                                  # default: double_ema on BTC-USDT 15m
cargo run --release -- list-strategies               # what else is available
cargo run --release -- --strategy rsi_reversion --param period=5..40 --split 0.7
```

## What it does

- Downloads (or reuses) OHLCV candles for one pair and timeframe.
- Precomputes the strategy's indicators once, across the whole parameter range.
- Runs one backtest per parameter tuple, in parallel, and keeps the best by
  Sharpe ratio.
- Prints the winner and appends it to `results/<pair>-<level>_v3.csv`.

Execution model: the signal is read from bar *i*, the fill happens at
`open[i+1]`, and the position is marked at `close[i+1]`. There is no
look-ahead. Fees are charged on both sides of a trade; there is no slippage or
minimum-notional model. Positions are long or flat — no shorting, no leverage,
no stop-losses.

## Prerequisites

- Rust 1.87 or newer (see `rust-version` in `Cargo.toml`).
- Internet access on the first run for a given pair/timeframe, unless the
  candle file is already in `dataKLines/`.
- Binance **spot** symbols only; `*-SWAP` / futures are rejected with a clear
  error.

## Strategies

| Name | Idea | Swept parameters |
| --- | --- | --- |
| `double_ema` *(default)* | Long while the fast EMA is above the slow EMA | `fast_min`, `slow_min`, `max_period` |
| `price_vs_ema` | Long while the close is above one EMA | `period` |
| `rsi_reversion` | Buy oversold, sell overbought | `period`, `oversold`, `overbought` |
| `stoch_rsi` | Same, on the StochRSI `%K` line | `stoch_period`, `oversold`, `overbought`, plus fixed `rsi_period`, `k_smooth`, `d_smooth` |
| `bb_reversion` | Buy below the lower Bollinger band, exit at the mean | `period`, `deviation_tenths` |
| `macd_cross` | Long while the MACD histogram is positive | `fast`, `slow`, `signal` |
| `supertrend` | Long while Supertrend points up | `period`, `multiplier_tenths` |

`list-strategies` prints the same table with each parameter's default.

Parameters are given as `--param name=value`, repeatable. A value is either a
range to sweep (`period=5..40`) or a single number that pins it
(`period=14`). An unrecognised name is an error listing the valid ones.

## Indicators

All in `src/indicators/`, all O(n) in the series length regardless of period
(the one exception is noted):

| Module | Indicators |
| --- | --- |
| `ma` | SMA, EMA, WMA, RMA (Wilder), DEMA, TEMA, HMA |
| `rolling` | rolling min / max (monotonic deque), population stddev, mean absolute deviation |
| `momentum` | RSI, Stochastic RSI, Stochastic `%K`/`%D`, MACD, ROC, CCI, Williams %R |
| `volatility` | true range, ATR, Bollinger Bands, Keltner Channels, Donchian Channels, Supertrend |
| `trend` | ADX / +DI / −DI, Parabolic SAR |

Two conventions hold throughout:

- **Warmup is `NaN`.** Every indicator returns a series the length of its
  input, `NaN` where it is not yet defined. The engine relies on this — every
  comparison against `NaN` is false, so a strategy emits `Hold` through the
  warmup with no explicit guard.
- **A leading `NaN` run in the *input* is skipped, not propagated.** That is
  what lets indicators compose: HMA is a WMA of a difference of WMAs, StochRSI
  is a stochastic of an RSI, a MACD signal line is an EMA of a MACD line.

CCI is the exception to the O(n) rule: its mean-absolute-deviation denominator
has no rolling form, so it costs O(n · period) and is not suited to a wide
period sweep.

Volume is carried through the pipeline but no volume indicator ships yet;
OBV, MFI and VWAP would each be one function in a new `indicators/volume.rs`.

## Extending it

### Adding an indicator

1. Write one function in the matching `src/indicators/` submodule:
   ```rust
   pub fn my_indicator(bars: &BarsF64, period: usize) -> Vec<f64>
   ```
   or `fn my_indicator(values: &[f64], period: usize) -> Vec<f64>` if it only
   reads one column. Start from `nan_series(len)` and use `valid_tail` to skip
   the input's warmup.
2. Add a test pinning it against hand-computed values, plus whatever invariant
   it should never violate (bounds, non-negativity, warmup length).

`PeriodCache` can then precompute it across a whole period range with no
further plumbing.

### Adding a strategy

1. Implement `Strategy` and `ConfigurableStrategy` in a new
   `src/strategy/` module.
2. Add one line to the `registry!` invocation in `src/strategy/registry.rs`.

Nothing in the engine, the CLI or the output layer changes, and
`registry::tests::every_registered_strategy_runs_end_to_end` will exercise it
automatically.

**Choosing a cache.** Use `PeriodCache` when parameter tuples sharing a period
can share one indicator series — the two-EMA sweep has ~180k combinations but
only 600 distinct series. Use `SeriesCache` when each tuple needs its own
series (Supertrend's bands ratchet; a MACD histogram depends on all three of
its periods). `SeriesCache` refuses a grid that would not fit in memory rather
than letting the process die.

## Data and caching

- `dataKLines/<pair>-<level>.feather` — cached candles (Apache Arrow IPC).
  A legacy `.json` cache is migrated on first load. Caches written before
  volume support (five columns) still load, with volume as `NaN`, and backfill
  on the next download.
- `results/<pair>-<level>_v3.csv` — appended run history. The version suffix
  isolates schema changes: the header is only written for an empty file, so a
  new column set has to mean a new filename.

Freshness is measured from the **last candle inside the file**, not the file's
mtime, and the threshold scales with the timeframe (two bar intervals, floored
at two minutes) so a 15m backtest cannot silently run on day-old data. When the
cache is stale, only the missing slice is fetched.

The download path deliberately **drops the currently forming candle** — Binance
will happily return it, and writing a half-formed bar into the cache freezes it
into history. The incremental fetch also re-reads the newest cached candle and
prefers the fresh copy, so a bar left stale by an older build repairs itself.
To rebuild a cache outright, use `--force`.

Downloads retry with exponential backoff, honour `Retry-After` on 429/418, and
fail fast on a 4xx that will never succeed (a bad symbol).

## CLI reference

```
Subcommands:
  download              Download klines, then exit. Always re-downloads.
  list-strategies       Print the strategies and their parameters, then exit

Options:
  --strategy <NAME>     Strategy to sweep (default: double_ema)
  --param <NAME=VALUE>  Strategy parameter; repeatable. Ranges are min..max
  --pair <BASE-QUOTE>   Trading pair (default: BTC-USDT)
  --level <INTERVAL>    1m 3m 5m 15m 30m 1h 2h 4h 6h 12h 1d 3d 1w 1M (default: 15m)
  --threads <N>         Rayon worker threads; 0 = auto (default: 1)
  --split <FRACTION>    Optimize on the leading FRACTION, report on the rest
  --force               Bypass the freshness guard and re-download
  --since <DATE|MS>     Override download start (YYYY-MM-DD or unix-ms)
  --data-dir <PATH>     Kline cache directory (default: dataKLines)
  --results-dir <PATH>  Results CSV directory (default: results)
  -h, --help            Show this message

Environment:
  BACKTEST_SHOW_PROGRESS=0|1   Toggle progress logging
  BACKTEST_FORCE_DOWNLOAD=0|1  Alternative to --force
```

**Full history.** Binance's BTC/USDT spot pair started trading on 2017-08-17,
so `--since 2017-08-17` (or earlier — the API clamps to its own first candle)
yields everything it will serve. A full 15m history from 2017 is roughly 300k
candles; the initial pull takes on the order of a minute, and subsequent runs
fetch only the delta.

## Reading the results

`--split` exists because a Sharpe argmax over ~180k parameter points on a
single series is an in-sample result by construction. With it, the winner is
chosen on the leading fraction of the data and re-scored on the held-out tail;
the tail is the number worth trusting. Indicators are still built over the
whole series, so the held-out segment starts with its warmup already behind it.

Reported alongside Sharpe: max drawdown, Sortino, Calmar, CAGR, trade count,
win rate and exposure. Exposure in particular is worth a look — a great Sharpe
at 2% exposure is a different animal from one at 90%. A position still open at
the last bar is not counted as a trade.

Sortino is reported as `inf` for a run that never had a losing bar; that is the
honest value, and it cannot win a sweep because ranking is on Sharpe alone.

## Precision

The `f32` (default) and `f64` features select the **storage** precision of the
indicator cache and the equity path. Indicator arithmetic runs in `f64`
regardless: it happens once per run, while the sweep reads the stored arrays
hundreds of thousands of times, so the storage width is what governs cost.

```bash
cargo run --release                                          # f32
cargo run --release --no-default-features --features f64     # f64
```

The two are mutually exclusive; enabling both, or neither, is a compile error.
`--all-features` therefore cannot be used, in CI or anywhere else.

## Layout

| Path | Role |
| --- | --- |
| `src/main.rs` | CLI parsing and orchestration. Names no concrete strategy. |
| `src/backtest.rs` | The sweep, the per-bar engine, `EngineConfig`, `BacktestMetrics` |
| `src/strategy/` | The `Strategy` trait, the registry, and each strategy |
| `src/indicators/` | Indicator library, `PeriodCache`, `SeriesCache` |
| `src/metrics.rs` | `EquityStats` and the ratio calculations |
| `src/exchange.rs`, `src/download.rs` | Binance client, pagination, caching policy |
| `src/feather.rs`, `src/data.rs` | Cache format, `CandleSeries`, `Bars`, `DataPaths` |
| `src/output.rs` | Results CSV |
| `tests/golden.rs` | Pins the `double_ema` result on the committed fixture |

## Development

```bash
cargo test
cargo test --no-default-features --features f64
cargo clippy --all-targets -- -D warnings
cargo fmt --check
cargo run --release --example bench_sweep -- 200 1     # sweep timing
```

`tests/golden.rs` is the safety net for engine changes: it pins the winning
parameters and metrics of `double_ema` on the committed `BTC-USDT-4h` fixture.
Parameters are asserted exactly; metrics carry a tolerance sized for a change
of accumulation scheme, not for a change of behaviour. If it fails, something
about the simulation changed — work out what before re-recording it.

## Performance notes

The release profile uses `opt-level = 3`, fat LTO, a single codegen unit, and
`target-cpu=native` (via `.cargo/config.toml`), so binaries are not portable to
older CPUs — rebuild on the target machine.

The sweep is memory-bandwidth-bound: its working set is the indicator cache,
which for a 600-period EMA sweep over 200k candles is hundreds of megabytes.
That is why the cache is one flat strided buffer rather than a `Vec<Vec<_>>`,
why per-bar statistics are accumulated as running scalars rather than
materialized, and why `f32` storage is the default.
