use crate::download::load_k_lines;
use crate::exchange::Level;
use crate::precision::BacktestFloat;
use anyhow::Result;
use std::path::{Path, PathBuf};

/// Loaded market data, one `Vec` per column. Stored as `f32` because that is
/// the precision the exchange quotes and the cache stores; widening happens
/// once per run in [`MarketArrays`], not per candle.
#[derive(Debug, Clone, PartialEq)]
pub struct CandleSeries {
    pub timestamps: Vec<u64>,
    pub open_prices: Vec<f32>,
    pub high_prices: Vec<f32>,
    pub low_prices: Vec<f32>,
    pub close_prices: Vec<f32>,
    /// Base-asset volume. `NaN` for candles from a cache written before volume
    /// support; see `exchange::unknown_volume`.
    pub volumes: Vec<f32>,
}

impl CandleSeries {
    pub fn len(&self) -> usize {
        self.close_prices.len()
    }

    pub fn is_empty(&self) -> bool {
        self.close_prices.is_empty()
    }

    /// Whether every column has the same length. A `false` here means the
    /// loader is broken; callers turn it into an error rather than
    /// backtesting a truncated prefix.
    pub fn is_rectangular(&self) -> bool {
        let n = self.close_prices.len();
        self.timestamps.len() == n
            && self.open_prices.len() == n
            && self.high_prices.len() == n
            && self.low_prices.len() == n
            && self.volumes.len() == n
    }
}

/// The market's columns widened to the active precision, owned for the
/// lifetime of a run. Built once; every indicator and the sweep read borrowed
/// [`Bars`] views of it.
pub struct MarketArrays<T> {
    open: Vec<T>,
    high: Vec<T>,
    low: Vec<T>,
    close: Vec<T>,
    volume: Vec<T>,
}

impl<T: BacktestFloat> MarketArrays<T> {
    pub fn from_series(market: &CandleSeries) -> Self {
        let widen = |values: &[f32]| values.iter().copied().map(T::from_f32).collect();
        Self {
            open: widen(&market.open_prices),
            high: widen(&market.high_prices),
            low: widen(&market.low_prices),
            close: widen(&market.close_prices),
            volume: widen(&market.volumes),
        }
    }

    pub fn bars(&self) -> Bars<'_, T> {
        Bars {
            open: &self.open,
            high: &self.high,
            low: &self.low,
            close: &self.close,
            volume: &self.volume,
        }
    }
}

/// Borrowed column view of one market — the single input type every indicator
/// and strategy takes. Adding an indicator means writing a function of this,
/// not threading new slices through the engine.
#[derive(Debug, Clone, Copy)]
pub struct Bars<'a, T> {
    pub open: &'a [T],
    pub high: &'a [T],
    pub low: &'a [T],
    pub close: &'a [T],
    pub volume: &'a [T],
}

impl<T> Bars<'_, T> {
    /// Number of candles. All columns are the same length by construction.
    pub fn len(&self) -> usize {
        self.close.len()
    }

    pub fn is_empty(&self) -> bool {
        self.close.is_empty()
    }
}

/// Test-only owned columns, so unit tests can hand a [`Bars`] to an indicator
/// or a strategy without building a whole [`CandleSeries`].
#[cfg(test)]
pub struct OwnedBars<T> {
    pub open: Vec<T>,
    pub high: Vec<T>,
    pub low: Vec<T>,
    pub close: Vec<T>,
    pub volume: Vec<T>,
}

#[cfg(test)]
impl<T: BacktestFloat> OwnedBars<T> {
    /// A flat market: every OHLC column equal to `close`, zero volume. Enough
    /// for any close-only indicator.
    pub fn from_close(close: Vec<T>) -> Self {
        Self {
            open: close.clone(),
            high: close.clone(),
            low: close.clone(),
            volume: vec![T::ZERO; close.len()],
            close,
        }
    }

    /// A market with distinct highs and lows, for range-based indicators
    /// (ATR, Stochastic, Donchian, ...).
    pub fn ohlc(open: Vec<T>, high: Vec<T>, low: Vec<T>, close: Vec<T>) -> Self {
        Self {
            volume: vec![T::ZERO; close.len()],
            open,
            high,
            low,
            close,
        }
    }

    pub fn bars(&self) -> Bars<'_, T> {
        Bars {
            open: &self.open,
            high: &self.high,
            low: &self.low,
            close: &self.close,
            volume: &self.volume,
        }
    }
}

/// Where the program reads and writes. Passed explicitly rather than baked in
/// as process-relative constants, so tests can point at a scratch directory
/// instead of the repository's live `dataKLines/`, and so the CLI can offer
/// `--data-dir` / `--results-dir`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataPaths {
    klines_dir: PathBuf,
    results_dir: PathBuf,
}

impl DataPaths {
    pub fn new(klines_dir: impl Into<PathBuf>, results_dir: impl Into<PathBuf>) -> Self {
        Self {
            klines_dir: klines_dir.into(),
            results_dir: results_dir.into(),
        }
    }

    pub fn klines_dir(&self) -> &Path {
        &self.klines_dir
    }

    #[must_use]
    pub fn with_klines_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.klines_dir = dir.into();
        self
    }

    #[must_use]
    pub fn with_results_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.results_dir = dir.into();
        self
    }

    /// Cached market data for `(pair, level)` — Apache Arrow IPC / Feather v2.
    pub fn feather(&self, pair: &str, level: &Level) -> PathBuf {
        self.klines_dir.join(format!("{pair}-{level}.feather"))
    }

    /// Legacy JSON cache path, read once by the migration helper in
    /// `download.rs` so an existing JSON cache is converted rather than
    /// forcing a full re-download.
    pub fn legacy_json(&self, pair: &str, level: &Level) -> PathBuf {
        self.klines_dir.join(format!("{pair}-{level}.json"))
    }

    /// Appended run history for `(pair, level)`.
    ///
    /// The version suffix isolates each CSV schema from the last —
    /// `write_to_file` only emits a header for an empty or missing target, so
    /// appending to an older file would silently interleave two layouts. Bump
    /// it whenever the column set changes.
    pub fn results(&self, pair: &str, level: &Level) -> PathBuf {
        self.results_dir.join(format!("{pair}-{level}_v3.csv"))
    }
}

impl Default for DataPaths {
    fn default() -> Self {
        Self::new("dataKLines", "results")
    }
}

pub fn load_data_file(paths: &DataPaths, pair: &str, level: &Level) -> Result<CandleSeries> {
    let k_v = load_k_lines(paths, pair, level)?;
    let mut series = CandleSeries {
        timestamps: Vec::with_capacity(k_v.len()),
        open_prices: Vec::with_capacity(k_v.len()),
        high_prices: Vec::with_capacity(k_v.len()),
        low_prices: Vec::with_capacity(k_v.len()),
        close_prices: Vec::with_capacity(k_v.len()),
        volumes: Vec::with_capacity(k_v.len()),
    };
    for k in &k_v {
        series.timestamps.push(k.time);
        series.open_prices.push(k.open);
        series.high_prices.push(k.high);
        series.low_prices.push(k.low);
        series.close_prices.push(k.close);
        series.volumes.push(k.volume);
    }
    Ok(series)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_data_file_reads_repository_fixture() {
        let candles = load_data_file(&DataPaths::default(), "BTC-USDT", &Level::Hour4)
            .expect("fixture data should load");
        assert!(
            candles.is_rectangular(),
            "every column must be the same length"
        );
        assert!(!candles.is_empty());
        // The fixture predates volume support and is deliberately left that
        // way, so the pre-volume read path stays exercised by the test suite.
        assert!(candles.volumes.iter().all(|v| v.is_nan()));
    }

    #[test]
    fn market_arrays_widen_every_column_and_hand_out_a_view() {
        let series = CandleSeries {
            timestamps: vec![0, 1],
            open_prices: vec![1.0, 2.0],
            high_prices: vec![3.0, 4.0],
            low_prices: vec![0.5, 1.5],
            close_prices: vec![2.5, 3.5],
            volumes: vec![10.0, 20.0],
        };
        let arrays = MarketArrays::<f64>::from_series(&series);
        let bars = arrays.bars();
        assert_eq!(bars.len(), 2);
        assert_eq!(bars.open, &[1.0_f64, 2.0]);
        assert_eq!(bars.high, &[3.0_f64, 4.0]);
        assert_eq!(bars.low, &[0.5_f64, 1.5]);
        assert_eq!(bars.close, &[2.5_f64, 3.5]);
        assert_eq!(bars.volume, &[10.0_f64, 20.0]);
    }

    #[test]
    fn is_rectangular_catches_a_short_column() {
        let mut series = CandleSeries {
            timestamps: vec![0, 1],
            open_prices: vec![1.0, 2.0],
            high_prices: vec![3.0, 4.0],
            low_prices: vec![0.5, 1.5],
            close_prices: vec![2.5, 3.5],
            volumes: vec![10.0, 20.0],
        };
        assert!(series.is_rectangular());
        series.high_prices.pop();
        assert!(!series.is_rectangular());
    }

    #[test]
    fn data_paths_compose_file_names_under_their_roots() {
        let paths = DataPaths::new("some/klines", "some/results");
        assert_eq!(
            paths.feather("BTC-USDT", &Level::Hour4),
            Path::new("some/klines").join("BTC-USDT-4h.feather")
        );
        assert_eq!(
            paths.legacy_json("BTC-USDT", &Level::Hour4),
            Path::new("some/klines").join("BTC-USDT-4h.json")
        );
        assert_eq!(
            paths.results("BTC-USDT", &Level::Hour4),
            Path::new("some/results").join("BTC-USDT-4h_v3.csv")
        );
    }
}
