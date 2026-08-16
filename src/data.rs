use crate::download::load_k_lines;
use crate::exchange::Level;
use anyhow::Result;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq)]
pub struct CandleSeries {
    pub timestamps: Vec<u64>,
    pub open_prices: Vec<f32>,
    pub close_prices: Vec<f32>,
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
    /// The `_v2` suffix isolates the Strategy/Params CSV schema from older
    /// Period1/Period2 files — `write_to_file` only emits a header for an
    /// empty or missing target, so appending to an old file would silently
    /// interleave two schemas.
    pub fn results(&self, pair: &str, level: &Level) -> PathBuf {
        self.results_dir.join(format!("{pair}-{level}_v2.csv"))
    }
}

impl Default for DataPaths {
    fn default() -> Self {
        Self::new("dataKLines", "results")
    }
}

pub fn load_data_file(paths: &DataPaths, pair: &str, level: &Level) -> Result<CandleSeries> {
    let k_v = load_k_lines(paths, pair, level)?;
    let mut timestamps = Vec::with_capacity(k_v.len());
    let mut open_prices = Vec::with_capacity(k_v.len());
    let mut close_prices = Vec::with_capacity(k_v.len());
    for k in &k_v {
        timestamps.push(k.time);
        open_prices.push(k.open);
        close_prices.push(k.close);
    }
    Ok(CandleSeries {
        timestamps,
        open_prices,
        close_prices,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_data_file_reads_repository_fixture() {
        let candles = load_data_file(&DataPaths::default(), "BTC-USDT", &Level::Hour4)
            .expect("fixture data should load");
        assert_eq!(candles.timestamps.len(), candles.close_prices.len());
        assert_eq!(candles.timestamps.len(), candles.open_prices.len());
        assert!(!candles.timestamps.is_empty());
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
            Path::new("some/results").join("BTC-USDT-4h_v2.csv")
        );
    }
}
