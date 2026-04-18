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

pub fn data_file_path(pair: &str, level: &Level) -> PathBuf {
    Path::new("dataKLines").join(format!("{pair}-{level}.feather"))
}

/// Path of the legacy JSON cache file for `(pair, level)`. Used by the
/// one-time migration helper in `download.rs` so existing JSON caches are
/// converted in place rather than forcing a full re-download.
pub fn legacy_json_path(pair: &str, level: &Level) -> PathBuf {
    Path::new("dataKLines").join(format!("{pair}-{level}.json"))
}

pub fn results_file_path(pair: &str, level: &Level) -> PathBuf {
    Path::new("results").join(format!("{pair}-{level}.csv"))
}

pub fn load_data_file(pair: &str, level: &Level) -> Result<CandleSeries> {
    let k_v = load_k_lines(pair, level)?;
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
        let candles = load_data_file("BTC-USDT", &Level::Hour4).expect("fixture data should load");
        assert_eq!(candles.timestamps.len(), candles.close_prices.len());
        assert_eq!(candles.timestamps.len(), candles.open_prices.len());
        assert!(!candles.timestamps.is_empty());
    }
}
