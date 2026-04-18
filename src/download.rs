use crate::data::data_file_path;
use crate::exchange::{get_k_range, Binance, Level, TimeRange, K};
use anyhow::{Context, Result};
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

const CACHE_MAX_AGE_MS: u64 = 2 * 24 * 60 * 60 * 1000;

pub fn load_k_lines(pair: &str, level: &Level) -> Result<Vec<K>> {
    let datafile = data_file_path(pair, level);

    let contents = fs::read_to_string(&datafile)
        .with_context(|| format!("Failed to read market data file: {}", datafile.display()))?;
    let mut k_v: Vec<K> = serde_json::from_str(&contents)
        .with_context(|| format!("Failed to parse market data file: {}", datafile.display()))?;

    let report = normalize_klines(&mut k_v);
    if report.changed() {
        eprintln!(
            "Note: {} had {} out-of-order and {} duplicate timestamps; normalized in place and rewriting cache.",
            datafile.display(),
            if report.was_unsorted { "some" } else { "no" },
            report.removed_duplicates,
        );
        let serialized = serde_json::to_string_pretty(&k_v)
            .context("Failed to serialize normalized candlesticks")?;
        fs::write(&datafile, serialized).with_context(|| {
            format!(
                "Failed to rewrite normalized market data file: {}",
                datafile.display()
            )
        })?;
    }

    Ok(k_v)
}

/// Result of a single `normalize_klines` pass.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct NormalizeReport {
    pub was_unsorted: bool,
    pub removed_duplicates: usize,
}

impl NormalizeReport {
    pub fn changed(&self) -> bool {
        self.was_unsorted || self.removed_duplicates > 0
    }
}

/// Sort candles ascending by timestamp and drop duplicates by timestamp.
/// Returns a `NormalizeReport` describing what was fixed.
pub fn normalize_klines(v: &mut Vec<K>) -> NormalizeReport {
    if v.len() <= 1 {
        return NormalizeReport::default();
    }
    let was_unsorted = v.windows(2).any(|w| w[0].time > w[1].time);
    if was_unsorted {
        v.sort_by_key(|k| k.time);
    }
    let before = v.len();
    v.dedup_by_key(|k| k.time);
    NormalizeReport {
        was_unsorted,
        removed_duplicates: before - v.len(),
    }
}

fn ensure_strictly_increasing_and_unique(v: &[K]) -> Result<()> {
    if v.len() <= 1 {
        return Ok(());
    }

    for i in 1..v.len() {
        if v[i].time <= v[i - 1].time {
            anyhow::bail!(
                "candles must have strictly increasing unique timestamps: {} followed by {}",
                v[i - 1].time,
                v[i].time
            );
        }
    }

    Ok(())
}

fn interval_check(v: &[K]) -> (bool, u64) {
    if v.len() <= 1 {
        return (true, 0);
    }

    let expected_interval = v[1].time.wrapping_sub(v[0].time);
    let mut max_gap = expected_interval;
    let mut is_constant = true;

    for i in 1..v.len() {
        let interval = v[i].time.wrapping_sub(v[i - 1].time);
        if interval != expected_interval {
            if interval > max_gap {
                max_gap = interval;
            }
            is_constant = false;
        }
    }

    (is_constant, max_gap)
}

pub async fn download_dump_k_lines_to_json<T>(
    product: &str,
    level: Level,
    range: T,
    force: bool,
) -> Result<()>
where
    T: Into<TimeRange>,
{
    let exchange = Binance::new().context("Failed to create Binance client")?;
    let folder_path = Path::new("dataKLines");

    fs::create_dir_all(folder_path)
        .with_context(|| format!("Failed to create directory: {}", folder_path.display()))?;

    let json_path = data_file_path(product, &level);
    let cache_exists = json_path.exists() && json_path.is_file();

    if force {
        println!("Force-download requested for {:?}.", json_path);
    } else if cache_exists {
        match cache_last_candle_time_ms(&json_path) {
            Ok(last_ms) => {
                let now_ms = now_unix_millis()?;
                let age_ms = now_ms.saturating_sub(last_ms);
                if is_cache_fresh(last_ms, now_ms, CACHE_MAX_AGE_MS) {
                    println!(
                        "File {:?} is up to date (last candle ~{}h old). Skip Download.",
                        json_path,
                        age_ms / 3_600_000
                    );
                    return Ok(());
                }
                println!(
                    "File {:?} last candle is ~{}h old; downloading...",
                    json_path,
                    age_ms / 3_600_000
                );
            }
            Err(error) => {
                eprintln!(
                    "Could not read cache freshness for {}: {error:#}; re-downloading.",
                    json_path.display()
                );
            }
        }
    } else {
        println!("Downloading {:?} because file does not exist...", json_path);
    }

    let mut k_vec = get_k_range(&exchange, product, level, range)
        .await
        .with_context(|| format!("Failed to download candlesticks for {product} {level}"))?;

    k_vec.reverse();

    ensure_strictly_increasing_and_unique(&k_vec)?;

    let (is_constant, max_gap) = interval_check(&k_vec);
    if !is_constant {
        println!(
            "Warning: times in k_vec are not all separated by the same amount. max gap: {} hours",
            max_gap / 60 / 60 / 1000
        );
    }

    println!("Done.");

    let serialized = serde_json::to_string_pretty(&k_vec)
        .context("Failed to serialize downloaded candlesticks")?;

    let mut file = File::create(&json_path)
        .with_context(|| format!("Failed to create market data file: {}", json_path.display()))?;
    file.write_all(serialized.as_bytes())
        .with_context(|| format!("Failed to write market data file: {}", json_path.display()))?;

    Ok(())
}

fn cache_last_candle_time_ms(path: &Path) -> Result<u64> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("Failed to read cache file: {}", path.display()))?;
    let v: Vec<K> = serde_json::from_str(&contents)
        .with_context(|| format!("Failed to parse cache file: {}", path.display()))?;
    v.iter()
        .map(|k| k.time)
        .max()
        .ok_or_else(|| anyhow::anyhow!("cache file has no candles: {}", path.display()))
}

fn is_cache_fresh(last_candle_ms: u64, now_ms: u64, max_age_ms: u64) -> bool {
    now_ms.saturating_sub(last_candle_ms) < max_age_ms
}

fn now_unix_millis() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before unix epoch")?
        .as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_validator_returns_structured_errors() {
        let candles = vec![
            K { time: 2, open: 1.0, high: 1.0, low: 1.0, close: 1.0 },
            K { time: 1, open: 1.0, high: 1.0, low: 1.0, close: 1.0 },
        ];
        assert!(ensure_strictly_increasing_and_unique(&candles).is_err());
    }

    #[test]
    fn normalize_klines_sorts_and_dedups() {
        let mut v = vec![
            K { time: 3, open: 1.0, high: 1.0, low: 1.0, close: 1.0 },
            K { time: 1, open: 2.0, high: 2.0, low: 2.0, close: 2.0 },
            K { time: 2, open: 3.0, high: 3.0, low: 3.0, close: 3.0 },
            K { time: 1, open: 4.0, high: 4.0, low: 4.0, close: 4.0 },
        ];
        let report = normalize_klines(&mut v);
        assert!(report.changed());
        assert!(report.was_unsorted);
        assert_eq!(report.removed_duplicates, 1);
        assert_eq!(v.iter().map(|k| k.time).collect::<Vec<_>>(), vec![1, 2, 3]);
    }

    #[test]
    fn normalize_klines_leaves_clean_data_untouched() {
        let mut v = vec![
            K { time: 1, open: 1.0, high: 1.0, low: 1.0, close: 1.0 },
            K { time: 2, open: 2.0, high: 2.0, low: 2.0, close: 2.0 },
            K { time: 3, open: 3.0, high: 3.0, low: 3.0, close: 3.0 },
        ];
        let report = normalize_klines(&mut v);
        assert!(!report.changed());
        assert_eq!(v.len(), 3);
    }

    #[test]
    fn normalize_klines_removes_only_consecutive_duplicates_after_sort() {
        let mut v = vec![
            K { time: 1, open: 1.0, high: 1.0, low: 1.0, close: 1.0 },
            K { time: 1, open: 9.0, high: 9.0, low: 9.0, close: 9.0 },
            K { time: 2, open: 2.0, high: 2.0, low: 2.0, close: 2.0 },
            K { time: 1, open: 5.0, high: 5.0, low: 5.0, close: 5.0 },
        ];
        let report = normalize_klines(&mut v);
        assert!(report.changed());
        assert_eq!(report.removed_duplicates, 2);
        assert_eq!(v.iter().map(|k| k.time).collect::<Vec<_>>(), vec![1, 2]);
    }

    #[test]
    fn normalize_report_changed_reflects_both_signals() {
        assert!(!NormalizeReport::default().changed());
        assert!(NormalizeReport { was_unsorted: true, removed_duplicates: 0 }.changed());
        assert!(NormalizeReport { was_unsorted: false, removed_duplicates: 1 }.changed());
    }

    #[test]
    fn ensure_strictly_increasing_accepts_sorted_unique() {
        let v = vec![
            K { time: 1, open: 1.0, high: 1.0, low: 1.0, close: 1.0 },
            K { time: 2, open: 1.0, high: 1.0, low: 1.0, close: 1.0 },
            K { time: 3, open: 1.0, high: 1.0, low: 1.0, close: 1.0 },
        ];
        assert!(ensure_strictly_increasing_and_unique(&v).is_ok());
    }

    #[test]
    fn ensure_strictly_increasing_rejects_equal_timestamps() {
        let v = vec![
            K { time: 1, open: 1.0, high: 1.0, low: 1.0, close: 1.0 },
            K { time: 1, open: 2.0, high: 2.0, low: 2.0, close: 2.0 },
        ];
        assert!(ensure_strictly_increasing_and_unique(&v).is_err());
    }

    #[test]
    fn ensure_strictly_increasing_accepts_empty_and_single() {
        assert!(ensure_strictly_increasing_and_unique(&[]).is_ok());
        let v = vec![K { time: 42, open: 1.0, high: 1.0, low: 1.0, close: 1.0 }];
        assert!(ensure_strictly_increasing_and_unique(&v).is_ok());
    }

    #[test]
    fn interval_check_detects_constant_cadence() {
        let v = vec![
            K { time: 0, open: 1.0, high: 1.0, low: 1.0, close: 1.0 },
            K { time: 100, open: 1.0, high: 1.0, low: 1.0, close: 1.0 },
            K { time: 200, open: 1.0, high: 1.0, low: 1.0, close: 1.0 },
        ];
        let (is_constant, max_gap) = interval_check(&v);
        assert!(is_constant);
        assert_eq!(max_gap, 100);
    }

    #[test]
    fn interval_check_reports_largest_gap() {
        let v = vec![
            K { time: 0, open: 1.0, high: 1.0, low: 1.0, close: 1.0 },
            K { time: 100, open: 1.0, high: 1.0, low: 1.0, close: 1.0 },
            K { time: 500, open: 1.0, high: 1.0, low: 1.0, close: 1.0 },
            K { time: 600, open: 1.0, high: 1.0, low: 1.0, close: 1.0 },
        ];
        let (is_constant, max_gap) = interval_check(&v);
        assert!(!is_constant);
        assert_eq!(max_gap, 400);
    }

    #[test]
    fn interval_check_handles_short_input() {
        assert_eq!(interval_check(&[]), (true, 0));
        let v = vec![K { time: 42, open: 1.0, high: 1.0, low: 1.0, close: 1.0 }];
        assert_eq!(interval_check(&v), (true, 0));
    }

    #[test]
    fn is_cache_fresh_classifies_recent_and_stale() {
        let day_ms = 24 * 60 * 60 * 1000u64;
        let now = 10 * day_ms;
        assert!(is_cache_fresh(now - day_ms / 2, now, day_ms));
        assert!(!is_cache_fresh(now - 2 * day_ms, now, day_ms));
        assert!(is_cache_fresh(now, now, day_ms), "zero age is fresh");
    }

    #[test]
    fn is_cache_fresh_treats_future_candle_as_fresh() {
        let day_ms = 24 * 60 * 60 * 1000u64;
        let now = 5 * day_ms;
        assert!(
            is_cache_fresh(now + day_ms, now, day_ms),
            "clock drift should not trigger re-download"
        );
    }

    #[test]
    fn cache_last_candle_time_returns_max_timestamp() {
        use std::time::{SystemTime, UNIX_EPOCH};
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "backtest_rust_cache_{}_{stamp}.json",
            std::process::id()
        ));
        let v = vec![
            K { time: 10, open: 1.0, high: 1.0, low: 1.0, close: 1.0 },
            K { time: 30, open: 1.0, high: 1.0, low: 1.0, close: 1.0 },
            K { time: 20, open: 1.0, high: 1.0, low: 1.0, close: 1.0 },
        ];
        fs::write(&path, serde_json::to_string(&v).unwrap()).expect("write temp file");

        let last = cache_last_candle_time_ms(&path).expect("parse last candle");
        assert_eq!(last, 30);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn cache_last_candle_time_errors_on_empty_file() {
        use std::time::{SystemTime, UNIX_EPOCH};
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "backtest_rust_cache_empty_{}_{stamp}.json",
            std::process::id()
        ));
        fs::write(&path, "[]").expect("write temp file");

        assert!(cache_last_candle_time_ms(&path).is_err());

        let _ = fs::remove_file(&path);
    }
}
