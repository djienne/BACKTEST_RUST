use crate::data::{data_file_path, legacy_json_path};
use crate::exchange::{get_k_range, Binance, Level, TimeRange, K};
use crate::feather;
use anyhow::{Context, Result};
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

const CACHE_MAX_AGE_MS: u64 = 2 * 24 * 60 * 60 * 1000;

pub fn load_k_lines(pair: &str, level: &Level) -> Result<Vec<K>> {
    let datafile = data_file_path(pair, level);
    let legacy = legacy_json_path(pair, level);
    if !datafile.exists() && legacy.exists() {
        migrate_legacy_json(&legacy, &datafile).with_context(|| {
            format!(
                "failed to migrate legacy JSON cache {} → {}",
                legacy.display(),
                datafile.display()
            )
        })?;
    }

    let mut k_v = feather::read(&datafile)?;

    let report = normalize_klines(&mut k_v);
    if report.changed() {
        eprintln!(
            "Note: {} had {} out-of-order and {} duplicate timestamps; normalized in place and rewriting cache.",
            datafile.display(),
            if report.was_unsorted { "some" } else { "no" },
            report.removed_duplicates,
        );
        feather::write(&datafile, &k_v).with_context(|| {
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

/// One-time conversion of a JSON kline cache into the new Feather format.
/// Reads the JSON, runs the same normalization the loader applies, writes the
/// Feather file, then removes the JSON. Idempotent: if the target Feather
/// already exists, the JSON is removed without re-conversion (the Feather
/// file is treated as the source of truth).
pub fn migrate_legacy_json(legacy: &Path, target: &Path) -> Result<()> {
    if target.exists() {
        if let Err(error) = fs::remove_file(legacy) {
            eprintln!(
                "Warning: failed to remove obsolete legacy cache {}: {error}",
                legacy.display()
            );
        }
        return Ok(());
    }
    let contents = fs::read_to_string(legacy)
        .with_context(|| format!("failed to read legacy cache {}", legacy.display()))?;
    let mut k_v: Vec<K> = serde_json::from_str(&contents)
        .with_context(|| format!("failed to parse legacy cache {}", legacy.display()))?;
    let _ = normalize_klines(&mut k_v);
    feather::write(target, &k_v)
        .with_context(|| format!("failed to write feather target {}", target.display()))?;
    eprintln!(
        "Migrated legacy cache: {} → {} ({} candles).",
        legacy.display(),
        target.display(),
        k_v.len()
    );
    if let Err(error) = fs::remove_file(legacy) {
        eprintln!(
            "Warning: feather written but legacy {} could not be removed: {error}",
            legacy.display()
        );
    }
    Ok(())
}

pub async fn download_dump_k_lines<T>(
    product: &str,
    level: Level,
    range: T,
    force: bool,
) -> Result<()>
where
    T: Into<TimeRange>,
{
    let folder_path = Path::new("dataKLines");
    fs::create_dir_all(folder_path)
        .with_context(|| format!("Failed to create directory: {}", folder_path.display()))?;

    let cache_path = data_file_path(product, &level);
    let legacy = legacy_json_path(product, &level);
    if !cache_path.exists() && legacy.exists() {
        migrate_legacy_json(&legacy, &cache_path).with_context(|| {
            format!(
                "failed to migrate legacy JSON cache {} → {}",
                legacy.display(),
                cache_path.display()
            )
        })?;
    }

    let cache_exists = cache_path.exists() && cache_path.is_file();
    let now_ms = now_unix_millis()?;
    let range: TimeRange = range.into();

    // Decide between: skip (fresh), incremental (stale + cache present),
    // full download (force, no cache, or cache empty/unreadable).
    let mut existing: Option<Vec<K>> = None;
    let download_range: TimeRange = if force || !cache_exists {
        if force {
            println!("Force-download requested for {:?}.", cache_path);
        } else {
            println!("Downloading {:?} because file does not exist...", cache_path);
        }
        range
    } else {
        match feather::read_last_time(&cache_path) {
            Ok(last_ms) => {
                let age_ms = now_ms.saturating_sub(last_ms);
                if is_cache_fresh(last_ms, now_ms, CACHE_MAX_AGE_MS) {
                    println!(
                        "File {:?} is up to date (last candle ~{}h old). Skip Download.",
                        cache_path,
                        age_ms / 3_600_000
                    );
                    return Ok(());
                }
                match feather::read(&cache_path) {
                    Ok(prev) => {
                        println!(
                            "File {:?} last candle is ~{}h old; fetching delta from {} onward...",
                            cache_path,
                            age_ms / 3_600_000,
                            last_ms + 1
                        );
                        existing = Some(prev);
                        TimeRange {
                            start: last_ms + 1,
                            end: range.end,
                        }
                    }
                    Err(error) => {
                        eprintln!(
                            "Could not read existing cache {} ({error:#}); falling back to full re-download.",
                            cache_path.display()
                        );
                        range
                    }
                }
            }
            Err(error) => {
                eprintln!(
                    "Could not read cache freshness for {} ({error:#}); falling back to full re-download.",
                    cache_path.display()
                );
                range
            }
        }
    };

    let exchange = Binance::new().context("Failed to create Binance client")?;
    let mut new_batch = get_k_range(&exchange, product, level, download_range)
        .await
        .with_context(|| format!("Failed to download candlesticks for {product} {level}"))?;
    new_batch.reverse();
    println!("Fetched {} new candle(s).", new_batch.len());

    let mut merged = match existing {
        Some(mut prev) => {
            prev.extend(new_batch);
            prev
        }
        None => new_batch,
    };

    // Normalize first (sort + dedup) so any overlap or out-of-order rows from
    // the fresh API page are absorbed before the strict sanity checks run.
    let _ = normalize_klines(&mut merged);

    if merged.is_empty() {
        anyhow::bail!(
            "Refusing to write an empty cache file at {}",
            cache_path.display()
        );
    }

    ensure_strictly_increasing_and_unique(&merged)
        .context("post-merge sanity check failed: merged candles are not strictly increasing")?;

    let (is_constant, max_gap) = interval_check(&merged);
    if !is_constant {
        println!(
            "Warning: merged candles have non-uniform spacing — max gap: {} hours",
            max_gap / 3_600_000
        );
    }

    println!("Done.");
    feather::write(&cache_path, &merged).with_context(|| {
        format!("Failed to write market data file: {}", cache_path.display())
    })?;

    Ok(())
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
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp(label: &str, ext: &str) -> std::path::PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "backtest_rust_dl_{label}_{}_{stamp}.{ext}",
            std::process::id()
        ))
    }

    fn small_klines(times: &[u64]) -> Vec<K> {
        times
            .iter()
            .copied()
            .map(|t| K {
                time: t,
                open: 1.0,
                high: 1.0,
                low: 1.0,
                close: 1.0,
            })
            .collect()
    }

    #[test]
    fn timestamp_validator_returns_structured_errors() {
        let candles = small_klines(&[2, 1]);
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
        let mut v = small_klines(&[1, 2, 3]);
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
        let v = small_klines(&[1, 2, 3]);
        assert!(ensure_strictly_increasing_and_unique(&v).is_ok());
    }

    #[test]
    fn ensure_strictly_increasing_rejects_equal_timestamps() {
        let v = small_klines(&[1, 1]);
        assert!(ensure_strictly_increasing_and_unique(&v).is_err());
    }

    #[test]
    fn ensure_strictly_increasing_accepts_empty_and_single() {
        assert!(ensure_strictly_increasing_and_unique(&[]).is_ok());
        let v = small_klines(&[42]);
        assert!(ensure_strictly_increasing_and_unique(&v).is_ok());
    }

    #[test]
    fn interval_check_detects_constant_cadence() {
        let v = small_klines(&[0, 100, 200]);
        let (is_constant, max_gap) = interval_check(&v);
        assert!(is_constant);
        assert_eq!(max_gap, 100);
    }

    #[test]
    fn interval_check_reports_largest_gap() {
        let v = small_klines(&[0, 100, 500, 600]);
        let (is_constant, max_gap) = interval_check(&v);
        assert!(!is_constant);
        assert_eq!(max_gap, 400);
    }

    #[test]
    fn interval_check_handles_short_input() {
        assert_eq!(interval_check(&[]), (true, 0));
        let v = small_klines(&[42]);
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
    fn migrate_legacy_json_converts_and_removes() {
        let legacy = unique_temp("migrate", "json");
        let target = unique_temp("migrate", "feather");
        let candles = small_klines(&[1, 2, 3]);
        fs::write(&legacy, serde_json::to_string(&candles).unwrap()).unwrap();

        migrate_legacy_json(&legacy, &target).expect("migration succeeds");

        assert!(target.exists(), "feather target should exist");
        assert!(!legacy.exists(), "legacy json should be removed");
        let back = feather::read(&target).expect("feather reads");
        assert_eq!(back, candles);

        let _ = fs::remove_file(&target);
    }

    #[test]
    fn migrate_legacy_json_normalizes_unsorted_input() {
        let legacy = unique_temp("migrate_unsorted", "json");
        let target = unique_temp("migrate_unsorted", "feather");
        let candles = small_klines(&[3, 1, 2, 1]); // unsorted + duplicate
        fs::write(&legacy, serde_json::to_string(&candles).unwrap()).unwrap();

        migrate_legacy_json(&legacy, &target).unwrap();

        let back = feather::read(&target).unwrap();
        assert_eq!(
            back.iter().map(|k| k.time).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        let _ = fs::remove_file(&target);
    }

    #[test]
    fn migrate_legacy_json_is_idempotent_when_target_exists() {
        let legacy = unique_temp("migrate_idem", "json");
        let target = unique_temp("migrate_idem", "feather");
        let json_candles = small_klines(&[1, 2]);
        let feather_candles = small_klines(&[10, 20, 30]); // distinct, used as target

        fs::write(&legacy, serde_json::to_string(&json_candles).unwrap()).unwrap();
        feather::write(&target, &feather_candles).unwrap();

        migrate_legacy_json(&legacy, &target).unwrap();

        // Target should be untouched (still the feather we pre-wrote),
        // legacy should be removed.
        assert!(!legacy.exists(), "stale json should be cleaned up");
        let back = feather::read(&target).unwrap();
        assert_eq!(back, feather_candles, "target must not be overwritten");
        let _ = fs::remove_file(&target);
    }

    #[test]
    fn migrate_legacy_json_errors_on_corrupt_input() {
        let legacy = unique_temp("migrate_bad", "json");
        let target = unique_temp("migrate_bad", "feather");
        fs::write(&legacy, "not valid json").unwrap();
        assert!(migrate_legacy_json(&legacy, &target).is_err());
        let _ = fs::remove_file(&legacy);
    }

    #[test]
    fn load_k_lines_round_trips_through_feather() {
        // Drive load_k_lines via a fake (pair, level) by writing to a known
        // path. Use a pair name unlikely to collide with anything else; we
        // operate inside the CWD's `dataKLines/` so this is best-effort.
        let pair = format!("__test_load_{}", std::process::id());
        let level = Level::Hour1;
        let path = data_file_path(&pair, &level);
        let _ = fs::create_dir_all(path.parent().unwrap());
        let candles = small_klines(&[1, 2, 3, 4]);
        feather::write(&path, &candles).unwrap();

        let loaded = load_k_lines(&pair, &level).expect("load succeeds");
        assert_eq!(loaded, candles);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn load_k_lines_normalizes_and_rewrites_in_place() {
        let pair = format!("__test_load_norm_{}", std::process::id());
        let level = Level::Hour1;
        let path = data_file_path(&pair, &level);
        let _ = fs::create_dir_all(path.parent().unwrap());
        // Out-of-order with one duplicate timestamp.
        let candles = small_klines(&[3, 1, 2, 1]);
        feather::write(&path, &candles).unwrap();

        let loaded = load_k_lines(&pair, &level).expect("load succeeds");
        assert_eq!(
            loaded.iter().map(|k| k.time).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );

        // Re-read from disk; should already be normalized so the second load
        // does not trigger another rewrite.
        let again = load_k_lines(&pair, &level).expect("second load");
        assert_eq!(again, loaded);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn load_k_lines_migrates_legacy_json_when_feather_absent() {
        let pair = format!("__test_load_legacy_{}", std::process::id());
        let level = Level::Hour1;
        let target = data_file_path(&pair, &level);
        let legacy = legacy_json_path(&pair, &level);
        let _ = fs::create_dir_all(target.parent().unwrap());
        let _ = fs::remove_file(&target);

        let candles = small_klines(&[5, 6, 7]);
        fs::write(&legacy, serde_json::to_string(&candles).unwrap()).unwrap();

        let loaded = load_k_lines(&pair, &level).expect("load succeeds via migration");
        assert_eq!(loaded, candles);
        assert!(target.exists(), "feather should appear after migration");
        assert!(!legacy.exists(), "legacy json should be cleaned up");

        let _ = fs::remove_file(&target);
    }
}
