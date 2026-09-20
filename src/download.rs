//! Cache policy: when to download, what to merge, and what never to keep.
//!
//! # Freshness
//!
//! Measured from the newest candle *inside* the file, not the file's mtime,
//! and scaled to the timeframe: a cache is stale once a newer bar has
//! certainly closed. A flat wall-clock threshold would let a 15m backtest run
//! on data hundreds of candles old.
//! Fresh caches are still scanned for gaps. Missing prefixes, stale tails and
//! internal gaps are fetched through the same provider; unresolved requested
//! intervals return an error after successful gap repairs have been saved.
//!
//! # Merging
//!
//! The incremental fetch deliberately restarts *at* the newest cached candle
//! rather than one millisecond past it, and [`normalize_klines`] keeps the
//! **last** of any duplicate timestamp. Together those let a bar that an older
//! build stored while it was still forming be replaced by the finished one.
//! `--force` rebuilds a cache outright.
//!
//! See [`crate::exchange::candle_is_closed`] for the other half of that fix:
//! the still-forming candle is dropped before it ever reaches the merge.

use crate::data::DataPaths;
use crate::exchange::{
    get_k_range, missing_candle_ranges, Binance, KlineProvider, Level, TimeRange, K,
};
use crate::feather;
use anyhow::{Context, Result};
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

/// A cache goes stale once its newest candle is this many bar intervals behind
/// the wall clock — i.e. as soon as a newer candle has certainly closed. A flat
/// wall-clock threshold would let a 15m backtest silently run on data hundreds
/// of candles old.
const CACHE_MAX_AGE_BARS: u64 = 2;
/// Floor for the above, so 1m data does not re-hit the API every other minute.
const CACHE_MIN_MAX_AGE_MS: u64 = 2 * 60 * 1000;

fn cache_max_age_ms(level: Level) -> u64 {
    level
        .approx_duration_ms()
        .saturating_mul(CACHE_MAX_AGE_BARS)
        .max(CACHE_MIN_MAX_AGE_MS)
}

pub fn load_k_lines(paths: &DataPaths, pair: &str, level: &Level) -> Result<Vec<K>> {
    let datafile = paths.feather(pair, level);
    let legacy = paths.legacy_json(pair, level);
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

/// Sort candles ascending by timestamp and drop duplicates by timestamp,
/// **keeping the last** occurrence of each. Returns a `NormalizeReport`
/// describing what was fixed.
///
/// "Last wins" is load-bearing, not a detail: the merge in
/// `download_dump_k_lines` appends freshly downloaded candles after the cached
/// ones and deliberately re-fetches the newest cached candle, so keeping the
/// later copy is what lets a stale bar be corrected. Keeping the first would
/// pin the stale copy forever.
pub fn normalize_klines(v: &mut Vec<K>) -> NormalizeReport {
    if v.len() <= 1 {
        return NormalizeReport::default();
    }
    let was_unsorted = v.windows(2).any(|w| w[0].time > w[1].time);
    if was_unsorted {
        // Stable, so equal timestamps keep their relative (cached-then-fresh)
        // order and the dedup below can rely on "later in the vector is newer".
        v.sort_by_key(|k| k.time);
    }
    let before = v.len();
    // `dedup_by` hands over (later, earlier) and drops the later one; copying
    // the later into the retained slot first turns it into "keep the last".
    v.dedup_by(|later, earlier| {
        if later.time == earlier.time {
            *earlier = *later;
            true
        } else {
            false
        }
    });
    NormalizeReport {
        was_unsorted,
        removed_duplicates: before - v.len(),
    }
}

/// One-time conversion of a JSON kline cache into the new Feather format.
/// Reads the JSON, runs the same normalization the loader applies, writes the
/// Feather file, then removes the JSON. Idempotent: if the target Feather
/// already exists, the JSON is removed without re-conversion (the Feather
/// file is treated as the source of truth).
pub fn migrate_legacy_json(legacy: &Path, target: &Path) -> Result<()> {
    if target.exists() {
        // Probe the target for a valid Feather IPC footer before treating it
        // as the source of truth. A corrupt or truncated `.feather` (e.g.
        // from a crashed write predating atomic rename) would otherwise be
        // blessed silently while the only good copy — the legacy JSON — is
        // deleted. If the probe fails, fall through to the conversion path.
        if feather::read_last_time(target).is_ok() {
            if let Err(error) = fs::remove_file(legacy) {
                eprintln!(
                    "Warning: failed to remove obsolete legacy cache {}: {error}",
                    legacy.display()
                );
            }
            return Ok(());
        }
        eprintln!(
            "Note: target {} exists but failed validation; rebuilding from legacy {}.",
            target.display(),
            legacy.display(),
        );
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

/// Refresh the on-disk cache for `(product, level)` from Binance.
/// Failed required downloads and unresolved gaps are errors, not permission to
/// backtest stale or incomplete data. `force` replaces the cache from `range`.
pub async fn download_dump_k_lines<T>(
    paths: &DataPaths,
    product: &str,
    level: Level,
    range: T,
    force: bool,
) -> Result<()>
where
    T: Into<TimeRange>,
{
    let exchange = Binance::new().context("Failed to create Binance client")?;
    download_with_provider(&exchange, paths, product, level, range, force).await
}

/// The body of [`download_dump_k_lines`], with the data source injected so the
/// cache-merge behaviour can be tested without touching the network.
pub async fn download_with_provider<P, T>(
    provider: &P,
    paths: &DataPaths,
    product: &str,
    level: Level,
    range: T,
    force: bool,
) -> Result<()>
where
    P: KlineProvider,
    T: Into<TimeRange>,
{
    let folder_path = paths.klines_dir();
    fs::create_dir_all(folder_path)
        .with_context(|| format!("Failed to create directory: {}", folder_path.display()))?;

    let cache_path = paths.feather(product, &level);
    let legacy = paths.legacy_json(product, &level);
    if !cache_path.exists() && legacy.exists() {
        migrate_legacy_json(&legacy, &cache_path).with_context(|| {
            format!(
                "failed to migrate legacy JSON cache {} → {}",
                legacy.display(),
                cache_path.display()
            )
        })?;
    }

    let now_ms = now_unix_millis()?;
    let range: TimeRange = range.into();
    anyhow::ensure!(
        range.end.is_none_or(|end| end >= range.start),
        "download range ends before it starts"
    );
    let mut merged = if force || !cache_path.is_file() {
        Vec::new()
    } else {
        match feather::read(&cache_path) {
            Ok(prev) => prev,
            Err(error) => {
                eprintln!(
                    "Could not read cache {} ({error:#}); falling back to full re-download.",
                    cache_path.display()
                );
                Vec::new()
            }
        }
    };
    let mut changed = normalize_klines(&mut merged).changed();
    let mut downloads = Vec::new();
    if let (Some(first), Some(last)) = (merged.first(), merged.last()) {
        if range.start < first.time {
            downloads.push(TimeRange {
                start: range.start,
                end: Some(range.end.unwrap_or(first.time - 1).min(first.time - 1)),
            });
        }
        if !is_cache_fresh(last.time, now_ms, cache_max_age_ms(level))
            && range.end.is_none_or(|end| end >= last.time)
        {
            // Re-read the last cached candle to repair older, unfinished copies.
            downloads.push(TimeRange {
                start: last.time,
                end: range.end,
            });
        }
    } else {
        downloads.push(range);
    }
    for window in downloads {
        let batch = get_k_range(provider, product, level, window)
            .await
            .with_context(|| {
                format!(
                    "Failed to download {product} {level} from {} to {:?}",
                    window.start, window.end
                )
            })?;
        changed |= !batch.is_empty();
        merged.extend(batch);
    }
    normalize_klines(&mut merged);
    if merged.is_empty() {
        anyhow::bail!(
            "Refusing to write an empty cache file at {}",
            cache_path.display()
        );
    }

    // Freshness says nothing about internal gaps. Make one bounded repair pass;
    // the provider already retries transient HTTP failures.
    for gap in missing_candle_ranges(merged.iter().map(|k| k.time), level)? {
        println!(
            "Repairing {product} {level} gap {}..={} unix-ms",
            gap.start,
            gap.end.unwrap()
        );
        match get_k_range(provider, product, level, gap).await {
            Ok(batch) => {
                changed |= !batch.is_empty();
                merged.extend(batch);
            }
            Err(error) => eprintln!("Gap repair failed: {error:#}"),
        }
    }
    normalize_klines(&mut merged);
    // Preserve successful repairs even when other intervals remain unavailable.
    if changed {
        feather::write(&cache_path, &merged).with_context(|| {
            format!("Failed to write market data file: {}", cache_path.display())
        })?;
    }
    let remaining = missing_candle_ranges(merged.iter().map(|k| k.time), level)?;
    if let Some(gap) = remaining
        .iter()
        .find(|gap| gap.end.unwrap() >= range.start && range.end.is_none_or(|end| gap.start <= end))
    {
        anyhow::bail!(
            "unresolved {product} {level} candle gap after repair: {}..={} unix-ms",
            gap.start,
            gap.end.unwrap()
        );
    }
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
        std::env::temp_dir().join(format!(
            "backtest_rust_dl_{label}_{}.{ext}",
            unique_suffix()
        ))
    }

    fn unique_suffix() -> String {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{}_{stamp}", std::process::id())
    }

    /// A scratch `DataPaths` under the system temp dir, removed on drop, so no
    /// test touches the repository's live `dataKLines/` or depends on the
    /// process working directory.
    struct TempPaths {
        root: std::path::PathBuf,
        paths: DataPaths,
    }

    impl TempPaths {
        fn new(label: &str) -> Self {
            let root = std::env::temp_dir()
                .join(format!("backtest_rust_paths_{label}_{}", unique_suffix()));
            let paths = DataPaths::new(root.join("dataKLines"), root.join("results"));
            fs::create_dir_all(paths.klines_dir()).unwrap();
            Self { root, paths }
        }
    }

    impl Drop for TempPaths {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn small_klines(times: &[u64]) -> Vec<K> {
        times.iter().copied().map(|t| K::flat(t, 1.0)).collect()
    }

    #[test]
    fn normalize_klines_sorts_and_dedups() {
        let mut v = vec![
            K::flat(3, 1.0),
            K::flat(1, 2.0),
            K::flat(2, 3.0),
            K::flat(1, 4.0),
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
            K::flat(1, 1.0),
            K::flat(1, 9.0),
            K::flat(2, 2.0),
            K::flat(1, 5.0),
        ];
        let report = normalize_klines(&mut v);
        assert!(report.changed());
        assert_eq!(report.removed_duplicates, 2);
        assert_eq!(v.iter().map(|k| k.time).collect::<Vec<_>>(), vec![1, 2]);
    }

    #[test]
    fn normalize_report_changed_reflects_both_signals() {
        assert!(!NormalizeReport::default().changed());
        assert!(NormalizeReport {
            was_unsorted: true,
            removed_duplicates: 0
        }
        .changed());
        assert!(NormalizeReport {
            was_unsorted: false,
            removed_duplicates: 1
        }
        .changed());
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
    fn migrate_legacy_json_recovers_from_corrupt_target() {
        let legacy = unique_temp("migrate_corrupt_target", "json");
        let target = unique_temp("migrate_corrupt_target", "feather");
        let candles = small_klines(&[7, 8, 9]);
        fs::write(&legacy, serde_json::to_string(&candles).unwrap()).unwrap();
        // Pre-write garbage as the "existing" target — simulates a corrupt
        // cache from a crashed write before atomic rename was added.
        fs::write(&target, b"garbage feather bytes").unwrap();

        migrate_legacy_json(&legacy, &target).expect("migration recovers");

        assert!(
            !legacy.exists(),
            "legacy json should be removed after rebuild"
        );
        let back = feather::read(&target).expect("target now valid feather");
        assert_eq!(back, candles);

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
        let temp = TempPaths::new("round_trip");
        let level = Level::Hour1;
        let candles = small_klines(&[1, 2, 3, 4]);
        feather::write(&temp.paths.feather("ANY-USDT", &level), &candles).unwrap();

        let loaded = load_k_lines(&temp.paths, "ANY-USDT", &level).expect("load succeeds");
        assert_eq!(loaded, candles);
    }

    #[test]
    fn load_k_lines_normalizes_and_rewrites_in_place() {
        let temp = TempPaths::new("normalize");
        let level = Level::Hour1;
        // Out-of-order with one duplicate timestamp.
        let candles = small_klines(&[3, 1, 2, 1]);
        feather::write(&temp.paths.feather("ANY-USDT", &level), &candles).unwrap();

        let loaded = load_k_lines(&temp.paths, "ANY-USDT", &level).expect("load succeeds");
        assert_eq!(
            loaded.iter().map(|k| k.time).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );

        // Re-read from disk; should already be normalized so the second load
        // does not trigger another rewrite.
        let again = load_k_lines(&temp.paths, "ANY-USDT", &level).expect("second load");
        assert_eq!(again, loaded);
    }

    #[test]
    fn load_k_lines_migrates_legacy_json_when_feather_absent() {
        let temp = TempPaths::new("legacy");
        let level = Level::Hour1;
        let target = temp.paths.feather("ANY-USDT", &level);
        let legacy = temp.paths.legacy_json("ANY-USDT", &level);

        let candles = small_klines(&[5, 6, 7]);
        fs::write(&legacy, serde_json::to_string(&candles).unwrap()).unwrap();

        let loaded =
            load_k_lines(&temp.paths, "ANY-USDT", &level).expect("load succeeds via migration");
        assert_eq!(loaded, candles);
        assert!(target.exists(), "feather should appear after migration");
        assert!(!legacy.exists(), "legacy json should be cleaned up");
    }

    #[test]
    fn normalize_klines_keeps_the_newest_row_for_a_duplicate_timestamp() {
        // Mirrors the incremental-download merge: the cached copy of candle
        // `2` came first and is stale, the re-fetched copy came second and is
        // the finished bar. The finished bar must win.
        let mut v = vec![
            K::flat(1, 1.0),
            K::flat(2, 9.0), // stale
            K {
                time: 2,
                open: 5.0,
                high: 7.0,
                low: 3.0,
                close: 6.0,
                volume: 42.0,
            }, // fresh
        ];
        let report = normalize_klines(&mut v);
        assert_eq!(report.removed_duplicates, 1);
        assert_eq!(v.len(), 2);
        assert_eq!(v[1].close, 6.0, "the later (freshly downloaded) row wins");
        assert_eq!(v[1].high, 7.0);
    }

    /// Returns one canned page, newest-first, then nothing.
    struct OnePageProvider {
        page: Vec<K>,
        served: std::sync::atomic::AtomicBool,
    }

    struct HistoryProvider {
        rows: Vec<K>,
        calls: std::sync::Mutex<Vec<u64>>,
        fail: bool,
    }

    impl KlineProvider for HistoryProvider {
        #[allow(clippy::manual_async_fn)] // Match the trait's self-borrowed future lifetime.
        fn get_k(
            &self,
            _: &str,
            _: Level,
            time: u64,
        ) -> impl std::future::Future<Output = Result<Vec<K>>> + Send + '_ {
            async move {
                self.calls.lock().unwrap().push(time);
                anyhow::ensure!(!self.fail, "simulated download failure");
                Ok(self
                    .rows
                    .iter()
                    .rev()
                    .filter(|k| k.time <= time)
                    .copied()
                    .collect())
            }
        }
    }

    #[tokio::test]
    async fn fresh_cache_repairs_internal_gaps_and_preserves_partial_repairs() {
        let last = (now_unix_millis().unwrap() / 60_000 - 1) * 60_000;
        let rows = small_klines(&(0..5).map(|i| last - (4 - i) * 60_000).collect::<Vec<_>>());
        let cached = vec![rows[0], rows[2], rows[4]];
        for (label, available, fail) in [
            ("complete", rows.clone(), false),
            ("partial", vec![rows[0], rows[1], rows[2], rows[4]], false),
            ("failed", rows.clone(), true),
        ] {
            let temp = TempPaths::new(label);
            let path = temp.paths.feather("ANY-USDT", &Level::Minute1);
            feather::write(&path, &cached).unwrap();
            let provider = HistoryProvider {
                rows: available,
                calls: Default::default(),
                fail,
            };
            let result = download_with_provider(
                &provider,
                &temp.paths,
                "ANY-USDT",
                Level::Minute1,
                rows[0].time..,
                false,
            )
            .await;
            let restored = feather::read(&path).unwrap();
            assert_eq!(
                *provider.calls.lock().unwrap(),
                vec![rows[2].time - 1, rows[4].time - 1]
            );
            if label == "complete" {
                result.unwrap();
                assert_eq!(restored, rows);
            } else {
                assert!(format!("{:#}", result.unwrap_err()).contains("unresolved"));
                assert_eq!(restored.len(), if fail { 3 } else { 4 });
                assert_eq!(restored.first(), cached.first());
                assert_eq!(restored.last(), cached.last());
            }
        }
    }

    #[tokio::test]
    async fn fresh_cache_backfills_earlier_start_and_never_hides_fetch_failure() {
        let last = (now_unix_millis().unwrap() / 60_000 - 1) * 60_000;
        let rows = small_klines(&(0..5).map(|i| last - (4 - i) * 60_000).collect::<Vec<_>>());
        for fail in [false, true] {
            let temp = TempPaths::new("prefix");
            let path = temp.paths.feather("ANY-USDT", &Level::Minute1);
            feather::write(&path, &rows[3..]).unwrap();
            let provider = HistoryProvider {
                rows: rows.clone(),
                calls: Default::default(),
                fail,
            };
            let result = download_with_provider(
                &provider,
                &temp.paths,
                "ANY-USDT",
                Level::Minute1,
                rows[0].time..,
                false,
            )
            .await;
            assert_eq!(provider.calls.lock().unwrap()[0], rows[3].time - 1);
            if fail {
                assert!(result.is_err());
                assert_eq!(feather::read(&path).unwrap(), rows[3..]);
            } else {
                result.unwrap();
                assert_eq!(feather::read(&path).unwrap(), rows);
            }
        }
    }

    impl OnePageProvider {
        fn new(page: Vec<K>) -> Self {
            Self {
                page,
                served: std::sync::atomic::AtomicBool::new(false),
            }
        }
    }

    impl crate::exchange::KlineProvider for OnePageProvider {
        #[allow(clippy::manual_async_fn)]
        fn get_k(
            &self,
            _product: &str,
            _level: Level,
            _time: u64,
        ) -> impl std::future::Future<Output = Result<Vec<K>>> + Send + '_ {
            async move {
                if self.served.swap(true, std::sync::atomic::Ordering::Relaxed) {
                    return Ok(Vec::new());
                }
                Ok(self.page.clone())
            }
        }
    }

    #[tokio::test]
    async fn incremental_download_repairs_a_stale_final_candle() {
        // The regression this covers end to end: an older build wrote the
        // still-forming candle into the cache. The delta fetch must re-read
        // that bar and the merge must keep the finished copy.
        let temp = TempPaths::new("repair");
        let level = Level::Hour1;
        let hour = 3_600_000u64;
        let now = now_unix_millis().unwrap();
        // Align to an hour boundary well in the past so every candle is closed.
        let newest_open = (now / hour) * hour - 5 * hour;
        let older_open = newest_open - hour;

        let stale_cache = vec![K::flat(older_open, 100.0), K::flat(newest_open, 111.0)];
        feather::write(&temp.paths.feather("ANY-USDT", &level), &stale_cache).unwrap();

        // The exchange's version of the same bar, plus one genuinely new bar.
        let provider = OnePageProvider::new(vec![
            K::flat(newest_open + hour, 222.0),
            K::flat(newest_open, 999.0),
        ]);

        download_with_provider(
            &provider,
            &temp.paths,
            "ANY-USDT",
            level,
            older_open..,
            false,
        )
        .await
        .expect("incremental download succeeds");

        let merged = feather::read(&temp.paths.feather("ANY-USDT", &level)).unwrap();
        assert_eq!(merged.len(), 3, "one bar appended, none duplicated");
        assert_eq!(
            merged[1].close, 999.0,
            "the re-fetched copy must overwrite the stale cached one"
        );
        assert_eq!(merged[2].close, 222.0, "the new bar is appended");
    }

    #[tokio::test]
    async fn download_refuses_to_replace_a_cache_with_nothing() {
        let temp = TempPaths::new("empty");
        let provider = OnePageProvider::new(Vec::new());
        let result = download_with_provider(
            &provider,
            &temp.paths,
            "ANY-USDT",
            Level::Hour1,
            0u64..,
            true,
        )
        .await;
        assert!(result.is_err(), "an empty download must not truncate data");
    }

    #[test]
    fn cache_max_age_scales_with_the_interval() {
        assert_eq!(cache_max_age_ms(Level::Day1), 2 * 86_400_000);
        assert_eq!(cache_max_age_ms(Level::Minute15), 2 * 15 * 60_000);
        assert_eq!(
            cache_max_age_ms(Level::Minute1),
            CACHE_MIN_MAX_AGE_MS,
            "the floor keeps 1m data from re-hitting the API constantly"
        );
    }

    #[test]
    fn a_15m_cache_two_days_old_is_no_longer_considered_fresh() {
        // The regression this replaces: a flat two-day threshold let a 15m
        // backtest run on data ~192 candles stale.
        let now = 10 * 86_400_000u64;
        let last = now - 86_400_000; // one day behind
        assert!(!is_cache_fresh(
            last,
            now,
            cache_max_age_ms(Level::Minute15)
        ));
        assert!(is_cache_fresh(last, now, cache_max_age_ms(Level::Day1)));
    }
}
