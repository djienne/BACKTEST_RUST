//! Apache Arrow IPC (Feather v2) read/write helpers for `Vec<K>`.
//!
//! The current schema is one `RecordBatch` with six non-nullable columns
//! (`time: UInt64`, `open/high/low/close/volume: Float32`). Files written
//! before volume support carry the same five leading columns and no sixth;
//! those still load, with volume set to `NaN`. Anything else is rejected up
//! front so a malformed cache fails loudly at load time rather than producing
//! garbage candles.

use crate::exchange::{unknown_volume, K};
use anyhow::{anyhow, Context, Result};
use arrow::array::{Array, Float32Array, RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;
use std::sync::Arc;

const TIME_FIELD: &str = "time";
const OPEN_FIELD: &str = "open";
const HIGH_FIELD: &str = "high";
const LOW_FIELD: &str = "low";
const CLOSE_FIELD: &str = "close";
const VOLUME_FIELD: &str = "volume";

/// Which on-disk layout a cache file uses. Only ever produced by
/// [`detect_schema`]; [`write`] always emits [`CacheSchema::WithVolume`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheSchema {
    /// Pre-volume: five columns.
    Legacy5Column,
    /// Current: five columns plus `volume`.
    WithVolume,
}

fn base_fields() -> Vec<Field> {
    vec![
        Field::new(TIME_FIELD, DataType::UInt64, false),
        Field::new(OPEN_FIELD, DataType::Float32, false),
        Field::new(HIGH_FIELD, DataType::Float32, false),
        Field::new(LOW_FIELD, DataType::Float32, false),
        Field::new(CLOSE_FIELD, DataType::Float32, false),
    ]
}

fn kline_schema() -> Schema {
    let mut fields = base_fields();
    fields.push(Field::new(VOLUME_FIELD, DataType::Float32, false));
    Schema::new(fields)
}

/// Write `candles` to `path` as a single-batch Feather v2 file.
///
/// Atomic on success: the bytes are written to `<path>.tmp` first and then
/// renamed over `path`. A crash mid-write leaves either the previous file
/// intact (if `<path>.tmp` was never renamed) or no `.tmp` leftover (if the
/// helper itself returned an error). `fs::rename` is atomic on Windows
/// (`MoveFileExW MOVEFILE_REPLACE_EXISTING`) and POSIX when source and
/// destination are on the same filesystem — the common case here.
pub fn write(path: &Path, candles: &[K]) -> Result<()> {
    let schema = Arc::new(kline_schema());

    let time = UInt64Array::from_iter_values(candles.iter().map(|k| k.time));
    let open = Float32Array::from_iter_values(candles.iter().map(|k| k.open));
    let high = Float32Array::from_iter_values(candles.iter().map(|k| k.high));
    let low = Float32Array::from_iter_values(candles.iter().map(|k| k.low));
    let close = Float32Array::from_iter_values(candles.iter().map(|k| k.close));
    let volume = Float32Array::from_iter_values(candles.iter().map(|k| k.volume));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(time),
            Arc::new(open),
            Arc::new(high),
            Arc::new(low),
            Arc::new(close),
            Arc::new(volume),
        ],
    )
    .context("failed to build RecordBatch for kline cache")?;

    let tmp = tmp_path(path);
    let result = write_batch_to(&tmp, &schema, &batch);
    if let Err(error) = result {
        let _ = std::fs::remove_file(&tmp);
        return Err(error);
    }
    if let Err(error) = std::fs::rename(&tmp, path) {
        let _ = std::fs::remove_file(&tmp);
        return Err(error)
            .with_context(|| format!("failed to rename {} → {}", tmp.display(), path.display()));
    }
    Ok(())
}

fn tmp_path(path: &Path) -> std::path::PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(".tmp");
    std::path::PathBuf::from(s)
}

fn write_batch_to(tmp: &Path, schema: &Arc<Schema>, batch: &RecordBatch) -> Result<()> {
    let file = File::create(tmp)
        .with_context(|| format!("failed to create temp feather file: {}", tmp.display()))?;
    let writer = BufWriter::new(file);
    let mut writer = FileWriter::try_new(writer, schema.as_ref())
        .with_context(|| format!("failed to construct feather writer for {}", tmp.display()))?;
    writer
        .write(batch)
        .with_context(|| format!("failed to write RecordBatch to {}", tmp.display()))?;
    writer
        .finish()
        .with_context(|| format!("failed to finalize feather file {}", tmp.display()))?;
    Ok(())
}

/// Read a Feather v2 file produced by `write` into `Vec<K>`.
pub fn read(path: &Path) -> Result<Vec<K>> {
    let file = File::open(path)
        .with_context(|| format!("failed to open feather file: {}", path.display()))?;
    let reader = BufReader::new(file);
    let reader = FileReader::try_new(reader, None)
        .with_context(|| format!("failed to open feather reader for {}", path.display()))?;

    let schema = detect_schema(reader.schema().as_ref(), path)?;
    if schema == CacheSchema::Legacy5Column {
        eprintln!(
            "Note: {} predates volume support; volume reads as NaN until the cache is re-downloaded.",
            path.display()
        );
    }

    let mut out: Vec<K> = Vec::new();
    for (idx, batch) in reader.enumerate() {
        let batch =
            batch.with_context(|| format!("failed to read batch {idx} from {}", path.display()))?;
        append_batch(&mut out, &batch, path, schema)?;
    }
    Ok(out)
}

/// Read just the maximum `time` value without materializing every row.
/// Returns `Err` for an empty file.
pub fn read_last_time(path: &Path) -> Result<u64> {
    let file = File::open(path)
        .with_context(|| format!("failed to open feather file: {}", path.display()))?;
    let reader = BufReader::new(file);
    let reader = FileReader::try_new(reader, None)
        .with_context(|| format!("failed to open feather reader for {}", path.display()))?;

    detect_schema(reader.schema().as_ref(), path)?;

    let mut max: Option<u64> = None;
    for (idx, batch) in reader.enumerate() {
        let batch =
            batch.with_context(|| format!("failed to read batch {idx} from {}", path.display()))?;
        let time = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| anyhow!("{}: time column has unexpected type", path.display()))?;
        if time.is_empty() {
            continue;
        }
        // The Feather file is written sorted ascending by `load_k_lines` and
        // `download_dump_k_lines`, so the last value of the last batch is the
        // max. We still take `max(scanned, last)` as a defensive insurance
        // against a manually-produced out-of-order file. The scan is a single
        // u64 column — microseconds even on hundreds of thousands of rows.
        let last = time.value(time.len() - 1);
        let scanned_max = (0..time.len())
            .map(|i| time.value(i))
            .max()
            .expect("non-empty array");
        let batch_max = scanned_max.max(last);
        max = Some(max.map_or(batch_max, |prev| prev.max(batch_max)));
    }
    max.ok_or_else(|| anyhow!("feather file has no candles: {}", path.display()))
}

/// Identify the on-disk layout, rejecting anything that is neither the current
/// schema nor the pre-volume one. The five leading columns must match exactly
/// in both cases — a file that disagrees there is not a kline cache.
fn detect_schema(actual: &Schema, path: &Path) -> Result<CacheSchema> {
    let expected = kline_schema();
    let expected_fields = expected.fields();
    let schema = match actual.fields().len() {
        6 => CacheSchema::WithVolume,
        5 => CacheSchema::Legacy5Column,
        other => {
            return Err(anyhow!(
                "{}: expected 6 columns (or 5 for a pre-volume cache), got {other}",
                path.display(),
            ))
        }
    };

    for (got, want) in actual.fields().iter().zip(expected_fields.iter()) {
        if got.name() != want.name()
            || got.data_type() != want.data_type()
            || got.is_nullable() != want.is_nullable()
        {
            return Err(anyhow!(
                "{}: schema mismatch — expected `{}: {:?}` (nullable={}), got `{}: {:?}` (nullable={})",
                path.display(),
                want.name(),
                want.data_type(),
                want.is_nullable(),
                got.name(),
                got.data_type(),
                got.is_nullable(),
            ));
        }
    }
    Ok(schema)
}

fn append_batch(
    out: &mut Vec<K>,
    batch: &RecordBatch,
    path: &Path,
    schema: CacheSchema,
) -> Result<()> {
    let time = downcast_u64(batch, 0, path)?;
    let open = downcast_f32(batch, 1, path)?;
    let high = downcast_f32(batch, 2, path)?;
    let low = downcast_f32(batch, 3, path)?;
    let close = downcast_f32(batch, 4, path)?;
    let volume = match schema {
        CacheSchema::WithVolume => Some(downcast_f32(batch, 5, path)?),
        CacheSchema::Legacy5Column => None,
    };

    let n = batch.num_rows();
    out.reserve(n);
    for i in 0..n {
        out.push(K {
            time: time.value(i),
            open: open.value(i),
            high: high.value(i),
            low: low.value(i),
            close: close.value(i),
            volume: volume.map_or_else(unknown_volume, |column| column.value(i)),
        });
    }
    Ok(())
}

fn downcast_u64<'a>(batch: &'a RecordBatch, idx: usize, path: &Path) -> Result<&'a UInt64Array> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| {
            anyhow!(
                "{}: column {idx} has unexpected type {:?}",
                path.display(),
                batch.column(idx).data_type()
            )
        })
}

fn downcast_f32<'a>(batch: &'a RecordBatch, idx: usize, path: &Path) -> Result<&'a Float32Array> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| {
            anyhow!(
                "{}: column {idx} has unexpected type {:?}",
                path.display(),
                batch.column(idx).data_type()
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path(label: &str) -> std::path::PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "backtest_rust_feather_{label}_{}_{stamp}.feather",
            std::process::id()
        ))
    }

    fn sample(n: usize) -> Vec<K> {
        (0..n)
            .map(|i| K {
                time: 1_000 + i as u64 * 60_000,
                open: 100.0 + i as f32,
                high: 200.0 + i as f32,
                low: 50.0 + i as f32,
                close: 150.0 + i as f32,
                volume: 10.0 + i as f32,
            })
            .collect()
    }

    /// Write a file in the pre-volume five-column layout, as builds before
    /// volume support produced.
    fn write_legacy_5_column(path: &Path, candles: &[K]) {
        let schema = Arc::new(Schema::new(base_fields()));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(UInt64Array::from_iter_values(
                    candles.iter().map(|k| k.time),
                )),
                Arc::new(Float32Array::from_iter_values(
                    candles.iter().map(|k| k.open),
                )),
                Arc::new(Float32Array::from_iter_values(
                    candles.iter().map(|k| k.high),
                )),
                Arc::new(Float32Array::from_iter_values(
                    candles.iter().map(|k| k.low),
                )),
                Arc::new(Float32Array::from_iter_values(
                    candles.iter().map(|k| k.close),
                )),
            ],
        )
        .unwrap();
        let file = std::fs::File::create(path).unwrap();
        let mut writer = FileWriter::try_new(file, schema.as_ref()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }

    #[test]
    fn legacy_five_column_caches_still_load_with_unknown_volume() {
        let path = temp_path("legacy_schema");
        let candles = sample(5);
        write_legacy_5_column(&path, &candles);

        let back = read(&path).expect("a pre-volume cache must still load");
        assert_eq!(back.len(), candles.len());
        for (got, want) in back.iter().zip(candles.iter()) {
            assert_eq!(got.time, want.time);
            assert_eq!(got.close, want.close);
            assert_eq!(got.high, want.high);
            assert!(got.volume.is_nan(), "absent volume must read as NaN");
        }
        // The freshness probe has to accept the old layout too, or an upgrade
        // would look like a corrupt cache and trigger a full re-download.
        assert_eq!(read_last_time(&path).unwrap(), candles.last().unwrap().time);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn rewriting_a_legacy_cache_upgrades_it_to_the_current_schema() {
        let path = temp_path("legacy_upgrade");
        let mut candles = sample(4);
        write_legacy_5_column(&path, &candles);

        // Simulate the download path: read, backfill, write back.
        let loaded = read(&path).unwrap();
        assert!(loaded.iter().all(|k| k.volume.is_nan()));
        for (candle, source) in candles.iter_mut().zip(loaded.iter()) {
            candle.time = source.time;
        }
        write(&path, &candles).unwrap();

        let back = read(&path).unwrap();
        assert_eq!(
            back, candles,
            "volume survives the round trip after upgrade"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn round_trip_preserves_klines() {
        let path = temp_path("round_trip");
        let candles = sample(50);
        write(&path, &candles).unwrap();
        let back = read(&path).unwrap();
        assert_eq!(candles, back);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn round_trip_handles_empty_input() {
        let path = temp_path("empty");
        write(&path, &[]).unwrap();
        let back = read(&path).unwrap();
        assert!(back.is_empty());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn round_trip_handles_single_row() {
        let path = temp_path("single");
        let one = vec![K {
            time: 42,
            open: 1.0,
            high: 2.0,
            low: 0.5,
            close: 1.5,
            volume: 3.25,
        }];
        write(&path, &one).unwrap();
        let back = read(&path).unwrap();
        assert_eq!(one, back);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn read_last_time_returns_max() {
        let path = temp_path("last_time");
        let candles = sample(100);
        let expected_max = candles.iter().map(|k| k.time).max().unwrap();
        write(&path, &candles).unwrap();
        assert_eq!(read_last_time(&path).unwrap(), expected_max);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn read_last_time_errors_on_empty_file() {
        let path = temp_path("last_time_empty");
        write(&path, &[]).unwrap();
        assert!(read_last_time(&path).is_err());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn read_rejects_missing_file() {
        let path = temp_path("missing");
        // Don't create it.
        assert!(read(&path).is_err());
    }

    #[test]
    fn read_rejects_garbage_file() {
        let path = temp_path("garbage");
        std::fs::write(&path, b"not an arrow file").unwrap();
        assert!(read(&path).is_err());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn write_overwrites_existing_target_atomically() {
        let path = temp_path("overwrite");
        let first = sample(20);
        let second = sample(5);
        write(&path, &first).unwrap();
        write(&path, &second).unwrap();
        let back = read(&path).unwrap();
        assert_eq!(back, second, "second write must replace the first");
        let tmp = tmp_path(&path);
        assert!(!tmp.exists(), "no .tmp leftover after success");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn write_cleans_up_temp_when_target_dir_missing() {
        let path = std::env::temp_dir()
            .join(format!(
                "backtest_rust_no_dir_{}_{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
            ))
            .join("nope")
            .join("missing.feather");
        assert!(write(&path, &sample(3)).is_err());
        let tmp = tmp_path(&path);
        assert!(!tmp.exists(), "no .tmp leftover when write fails");
    }

    #[test]
    fn read_rejects_nullable_schema() {
        use arrow::ipc::writer::FileWriter;
        let path = temp_path("nullable");
        // Build a schema with a nullable `time` column — different from what
        // `kline_schema()` produces.
        let bad_schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::UInt64, true),
            Field::new("open", DataType::Float32, false),
            Field::new("high", DataType::Float32, false),
            Field::new("low", DataType::Float32, false),
            Field::new("close", DataType::Float32, false),
        ]));
        let candles = sample(3);
        let time = UInt64Array::from_iter_values(candles.iter().map(|k| k.time));
        let open = Float32Array::from_iter_values(candles.iter().map(|k| k.open));
        let high = Float32Array::from_iter_values(candles.iter().map(|k| k.high));
        let low = Float32Array::from_iter_values(candles.iter().map(|k| k.low));
        let close = Float32Array::from_iter_values(candles.iter().map(|k| k.close));
        let batch = RecordBatch::try_new(
            Arc::clone(&bad_schema),
            vec![
                Arc::new(time),
                Arc::new(open),
                Arc::new(high),
                Arc::new(low),
                Arc::new(close),
            ],
        )
        .unwrap();
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = FileWriter::try_new(file, bad_schema.as_ref()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();

        let err = read(&path).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("nullable"),
            "expected nullability complaint, got: {msg}"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn read_rejects_an_unrecognised_column_count() {
        let path = temp_path("four_columns");
        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::UInt64, false),
            Field::new("open", DataType::Float32, false),
            Field::new("high", DataType::Float32, false),
            Field::new("low", DataType::Float32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(UInt64Array::from_iter_values([1u64, 2])),
                Arc::new(Float32Array::from_iter_values([1.0f32, 2.0])),
                Arc::new(Float32Array::from_iter_values([1.0f32, 2.0])),
                Arc::new(Float32Array::from_iter_values([1.0f32, 2.0])),
            ],
        )
        .unwrap();
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = FileWriter::try_new(file, schema.as_ref()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();

        let msg = format!("{:#}", read(&path).unwrap_err());
        assert!(msg.contains("columns"), "got: {msg}");
        let _ = std::fs::remove_file(&path);
    }
}
