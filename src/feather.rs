//! Apache Arrow IPC (Feather v2) read/write helpers for `Vec<K>`.
//!
//! Schema is fixed: one `RecordBatch` with five non-nullable columns
//! (`time: UInt64`, `open/high/low/close: Float32`). Wrong schemas are
//! rejected up front so a malformed cache file fails loudly at load time
//! rather than producing garbage candles.

use crate::exchange::K;
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

fn kline_schema() -> Schema {
    Schema::new(vec![
        Field::new(TIME_FIELD, DataType::UInt64, false),
        Field::new(OPEN_FIELD, DataType::Float32, false),
        Field::new(HIGH_FIELD, DataType::Float32, false),
        Field::new(LOW_FIELD, DataType::Float32, false),
        Field::new(CLOSE_FIELD, DataType::Float32, false),
    ])
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

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(time),
            Arc::new(open),
            Arc::new(high),
            Arc::new(low),
            Arc::new(close),
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
        return Err(error).with_context(|| {
            format!(
                "failed to rename {} → {}",
                tmp.display(),
                path.display()
            )
        });
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
    let mut writer = FileWriter::try_new(writer, schema.as_ref()).with_context(|| {
        format!("failed to construct feather writer for {}", tmp.display())
    })?;
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

    validate_schema(reader.schema().as_ref(), path)?;

    let mut out: Vec<K> = Vec::new();
    for (idx, batch) in reader.enumerate() {
        let batch = batch
            .with_context(|| format!("failed to read batch {idx} from {}", path.display()))?;
        append_batch(&mut out, &batch, path)?;
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

    validate_schema(reader.schema().as_ref(), path)?;

    let mut max: Option<u64> = None;
    for (idx, batch) in reader.enumerate() {
        let batch = batch
            .with_context(|| format!("failed to read batch {idx} from {}", path.display()))?;
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

fn validate_schema(actual: &Schema, path: &Path) -> Result<()> {
    let expected = kline_schema();
    if actual.fields().len() != expected.fields().len() {
        return Err(anyhow!(
            "{}: expected {} columns, got {}",
            path.display(),
            expected.fields().len(),
            actual.fields().len()
        ));
    }
    for (got, want) in actual.fields().iter().zip(expected.fields().iter()) {
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
    Ok(())
}

fn append_batch(out: &mut Vec<K>, batch: &RecordBatch, path: &Path) -> Result<()> {
    let time = downcast_u64(batch, 0, path)?;
    let open = downcast_f32(batch, 1, path)?;
    let high = downcast_f32(batch, 2, path)?;
    let low = downcast_f32(batch, 3, path)?;
    let close = downcast_f32(batch, 4, path)?;

    let n = batch.num_rows();
    out.reserve(n);
    for i in 0..n {
        out.push(K {
            time: time.value(i),
            open: open.value(i),
            high: high.value(i),
            low: low.value(i),
            close: close.value(i),
        });
    }
    Ok(())
}

fn downcast_u64<'a>(
    batch: &'a RecordBatch,
    idx: usize,
    path: &Path,
) -> Result<&'a UInt64Array> {
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

fn downcast_f32<'a>(
    batch: &'a RecordBatch,
    idx: usize,
    path: &Path,
) -> Result<&'a Float32Array> {
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
            })
            .collect()
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
}
