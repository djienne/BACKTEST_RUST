//! Append-only CSV of run results.
//!
//! The header is written only for an empty or missing target, so a schema
//! change must also change the file name (see `DataPaths::results`) — otherwise
//! new rows would silently append under an old header.

use crate::backtest::BacktestMetrics;
use chrono::Utc;
use std::borrow::Cow;
use std::fs;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::Path;

#[derive(Debug, Clone, PartialEq)]
pub struct ResultRow<'a> {
    pub ohlcv_file: &'a str,
    pub precision: &'a str,
    pub strategy: &'a str,
    pub params: &'a str,
    pub duration_ms: f64,
    /// Metrics over the optimization segment.
    pub metrics: BacktestMetrics,
    /// Metrics on the held-out tail when `--split` was used; empty columns
    /// otherwise, so one file can hold both kinds of run.
    pub out_of_sample: Option<BacktestMetrics>,
}

/// Column names of one metrics block, prefixed for the out-of-sample copy.
fn metric_headers(prefix: &str) -> String {
    [
        "portfolio_val",
        "max_dd",
        "sharpe_ratio",
        "sortino_ratio",
        "calmar_ratio",
        "cagr",
        "trades",
        "win_rate",
        "exposure",
    ]
    .map(|name| format!("{prefix}{name}"))
    .join(",")
}

fn header() -> String {
    format!(
        "Filename,Date,Precision,Strategy,Params,DurationMs,{},{}",
        metric_headers(""),
        metric_headers("oos_")
    )
}

/// One metrics block as CSV columns.
///
/// Ratios are written with `{}` rather than a fixed precision: Sortino can be
/// infinite, and rounding to three decimals in a machine-readable file discards
/// information a spreadsheet can round back itself.
fn metric_columns(m: &BacktestMetrics) -> String {
    format!(
        "{:.4},{:.4},{},{},{},{},{},{:.2},{:.2}",
        m.final_value,
        m.max_drawdown,
        m.sharpe_ratio,
        m.sortino_ratio,
        m.calmar_ratio,
        m.cagr,
        m.trades,
        m.win_rate,
        m.exposure,
    )
}

/// Empty placeholders, one per metrics column, for a run without a split.
fn blank_metric_columns() -> String {
    [""; 9].join(",")
}

pub fn csv_escape(field: &str) -> Cow<'_, str> {
    if field.contains(',') || field.contains('"') || field.contains('\n') {
        let escaped = field.replace('"', "\"\"");
        Cow::Owned(format!("\"{escaped}\""))
    } else {
        Cow::Borrowed(field)
    }
}

pub fn write_to_file(output_path: &Path, row: &ResultRow<'_>) -> std::io::Result<()> {
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let should_write_header = !output_path.exists()
        || fs::metadata(output_path)
            .map(|metadata| metadata.len() == 0)
            .unwrap_or(true);
    let file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(output_path)?;

    let mut writer = BufWriter::new(file);

    if should_write_header {
        writeln!(writer, "{}", header())?;
    }

    let now = Utc::now();
    writeln!(
        writer,
        "{},{},{},{},{},{:.3},{},{}",
        csv_escape(row.ohlcv_file),
        now.to_rfc3339(),
        csv_escape(row.precision),
        csv_escape(row.strategy),
        csv_escape(row.params),
        row.duration_ms,
        metric_columns(&row.metrics),
        row.out_of_sample
            .as_ref()
            .map_or_else(blank_metric_columns, metric_columns),
    )?;

    writer.flush()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metrics(final_value: f64, sharpe: f64) -> BacktestMetrics {
        BacktestMetrics {
            final_value,
            max_drawdown: 12.5,
            sharpe_ratio: sharpe,
            sortino_ratio: 2.0,
            calmar_ratio: 1.5,
            cagr: 30.0,
            trades: 7,
            win_rate: 57.0,
            exposure: 44.0,
        }
    }

    fn row<'a>(params: &'a str, out_of_sample: Option<BacktestMetrics>) -> ResultRow<'a> {
        ResultRow {
            ohlcv_file: "BTC-USDT_4h",
            precision: "f32",
            strategy: "double_ema",
            params,
            duration_ms: 10.0,
            metrics: metrics(1234.5, 1.25),
            out_of_sample,
        }
    }

    #[test]
    fn csv_escape_passes_through_safe_strings() {
        assert_eq!(csv_escape("BTC-USDT_4h"), Cow::Borrowed("BTC-USDT_4h"));
        assert_eq!(csv_escape("f32"), Cow::Borrowed("f32"));
    }

    #[test]
    fn csv_escape_quotes_and_doubles_quotes() {
        assert_eq!(csv_escape("a,b"), Cow::Owned::<str>("\"a,b\"".to_string()));
        assert_eq!(
            csv_escape("a\"b"),
            Cow::Owned::<str>("\"a\"\"b\"".to_string())
        );
        assert_eq!(
            csv_escape("a\nb"),
            Cow::Owned::<str>("\"a\nb\"".to_string())
        );
    }

    #[test]
    fn csv_escape_quotes_params_with_commas() {
        // "fast=5,slow=10" contains a comma → must be quoted.
        assert_eq!(
            csv_escape("fast=5,slow=10"),
            Cow::Owned::<str>("\"fast=5,slow=10\"".to_string())
        );
    }

    #[test]
    fn every_row_has_exactly_as_many_columns_as_the_header() {
        // The blank out-of-sample block is the easy thing to get wrong; a
        // ragged CSV silently misaligns every column after it.
        let expected = header().split(',').count();
        let with_split = format!(
            "x,x,x,x,x,x,{},{}",
            metric_columns(&metrics(1.0, 1.0)),
            metric_columns(&metrics(2.0, 2.0))
        );
        let without_split = format!(
            "x,x,x,x,x,x,{},{}",
            metric_columns(&metrics(1.0, 1.0)),
            blank_metric_columns()
        );
        assert_eq!(with_split.split(',').count(), expected);
        assert_eq!(without_split.split(',').count(), expected);
    }

    #[test]
    fn write_to_file_appends_results_history() {
        let output_path = std::env::temp_dir().join(format!(
            "backtest_rust_results_{}.csv",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        write_to_file(&output_path, &row("fast=5,slow=10", None)).unwrap();
        write_to_file(
            &output_path,
            &row("fast=12,slow=24", Some(metrics(999.0, 0.5))),
        )
        .unwrap();

        let contents = fs::read_to_string(&output_path).unwrap();
        let lines = contents.lines().collect::<Vec<_>>();

        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0], header());
        // Params with commas must round-trip through csv_escape.
        assert!(lines[1].contains("\"fast=5,slow=10\""));
        assert!(lines[2].contains("\"fast=12,slow=24\""));
        assert!(
            lines[2].contains("999.0000"),
            "out-of-sample value must reach the file: {}",
            lines[2]
        );

        fs::remove_file(output_path).unwrap();
    }

    #[test]
    fn an_infinite_sortino_stays_readable_in_the_csv() {
        let mut m = metrics(1000.0, 1.0);
        m.sortino_ratio = f64::INFINITY;
        assert!(
            metric_columns(&m).contains("inf"),
            "got: {}",
            metric_columns(&m)
        );
    }
}
