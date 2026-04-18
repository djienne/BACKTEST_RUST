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
    pub port_value: f64,
    pub max_dd: f64,
    pub sharpe_ratio: f64,
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
        writeln!(
            writer,
            "Filename,Date,Precision,Strategy,Params,DurationMs,portfolio_val,max_dd,sharpe_ratio"
        )?;
    }

    let now = Utc::now();
    writeln!(
        writer,
        "{},{},{},{},{},{:.3},{:.3},{:.3},{:.3}",
        csv_escape(row.ohlcv_file),
        now.to_rfc3339(),
        csv_escape(row.precision),
        csv_escape(row.strategy),
        csv_escape(row.params),
        row.duration_ms,
        row.port_value,
        row.max_dd,
        row.sharpe_ratio,
    )?;

    writer.flush()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn csv_escape_passes_through_safe_strings() {
        assert_eq!(csv_escape("BTC-USDT_4h"), Cow::Borrowed("BTC-USDT_4h"));
        assert_eq!(csv_escape("f32"), Cow::Borrowed("f32"));
    }

    #[test]
    fn csv_escape_quotes_and_doubles_quotes() {
        assert_eq!(csv_escape("a,b"), Cow::Owned::<str>("\"a,b\"".to_string()));
        assert_eq!(csv_escape("a\"b"), Cow::Owned::<str>("\"a\"\"b\"".to_string()));
        assert_eq!(csv_escape("a\nb"), Cow::Owned::<str>("\"a\nb\"".to_string()));
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
    fn write_to_file_appends_results_history() {
        let output_path = std::env::temp_dir().join(format!(
            "backtest_rust_results_{}.csv",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        write_to_file(
            &output_path,
            &ResultRow {
                ohlcv_file: "BTC-USDT_4h",
                precision: "f32",
                strategy: "double_ema",
                params: "fast=5,slow=10",
                duration_ms: 10.0,
                port_value: 1.0,
                max_dd: 2.0,
                sharpe_ratio: 3.0,
            },
        )
        .unwrap();
        write_to_file(
            &output_path,
            &ResultRow {
                ohlcv_file: "BTC-USDT_4h",
                precision: "f64",
                strategy: "double_ema",
                params: "fast=12,slow=24",
                duration_ms: 11.0,
                port_value: 6.0,
                max_dd: 7.0,
                sharpe_ratio: 8.0,
            },
        )
        .unwrap();

        let contents = fs::read_to_string(&output_path).unwrap();
        let lines = contents.lines().collect::<Vec<_>>();

        assert_eq!(lines.len(), 3);
        assert_eq!(
            lines[0],
            "Filename,Date,Precision,Strategy,Params,DurationMs,portfolio_val,max_dd,sharpe_ratio"
        );
        // Params with commas must round-trip through csv_escape.
        assert!(lines[1].contains("\"fast=5,slow=10\""));
        assert!(lines[2].contains("\"fast=12,slow=24\""));

        fs::remove_file(output_path).unwrap();
    }
}
