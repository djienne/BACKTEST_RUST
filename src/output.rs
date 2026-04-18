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
    pub duration_ms: f64,
    pub port_value: f64,
    pub max_dd: f64,
    pub sharpe_ratio: f64,
    pub period1: usize,
    pub period2: usize,
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
            "Filename,Date,Precision,DurationMs,portfolio_val,max_dd,sharpe_ratio,Period1,Period2"
        )?;
    }

    let now = Utc::now();
    writeln!(
        writer,
        "{},{},{},{:.3},{:.3},{:.3},{:.3},{},{}",
        csv_escape(row.ohlcv_file),
        now.to_rfc3339(),
        csv_escape(row.precision),
        row.duration_ms,
        row.port_value,
        row.max_dd,
        row.sharpe_ratio,
        row.period1,
        row.period2
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
                duration_ms: 10.0,
                port_value: 1.0,
                max_dd: 2.0,
                sharpe_ratio: 3.0,
                period1: 4,
                period2: 5,
            },
        )
        .unwrap();
        write_to_file(
            &output_path,
            &ResultRow {
                ohlcv_file: "BTC-USDT_4h",
                precision: "f64",
                duration_ms: 11.0,
                port_value: 6.0,
                max_dd: 7.0,
                sharpe_ratio: 8.0,
                period1: 9,
                period2: 10,
            },
        )
        .unwrap();

        let contents = fs::read_to_string(&output_path).unwrap();
        let lines = contents.lines().collect::<Vec<_>>();

        assert_eq!(lines.len(), 3);
        assert_eq!(
            lines[0],
            "Filename,Date,Precision,DurationMs,portfolio_val,max_dd,sharpe_ratio,Period1,Period2"
        );

        fs::remove_file(output_path).unwrap();
    }
}
