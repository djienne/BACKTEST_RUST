use super::exchange::{get_k_range, Binance, Level, TimeRange, K};
use anyhow::{Context, Result};
use chrono::prelude::*; // This crate provides easy-to-use date and time functions
use std::fs;
use std::io::{BufWriter, Write};
use std::time::Duration;
use std::{
    fs::File,
    fs::OpenOptions,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, PartialEq)]
pub struct CandleSeries {
    pub timestamps: Vec<u64>,
    pub open_prices: Vec<f32>,
    pub close_prices: Vec<f32>,
}

pub fn data_file_path(pair: &str, level: &Level) -> PathBuf {
    Path::new("dataKLines").join(format!("{pair}-{level}.json"))
}

pub fn results_file_path(pair: &str, level: &Level) -> PathBuf {
    Path::new("results").join(format!("{pair}-{level}.csv"))
}

pub fn load_data_file(pair: &str, level: &Level) -> Result<CandleSeries> {
    let k_v = load_k_lines(pair, level)?;

    let timestamps = k_v.iter().map(|k| k.time).collect();
    let open_prices = k_v.iter().map(|k| k.open).collect();
    let close_prices = k_v.iter().map(|k| k.close).collect();

    Ok(CandleSeries {
        timestamps,
        open_prices,
        close_prices,
    })
}

pub fn calculate_max_drawdown(portfolio_values: &[f32]) -> f32 {
    if portfolio_values.is_empty() {
        return 0.0;
    }

    let mut max_drawdown = 0.0;
    let mut peak = portfolio_values[0];

    for &value in portfolio_values.iter() {
        if value > peak {
            peak = value;
        }
        let drawdown = if peak > 0.0 {
            (peak - value) / peak
        } else {
            0.0
        };
        if drawdown > max_drawdown {
            max_drawdown = drawdown;
        }
    }

    max_drawdown * 100.0
}

pub fn calculate_sharpe_ratio(
    returns: &[f32],
    risk_free_rate: f32,
    periods_per_year: usize,
) -> f32 {
    if returns.len() < 2 || periods_per_year == 0 {
        return 0.0; // Return 0 if no returns data is available to avoid division by zero
    }

    let mean_return = returns.iter().sum::<f32>() / returns.len() as f32;
    let std_dev_return = (returns
        .iter()
        .map(|&r| (r - mean_return).powi(2))
        .sum::<f32>()
        / (returns.len() - 1) as f32)
        .sqrt();
    let annualized_std_dev = std_dev_return / (periods_per_year as f32).sqrt();

    if !annualized_std_dev.is_finite() || annualized_std_dev == 0.0 {
        0.0 // Avoid division by zero if standard deviation is zero
    } else {
        (mean_return - risk_free_rate) / annualized_std_dev
    }
}

pub fn write_to_file(
    output_path: &Path,
    ohlcv_file: &str,
    port_value: f32,
    max_dd: f32,
    sharpe_ratio: f32,
    period1: usize,
    period2: usize,
) -> std::io::Result<()> {
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
            "Filename,Date,portfolio_val,max_dd,sharpe_ratio,Period1,Period2"
        )?;
    }

    let now = Utc::now();
    writeln!(
        writer,
        "{},{},{:.3},{:.3},{:.3},{},{}",
        ohlcv_file,
        now.to_rfc3339(),
        port_value,
        max_dd,
        sharpe_ratio,
        period1,
        period2
    )?;

    writer.flush() // Make sure to flush the buffer
}

pub fn load_k_lines(pair: &str, level: &Level) -> Result<Vec<K>> {
    let datafile = data_file_path(pair, level);

    let contents = fs::read_to_string(&datafile)
        .with_context(|| format!("Failed to read market data file: {}", datafile.display()))?;
    let k_v: Vec<K> = serde_json::from_str(&contents)
        .with_context(|| format!("Failed to parse market data file: {}", datafile.display()))?;

    Ok(k_v)
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

pub async fn download_dump_k_lines_to_json<T>(product: &str, level: Level, range: T) -> Result<()>
where
    T: Into<TimeRange>,
{
    let exchange = Binance::new().context("Failed to create Binance client")?;
    let folder_path = Path::new("dataKLines");

    fs::create_dir_all(folder_path)
        .with_context(|| format!("Failed to create directory: {}", folder_path.display()))?;

    let json_path = data_file_path(product, &level);

    // Check if the file exists
    let reason1 = json_path.exists() && json_path.is_file();

    // Return early if the file exists and is recent
    if reason1 && was_modified_less_than_x_day_ago(&json_path, 2)? {
        println!(
            "File {:?} already exists and is recent (< 2 days old). Skip Download.",
            json_path
        );
        return Ok(());
    }

    if !reason1 {
        println!("Downloading {:?} because file does not exist...", json_path);
    } else {
        println!("Downloading {:?} because file is too old...", json_path);
    }

    let mut k_vec = get_k_range(&exchange, product, level, range)
        .await
        .with_context(|| format!("Failed to download candlesticks for {product} {level}"))?;

    k_vec.reverse();

    // do some checks

    ensure_strictly_increasing_and_unique(&k_vec)?;

    let (result, maxgap) = interval_check(&k_vec);

    if !result {
        println!(
            "Warning: times in k_vec are not all separated by the same amount. max gap: {:} hours",
            maxgap / 60 / 60 / 1000
        );
    }

    //println!("{:?}", k_vec);
    println!("Done.");

    let serialized = serde_json::to_string_pretty(&k_vec)
        .context("Failed to serialize downloaded candlesticks")?;
    //println!("Serialized to JSON: {:?}", serialized);

    // Write to a file.
    let mut file = File::create(&json_path)
        .with_context(|| format!("Failed to create market data file: {}", json_path.display()))?;
    file.write_all(serialized.as_bytes())
        .with_context(|| format!("Failed to write market data file: {}", json_path.display()))?;

    Ok(())
}

pub fn was_modified_less_than_x_day_ago(path: &Path, nb_days: u64) -> Result<bool> {
    let modified_time = fs::metadata(path)
        .with_context(|| format!("Failed to read metadata for {}", path.display()))?
        .modified()
        .with_context(|| format!("Failed to read modified time for {}", path.display()))?;

    let day = Duration::from_secs(86_400 * nb_days);
    let elapsed_time = modified_time
        .elapsed()
        .with_context(|| format!("Failed to calculate file age for {}", path.display()))?;

    Ok(elapsed_time < day)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sharpe_ratio_is_zero_for_a_single_return() {
        assert_eq!(calculate_sharpe_ratio(&[0.05], 0.0, 365), 0.0);
    }

    #[test]
    fn load_data_file_reads_repository_fixture() {
        let candles = load_data_file("BTC-USDT", &Level::Hour4).expect("fixture data should load");

        assert_eq!(candles.timestamps.len(), candles.close_prices.len());
        assert_eq!(candles.timestamps.len(), candles.open_prices.len());
        assert!(!candles.timestamps.is_empty());
    }

    #[test]
    fn calculate_max_drawdown_is_safe_for_empty_inputs() {
        assert_eq!(calculate_max_drawdown(&[]), 0.0);
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

        write_to_file(&output_path, "BTC-USDT_4h", 1.0, 2.0, 3.0, 4, 5).unwrap();
        write_to_file(&output_path, "BTC-USDT_4h", 6.0, 7.0, 8.0, 9, 10).unwrap();

        let contents = fs::read_to_string(&output_path).unwrap();
        let lines = contents.lines().collect::<Vec<_>>();

        assert_eq!(lines.len(), 3);
        assert_eq!(
            lines[0],
            "Filename,Date,portfolio_val,max_dd,sharpe_ratio,Period1,Period2"
        );

        fs::remove_file(output_path).unwrap();
    }

    #[test]
    fn timestamp_validator_returns_structured_errors() {
        let candles = vec![
            K {
                time: 2,
                open: 1.0,
                high: 1.0,
                low: 1.0,
                close: 1.0,
            },
            K {
                time: 1,
                open: 1.0,
                high: 1.0,
                low: 1.0,
                close: 1.0,
            },
        ];

        assert!(ensure_strictly_increasing_and_unique(&candles).is_err());
    }
}
