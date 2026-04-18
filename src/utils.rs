use super::auto_trading::*;
use anyhow::{Context, Result};
use chrono::prelude::*; // This crate provides easy-to-use date and time functions
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{BufWriter, Write};
use std::time::Duration;
use std::{
    fs::File,
    fs::OpenOptions,
    io::BufReader,
    path::{Path, PathBuf},
};

#[derive(Serialize, Deserialize)]
pub struct MarketData {
    timestamp: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

pub fn convert_f64_to_f32(f64_vec: Vec<f64>) -> Vec<f32> {
    f64_vec.into_iter().map(|value: f64| value as f32).collect()
}

pub fn convert_f32_to_f64(f32_values: &[f32]) -> Vec<f64> {
    f32_values.iter().map(|&value: &f32| value as f64).collect()
}

fn convert_vec_u64_to_i64(vec_u64: Vec<u64>) -> Vec<i64> {
    vec_u64
        .iter()
        .map(|&x| {
            if x > i64::MAX as u64 {
                i64::MAX
            } else {
                x as i64
            }
        })
        .collect()
}

pub fn data_file_path(pair: &str, level: &Level) -> PathBuf {
    Path::new("dataKLines").join(format!("{pair}-{level}.json"))
}

pub fn load_data_file(pair: &str, level: &Level) -> Result<(Vec<i64>, Vec<f32>)> {
    let k_v = load_k_lines(pair, level)?;

    let close_prices: Vec<f32> = k_v.iter().map(|k| k.close).collect();
    let timestamps: Vec<u64> = k_v.iter().map(|k| k.time).collect();
    let timestamps: Vec<i64> = convert_vec_u64_to_i64(timestamps);

    Ok((timestamps, close_prices))
}

pub fn calculate_max_drawdown(portfolio_values: &[f32]) -> f32 {
    let mut max_drawdown = 0.0;
    let mut peak = portfolio_values[0];

    for &value in portfolio_values.iter() {
        if value > peak {
            peak = value;
        }
        let drawdown = (peak - value) / peak;
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
    ohlcv_file: &str,
    port_value: f32,
    max_dd: f32,
    sharpe_ratio: f32,
    period1: usize,
    period2: usize,
) -> std::io::Result<()> {
    let file = OpenOptions::new()
        .write(true)
        .truncate(true) // Truncate the file each time it's opened
        .create(true)
        .open("results.csv")?;

    let mut writer = BufWriter::new(file);

    // Check if file is empty to write headers
    if writer.get_ref().metadata()?.len() == 0 {
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

pub fn load_market_data(filepath: impl AsRef<Path>) -> Result<Vec<MarketData>> {
    let file = File::open(filepath.as_ref())
        .with_context(|| format!("Failed to open file: {:?}", filepath.as_ref()))?;
    let reader = BufReader::new(file);
    let data = serde_json::from_reader(reader).with_context(|| "Failed to parse JSON data")?;
    Ok(data)
}

pub fn extract_fields(data: &[MarketData]) -> (Vec<i64>, Vec<f64>) {
    let timestamps = data.iter().map(|entry| entry.timestamp).collect();
    let close_prices = data.iter().map(|entry| entry.close).collect();
    (timestamps, close_prices)
}

pub fn get_filename(path_str: &str) -> &str {
    let path = Path::new(path_str);

    // Extract the file stem (base name without extension)
    if let Some(stem) = path.file_stem() {
        if let Some(stem_str) = stem.to_str() {
            //println!("Base name: {}", stem_str);
            return stem_str;
        } else {
            println!("File stem contains invalid UTF-8 characters.");
            return "error";
        }
    } else {
        println!("No base name found in the path.");
        return "error";
    }
}

pub fn load_k_lines(pair: &str, level: &Level) -> Result<Vec<K>> {
    let datafile = data_file_path(pair, level);

    let contents = fs::read_to_string(&datafile)
        .with_context(|| format!("Failed to read market data file: {}", datafile.display()))?;
    let k_v: Vec<K> = serde_json::from_str(&contents)
        .with_context(|| format!("Failed to parse market data file: {}", datafile.display()))?;

    Ok(k_v)
}

fn is_strictly_increasing_and_unique(v: &[K]) -> bool {
    if v.len() <= 1 {
        return true; // A vector with 0 or 1 element is trivially strictly increasing and unique
    }

    let mut found: bool = false;

    for i in 1..v.len() {
        if v[i].time <= v[i - 1].time {
            println!("{:}", v[i - 1].time);
            println!("{:}", v[i].time);
            found = true;
        }
    }

    if !found {
        return true;
    } else {
        return false;
    }
}

fn has_constant_interval(v: &[K]) -> (bool, u64) {
    if v.len() <= 1 {
        return (true, 0); // A vector with 0 or 1 element trivially has a constant interval
    }

    // Calculate the expected interval from the first two elements
    let expected_interval = v[1].time.wrapping_sub(v[0].time);

    let mut found: bool = false;

    let mut max_gap = expected_interval;

    for i in 1..v.len() {
        let interval = v[i].time.wrapping_sub(v[i - 1].time);
        if interval != expected_interval {
            if interval > max_gap {
                max_gap = interval;
            }
            // println!("{}", i);
            // println!("{}", expected_interval);
            // println!("{:}", v[i - 1].time);
            // println!("{:}", v[i].time);
            // println!("{:}", v[i + 1].time);
            // println!("{:}", v[i].time - v[i - 1].time);
            // println!("{:}", v[i + 1].time - v[i].time);
            found = true;
        }
    }

    if !found {
        return (true, max_gap);
    } else {
        return (false, max_gap);
    }
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

    anyhow::ensure!(
        is_strictly_increasing_and_unique(&k_vec),
        "Downloaded candlesticks are not strictly increasing and unique"
    );

    let (result, maxgap) = has_constant_interval(&k_vec);

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

    let day = Duration::from_secs(86_400 * nb_days); // 24 hours
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
        let (timestamps, close_prices) =
            load_data_file("BTC-USDT", &Level::Hour4).expect("fixture data should load");

        assert_eq!(timestamps.len(), close_prices.len());
        assert!(!timestamps.is_empty());
    }
}
