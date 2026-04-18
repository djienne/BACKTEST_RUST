use crate::data::data_file_path;
use crate::exchange::{get_k_range, Binance, Level, TimeRange, K};
use anyhow::{Context, Result};
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::time::Duration;

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
    let cache_exists = json_path.exists() && json_path.is_file();

    if cache_exists && was_modified_less_than_x_day_ago(&json_path, 2)? {
        println!(
            "File {:?} already exists and is recent (< 2 days old). Skip Download.",
            json_path
        );
        return Ok(());
    }

    if cache_exists {
        println!("Downloading {:?} because file is too old...", json_path);
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

pub fn was_modified_less_than_x_day_ago(path: &Path, nb_days: u64) -> Result<bool> {
    let modified_time = fs::metadata(path)
        .with_context(|| format!("Failed to read metadata for {}", path.display()))?
        .modified()
        .with_context(|| format!("Failed to read modified time for {}", path.display()))?;

    let max_age = Duration::from_secs(86_400 * nb_days);
    let elapsed_time = modified_time
        .elapsed()
        .with_context(|| format!("Failed to calculate file age for {}", path.display()))?;

    Ok(elapsed_time < max_age)
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
}
