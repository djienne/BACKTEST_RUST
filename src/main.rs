use anyhow::Context as _;
use backtest_rust::backtest::{
    print_precision_comparison, run, ExecutionModel, RunConfig, RunReport,
};
use backtest_rust::data::{data_file_path, load_data_file, results_file_path};
use backtest_rust::download::download_dump_k_lines_to_json;
use backtest_rust::exchange::Level;
use backtest_rust::output::{write_to_file, ResultRow};
use backtest_rust::precision::Precision;
use chrono::TimeZone;
use chrono::Utc;
use std::fmt::Display;
use std::str::FromStr;
use std::time::Instant;

fn default_download_start() -> u64 {
    chrono::NaiveDate::from_ymd_opt(2019, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_millis() as u64
}

fn default_run_config() -> RunConfig {
    RunConfig {
        pair: "BTC-USDT",
        level: Level::Hour4,
        threads: 12,
        fast_period_min: 5,
        slow_period_min: 6,
        max_period: 600,
        starting_capital: 1000.0,
        fee_rate: 0.0015,
        execution_model: ExecutionModel::NextOpen,
        precision: Precision::F32,
        compare_precisions: false,
        show_progress: true,
        progress_step: 10_000,
        download_start: default_download_start(),
    }
}

fn parse_env_bool(value: &str) -> anyhow::Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => anyhow::bail!("invalid boolean value '{value}'"),
    }
}

fn read_env_opt<T>(name: &str) -> anyhow::Result<Option<T>>
where
    T: FromStr,
    T::Err: Display,
{
    match std::env::var(name) {
        Ok(raw) => raw
            .parse::<T>()
            .map(Some)
            .map_err(|err| anyhow::anyhow!("invalid {name} value '{raw}': {err}")),
        Err(_) => Ok(None),
    }
}

fn read_env_bool(name: &str) -> anyhow::Result<Option<bool>> {
    match std::env::var(name) {
        Ok(raw) => parse_env_bool(&raw).map(Some),
        Err(_) => Ok(None),
    }
}

fn load_run_config() -> anyhow::Result<RunConfig> {
    let mut config = default_run_config();
    if let Some(value) = read_env_opt::<Precision>("BACKTEST_PRECISION")? {
        config.precision = value;
    }
    if let Some(value) = read_env_bool("BACKTEST_COMPARE_PRECISIONS")? {
        config.compare_precisions = value;
    }
    if let Some(value) = read_env_bool("BACKTEST_SHOW_PROGRESS")? {
        config.show_progress = value;
    }
    Ok(config)
}

fn print_boundary_timestamp(label: &str, ts: Option<u64>) {
    let Some(ts) = ts else { return };
    let signed = match i64::try_from(ts) {
        Ok(v) => v,
        Err(_) => {
            eprintln!("{label} timestamp: out of i64 range ({ts})");
            return;
        }
    };
    match Utc.timestamp_millis_opt(signed).single() {
        Some(date) => println!("{} timestamp : {}", label, date.format("%Y-%m-%d")),
        None => eprintln!("{label} timestamp: invalid millis ({ts})"),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let start = Instant::now();
    let config = load_run_config()?;

    let data_file = data_file_path(config.pair, &config.level);
    if let Err(error) =
        download_dump_k_lines_to_json(config.pair, config.level, config.download_start..).await
    {
        if !data_file.is_file() {
            return Err(error).with_context(|| {
                format!(
                    "failed to download market data and no cached file is available at {}",
                    data_file.display()
                )
            });
        }
        eprintln!(
            "Download failed ({error:#}). Falling back to cached data at {}.",
            data_file.display()
        );
    }

    println!("Doing: {:} {:}", config.pair, config.level);
    let market = load_data_file(config.pair, &config.level)?;
    print_boundary_timestamp("First", market.timestamps.first().copied());
    print_boundary_timestamp("Last ", market.timestamps.last().copied());

    let RunReport {
        selected,
        comparison,
    } = run(config, &market)?;

    if let Some((f32_run, f64_run)) = comparison {
        print_precision_comparison(f32_run, f64_run);
    }

    println!("Done");
    println!("Precision: {}", selected.precision);
    println!(
        "Best result: sharpe: {:.6}, max_dd: {:.4}, Period1: {}, Period2: {}",
        selected.best.metrics.sharpe_ratio,
        selected.best.metrics.max_drawdown,
        selected.best.fast_period,
        selected.best.slow_period
    );
    println!(
        "Final portfolio value: {:.3}$",
        selected.best.metrics.final_value
    );
    println!("Sweep duration: {:.3}s", selected.duration.as_secs_f64());

    let ohlcv_file = format!("{}_{}", config.pair, config.level);
    let precision = selected.precision.to_string();
    write_to_file(
        &results_file_path(config.pair, &config.level),
        &ResultRow {
            ohlcv_file: &ohlcv_file,
            precision: &precision,
            duration_ms: selected.duration.as_secs_f64() * 1000.0,
            port_value: selected.best.metrics.final_value,
            max_dd: selected.best.metrics.max_drawdown,
            sharpe_ratio: selected.best.metrics.sharpe_ratio,
            period1: selected.best.fast_period,
            period2: selected.best.slow_period,
        },
    )?;

    println!("Time elapsed: {:?}", start.elapsed());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_env_bool_understands_common_values() {
        assert!(parse_env_bool("true").unwrap());
        assert!(parse_env_bool("1").unwrap());
        assert!(!parse_env_bool("false").unwrap());
        assert!(!parse_env_bool("0").unwrap());
    }
}
