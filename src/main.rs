//! Command-line entry point.
//!
//! Parses arguments, refreshes the candle cache, hands the market to a
//! strategy chosen by name, and prints and records the result.
//!
//! This file names **no concrete strategy**: dispatch goes through
//! [`backtest_rust::strategy::registry`], which is what keeps adding a
//! strategy from touching the CLI. The one exception is
//! [`backtest_rust::strategy::registry::DEFAULT_STRATEGY`], used when
//! `--strategy` is absent.
//!
//! The argument parser is hand-rolled rather than pulled from a crate: the
//! surface is a dozen flags, and the dependency list is deliberately short.

use anyhow::Context as _;
use backtest_rust::backtest::{BacktestMetrics, EngineConfig, ExecutionModel};
use backtest_rust::data::{load_data_file, DataPaths};
use backtest_rust::download::download_dump_k_lines;
use backtest_rust::exchange::Level;
use backtest_rust::output::{write_to_file, ResultRow};
use backtest_rust::strategy::params::ParamSpec;
use backtest_rust::strategy::registry::{self, DEFAULT_STRATEGY};
use chrono::TimeZone;
use chrono::Utc;
use std::borrow::Cow;
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

fn default_engine_config() -> EngineConfig {
    EngineConfig {
        pair: Cow::Borrowed("BTC-USDT"),
        level: Level::Minute15,
        threads: 1,
        starting_capital: 1000.0,
        fee_rate: 0.0015,
        risk_free_rate: 0.0,
        execution_model: ExecutionModel::NextOpen,
        show_progress: true,
        progress_step: 10_000,
        download_start: default_download_start(),
        split: None,
    }
}

fn parse_env_bool(value: &str) -> anyhow::Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => anyhow::bail!("invalid boolean value '{value}'"),
    }
}

fn read_env_bool(name: &str) -> anyhow::Result<Option<bool>> {
    match std::env::var(name) {
        Ok(raw) => parse_env_bool(&raw).map(Some),
        Err(_) => Ok(None),
    }
}

#[derive(Debug, PartialEq, Eq)]
enum RunMode {
    Full,
    DownloadOnly,
    ListStrategies,
}

#[derive(Debug)]
struct CliOpts {
    mode: RunMode,
    force_download: bool,
    since: Option<u64>,
    level: Option<Level>,
    pair: Option<String>,
    threads: Option<usize>,
    data_dir: Option<String>,
    results_dir: Option<String>,
    split: Option<f32>,
    strategy: Option<String>,
    params: Vec<String>,
}

impl CliOpts {
    fn data_paths(&self) -> DataPaths {
        let mut paths = DataPaths::default();
        if let Some(dir) = &self.data_dir {
            paths = paths.with_klines_dir(dir);
        }
        if let Some(dir) = &self.results_dir {
            paths = paths.with_results_dir(dir);
        }
        paths
    }
}

fn parse_pair_value(value: &str) -> anyhow::Result<String> {
    let upper = value.trim().to_ascii_uppercase();
    if !upper.contains('-') {
        anyhow::bail!("invalid --pair '{value}'; expected BASE-QUOTE form (e.g. BTC-USDT)");
    }
    Ok(upper)
}

fn parse_threads_value(value: &str) -> anyhow::Result<usize> {
    let n: usize = value
        .trim()
        .parse()
        .with_context(|| format!("invalid --threads '{value}'; expected non-negative integer"))?;
    if n == 0 {
        let auto = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1);
        Ok(auto)
    } else {
        Ok(n)
    }
}

fn parse_since_value(value: &str) -> anyhow::Result<u64> {
    if value.chars().all(|c| c.is_ascii_digit()) {
        return value
            .parse::<u64>()
            .with_context(|| format!("invalid --since unix-ms value '{value}'"));
    }
    let date = chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").with_context(|| {
        format!("invalid --since '{value}'; expected YYYY-MM-DD or unix-milliseconds")
    })?;
    Ok(date
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_millis() as u64)
}

fn parse_cli_args<I, S>(args: I) -> anyhow::Result<CliOpts>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut mode = RunMode::Full;
    let mut force_download = false;
    let mut since: Option<u64> = None;
    let mut level: Option<Level> = None;
    let mut pair: Option<String> = None;
    let mut threads: Option<usize> = None;
    let mut data_dir: Option<String> = None;
    let mut results_dir: Option<String> = None;
    let mut split: Option<f32> = None;
    let mut strategy: Option<String> = None;
    let mut params: Vec<String> = Vec::new();

    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.as_ref() {
            "download" => mode = RunMode::DownloadOnly,
            "list-strategies" => mode = RunMode::ListStrategies,
            "--strategy" => {
                let value = iter
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--strategy requires a name"))?;
                strategy = Some(value.as_ref().trim().to_ascii_lowercase());
            }
            "--param" => {
                let value = iter.next().ok_or_else(|| {
                    anyhow::anyhow!("--param requires an entry (e.g. period=5..100)")
                })?;
                params.push(value.as_ref().to_string());
            }
            "--force" => force_download = true,
            "--since" => {
                let value = iter.next().ok_or_else(|| {
                    anyhow::anyhow!("--since requires a value (YYYY-MM-DD or unix-ms)")
                })?;
                since = Some(parse_since_value(value.as_ref())?);
            }
            "--level" => {
                let value = iter.next().ok_or_else(|| {
                    anyhow::anyhow!("--level requires a value (e.g. 5m, 15m, 1h, 4h, 1d)")
                })?;
                level = Some(Level::from_str(value.as_ref()).map_err(|e| anyhow::anyhow!("{e}"))?);
            }
            "--pair" => {
                let value = iter
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--pair requires a value (e.g. BTC-USDT)"))?;
                pair = Some(parse_pair_value(value.as_ref())?);
            }
            "--threads" => {
                let value = iter.next().ok_or_else(|| {
                    anyhow::anyhow!("--threads requires a value (positive integer or 0 for auto)")
                })?;
                threads = Some(parse_threads_value(value.as_ref())?);
            }
            "--data-dir" => {
                let value = iter
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--data-dir requires a path"))?;
                data_dir = Some(value.as_ref().to_string());
            }
            "--results-dir" => {
                let value = iter
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--results-dir requires a path"))?;
                results_dir = Some(value.as_ref().to_string());
            }
            "--split" => {
                let value = iter.next().ok_or_else(|| {
                    anyhow::anyhow!("--split requires a fraction between 0 and 1 (e.g. 0.7)")
                })?;
                split = Some(parse_split_value(value.as_ref())?);
            }
            "-h" | "--help" => {
                print_usage();
                std::process::exit(0);
            }
            other => anyhow::bail!("unknown argument: {other}"),
        }
    }

    Ok(CliOpts {
        mode,
        force_download,
        since,
        level,
        pair,
        threads,
        data_dir,
        results_dir,
        split,
        strategy,
        params,
    })
}

fn parse_split_value(value: &str) -> anyhow::Result<f32> {
    let fraction: f32 = value
        .trim()
        .parse()
        .with_context(|| format!("invalid --split '{value}'; expected a fraction like 0.7"))?;
    if !(0.0..=1.0).contains(&fraction) {
        anyhow::bail!("invalid --split '{value}'; must be between 0 and 1");
    }
    Ok(fraction)
}

fn print_strategies() {
    println!("Available strategies:\n");
    for info in registry::available() {
        let default = if info.name == DEFAULT_STRATEGY {
            "  (default)"
        } else {
            ""
        };
        println!("  {}{default}\n    {}", info.name, info.description);
        for (parameter, help) in info.parameters {
            println!("      --param {parameter}=…  {help}");
        }
        println!();
    }
    println!("Ranges are written min..max; a bare number pins the value.");
}

fn print_usage() {
    println!("{}", usage_text());
}

/// The `--help` text.
///
/// Returned rather than printed so the test below can assert that the CLI
/// reference in `README.md` is this exact text. Two copies of a flag list is
/// how documentation starts lying.
fn usage_text() -> String {
    format!(
        "Usage: backtest_rust [SUBCOMMAND] [OPTIONS]\n\n\
         Subcommands:\n  \
           download              Download historical klines, then exit (no sweep). Always re-downloads (bypasses the freshness guard).\n  \
           list-strategies       Print the available strategies and their parameters, then exit\n\n\
         Options:\n  \
           --strategy <NAME>     Strategy to sweep (default: {DEFAULT_STRATEGY}; see list-strategies)\n  \
           --param <NAME=VALUE>  Strategy parameter; repeatable. Ranges are min..max\n  \
           --pair <BASE-QUOTE>   Trading pair, e.g. BTC-USDT (default: BTC-USDT)\n  \
           --level <INTERVAL>    Candle interval: 1m 3m 5m 15m 30m 1h 2h 4h 6h 12h 1d 3d 1w 1M (default: 15m)\n  \
           --threads <N>         Rayon worker threads; 0 = auto (default: 1)\n  \
           --force               Bypass the freshness guard and re-download\n  \
           --since <DATE|MS>     Override download start (YYYY-MM-DD or unix-ms)\n  \
           --split <FRACTION>    Optimize on the leading FRACTION of candles and\n                         \
                                 report the winner on the held-out remainder\n  \
           --data-dir <PATH>     Kline cache directory (default: dataKLines)\n  \
           --results-dir <PATH>  Results CSV directory (default: results)\n  \
           -h, --help            Show this message\n\n\
         Environment variables:\n  \
           BACKTEST_SHOW_PROGRESS=0|1   Toggle per-iteration progress log\n  \
           BACKTEST_FORCE_DOWNLOAD=0|1  Alternative to --force for the default mode"
    )
}

fn load_engine_config() -> anyhow::Result<EngineConfig> {
    let mut config = default_engine_config();
    if let Some(value) = read_env_bool("BACKTEST_SHOW_PROGRESS")? {
        config.show_progress = value;
    }
    Ok(config)
}

fn print_metrics(label: &str, m: &BacktestMetrics) {
    println!(
        "{label}: value {:.3}$ | sharpe {:.6} | sortino {:.6} | calmar {:.3} | \
         cagr {:.2}% | max_dd {:.2}% | trades {} | win {:.1}% | exposure {:.1}%",
        m.final_value,
        m.sharpe_ratio,
        m.sortino_ratio,
        m.calmar_ratio,
        m.cagr,
        m.max_drawdown,
        m.trades,
        m.win_rate,
        m.exposure,
    );
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
    let raw_args: Vec<String> = std::env::args().skip(1).collect();
    let cli = parse_cli_args(&raw_args)?;

    if cli.mode == RunMode::ListStrategies {
        print_strategies();
        return Ok(());
    }

    let strategy_name = cli
        .strategy
        .clone()
        .unwrap_or_else(|| DEFAULT_STRATEGY.to_string());
    let strategy_params = ParamSpec::parse(&cli.params)?;

    let mut engine = load_engine_config()?;

    if let Some(since) = cli.since {
        engine.download_start = since;
    }
    if let Some(level) = cli.level {
        engine.level = level;
    }
    if let Some(pair) = cli.pair.clone() {
        engine.pair = Cow::Owned(pair);
    }
    if let Some(threads) = cli.threads {
        engine.threads = threads;
    }
    if let Some(split) = cli.split {
        engine.split = Some(split);
    }

    let env_force = read_env_bool("BACKTEST_FORCE_DOWNLOAD")?.unwrap_or(false);
    let force = cli.force_download || env_force || cli.mode == RunMode::DownloadOnly;

    let paths = cli.data_paths();
    let data_file = paths.feather(&engine.pair, &engine.level);

    let download_result = download_dump_k_lines(
        &paths,
        &engine.pair,
        engine.level,
        engine.download_start..,
        force,
    )
    .await;

    if cli.mode == RunMode::DownloadOnly {
        download_result.with_context(|| {
            format!(
                "failed to download market data for {} {}",
                engine.pair, engine.level
            )
        })?;
        println!("Download complete: {}", data_file.display());
        println!("Time elapsed: {:?}", start.elapsed());
        return Ok(());
    }

    if let Err(error) = download_result {
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

    println!("Doing: {} {}", engine.pair, engine.level);
    let market = load_data_file(&paths, &engine.pair, &engine.level)?;
    print_boundary_timestamp("First", market.timestamps.first().copied());
    print_boundary_timestamp("Last ", market.timestamps.last().copied());

    let report = registry::run_named(&strategy_name, &engine, &strategy_params, &market)?;

    println!("Done");
    println!("Strategy: {}", report.name);
    println!("Precision: {}", report.precision);
    println!("Best params: {}", report.params);
    print_metrics(
        if report.out_of_sample.is_some() {
            "In-sample"
        } else {
            "Result"
        },
        &report.metrics,
    );
    if let Some(out_of_sample) = &report.out_of_sample {
        print_metrics("Out-of-sample", out_of_sample);
        println!(
            "  (the out-of-sample row is the honest one; the in-sample row is \
             fitted to its own data)"
        );
    }
    println!("Sweep duration: {:.3}s", report.duration.as_secs_f64());

    let ohlcv_file = format!("{}-{}", engine.pair, engine.level);
    let precision = report.precision.to_string();
    write_to_file(
        &paths.results(&engine.pair, &engine.level),
        &ResultRow {
            ohlcv_file: &ohlcv_file,
            precision: &precision,
            strategy: report.name,
            params: &report.params,
            duration_ms: report.duration.as_secs_f64() * 1000.0,
            metrics: report.metrics,
            out_of_sample: report.out_of_sample,
        },
    )?;

    println!("Time elapsed: {:?}", start.elapsed());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The README's CLI section must be the real `--help` text, verbatim.
    /// Without this the two drift apart quietly, and a flag list that is
    /// almost right is worse than none.
    #[test]
    fn the_readme_quotes_the_help_output_exactly() {
        // Git may check the README out with CRLF; compare on content only.
        let readme = include_str!("../README.md").replace('\r', "");
        let usage = usage_text().replace('\r', "");
        assert!(
            readme.contains(&usage),
            "README.md is out of date with --help. Replace its CLI reference \
             block with:\n\n{usage}\n"
        );
    }

    #[test]
    fn parse_env_bool_understands_common_values() {
        assert!(parse_env_bool("true").unwrap());
        assert!(parse_env_bool("1").unwrap());
        assert!(!parse_env_bool("false").unwrap());
        assert!(!parse_env_bool("0").unwrap());
    }

    #[test]
    fn parse_cli_args_defaults_to_full_mode() {
        let cli = parse_cli_args::<_, &str>([]).unwrap();
        assert_eq!(cli.mode, RunMode::Full);
        assert!(!cli.force_download);
        assert!(cli.since.is_none());
    }

    #[test]
    fn parse_cli_args_reads_download_subcommand() {
        let cli = parse_cli_args(["download"]).unwrap();
        assert_eq!(cli.mode, RunMode::DownloadOnly);
    }

    #[test]
    fn parse_cli_args_reads_force_flag() {
        assert!(parse_cli_args(["--force"]).unwrap().force_download);
        assert!(parse_cli_args(["--force-download"]).is_err());
    }

    #[test]
    fn parse_cli_args_reads_since_as_date_or_millis() {
        let from_date = parse_cli_args(["--since", "2019-01-01"]).unwrap();
        assert_eq!(from_date.since, Some(1_546_300_800_000));

        let from_ms = parse_cli_args(["--since", "1502928000000"]).unwrap();
        assert_eq!(from_ms.since, Some(1_502_928_000_000));
    }

    #[test]
    fn parse_cli_args_rejects_missing_since_value() {
        assert!(parse_cli_args(["--since"]).is_err());
    }

    #[test]
    fn parse_cli_args_rejects_unknown_flag() {
        assert!(parse_cli_args(["--nope"]).is_err());
    }

    #[test]
    fn parse_since_value_rejects_garbage() {
        assert!(parse_since_value("not-a-date").is_err());
    }

    #[test]
    fn parse_cli_args_reads_level_flag() {
        let cli = parse_cli_args(["--level", "4h"]).unwrap();
        assert_eq!(cli.level, Some(Level::Hour4));

        let cli2 = parse_cli_args(["--level", "1M"]).unwrap();
        assert_eq!(cli2.level, Some(Level::Month1), "1M (capital) is monthly");

        let cli3 = parse_cli_args(["--level", "1m"]).unwrap();
        assert_eq!(
            cli3.level,
            Some(Level::Minute1),
            "1m (lowercase) is one-minute"
        );
    }

    #[test]
    fn parse_cli_args_rejects_unknown_level() {
        assert!(parse_cli_args(["--level", "xyz"]).is_err());
    }

    #[test]
    fn parse_cli_args_reads_pair_and_threads() {
        let cli = parse_cli_args(["--pair", "eth-usdt", "--threads", "8"]).unwrap();
        assert_eq!(cli.pair, Some("ETH-USDT".to_string()), "pair upper-cased");
        assert_eq!(cli.threads, Some(8));
    }

    #[test]
    fn parse_cli_args_threads_zero_means_auto() {
        let cli = parse_cli_args(["--threads", "0"]).unwrap();
        let threads = cli.threads.unwrap();
        assert!(
            threads >= 1,
            "auto must resolve to at least 1, got {threads}"
        );
    }

    #[test]
    fn parse_pair_value_rejects_missing_separator() {
        assert!(parse_pair_value("BTCUSDT").is_err());
        assert!(parse_pair_value("BTC-USDT").is_ok());
    }

    #[test]
    fn parse_cli_args_reads_strategy_and_repeated_params() {
        let cli = parse_cli_args([
            "--strategy",
            "RSI_Reversion",
            "--param",
            "period=5..30",
            "--param",
            "oversold=25",
        ])
        .unwrap();
        assert_eq!(
            cli.strategy,
            Some("rsi_reversion".to_string()),
            "strategy names fold case"
        );
        assert_eq!(cli.params, vec!["period=5..30", "oversold=25"]);
        let spec = ParamSpec::parse(&cli.params).unwrap();
        assert_eq!(spec.range("period", 1..=1).unwrap(), 5..=30);
    }

    #[test]
    fn parse_cli_args_reads_the_list_strategies_subcommand() {
        let cli = parse_cli_args(["list-strategies"]).unwrap();
        assert_eq!(cli.mode, RunMode::ListStrategies);
    }

    #[test]
    fn parse_cli_args_rejects_flags_missing_their_value() {
        assert!(parse_cli_args(["--strategy"]).is_err());
        assert!(parse_cli_args(["--param"]).is_err());
        assert!(parse_cli_args(["--split"]).is_err());
        assert!(parse_cli_args(["--data-dir"]).is_err());
    }

    #[test]
    fn parse_cli_args_reads_split_and_directories() {
        let cli = parse_cli_args([
            "--split",
            "0.7",
            "--data-dir",
            "some/klines",
            "--results-dir",
            "some/results",
        ])
        .unwrap();
        assert_eq!(cli.split, Some(0.7));
        let paths = cli.data_paths();
        assert_eq!(paths.klines_dir(), std::path::Path::new("some/klines"));
    }

    #[test]
    fn parse_split_value_rejects_values_outside_zero_to_one() {
        assert!(parse_split_value("1.5").is_err());
        assert!(parse_split_value("-0.2").is_err());
        assert!(parse_split_value("half").is_err());
        assert_eq!(parse_split_value("0.8").unwrap(), 0.8);
    }

    #[test]
    fn parse_cli_args_combines_all_flags() {
        let cli = parse_cli_args([
            "download",
            "--pair",
            "SOL-USDT",
            "--level",
            "1h",
            "--threads",
            "4",
            "--since",
            "2024-01-01",
            "--force",
        ])
        .unwrap();
        assert_eq!(cli.mode, RunMode::DownloadOnly);
        assert_eq!(cli.pair, Some("SOL-USDT".to_string()));
        assert_eq!(cli.level, Some(Level::Hour1));
        assert_eq!(cli.threads, Some(4));
        assert!(cli.since.is_some());
        assert!(cli.force_download);
    }
}
