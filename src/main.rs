use anyhow::Context as _;
use backtest_rust::backtest::{run, EngineConfig, ExecutionModel};
use backtest_rust::data::{data_file_path, load_data_file, results_file_path};
use backtest_rust::download::download_dump_k_lines;
use backtest_rust::exchange::Level;
use backtest_rust::output::{write_to_file, ResultRow};
use backtest_rust::strategy::double_ema::{DoubleEmaConfig, DoubleEmaCrossover};
use backtest_rust::strategy::Strategy;
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
        execution_model: ExecutionModel::NextOpen,
        show_progress: true,
        progress_step: 10_000,
        download_start: default_download_start(),
    }
}

fn default_strategy_config() -> DoubleEmaConfig {
    DoubleEmaConfig {
        fast_period_min: 5,
        slow_period_min: 6,
        max_period: 600,
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
}

#[derive(Debug)]
struct CliOpts {
    mode: RunMode,
    force_download: bool,
    since: Option<u64>,
    level: Option<Level>,
    pair: Option<String>,
    threads: Option<usize>,
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

    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.as_ref() {
            "download" => mode = RunMode::DownloadOnly,
            "--force" => force_download = true,
            "--since" => {
                let value = iter
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--since requires a value (YYYY-MM-DD or unix-ms)"))?;
                since = Some(parse_since_value(value.as_ref())?);
            }
            "--level" => {
                let value = iter
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--level requires a value (e.g. 5m, 15m, 1h, 4h, 1d)"))?;
                level = Some(
                    Level::from_str(value.as_ref())
                        .map_err(|e| anyhow::anyhow!("{e}"))?,
                );
            }
            "--pair" => {
                let value = iter
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--pair requires a value (e.g. BTC-USDT)"))?;
                pair = Some(parse_pair_value(value.as_ref())?);
            }
            "--threads" => {
                let value = iter
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--threads requires a value (positive integer or 0 for auto)"))?;
                threads = Some(parse_threads_value(value.as_ref())?);
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
    })
}

fn print_usage() {
    println!(
        "Usage: backtest_rust [SUBCOMMAND] [OPTIONS]\n\n\
         Subcommands:\n  \
           download              Download historical klines, then exit (no sweep). Always re-downloads (bypasses the freshness guard).\n\n\
         Options:\n  \
           --pair <BASE-QUOTE>   Trading pair, e.g. BTC-USDT (default: BTC-USDT)\n  \
           --level <INTERVAL>    Candle interval: 1m 3m 5m 15m 30m 1h 2h 4h 6h 12h 1d 3d 1w 1M (default: 15m)\n  \
           --threads <N>         Rayon worker threads; 0 = auto (default: 1)\n  \
           --force               Bypass the freshness guard and re-download\n  \
           --since <DATE|MS>     Override download start (YYYY-MM-DD or unix-ms)\n  \
           -h, --help            Show this message\n\n\
         Environment variables:\n  \
           BACKTEST_SHOW_PROGRESS=0|1   Toggle per-iteration progress log\n  \
           BACKTEST_FORCE_DOWNLOAD=0|1  Alternative to --force for the default mode"
    );
}

fn load_engine_config() -> anyhow::Result<EngineConfig> {
    let mut config = default_engine_config();
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
    let raw_args: Vec<String> = std::env::args().skip(1).collect();
    let cli = parse_cli_args(&raw_args)?;

    let mut engine = load_engine_config()?;
    let strategy_config = default_strategy_config();

    if let Some(since) = cli.since {
        engine.download_start = since;
    }
    if let Some(level) = cli.level {
        engine.level = level;
    }
    if let Some(pair) = cli.pair {
        engine.pair = Cow::Owned(pair);
    }
    if let Some(threads) = cli.threads {
        engine.threads = threads;
    }

    let env_force = read_env_bool("BACKTEST_FORCE_DOWNLOAD")?.unwrap_or(false);
    let force = cli.force_download || env_force || cli.mode == RunMode::DownloadOnly;

    let data_file = data_file_path(&engine.pair, &engine.level);

    let download_result = download_dump_k_lines(
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
    let market = load_data_file(&engine.pair, &engine.level)?;
    print_boundary_timestamp("First", market.timestamps.first().copied());
    print_boundary_timestamp("Last ", market.timestamps.last().copied());

    let selected = run::<DoubleEmaCrossover>(&engine, &strategy_config, &market)?;
    let params_summary = DoubleEmaCrossover::param_summary(selected.best.params);

    println!("Done");
    println!("Strategy: {}", DoubleEmaCrossover::NAME);
    println!("Precision: {}", selected.precision);
    println!(
        "Best result: sharpe: {:.6}, max_dd: {:.4}, params: {}",
        selected.best.metrics.sharpe_ratio,
        selected.best.metrics.max_drawdown,
        params_summary,
    );
    println!(
        "Final portfolio value: {:.3}$",
        selected.best.metrics.final_value
    );
    println!("Sweep duration: {:.3}s", selected.duration.as_secs_f64());

    let ohlcv_file = format!("{}-{}", engine.pair, engine.level);
    let precision = selected.precision.to_string();
    write_to_file(
        &results_file_path(&engine.pair, &engine.level),
        &ResultRow {
            ohlcv_file: &ohlcv_file,
            precision: &precision,
            strategy: DoubleEmaCrossover::NAME,
            params: &params_summary,
            duration_ms: selected.duration.as_secs_f64() * 1000.0,
            port_value: selected.best.metrics.final_value,
            max_dd: selected.best.metrics.max_drawdown,
            sharpe_ratio: selected.best.metrics.sharpe_ratio,
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
        assert_eq!(cli3.level, Some(Level::Minute1), "1m (lowercase) is one-minute");
    }

    #[test]
    fn parse_cli_args_rejects_unknown_level() {
        assert!(parse_cli_args(["--level", "xyz"]).is_err());
    }

    #[test]
    fn parse_cli_args_reads_pair_and_threads() {
        let cli =
            parse_cli_args(["--pair", "eth-usdt", "--threads", "8"]).unwrap();
        assert_eq!(cli.pair, Some("ETH-USDT".to_string()), "pair upper-cased");
        assert_eq!(cli.threads, Some(8));
    }

    #[test]
    fn parse_cli_args_threads_zero_means_auto() {
        let cli = parse_cli_args(["--threads", "0"]).unwrap();
        let threads = cli.threads.unwrap();
        assert!(threads >= 1, "auto must resolve to at least 1, got {threads}");
    }

    #[test]
    fn parse_pair_value_rejects_missing_separator() {
        assert!(parse_pair_value("BTCUSDT").is_err());
        assert!(parse_pair_value("BTC-USDT").is_ok());
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
