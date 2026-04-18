use anyhow::Context as _;
use backtest_rust::backtest::{run, ExecutionModel, RunConfig, RunReport};
use backtest_rust::data::{data_file_path, load_data_file, results_file_path};
use backtest_rust::download::download_dump_k_lines;
use backtest_rust::exchange::Level;
use backtest_rust::output::{write_to_file, ResultRow};
use chrono::TimeZone;
use chrono::Utc;
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
        level: Level::Minute15,
        threads: 1,
        fast_period_min: 5,
        slow_period_min: 6,
        max_period: 600,
        starting_capital: 1000.0,
        fee_rate: 0.0015,
        execution_model: ExecutionModel::NextOpen,
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

    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.as_ref() {
            "download" => mode = RunMode::DownloadOnly,
            "--force" | "--force-download" => force_download = true,
            "--since" => {
                let value = iter
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--since requires a value (YYYY-MM-DD or unix-ms)"))?;
                since = Some(parse_since_value(value.as_ref())?);
            }
            "-h" | "--help" => {
                print_usage();
                std::process::exit(0);
            }
            other => anyhow::bail!("unknown argument: {other}"),
        }
    }

    Ok(CliOpts { mode, force_download, since })
}

fn print_usage() {
    println!(
        "Usage: backtest_rust [SUBCOMMAND] [OPTIONS]\n\n\
         Subcommands:\n  \
           download           Download historical klines, then exit (no sweep)\n\n\
         Options:\n  \
           --force, --force-download   Bypass the freshness guard and re-download\n  \
           --since <DATE|MS>           Override download start (YYYY-MM-DD or unix-ms)\n  \
           -h, --help                  Show this message\n\n\
         Environment variables:\n  \
           BACKTEST_SHOW_PROGRESS=0|1   Toggle per-iteration progress log\n  \
           BACKTEST_FORCE_DOWNLOAD=0|1  Alternative to --force for the default mode"
    );
}

fn load_run_config() -> anyhow::Result<RunConfig> {
    let mut config = default_run_config();
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

    let mut config = load_run_config()?;
    if let Some(since) = cli.since {
        config.download_start = since;
    }

    let env_force = read_env_bool("BACKTEST_FORCE_DOWNLOAD")?.unwrap_or(false);
    let force = cli.force_download || env_force || cli.mode == RunMode::DownloadOnly;

    let data_file = data_file_path(config.pair, &config.level);

    if cli.mode == RunMode::DownloadOnly {
        download_dump_k_lines(
            config.pair,
            config.level,
            config.download_start..,
            force,
        )
        .await
        .with_context(|| {
            format!(
                "failed to download market data for {} {}",
                config.pair, config.level
            )
        })?;
        println!("Download complete: {}", data_file.display());
        println!("Time elapsed: {:?}", start.elapsed());
        return Ok(());
    }

    if let Err(error) = download_dump_k_lines(
        config.pair,
        config.level,
        config.download_start..,
        force,
    )
    .await
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

    let RunReport { selected } = run(config, &market)?;

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
        assert!(parse_cli_args(["--force-download"]).unwrap().force_download);
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
}
