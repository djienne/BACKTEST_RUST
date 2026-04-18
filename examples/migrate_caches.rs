//! One-shot helper: re-download every timeframe of BTC-USDT as a Feather
//! cache file. Used during the JSON → Feather migration. Safe to delete after.
//!
//! Run: `cargo run --release --example migrate_caches`

use backtest_rust::download::download_dump_k_lines;
use backtest_rust::exchange::Level;

const PAIR: &str = "BTC-USDT";
const SINCE_MS: u64 = 1_546_300_800_000; // 2019-01-01T00:00:00Z

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let levels = [
        Level::Minute5,
        Level::Minute15,
        Level::Minute30,
        Level::Hour1,
        Level::Hour2,
        Level::Hour4,
        Level::Hour12,
    ];

    for level in levels {
        println!("\n=== Re-downloading {PAIR} {level} ===");
        if let Err(error) = download_dump_k_lines(PAIR, level, SINCE_MS.., true).await {
            eprintln!("FAILED for {level}: {error:#}");
        }
    }
    Ok(())
}
