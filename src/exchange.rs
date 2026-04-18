use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::ops::{Range, RangeFrom, RangeFull, RangeInclusive};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const INTER_PAGE_DELAY_MS: u64 = 50;

// All variants are kept so the candlestick interval can be selected via
// configuration. The current binary defaults to Minute15; the rest stay
// around for any future timeframe selection (env var, CLI flag, or strategy
// config).
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Level {
    Minute1,
    Minute3,
    Minute5,
    Minute15,
    Minute30,
    Hour1,
    Hour2,
    Hour4,
    Hour6,
    Hour12,
    Day1,
    Day3,
    Week1,
    Month1,
}

impl Level {
    fn as_binance_str(self) -> &'static str {
        match self {
            Level::Minute1 => "1m",
            Level::Minute3 => "3m",
            Level::Minute5 => "5m",
            Level::Minute15 => "15m",
            Level::Minute30 => "30m",
            Level::Hour1 => "1h",
            Level::Hour2 => "2h",
            Level::Hour4 => "4h",
            Level::Hour6 => "6h",
            Level::Hour12 => "12h",
            Level::Day1 => "1d",
            Level::Day3 => "3d",
            Level::Week1 => "1w",
            Level::Month1 => "1M",
        }
    }
}

impl std::fmt::Display for Level {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_binance_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct K {
    pub time: u64,
    pub open: f32,
    pub high: f32,
    pub low: f32,
    pub close: f32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeRange {
    pub start: u64,
    /// Inclusive upper bound; `None` means "up to current wall-clock time".
    pub end: Option<u64>,
}

impl From<RangeFrom<u64>> for TimeRange {
    fn from(value: RangeFrom<u64>) -> Self {
        Self {
            start: value.start,
            end: None,
        }
    }
}

impl From<Range<u64>> for TimeRange {
    fn from(value: Range<u64>) -> Self {
        Self {
            start: value.start,
            end: value.end.checked_sub(1),
        }
    }
}

impl From<RangeInclusive<u64>> for TimeRange {
    fn from(value: RangeInclusive<u64>) -> Self {
        let (start, end) = value.into_inner();
        Self {
            start,
            end: Some(end),
        }
    }
}

impl From<RangeFull> for TimeRange {
    fn from(_value: RangeFull) -> Self {
        Self {
            start: 0,
            end: None,
        }
    }
}

/// Source of kline data. Abstracts over the live Binance HTTP client and
/// any test fake. Concrete impls just need to return one page of candles
/// ending at (or before) `time`, newest-first. `time = 0` means "now".
pub trait KlineProvider {
    fn get_k(
        &self,
        product: &str,
        level: Level,
        time: u64,
    ) -> impl Future<Output = Result<Vec<K>>> + Send + '_;
}

#[derive(Debug, Clone)]
pub struct Binance {
    client: reqwest::Client,
}

impl Binance {
    pub fn new() -> Result<Self> {
        Ok(Self {
            client: reqwest::ClientBuilder::new()
                .timeout(Duration::from_secs(5))
                .build()?,
        })
    }
}

impl KlineProvider for Binance {
    fn get_k(
        &self,
        product: &str,
        level: Level,
        time: u64,
    ) -> impl Future<Output = Result<Vec<K>>> + Send + '_ {
        let product = product.to_owned();
        async move {
            if product.contains("SWAP") {
                return Err(anyhow!(
                    "SWAP / futures products are not supported by this client (got {product})"
                ));
            }
            let symbol = product.replace('-', "");
            let interval = level.as_binance_str();
            let query = build_klines_query(&symbol, interval, time);

            let response: serde_json::Value = self
                .client
                .get("https://api.binance.com/api/v3/klines")
                .query(&query)
                .send()
                .await?
                .json()
                .await?;

            let array = response
                .as_array()
                .ok_or_else(|| anyhow!("binance klines: expected array, got {response}"))?;

            let mut result = Vec::with_capacity(array.len());
            for item in array.iter().rev() {
                let values = item.as_array().ok_or_else(|| {
                    anyhow!("binance klines item: expected array, got {item}")
                })?;
                result.push(K {
                    time: values
                        .first()
                        .and_then(serde_json::Value::as_u64)
                        .ok_or_else(|| {
                            anyhow!("binance klines item: missing open time in {item}")
                        })?,
                    open: parse_field(values, 1, "open price", item)?,
                    high: parse_field(values, 2, "high price", item)?,
                    low: parse_field(values, 3, "low price", item)?,
                    close: parse_field(values, 4, "close price", item)?,
                });
            }
            Ok(result)
        }
    }
}

/// Build the query parameters for Binance's `/api/v3/klines` endpoint.
/// `end_time_ms` is treated as **inclusive** to match the Binance API
/// contract; pass `0` to omit the parameter entirely.
fn build_klines_query(symbol: &str, interval: &str, end_time_ms: u64) -> Vec<(&'static str, String)> {
    let mut query: Vec<(&'static str, String)> = vec![
        ("symbol", symbol.to_string()),
        ("interval", interval.to_string()),
        ("limit", "1500".to_string()),
    ];
    if end_time_ms != 0 {
        query.push(("endTime", end_time_ms.to_string()));
    }
    query
}

fn parse_field(
    values: &[serde_json::Value],
    index: usize,
    name: &str,
    item: &serde_json::Value,
) -> Result<f32> {
    values
        .get(index)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow!("binance klines item: missing {name} in {item}"))?
        .parse::<f32>()
        .map_err(|error| anyhow!("binance klines item: invalid {name} in {item}: {error}"))
}

pub async fn get_k_range<P, T>(
    provider: &P,
    product: &str,
    level: Level,
    range: T,
) -> Result<Vec<K>>
where
    P: KlineProvider,
    T: Into<TimeRange>,
{
    let range = range.into();
    let mut result = Vec::new();

    let mut end = match range.end {
        Some(end) => end,
        None => SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock before unix epoch")?
            .as_millis() as u64,
    };

    loop {
        let v = provider.get_k(product, level, end).await?;
        if let Some(k) = v.last() {
            if k.time < range.start {
                for i in v {
                    if i.time >= range.start {
                        result.push(i);
                    }
                }
                break;
            }
            // Move past the just-fetched oldest candle so the next page does
            // not re-fetch it (Binance treats `endTime` as inclusive).
            // `saturating_sub` guards the unrealistic `k.time == 0` edge.
            end = k.time.saturating_sub(1);
            result.extend(v);
            tokio::time::sleep(Duration::from_millis(INTER_PAGE_DELAY_MS)).await;
        } else {
            break;
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn level_display_matches_binance_intervals() {
        assert_eq!(Level::Hour4.to_string(), "4h");
        assert_eq!(Level::Minute15.to_string(), "15m");
        assert_eq!(Level::Month1.to_string(), "1M");
    }

    #[test]
    fn time_range_from_range_from_uses_unbounded_end() {
        let range: TimeRange = (1_000u64..).into();
        assert_eq!(range.start, 1_000);
        assert_eq!(range.end, None);
    }

    #[test]
    fn time_range_from_range_inclusive_keeps_end() {
        let range: TimeRange = (10u64..=20).into();
        assert_eq!(range.start, 10);
        assert_eq!(range.end, Some(20));
    }

    #[test]
    fn time_range_from_range_makes_end_inclusive_minus_one() {
        let range: TimeRange = (10u64..20).into();
        assert_eq!(range.start, 10);
        assert_eq!(range.end, Some(19));
    }

    #[test]
    fn time_range_from_range_full_is_unbounded_from_zero() {
        let range: TimeRange = (..).into();
        assert_eq!(range.start, 0);
        assert_eq!(range.end, None);
    }

    #[test]
    fn build_klines_query_includes_inclusive_end_time() {
        let q = build_klines_query("BTCUSDT", "4h", 1_700_000_000_000);
        assert!(q.contains(&("symbol", "BTCUSDT".to_string())));
        assert!(q.contains(&("interval", "4h".to_string())));
        assert!(q.contains(&("limit", "1500".to_string())));
        assert!(
            q.contains(&("endTime", "1700000000000".to_string())),
            "endTime must be passed through unchanged: {q:?}"
        );
    }

    #[test]
    fn build_klines_query_omits_end_time_when_zero() {
        let q = build_klines_query("BTCUSDT", "4h", 0);
        assert!(
            q.iter().all(|(k, _)| *k != "endTime"),
            "endTime must be omitted when the caller passes 0: {q:?}"
        );
    }

    #[tokio::test]
    async fn binance_get_k_rejects_swap_products() {
        let exchange = Binance::new().unwrap();
        let result = exchange.get_k("BTC-USDT-SWAP", Level::Hour4, 0).await;
        assert!(result.is_err());
        let message = format!("{:#}", result.unwrap_err());
        assert!(message.contains("SWAP"), "error message: {message}");
    }

    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Mutex;

    /// Test fake: returns pre-canned pages, records the `time` arg of each
    /// call so tests can assert on the pagination cursor.
    struct FakeProvider {
        pages: Vec<Vec<K>>,
        cursor: AtomicUsize,
        received_times: Mutex<Vec<u64>>,
    }

    impl FakeProvider {
        fn new(pages: Vec<Vec<K>>) -> Self {
            Self {
                pages,
                cursor: AtomicUsize::new(0),
                received_times: Mutex::new(Vec::new()),
            }
        }
        fn calls(&self) -> Vec<u64> {
            self.received_times.lock().unwrap().clone()
        }
    }

    impl KlineProvider for FakeProvider {
        fn get_k(
            &self,
            _product: &str,
            _level: Level,
            time: u64,
        ) -> impl Future<Output = Result<Vec<K>>> + Send + '_ {
            async move {
                self.received_times.lock().unwrap().push(time);
                let i = self.cursor.fetch_add(1, AtomicOrdering::Relaxed);
                Ok(self.pages.get(i).cloned().unwrap_or_default())
            }
        }
    }

    fn k(time: u64) -> K {
        K {
            time,
            open: 1.0,
            high: 1.0,
            low: 1.0,
            close: 1.0,
        }
    }

    #[tokio::test]
    async fn get_k_range_paginates_until_start_reached() {
        // Two pages, newest-first per page (matches Binance's reversed
        // post-processing in get_k). Range starts at t=100; page 1 covers
        // [200..300], page 2 covers [100..200] which is the boundary, page 3
        // would be empty so the loop terminates at the boundary instead.
        let page1 = vec![k(300), k(250), k(200)]; // newest-first
        let page2 = vec![k(200), k(150), k(100)];
        let provider = FakeProvider::new(vec![page1.clone(), page2.clone()]);

        let result = get_k_range(&provider, "ANY", Level::Hour4, 100u64..=300u64)
            .await
            .unwrap();

        // First call must use the inclusive end (300) per A1's fix.
        let calls = provider.calls();
        assert_eq!(calls[0], 300, "first call must pass the inclusive end");
        // Second call must move past the oldest of page1 (200) to avoid
        // refetching it: 200 - 1 = 199.
        assert_eq!(
            calls[1], 199,
            "second call must advance past oldest of previous page"
        );

        // All candles ≥ start are present, none below.
        assert!(result.iter().all(|c| c.time >= 100));
        assert!(result.iter().any(|c| c.time == 100));
        assert!(result.iter().any(|c| c.time == 300));
    }

    #[tokio::test]
    async fn get_k_range_filters_to_start_on_final_page() {
        // Final page contains candles before the start; they must be dropped.
        let page1 = vec![k(80), k(60), k(40)]; // all < range.start=50 → drop 40
        let provider = FakeProvider::new(vec![page1]);

        let result = get_k_range(&provider, "ANY", Level::Hour4, 50u64..=100u64)
            .await
            .unwrap();

        assert!(result.iter().all(|c| c.time >= 50));
        assert!(!result.iter().any(|c| c.time == 40));
    }

    #[tokio::test]
    async fn get_k_range_handles_empty_first_page() {
        let provider = FakeProvider::new(vec![Vec::new()]);
        let result = get_k_range(&provider, "ANY", Level::Hour4, 0u64..=10u64)
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn get_k_range_uses_explicit_inclusive_end() {
        let provider = FakeProvider::new(vec![vec![k(20), k(15), k(10)]]);
        let _ = get_k_range(&provider, "ANY", Level::Hour4, 10u64..=20u64)
            .await
            .unwrap();
        assert_eq!(
            provider.calls()[0],
            20,
            "RangeInclusive end must be passed through unchanged"
        );
    }
}
