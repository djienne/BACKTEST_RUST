//! Binance spot kline client, and the candle types everything else speaks.
//!
//! [`KlineProvider`] is the seam: [`Binance`] is the live implementation, and
//! tests substitute a fake so pagination and cache-merge behaviour can be
//! exercised without the network.
//!
//! # Pagination
//!
//! Binance's `/klines` endpoint is queried backwards from an inclusive
//! `endTime`, one 1500-candle page at a time, until a page reaches past the
//! requested start. Each page advances the cursor one millisecond past its
//! oldest candle, because `endTime` is inclusive and would otherwise refetch
//! it forever.
//!
//! # Retries
//!
//! Non-success statuses are classified rather than fed to the JSON parser:
//! 429 and 418 are retried honouring `Retry-After` (capped), 5xx backs off
//! exponentially, and any other 4xx fails immediately — a bad symbol will fail
//! identically however many times it is asked.
//!
//! # Unclosed candles
//!
//! The API happily returns the currently forming bar, whose OHLC is still
//! moving. [`candle_is_closed`] identifies those and [`get_k_range`] drops
//! them, so a half-formed bar is never written into a cache.

use anyhow::{anyhow, Context, Result};
use chrono::{Months, TimeZone, Utc};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::ops::{Range, RangeFrom, RangeFull, RangeInclusive};
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const INTER_PAGE_DELAY_MS: u64 = 50;
const BINANCE_KLINES_URL: &str = "https://api.binance.com/api/v3/klines";
/// A 1500-candle page over a slow link needs more than the 5s this used to use.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_MAX_RETRIES: u32 = 4;
const DEFAULT_BACKOFF: Duration = Duration::from_millis(500);
/// Upper bound on a server-supplied `Retry-After`, so a hostile or mistaken
/// header cannot park the whole download for hours.
const MAX_RETRY_AFTER: Duration = Duration::from_secs(300);

// All variants are reachable via the `--level` CLI flag (see `Level::FromStr`).
// The binary defaults to `Minute15`.
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

    /// Exact bar duration in milliseconds, or `None` for `Month1` — calendar
    /// months are 28–31 days, so that one needs date arithmetic instead
    /// (see [`candle_close_time_ms`]).
    pub fn fixed_duration_ms(self) -> Option<u64> {
        const MINUTE: u64 = 60_000;
        const HOUR: u64 = 60 * MINUTE;
        const DAY: u64 = 24 * HOUR;
        Some(match self {
            Level::Minute1 => MINUTE,
            Level::Minute3 => 3 * MINUTE,
            Level::Minute5 => 5 * MINUTE,
            Level::Minute15 => 15 * MINUTE,
            Level::Minute30 => 30 * MINUTE,
            Level::Hour1 => HOUR,
            Level::Hour2 => 2 * HOUR,
            Level::Hour4 => 4 * HOUR,
            Level::Hour6 => 6 * HOUR,
            Level::Hour12 => 12 * HOUR,
            Level::Day1 => DAY,
            Level::Day3 => 3 * DAY,
            Level::Week1 => 7 * DAY,
            Level::Month1 => return None,
        })
    }

    /// Bar duration for heuristics that only need an order of magnitude
    /// (cache-freshness thresholds). `Month1` is approximated as 30 days.
    pub fn approx_duration_ms(self) -> u64 {
        self.fixed_duration_ms().unwrap_or(30 * 24 * 60 * 60 * 1000)
    }
}

/// Close time of the candle that opens at `open_ms` — i.e. the open time of the
/// following candle. `None` when the timestamp is not representable as a date,
/// which callers must treat as "unknown", never as "closed".
pub fn candle_close_time_ms(level: Level, open_ms: u64) -> Option<u64> {
    match level.fixed_duration_ms() {
        Some(duration) => open_ms.checked_add(duration),
        None => {
            let opened = Utc
                .timestamp_millis_opt(i64::try_from(open_ms).ok()?)
                .single()?;
            let next = opened.checked_add_months(Months::new(1))?;
            u64::try_from(next.timestamp_millis()).ok()
        }
    }
}

/// Whether the candle opening at `open_ms` has finished forming by `now_ms`.
///
/// Binance happily returns the **currently forming** candle, whose OHLC is
/// still moving. Writing that into the cache freezes a half-formed bar into
/// history, so the download path drops anything this returns `false` for.
/// Unknown timestamps are reported as closed: keeping a suspect candle is
/// recoverable (the next incremental fetch re-reads and replaces it), whereas
/// silently dropping real data is not.
pub fn candle_is_closed(level: Level, open_ms: u64, now_ms: u64) -> bool {
    candle_close_time_ms(level, open_ms).is_none_or(|close_ms| close_ms <= now_ms)
}

impl std::fmt::Display for Level {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_binance_str())
    }
}

impl FromStr for Level {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Special-case the monthly interval before lowercasing — Binance uses
        // `1M` (capital M) for month while `1m` is one minute. Also accept
        // the friendlier `1mo` alias.
        let trimmed = s.trim();
        if trimmed == "1M" || trimmed.eq_ignore_ascii_case("1mo") {
            return Ok(Level::Month1);
        }
        match trimmed.to_ascii_lowercase().as_str() {
            "1m" => Ok(Level::Minute1),
            "3m" => Ok(Level::Minute3),
            "5m" => Ok(Level::Minute5),
            "15m" => Ok(Level::Minute15),
            "30m" => Ok(Level::Minute30),
            "1h" => Ok(Level::Hour1),
            "2h" => Ok(Level::Hour2),
            "4h" => Ok(Level::Hour4),
            "6h" => Ok(Level::Hour6),
            "12h" => Ok(Level::Hour12),
            "1d" => Ok(Level::Day1),
            "3d" => Ok(Level::Day3),
            "1w" => Ok(Level::Week1),
            other => Err(format!(
                "unknown level '{other}' (expected one of: 1m 3m 5m 15m 30m 1h 2h 4h 6h 12h 1d 3d 1w 1M)"
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct K {
    pub time: u64,
    pub open: f32,
    pub high: f32,
    pub low: f32,
    pub close: f32,
    /// Base-asset volume. `NaN` means "this cache predates volume support",
    /// which is distinguishable from a genuine zero-volume bar; it backfills
    /// on the next download.
    #[serde(default = "unknown_volume")]
    pub volume: f32,
}

/// Sentinel for a candle loaded from a cache written before volume existed.
pub fn unknown_volume() -> f32 {
    f32::NAN
}

#[cfg(test)]
impl K {
    /// A flat candle: open/high/low/close all `price`, volume `price`. Keeps
    /// the many timestamp-focused tests from spelling out six fields each.
    pub fn flat(time: u64, price: f32) -> Self {
        Self {
            time,
            open: price,
            high: price,
            low: price,
            close: price,
            volume: price,
        }
    }
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
    max_retries: u32,
    base_backoff: Duration,
}

impl Binance {
    pub fn new() -> Result<Self> {
        Self::with_retries(DEFAULT_MAX_RETRIES)
    }

    /// Same as [`Binance::new`] with an explicit retry budget. `0` disables
    /// retries, which is what tests want.
    pub fn with_retries(max_retries: u32) -> Result<Self> {
        Ok(Self {
            client: reqwest::ClientBuilder::new()
                .timeout(REQUEST_TIMEOUT)
                .build()?,
            max_retries,
            base_backoff: DEFAULT_BACKOFF,
        })
    }

    /// Fetch one page, retrying transient failures with exponential backoff.
    async fn fetch_page(&self, query: &[(&'static str, String)]) -> Result<serde_json::Value> {
        let mut attempt = 0u32;
        loop {
            match self.try_fetch_page(query).await {
                Ok(value) => return Ok(value),
                Err(FetchError::Fatal(error)) => return Err(error),
                Err(FetchError::Retryable { error, delay }) => {
                    if attempt >= self.max_retries {
                        return Err(error.context(format!(
                            "binance klines: gave up after {} attempt(s)",
                            attempt + 1
                        )));
                    }
                    let wait = delay.unwrap_or_else(|| backoff_delay(self.base_backoff, attempt));
                    eprintln!(
                        "binance klines: {error:#} — retrying in {:.1}s ({}/{})",
                        wait.as_secs_f64(),
                        attempt + 1,
                        self.max_retries
                    );
                    tokio::time::sleep(wait).await;
                    attempt += 1;
                }
            }
        }
    }

    async fn try_fetch_page(
        &self,
        query: &[(&'static str, String)],
    ) -> std::result::Result<serde_json::Value, FetchError> {
        let response = match self
            .client
            .get(BINANCE_KLINES_URL)
            .query(query)
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                let transient = error.is_timeout() || error.is_connect();
                let error = anyhow::Error::new(error).context("binance klines: request failed");
                return Err(if transient {
                    FetchError::Retryable { error, delay: None }
                } else {
                    FetchError::Fatal(error)
                });
            }
        };

        // Without this, a 429 or 5xx body is fed straight to the JSON parser
        // and surfaces as a baffling "expected array, got {...}".
        let status = response.status();
        if !status.is_success() {
            let retry_after = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let body = response.text().await.unwrap_or_default();
            let error = anyhow!("binance klines: HTTP {status} — {}", truncate(&body, 300));
            return Err(match classify_status(status, retry_after.as_deref()) {
                Retry::No => FetchError::Fatal(error),
                Retry::After(delay) => FetchError::Retryable { error, delay },
            });
        }

        response
            .json()
            .await
            .map_err(|error| FetchError::Retryable {
                error: anyhow::Error::new(error).context("binance klines: malformed response body"),
                delay: None,
            })
    }
}

enum FetchError {
    Fatal(anyhow::Error),
    Retryable {
        error: anyhow::Error,
        /// Server-requested delay, when it supplied one.
        delay: Option<Duration>,
    },
}

#[derive(Debug, PartialEq, Eq)]
enum Retry {
    No,
    After(Option<Duration>),
}

/// Retry policy for a non-success status. Rate limits (429) and IP bans (418)
/// are retryable and carry a `Retry-After`; 5xx is retryable without one; every
/// other 4xx (bad symbol, bad interval) is a client mistake that will fail
/// identically forever.
fn classify_status(status: StatusCode, retry_after: Option<&str>) -> Retry {
    let too_many = status == StatusCode::TOO_MANY_REQUESTS || status.as_u16() == 418;
    if too_many {
        let delay = retry_after
            .and_then(|raw| raw.trim().parse::<u64>().ok())
            .map(Duration::from_secs)
            .map(|delay| delay.min(MAX_RETRY_AFTER));
        return Retry::After(delay);
    }
    if status.is_server_error() {
        return Retry::After(None);
    }
    Retry::No
}

fn backoff_delay(base: Duration, attempt: u32) -> Duration {
    base * 2u32.saturating_pow(attempt.min(6))
}

fn truncate(text: &str, max_chars: usize) -> String {
    match text.char_indices().nth(max_chars) {
        Some((index, _)) => format!("{}…", &text[..index]),
        None => text.to_owned(),
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

            let response = self.fetch_page(&query).await?;

            let array = response
                .as_array()
                .ok_or_else(|| anyhow!("binance klines: expected array, got {response}"))?;

            let mut result = Vec::with_capacity(array.len());
            for item in array.iter().rev() {
                let values = item
                    .as_array()
                    .ok_or_else(|| anyhow!("binance klines item: expected array, got {item}"))?;
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
                    volume: parse_field(values, 5, "volume", item)?,
                });
            }
            Ok(result)
        }
    }
}

/// Build the query parameters for Binance's `/api/v3/klines` endpoint.
/// `end_time_ms` is treated as **inclusive** to match the Binance API
/// contract; pass `0` to omit the parameter entirely.
fn build_klines_query(
    symbol: &str,
    interval: &str,
    end_time_ms: u64,
) -> Vec<(&'static str, String)> {
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

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before unix epoch")?
        .as_millis() as u64;
    let mut end = range.end.unwrap_or(now_ms);

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

    // Drop the still-forming candle(s) at the head of the window. Only the very
    // newest can normally be unfinished, but filtering is cheap and it also
    // covers a clock that has drifted behind the exchange's.
    let before = result.len();
    result.retain(|candle| candle_is_closed(level, candle.time, now_ms));
    if result.len() != before {
        eprintln!(
            "Skipped {} still-forming {level} candle(s); they will be fetched once closed.",
            before - result.len()
        );
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
    fn level_from_str_round_trips_via_display() {
        for level in [
            Level::Minute1,
            Level::Minute3,
            Level::Minute5,
            Level::Minute15,
            Level::Minute30,
            Level::Hour1,
            Level::Hour2,
            Level::Hour4,
            Level::Hour6,
            Level::Hour12,
            Level::Day1,
            Level::Day3,
            Level::Week1,
            Level::Month1,
        ] {
            let parsed: Level = Level::from_str(&level.to_string())
                .unwrap_or_else(|e| panic!("round-trip failed for {level}: {e}"));
            assert_eq!(parsed, level);
        }
    }

    #[test]
    fn level_from_str_distinguishes_minute_from_month() {
        assert_eq!(Level::from_str("1m").unwrap(), Level::Minute1);
        assert_eq!(Level::from_str("1M").unwrap(), Level::Month1);
        assert_eq!(Level::from_str("1mo").unwrap(), Level::Month1, "alias");
        assert_eq!(Level::from_str("1MO").unwrap(), Level::Month1, "alias");
    }

    #[test]
    fn level_from_str_rejects_garbage() {
        assert!(Level::from_str("xyz").is_err());
        assert!(Level::from_str("").is_err());
        assert!(Level::from_str("99h").is_err());
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
        // The trait's `+ '_` ties the future to `&self` only, so the body may
        // not borrow `product`; `async fn` would capture every input lifetime.
        #[allow(clippy::manual_async_fn)]
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
        K::flat(time, 1.0)
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
    async fn get_k_range_drops_the_still_forming_candle() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let four_hours = 4 * 60 * 60 * 1000u64;
        // Head of the page opens inside the current 4h bucket → still forming.
        let forming = now - four_hours / 2;
        let closed = now - 2 * four_hours;
        let provider = FakeProvider::new(vec![vec![k(forming), k(closed)]]);

        let result = get_k_range(&provider, "ANY", Level::Hour4, 0u64..)
            .await
            .unwrap();

        assert!(
            !result.iter().any(|c| c.time == forming),
            "the forming candle must not reach the cache"
        );
        assert!(
            result.iter().any(|c| c.time == closed),
            "closed candles must survive"
        );
    }

    #[test]
    fn fixed_duration_ms_matches_the_interval() {
        assert_eq!(Level::Minute15.fixed_duration_ms(), Some(15 * 60_000));
        assert_eq!(Level::Hour4.fixed_duration_ms(), Some(4 * 3_600_000));
        assert_eq!(Level::Week1.fixed_duration_ms(), Some(7 * 86_400_000));
        assert_eq!(Level::Month1.fixed_duration_ms(), None, "calendar month");
        assert_eq!(Level::Month1.approx_duration_ms(), 30 * 86_400_000);
    }

    #[test]
    fn candle_close_time_uses_calendar_months_for_monthly_bars() {
        // 2024-01-01T00:00:00Z → 2024-02-01T00:00:00Z (31 days, not 30).
        let jan = 1_704_067_200_000u64;
        let feb = 1_706_745_600_000u64;
        assert_eq!(candle_close_time_ms(Level::Month1, jan), Some(feb));
        assert_eq!(feb - jan, 31 * 86_400_000, "January really is 31 days");
    }

    #[test]
    fn candle_is_closed_compares_against_the_bar_end() {
        let open = 1_704_067_200_000u64;
        let hour = 3_600_000u64;
        assert!(!candle_is_closed(Level::Hour1, open, open), "just opened");
        assert!(
            !candle_is_closed(Level::Hour1, open, open + hour - 1),
            "one millisecond short of closing"
        );
        assert!(candle_is_closed(Level::Hour1, open, open + hour), "closed");
    }

    #[test]
    fn candle_is_closed_treats_unrepresentable_timestamps_as_closed() {
        // Keeping a suspect candle is recoverable; dropping real data is not.
        assert!(candle_is_closed(Level::Month1, u64::MAX, 0));
    }

    #[test]
    fn classify_status_retries_rate_limits_with_the_server_delay() {
        assert_eq!(
            classify_status(StatusCode::TOO_MANY_REQUESTS, Some("3")),
            Retry::After(Some(Duration::from_secs(3)))
        );
        assert_eq!(
            classify_status(StatusCode::from_u16(418).unwrap(), Some("120")),
            Retry::After(Some(Duration::from_secs(120))),
            "418 is Binance's IP ban"
        );
        assert_eq!(
            classify_status(StatusCode::TOO_MANY_REQUESTS, None),
            Retry::After(None),
            "no header → fall back to backoff"
        );
    }

    #[test]
    fn classify_status_caps_a_hostile_retry_after() {
        assert_eq!(
            classify_status(StatusCode::TOO_MANY_REQUESTS, Some("999999")),
            Retry::After(Some(MAX_RETRY_AFTER))
        );
        assert_eq!(
            classify_status(StatusCode::TOO_MANY_REQUESTS, Some("nonsense")),
            Retry::After(None),
            "unparseable header → backoff, not a panic"
        );
    }

    #[test]
    fn classify_status_retries_server_errors_but_not_client_errors() {
        assert_eq!(
            classify_status(StatusCode::SERVICE_UNAVAILABLE, None),
            Retry::After(None)
        );
        assert_eq!(
            classify_status(StatusCode::BAD_REQUEST, None),
            Retry::No,
            "an invalid symbol will fail identically forever"
        );
        assert_eq!(classify_status(StatusCode::NOT_FOUND, None), Retry::No);
    }

    #[test]
    fn backoff_delay_doubles_and_saturates() {
        let base = Duration::from_millis(500);
        assert_eq!(backoff_delay(base, 0), Duration::from_millis(500));
        assert_eq!(backoff_delay(base, 1), Duration::from_secs(1));
        assert_eq!(backoff_delay(base, 3), Duration::from_secs(4));
        // The `min(6)` clamp keeps the multiplier from overflowing.
        assert_eq!(backoff_delay(base, 99), Duration::from_secs(32));
    }

    #[test]
    fn truncate_marks_shortened_bodies_and_respects_char_boundaries() {
        assert_eq!(truncate("short", 100), "short");
        assert_eq!(truncate("abcdef", 3), "abc…");
        assert_eq!(truncate("héllo", 2), "hé…", "must not split a code point");
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
