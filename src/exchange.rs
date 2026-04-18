use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
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

    pub async fn get_k(&self, product: &str, level: Level, time: u64) -> Result<Vec<K>> {
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
            let values = item
                .as_array()
                .ok_or_else(|| anyhow!("binance klines item: expected array, got {item}"))?;
            result.push(K {
                time: values
                    .first()
                    .and_then(serde_json::Value::as_u64)
                    .ok_or_else(|| anyhow!("binance klines item: missing open time in {item}"))?,
                open: parse_field(values, 1, "open price", item)?,
                high: parse_field(values, 2, "high price", item)?,
                low: parse_field(values, 3, "low price", item)?,
                close: parse_field(values, 4, "close price", item)?,
            });
        }
        Ok(result)
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

pub async fn get_k_range<T>(
    exchange: &Binance,
    product: &str,
    level: Level,
    range: T,
) -> Result<Vec<K>>
where
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
        let v = exchange.get_k(product, level, end).await?;
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
}
