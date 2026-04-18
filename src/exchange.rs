use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::ops::RangeFrom;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

#[derive(Debug, Clone, Copy)]
pub struct TimeRange {
    pub start: u64,
    pub end: u64,
}

impl From<RangeFrom<u64>> for TimeRange {
    fn from(value: RangeFrom<u64>) -> Self {
        Self {
            start: value.start,
            end: u64::MAX - 1,
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
        let symbol = product.replace('-', "");
        let interval = level.as_binance_str();

        let mut query: Vec<(&str, String)> = vec![
            ("symbol", symbol),
            ("interval", interval.to_string()),
            ("limit", "1500".to_string()),
        ];
        if time != 0 {
            query.push(("endTime", (time - 1).to_string()));
        }

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
                    .get(0)
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

    if range.start == 0 && range.end == 0 {
        let mut time = 0;
        loop {
            let v = exchange.get_k(product, level, time).await?;
            if let Some(k) = v.last() {
                time = k.time;
                result.extend(v);
            } else {
                break;
            }
        }
        return Ok(result);
    }

    let mut end = range.end;
    if end == u64::MAX - 1 {
        end = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock before unix epoch")?
            .as_millis() as u64;
    }

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
            end = k.time;
            result.extend(v);
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
        assert_eq!(range.end, u64::MAX - 1);
    }
}
