use crate::*;

/// Exchange.
#[async_trait::async_trait]
pub trait Exchange {
    /// Retrieve candlestick (K-line) data.
    ///
    /// * `product`: Trading product, for example, spot BTC-USDT, futures contract BTC-USDT-SWAP.
    /// * `level`: Time level.
    /// * `time`: Fetch data prior to this time, in milliseconds, 0 indicates fetching the most recent data.
    /// * `return`: Array of candlestick data, with newer data at the front.
    async fn get_k<S>(&self, product: S, level: Level, time: u64) -> anyhow::Result<Vec<K>>
    where
        S: AsRef<str>,
        S: Send;

    /// Retrieve the minimum order quantity.
    ///
    /// * `product`: Trading product, for example, spot BTC-USDT, futures contract BTC-USDT-SWAP.
    /// * `return`: In units of the currency.
    #[cfg(test)]
    async fn get_min_size<S>(&self, product: S) -> anyhow::Result<f32>
    where
        S: AsRef<str>,
        S: Send;

    /// Retrieve the minimum nominal value.
    ///
    /// * `product`: Trading product, for example, spot BTC-USDT, futures contract BTC-USDT-SWAP.
    /// * `return`: In units of fiat currency, returns 0 if the exchange does not specify.
    #[cfg(test)]
    async fn get_min_notional<S>(&self, product: S) -> anyhow::Result<f32>
    where
        S: AsRef<str>,
        S: Send;
}

fn value_array<'a>(
    value: &'a serde_json::Value,
    context: &str,
) -> anyhow::Result<&'a Vec<serde_json::Value>> {
    value
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("{context}: expected array, got {value}"))
}

#[cfg(test)]
fn object_array_field<'a>(
    value: &'a serde_json::Value,
    field: &str,
    context: &str,
) -> anyhow::Result<&'a Vec<serde_json::Value>> {
    value
        .get(field)
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("{context}: missing or invalid '{field}' field in {value}"))
}

#[cfg(test)]
fn object_str_field<'a>(
    value: &'a serde_json::Value,
    field: &str,
    context: &str,
) -> anyhow::Result<&'a str> {
    value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("{context}: missing or invalid '{field}' field in {value}"))
}

#[cfg(test)]
fn parse_object_f32(value: &serde_json::Value, field: &str, context: &str) -> anyhow::Result<f32> {
    object_str_field(value, field, context)?
        .parse::<f32>()
        .map_err(|error| anyhow::anyhow!("{context}: invalid float in '{field}': {error}"))
}

/// Local Exchange.
#[cfg(test)]
#[derive(Debug, Clone)]
pub struct LocalExchange {
    inner: Vec<(String, Level, Vec<K>, f32, f32)>,
}

#[cfg(test)]
impl LocalExchange {
    pub fn new() -> Self {
        Self { inner: Vec::new() }
    }

    /// Insert data.
    ///
    /// * `product`: Trading product, for example, spot BTC-USDT, futures contract BTC-USDT-SWAP.
    /// * `level`: Time level.
    /// * `k`: Candlestick (K-line) data.
    /// * `min_size`: Minimum order quantity.
    /// * `min_notional`: Minimum nominal value.
    pub fn push<S>(
        mut self,
        product: S,
        level: Level,
        k: Vec<K>,
        min_size: f32,
        min_notional: f32,
    ) -> Self
    where
        S: AsRef<str>,
    {
        self.inner.push((
            product.as_ref().to_string(),
            level,
            k,
            min_size,
            min_notional,
        ));
        self
    }
}

#[cfg(test)]
impl std::ops::Deref for LocalExchange {
    type Target = Vec<(String, Level, Vec<K>, f32, f32)>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[cfg(test)]
impl std::ops::DerefMut for LocalExchange {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl Exchange for LocalExchange {
    async fn get_k<S>(&self, product: S, level: Level, time: u64) -> anyhow::Result<Vec<K>>
    where
        S: AsRef<str>,
        S: Send,
    {
        let product = product.as_ref();
        self.inner
            .iter()
            .find(|v| v.0 == product && v.1 == level)
            .map(|v| {
                v.2.iter()
                    .filter(|v| time == 0 || v.time < time)
                    .cloned()
                    .collect()
            })
            .ok_or(anyhow::anyhow!(
                "exchange: no product: {} level: {}",
                product,
                level
            ))
    }

    async fn get_min_size<S>(&self, product: S) -> anyhow::Result<f32>
    where
        S: AsRef<str>,
        S: Send,
    {
        let product = product.as_ref();
        self.inner
            .iter()
            .find(|v| v.0 == product)
            .map(|v| v.3)
            .ok_or(anyhow::anyhow!("exchange: no product: {}", product))
    }

    async fn get_min_notional<S>(&self, product: S) -> anyhow::Result<f32>
    where
        S: AsRef<str>,
        S: Send,
    {
        let product = product.as_ref();
        self.inner
            .iter()
            .find(|v| v.0 == product)
            .map(|v| v.4)
            .ok_or(anyhow::anyhow!("exchange: no product: {}", product))
    }
}

/// OKEx.
#[cfg(test)]
#[derive(Debug, Clone)]
pub struct Okx {
    client: reqwest::Client,
    base_url: String,
}

#[cfg(test)]
impl Okx {
    pub fn new() -> anyhow::Result<Self> {
        Ok(Self {
            client: reqwest::ClientBuilder::new()
                .timeout(std::time::Duration::from_secs(5))
                .build()?,
            base_url: "https://www.okx.com".to_string(),
        })
    }

    #[cfg(test)]
    pub fn with_client(client: reqwest::Client) -> Self {
        Self {
            client,
            base_url: "https://www.okx.com".to_string(),
        }
    }

    #[cfg(test)]
    pub fn base_url<S>(mut self, base_url: S) -> Self
    where
        S: AsRef<str>,
    {
        self.base_url = base_url.as_ref().to_string();
        self
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl Exchange for Okx {
    async fn get_k<S>(&self, product: S, level: Level, time: u64) -> anyhow::Result<Vec<K>>
    where
        S: AsRef<str>,
        S: Send,
    {
        let product = product.as_ref();

        let product = if product.contains("-") {
            product.into()
        } else {
            product_mapping(product)
        };

        let (level, millis) = match level {
            Level::Minute1 => ("1m", 60 * 1000),
            Level::Minute3 => ("3m", 3 * 60 * 1000),
            Level::Minute5 => ("5m", 5 * 60 * 1000),
            Level::Minute15 => ("15m", 15 * 60 * 1000),
            Level::Minute30 => ("30m", 30 * 60 * 1000),
            Level::Hour1 => ("1H", 60 * 60 * 1000),
            Level::Hour2 => ("2H", 2 * 60 * 60 * 1000),
            Level::Hour4 => ("4H", 4 * 60 * 60 * 1000),
            Level::Hour6 => ("6Hutc", 6 * 60 * 60 * 1000),
            Level::Hour12 => ("12Hutc", 12 * 60 * 60 * 1000),
            Level::Day1 => ("1Dutc", 24 * 60 * 60 * 1000),
            Level::Day3 => ("3Dutc", 3 * 24 * 60 * 60 * 1000),
            Level::Week1 => ("1Wutc", 7 * 24 * 60 * 60 * 1000),
            Level::Month1 => {
                // Get the difference between the current timestamp and the beginning of the month timestamp.
                let now = chrono::Utc::now();
                (
                    "1Mutc",
                    std::time::SystemTime::now()
                        .duration_since(std::time::SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64
                        - chrono::TimeZone::with_ymd_and_hms(
                            &chrono::Utc,
                            chrono::Datelike::year(&now),
                            chrono::Datelike::month(&now),
                            1,
                            0,
                            0,
                            0,
                        )
                        .unwrap()
                        .timestamp_millis() as u64,
                )
            }
        };

        let mut url = self.base_url.clone();

        let args = if time == 0 || {
            if let Some(v) = std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .unwrap()
                .checked_sub(std::time::Duration::from_millis(time))
            {
                v <= std::time::Duration::from_millis(millis)
            } else {
                false
            }
        } {
            url += "/api/v5/market/candles";
            serde_json::json!({
                "instId": product,
                "bar": level,
                "limit": "300"
            })
        } else {
            url += "/api/v5/market/history-candles";
            serde_json::json!({
                "instId": product,
                "bar": level,
                "after": time,
            })
        };

        let result = self
            .client
            .get(&url)
            .query(&args)
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        anyhow::ensure!(result["code"] == "0", result.to_string());

        let array = result["data"]
            .as_array()
            .ok_or(anyhow::anyhow!("interface exception"))?;

        let mut result = Vec::with_capacity(array.len());

        for i in array {
            result.push(K {
                time: i[0]
                    .as_str()
                    .ok_or(anyhow::anyhow!("interface exception"))?
                    .parse::<u64>()?,
                open: i[1]
                    .as_str()
                    .ok_or(anyhow::anyhow!("interface exception"))?
                    .parse::<f32>()?,
                high: i[2]
                    .as_str()
                    .ok_or(anyhow::anyhow!("interface exception"))?
                    .parse::<f32>()?,
                low: i[3]
                    .as_str()
                    .ok_or(anyhow::anyhow!("interface exception"))?
                    .parse::<f32>()?,
                close: i[4]
                    .as_str()
                    .ok_or(anyhow::anyhow!("interface exception"))?
                    .parse::<f32>()?,
            });
        }

        Ok(result)
    }

    #[cfg(test)]
    async fn get_min_size<S>(&self, product: S) -> anyhow::Result<f32>
    where
        S: AsRef<str>,
        S: Send,
    {
        let product = product.as_ref();

        let product = if product.contains("-") {
            product.into()
        } else {
            product_mapping(product)
        };

        let inst_type = if product.contains("SWAP") {
            "SWAP"
        } else {
            "SPOT"
        };

        let result = self
            .client
            .get(self.base_url.clone() + "/api/v5/public/instruments")
            .query(&serde_json::json!({
                "instType": inst_type,
                "instId": product
            }))
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        anyhow::ensure!(result["code"] == "0", result.to_string());

        Ok(if inst_type == "SWAP" {
            result["data"][0]["ctVal"]
                .as_str()
                .ok_or(anyhow::anyhow!("interface exception"))?
                .parse::<f32>()?
        } else {
            result["data"][0]["minSz"]
                .as_str()
                .ok_or(anyhow::anyhow!("interface exception"))?
                .parse::<f32>()?
        })
    }

    #[cfg(test)]
    async fn get_min_notional<S>(&self, product: S) -> anyhow::Result<f32>
    where
        S: AsRef<str>,
        S: Send,
    {
        _ = product;
        Ok(0.0)
    }
}

/// Binance.
#[derive(Debug, Clone)]
pub struct Binance {
    client: reqwest::Client,
    base_url_override: Option<String>,
}

impl Binance {
    pub fn new() -> anyhow::Result<Self> {
        Ok(Self {
            client: reqwest::ClientBuilder::new()
                .timeout(std::time::Duration::from_secs(5))
                .build()?,
            base_url_override: None,
        })
    }

    #[cfg(test)]
    pub fn with_client(client: reqwest::Client) -> Self {
        Self {
            client,
            base_url_override: None,
        }
    }

    #[cfg(test)]
    pub fn base_url<S>(mut self, base_url: S) -> Self
    where
        S: AsRef<str>,
    {
        self.base_url_override = Some(base_url.as_ref().trim_end_matches('/').to_string());
        self
    }

    fn endpoint(&self, is_futures: bool, path: &str) -> String {
        let base_url = self.base_url_override.as_deref().unwrap_or({
            if is_futures {
                "https://fapi.binance.com"
            } else {
                "https://api.binance.com"
            }
        });

        format!("{base_url}{path}")
    }
}

#[async_trait::async_trait]
impl crate::Exchange for Binance {
    async fn get_k<S>(
        &self,
        product: S,
        level: crate::Level,
        time: u64,
    ) -> anyhow::Result<Vec<crate::K>>
    where
        S: AsRef<str>,
        S: Send,
    {
        let product = product.as_ref();

        let product = if product.contains("-") {
            product_mapping(product)
        } else {
            product.into()
        };

        let level = match level {
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
        };

        let new_product = product.trim_end_matches("SWAP");
        let is_futures = product.ends_with("SWAP");
        let url = if is_futures {
            self.endpoint(true, "/fapi/v1/continuousKlines")
        } else {
            self.endpoint(false, "/api/v3/klines")
        };

        let args = if is_futures {
            if time == 0 {
                serde_json::json!({
                    "pair": new_product,
                    "interval": level,
                    "contractType": "PERPETUAL",
                    "limit": 1500
                })
            } else {
                serde_json::json!({
                    "pair": new_product,
                    "interval": level,
                    "contractType": "PERPETUAL",
                    "endTime": time - 1,
                    "limit": 1500
                })
            }
        } else if time == 0 {
            serde_json::json!({
                "symbol": new_product,
                "interval": level,
                "limit": 1500
            })
        } else {
            serde_json::json!({
                "symbol": new_product,
                "interval": level,
                "endTime": time - 1,
                "limit": 1500
            })
        };

        let result = self
            .client
            .get(&url)
            .query(&args)
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        anyhow::ensure!(result.is_array(), result.to_string());

        let array = value_array(&result, "binance klines")?;

        let mut result = Vec::with_capacity(array.len());

        for i in array.iter().rev() {
            let values = value_array(i, "binance klines item")?;
            result.push(K {
                time: values
                    .first()
                    .and_then(serde_json::Value::as_u64)
                    .ok_or(anyhow::anyhow!(
                        "binance klines item: missing or invalid open time in {i}"
                    ))?,
                open: values
                    .get(1)
                    .and_then(serde_json::Value::as_str)
                    .ok_or(anyhow::anyhow!(
                        "binance klines item: missing or invalid open price in {i}"
                    ))?
                    .parse::<f32>()?,
                high: values
                    .get(2)
                    .and_then(serde_json::Value::as_str)
                    .ok_or(anyhow::anyhow!(
                        "binance klines item: missing or invalid high price in {i}"
                    ))?
                    .parse::<f32>()?,
                low: values
                    .get(3)
                    .and_then(serde_json::Value::as_str)
                    .ok_or(anyhow::anyhow!(
                        "binance klines item: missing or invalid low price in {i}"
                    ))?
                    .parse::<f32>()?,
                close: values
                    .get(4)
                    .and_then(serde_json::Value::as_str)
                    .ok_or(anyhow::anyhow!(
                        "binance klines item: missing or invalid close price in {i}"
                    ))?
                    .parse::<f32>()?,
            });
        }

        Ok(result)
    }

    #[cfg(test)]
    async fn get_min_size<S>(&self, product: S) -> anyhow::Result<f32>
    where
        S: AsRef<str>,
        S: Send,
    {
        let product = product.as_ref();

        let product = if product.contains("-") {
            product_mapping(product)
        } else {
            product.into()
        };

        let new_product = product.trim_end_matches("SWAP");

        let url = format!(
            "{}?symbol={}",
            self.endpoint(
                product.ends_with("SWAP"),
                if product.ends_with("SWAP") {
                    "/fapi/v1/exchangeInfo"
                } else {
                    "/api/v3/exchangeInfo"
                }
            ),
            new_product
        );

        let result = self
            .client
            .get(&url)
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        let symbols = object_array_field(&result, "symbols", "binance exchangeInfo")?;
        let symbol = symbols
            .iter()
            .find(|v| v.get("symbol").and_then(serde_json::Value::as_str) == Some(new_product))
            .ok_or(anyhow::anyhow!("exchange: no product: {}", product))?;
        let filters = object_array_field(symbol, "filters", "binance exchangeInfo symbol")?;
        let lot_size = filters
            .iter()
            .find(|v| v.get("filterType").and_then(serde_json::Value::as_str) == Some("LOT_SIZE"))
            .ok_or(anyhow::anyhow!(
                "exchange: missing LOT_SIZE filter for product: {}",
                product
            ))?;

        parse_object_f32(lot_size, "minQty", "binance LOT_SIZE filter")
    }

    #[cfg(test)]
    async fn get_min_notional<S>(&self, product: S) -> anyhow::Result<f32>
    where
        S: AsRef<str>,
        S: Send,
    {
        let product = product.as_ref();

        let product = if product.contains("-") {
            product_mapping(product)
        } else {
            product.into()
        };

        let new_product = product.trim_end_matches("SWAP");

        let url = format!(
            "{}?symbol={}",
            self.endpoint(
                product.ends_with("SWAP"),
                if product.ends_with("SWAP") {
                    "/fapi/v1/exchangeInfo"
                } else {
                    "/api/v3/exchangeInfo"
                }
            ),
            new_product
        );

        let result = self
            .client
            .get(&url)
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        let symbols = object_array_field(&result, "symbols", "binance exchangeInfo")?;
        let symbol = symbols
            .iter()
            .find(|v| v.get("symbol").and_then(serde_json::Value::as_str) == Some(new_product))
            .ok_or(anyhow::anyhow!("exchange: no product: {}", product))?;
        let filters = object_array_field(symbol, "filters", "binance exchangeInfo symbol")?;
        let filter_name = if product.ends_with("SWAP") {
            "MIN_NOTIONAL"
        } else {
            "NOTIONAL"
        };
        let field_name = if product.ends_with("SWAP") {
            "notional"
        } else {
            "minNotional"
        };
        let notional = filters
            .iter()
            .find(|v| v.get("filterType").and_then(serde_json::Value::as_str) == Some(filter_name))
            .ok_or(anyhow::anyhow!(
                "exchange: missing {} filter for product: {}",
                filter_name,
                product
            ))?;

        parse_object_f32(notional, field_name, "binance notional filter")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn mock_base_url(body: &'static str) -> String {
        mock_base_url_with_capture(body, None).await.0
    }

    async fn mock_base_url_with_capture(
        body: &'static str,
        request_line: Option<Arc<Mutex<String>>>,
    ) -> (String, Arc<Mutex<String>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock listener");
        let address = listener.local_addr().expect("listener address");
        let request_line = request_line.unwrap_or_else(|| Arc::new(Mutex::new(String::new())));
        let captured_request_line = Arc::clone(&request_line);

        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept mock connection");
            let mut buffer = vec![0u8; 4096];
            let bytes_read = socket.read(&mut buffer).await.expect("read mock request");
            let request = String::from_utf8_lossy(&buffer[..bytes_read]);
            if let Some(first_line) = request.lines().next() {
                *captured_request_line.lock().unwrap() = first_line.to_string();
            }
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            socket
                .write_all(response.as_bytes())
                .await
                .expect("write mock response");
        });

        (format!("http://{address}/"), request_line)
    }

    #[tokio::test]
    async fn binance_get_k_parses_mocked_klines() {
        let base_url =
            mock_base_url(r#"[ [1000,"1.0","2.0","0.5","1.5"], [2000,"2.0","3.0","1.5","2.5"] ]"#)
                .await;
        let exchange = Binance::with_client(reqwest::Client::new()).base_url(base_url);

        let k = exchange.get_k("BTC-USDT", Level::Hour1, 0).await.unwrap();

        assert_eq!(k.len(), 2);
        assert_eq!(k[0].time, 2000);
        assert_eq!(k[1].time, 1000);
        assert_eq!(k[0].close, 2.5);
    }

    #[tokio::test]
    async fn binance_get_k_rejects_non_array_payloads() {
        let base_url = mock_base_url(r#"{"message":"nope"}"#).await;
        let exchange = Binance::with_client(reqwest::Client::new()).base_url(base_url);

        assert!(exchange.get_k("BTC-USDT", Level::Hour1, 0).await.is_err());
    }

    #[tokio::test]
    async fn binance_get_min_size_errors_without_lot_size_filter() {
        let base_url = mock_base_url(
            r#"{"symbols":[{"symbol":"BTCUSDT","filters":[{"filterType":"PRICE_FILTER","tickSize":"0.1"}]}]}"#,
        )
        .await;
        let exchange = Binance::with_client(reqwest::Client::new()).base_url(base_url);

        assert!(exchange.get_min_size("BTC-USDT").await.is_err());
    }

    #[tokio::test]
    async fn binance_get_min_notional_errors_without_notional_filter() {
        let base_url = mock_base_url(
            r#"{"symbols":[{"symbol":"BTCUSDT","filters":[{"filterType":"LOT_SIZE","minQty":"0.001"}]}]}"#,
        )
        .await;
        let exchange = Binance::with_client(reqwest::Client::new()).base_url(base_url);

        assert!(exchange.get_min_notional("BTC-USDT").await.is_err());
    }

    #[tokio::test]
    async fn binance_custom_base_url_uses_clean_paths() {
        let request_line = Arc::new(Mutex::new(String::new()));
        let (base_url, captured_request_line) =
            mock_base_url_with_capture(r#"[ [1000,"1.0","2.0","0.5","1.5"] ]"#, Some(request_line))
                .await;
        let exchange = Binance::with_client(reqwest::Client::new()).base_url(base_url);

        exchange.get_k("BTC-USDT", Level::Hour1, 0).await.unwrap();

        let request_line = captured_request_line.lock().unwrap().clone();
        assert!(request_line.starts_with("GET /api/v3/klines?"));
        assert!(!request_line.contains("api.binance.com"));
        assert!(!request_line.contains("fapi.binance.com"));
    }
}
