//! Can be implemented using recursion, but this may cause a stack overflow, so it is fully implemented using a Vec instead.

use crate::*;
use chrono::TimeZone;

pub fn yield_map<'a, F>(source: &'a Source, f: F) -> impl Iterator<Item = f32> + 'a
where
    F: FnMut(&Source) -> f32 + 'a,
{
    source.iter().enumerate().map(|v| &source[v.0..]).map(f)
}

pub fn yield_nan<F>(source: &Source, mut f: F) -> f32
where
    F: FnMut(f32, &Source) -> f32,
{
    let mut result = f32::NAN;
    let mut index = source.len();

    while index != 0 {
        index -= 1;
        result = f(result, &source[index..]);
    }

    result
}

pub fn highest(source: &Source, length: usize) -> f32 {
    if source.len() < length {
        return f32::NAN;
    }

    let mut result = source[0];

    for i in 1..length {
        if source[i] > result {
            result = source[i];
        }
    }

    result
}

pub fn highest_index(source: &Source, length: usize) -> Option<usize> {
    if source.len() < length {
        return None;
    }

    let mut result = source[0];
    let mut index = 0;

    for i in 1..length {
        if source[i] > result {
            result = source[i];
            index = i;
        }
    }

    Some(index)
}

pub fn lowest(source: &Source, length: usize) -> f32 {
    if source.len() < length {
        return f32::NAN;
    }

    let mut result = source[0];

    for i in 1..length {
        if source[i] < result {
            result = source[i];
        }
    }

    result
}

pub fn lowest_index(source: &Source, length: usize) -> Option<usize> {
    if source.len() < length {
        return None;
    }

    let mut result = source[0];
    let mut index = 0;

    for i in 1..length {
        if source[i] < result {
            result = source[i];
            index = i;
        }
    }

    Some(index)
}

pub fn sma(source: &Source, length: usize) -> f32 {
    if source.len() < length {
        return f32::NAN;
    }

    source.iter().take(length).sum::<f32>() / length as f32
}

pub fn ema(source: &Source, length: usize) -> f32 {
    if source.len() < length {
        return f32::NAN;
    }

    let alpha = 2.0f32 / (length + 1) as f32;

    yield_nan(source, |prev, source| {
        if prev.is_nan() {
            source[0]
        } else {
            alpha * source + (1.0 - alpha) * prev
        }
    })
}

pub fn rma(source: &Source, length: usize) -> f32 {
    if source.len() < length {
        return f32::NAN;
    }

    let alpha = 1.0 / length as f32;

    yield_nan(source, |prev, source| {
        if prev.is_nan() {
            sma(source, length)
        } else {
            alpha * source + (1.0f32 - alpha) * prev
        }
    })
}

pub fn cci(source: &Source, length: usize) -> f32 {
    if source.len() < length {
        return f32::NAN;
    }

    let ma = sma(source, length);
    let sum = yield_map(&source[..length], |v| (v - ma).abs()).sum::<f32>();
    (source - ma) / (0.015f32 * (sum / length as f32))
}

pub fn macd(
    source: &Source,
    short_length: usize,
    long_length: usize,
    dea_length: usize,
) -> (f32, f32, f32) {
    if source.len() < short_length || source.len() < long_length || source.len() < dea_length {
        return (f32::NAN, f32::NAN, f32::NAN);
    }

    let dif = ema(source, short_length) - ema(source, long_length);
    let dea = ema(
        Source::new(
            // Do not use take
            &yield_map(source, |v| ema(v, short_length) - ema(v, long_length))
                .collect::<Vec<f32>>(),
        ),
        dea_length,
    );
    let macd = (dif - dea) * 2.0;
    (dif, dea, macd)
}

pub fn rsi(source: &Source, length: usize) -> f32 {
    if source.len() < length {
        return f32::NAN;
    }

    let u = yield_map(source, |v| {
        let temp = v - v[1];
        let temp = if temp.is_nan() { 0.0 } else { temp };
        temp.max(0.0)
    })
    .collect::<Vec<f32>>();

    let d = yield_map(source, |v| {
        let temp = v[1] - v;
        let temp = if temp.is_nan() { 0.0 } else { temp };
        temp.max(0.0)
    })
    .collect::<Vec<f32>>();

    let rs = rma(Source::new(&u), length) / rma(Source::new(&d), length);

    100.0f32 - 100.0f32 / (1.0f32 + rs)
}

/// Returns true if on the current candlestick (`k` line), the value of `source` is greater than the value of `value`,
/// and on the previous candlestick, the value of `source` was less than or equal to the value of `value`.
///
/// * `source`: Data series.
/// * `value`: Value.
pub fn crossover(source: &Source, value: f32) -> bool {
    source > value && source[1] <= value
}

/// Returns true if on the current candlestick (`k` line), the value of `source` is greater than the value of `value`,
/// and on the previous candlestick, the value of `source` was less than or equal to the value of `value`.
///
/// * `source`: Data series.
/// * `value`: Value.
/// * `f`: Mapping function.
pub fn crossover_map<F>(source: &Source, value: f32, mut f: F) -> bool
where
    F: FnMut(&Source) -> f32,
{
    f(source) > value && f(&source[1..]) <= value
}

/// Returns true if on the current candlestick (`k` line), the value of `source` is less than the value of `value`,
/// and on the previous candlestick, the value of `source` was greater than or equal to the value of `value`.
///
/// * `source`: Data series.
/// * `value`: Value.
/// * `f`: Mapping function.
pub fn crossunder(source: &Source, value: f32) -> bool {
    source < value && source[1] >= value
}

/// Returns true if on the current candlestick (`k` line), the value of `source` is less than the value of `value`,
/// and on the previous candlestick, the value of `source` was greater than or equal to the value of `value`.
///
/// * `source`: Data series.
/// * `value`: Value.
/// * `f`: Mapping function.
pub fn crossunder_map<F>(source: &Source, value: f32, mut f: F) -> bool
where
    F: FnMut(&Source) -> f32,
{
    f(source) < value && f(&source[1..]) >= value
}

/// Convert a timestamp to local time text.
///
/// * `value`: Timestamp.
/// * `return`: Local time text.
fn utc_datetime_from_millis(value: u64) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(value as i64)
        .unwrap_or_else(|| panic!("invalid timestamp millis: {value}"))
}

fn naive_datetime_from_millis(value: u64) -> chrono::NaiveDateTime {
    utc_datetime_from_millis(value).naive_utc()
}

fn naive_datetime_to_millis(value: chrono::NaiveDateTime) -> u64 {
    value.and_utc().timestamp_millis() as u64
}

pub fn time_to_string(value: u64) -> String {
    utc_datetime_from_millis(value)
        .with_timezone(&chrono::Local)
        .format("%Y-%m-%d %H:%M:%S")
        .to_string()
}

/// Convert local time text to a timestamp.
///
/// * `value`: Local time text.
/// * `return`: Timestamp.
pub fn string_to_time<S>(value: S) -> u64
where
    S: AsRef<str>,
{
    let local = chrono::NaiveDateTime::parse_from_str(value.as_ref(), "%Y-%m-%d %H:%M:%S")
        .unwrap_or_else(|error| panic!("invalid local timestamp '{}': {error}", value.as_ref()));

    chrono::Local
        .from_local_datetime(&local)
        .single()
        .unwrap_or_else(|| panic!("ambiguous local timestamp '{}'", value.as_ref()))
        .timestamp_millis() as u64
}

/// Retrieve candlestick data for a specified range.
///
/// * `product`: Trading product, for example, spot BTC-USDT, futures contract BTC-USDT-SWAP.
/// * `level`: Time level.
/// * `range`: Time range, where 0 indicates fetching all data, and a..b specifies data within the timestamp range from a to b,
/// * `return`: Candlestick array, with newer data at the front.
pub async fn get_k_range<E, S, T>(
    exchange: &E,
    product: S,
    level: Level,
    range: T,
) -> anyhow::Result<Vec<K>>
where
    E: Exchange,
    S: AsRef<str>,
    T: Into<TimeRange>,
{
    let product = product.as_ref();

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
        end = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
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

/// Retrieve candlestick data for a specified range.
///
/// * `product`: Trading product, for example, spot BTC-USDT, futures contract BTC-USDT-SWAP.
/// * `level`: Time level.
/// * `range`: Time range, where 0 indicates fetching all data, and a..b specifies data within the timestamp range from a to b,
/// * `millis`: Milliseconds of delay.
/// * `return`: Candlestick array, with newer data at the front.
pub async fn get_k_range_sleep<E, S, T>(
    exchange: &E,
    product: S,
    level: Level,
    range: T,
    millis: u64,
) -> anyhow::Result<Vec<K>>
where
    E: Exchange,
    S: AsRef<str>,
    T: Into<TimeRange>,
{
    let product = product.as_ref();

    let range = range.into();

    let mut result = Vec::new();

    if range.start == 0 && range.end == 0 {
        let mut time = 0;

        loop {
            let v = exchange.get_k(product, level, time).await?;

            if let Some(k) = v.last() {
                time = k.time;
                result.extend(v);
                tokio::time::sleep(std::time::Duration::from_millis(millis)).await;
            } else {
                break;
            }
        }

        return Ok(result);
    }

    let mut end = range.end;

    if end == u64::MAX - 1 {
        end = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
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
            tokio::time::sleep(std::time::Duration::from_millis(millis)).await;
        } else {
            break;
        }
    }

    Ok(result)
}

/// Convert a candlestick timestamp to another time level.
///
/// * `time`: Candlestick timestamp.
/// * `level`: The time level to convert to.
/// * `return`: The timestamp for the current candlestick, and the timestamp for the next candlestick.
pub fn k_time_convert(time: u64, level: Level) -> (u64, u64) {
    match level {
        Level::Minute1 => (time, time + 1000 * 60),
        Level::Minute3 => {
            let start = naive_datetime_from_millis(time);
            let start = start
                .date()
                .and_hms_opt(
                    chrono::Timelike::hour(&start),
                    chrono::Timelike::minute(&start) / 3 * 3,
                    0,
                )
                .unwrap();
            let end = start + chrono::Duration::minutes(3);
            (
                naive_datetime_to_millis(start),
                naive_datetime_to_millis(end),
            )
        }
        Level::Minute5 => {
            let start = naive_datetime_from_millis(time);
            let start = start
                .date()
                .and_hms_opt(
                    chrono::Timelike::hour(&start),
                    chrono::Timelike::minute(&start) / 5 * 5,
                    0,
                )
                .unwrap();
            let end = start + chrono::Duration::minutes(5);
            (
                naive_datetime_to_millis(start),
                naive_datetime_to_millis(end),
            )
        }
        Level::Minute15 => {
            let start = naive_datetime_from_millis(time);
            let start = start
                .date()
                .and_hms_opt(
                    chrono::Timelike::hour(&start),
                    chrono::Timelike::minute(&start) / 15 * 15,
                    0,
                )
                .unwrap();
            let end = start + chrono::Duration::minutes(15);
            (
                naive_datetime_to_millis(start),
                naive_datetime_to_millis(end),
            )
        }
        Level::Minute30 => {
            let start = naive_datetime_from_millis(time);
            let start = start
                .date()
                .and_hms_opt(
                    chrono::Timelike::hour(&start),
                    chrono::Timelike::minute(&start) / 30 * 30,
                    0,
                )
                .unwrap();
            let end = start + chrono::Duration::minutes(30);
            (
                naive_datetime_to_millis(start),
                naive_datetime_to_millis(end),
            )
        }
        Level::Hour1 => {
            let start = naive_datetime_from_millis(time);
            let start = start
                .date()
                .and_hms_opt(chrono::Timelike::hour(&start), 0, 0)
                .unwrap();
            let end = start + chrono::Duration::hours(1);
            (
                naive_datetime_to_millis(start),
                naive_datetime_to_millis(end),
            )
        }
        Level::Hour2 => {
            let start = naive_datetime_from_millis(time);
            let start = start
                .date()
                .and_hms_opt(chrono::Timelike::hour(&start) / 2 * 2, 0, 0)
                .unwrap();
            let end = start + chrono::Duration::hours(2);
            (
                naive_datetime_to_millis(start),
                naive_datetime_to_millis(end),
            )
        }
        Level::Hour4 => {
            let start = naive_datetime_from_millis(time);
            let start = start
                .date()
                .and_hms_opt(chrono::Timelike::hour(&start) / 4 * 4, 0, 0)
                .unwrap();
            let end = start + chrono::Duration::hours(4);
            (
                naive_datetime_to_millis(start),
                naive_datetime_to_millis(end),
            )
        }
        Level::Hour6 => {
            let start = naive_datetime_from_millis(time);
            let start = start
                .date()
                .and_hms_opt(chrono::Timelike::hour(&start) / 6 * 6, 0, 0)
                .unwrap();
            let end = start + chrono::Duration::hours(6);
            (
                naive_datetime_to_millis(start),
                naive_datetime_to_millis(end),
            )
        }
        Level::Hour12 => {
            let start = naive_datetime_from_millis(time);
            let start = start
                .date()
                .and_hms_opt(chrono::Timelike::hour(&start) / 12 * 12, 0, 0)
                .unwrap();
            let end = start + chrono::Duration::hours(12);
            (
                naive_datetime_to_millis(start),
                naive_datetime_to_millis(end),
            )
        }
        Level::Day1 => {
            let start = naive_datetime_from_millis(time)
                .date()
                .and_hms_opt(0, 0, 0)
                .unwrap();
            let end = start + chrono::Days::new(1);
            (
                naive_datetime_to_millis(start),
                naive_datetime_to_millis(end),
            )
        }
        Level::Day3 => {
            let start = naive_datetime_from_millis(time);
            let start = chrono::Datelike::with_day(
                &start.date(),
                chrono::Datelike::day(&start.date()) / 3 * 3 + 1,
            )
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
            let end = start + chrono::Days::new(3);
            (
                naive_datetime_to_millis(start),
                naive_datetime_to_millis(end),
            )
        }
        Level::Week1 => {
            let start = naive_datetime_from_millis(time)
                .date()
                .week(chrono::Weekday::Mon)
                .first_day()
                .and_hms_opt(0, 0, 0)
                .unwrap();
            let end = start + chrono::Duration::weeks(1);
            (
                naive_datetime_to_millis(start),
                naive_datetime_to_millis(end),
            )
        }
        Level::Month1 => {
            let start = chrono::Datelike::with_day(&naive_datetime_from_millis(time).date(), 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap();
            let end = start + chrono::Months::new(1);
            (
                naive_datetime_to_millis(start),
                naive_datetime_to_millis(end),
            )
        }
    }
}

/// Convert a candlestick array to another time level.
///
/// * `array`: Candlestick array, with newer data at the front.
/// * `level`: The time level to convert to, which must be greater than or equal to the candlestick's time level.
/// * `return`: Candlestick array, with newer data at the front.
pub fn k_convert<T>(array: T, level: Level) -> Vec<K>
where
    T: AsRef<[K]>,
{
    let array = array.as_ref();

    let mut result = Vec::new();

    if array.is_empty() {
        return result;
    }

    let mut i = array.len() - 1;

    loop {
        let k = array[i];

        let (start, next_start) = k_time_convert(k.time, level);

        let start_k = array[i..]
            .iter()
            .position(|v| v.time <= start)
            .map(|v| (i + v, array[i + v]))
            .unwrap_or((array.len() - 1, *array.last().unwrap()));

        let next_start_k = array[..=i]
            .iter()
            .rev()
            .enumerate()
            .find(|v| v.1.time >= next_start)
            .map(|v| (i - v.0, array[i - v.0]))
            .unwrap_or((0, *array.first().unwrap()));

        let end_k = next_start_k
            .0
            .checked_add(1)
            .and_then(|v| array.get(v).map(|k| (v, k)))
            .unwrap_or((next_start_k.0, &array[next_start_k.0]));

        let mut k = K {
            time: start_k.1.time,
            open: start_k.1.open,
            high: 0.0,
            low: f32::MAX,
            close: end_k.1.close,
        };

        array[end_k.0..=start_k.0].iter().for_each(|v| {
            k.high = k.high.max(v.high);
            k.low = k.low.min(v.low)
        });

        result.push(k);

        i = next_start_k.0;

        if start_k.0 == next_start_k.0 || i == 0 {
            break;
        }
    }

    result.reverse();

    result
}

/// Quickly calculate EMA.
pub struct EMACache {
    last: f32,
}

impl EMACache {
    pub fn new() -> Self {
        Self { last: f32::NAN }
    }

    /// Calculate EMA.
    ///
    /// * `source`: Data series, the current `source[1..]` must be equal to the previous `source`.
    /// * `length`: Length, the current length must be equal to the previous length.
    /// * `return`: EMA.
    pub fn ema(&mut self, source: &Source, length: usize) -> f32 {
        self.last = if self.last.is_nan() {
            ema(source, length)
        } else {
            if source.len() < length {
                return f32::NAN;
            }

            let alpha = 2.0f32 / (length + 1) as f32;
            alpha * source + (1.0f32 - alpha) * self.last
        };
        self.last
    }
}

/// Quickly calculate RMA.
pub struct RMACache {
    last: f32,
}

impl RMACache {
    pub fn new() -> Self {
        Self { last: f32::NAN }
    }

    /// Calculate RMA.
    ///
    /// * `source`: Data series, the current `source[1..]` must be equal to the previous `source`.
    /// * `length`: Length, the current length must be equal to the previous length.
    /// * `return`: RMA.
    pub fn rma(&mut self, source: &Source, length: usize) -> f32 {
        self.last = if self.last.is_nan() {
            rma(source, length)
        } else {
            if source.len() < length {
                return f32::NAN;
            }

            let alpha = 1.0f32 / length as f32;
            alpha * source + (1.0f32 - alpha) * self.last
        };
        self.last
    }
}

/// Quickly calculate MACD.
pub struct MACDCache {
    short_ema: EMACache,
    long_ema: EMACache,
    dea_ema: EMACache,
    dea: std::collections::VecDeque<f32>,
}

impl MACDCache {
    pub fn new() -> Self {
        Self {
            short_ema: EMACache::new(),
            long_ema: EMACache::new(),
            dea_ema: EMACache::new(),
            dea: std::collections::VecDeque::new(),
        }
    }
    /// Calculate MACD.
    ///
    /// * `source`: Data series, the current `source[1..]` must be equal to the previous `source`.
    /// * `short_length`: Length of the fast line, current length must be equal to the previous length.
    /// * `long_length`: Length of the slow line, current length must be equal to the previous length.
    /// * `dea_length`: Length of the DEA line, current length must be equal to the previous length.
    /// * `return`: MACD.
    pub fn macd(
        &mut self,
        source: &Source,
        short_length: usize,
        long_length: usize,
        dea_length: usize,
    ) -> (f32, f32, f32) {
        if source.len() < short_length || source.len() < long_length || source.len() < dea_length {
            return (f32::NAN, f32::NAN, f32::NAN);
        }

        let dif = self.short_ema.ema(source, short_length) - self.long_ema.ema(source, long_length);
        self.dea.push_front(dif);
        let dea = self
            .dea_ema
            .ema(Source::new(self.dea.as_slices().0), dea_length);
        let macd = (dif - dea) * 2.0f32;
        (dif, dea, macd)
    }
}

/// Quickly calculate RSI.
pub struct RSICache {
    u: std::collections::VecDeque<f32>,
    d: std::collections::VecDeque<f32>,
    u_rma: RMACache,
    d_rma: RMACache,
}

impl RSICache {
    pub fn new() -> Self {
        Self {
            u: std::collections::VecDeque::new(),
            d: std::collections::VecDeque::new(),
            u_rma: RMACache::new(),
            d_rma: RMACache::new(),
        }
    }

    /// Calculate the Relative Strength Index (RSI).
    ///
    /// * `source`: Data series, the current `source[1..]` must be equal to the previous `source`.
    /// * `length`: Length, the current length must be equal to the previous length.
    /// * `return`: RSI.
    pub fn rsi(&mut self, source: &Source, length: usize) -> f32 {
        if source.len() < length {
            return f32::NAN;
        }

        self.u.push_front({
            let temp = source - source[1];
            let temp = if temp.is_nan() { 0.0 } else { temp };
            temp.max(0.0)
        });

        self.d.push_front({
            let temp = source[1] - source;
            let temp = if temp.is_nan() { 0.0 } else { temp };
            temp.max(0.0)
        });

        let rs = self.u_rma.rma(Source::new(self.u.as_slices().0), length)
            / self.d_rma.rma(Source::new(self.d.as_slices().0), length);

        100.0f32 - 100.0f32 / (1.0f32 + rs)
    }
}

/// Trading product mapping.
/// BTC-USDT <-> BTCUSDT.
/// BTC-USDT-SWAP <-> BTCUSDTSWAP.
///
/// * `value`: Trading product, for example, spot BTC-USDT, contract BTC-USDT-SWAP.
/// * `return`: Mapped value.
pub fn product_mapping<S>(value: S) -> std::borrow::Cow<'static, str>
where
    S: AsRef<str>,
{
    let value = value.as_ref();
    let result = match value {
        "MDT-USDT" => "MDTUSDT",
        "MDT-USDT-SWAP" => "MDTUSDTSWAP",
        "KNC-USDT" => "KNCUSDT",
        "KNC-USDT-SWAP" => "KNCUSDTSWAP",
        "CVX-USDT" => "CVXUSDT",
        "CVX-USDT-SWAP" => "CVXUSDTSWAP",
        "AGLD-USDT" => "AGLDUSDT",
        "AGLD-USDT-SWAP" => "AGLDUSDTSWAP",
        "IOTA-USDT" => "IOTAUSDT",
        "IOTA-USDT-SWAP" => "IOTAUSDTSWAP",
        "QTUM-USDT" => "QTUMUSDT",
        "QTUM-USDT-SWAP" => "QTUMUSDTSWAP",
        "AXS-USDT" => "AXSUSDT",
        "AXS-USDT-SWAP" => "AXSUSDTSWAP",
        "LQTY-USDT" => "LQTYUSDT",
        "LQTY-USDT-SWAP" => "LQTYUSDTSWAP",
        "ETC-USDT" => "ETCUSDT",
        "ETC-USDT-SWAP" => "ETCUSDTSWAP",
        "ASTR-USDT" => "ASTRUSDT",
        "ASTR-USDT-SWAP" => "ASTRUSDTSWAP",
        "BCH-USDT" => "BCHUSDT",
        "BCH-USDT-SWAP" => "BCHUSDTSWAP",
        "APT-USDT" => "APTUSDT",
        "APT-USDT-SWAP" => "APTUSDTSWAP",
        "TRX-USDT" => "TRXUSDT",
        "TRX-USDT-SWAP" => "TRXUSDTSWAP",
        "CELR-USDT" => "CELRUSDT",
        "CELR-USDT-SWAP" => "CELRUSDTSWAP",
        "CELO-USDT" => "CELOUSDT",
        "CELO-USDT-SWAP" => "CELOUSDTSWAP",
        "SAND-USDT" => "SANDUSDT",
        "SAND-USDT-SWAP" => "SANDUSDTSWAP",
        "KLAY-USDT" => "KLAYUSDT",
        "KLAY-USDT-SWAP" => "KLAYUSDTSWAP",
        "T-USDT" => "TUSDT",
        "T-USDT-SWAP" => "TUSDTSWAP",
        "FLM-USDT" => "FLMUSDT",
        "FLM-USDT-SWAP" => "FLMUSDTSWAP",
        "RAY-USDT" => "RAYUSDT",
        "RAY-USDT-SWAP" => "RAYUSDTSWAP",
        "SOL-USDT" => "SOLUSDT",
        "SOL-USDT-SWAP" => "SOLUSDTSWAP",
        "API3-USDT" => "API3USDT",
        "API3-USDT-SWAP" => "API3USDTSWAP",
        "YFI-USDT" => "YFIUSDT",
        "YFI-USDT-SWAP" => "YFIUSDTSWAP",
        "LDO-USDT" => "LDOUSDT",
        "LDO-USDT-SWAP" => "LDOUSDTSWAP",
        "NMR-USDT" => "NMRUSDT",
        "NMR-USDT-SWAP" => "NMRUSDTSWAP",
        "AAVE-USDT" => "AAVEUSDT",
        "AAVE-USDT-SWAP" => "AAVEUSDTSWAP",
        "TRB-USDT" => "TRBUSDT",
        "TRB-USDT-SWAP" => "TRBUSDTSWAP",
        "MATIC-USDT" => "MATICUSDT",
        "MATIC-USDT-SWAP" => "MATICUSDTSWAP",
        "DOGE-USDT" => "DOGEUSDT",
        "DOGE-USDT-SWAP" => "DOGEUSDTSWAP",
        "XRP-USDT" => "XRPUSDT",
        "XRP-USDT-SWAP" => "XRPUSDTSWAP",
        "ENS-USDT" => "ENSUSDT",
        "ENS-USDT-SWAP" => "ENSUSDTSWAP",
        "LPT-USDT" => "LPTUSDT",
        "LPT-USDT-SWAP" => "LPTUSDTSWAP",
        "BNB-USDT" => "BNBUSDT",
        "BNB-USDT-SWAP" => "BNBUSDTSWAP",
        "SPELL-USDT" => "SPELLUSDT",
        "SPELL-USDT-SWAP" => "SPELLUSDTSWAP",
        "CRV-USDT" => "CRVUSDT",
        "CRV-USDT-SWAP" => "CRVUSDTSWAP",
        "ARB-USDT" => "ARBUSDT",
        "ARB-USDT-SWAP" => "ARBUSDTSWAP",
        "EGLD-USDT" => "EGLDUSDT",
        "EGLD-USDT-SWAP" => "EGLDUSDTSWAP",
        "NEO-USDT" => "NEOUSDT",
        "NEO-USDT-SWAP" => "NEOUSDTSWAP",
        "CHZ-USDT" => "CHZUSDT",
        "CHZ-USDT-SWAP" => "CHZUSDTSWAP",
        "USDC-USDT" => "USDCUSDT",
        "USDC-USDT-SWAP" => "USDCUSDTSWAP",
        "DOT-USDT" => "DOTUSDT",
        "DOT-USDT-SWAP" => "DOTUSDTSWAP",
        "MINA-USDT" => "MINAUSDT",
        "MINA-USDT-SWAP" => "MINAUSDTSWAP",
        "EOS-USDT" => "EOSUSDT",
        "EOS-USDT-SWAP" => "EOSUSDTSWAP",
        "GMX-USDT" => "GMXUSDT",
        "GMX-USDT-SWAP" => "GMXUSDTSWAP",
        "GRT-USDT" => "GRTUSDT",
        "GRT-USDT-SWAP" => "GRTUSDTSWAP",
        "SKL-USDT" => "SKLUSDT",
        "SKL-USDT-SWAP" => "SKLUSDTSWAP",
        "IOST-USDT" => "IOSTUSDT",
        "IOST-USDT-SWAP" => "IOSTUSDTSWAP",
        "RDNT-USDT" => "RDNTUSDT",
        "RDNT-USDT-SWAP" => "RDNTUSDTSWAP",
        "BTC-USDT" => "BTCUSDT",
        "BTC-USDT-SWAP" => "BTCUSDTSWAP",
        "WOO-USDT" => "WOOUSDT",
        "WOO-USDT-SWAP" => "WOOUSDTSWAP",
        "ACH-USDT" => "ACHUSDT",
        "ACH-USDT-SWAP" => "ACHUSDTSWAP",
        "APE-USDT" => "APEUSDT",
        "APE-USDT-SWAP" => "APEUSDTSWAP",
        "ID-USDT" => "IDUSDT",
        "ID-USDT-SWAP" => "IDUSDTSWAP",
        "ADA-USDT" => "ADAUSDT",
        "ADA-USDT-SWAP" => "ADAUSDTSWAP",
        "HNT-USDT" => "HNTUSDT",
        "HNT-USDT-SWAP" => "HNTUSDTSWAP",
        "ALPHA-USDT" => "ALPHAUSDT",
        "ALPHA-USDT-SWAP" => "ALPHAUSDTSWAP",
        "CFX-USDT" => "CFXUSDT",
        "CFX-USDT-SWAP" => "CFXUSDTSWAP",
        "SRM-USDT" => "SRMUSDT",
        "SRM-USDT-SWAP" => "SRMUSDTSWAP",
        "UNI-USDT" => "UNIUSDT",
        "UNI-USDT-SWAP" => "UNIUSDTSWAP",
        "THETA-USDT" => "THETAUSDT",
        "THETA-USDT-SWAP" => "THETAUSDTSWAP",
        "HBAR-USDT" => "HBARUSDT",
        "HBAR-USDT-SWAP" => "HBARUSDTSWAP",
        "ZEC-USDT" => "ZECUSDT",
        "ZEC-USDT-SWAP" => "ZECUSDTSWAP",
        "SUSHI-USDT" => "SUSHIUSDT",
        "SUSHI-USDT-SWAP" => "SUSHIUSDTSWAP",
        "LTC-USDT" => "LTCUSDT",
        "LTC-USDT-SWAP" => "LTCUSDTSWAP",
        "ICX-USDT" => "ICXUSDT",
        "ICX-USDT-SWAP" => "ICXUSDTSWAP",
        "LINK-USDT" => "LINKUSDT",
        "LINK-USDT-SWAP" => "LINKUSDTSWAP",
        "XTZ-USDT" => "XTZUSDT",
        "XTZ-USDT-SWAP" => "XTZUSDTSWAP",
        "RVN-USDT" => "RVNUSDT",
        "RVN-USDT-SWAP" => "RVNUSDTSWAP",
        "WLD-USDT" => "WLDUSDT",
        "WLD-USDT-SWAP" => "WLDUSDTSWAP",
        "OP-USDT" => "OPUSDT",
        "OP-USDT-SWAP" => "OPUSDTSWAP",
        "REN-USDT" => "RENUSDT",
        "REN-USDT-SWAP" => "RENUSDTSWAP",
        "BLUR-USDT" => "BLURUSDT",
        "BLUR-USDT-SWAP" => "BLURUSDTSWAP",
        "SUI-USDT" => "SUIUSDT",
        "SUI-USDT-SWAP" => "SUIUSDTSWAP",
        "ICP-USDT" => "ICPUSDT",
        "ICP-USDT-SWAP" => "ICPUSDTSWAP",
        "XMR-USDT" => "XMRUSDT",
        "XMR-USDT-SWAP" => "XMRUSDTSWAP",
        "ZEN-USDT" => "ZENUSDT",
        "ZEN-USDT-SWAP" => "ZENUSDTSWAP",
        "FTM-USDT" => "FTMUSDT",
        "FTM-USDT-SWAP" => "FTMUSDTSWAP",
        "MAGIC-USDT" => "MAGICUSDT",
        "MAGIC-USDT-SWAP" => "MAGICUSDTSWAP",
        "DGB-USDT" => "DGBUSDT",
        "DGB-USDT-SWAP" => "DGBUSDTSWAP",
        "LRC-USDT" => "LRCUSDT",
        "LRC-USDT-SWAP" => "LRCUSDTSWAP",
        "DYDX-USDT" => "DYDXUSDT",
        "DYDX-USDT-SWAP" => "DYDXUSDTSWAP",
        "ZRX-USDT" => "ZRXUSDT",
        "ZRX-USDT-SWAP" => "ZRXUSDTSWAP",
        "SC-USDT" => "SCUSDT",
        "SC-USDT-SWAP" => "SCUSDTSWAP",
        "FIL-USDT" => "FILUSDT",
        "FIL-USDT-SWAP" => "FILUSDTSWAP",
        "RSR-USDT" => "RSRUSDT",
        "RSR-USDT-SWAP" => "RSRUSDTSWAP",
        "ETH-BTC" => "ETHBTC",
        "ETH-BTC-SWAP" => "ETHBTCSWAP",
        "ONT-USDT" => "ONTUSDT",
        "ONT-USDT-SWAP" => "ONTUSDTSWAP",
        "FXS-USDT" => "FXSUSDT",
        "FXS-USDT-SWAP" => "FXSUSDTSWAP",
        "UMA-USDT" => "UMAUSDT",
        "UMA-USDT-SWAP" => "UMAUSDTSWAP",
        "AR-USDT" => "ARUSDT",
        "AR-USDT-SWAP" => "ARUSDTSWAP",
        "BAND-USDT" => "BANDUSDT",
        "BAND-USDT-SWAP" => "BANDUSDTSWAP",
        "XLM-USDT" => "XLMUSDT",
        "XLM-USDT-SWAP" => "XLMUSDTSWAP",
        "SNX-USDT" => "SNXUSDT",
        "SNX-USDT-SWAP" => "SNXUSDTSWAP",
        "ATOM-USDT" => "ATOMUSDT",
        "ATOM-USDT-SWAP" => "ATOMUSDTSWAP",
        "BAT-USDT" => "BATUSDT",
        "BAT-USDT-SWAP" => "BATUSDTSWAP",
        "MANA-USDT" => "MANAUSDT",
        "MANA-USDT-SWAP" => "MANAUSDTSWAP",
        "CVC-USDT" => "CVCUSDT",
        "CVC-USDT-SWAP" => "CVCUSDTSWAP",
        "XEM-USDT" => "XEMUSDT",
        "XEM-USDT-SWAP" => "XEMUSDTSWAP",
        "SSV-USDT" => "SSVUSDT",
        "SSV-USDT-SWAP" => "SSVUSDTSWAP",
        "KSM-USDT" => "KSMUSDT",
        "KSM-USDT-SWAP" => "KSMUSDTSWAP",
        "JOE-USDT" => "JOEUSDT",
        "JOE-USDT-SWAP" => "JOEUSDTSWAP",
        "ETH-USDT" => "ETHUSDT",
        "ETH-USDT-SWAP" => "ETHUSDTSWAP",
        "STORJ-USDT" => "STORJUSDT",
        "STORJ-USDT-SWAP" => "STORJUSDTSWAP",
        "GMT-USDT" => "GMTUSDT",
        "GMT-USDT-SWAP" => "GMTUSDTSWAP",
        "OMG-USDT" => "OMGUSDT",
        "OMG-USDT-SWAP" => "OMGUSDTSWAP",
        "PEOPLE-USDT" => "PEOPLEUSDT",
        "PEOPLE-USDT-SWAP" => "PEOPLEUSDTSWAP",
        "BAL-USDT" => "BALUSDT",
        "BAL-USDT-SWAP" => "BALUSDTSWAP",
        "ZIL-USDT" => "ZILUSDT",
        "ZIL-USDT-SWAP" => "ZILUSDTSWAP",
        "FLOW-USDT" => "FLOWUSDT",
        "FLOW-USDT-SWAP" => "FLOWUSDTSWAP",
        "IMX-USDT" => "IMXUSDT",
        "IMX-USDT-SWAP" => "IMXUSDTSWAP",
        "COMP-USDT" => "COMPUSDT",
        "COMP-USDT-SWAP" => "COMPUSDTSWAP",
        "ALGO-USDT" => "ALGOUSDT",
        "ALGO-USDT-SWAP" => "ALGOUSDTSWAP",
        "WAVES-USDT" => "WAVESUSDT",
        "WAVES-USDT-SWAP" => "WAVESUSDTSWAP",
        "DASH-USDT" => "DASHUSDT",
        "DASH-USDT-SWAP" => "DASHUSDTSWAP",
        "ENJ-USDT" => "ENJUSDT",
        "ENJ-USDT-SWAP" => "ENJUSDTSWAP",
        "1INCH-USDT" => "1INCHUSDT",
        "1INCH-USDT-SWAP" => "1INCHUSDTSWAP",
        "PERP-USDT" => "PERPUSDT",
        "PERP-USDT-SWAP" => "PERPUSDTSWAP",
        "NEAR-USDT" => "NEARUSDT",
        "NEAR-USDT-SWAP" => "NEARUSDTSWAP",
        "ANT-USDT" => "ANTUSDT",
        "ANT-USDT-SWAP" => "ANTUSDTSWAP",
        "GAL-USDT" => "GALUSDT",
        "GAL-USDT-SWAP" => "GALUSDTSWAP",
        "ONE-USDT" => "ONEUSDT",
        "ONE-USDT-SWAP" => "ONEUSDTSWAP",
        "MKR-USDT" => "MKRUSDT",
        "MKR-USDT-SWAP" => "MKRUSDTSWAP",
        "GALA-USDT" => "GALAUSDT",
        "GALA-USDT-SWAP" => "GALAUSDTSWAP",
        "AVAX-USDT" => "AVAXUSDT",
        "AVAX-USDT-SWAP" => "AVAXUSDTSWAP",
        "MASK-USDT" => "MASKUSDT",
        "MASK-USDT-SWAP" => "MASKUSDTSWAP",
        "STX-USDT" => "STXUSDT",
        "STX-USDT-SWAP" => "STXUSDTSWAP",
        // ================================
        "MDTUSDT" => "MDT-USDT",
        "MDTUSDTSWAP" => "MDT-USDT-SWAP",
        "KNCUSDT" => "KNC-USDT",
        "KNCUSDTSWAP" => "KNC-USDT-SWAP",
        "CVXUSDT" => "CVX-USDT",
        "CVXUSDTSWAP" => "CVX-USDT-SWAP",
        "AGLDUSDT" => "AGLD-USDT",
        "AGLDUSDTSWAP" => "AGLD-USDT-SWAP",
        "IOTAUSDT" => "IOTA-USDT",
        "IOTAUSDTSWAP" => "IOTA-USDT-SWAP",
        "QTUMUSDT" => "QTUM-USDT",
        "QTUMUSDTSWAP" => "QTUM-USDT-SWAP",
        "AXSUSDT" => "AXS-USDT",
        "AXSUSDTSWAP" => "AXS-USDT-SWAP",
        "LQTYUSDT" => "LQTY-USDT",
        "LQTYUSDTSWAP" => "LQTY-USDT-SWAP",
        "ETCUSDT" => "ETC-USDT",
        "ETCUSDTSWAP" => "ETC-USDT-SWAP",
        "ASTRUSDT" => "ASTR-USDT",
        "ASTRUSDTSWAP" => "ASTR-USDT-SWAP",
        "BCHUSDT" => "BCH-USDT",
        "BCHUSDTSWAP" => "BCH-USDT-SWAP",
        "APTUSDT" => "APT-USDT",
        "APTUSDTSWAP" => "APT-USDT-SWAP",
        "TRXUSDT" => "TRX-USDT",
        "TRXUSDTSWAP" => "TRX-USDT-SWAP",
        "CELRUSDT" => "CELR-USDT",
        "CELRUSDTSWAP" => "CELR-USDT-SWAP",
        "CELOUSDT" => "CELO-USDT",
        "CELOUSDTSWAP" => "CELO-USDT-SWAP",
        "SANDUSDT" => "SAND-USDT",
        "SANDUSDTSWAP" => "SAND-USDT-SWAP",
        "KLAYUSDT" => "KLAY-USDT",
        "KLAYUSDTSWAP" => "KLAY-USDT-SWAP",
        "TUSDT" => "T-USDT",
        "TUSDTSWAP" => "T-USDT-SWAP",
        "FLMUSDT" => "FLM-USDT",
        "FLMUSDTSWAP" => "FLM-USDT-SWAP",
        "RAYUSDT" => "RAY-USDT",
        "RAYUSDTSWAP" => "RAY-USDT-SWAP",
        "SOLUSDT" => "SOL-USDT",
        "SOLUSDTSWAP" => "SOL-USDT-SWAP",
        "API3USDT" => "API3-USDT",
        "API3USDTSWAP" => "API3-USDT-SWAP",
        "YFIUSDT" => "YFI-USDT",
        "YFIUSDTSWAP" => "YFI-USDT-SWAP",
        "LDOUSDT" => "LDO-USDT",
        "LDOUSDTSWAP" => "LDO-USDT-SWAP",
        "NMRUSDT" => "NMR-USDT",
        "NMRUSDTSWAP" => "NMR-USDT-SWAP",
        "AAVEUSDT" => "AAVE-USDT",
        "AAVEUSDTSWAP" => "AAVE-USDT-SWAP",
        "TRBUSDT" => "TRB-USDT",
        "TRBUSDTSWAP" => "TRB-USDT-SWAP",
        "MATICUSDT" => "MATIC-USDT",
        "MATICUSDTSWAP" => "MATIC-USDT-SWAP",
        "DOGEUSDT" => "DOGE-USDT",
        "DOGEUSDTSWAP" => "DOGE-USDT-SWAP",
        "XRPUSDT" => "XRP-USDT",
        "XRPUSDTSWAP" => "XRP-USDT-SWAP",
        "ENSUSDT" => "ENS-USDT",
        "ENSUSDTSWAP" => "ENS-USDT-SWAP",
        "LPTUSDT" => "LPT-USDT",
        "LPTUSDTSWAP" => "LPT-USDT-SWAP",
        "BNBUSDT" => "BNB-USDT",
        "BNBUSDTSWAP" => "BNB-USDT-SWAP",
        "SPELLUSDT" => "SPELL-USDT",
        "SPELLUSDTSWAP" => "SPELL-USDT-SWAP",
        "CRVUSDT" => "CRV-USDT",
        "CRVUSDTSWAP" => "CRV-USDT-SWAP",
        "ARBUSDT" => "ARB-USDT",
        "ARBUSDTSWAP" => "ARB-USDT-SWAP",
        "EGLDUSDT" => "EGLD-USDT",
        "EGLDUSDTSWAP" => "EGLD-USDT-SWAP",
        "NEOUSDT" => "NEO-USDT",
        "NEOUSDTSWAP" => "NEO-USDT-SWAP",
        "CHZUSDT" => "CHZ-USDT",
        "CHZUSDTSWAP" => "CHZ-USDT-SWAP",
        "USDCUSDT" => "USDC-USDT",
        "USDCUSDTSWAP" => "USDC-USDT-SWAP",
        "DOTUSDT" => "DOT-USDT",
        "DOTUSDTSWAP" => "DOT-USDT-SWAP",
        "MINAUSDT" => "MINA-USDT",
        "MINAUSDTSWAP" => "MINA-USDT-SWAP",
        "EOSUSDT" => "EOS-USDT",
        "EOSUSDTSWAP" => "EOS-USDT-SWAP",
        "GMXUSDT" => "GMX-USDT",
        "GMXUSDTSWAP" => "GMX-USDT-SWAP",
        "GRTUSDT" => "GRT-USDT",
        "GRTUSDTSWAP" => "GRT-USDT-SWAP",
        "SKLUSDT" => "SKL-USDT",
        "SKLUSDTSWAP" => "SKL-USDT-SWAP",
        "IOSTUSDT" => "IOST-USDT",
        "IOSTUSDTSWAP" => "IOST-USDT-SWAP",
        "RDNTUSDT" => "RDNT-USDT",
        "RDNTUSDTSWAP" => "RDNT-USDT-SWAP",
        "BTCUSDT" => "BTC-USDT",
        "BTCUSDTSWAP" => "BTC-USDT-SWAP",
        "WOOUSDT" => "WOO-USDT",
        "WOOUSDTSWAP" => "WOO-USDT-SWAP",
        "ACHUSDT" => "ACH-USDT",
        "ACHUSDTSWAP" => "ACH-USDT-SWAP",
        "APEUSDT" => "APE-USDT",
        "APEUSDTSWAP" => "APE-USDT-SWAP",
        "IDUSDT" => "ID-USDT",
        "IDUSDTSWAP" => "ID-USDT-SWAP",
        "ADAUSDT" => "ADA-USDT",
        "ADAUSDTSWAP" => "ADA-USDT-SWAP",
        "HNTUSDT" => "HNT-USDT",
        "HNTUSDTSWAP" => "HNT-USDT-SWAP",
        "ALPHAUSDT" => "ALPHA-USDT",
        "ALPHAUSDTSWAP" => "ALPHA-USDT-SWAP",
        "CFXUSDT" => "CFX-USDT",
        "CFXUSDTSWAP" => "CFX-USDT-SWAP",
        "SRMUSDT" => "SRM-USDT",
        "SRMUSDTSWAP" => "SRM-USDT-SWAP",
        "UNIUSDT" => "UNI-USDT",
        "UNIUSDTSWAP" => "UNI-USDT-SWAP",
        "THETAUSDT" => "THETA-USDT",
        "THETAUSDTSWAP" => "THETA-USDT-SWAP",
        "HBARUSDT" => "HBAR-USDT",
        "HBARUSDTSWAP" => "HBAR-USDT-SWAP",
        "ZECUSDT" => "ZEC-USDT",
        "ZECUSDTSWAP" => "ZEC-USDT-SWAP",
        "SUSHIUSDT" => "SUSHI-USDT",
        "SUSHIUSDTSWAP" => "SUSHI-USDT-SWAP",
        "LTCUSDT" => "LTC-USDT",
        "LTCUSDTSWAP" => "LTC-USDT-SWAP",
        "ICXUSDT" => "ICX-USDT",
        "ICXUSDTSWAP" => "ICX-USDT-SWAP",
        "LINKUSDT" => "LINK-USDT",
        "LINKUSDTSWAP" => "LINK-USDT-SWAP",
        "XTZUSDT" => "XTZ-USDT",
        "XTZUSDTSWAP" => "XTZ-USDT-SWAP",
        "RVNUSDT" => "RVN-USDT",
        "RVNUSDTSWAP" => "RVN-USDT-SWAP",
        "WLDUSDT" => "WLD-USDT",
        "WLDUSDTSWAP" => "WLD-USDT-SWAP",
        "OPUSDT" => "OP-USDT",
        "OPUSDTSWAP" => "OP-USDT-SWAP",
        "RENUSDT" => "REN-USDT",
        "RENUSDTSWAP" => "REN-USDT-SWAP",
        "BLURUSDT" => "BLUR-USDT",
        "BLURUSDTSWAP" => "BLUR-USDT-SWAP",
        "SUIUSDT" => "SUI-USDT",
        "SUIUSDTSWAP" => "SUI-USDT-SWAP",
        "ICPUSDT" => "ICP-USDT",
        "ICPUSDTSWAP" => "ICP-USDT-SWAP",
        "XMRUSDT" => "XMR-USDT",
        "XMRUSDTSWAP" => "XMR-USDT-SWAP",
        "ZENUSDT" => "ZEN-USDT",
        "ZENUSDTSWAP" => "ZEN-USDT-SWAP",
        "FTMUSDT" => "FTM-USDT",
        "FTMUSDTSWAP" => "FTM-USDT-SWAP",
        "MAGICUSDT" => "MAGIC-USDT",
        "MAGICUSDTSWAP" => "MAGIC-USDT-SWAP",
        "DGBUSDT" => "DGB-USDT",
        "DGBUSDTSWAP" => "DGB-USDT-SWAP",
        "LRCUSDT" => "LRC-USDT",
        "LRCUSDTSWAP" => "LRC-USDT-SWAP",
        "DYDXUSDT" => "DYDX-USDT",
        "DYDXUSDTSWAP" => "DYDX-USDT-SWAP",
        "ZRXUSDT" => "ZRX-USDT",
        "ZRXUSDTSWAP" => "ZRX-USDT-SWAP",
        "SCUSDT" => "SC-USDT",
        "SCUSDTSWAP" => "SC-USDT-SWAP",
        "FILUSDT" => "FIL-USDT",
        "FILUSDTSWAP" => "FIL-USDT-SWAP",
        "RSRUSDT" => "RSR-USDT",
        "RSRUSDTSWAP" => "RSR-USDT-SWAP",
        "ETHBTC" => "ETH-BTC",
        "ETHBTCSWAP" => "ETH-BTC-SWAP",
        "ONTUSDT" => "ONT-USDT",
        "ONTUSDTSWAP" => "ONT-USDT-SWAP",
        "FXSUSDT" => "FXS-USDT",
        "FXSUSDTSWAP" => "FXS-USDT-SWAP",
        "UMAUSDT" => "UMA-USDT",
        "UMAUSDTSWAP" => "UMA-USDT-SWAP",
        "ARUSDT" => "AR-USDT",
        "ARUSDTSWAP" => "AR-USDT-SWAP",
        "BANDUSDT" => "BAND-USDT",
        "BANDUSDTSWAP" => "BAND-USDT-SWAP",
        "XLMUSDT" => "XLM-USDT",
        "XLMUSDTSWAP" => "XLM-USDT-SWAP",
        "SNXUSDT" => "SNX-USDT",
        "SNXUSDTSWAP" => "SNX-USDT-SWAP",
        "ATOMUSDT" => "ATOM-USDT",
        "ATOMUSDTSWAP" => "ATOM-USDT-SWAP",
        "BATUSDT" => "BAT-USDT",
        "BATUSDTSWAP" => "BAT-USDT-SWAP",
        "MANAUSDT" => "MANA-USDT",
        "MANAUSDTSWAP" => "MANA-USDT-SWAP",
        "CVCUSDT" => "CVC-USDT",
        "CVCUSDTSWAP" => "CVC-USDT-SWAP",
        "XEMUSDT" => "XEM-USDT",
        "XEMUSDTSWAP" => "XEM-USDT-SWAP",
        "SSVUSDT" => "SSV-USDT",
        "SSVUSDTSWAP" => "SSV-USDT-SWAP",
        "KSMUSDT" => "KSM-USDT",
        "KSMUSDTSWAP" => "KSM-USDT-SWAP",
        "JOEUSDT" => "JOE-USDT",
        "JOEUSDTSWAP" => "JOE-USDT-SWAP",
        "ETHUSDT" => "ETH-USDT",
        "ETHUSDTSWAP" => "ETH-USDT-SWAP",
        "STORJUSDT" => "STORJ-USDT",
        "STORJUSDTSWAP" => "STORJ-USDT-SWAP",
        "GMTUSDT" => "GMT-USDT",
        "GMTUSDTSWAP" => "GMT-USDT-SWAP",
        "OMGUSDT" => "OMG-USDT",
        "OMGUSDTSWAP" => "OMG-USDT-SWAP",
        "PEOPLEUSDT" => "PEOPLE-USDT",
        "PEOPLEUSDTSWAP" => "PEOPLE-USDT-SWAP",
        "BALUSDT" => "BAL-USDT",
        "BALUSDTSWAP" => "BAL-USDT-SWAP",
        "ZILUSDT" => "ZIL-USDT",
        "ZILUSDTSWAP" => "ZIL-USDT-SWAP",
        "FLOWUSDT" => "FLOW-USDT",
        "FLOWUSDTSWAP" => "FLOW-USDT-SWAP",
        "IMXUSDT" => "IMX-USDT",
        "IMXUSDTSWAP" => "IMX-USDT-SWAP",
        "COMPUSDT" => "COMP-USDT",
        "COMPUSDTSWAP" => "COMP-USDT-SWAP",
        "ALGOUSDT" => "ALGO-USDT",
        "ALGOUSDTSWAP" => "ALGO-USDT-SWAP",
        "WAVESUSDT" => "WAVES-USDT",
        "WAVESUSDTSWAP" => "WAVES-USDT-SWAP",
        "DASHUSDT" => "DASH-USDT",
        "DASHUSDTSWAP" => "DASH-USDT-SWAP",
        "ENJUSDT" => "ENJ-USDT",
        "ENJUSDTSWAP" => "ENJ-USDT-SWAP",
        "1INCHUSDT" => "1INCH-USDT",
        "1INCHUSDTSWAP" => "1INCH-USDT-SWAP",
        "PERPUSDT" => "PERP-USDT",
        "PERPUSDTSWAP" => "PERP-USDT-SWAP",
        "NEARUSDT" => "NEAR-USDT",
        "NEARUSDTSWAP" => "NEAR-USDT-SWAP",
        "ANTUSDT" => "ANT-USDT",
        "ANTUSDTSWAP" => "ANT-USDT-SWAP",
        "GALUSDT" => "GAL-USDT",
        "GALUSDTSWAP" => "GAL-USDT-SWAP",
        "ONEUSDT" => "ONE-USDT",
        "ONEUSDTSWAP" => "ONE-USDT-SWAP",
        "MKRUSDT" => "MKR-USDT",
        "MKRUSDTSWAP" => "MKR-USDT-SWAP",
        "GALAUSDT" => "GALA-USDT",
        "GALAUSDTSWAP" => "GALA-USDT-SWAP",
        "AVAXUSDT" => "AVAX-USDT",
        "AVAXUSDTSWAP" => "AVAX-USDT-SWAP",
        "MASKUSDT" => "MASK-USDT",
        "MASKUSDTSWAP" => "MASK-USDT-SWAP",
        "STXUSDT" => "STX-USDT",
        "STXUSDTSWAP" => "STX-USDT-SWAP",
        _ => "",
    };

    if result.is_empty() {
        std::borrow::Cow::Owned(if value.contains("-") {
            value.replace("-", "")
        } else if value.ends_with("SWAP") {
            value.replace("USDT", "-USDT-")
        } else {
            value.replace("USDT", "-USDT")
        })
    } else {
        std::borrow::Cow::Borrowed(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn product_mapping_falls_back_for_unknown_spot_symbols() {
        assert_eq!(product_mapping("FOO-USDT"), "FOOUSDT");
        assert_eq!(product_mapping("FOOUSDT"), "FOO-USDT");
    }

    #[test]
    fn product_mapping_falls_back_for_unknown_swap_symbols() {
        assert_eq!(product_mapping("FOO-USDT-SWAP"), "FOOUSDTSWAP");
        assert_eq!(product_mapping("FOOUSDTSWAP"), "FOO-USDT-SWAP");
    }

    #[test]
    fn local_time_round_trip_preserves_exact_seconds() {
        let timestamp = 1_715_164_800_000_u64;
        let text = time_to_string(timestamp);

        assert_eq!(string_to_time(text), timestamp);
    }

    #[test]
    fn k_time_convert_aligns_to_hour_boundaries() {
        let timestamp = chrono::NaiveDate::from_ymd_opt(2024, 5, 8)
            .unwrap()
            .and_hms_opt(5, 20, 0)
            .unwrap()
            .and_utc()
            .timestamp_millis() as u64;
        let (start, end) = k_time_convert(timestamp, Level::Hour4);

        assert_eq!(
            start,
            chrono::NaiveDate::from_ymd_opt(2024, 5, 8)
                .unwrap()
                .and_hms_opt(4, 0, 0)
                .unwrap()
                .and_utc()
                .timestamp_millis() as u64
        );
        assert_eq!(
            end,
            chrono::NaiveDate::from_ymd_opt(2024, 5, 8)
                .unwrap()
                .and_hms_opt(8, 0, 0)
                .unwrap()
                .and_utc()
                .timestamp_millis() as u64
        );
    }
}

// Converts backtesting results to HTML text.
//
// * `k`: Candlestick data.
// * `result`: Backtesting results.
// * `return`: HTML text.
// pub fn to_html<A, B>(k: A, result: B) -> String
// where
//     A: AsRef<[K]>,
//     B: AsRef<[Position]>,
// {
//     let k = k.as_ref();
//     let result = result.as_ref();
//     let text =
//         include_str!("../template.txt").replace("{data}", &serde_json::to_string(k).unwrap());
//     let mut mark = String::new();
//     for p in result {
//         for (index, i) in p.log.iter().enumerate() {
//             mark += &format!(
//                 "mark({},\"{}\",{});",
//                 i.time,
//                 match i.side {
//                     Side::EnterLong =>
//                         if index == 0 {
//                             "BuyLong First"
//                         } else {
//                             "BuyLong +1"
//                         },
//                     Side::EnterShort =>
//                         if index == 0 {
//                             "SellShort First"
//                         } else {
//                             "SellShort +1"
//                         },
//                     Side::ExitLong =>
//                         if index == p.log.len() - 1 {
//                             "BuySell All"
//                         } else {
//                             "BuySell -1"
//                         },
//                     Side::ExitShort =>
//                         if index == p.log.len() - 1 {
//                             "SellLong All"
//                         } else {
//                             "SellLong -1"
//                         },
//                 },
//                 i.price
//             );
//         }
//     }
//     text.replace("{mark}", &mark)
// }
