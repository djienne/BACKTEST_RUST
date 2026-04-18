use crate::*;
use std::ops;

/// Candlestick (K-line) k.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct K {
    /// Time of the candlestick.
    pub time: u64,

    /// Opening price.
    pub open: f32,

    /// Highest price.
    pub high: f32,

    /// Lowest price.
    pub low: f32,

    /// Closing price.
    pub close: f32,
}

impl std::fmt::Display for K {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{\"time\": {}, \"open\": {}, \"high\": {}, \"low\": {}, \"close\": {}}}",
            time_to_string(self.time),
            self.open,
            self.high,
            self.low,
            self.close
        )
    }
}

/// Time levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Level {
    /// 1 minute.
    Minute1,

    /// 3 minutes.
    Minute3,

    /// 5 minutes.
    Minute5,

    /// 15 minutes.
    Minute15,

    /// 30 minutes.
    Minute30,

    /// 1 hour.
    Hour1,

    /// 2 hours.
    Hour2,

    /// 4 hours.
    Hour4,

    /// 6 hours.
    Hour6,

    /// 12 hours.
    Hour12,

    /// 1 day.
    Day1,

    /// 3 days.
    Day3,

    /// 1 week.
    Week1,

    /// 1 month.
    Month1,
}

impl std::fmt::Display for Level {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Level::Minute1 => f.write_str("1m"),
            Level::Minute3 => f.write_str("3m"),
            Level::Minute5 => f.write_str("5m"),
            Level::Minute15 => f.write_str("15m"),
            Level::Minute30 => f.write_str("30m"),
            Level::Hour1 => f.write_str("1h"),
            Level::Hour2 => f.write_str("2h"),
            Level::Hour4 => f.write_str("4h"),
            Level::Hour6 => f.write_str("6h"),
            Level::Hour12 => f.write_str("12h"),
            Level::Day1 => f.write_str("1d"),
            Level::Day3 => f.write_str("3d"),
            Level::Week1 => f.write_str("1w"),
            Level::Month1 => f.write_str("1M"),
        }
    }
}

/// Data series.
/// Out-of-bound index will return f32::NAN.
/// Out-of-bound slice will return &[].
#[repr(transparent)] // Keep the layout identical to `[f32]` for slice reborrowing.
#[derive(Debug)]
pub struct Source {
    pub inner: [f32],
}

impl Source {
    pub fn new(value: &[f32]) -> &Self {
        // SAFETY: `Source` is `#[repr(transparent)]` over `[f32]`, so a slice reference
        // can be reborrowed as `&Source` without changing layout or lifetime.
        unsafe { &*(value as *const [f32] as *const Self) }
    }

    fn index<T>(&self, index: T) -> &Source
    where
        T: std::slice::SliceIndex<[f32], Output = [f32]>,
    {
        Self::new(self.inner.get(index).unwrap_or(&[]))
    }
}

impl std::ops::Deref for Source {
    type Target = [f32];

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::fmt::Display for &Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", self[0]))
    }
}

impl std::ops::Index<usize> for Source {
    type Output = f32;

    fn index(&self, index: usize) -> &Self::Output {
        self.inner.get(index).unwrap_or(&f32::NAN)
    }
}

impl std::ops::Index<std::ops::Range<usize>> for Source {
    type Output = Source;

    fn index(&self, index: std::ops::Range<usize>) -> &Self::Output {
        self.index(index)
    }
}

impl std::ops::Index<std::ops::RangeFrom<usize>> for Source {
    type Output = Source;

    fn index(&self, index: std::ops::RangeFrom<usize>) -> &Self::Output {
        self.index(index)
    }
}

impl std::ops::Index<std::ops::RangeTo<usize>> for Source {
    type Output = Source;

    fn index(&self, index: std::ops::RangeTo<usize>) -> &Self::Output {
        self.index(index)
    }
}

impl std::ops::Index<std::ops::RangeFull> for Source {
    type Output = Source;

    fn index(&self, index: std::ops::RangeFull) -> &Self::Output {
        self.index(index)
    }
}

impl std::ops::Index<std::ops::RangeInclusive<usize>> for Source {
    type Output = Source;

    fn index(&self, index: std::ops::RangeInclusive<usize>) -> &Self::Output {
        self.index(index)
    }
}

impl std::ops::Index<std::ops::RangeToInclusive<usize>> for Source {
    type Output = Source;

    fn index(&self, index: std::ops::RangeToInclusive<usize>) -> &Self::Output {
        self.index(index)
    }
}

impl PartialEq<i64> for &Source {
    fn eq(&self, other: &i64) -> bool {
        self[0] == *other as f32
    }
}

impl PartialEq<f32> for &Source {
    fn eq(&self, other: &f32) -> bool {
        self[0] == *other
    }
}

impl PartialEq<[f32]> for Source {
    fn eq(&self, other: &[f32]) -> bool {
        &self.inner == other
    }
}

impl PartialEq for &Source {
    fn eq(&self, other: &Self) -> bool {
        self[0] == other[0]
    }
}

impl PartialOrd<i64> for &Source {
    fn partial_cmp(&self, other: &i64) -> Option<std::cmp::Ordering> {
        self[0].partial_cmp(&(*other as f32))
    }
}

impl PartialOrd<f32> for &Source {
    fn partial_cmp(&self, other: &f32) -> Option<std::cmp::Ordering> {
        self[0].partial_cmp(other)
    }
}

impl PartialOrd<[f32]> for Source {
    fn partial_cmp(&self, other: &[f32]) -> Option<std::cmp::Ordering> {
        self.inner.partial_cmp(other)
    }
}

impl PartialOrd for &Source {
    fn partial_cmp(&self, other: &&Source) -> Option<std::cmp::Ordering> {
        self[0].partial_cmp(&other[0])
    }
}

overload::overload!((a: &Source) + (b: i64) -> f32 { a[0] + b as f32 });

overload::overload!((a: &Source) - (b: i64) -> f32 { a[0] - b as f32 });

overload::overload!((a: &Source) * (b: i64) -> f32 { a[0] * b as f32 });

overload::overload!((a: &Source) / (b: i64) -> f32 { a[0] / b as f32 });

overload::overload!((a: &Source) % (b: i64) -> f32 { a[0] % b as f32 });

overload::overload!((a: &Source) + (b: f32) -> f32 { a[0] + b });

overload::overload!((a: &Source) - (b: f32) -> f32 { a[0] - b });

overload::overload!((a: &Source) * (b: f32) -> f32 { a[0] * b });

overload::overload!((a: &Source) / (b: f32) -> f32 { a[0] / b });

overload::overload!((a: &Source) % (b: f32) -> f32 { a[0] % b });

overload::overload!((a: i64) + (b: &Source) -> f32 { a as f32 + b[0] });

overload::overload!((a: i64) - (b: &Source) -> f32 { a as f32 - b[0] });

overload::overload!((a: i64) * (b: &Source) -> f32 { a as f32 * b[0] });

overload::overload!((a: i64) / (b: &Source) -> f32 { a as f32 / b[0] });

overload::overload!((a: i64) % (b: &Source) -> f32 { a as f32 % b[0] });

overload::overload!((a: f32) + (b: &Source) -> f32 { a + b[0] });

overload::overload!((a: f32) - (b: &Source) -> f32 { a - b[0] });

overload::overload!((a: f32) * (b: &Source) -> f32 { a * b[0] });

overload::overload!((a: f32) / (b: &Source) -> f32 { a / b[0] });

overload::overload!((a: f32) % (b: &Source) -> f32 { a % b[0] });

overload::overload!((a: &Source) + (b: &Source) -> f32 { a[0] + b[0] });

overload::overload!((a: &Source) - (b: &Source) -> f32 { a[0] - b[0] });

overload::overload!((a: &Source) * (b: &Source) -> f32 { a[0] * b[0] });

overload::overload!((a: &Source) / (b: &Source) -> f32 { a[0] / b[0] });

overload::overload!((a: &Source) % (b: &Source) -> f32 { a[0] % b[0] });

/// Time range.
#[derive(Debug, Clone, Copy)]
pub struct TimeRange {
    pub start: u64,
    pub end: u64,
}

impl From<u64> for TimeRange {
    fn from(value: u64) -> Self {
        Self {
            start: 0,
            end: value,
        }
    }
}

impl From<std::ops::Range<u64>> for TimeRange {
    fn from(value: std::ops::Range<u64>) -> Self {
        Self {
            start: value.start,
            end: value.end - 1,
        }
    }
}

impl From<std::ops::RangeFrom<u64>> for TimeRange {
    fn from(value: std::ops::RangeFrom<u64>) -> Self {
        Self {
            start: value.start,
            end: u64::MAX - 1,
        }
    }
}

impl From<std::ops::RangeTo<u64>> for TimeRange {
    fn from(value: std::ops::RangeTo<u64>) -> Self {
        Self {
            start: 0,
            end: value.end - 1,
        }
    }
}

impl From<std::ops::RangeFull> for TimeRange {
    fn from(_: std::ops::RangeFull) -> Self {
        Self {
            start: 0,
            end: u64::MAX - 1,
        }
    }
}

impl From<std::ops::RangeInclusive<u64>> for TimeRange {
    fn from(value: std::ops::RangeInclusive<u64>) -> Self {
        Self {
            start: *value.start(),
            end: *value.end(),
        }
    }
}

impl From<std::ops::RangeToInclusive<u64>> for TimeRange {
    fn from(value: std::ops::RangeToInclusive<u64>) -> Self {
        Self {
            start: 0,
            end: value.end,
        }
    }
}

/// Order direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum Side {
    /// Buy to open a long position.
    EnterLong,

    /// Sell to open a short position.
    EnterShort,

    /// Sell to close a long position.
    ExitLong,

    /// Buy to close a short position.
    ExitShort,
}

/// Trading record.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct Record {
    /// Position direction.
    pub side: Side,

    /// Price.
    pub price: f32,

    /// Position quantity.
    pub quantity: f32,

    /// Margin.
    pub margin: f32,

    /// Fee.
    pub fee: f32,

    /// Profit.
    pub profit: f32,

    /// Profit ratio.
    pub profit_ratio: f32,

    /// Time.
    pub time: u64,
}

/// Position.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Position {
    /// Trading product, for example, spot BTC-USDT, futures contract BTC-USDT-SWAP.
    pub product: String,

    /// Leverage.
    pub lever: u32,

    /// Position direction.
    pub side: Side,

    /// Average opening price.
    pub open_price: f32,

    /// Position quantity, in units of currency.
    pub quantity: f32,

    /// Margin.
    pub margin: f32,

    /// Liquidation price, 0 indicates no liquidation.
    pub liquidation_price: f32,

    /// Average closing price.
    pub close_price: f32,

    /// Profit.
    pub profit: f32,

    /// Profit ratio.
    pub profit_ratio: f32,

    /// Fee.
    pub fee: f32,

    /// Opening time.
    pub open_time: u64,

    /// Closing time.
    pub close_time: u64,

    /// Trading records.
    pub log: Vec<Record>,
}

/// Order.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Delegate {
    /// Position direction.
    pub side: Side,

    /// Order price.
    pub price: Price,

    /// Order quantity.
    pub quantity: f32,

    /// Margin.
    pub margin: f32,

    /// Additional margin.
    pub append_margin: f32,
}

/// Order Status.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DelegateState {
    /// Single order.
    Single(Delegate),

    /// Hedging order, opening order.
    Hedging(Delegate, Delegate),

    /// Hedging order, opening order, take profit order.
    HedgingProfit(Delegate, Delegate, Delegate),

    /// Hedging order, opening order, stop loss order.
    HedgingLoss(Delegate, Delegate, Delegate),

    /// Hedging order, opening order, take profit order, stop loss order.
    HedgingProfitLoss(Delegate, Delegate, Delegate, Delegate),

    /// Opening order, take profit order.
    OpenProfit(Delegate, Delegate),

    /// Opening order, stop loss order.
    OpenLoss(Delegate, Delegate),

    /// Opening order, take profit order, stop loss order.
    OpenProfitLoss(Delegate, Delegate, Delegate),

    /// Take profit order, stop loss order.
    ProfitLoss(Delegate, Delegate),
}

/// Price.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Price {
    /// Greater than or equal to the trigger price, market price.
    GreaterThanMarket(f32),

    /// Less than or equal to the trigger price, market price.
    LessThanMarket(f32),

    /// Greater than or equal to the trigger price, limit order.
    GreaterThanLimit(f32, f32),

    /// Less than or equal to the trigger price, limit order.
    LessThanLimit(f32, f32),
}

/// Context environment.
pub struct Context<'a> {
    /// Trading product, for example, spot BTC-USDT, futures contract BTC-USDT-SWAP.
    pub product: &'a str,

    /// Minimum order quantity.
    pub min_size: f32,

    /// Minimum nominal value.
    pub min_notional: f32,

    /// Time level.
    pub level: Level,

    /// Time of the candlestick.
    pub time: u64,

    /// Series of opening prices.
    pub open: &'a Source,

    /// Series of highest prices.
    pub high: &'a Source,

    /// Series of lowest prices.
    pub low: &'a Source,

    /// Series of closing prices.
    pub close: &'a Source,

    /// Internal trading interface.
    pub(crate) trading: &'a mut dyn Trading,
}

impl<'a> Context<'a> {
    /// Order.
    /// If the limit price for going long is greater than the market price, then the order will execute only when the price is greater than or equal to the limit price.
    /// If the limit price for going short is less than the market price, then the order will execute only when the price is less than or equal to the limit price.
    /// If the limit price to close a long position is less than the market price, then the order will execute only when the price is less than or equal to the limit price.
    /// If the limit price to close a short position is greater than the market price, then the order will execute only when the price is greater than or equal to the limit price.
    /// The take profit trigger price for going long cannot be less than or equal to the order price.
    /// The take profit trigger price for going short cannot be greater than or equal to the order price.
    /// The stop loss trigger price for going long cannot be greater than or equal to the order price.
    /// The stop loss trigger price for going short cannot be less than or equal to the order price.
    /// A limit order to close a position will not be executed during the current candlestick.
    /// Closing a position will not result in an opposite order being opened, and the closing quantity can only be less than or equal to the existing position quantity.
    /// If after attempting to close, the quantity of an existing limit order to close is less than the position quantity, the order will be cancelled.
    /// Take profit and stop loss for closing positions are invalid.
    ///
    /// * `side`: Order direction.
    /// * `price`: Order price, 0 for market price, others for limit price.
    /// * `return`: Order ID.
    pub fn order(&mut self, side: Side, price: f32) -> anyhow::Result<u64> {
        self.trading.order(
            self.product,
            side,
            price,
            Unit::Ignore,
            Unit::Ignore,
            Unit::Ignore,
            Unit::Ignore,
            Unit::Ignore,
            Unit::Ignore,
        )
    }

    /// Order.
    /// If the limit price for going long is greater than the market price, then the order will only execute when the price is greater than or equal to the limit price.
    /// If the limit price for going short is less than the market price, then the order will only execute when the price is less than or equal to the limit price.
    /// If the limit price to close a long position is less than the market price, then the order will only execute when the price is less than or equal to the limit price.
    /// If the limit price to close a short position is greater than the market price, then the order will only execute when the price is greater than or equal to the limit price.
    /// The take profit trigger price for going long cannot be less than or equal to the order price.
    /// The take profit trigger price for going short cannot be greater than or equal to the order price.
    /// The stop loss trigger price for going long cannot be greater than or equal to the order price.
    /// The stop loss trigger price for going short cannot be less than or equal to the order price.
    /// A limit order to close a position will not be executed during the current candlestick.
    /// Closing a position will not result in an opposite order being opened, and the closing quantity can only be less than or equal to the existing position quantity.
    /// If after attempting to close, the quantity of an existing limit order to close is less than the position quantity, the order will be cancelled.
    /// Take profit and stop loss for closing positions are invalid.
    ///
    /// * `side`: Order direction.
    /// * `price`: Order price, 0 for market price, others for limit price.
    /// * `stop_profit_condition`: Stop profit trigger price, [`Unit::Ignore`] means not set, and thus `stop_profit` is invalid.
    /// * `stop_loss_condition`: Stop loss trigger price, [`Unit::Ignore`] means not set, and thus `stop_loss` is invalid.
    /// * `return`: Order ID.
    pub fn order_profit_loss(
        &mut self,
        side: Side,
        price: f32,
        stop_profit_condition: Unit,
        stop_loss_condition: Unit,
    ) -> anyhow::Result<u64> {
        self.trading.order(
            self.product,
            side,
            price,
            Unit::Ignore,
            Unit::Ignore,
            stop_profit_condition,
            stop_loss_condition,
            Unit::Ignore,
            Unit::Ignore,
        )
    }

    /// Order.
    /// If the limit price for a long position is greater than the market price, the order will only be executed when the price is greater than or equal to the limit price.
    /// If the limit price for a short position is less than the market price, the order will only be executed when the price is less than or equal to the limit price.
    /// If the limit price to close a long position is less than the market price, the order will only be executed when the price is less than or equal to the limit price.
    /// If the limit price to close a short position is greater than the market price, the order will only be executed when the price is greater than or equal to the limit price.
    /// The take profit trigger price for a long position cannot be less than or equal to the order price.
    /// The take profit trigger price for a short position cannot be greater than or equal to the order price.
    /// The stop loss trigger price for a long position cannot be greater than or equal to the order price.
    /// The stop loss trigger price for a short position cannot be less than or equal to the order price.
    /// A limit order to close a position will not be executed during the current candlestick.
    /// Closing a position will not result in an opposite order being opened, and the closing quantity can only be less than or equal to the existing position quantity.
    /// If after attempting to close, the quantity of an existing limit order to close is less than the position quantity, the order will be cancelled.
    /// Take profit and stop loss for closing positions are ineffective.
    ///
    /// * `side`: Order direction.
    /// * `price`: Order price, 0 for market price, others for limit price.
    /// * `stop_profit_condition`: Stop profit trigger price, [`Unit::Ignore`] means not set, and thus `stop_profit` is ineffective.
    /// * `stop_loss_condition`: Stop loss trigger price, [`Unit::Ignore`] means not set, and thus `stop_loss` is ineffective.
    /// * `stop_profit`: Stop profit order price, [`Unit::Ignore`] means not set, others indicate limit price.
    /// * `stop_loss`: Stop loss order price, [`Unit::Ignore`] means not set, others indicate limit price.
    /// * `return`: Order ID.
    pub fn order_profit_loss_condition(
        &mut self,
        side: Side,
        price: f32,
        stop_profit_condition: Unit,
        stop_loss_condition: Unit,
        stop_profit: Unit,
        stop_loss: Unit,
    ) -> anyhow::Result<u64> {
        self.trading.order(
            self.product,
            side,
            price,
            Unit::Ignore,
            Unit::Ignore,
            stop_profit_condition,
            stop_loss_condition,
            stop_profit,
            stop_loss,
        )
    }

    /// Order.
    /// If the limit price for going long is greater than the market price, then the transaction will only occur when the price is greater than or equal to the limit price.
    /// If the limit price for going short is less than the market price, then the transaction will only occur when the price is less than or equal to the limit price.
    /// If the limit price to close a long position is less than the market price, then the transaction will only occur when the price is less than or equal to the limit price.
    /// If the limit price to close a short position is greater than the market price, then the transaction will only occur when the price is greater than or equal to the limit price.
    /// The take profit trigger price for going long cannot be less than or equal to the order price.
    /// The take profit trigger price for going short cannot be greater than or equal to the order price.
    /// The stop loss trigger price for going long cannot be greater than or equal to the order price.
    /// The stop loss trigger price for going short cannot be less than or equal to the order price.
    /// A limit order to close a position will not be executed in the current candlestick.
    /// Closing a position will not cause the position to open in the opposite direction, and the quantity closed can only be less than or equal to the current position quantity.
    /// If, after executing a close position operation, the quantity of an existing limit order to close is less than the position quantity, the order will be cancelled.
    /// Take profit and stop loss for closing positions are invalid.
    ///
    /// * `side`: Order direction.
    /// * `price`: Order price, 0 indicates market price, others indicate limit price.
    /// * `quantity`: Order quantity, in units of currency. If it is for opening a position, [`Unit::Ignore`] means to use the setting from [`Config::quantity`]; if it is for closing a position, [`Unit::Ignore`] means all of the position, [`Unit::Proportion`] means a proportion of the position.
    /// * `margin`: Margin, [`Unit::Ignore`] means to use the setting from [`Config::margin`]; the margin multiplied by leverage must be greater than the position value, i.e., [`Config::margin`] * [`Config::lever`] >= [`Config::quantity`], and any excess margin is considered additional margin.
    /// * `return`: Order ID.
    pub fn order_quantity_margin(
        &mut self,
        side: Side,
        price: f32,
        quantity: Unit,
        margin: Unit,
    ) -> anyhow::Result<u64> {
        self.trading.order(
            self.product,
            side,
            price,
            quantity,
            margin,
            Unit::Ignore,
            Unit::Ignore,
            Unit::Ignore,
            Unit::Ignore,
        )
    }

    /// Order.
    /// If the limit price for going long is greater than the market price, then the transaction will only occur when the price is greater than or equal to the limit price.
    /// If the limit price for going short is less than the market price, then the transaction will only occur when the price is less than or equal to the limit price.
    /// If the limit price to close a long position is less than the market price, then the transaction will only occur when the price is less than or equal to the limit price.
    /// If the limit price to close a short position is greater than the market price, then the transaction will only occur when the price is greater than or equal to the limit price.
    /// The take profit trigger price for going long cannot be less than or equal to the order price.
    /// The take profit trigger price for going short cannot be greater than or equal to the order price.
    /// The stop loss trigger price for going long cannot be greater than or equal to the order price.
    /// The stop loss trigger price for going short cannot be less than or equal to the order price.
    /// A limit order to close a position will not be executed in the current candlestick.
    /// Closing a position will not cause an order to open in the opposite direction, and the quantity closed can only be less than or equal to the current position quantity.
    /// If, after executing a close position operation, the quantity of an existing limit order to close is less than the position quantity, the order will be cancelled.
    /// Take profit and stop loss for closing positions are invalid.
    ///
    /// * `side`: Order direction.
    /// * `price`: Order price, 0 indicates market price, others indicate limit price.
    /// * `quantity`: Order quantity, in units of currency. If opening a position, [`Unit::Ignore`] indicates using the setting from [`Config::quantity`]; if closing a position, [`Unit::Ignore`] indicates the entire position, [`Unit::Proportion`] indicates a proportion of the position.
    /// * `margin`: Margin, [`Unit::Ignore`] indicates using the setting from [`Config::margin`]; the margin multiplied by leverage must be greater than the position value, i.e., [`Config::margin`] * [`Config::lever`] >= [`Config::quantity`], and any excess margin is considered additional margin.
    /// * `stop_profit_condition`: Stop profit trigger price, [`Unit::Ignore`] means not set, and `stop_profit` is invalid.
    /// * `stop_loss_condition`: Stop loss trigger price, [`Unit::Ignore`] means not set, and `stop_loss` is invalid.
    /// * `stop_profit`: Stop profit order price, [`Unit::Ignore`] means not set, others indicate limit price.
    /// * `stop_loss`: Stop loss order price, [`Unit::Ignore`] means not set, others indicate limit price.
    /// * `return`: Order ID.
    pub fn order_condition(
        &mut self,
        side: Side,
        price: f32,
        quantity: Unit,
        margin: Unit,
        stop_profit_condition: Unit,
        stop_loss_condition: Unit,
        stop_profit: Unit,
        stop_loss: Unit,
    ) -> anyhow::Result<u64> {
        self.trading.order(
            self.product,
            side,
            price,
            quantity,
            margin,
            stop_profit_condition,
            stop_loss_condition,
            stop_profit,
            stop_loss,
        )
    }

    /// Cancel order.
    /// For orders that have already been executed, this will cancel any associated take profit and stop loss orders.
    ///
    /// * `id`: Order ID, 0 means cancel all orders.
    pub fn cancel(&mut self, id: u64) -> bool {
        self.trading.cancel(id)
    }

    /// Get balance.
    pub fn balance(&self) -> f32 {
        self.trading.balance()
    }

    /// Get order.
    ///
    /// * `product`: Order ID.
    /// * `return`: The state of the order, returns None if the order does not exist or has already been executed.
    pub fn delegate(&self, id: u64) -> Option<DelegateState> {
        self.trading.delegate(id)
    }

    /// Get position.
    ///
    /// * `id`: Order ID.
    pub fn position(&self) -> Option<&Position> {
        self.trading.position(self.product)
    }
}

/// Trading Interface.
pub trait Trading {
    /// Order.
    /// If the long position limit price is greater than the market price, the order will be executed only when the price is greater than or equal to the limit price.
    /// If the short position limit price is less than the market price, the order will be executed only when the price is less than or equal to the limit price.
    /// If the limit price to close a long position is less than the market price, the order will be executed only when the price is less than or equal to the limit price.
    /// If the limit price to close a short position is greater than the market price, the order will be executed only when the price is greater than or equal to the limit price.
    /// The take profit trigger price for a long position cannot be less than or equal to the order price.
    /// The take profit trigger price for a short position cannot be greater than or equal to the order price.
    /// The stop loss trigger price for a long position cannot be greater than or equal to the order price.
    /// The stop loss trigger price for a short position cannot be less than or equal to the order price.
    /// A limit order to close a position will not be executed in the current candlestick.
    /// Closing a position will not cause the position to open in the opposite direction, and the quantity closed can only be less than or equal to the current position quantity.
    /// If, after executing a close position operation, the quantity of an existing limit order to close is less than the position quantity, the order will be cancelled.
    /// Stop profit and stop loss for closing positions are invalid.
    ///
    /// * `product` Trading product, for example, spot BTC-USDT, contract BTC-USDT-SWAP.
    /// * `side` Order direction.
    /// * `price` Order price, 0 indicates market price, others indicate limit price.
    /// * `quantity` Order quantity, in units of currency. If it's opening a position, [`Unit::Ignore`] means to use the setting from [`Config::quantity`]. If it's closing a position, [`Unit::Ignore`] means all of the position, [`Unit::Proportion`] means a proportion of the position.
    /// * `margin` Margin, [`Unit::Ignore`] means to use the setting from [`Config::margin`]. The margin multiplied by leverage must be greater than the position value, i.e., [`Config::margin`] * [`Config::lever`] >= [`Config::quantity`], and any excess margin is considered additional margin.
    /// * `stop_profit_condition` Stop profit trigger price, [`Unit::Ignore`] means not set, and `stop_profit` is invalid.
    /// * `stop_loss_condition` Stop loss trigger price, [`Unit::Ignore`] means not set, and `stop_loss` is invalid.
    /// * `stop_profit` Stop profit order price, [`Unit::Ignore`] means not set, others indicate limit price.
    /// * `stop_loss` Stop loss order price, [`Unit::Ignore`] means not set, others indicate limit price.
    /// * `return` Order id.
    fn order(
        &mut self,
        product: &str,
        side: Side,
        price: f32,
        quantity: Unit,
        margin: Unit,
        stop_profit_condition: Unit,
        stop_loss_condition: Unit,
        stop_profit: Unit,
        stop_loss: Unit,
    ) -> anyhow::Result<u64>;

    /// Cancel order.
    /// For orders that have been executed, this will cancel any associated take profit and stop loss orders.
    ///
    /// * `id`: Order ID, 0 means cancel all orders.
    fn cancel(&mut self, id: u64) -> bool;

    /// Get balance.
    fn balance(&self) -> f32;

    /// Get order.
    ///
    /// * `product`: Order ID.
    /// * `return`: The state of the order, returns None if the order does not exist or has already been executed.
    fn delegate(&self, id: u64) -> Option<DelegateState>;

    /// Get position.
    ///
    /// * `id`: Order ID.
    fn position(&self, product: &str) -> Option<&Position>;
}

/// Quantity, Proportion
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Unit {
    /// Ignore.
    Ignore,

    /// Quantity.
    Quantity(f32),

    /// Proportion.
    Proportion(f32),
}

/// Trading Configuration.
#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub initial_margin: f32,
    pub lever: u32,
    pub open_fee: f32,
    pub close_fee: f32,
    pub deviation: f32,
    pub maintenance: f32,
    pub quantity: Unit,
    pub margin: Unit,
    pub max_margin: Unit,
}

impl Config {
    pub fn new() -> Self {
        Config {
            initial_margin: 0.0,
            lever: 1,
            open_fee: 0.0,
            close_fee: 0.0,
            deviation: 0.0,
            maintenance: 0.0,
            quantity: Unit::Ignore,
            margin: Unit::Ignore,
            max_margin: Unit::Ignore,
        }
    }

    /// Initial margin.
    pub fn initial_margin(mut self, value: f32) -> Self {
        self.initial_margin = value;
        self
    }

    /// Leverage.
    pub fn lever(mut self, value: u32) -> Self {
        self.lever = value;
        self
    }

    /// Fee rate for placing orders.
    pub fn open_fee(mut self, value: f32) -> Self {
        self.open_fee = value;
        self
    }

    /// Fee rate for taking orders.
    pub fn close_fee(mut self, value: f32) -> Self {
        self.close_fee = value;
        self
    }

    /// Slippage rate.
    pub fn deviation(mut self, value: f32) -> Self {
        self.deviation = value;
        self
    }

    /// Maintenance margin rate.
    pub fn maintenance(mut self, value: f32) -> Self {
        self.maintenance = value;
        self
    }

    /// The value of the position for each opening.
    /// Default is the minimum order quantity.
    ///
    /// * [`Unit::Quantity`] Amount, in units of the currency.
    /// * [`Unit::Proportion`] Proportion of the initial margin used.
    pub fn quantity(mut self, value: Unit) -> Self {
        self.quantity = value;
        self
    }

    /// The margin invested for each opening.
    /// Default is the minimum cost required to open a position.
    /// The margin multiplied by leverage must be greater than the value of the position, i.e., [`Config::margin`] * [`Config::lever`] >= [`Config::quantity`].
    /// Any excess margin beyond the position value is considered additional margin.
    ///
    /// * [`Unit::Quantity`] Amount, in units of fiat currency.
    /// * [`Unit::Proportion`] Proportion of the initial margin used.
    pub fn margin(mut self, value: Unit) -> Self {
        self.margin = value;
        self
    }

    /// Maximum amount of margin that can be invested, exceeding which will result in order failure.
    /// Default is no limit.
    ///
    /// * [`Unit::Quantity`] Amount, such as USDT.
    /// * [`Unit::Proportion`] Proportion of the initial margin used.
    pub fn max_margin(mut self, value: Unit) -> Self {
        self.max_margin = value;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_supports_safe_slice_views() {
        let values = [1.0, 2.0, 3.0];
        let source = Source::new(&values);

        assert_eq!(source[0], 1.0);
        assert_eq!(source[1..][0], 2.0);
        assert!(source[10].is_nan());
        assert!(source[10..].is_empty());
    }

    #[test]
    fn source_comparisons_do_not_panic_on_empty_slices() {
        let empty = Source::new(&[]);
        let equals = empty == 1.0;
        let greater = empty > 1.0;
        let lower = empty < 1_i64;

        assert!(!equals);
        assert!(!greater);
        assert!(!lower);
    }
}
