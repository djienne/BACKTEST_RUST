use crate::*;

/// Information.
#[derive(Debug)]
struct Message {
    /// Minimum order quantity.
    min_size: f32,

    /// Minimum nominal value.
    min_notional: f32,

    /// Candlestick data.
    k: K,

    /// Order ID, order status.
    delegate: Vec<(u64, DelegateState)>,

    /// Position.
    position: Option<Position>,
}

/// Matching Engine.
#[derive(Debug)]
pub struct MatchEngine {
    /// Balance.
    balance: f32,

    /// Order ID.
    id: u64,

    /// Trading configuration.
    config: Config,

    /// Product, information.
    product: Vec<(String, Message)>,

    /// Historical positions.
    history: Vec<Position>,
}

impl MatchEngine {
    fn delegate_trigger_price(price: Price) -> f32 {
        match price {
            Price::GreaterThanMarket(v)
            | Price::LessThanMarket(v)
            | Price::GreaterThanLimit(v, _)
            | Price::LessThanLimit(v, _) => v,
        }
    }

    fn refundable_open_delegate(delegate_state: &DelegateState) -> Option<Delegate> {
        match delegate_state {
            DelegateState::Single(v) if v.side == Side::EnterLong || v.side == Side::EnterShort => {
                Some(*v)
            }
            DelegateState::Hedging(.., v)
            | DelegateState::HedgingProfit(_, v, ..)
            | DelegateState::HedgingLoss(_, v, ..)
            | DelegateState::HedgingProfitLoss(_, v, ..)
            | DelegateState::OpenProfit(v, ..)
            | DelegateState::OpenLoss(v, ..)
            | DelegateState::OpenProfitLoss(v, ..) => Some(*v),
            _ => None,
        }
    }

    fn reserved_open_funds(config: Config, delegate: Delegate) -> f32 {
        delegate.margin
            + Self::delegate_trigger_price(delegate.price) * delegate.quantity * config.open_fee
    }

    fn calculate_liquidation_price(
        side: Side,
        open_price: f32,
        quantity: f32,
        margin: f32,
        lever: u32,
        maintenance: f32,
        close_fee: f32,
    ) -> f32 {
        if quantity <= 0.0 {
            return 0.0;
        }

        let imr = 1.0 / lever as f32;
        let price = if side == Side::EnterLong {
            let denominator = 1.0 - maintenance - close_fee;
            if denominator <= 0.0 {
                return 0.0;
            }

            (open_price * (1.0 - imr) - (margin - open_price * quantity * imr) / quantity)
                / denominator
        } else {
            let denominator = 1.0 + maintenance + close_fee;
            (open_price * (1.0 + imr) + (margin - open_price * quantity * imr) / quantity)
                / denominator
        };

        price.max(0.0)
    }

    /// Construct a matching engine.
    ///
    /// * `config` Trading configuration.
    pub fn new(config: Config) -> Self {
        Self {
            balance: config.initial_margin,
            id: 0,
            config,
            product: Vec::new(),
            history: Vec::new(),
        }
    }

    /// Get balance.
    pub fn balance(&self) -> f32 {
        self.balance
    }

    /// Get order.
    ///
    /// * `product` Order ID.
    /// * `return` The status of the order, if the order does not exist or has already been executed, then returns None.
    pub fn delegate(&self, id: u64) -> Option<DelegateState> {
        for i in self.product.iter() {
            if let Some(v) = i.1.delegate.iter().find(|v| v.0 == id).map(|v| v.1) {
                return Some(v);
            }
        }

        None
    }

    /// Get current position.
    ///
    /// * `product` Trading product, for example, spot BTC-USDT, contract BTC-USDT-SWAP.
    /// * `return` Position.
    pub fn position<S>(&self, product: S) -> Option<&Position>
    where
        S: AsRef<str>,
    {
        let product = product.as_ref();
        self.product
            .iter()
            .find(|v| v.0 == product)
            .map(|v| &v.1.position)
            .and_then(|v| v.as_ref())
    }

    /// Get historical positions.
    pub fn history(&self) -> &Vec<Position> {
        &self.history
    }

    /// Insert product.
    ///
    /// * `product` Trading product.
    /// * `min_size` Minimum order quantity.
    /// * `min_notional` Minimum nominal value.
    pub fn insert_product<S>(&mut self, product: S, min_size: f32, min_notional: f32)
    where
        S: AsRef<str>,
    {
        let product = product.as_ref();

        let message = Message {
            min_size,
            min_notional,
            k: K {
                time: 114514,
                open: 0.0,
                high: 0.0,
                low: 0.0,
                close: 0.0,
            },
            delegate: Vec::new(),
            position: None,
        };

        if let Some(v) = self.product.iter().position(|v| v.0 == product) {
            self.product[v].1 = message;
        } else {
            self.product.push((product.to_string(), message));
        }
    }

    /// Remove a product.
    ///
    /// * `product` Trading product.
    pub fn remove_product<S>(&mut self, product: S)
    where
        S: AsRef<str>,
    {
        let product = product.as_ref();
        self.product.retain(|v| v.0 != product);
    }

    /// Preparation.
    /// Preparation is required before making an order.
    /// Before preparation, a product must be inserted.
    ///
    /// * `product`: Trading product.
    /// * `k`: Candlestick data.
    pub fn ready<S>(&mut self, product: S, k: K)
    where
        S: AsRef<str>,
    {
        let product = product.as_ref();
        self.product
            .iter_mut()
            .find(|v| v.0 == product)
            .map(|v| &mut v.1)
            .unwrap_or_else(|| panic!("no product: {}", product))
            .k = k;
    }

    /// Order.
    /// If the limit price for going long is greater than the market price, then the transaction will only occur when the price is greater than or equal to the limit price.
    /// If the limit price for going short is less than the market price, then the transaction will only occur when the price is less than or equal to the limit price.
    /// If the limit price for closing a long position is less than the market price, then the transaction will only occur when the price is less than or equal to the limit price.
    /// If the limit price for closing a short position is greater than the market price, then the transaction will only occur when the price is greater than or equal to the limit price.
    /// The take profit trigger price for going long cannot be less than or equal to the order price.
    /// The take profit trigger price for going short cannot be greater than or equal to the order price.
    /// The stop loss trigger price for going long cannot be greater than or equal to the order price.
    /// The stop loss trigger price for going short cannot be less than or equal to the order price.
    /// A limit order to close a position will not be executed in the current candlestick.
    /// Closing a position will not cause an order to open in the opposite direction, and the quantity closed can only be less than or equal to the current position quantity.
    /// If, after executing a close position operation, the quantity of an existing limit order to close is less than the position quantity, the order will be cancelled.
    /// Take profit and stop loss for closing positions are invalid.
    ///
    /// * `product` Trading product, for example, spot BTC-USDT, contract BTC-USDT-SWAP.
    /// * `side` Order direction.
    /// * `price` Order price, 0 indicates market price, others indicate limit price.
    /// * `quantity` Order quantity, in units of currency. If it is for opening a position, [`Unit::Ignore`] means to use the setting from [`Config::quantity`]; if it is for closing a position, [`Unit::Ignore`] means all of the position, [`Unit::Proportion`] means a proportion of the position.
    /// * `margin` Margin, [`Unit::Ignore`] means to use the setting from [`Config::margin`]; the margin multiplied by leverage must be greater than the position value, i.e., [`Config::margin`] * [`Config::lever`] >= [`Config::quantity`], and any excess margin is considered additional margin.
    /// * `stop_profit_condition` Stop profit trigger price, [`Unit::Ignore`] means not set, and `stop_profit` is invalid.
    /// * `stop_loss_condition` Stop loss trigger price, [`Unit::Ignore`] means not set, and `stop_loss` is invalid.
    /// * `stop_profit` Stop profit order price, [`Unit::Ignore`] means not set, others indicate limit price.
    /// * `stop_loss` Stop loss order price, [`Unit::Ignore`] means not set, others indicate limit price.
    /// * `return` Order id.
    pub fn order<S>(
        &mut self,
        product: S,
        side: Side,
        price: f32,
        quantity: Unit,
        margin: Unit,
        stop_profit_condition: Unit,
        stop_loss_condition: Unit,
        stop_profit: Unit,
        stop_loss: Unit,
    ) -> anyhow::Result<u64>
    where
        S: AsRef<str>,
    {
        let product = product.as_ref();

        let Message {
            min_size,
            min_notional,
            k,
            delegate,
            position,
        } = self
            .product
            .iter_mut()
            .find(|v| v.0 == product)
            .map(|v| &mut v.1)
            .ok_or(anyhow::anyhow!("no product: {}", product))?;

        if side == Side::EnterLong || side == Side::EnterShort {
            // Market price conversion
            let price = if price == 0.0 { k.close } else { price };

            // Order quantity
            let quantity = match if quantity == Unit::Ignore {
                self.config.quantity
            } else {
                quantity
            } {
                Unit::Ignore => *min_size,
                Unit::Quantity(v) => v,
                Unit::Proportion(v) => {
                    (self.config.initial_margin * v / price / *min_size).floor() * *min_size
                }
            };

            // The order quantity must not be less than the minimum order size.
            if quantity < *min_size {
                anyhow::bail!(
                    "product {}: open quantity < min size: {} < {}",
                    product,
                    quantity,
                    min_size
                );
            }

            // The value of the order quantity
            let quantity_value = price * quantity;

            // The value of the order quantity must not be less than the minimum nominal value
            if quantity_value < *min_notional {
                anyhow::bail!(
                    "product {}: open quantity value < min notional: {} < {}",
                    product,
                    quantity_value,
                    min_notional
                );
            }

            // The margin invested
            let margin = match if margin == Unit::Ignore {
                self.config.margin
            } else {
                margin
            } {
                Unit::Ignore => price * quantity / self.config.lever as f32,
                Unit::Quantity(v) => v,
                Unit::Proportion(v) => self.config.initial_margin * v,
            };

            // The margin must be sufficient to maintain the value of the position
            // Writing it as multiplication may lead to precision issues
            if margin < quantity_value / self.config.lever as f32 {
                anyhow::bail!(
                    "product {}: margin * lever < open quantity value: {} * {} < {}",
                    product,
                    margin,
                    self.config.lever,
                    quantity_value
                );
            }

            // Commission fee
            let fee = price * quantity * self.config.open_fee;

            // Check balance
            if self.balance < margin + fee {
                anyhow::bail!(
                    "product {}: insufficient fund: balance < margin + fee: {} < {} + {}",
                    product,
                    self.balance,
                    margin,
                    fee
                );
            }

            // Check the maximum margin invested
            if self.config.max_margin != Unit::Ignore {
                if let Some(position) = position {
                    let max_margin = match self.config.max_margin {
                        Unit::Quantity(v) => v,
                        Unit::Proportion(v) => self.config.initial_margin * v,
                        Unit::Ignore => unreachable!("max_margin was checked above"),
                    };

                    if position.margin + margin > max_margin {
                        anyhow::bail!(
                "product {}: position margin + open margin > max margin: {} + {} > {:?}",
                product,
                position.margin,
                margin,
                max_margin,
            );
                    }
                }
            }

            // Check stop loss and take profit parameters
            if stop_profit_condition == Unit::Ignore && stop_profit != Unit::Ignore {
                anyhow::bail!(
                    "product {}: stop profit must be zero, because stop profit condition is zero",
                    product
                );
            }

            if stop_loss_condition == Unit::Ignore && stop_loss != Unit::Ignore {
                anyhow::bail!(
                    "product {}: stop loss must be zero, because stop loss condition is zero",
                    product
                )
            }

            let stop_profit_condition = match stop_profit_condition {
                Unit::Ignore => Unit::Ignore,
                Unit::Quantity(v) => Unit::Quantity(v),
                Unit::Proportion(v) => Unit::Quantity(if side == Side::EnterLong {
                    price + price * v
                } else {
                    price - price * v
                }),
            };

            let stop_loss_condition = match stop_loss_condition {
                Unit::Ignore => Unit::Ignore,
                Unit::Quantity(v) => Unit::Quantity(v),
                Unit::Proportion(v) => Unit::Quantity(if side == Side::EnterLong {
                    price - price * v
                } else {
                    price + price * v
                }),
            };

            let stop_profit = match stop_profit {
                Unit::Ignore => Unit::Ignore,
                Unit::Quantity(v) => Unit::Quantity(v),
                Unit::Proportion(v) => Unit::Quantity(if side == Side::EnterLong {
                    price + price * v
                } else {
                    price - price * v
                }),
            };

            let stop_loss = match stop_loss {
                Unit::Ignore => Unit::Ignore,
                Unit::Quantity(v) => Unit::Quantity(v),
                Unit::Proportion(v) => Unit::Quantity(if side == Side::EnterLong {
                    price - price * v
                } else {
                    price + price * v
                }),
            };

            if let Unit::Quantity(v) = stop_profit_condition {
                if v <= 0.0 {
                    anyhow::bail!("product {}: stop profit condition invalid: {}", product, v)
                }
            }

            if let Unit::Quantity(v) = stop_loss_condition {
                if v <= 0.0 {
                    anyhow::bail!("product {}: stop loss condition invalid: {}", product, v)
                }
            }

            if let Unit::Quantity(v) = stop_profit {
                if v <= 0.0 {
                    anyhow::bail!("product {}: stop profit invalid: {}", product, v)
                }
            }

            if let Unit::Quantity(v) = stop_loss {
                if v <= 0.0 {
                    anyhow::bail!("product {}: stop loss invalid: {}", product, v)
                }
            }

            // Check if the stop loss and take profit are beneficial for the position
            if side == Side::EnterLong {
                if let Unit::Quantity(v) = stop_profit_condition {
                    if v <= price {
                        anyhow::bail!(
                            "product {}: buy long, but stop profit condition <= open price: {} <= {}",
                            product,
                            v,
                            price
                        );
                    }
                }

                if let Unit::Quantity(v) = stop_loss_condition {
                    if v >= price {
                        anyhow::bail!(
                            "product {}: buy long, but stop loss condition >= open price: {} >= {}",
                            product,
                            v,
                            price
                        );
                    }
                }
            } else {
                if let Unit::Quantity(v) = stop_profit_condition {
                    if v >= price {
                        anyhow::bail!(
                            "product {}: sell short, but stop profit condition >= open price: {} >= {}",
                            product,
                            v,
                            price
                        );
                    }
                }

                if let Unit::Quantity(v) = stop_loss_condition {
                    if v <= price {
                        anyhow::bail!(
                            "product {}: sell short, but stop loss condition <= open price: {} <= {}",
                            product,
                            v,
                            price
                        );
                    }
                }
            };

            let price = if price >= k.close {
                Price::GreaterThanMarket(price)
            } else {
                Price::LessThanMarket(price)
            };

            let ds = match (stop_profit_condition, stop_loss_condition) {
                (Unit::Quantity(a), Unit::Ignore) => DelegateState::OpenProfit(
                    Delegate {
                        side,
                        price,
                        quantity,
                        margin,
                        append_margin: 0.0,
                    },
                    Delegate {
                        side: if side == Side::EnterLong {
                            Side::ExitLong
                        } else {
                            Side::ExitShort
                        },
                        price: if side == Side::EnterLong {
                            match stop_profit {
                                Unit::Quantity(b) => Price::GreaterThanLimit(a, b),
                                _ => Price::GreaterThanMarket(a),
                            }
                        } else {
                            match stop_profit {
                                Unit::Quantity(b) => Price::LessThanLimit(a, b),
                                _ => Price::LessThanMarket(a),
                            }
                        },
                        quantity,
                        margin,
                        append_margin: 0.0,
                    },
                ),
                (Unit::Ignore, Unit::Quantity(a)) => DelegateState::OpenLoss(
                    Delegate {
                        side,
                        price,
                        quantity,
                        margin,
                        append_margin: 0.0,
                    },
                    Delegate {
                        side: if side == Side::EnterLong {
                            Side::ExitLong
                        } else {
                            Side::ExitShort
                        },
                        price: if side == Side::EnterLong {
                            match stop_loss {
                                Unit::Quantity(b) => Price::LessThanLimit(a, b),
                                _ => Price::LessThanMarket(a),
                            }
                        } else {
                            match stop_loss {
                                Unit::Quantity(b) => Price::GreaterThanLimit(a, b),
                                _ => Price::GreaterThanMarket(a),
                            }
                        },
                        quantity,
                        margin,
                        append_margin: 0.0,
                    },
                ),
                (Unit::Quantity(a), Unit::Quantity(b)) => DelegateState::OpenProfitLoss(
                    Delegate {
                        side,
                        price,
                        quantity,
                        margin,
                        append_margin: 0.0,
                    },
                    Delegate {
                        side: if side == Side::EnterLong {
                            Side::ExitLong
                        } else {
                            Side::ExitShort
                        },
                        price: if side == Side::EnterLong {
                            match stop_profit {
                                Unit::Quantity(v) => Price::GreaterThanLimit(a, v),
                                _ => Price::GreaterThanMarket(a),
                            }
                        } else {
                            match stop_profit {
                                Unit::Quantity(v) => Price::LessThanLimit(a, v),
                                _ => Price::LessThanMarket(a),
                            }
                        },
                        quantity,
                        margin,
                        append_margin: 0.0,
                    },
                    Delegate {
                        side: if side == Side::EnterLong {
                            Side::ExitLong
                        } else {
                            Side::ExitShort
                        },
                        price: if side == Side::EnterLong {
                            match stop_loss {
                                Unit::Quantity(v) => Price::LessThanLimit(b, v),
                                _ => Price::LessThanMarket(b),
                            }
                        } else {
                            match stop_loss {
                                Unit::Quantity(v) => Price::GreaterThanLimit(b, v),
                                _ => Price::GreaterThanMarket(b),
                            }
                        },
                        quantity,
                        margin,
                        append_margin: 0.0,
                    },
                ),
                _ => DelegateState::Single(Delegate {
                    side,
                    price,
                    quantity,
                    margin,
                    append_margin: 0.0,
                }),
            };

            self.balance -= margin + fee;

            self.id += 1;

            delegate.push((self.id, ds));

            return Ok(self.id);
        }

        if let Some(position) = position {
            if side == Side::ExitLong && position.side == Side::EnterShort {
                anyhow::bail!(
                    "product {}: buy sell, but position side is sell short",
                    product,
                );
            }

            if side == Side::ExitShort && position.side == Side::EnterLong {
                anyhow::bail!(
                    "product {}: sell long, but position side is buy long",
                    product,
                );
            }

            let price = if price == 0.0 { k.close } else { price };

            // Order quantity
            let quantity = match quantity {
                Unit::Ignore => position.quantity,
                Unit::Quantity(v) => v,
                Unit::Proportion(v) => (position.quantity * v / *min_size).floor() * *min_size,
            };

            // The order quantity cannot be less than the minimum order quantity.
            if quantity < *min_size {
                anyhow::bail!(
                    "product {}: close quantity < min size: {} < {}",
                    product,
                    quantity,
                    min_size
                );
            }

            // Value of the order quantity
            let quantity_value = position.open_price * quantity;

            // The value of the order quantity cannot be less than the minimum order value
            if quantity_value < *min_notional {
                anyhow::bail!(
                    "product {}: close quantity value < min notional: {} < {}",
                    product,
                    quantity_value,
                    min_notional
                );
            }

            // The closing quantity must be less than the holding quantity
            if quantity > position.quantity {
                anyhow::bail!(
                    "product {}: close quantity > position quantity: {} > {}",
                    product,
                    quantity,
                    position.quantity,
                );
            };

            self.id += 1;

            delegate.push((
                self.id,
                DelegateState::Single(Delegate {
                    side,
                    price: if price >= k.close {
                        Price::GreaterThanMarket(price)
                    } else {
                        Price::LessThanMarket(price)
                    },
                    quantity,
                    margin: quantity / position.quantity * position.margin,
                    append_margin: 0.0,
                }),
            ));

            return Ok(self.id);
        }

        anyhow::bail!("no position: {}", product);
    }

    /// Cancel order.
    ///
    /// * `id`: Order ID.
    pub fn cancel(&mut self, id: u64) -> bool {
        let config = self.config;

        if id == 0 {
            for (_, message) in self.product.iter_mut() {
                for (_, delegate_state) in &message.delegate {
                    if let Some(delegate) = Self::refundable_open_delegate(delegate_state) {
                        self.balance += Self::reserved_open_funds(config, delegate);
                    }
                }
                message.delegate.clear();
            }
            return true;
        }

        for i in self.product.iter_mut() {
            if let Some(index) = i.1.delegate.iter().position(|v| v.0 == id) {
                if let Some(delegate) = Self::refundable_open_delegate(&i.1.delegate[index].1) {
                    self.balance += Self::reserved_open_funds(config, delegate);
                }
                i.1.delegate.remove(index);
                return true;
            }
        }

        false
    }

    /// Update.
    pub fn update(&mut self) {
        self.update_liquidation();
        self.update_close_delegate();
        self.update_open_delegate();
        self.update_profit_loss();
    }

    fn update_liquidation(&mut self) {
        for (.., Message { k, position, .. }) in self.product.iter_mut() {
            if position.is_none() {
                continue;
            }

            let current_position = position.as_mut().unwrap();

            if !(current_position.side == Side::EnterLong
                && k.low <= current_position.liquidation_price
                || current_position.side == Side::EnterShort
                    && k.high >= current_position.liquidation_price)
            {
                continue;
            }

            let record = Record {
                side: if current_position.side == Side::EnterLong {
                    Side::ExitLong
                } else {
                    Side::ExitShort
                },
                price: current_position.liquidation_price,
                quantity: current_position.quantity,
                margin: current_position.margin,
                fee: 0.0,
                profit: -current_position.margin,
                profit_ratio: -1.0,
                time: k.time,
            };

            current_position.log.push(record);

            self.history
                .push(new_history_position(position.take().unwrap()));
        }
    }

    fn update_close_delegate(&mut self) {
        let mut handle =
            |k: &K, delegate_state: &mut DelegateState, position: &mut Option<Position>| {
                let mut flag = 0;

                macro_rules! remove_or_convert {
                    () => {
                        match delegate_state {
                            DelegateState::Hedging(.., v) => {
                                *delegate_state = DelegateState::Single(*v);
                                false
                            }
                            DelegateState::HedgingProfit(.., a, b) => {
                                *delegate_state = DelegateState::OpenProfit(*a, *b);
                                false
                            }
                            DelegateState::HedgingLoss(.., a, b) => {
                                *delegate_state = DelegateState::OpenLoss(*a, *b);
                                false
                            }
                            DelegateState::HedgingProfitLoss(.., a, b, c) => {
                                *delegate_state = DelegateState::OpenProfitLoss(*a, *b, *c);
                                false
                            }
                            _ => true,
                        }
                    };
                }

                loop {
                    let delegate = match delegate_state {
                        DelegateState::Single(v)
                            if v.side == Side::ExitLong || v.side == Side::ExitShort =>
                        {
                            v
                        }
                        DelegateState::Hedging(v, ..)
                        | DelegateState::HedgingProfit(v, ..)
                        | DelegateState::HedgingLoss(v, ..)
                        | DelegateState::HedgingProfitLoss(v, ..) => v,
                        DelegateState::ProfitLoss(a, b) => {
                            if flag == 0 {
                                flag = 1;
                                b
                            } else if flag == 1 {
                                flag = 2;
                                a
                            } else {
                                return false;
                            }
                        }
                        _ => return false,
                    };

                    let current_position = if let Some(v) = position {
                        // If the order direction is not equal to the position direction, cancel the order, this is due to hedging positions.
                        if delegate.side == Side::ExitLong && v.side == Side::EnterShort
                            || delegate.side == Side::ExitShort && v.side == Side::EnterLong
                        {
                            return remove_or_convert!();
                        }

                        // If the quantity of the closing order is greater than the position quantity, cancel the order
                        if delegate.quantity > v.quantity {
                            return remove_or_convert!();
                        }

                        v
                    } else {
                        // If the position is forcibly closed, cancel the order
                        return remove_or_convert!();
                    };

                    if !match delegate.price {
                        Price::GreaterThanMarket(v) | Price::GreaterThanLimit(v, _) => k.high >= v,
                        Price::LessThanMarket(v) | Price::LessThanLimit(v, _) => k.low <= v,
                    } {
                        if flag == 1 {
                            continue;
                        }

                        return false;
                    }

                    match delegate.price {
                        Price::GreaterThanMarket(v) | Price::LessThanMarket(v) => {
                            // Limit order
                            let profit = if current_position.side == Side::EnterLong {
                                (v - current_position.open_price) * delegate.quantity
                            } else {
                                (current_position.open_price - v) * delegate.quantity
                            };

                            let record = Record {
                                side: delegate.side,
                                price: v,
                                quantity: delegate.quantity,
                                margin: delegate.margin + delegate.append_margin,
                                fee: v * delegate.quantity * self.config.close_fee,
                                profit,
                                profit_ratio: profit / delegate.margin,
                                time: k.time,
                            };

                            self.balance += record.profit + record.margin - record.fee;

                            current_position.quantity -= delegate.quantity;
                            current_position.margin -= delegate.margin;
                            current_position.log.push(record);

                            if current_position.quantity == 0.0 {
                                self.history
                                    .push(new_history_position(position.take().unwrap()));
                            }

                            return remove_or_convert!();
                        }
                        Price::GreaterThanLimit(a, b) | Price::LessThanLimit(a, b) => {
                            // Limit trigger, limit order
                            let temp = if delegate.side == Side::ExitLong && a <= b {
                                //                   C
                                //          B        |
                                // A        |        |
                                // |        |        |
                                // open  condition  price
                                Delegate {
                                    side: delegate.side,
                                    price: Price::GreaterThanMarket(b),
                                    quantity: delegate.quantity,
                                    margin: delegate.margin,
                                    append_margin: 0.0,
                                }
                            } else if delegate.side == Side::ExitLong {
                                //
                                //          B
                                // A        |        C
                                // |        |        |
                                // open  condition  price
                                Delegate {
                                    side: delegate.side,
                                    price: Price::LessThanMarket(b),
                                    quantity: delegate.quantity,
                                    margin: delegate.margin,
                                    append_margin: 0.0,
                                }
                            } else if delegate.side == Side::ExitShort && a >= b {
                                // A
                                // |        B
                                // |        |        C
                                // |        |        |
                                // open  condition  price
                                Delegate {
                                    side: delegate.side,
                                    price: Price::LessThanMarket(b),
                                    quantity: delegate.quantity,
                                    margin: delegate.margin,
                                    append_margin: 0.0,
                                }
                            } else {
                                // A                 C
                                // |        B        |
                                // |        |        |
                                // |        |        |
                                // open  condition  price
                                Delegate {
                                    side: delegate.side,
                                    price: Price::GreaterThanMarket(b),
                                    quantity: delegate.quantity,
                                    margin: delegate.margin,
                                    append_margin: 0.0,
                                }
                            };

                            if flag != 0 {
                                *delegate_state = DelegateState::Single(temp);
                            } else {
                                *delegate = temp;
                            }
                        }
                    }
                }
            };

        for (
            ..,
            Message {
                k,
                delegate,
                position,
                ..
            },
        ) in self.product.iter_mut()
        {
            let mut i = 0;

            while i < delegate.len() {
                if handle(k, &mut delegate[i].1, position) {
                    delegate.remove(i);
                } else {
                    i += 1;
                }
            }
        }
    }

    fn update_open_delegate(&mut self) {
        enum State {
            Next,
            Close(DelegateState),
            ReloadRemove,
            Remove,
        }

        let handle = |product: &String,
                      k: &K,
                      delegate_state: &mut DelegateState,
                      position: &mut Option<Position>| {
            let delegate = match delegate_state {
                DelegateState::Single(v)
                    if v.side == Side::EnterLong || v.side == Side::EnterShort =>
                {
                    v
                }
                DelegateState::OpenProfit(v, ..)
                | DelegateState::OpenLoss(v, ..)
                | DelegateState::OpenProfitLoss(v, ..) => v,
                _ => return State::Next,
            };

            if !match delegate.price {
                Price::GreaterThanMarket(v) | Price::GreaterThanLimit(v, _) => k.high >= v,
                Price::LessThanMarket(v) | Price::LessThanLimit(v, _) => k.low <= v,
            } {
                return State::Next;
            }

            let price = match delegate.price {
                Price::GreaterThanMarket(v) => v,
                Price::LessThanMarket(v) => v,
                Price::GreaterThanLimit(v, _) => v,
                Price::LessThanLimit(v, _) => v,
            };

            // Calculate the average opening price
            // New direction, new price, new position size, new margin, additional margin
            let (new_side, new_price, new_quantity, new_margin) = match position {
                Some(v) => {
                    if v.side == delegate.side {
                        // Add position
                        let quantity = v.quantity + delegate.quantity;

                        let open_price = ((v.open_price * v.quantity)
                            + (price * delegate.quantity))
                            / (v.quantity + delegate.quantity);

                        (
                            delegate.side,
                            open_price,
                            quantity,
                            v.margin + delegate.margin,
                        )
                    } else {
                        // Although the reduction of positions is processed during the order placement, it only occurs if there is an existing position
                        // Here, handling the scenario where multiple orders are executed simultaneously with different directions
                        return if v.quantity < delegate.quantity {
                            let new_margin = v.quantity / delegate.quantity * delegate.margin;
                            let sub_margin = delegate.margin - new_margin;
                            delegate.quantity -= v.quantity;
                            delegate.margin = new_margin;
                            State::Close(DelegateState::Single(Delegate {
                                side: if v.side == Side::EnterLong {
                                    Side::ExitLong
                                } else {
                                    Side::ExitShort
                                },
                                price: delegate.price,
                                quantity: v.quantity,
                                margin: v.margin,
                                append_margin: sub_margin,
                            }))
                        } else {
                            delegate.side = if v.side == Side::EnterLong {
                                Side::ExitLong
                            } else {
                                Side::ExitShort
                            };
                            delegate.append_margin = delegate.margin;
                            delegate.margin = delegate.quantity / v.quantity * v.margin;
                            *delegate_state = DelegateState::Single(*delegate);
                            State::ReloadRemove
                        };
                    }
                }
                _ => (delegate.side, price, delegate.quantity, delegate.margin),
            };

            // Calculate the taker fee to prevent shortfalls, i.e., situations where the balance is insufficient to cover the fee.
            // Long position liquidation price = Entry price × (1 - initial margin rate + maintenance margin rate) - (additional margin / position size) + taker fee
            // Short position liquidation price = Entry price × (1 + initial margin rate - maintenance margin rate) + (additional margin / position size) - taker fee
            // Initial margin rate = 1 / leverage
            // Additional margin = Account balance - initial margin
            // Initial margin = Entry price / leverage
            let liquidation_price = Self::calculate_liquidation_price(
                new_side,
                new_price,
                new_quantity,
                new_margin,
                self.config.lever,
                self.config.maintenance,
                self.config.close_fee,
            );

            let price = match delegate.price {
                Price::GreaterThanMarket(v) => v,
                Price::LessThanMarket(v) => v,
                Price::GreaterThanLimit(v, _) => v,
                Price::LessThanLimit(v, _) => v,
            };

            // Trading record
            let record = Record {
                side: delegate.side,
                price,
                quantity: delegate.quantity,
                margin: delegate.margin,
                fee: price * delegate.quantity * self.config.open_fee,
                profit: 0.0,
                profit_ratio: 0.0,
                time: k.time,
            };

            match position {
                Some(v) => {
                    // If a position already exists, modify the position directly
                    v.side = new_side;
                    v.open_price = new_price;
                    v.quantity = new_quantity;
                    v.margin = new_margin;
                    v.liquidation_price = liquidation_price;
                    v.log.push(record);
                }
                None => {
                    // Create a new position
                    let mut current_position = Position {
                        product: product.clone(),
                        lever: self.config.lever,
                        side: new_side,
                        open_price: new_price,
                        quantity: new_quantity,
                        margin: new_margin,
                        liquidation_price,
                        close_price: 0.0,
                        profit: 0.0,
                        profit_ratio: 0.0,
                        fee: 0.0,
                        open_time: k.time,
                        close_time: 0,
                        log: Vec::new(),
                    };

                    current_position.log.push(record);

                    position.replace(current_position);
                }
            };

            match delegate_state {
                DelegateState::OpenProfit(.., v) => {
                    *delegate_state = DelegateState::Single(*v);
                    State::Next
                }
                DelegateState::OpenLoss(.., v) => {
                    *delegate_state = DelegateState::Single(*v);
                    State::Next
                }
                DelegateState::OpenProfitLoss(.., a, b) => {
                    *delegate_state = DelegateState::ProfitLoss(*a, *b);
                    State::Next
                }
                _ => State::Remove,
            }
        };

        for (
            product,
            Message {
                k,
                delegate,
                position,
                ..
            },
        ) in self.product.iter_mut()
        {
            let mut i = 0;

            while i < delegate.len() {
                match handle(product, k, &mut delegate[i].1, position) {
                    State::Next => {
                        i += 1;
                    }
                    State::Close(v) => {
                        delegate.insert(0, (0, v));
                        self.update_close_delegate();
                        self.update_open_delegate();
                        return;
                    }
                    State::ReloadRemove => {
                        self.update_close_delegate();
                        self.update_open_delegate();
                        return;
                    }
                    State::Remove => {
                        delegate.remove(i);
                    }
                }
            }
        }
    }

    fn update_profit_loss(&mut self) {
        for (.., Message { k, position, .. }) in self.product.iter_mut() {
            if let Some(v) = position {
                let profit = if v.side == Side::EnterLong {
                    (k.close - v.open_price) * v.quantity
                } else {
                    (v.open_price - k.close) * v.quantity
                };
                v.profit = profit;
                v.profit_ratio = profit / v.margin
            }
        }
    }
}

/// Calculate position statistics based on log data.
///
/// * `Maximum position size`.
/// * `Maximum margin`.
/// * `Profit`.
/// * `Return on Investment`.
/// * `Commission Fee`.
/// * `Last closing price`.
/// * `Last closing time`.
fn new_history_position(mut position: Position) -> Position {
    position.profit = position.log.iter().map(|v| v.profit).sum();
    position.fee = position.log.iter().map(|v| v.fee).sum();
    let mut max_quantity = 0.0;
    let mut sum_quantity = 0.0;
    let mut max_margin = 0.0;
    let mut sum_margin = 0.0;

    position.log.iter().for_each(|v| {
        sum_quantity += if v.side == Side::EnterLong || v.side == Side::EnterShort {
            v.quantity
        } else {
            -v.quantity
        };

        if sum_quantity > max_quantity {
            max_quantity = sum_quantity;
        }

        sum_margin += if v.side == Side::EnterLong || v.side == Side::EnterShort {
            v.margin
        } else {
            -v.margin
        };

        if sum_margin > max_margin {
            max_margin = sum_margin;
        }
    });

    position.quantity = max_quantity;
    position.margin = max_margin;
    position.profit_ratio = if max_margin == 0.0 {
        0.0
    } else {
        position.profit / max_margin
    };
    position.close_price = position.log.last().unwrap().price;
    position.close_time = position.log.last().unwrap().time;
    position
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approx_eq(left: f32, right: f32) {
        assert!((left - right).abs() < 1e-4, "left={left}, right={right}");
    }

    fn sample_k(time: u64, price: f32) -> K {
        K {
            time,
            open: price,
            high: price,
            low: price,
            close: price,
        }
    }

    #[test]
    fn cancel_all_refunds_reserved_open_order_funds() {
        let mut engine =
            MatchEngine::new(Config::new().initial_margin(1000.0).lever(1).open_fee(0.01));
        engine.insert_product("BTC-USDT", 0.1, 0.0);
        engine.ready("BTC-USDT", sample_k(1, 100.0));

        let order_id = engine
            .order(
                "BTC-USDT",
                Side::EnterLong,
                0.0,
                Unit::Quantity(1.0),
                Unit::Quantity(100.0),
                Unit::Ignore,
                Unit::Ignore,
                Unit::Ignore,
                Unit::Ignore,
            )
            .unwrap();

        approx_eq(engine.balance(), 899.0);
        assert!(engine.cancel(0));
        approx_eq(engine.balance(), 1000.0);
        assert!(engine.delegate(order_id).is_none());
    }

    #[test]
    fn proportional_close_uses_position_size_instead_of_price() {
        let mut engine = MatchEngine::new(Config::new().initial_margin(1000.0).lever(1));
        engine.insert_product("BTC-USDT", 0.1, 0.0);
        engine.ready("BTC-USDT", sample_k(1, 10.0));

        engine
            .order(
                "BTC-USDT",
                Side::EnterLong,
                0.0,
                Unit::Quantity(2.0),
                Unit::Quantity(20.0),
                Unit::Ignore,
                Unit::Ignore,
                Unit::Ignore,
                Unit::Ignore,
            )
            .unwrap();
        engine.update();

        let position = engine.position("BTC-USDT").unwrap();
        approx_eq(position.quantity, 2.0);
        approx_eq(position.margin, 20.0);

        engine.ready("BTC-USDT", sample_k(2, 10.0));
        let order_id = engine
            .order(
                "BTC-USDT",
                Side::ExitLong,
                0.0,
                Unit::Proportion(0.5),
                Unit::Ignore,
                Unit::Ignore,
                Unit::Ignore,
                Unit::Ignore,
                Unit::Ignore,
            )
            .unwrap();

        match engine.delegate(order_id) {
            Some(DelegateState::Single(delegate)) => {
                approx_eq(delegate.quantity, 1.0);
                approx_eq(delegate.margin, 10.0);
            }
            other => panic!("unexpected delegate state: {other:?}"),
        }
    }

    #[test]
    fn liquidation_price_uses_fee_rate_instead_of_absolute_fee() {
        let mut engine =
            MatchEngine::new(Config::new().initial_margin(1000.0).lever(2).close_fee(0.1));
        engine.insert_product("BTC-USDT", 0.1, 0.0);
        engine.ready("BTC-USDT", sample_k(1, 100.0));

        engine
            .order(
                "BTC-USDT",
                Side::EnterLong,
                0.0,
                Unit::Quantity(1.0),
                Unit::Quantity(50.0),
                Unit::Ignore,
                Unit::Ignore,
                Unit::Ignore,
                Unit::Ignore,
            )
            .unwrap();
        engine.update();

        let position = engine.position("BTC-USDT").unwrap();
        approx_eq(position.liquidation_price, 55.555557);
    }

    #[test]
    fn scaling_in_without_extra_margin_keeps_liquidation_consistent() {
        let mut engine = MatchEngine::new(Config::new().initial_margin(1000.0).lever(2));
        engine.insert_product("BTC-USDT", 0.1, 0.0);

        engine.ready("BTC-USDT", sample_k(1, 100.0));
        engine
            .order(
                "BTC-USDT",
                Side::EnterLong,
                0.0,
                Unit::Quantity(1.0),
                Unit::Quantity(50.0),
                Unit::Ignore,
                Unit::Ignore,
                Unit::Ignore,
                Unit::Ignore,
            )
            .unwrap();
        engine.update();

        engine.ready("BTC-USDT", sample_k(2, 200.0));
        engine
            .order(
                "BTC-USDT",
                Side::EnterLong,
                0.0,
                Unit::Quantity(1.0),
                Unit::Quantity(100.0),
                Unit::Ignore,
                Unit::Ignore,
                Unit::Ignore,
                Unit::Ignore,
            )
            .unwrap();
        engine.update();

        let position = engine.position("BTC-USDT").unwrap();
        approx_eq(position.open_price, 150.0);
        approx_eq(position.margin, 150.0);
        approx_eq(position.liquidation_price, 75.0);
    }

    #[test]
    fn closed_position_profit_ratio_uses_total_profit_over_max_margin() {
        let mut engine = MatchEngine::new(Config::new().initial_margin(1000.0).lever(1));
        engine.insert_product("BTC-USDT", 1.0, 0.0);

        engine.ready("BTC-USDT", sample_k(1, 100.0));
        engine
            .order(
                "BTC-USDT",
                Side::EnterLong,
                0.0,
                Unit::Quantity(2.0),
                Unit::Quantity(200.0),
                Unit::Ignore,
                Unit::Ignore,
                Unit::Ignore,
                Unit::Ignore,
            )
            .unwrap();
        engine.update();

        engine.ready("BTC-USDT", sample_k(2, 150.0));
        engine
            .order(
                "BTC-USDT",
                Side::ExitLong,
                0.0,
                Unit::Quantity(1.0),
                Unit::Ignore,
                Unit::Ignore,
                Unit::Ignore,
                Unit::Ignore,
                Unit::Ignore,
            )
            .unwrap();
        engine.update();

        engine.ready("BTC-USDT", sample_k(3, 150.0));
        engine
            .order(
                "BTC-USDT",
                Side::ExitLong,
                0.0,
                Unit::Quantity(1.0),
                Unit::Ignore,
                Unit::Ignore,
                Unit::Ignore,
                Unit::Ignore,
                Unit::Ignore,
            )
            .unwrap();
        engine.update();

        let history = engine.history();
        assert_eq!(history.len(), 1);
        approx_eq(history[0].profit, 100.0);
        approx_eq(history[0].margin, 200.0);
        approx_eq(history[0].profit_ratio, 0.5);
    }
}
