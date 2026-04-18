use crate::*;

struct IndexIter<'a> {
    k: &'a [K],
    strategy_k: &'a [K],
    k_index: usize,
    strategy_index: usize,
}

impl<'a> IndexIter<'a> {
    fn new(k: &'a [K], strategy_k: &'a [K]) -> Self {
        Self {
            k,
            strategy_k,
            k_index: k.len(),
            strategy_index: strategy_k.len(),
        }
    }
}

impl<'a> Iterator for IndexIter<'a> {
    type Item = (usize, usize);

    /// Get the starting and ending indices of the strategy K-lines after magnification.
    ///
    /// ```
    /// k = [1000, 900, 800, 700, 600, 500, 400, 300, 200, 100]
    /// strategy = [1000, 700, 400, 100]
    /// Some((7, 3))
    /// Some((4, 2))
    /// Some((1, 1))
    /// None
    /// ```
    fn next(&mut self) -> Option<Self::Item> {
        match self.strategy_k[..self.strategy_index] {
            [.., start, _] => {
                self.k_index = self.k[..self.k_index]
                    .iter()
                    .rposition(|v| v.time >= start.time)?;
                self.strategy_index -= 1;
                (self.k_index + 1 < self.k.len()).then_some((self.strategy_index, self.k_index + 1))
            }
            _ => None,
        }
    }
}

struct Scanner<'a> {
    iter: IndexIter<'a>,
    last: Option<(usize, usize)>,
}

impl<'a> Scanner<'a> {
    fn new(k: &'a [K], strategy_k: &'a [K]) -> Self {
        Self {
            iter: IndexIter::new(k, strategy_k),
            last: None,
        }
    }

    fn get(&mut self) -> Option<(usize, usize)> {
        self.last = self.last.or_else(|| self.iter.next());
        self.last
    }

    fn next(&mut self) {
        self.last = self.iter.next()
    }
}

/// Backtester.
pub struct Backtester<T> {
    exchange: T,
    config: Config,
}

impl<T> Backtester<T>
where
    T: Exchange,
{
    /// Construct the backtester.
    ///
    /// * `exchange` - The exchange.
    /// * `config` - Trading configuration.
    ///
    pub fn new(exchange: T, config: Config) -> Self {
        Self { exchange, config }
    }

    /// Begin backtesting.
    ///
    /// * `strategy` - The strategy.
    /// * `product` - Trading product, for example, spot BTC-USDT, futures BTC-USDT-SWAP.
    /// * `strategy_level` - The time level of the strategy, i.e., the time cycle for invoking the strategy.
    /// * `range` - Fetch data within this time range, in milliseconds, 0 means fetch all data, a..b means fetch data from range a to b.
    /// * `return` - The results of the backtest.
    ///
    pub async fn start<F, S, I>(
        &self,
        strategy: F,
        product: S,
        strategy_level: Level,
        range: I,
    ) -> anyhow::Result<Vec<Position>>
    where
        F: FnMut(&mut Context),
        S: AsRef<str>,
        I: Into<TimeRange>,
    {
        self.start_amplifier(strategy, product, strategy_level, strategy_level, range)
            .await
    }

    /// Begin backtesting.
    /// Fetches K-line data of `k_level` and strategy_level time levels from the exchange.
    ///
    /// * `strategy` - The strategy.
    /// * `product` - Trading product, for example, spot BTC-USDT, futures BTC-USDT-SWAP.
    /// * `k_level` - The time level of K-lines, the matching engine will process profits and losses, liquidations, and orders based on this time level.
    /// * `strategy_level` - The time level of the strategy, that is, the time period for invoking the strategy.
    /// * `range` - Fetch data within this time range, in milliseconds, 0 means fetch all data, a..b means fetch data from range a to b.
    /// * `return` - The results of the backtest.
    ///
    pub async fn start_amplifier<F, S, I>(
        &self,
        mut strategy: F,
        product: S,
        k_level: Level,
        strategy_level: Level,
        range: I,
    ) -> anyhow::Result<Vec<Position>>
    where
        F: FnMut(&mut Context),
        S: AsRef<str>,
        I: Into<TimeRange>,
    {
        struct TradingImpl {
            me: MatchEngine,
        }

        impl TradingImpl {
            fn new(config: Config) -> Self {
                Self {
                    me: MatchEngine::new(config),
                }
            }
        }

        impl Trading for TradingImpl {
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
            ) -> anyhow::Result<u64> {
                self.me.order(
                    product,
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

            fn cancel(&mut self, id: u64) -> bool {
                self.me.cancel(id)
            }

            fn balance(&self) -> f32 {
                self.me.balance()
            }

            fn delegate(&self, id: u64) -> Option<DelegateState> {
                self.me.delegate(id)
            }

            fn position(&self, product: &str) -> Option<&Position> {
                self.me.position(product)
            }
        }

        anyhow::ensure!(
            (k_level as u32) <= (strategy_level as u32),
            "product: {}: strategy level must be greater than k level",
            product.as_ref(),
        );

        let product = product.as_ref();
        let range = range.into();
        let min_size = self.exchange.get_min_size(product).await?;
        let min_notional = self.exchange.get_min_notional(product).await?;
        let k = get_k_range(&self.exchange, product, k_level, range).await?;
        let strategy_k;

        let strategy_k = if k_level == strategy_level {
            k.as_slice()
        } else {
            strategy_k = get_k_range(&self.exchange, product, strategy_level, range).await?;
            strategy_k.as_slice()
        };

        let open = strategy_k.iter().map(|v| v.open).collect::<Vec<_>>();
        let high = strategy_k.iter().map(|v| v.high).collect::<Vec<_>>();
        let low = strategy_k.iter().map(|v| v.low).collect::<Vec<_>>();
        let close = strategy_k.iter().map(|v| v.close).collect::<Vec<_>>();

        let mut scanner = Scanner::new(&k, strategy_k);
        let mut ti = TradingImpl::new(self.config);

        ti.me.insert_product(product, min_size, min_notional);

        for index in (0..k.len()).rev() {
            ti.me.ready(
                product,
                K {
                    time: k[index].time,
                    open: k[index].open,
                    high: k[index].high,
                    low: k[index].low,
                    close: k[index].close,
                },
            );

            if let Some((start_index, end_index)) = if k_level == strategy_level {
                Some((index, index))
            } else {
                scanner.get()
            } {
                if index == end_index {
                    let time = strategy_k[start_index].time;
                    let open = Source::new(&open[start_index..]);
                    let high = Source::new(&high[start_index..]);
                    let low = Source::new(&low[start_index..]);
                    let close = Source::new(&close[start_index..]);

                    let mut cx = Context {
                        product,
                        min_size,
                        min_notional,
                        level: strategy_level,
                        time,
                        open,
                        high,
                        low,
                        close,
                        trading: &mut ti,
                    };

                    strategy(&mut cx);

                    scanner.next()
                }
            }

            ti.me.update();
        }

        Ok(ti.me.history().clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candle(time: u64, price: f32) -> K {
        K {
            time,
            open: price,
            high: price,
            low: price,
            close: price,
        }
    }

    #[tokio::test]
    async fn backtester_executes_a_basic_open_then_close_strategy() {
        let exchange = LocalExchange::new().push(
            "BTC-USDT",
            Level::Hour1,
            vec![
                candle(3000, 120.0),
                candle(2000, 110.0),
                candle(1000, 100.0),
            ],
            1.0,
            0.0,
        );
        let backtester = Backtester::new(exchange, Config::new().initial_margin(1000.0).lever(1));

        let result = backtester
            .start(
                |cx| {
                    if cx.time == 1000 {
                        cx.order_quantity_margin(
                            Side::EnterLong,
                            0.0,
                            Unit::Quantity(1.0),
                            Unit::Quantity(100.0),
                        )
                        .unwrap();
                    } else if cx.time == 2000 {
                        cx.order(Side::ExitLong, 0.0).unwrap();
                    }
                },
                "BTC-USDT",
                Level::Hour1,
                0..4000,
            )
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].open_time, 1000);
        assert_eq!(result[0].close_time, 2000);
        assert_eq!(result[0].profit, 10.0);
    }

    #[tokio::test]
    async fn amplifier_runs_strategy_on_the_higher_timeframe() {
        let exchange = LocalExchange::new()
            .push(
                "BTC-USDT",
                Level::Minute1,
                vec![
                    candle(420000, 16.0),
                    candle(360000, 15.0),
                    candle(300000, 14.0),
                    candle(240000, 13.0),
                    candle(180000, 12.0),
                    candle(120000, 11.0),
                    candle(60000, 10.0),
                ],
                1.0,
                0.0,
            )
            .push(
                "BTC-USDT",
                Level::Minute3,
                vec![
                    candle(420000, 16.0),
                    candle(240000, 13.0),
                    candle(60000, 10.0),
                ],
                1.0,
                0.0,
            );
        let backtester = Backtester::new(exchange, Config::new().initial_margin(1000.0));
        let mut strategy_times = Vec::new();

        backtester
            .start_amplifier(
                |cx| strategy_times.push(cx.time),
                "BTC-USDT",
                Level::Minute1,
                Level::Minute3,
                0..480000,
            )
            .await
            .unwrap();

        assert_eq!(strategy_times, vec![60000, 240000]);
    }
}
