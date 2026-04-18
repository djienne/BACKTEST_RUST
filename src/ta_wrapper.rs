use crate::precision::BacktestFloat;

/// Calculate EMA for a given vector of closing prices and a period.
/// Initial values before the period are met are set to NaN.
pub fn calculate_ema<T: BacktestFloat>(close_prices: &[T], period: usize) -> Vec<T> {
    let mut ema_values = vec![T::NAN; close_prices.len()];

    if period == 0 || close_prices.is_empty() {
        return ema_values;
    }

    let alpha = T::from_f32(2.0) / (T::from_usize(period) + T::ONE);
    let mut ema = close_prices[0];

    for (index, &price) in close_prices.iter().enumerate() {
        if index == 0 {
            ema = price;
        } else {
            ema = alpha * price + (T::ONE - alpha) * ema;
        }

        if index + 1 >= period {
            ema_values[index] = ema;
        }
    }

    ema_values
}

/// Stores all needed indicators (EMA values for selected periods).
pub struct EMAStore<T> {
    period_min: usize,
    emas: Vec<Vec<T>>,
}

impl<T: BacktestFloat> EMAStore<T> {
    pub fn new(close_prices: &[T], period_min: usize, period_max: usize) -> Self {
        let mut emas = Vec::with_capacity(period_max.saturating_sub(period_min) + 1);
        for period in period_min..=period_max {
            emas.push(calculate_ema(close_prices, period));
        }
        EMAStore { period_min, emas }
    }

    /// Returns the precomputed EMA series for `period`, or `None` if `period`
    /// was outside the `[period_min, period_max]` range supplied at construction.
    pub fn get_ema(&self, period: usize) -> Option<&[T]> {
        period
            .checked_sub(self.period_min)
            .and_then(|index| self.emas.get(index))
            .map(Vec::as_slice)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ema_period_one_is_safe_and_matches_prices() {
        let values = calculate_ema(&[1.0_f32, 2.0, 3.0], 1);
        assert_eq!(values, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn ema_store_returns_none_for_periods_outside_range() {
        let store = EMAStore::<f32>::new(&[1.0, 2.0, 3.0], 2, 4);
        assert!(store.get_ema(1).is_none());
        assert!(store.get_ema(5).is_none());
        assert!(store.get_ema(2).is_some());
        assert!(store.get_ema(4).is_some());
    }
}
