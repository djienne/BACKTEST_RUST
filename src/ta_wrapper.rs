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

/// Calculate the Relative Strength Index (RSI)
#[cfg(test)]
pub fn calculate_rsi(close_prices: &[f32], period: usize) -> Vec<f32> {
    let mut rsi_values = vec![f32::NAN; close_prices.len()];

    if period == 0 || close_prices.len() <= period {
        return rsi_values;
    }

    let mut average_gain = 0.0;
    let mut average_loss = 0.0;

    for index in 1..=period {
        let delta = close_prices[index] - close_prices[index - 1];
        if delta >= 0.0 {
            average_gain += delta;
        } else {
            average_loss -= delta;
        }
    }

    average_gain /= period as f32;
    average_loss /= period as f32;
    rsi_values[period] = compute_rsi(average_gain, average_loss);

    for index in period + 1..close_prices.len() {
        let delta = close_prices[index] - close_prices[index - 1];
        let gain = delta.max(0.0);
        let loss = (-delta).max(0.0);

        average_gain = (average_gain * (period as f32 - 1.0) + gain) / period as f32;
        average_loss = (average_loss * (period as f32 - 1.0) + loss) / period as f32;
        rsi_values[index] = compute_rsi(average_gain, average_loss);
    }

    rsi_values
}

#[cfg(test)]
fn compute_rsi(average_gain: f32, average_loss: f32) -> f32 {
    if average_loss == 0.0 {
        100.0
    } else {
        let relative_strength = average_gain / average_loss;
        100.0 - 100.0 / (1.0 + relative_strength)
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// Stuctures containing all needed indicators (selected periods)
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

    pub fn get_ema(&self, period: usize) -> &[T] {
        period
            .checked_sub(self.period_min)
            .and_then(|index| self.emas.get(index))
            .map(Vec::as_slice)
            .unwrap_or(&[])
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
    fn rsi_period_one_is_safe() {
        let values = calculate_rsi(&[1.0, 2.0, 3.0], 1);

        assert!(values[0].is_nan());
        assert_eq!(values[1], 100.0);
        assert_eq!(values[2], 100.0);
    }
}
