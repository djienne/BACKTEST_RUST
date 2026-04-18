use std::collections::HashMap;

/// Calculate EMA for a given vector of closing prices and a period.
/// Initial values before the period are met are set to NaN.
pub fn calculate_ema(close_prices: &[f32], period: usize) -> Vec<f32> {
    let mut ema_values = vec![f32::NAN; close_prices.len()];

    if period == 0 || close_prices.is_empty() {
        return ema_values;
    }

    let alpha = 2.0 / (period as f32 + 1.0);
    let mut ema = close_prices[0];

    for (index, &price) in close_prices.iter().enumerate() {
        if index == 0 {
            ema = price;
        } else {
            ema = alpha * price + (1.0 - alpha) * ema;
        }

        if index + 1 >= period {
            ema_values[index] = ema;
        }
    }

    ema_values
}

/// Calculate the Relative Strength Index (RSI)
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
pub struct EMAStore {
    emas: HashMap<usize, Vec<f32>>,
}

impl EMAStore {
    pub fn new(close_prices: &[f32], period_min: usize, period_max: usize) -> Self {
        let mut emas = HashMap::new();
        for period in period_min..=period_max {
            emas.insert(period, calculate_ema(close_prices, period));
        }
        EMAStore { emas }
    }

    pub fn get_ema(&self, period: usize) -> &[f32] {
        self.emas.get(&period).map(Vec::as_slice).unwrap_or(&[])
    }
}

//

pub struct RSIStore {
    rsis: HashMap<usize, Vec<f32>>,
}

impl RSIStore {
    pub fn new(close_prices: &[f32], period_min: usize, period_max: usize) -> Self {
        let mut rsis = HashMap::new();
        for period in period_min..=period_max {
            rsis.insert(period, calculate_rsi(close_prices, period));
        }
        RSIStore { rsis }
    }

    pub fn get_rsi(&self, period: usize) -> &[f32] {
        self.rsis.get(&period).map(Vec::as_slice).unwrap_or(&[])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ema_period_one_is_safe_and_matches_prices() {
        let values = calculate_ema(&[1.0, 2.0, 3.0], 1);

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
