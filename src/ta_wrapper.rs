use super::utils;
use std::collections::HashMap;
use ta::indicators::ExponentialMovingAverage;
use ta::indicators::RelativeStrengthIndex;
use ta::Next;

/// Calculate EMA for a given vector of closing prices and a period.
/// Initial values before the period are met are set to NaN.
pub fn calculate_ema(close_prices: &[f32], period: usize) -> Vec<f32> {
    let close_prices2 = utils::convert_f32_to_f64(close_prices);

    let mut ema = ExponentialMovingAverage::new(period).expect("Failed to create EMA");
    let mut ema_values = vec![f64::NAN; close_prices2.len()]; // Initialize with NaN for the size of close_prices2

    for (i, price) in close_prices2.iter().enumerate() {
        let ema_value = ema.next(*price);
        ema_values[i] = if i < period - 1 { f64::NAN } else { ema_value };
    }

    return utils::convert_f64_to_f32(ema_values);
}

/// Calculate the Relative Strength Index (RSI)
pub fn calculate_rsi(close_prices: &[f32], period: usize) -> Vec<f32> {
    let close_prices2 = utils::convert_f32_to_f64(close_prices);

    let mut rsi = RelativeStrengthIndex::new(period).expect("Failed to create RSI");
    let mut rsi_values = vec![f64::NAN; close_prices2.len()]; // Initialize with NaN for the size of close_prices2

    for (i, price) in close_prices2.iter().enumerate() {
        let rsi_value = rsi.next(*price);
        rsi_values[i] = if i < period - 1 { f64::NAN } else { rsi_value };
    }

    return utils::convert_f64_to_f32(rsi_values);
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
