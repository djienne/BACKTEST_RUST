use std::fmt;
use std::ops::{Add, Div, Mul, Sub};
use std::str::FromStr;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Precision {
    F32,
    F64,
}

impl fmt::Display for Precision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Precision::F32 => f.write_str("f32"),
            Precision::F64 => f.write_str("f64"),
        }
    }
}

impl FromStr for Precision {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "f32" => Ok(Precision::F32),
            "f64" => Ok(Precision::F64),
            _ => Err(format!(
                "unsupported precision '{value}', expected 'f32' or 'f64'"
            )),
        }
    }
}

pub trait BacktestFloat:
    Copy
    + Send
    + Sync
    + PartialOrd
    + Add<Output = Self>
    + Sub<Output = Self>
    + Mul<Output = Self>
    + Div<Output = Self>
    + 'static
{
    const ZERO: Self;
    const ONE: Self;
    const NAN: Self;

    fn from_f32(value: f32) -> Self;
    fn from_usize(value: usize) -> Self;
    fn to_f64(self) -> f64;
    fn sqrt(self) -> Self;
    fn is_finite(self) -> bool;
}

impl BacktestFloat for f32 {
    const ZERO: Self = 0.0;
    const ONE: Self = 1.0;
    const NAN: Self = f32::NAN;

    fn from_f32(value: f32) -> Self {
        value
    }

    fn from_usize(value: usize) -> Self {
        value as f32
    }

    fn to_f64(self) -> f64 {
        self as f64
    }

    fn sqrt(self) -> Self {
        self.sqrt()
    }

    fn is_finite(self) -> bool {
        self.is_finite()
    }
}

impl BacktestFloat for f64 {
    const ZERO: Self = 0.0;
    const ONE: Self = 1.0;
    const NAN: Self = f64::NAN;

    fn from_f32(value: f32) -> Self {
        value as f64
    }

    fn from_usize(value: usize) -> Self {
        value as f64
    }

    fn to_f64(self) -> f64 {
        self
    }

    fn sqrt(self) -> Self {
        self.sqrt()
    }

    fn is_finite(self) -> bool {
        self.is_finite()
    }
}
