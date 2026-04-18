//! Precision selection and float abstraction for the backtest sweep.
//!
//! The active precision is chosen at compile time via Cargo features:
//! `f32` (default) or `f64`. Exactly one must be enabled. The `Float` type
//! alias and `ACTIVE_PRECISION` constant resolve to the selected precision so
//! the rest of the crate stays generic-free at the binary surface.
//!
//! `BacktestFloat` remains a small in-house trait covering the f32/f64 ops
//! the sweep needs. We deliberately don't depend on `num-traits::Float` to
//! keep the dependency surface tight.

use std::fmt;
use std::ops::{Add, Div, Mul, Sub};

#[cfg(all(feature = "f32", feature = "f64"))]
compile_error!("Cargo features `f32` and `f64` are mutually exclusive — pick exactly one");

#[cfg(not(any(feature = "f32", feature = "f64")))]
compile_error!("Either feature `f32` or `f64` must be enabled (default is `f32`)");

#[cfg(all(feature = "f32", not(feature = "f64")))]
pub type Float = f32;

#[cfg(all(feature = "f64", not(feature = "f32")))]
pub type Float = f64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Precision {
    F32,
    F64,
}

#[cfg(all(feature = "f32", not(feature = "f64")))]
pub const ACTIVE_PRECISION: Precision = Precision::F32;

#[cfg(all(feature = "f64", not(feature = "f32")))]
pub const ACTIVE_PRECISION: Precision = Precision::F64;

impl fmt::Display for Precision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Precision::F32 => f.write_str("f32"),
            Precision::F64 => f.write_str("f64"),
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
