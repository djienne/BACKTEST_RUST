//! `--param name=value` parsing.
//!
//! Values are kept as strings and interpreted by whichever strategy reads
//! them, so a strategy declares the type it wants (`scalar`, `decimal`,
//! `range`) rather than the parser guessing. Unknown names are an error, not a
//! shrug: a typo in `--param perod=14` that silently used the default would be
//! indistinguishable from a bad strategy.

use anyhow::{bail, Context, Result};
use std::collections::BTreeMap;
use std::ops::RangeInclusive;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParamSpec {
    values: BTreeMap<String, String>,
}

impl ParamSpec {
    /// Parse `name=value` entries. A repeated name is an error rather than a
    /// silent last-one-wins.
    pub fn parse<I, S>(entries: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut values = BTreeMap::new();
        for entry in entries {
            let entry = entry.as_ref();
            let (name, value) = entry
                .split_once('=')
                .with_context(|| format!("invalid --param '{entry}'; expected name=value"))?;
            let name = name.trim().to_ascii_lowercase();
            if name.is_empty() {
                bail!("invalid --param '{entry}'; the name is empty");
            }
            if values
                .insert(name.clone(), value.trim().to_string())
                .is_some()
            {
                bail!("--param '{name}' was given more than once");
            }
        }
        Ok(Self { values })
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Fail if any supplied name is not in `known`, listing what was expected.
    pub fn reject_unknown(&self, known: &[&str]) -> Result<()> {
        let unknown: Vec<&str> = self
            .values
            .keys()
            .map(String::as_str)
            .filter(|name| !known.contains(name))
            .collect();
        if !unknown.is_empty() {
            bail!(
                "unknown --param name(s): {}. This strategy accepts: {}",
                unknown.join(", "),
                known.join(", ")
            );
        }
        Ok(())
    }

    /// A whole-number parameter, e.g. `period=14`.
    pub fn scalar(&self, name: &str, default: usize) -> Result<usize> {
        match self.values.get(name) {
            None => Ok(default),
            Some(raw) => raw
                .parse()
                .with_context(|| format!("--param {name}='{raw}': expected a whole number")),
        }
    }

    /// A fractional parameter, e.g. `multiplier=2.5`.
    pub fn decimal(&self, name: &str, default: f64) -> Result<f64> {
        match self.values.get(name) {
            None => Ok(default),
            Some(raw) => raw
                .parse()
                .with_context(|| format!("--param {name}='{raw}': expected a number")),
        }
    }

    /// An inclusive range to sweep, written `min..max`. A bare number is
    /// accepted and means a range of one, so `--param period=14` pins a value
    /// that would otherwise be swept.
    pub fn range(
        &self,
        name: &str,
        default: RangeInclusive<usize>,
    ) -> Result<RangeInclusive<usize>> {
        let Some(raw) = self.values.get(name) else {
            return Ok(default);
        };
        let (min, max) = match raw.split_once("..") {
            Some((min, max)) => (min.trim(), max.trim()),
            None => (raw.as_str(), raw.as_str()),
        };
        let parse = |text: &str| -> Result<usize> {
            text.parse().with_context(|| {
                format!("--param {name}='{raw}': expected a whole number or min..max")
            })
        };
        let (min, max) = (parse(min)?, parse(max)?);
        if min > max {
            bail!("--param {name}='{raw}': the range start exceeds its end");
        }
        if min == 0 {
            bail!("--param {name}='{raw}': periods must be at least 1");
        }
        Ok(min..=max)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_reads_name_value_pairs() {
        let spec = ParamSpec::parse(["period=14", "MULT=2.5"]).unwrap();
        assert_eq!(spec.scalar("period", 0).unwrap(), 14);
        assert_eq!(spec.decimal("mult", 0.0).unwrap(), 2.5, "names fold case");
    }

    #[test]
    fn parse_rejects_malformed_entries() {
        assert!(ParamSpec::parse(["period"]).is_err(), "no '='");
        assert!(ParamSpec::parse(["=14"]).is_err(), "no name");
        assert!(
            ParamSpec::parse(["period=1", "period=2"]).is_err(),
            "a repeated name is more likely a mistake than an override"
        );
    }

    #[test]
    fn defaults_apply_when_a_name_is_absent() {
        let spec = ParamSpec::default();
        assert_eq!(spec.scalar("period", 14).unwrap(), 14);
        assert_eq!(spec.decimal("mult", 3.0).unwrap(), 3.0);
        assert_eq!(spec.range("period", 5..=20).unwrap(), 5..=20);
        assert!(spec.is_empty());
    }

    #[test]
    fn range_accepts_min_max_and_a_bare_value() {
        let spec = ParamSpec::parse(["a=5..100", "b=30"]).unwrap();
        assert_eq!(spec.range("a", 1..=2).unwrap(), 5..=100);
        assert_eq!(
            spec.range("b", 1..=2).unwrap(),
            30..=30,
            "a bare number pins the value"
        );
    }

    #[test]
    fn range_rejects_nonsense() {
        let spec = ParamSpec::parse(["a=100..5", "b=0..10", "c=x..y"]).unwrap();
        assert!(spec.range("a", 1..=2).is_err(), "inverted");
        assert!(spec.range("b", 1..=2).is_err(), "zero period");
        assert!(spec.range("c", 1..=2).is_err(), "not numbers");
    }

    #[test]
    fn scalar_and_decimal_report_the_offending_value() {
        let spec = ParamSpec::parse(["period=lots"]).unwrap();
        let message = format!("{:#}", spec.scalar("period", 1).unwrap_err());
        assert!(message.contains("period='lots'"), "got: {message}");
    }

    #[test]
    fn reject_unknown_catches_a_typo_and_says_what_was_expected() {
        let spec = ParamSpec::parse(["perod=14"]).unwrap();
        let error = spec
            .reject_unknown(&["period", "oversold"])
            .expect_err("a typo must not silently fall back to defaults");
        let message = format!("{error:#}");
        assert!(message.contains("perod"), "names the typo: {message}");
        assert!(
            message.contains("period"),
            "and the alternatives: {message}"
        );
        assert!(ParamSpec::parse(["period=14"])
            .unwrap()
            .reject_unknown(&["period"])
            .is_ok());
    }
}
