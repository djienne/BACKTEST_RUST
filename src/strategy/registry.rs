//! Name-to-strategy dispatch.
//!
//! The point of this module is that `main.rs` never mentions a concrete
//! strategy. [`run_named`] returns a [`StrategyReport`] whose parameters are
//! already a string, which erases `S::Params` and lets one call site print and
//! record the result of any strategy.
//!
//! # Adding a strategy
//!
//! 1. Implement [`Strategy`] and [`ConfigurableStrategy`] in a new module.
//! 2. Add one line to the `registry!` invocation below.
//!
//! That is the whole change — no edits to the engine, the CLI or the output
//! layer.

use crate::backtest::{run, BacktestMetrics, EngineConfig};
use crate::data::CandleSeries;
use crate::precision::Precision;
use crate::strategy::params::ParamSpec;
use crate::strategy::{ConfigurableStrategy, Strategy};
use anyhow::{Context, Result};
use std::time::Duration;

/// A finished run with its strategy-specific types erased.
#[derive(Debug, Clone, PartialEq)]
pub struct StrategyReport {
    pub name: &'static str,
    /// The winning parameters, already rendered by `Strategy::param_summary`.
    pub params: String,
    pub metrics: BacktestMetrics,
    pub out_of_sample: Option<BacktestMetrics>,
    pub precision: Precision,
    pub duration: Duration,
}

/// What `list-strategies` and `--help` print.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StrategyInfo {
    pub name: &'static str,
    pub description: &'static str,
    pub parameters: &'static [(&'static str, &'static str)],
}

/// Run one strategy generically and erase its parameter type.
fn dispatch<S: ConfigurableStrategy>(
    engine: &EngineConfig,
    spec: &ParamSpec,
    market: &CandleSeries,
) -> Result<StrategyReport> {
    let config = S::config_from(spec)
        .with_context(|| format!("invalid parameters for strategy '{}'", S::NAME))?;
    let outcome = run::<S>(engine, &config, market)?;
    Ok(StrategyReport {
        name: S::NAME,
        params: S::param_summary(outcome.best.params),
        metrics: outcome.best.metrics,
        out_of_sample: outcome.out_of_sample,
        precision: outcome.precision,
        duration: outcome.duration,
    })
}

macro_rules! registry {
    ($($strategy:ty),+ $(,)?) => {
        /// Every strategy the CLI can run, in listing order.
        pub fn available() -> Vec<StrategyInfo> {
            vec![$(
                StrategyInfo {
                    name: <$strategy>::NAME,
                    description: <$strategy>::DESCRIPTION,
                    parameters: <$strategy>::PARAMETERS,
                },
            )+]
        }

        /// Run the named strategy. Unknown names list the valid ones.
        pub fn run_named(
            name: &str,
            engine: &EngineConfig,
            spec: &ParamSpec,
            market: &CandleSeries,
        ) -> Result<StrategyReport> {
            match name {
                $(
                    <$strategy>::NAME => dispatch::<$strategy>(engine, spec, market),
                )+
                other => anyhow::bail!(
                    "unknown strategy '{other}'. Available: {}",
                    available()
                        .iter()
                        .map(|info| info.name)
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            }
        }
    };
}

registry![
    crate::strategy::double_ema::DoubleEmaCrossover,
    crate::strategy::single_ema::PriceVsEma,
    crate::strategy::rsi_reversion::RsiReversion,
    crate::strategy::stoch_rsi::StochRsi,
    crate::strategy::bb_reversion::BollingerReversion,
    crate::strategy::macd_cross::MacdCross,
    crate::strategy::supertrend::SupertrendFollow,
];

/// The default strategy when `--strategy` is not given.
pub const DEFAULT_STRATEGY: &str = crate::strategy::double_ema::DoubleEmaCrossover::NAME;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backtest::ExecutionModel;
    use crate::exchange::Level;
    use std::borrow::Cow;

    fn engine() -> EngineConfig {
        EngineConfig {
            pair: Cow::Borrowed("TEST-USDT"),
            level: Level::Hour1,
            threads: 2,
            starting_capital: 1000.0,
            fee_rate: 0.0015,
            risk_free_rate: 0.0,
            execution_model: ExecutionModel::NextOpen,
            show_progress: false,
            progress_step: usize::MAX,
            download_start: 0,
            split: None,
        }
    }

    fn market(n: usize) -> CandleSeries {
        let mut series = CandleSeries {
            timestamps: Vec::new(),
            open_prices: Vec::new(),
            high_prices: Vec::new(),
            low_prices: Vec::new(),
            close_prices: Vec::new(),
            volumes: Vec::new(),
        };
        for i in 0..n {
            let close = 100.0 + ((i as f32) * 0.15).sin() * 12.0 + (i as f32) * 0.02;
            series.timestamps.push(i as u64 * 3_600_000);
            series.open_prices.push(close + 0.1);
            series.high_prices.push(close + 1.0);
            series.low_prices.push(close - 1.0);
            series.close_prices.push(close);
            series.volumes.push(1_000.0);
        }
        series
    }

    /// Narrow every strategy's grid so the whole registry runs quickly.
    fn narrow_spec(name: &str) -> ParamSpec {
        let entries: &[&str] = match name {
            "double_ema" | "price_vs_ema" => &[],
            "rsi_reversion" => &["period=5..7", "oversold=30", "overbought=70"],
            "stoch_rsi" => &["stoch_period=5..7", "oversold=20", "overbought=80"],
            "bb_reversion" => &["period=10..12", "deviation_tenths=20"],
            "macd_cross" => &["fast=8..9", "slow=20..21", "signal=9"],
            "supertrend" => &["period=7..8", "multiplier_tenths=20..21"],
            other => panic!("no narrow spec for '{other}' — add one when registering it"),
        };
        ParamSpec::parse(entries).unwrap()
    }

    #[test]
    fn every_registered_strategy_runs_end_to_end() {
        // The test that makes the registry trustworthy: a strategy added to
        // the macro but broken cannot pass this.
        let market = market(400);
        for info in available() {
            let report = run_named(info.name, &engine(), &narrow_spec(info.name), &market)
                .unwrap_or_else(|e| panic!("strategy '{}' failed: {e:#}", info.name));
            assert_eq!(report.name, info.name);
            assert!(
                !report.params.is_empty(),
                "'{}' reported no parameters",
                info.name
            );
            assert!(
                report.metrics.final_value.is_finite(),
                "'{}' produced a non-finite result",
                info.name
            );
            assert!(report.out_of_sample.is_none(), "no split requested");
        }
    }

    #[test]
    fn every_registered_strategy_has_usable_help() {
        for info in available() {
            assert!(!info.description.is_empty(), "'{}'", info.name);
            for (parameter, help) in info.parameters {
                assert!(
                    !parameter.is_empty() && !help.is_empty(),
                    "'{}' documents a parameter badly",
                    info.name
                );
            }
        }
    }

    /// Registering a strategy without documenting it is the most likely way
    /// for the README to fall behind, so it fails the build instead.
    #[test]
    fn the_readme_lists_every_registered_strategy() {
        let readme = include_str!("../../README.md");
        for info in available() {
            assert!(
                readme.contains(info.name),
                "README.md does not mention the '{}' strategy",
                info.name
            );
            for (parameter, _) in info.parameters {
                assert!(
                    readme.contains(parameter),
                    "README.md does not mention '{}''s --param {parameter}",
                    info.name
                );
            }
        }
    }

    #[test]
    fn strategy_names_are_unique() {
        let mut names: Vec<&str> = available().iter().map(|info| info.name).collect();
        let before = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), before, "two strategies share a name");
    }

    #[test]
    fn the_default_strategy_is_registered() {
        assert!(available().iter().any(|info| info.name == DEFAULT_STRATEGY));
    }

    #[test]
    fn an_unknown_name_lists_the_alternatives() {
        let error = run_named("nope", &engine(), &ParamSpec::default(), &market(50))
            .expect_err("unknown strategies must not run");
        let message = format!("{error:#}");
        assert!(message.contains("nope"), "{message}");
        assert!(message.contains(DEFAULT_STRATEGY), "{message}");
    }

    #[test]
    fn a_bad_parameter_is_reported_against_its_strategy() {
        let spec = ParamSpec::parse(["oversld=30"]).unwrap();
        let error = run_named("rsi_reversion", &engine(), &spec, &market(50))
            .expect_err("a typo must not be ignored");
        let message = format!("{error:#}");
        assert!(message.contains("rsi_reversion"), "{message}");
        assert!(message.contains("oversld"), "{message}");
    }

    #[test]
    fn a_split_run_reports_held_out_metrics_for_any_strategy() {
        let mut engine = engine();
        engine.split = Some(0.7);
        let report = run_named(
            "rsi_reversion",
            &engine,
            &narrow_spec("rsi_reversion"),
            &market(400),
        )
        .unwrap();
        assert!(report.out_of_sample.is_some());
    }
}
