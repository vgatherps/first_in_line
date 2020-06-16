use crate::signal_graph::graph_registrar::*;
use crate::signal_graph::security_index::*;

use std::collections::HashMap;

fn book_signal_name(sec: &Security) -> String {
    format!("book_{}_{}", sec.exchange, sec.product)
}

fn book_signal_from_security(sec: &Security) -> (String, SignalCall) {
    let signal_name = book_signal_name(sec);
    (
        signal_name,
        SignalCall {
            signal_name: "book_fair".to_string(),
            inputs: vec![("book".to_string(), NamedSignalType::Book(sec.clone()))]
                .into_iter()
                .collect(),
        },
    )
}

fn ema_signal_name(sec: &Security, which: &str, speed: &str) -> String {
    format!("ema_{}_{}_{}", which, speed, book_signal_name(sec))
}

fn ema_signal_from_security(sec: &Security, which: &str, speed: &str) -> (String, SignalCall) {
    let book_size_name = book_signal_name(sec);
    let signal_name = ema_signal_name(sec, which, speed);
    (
        signal_name,
        SignalCall {
            signal_name: "ema".to_string(),
            inputs: vec![(
                "input".to_string(),
                NamedSignalType::Consumer((book_size_name, which.to_string())),
            )]
            .into_iter()
            .collect(),
        },
    )
}

fn premium_signal_name(sec: &Security) -> String {
    format!("premium_{}", book_signal_name(sec))
}

fn premium_signal_from_security(sec: &Security) -> (String, SignalCall) {
    let fast_fair = ema_signal_name(sec, "fair", "fast");
    let slow_fair = ema_signal_name(sec, "fair", "slow");
    (
        premium_signal_name(sec),
        SignalCall {
            signal_name: "premium".to_string(),
            inputs: vec![
                (
                    "in1".to_string(),
                    NamedSignalType::Consumer((fast_fair, "output".to_string())),
                ),
                (
                    "in2".to_string(),
                    NamedSignalType::Consumer((slow_fair, "output".to_string())),
                ),
            ]
            .into_iter()
            .collect(),
        },
    )
}

pub fn generate_signal_list(securities: &[Security]) -> Vec<(String, SignalCall)> {
    let book_signals = securities.iter().map(book_signal_from_security);
    let ema_size_signals = securities
        .iter()
        .map(|sec| ema_signal_from_security(sec, "size", "slow"));
    let ema_fast_signals = securities
        .iter()
        .map(|sec| ema_signal_from_security(sec, "fair", "fast"));
    let ema_slow_signals = securities
        .iter()
        .map(|sec| ema_signal_from_security(sec, "fair", "slow"));
    let premium_signals = securities.iter().map(premium_signal_from_security);
    let aggregator = vec![(
        "aggregate".to_string(),
        SignalCall {
            signal_name: "aggregator".to_string(),
            inputs: vec![
                (
                    "fair_mids".to_string(),
                    NamedSignalType::Aggregate(
                        securities
                            .iter()
                            .map(|s| (premium_signal_name(s), "output".to_string()))
                            .collect(),
                    ),
                ),
                (
                    "fair_sizes".to_string(),
                    NamedSignalType::Aggregate(
                        securities
                            .iter()
                            .map(|s| (ema_signal_name(s, "size", "slow"), "output".to_string()))
                            .collect(),
                    ),
                ),
            ]
            .into_iter()
            .collect(),
        },
    )];

    let rval = book_signals
        .chain(aggregator.into_iter())
        .chain(ema_size_signals.into_iter())
        .chain(ema_fast_signals.into_iter())
        .chain(ema_slow_signals.into_iter())
        .chain(premium_signals.into_iter())
        .collect();
    rval
}

pub fn generate_inputs(securities: &[Security]) -> HashMap<String, String> {
    let book_inputs = securities.iter().map(|sec| {
        (
            book_signal_name(sec),
            "{
            \"score_denom\": 1.0,
            \"score_offset\": 0.1,
            \"dollars_out\": 10,
            \"levels_out\": 10
        }"
            .to_string(),
        )
    });
    let ema_size_inputs = securities.iter().map(|sec| {
        (
            ema_signal_name(sec, "size", "slow"),
            "{
                    \"ratio\": 0.001
                }"
            .to_string(),
        )
    });
    let ema_fast_inputs = securities.iter().map(|sec| {
        (
            ema_signal_name(sec, "fair", "fast"),
            "{
                    \"ratio\": 0.07
                }"
            .to_string(),
        )
    });
    let ema_slow_inputs = securities.iter().map(|sec| {
        (
            ema_signal_name(sec, "fair", "slow"),
            "{
                    \"ratio\": 0.01
                }"
            .to_string(),
        )
    });

    book_inputs
        .chain(ema_size_inputs)
        .chain(ema_fast_inputs)
        .chain(ema_slow_inputs)
        .collect()
}
