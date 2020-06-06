#![allow(warnings)]
#[macro_use]
use arby::{make_book_callback, make_consumer_callback, make_aggregate_callback};
use arby::order_book::*;
use arby::signal_graph::graph_registrar::*;
use arby::signal_graph::interface_types::*;
use arby::signal_graph::security_index::{Security, SecurityMap, SmallString};

use std::collections::HashMap;

struct DummyBookSignal {
    current_val: f64,
}

struct DummyConsumerSignal {
    current_val: f64,
}

struct DummyAggregateSignal {}

fn handle_book(signal: &mut DummyBookSignal, update: BookUpdate, time: u64) -> f64 {
    signal.current_val += 1.0;
    signal.current_val
}

fn handle_consumer(signal: &mut DummyConsumerSignal, value: f64, time: u64) -> f64 {
    signal.current_val = value;
    value
}

fn handle_aggregate(_: &mut DummyAggregateSignal, update: AggregateUpdate, time: u64) -> f64 {
    assert_eq!(update.iter_changed().count(), 3);
    update.iter_changed().map(|(_, val)| val).sum()
}

impl RegisterSignal for DummyBookSignal {
    type Child = DummyBookSignal;

    fn get_signals() -> HashMap<&'static str, Signal> {
        let signals = vec![("input", make_book_callback!(handle_book, DummyBookSignal))];
        signals.into_iter().collect()
    }

    fn create(
        _: HashMap<&'static str, ConsumerSignal>,
        _: HashMap<&'static str, Vec<AggregateSignal>>,
    ) -> DummyBookSignal {
        DummyBookSignal { current_val: 0.0 }
    }
}

impl RegisterSignal for DummyConsumerSignal {
    type Child = DummyConsumerSignal;
    fn get_signals() -> HashMap<&'static str, Signal> {
        let signals = vec![(
            "input",
            make_consumer_callback!(handle_consumer, DummyConsumerSignal),
        )];
        signals.into_iter().collect()
    }

    fn create(
        _: HashMap<&'static str, ConsumerSignal>,
        _: HashMap<&'static str, Vec<AggregateSignal>>,
    ) -> DummyConsumerSignal {
        DummyConsumerSignal { current_val: 0.0 }
    }
}

impl RegisterSignal for DummyAggregateSignal {
    type Child = DummyAggregateSignal;
    fn get_signals() -> HashMap<&'static str, Signal> {
        let signals = vec![(
            "input",
            make_aggregate_callback!(handle_aggregate, DummyAggregateSignal),
        )];
        signals.into_iter().collect()
    }
    fn create(
        _: HashMap<&'static str, ConsumerSignal>,
        _: HashMap<&'static str, Vec<AggregateSignal>>,
    ) -> DummyAggregateSignal {
        DummyAggregateSignal {}
    }
}

#[test]
fn construct_graph() {
    let signals = vec![
        ("dummy_book", DummyBookSignal::get_definition()),
        ("dummy_signal", DummyConsumerSignal::get_definition()),
    ];

    let registrar = GraphRegistrar::new(&signals).unwrap();

    let btc = Security {
        product: SmallString::from_str("BTCXBT"),
        exchange: SmallString::from_str("BITMEX"),
    };

    let sec_map = unsafe { SecurityMap::new_unchecked(&[btc.clone()]) };

    let layout_vec = vec![
        (
            "book".to_string(),
            SignalCall {
                signal_name: "dummy_book".to_string(),
                inputs: vec![("input".to_string(), NamedSignalType::Book(btc.clone()))]
                    .into_iter()
                    .collect(),
            },
        ),
        (
            "consumer1".to_string(),
            SignalCall {
                signal_name: "dummy_signal".to_string(),
                inputs: vec![(
                    "input".to_string(),
                    NamedSignalType::Inner(false, vec!["book".to_string()]),
                )]
                .into_iter()
                .collect(),
            },
        ),
        (
            "consumer2".to_string(),
            SignalCall {
                signal_name: "dummy_signal".to_string(),
                inputs: vec![(
                    "input".to_string(),
                    NamedSignalType::Inner(false, vec!["book".to_string()]),
                )]
                .into_iter()
                .collect(),
            },
        ),
        (
            "consumer3".to_string(),
            SignalCall {
                signal_name: "dummy_signal".to_string(),
                inputs: vec![(
                    "input".to_string(),
                    NamedSignalType::Inner(false, vec!["book".to_string()]),
                )]
                .into_iter()
                .collect(),
            },
        ),
    ];

    let mut graph = registrar.generate_graph(&layout_vec, &sec_map).unwrap();

    let book = graph.signal_listener("book").unwrap();
    let consumer1 = graph.signal_listener("consumer1").unwrap();
    let consumer2 = graph.signal_listener("consumer2").unwrap();
    let consumer3 = graph.signal_listener("consumer3").unwrap();

    let dummy_bbo = BookState {
        bbo: Bbo::default(),
        book: OrderBook::new(),
    };

    let update = BookUpdate {
        book: &dummy_bbo,
        updated_mask: BookUpdateMask::default(),
    };

    graph.trigger_book(update, sec_map.to_index(&btc).unwrap(), 0);
    graph.trigger_book(update, sec_map.to_index(&btc).unwrap(), 0);
    graph.trigger_book(update, sec_map.to_index(&btc).unwrap(), 0);
    graph.trigger_book(update, sec_map.to_index(&btc).unwrap(), 0);
    graph.trigger_book(update, sec_map.to_index(&btc).unwrap(), 0);
}
