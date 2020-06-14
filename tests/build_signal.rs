#![allow(warnings)]
#[macro_use]
use arby::order_book::*;
use arby::signal_graph::graph_registrar::*;
use arby::signal_graph::interface_types::*;
use arby::signal_graph::security_index::{Security, SecurityMap, SmallString};

use std::collections::{HashMap, HashSet};

struct DummyBookSignal {
    output: ConsumerOutput,
}

struct DummyConsumerSignal {
    output: ConsumerOutput,
    input: ConsumerInput,
}

impl CallSignal for DummyBookSignal {
    fn call_signal(&mut self, time: u128, graph: &GraphHandle) {
        assert!(!self.output.was_written(graph));
        self.output
            .set(1.0 + self.output.get(graph).unwrap_or(1.0), graph)
    }
}

impl CallSignal for DummyConsumerSignal {
    fn call_signal(&mut self, time: u128, graph: &GraphHandle) {
        assert!(!self.output.was_written(graph));
        self.output.set(self.input.get(graph).unwrap(), graph);
    }
}

impl RegisterSignal for DummyBookSignal {
    type Child = DummyBookSignal;
    const PARAMS: bool = false;

    fn get_inputs() -> HashMap<&'static str, SignalType> {
        let signals = vec![("input", SignalType::Book)];
        signals.into_iter().collect()
    }

    fn get_outputs() -> HashSet<&'static str> {
        vec!["out"].into_iter().collect()
    }

    fn create(
        mut outs: HashMap<&'static str, ConsumerOutput>,
        _: InputLoader,
        json: Option<&str>
    ) -> Result<DummyBookSignal, anyhow::Error> {
        assert_eq!(json, None);
        Ok(DummyBookSignal {
            output: outs.remove("out").unwrap(),
        })
    }
}

impl RegisterSignal for DummyConsumerSignal {
    type Child = DummyConsumerSignal;
    const PARAMS: bool = false;
    fn get_inputs() -> HashMap<&'static str, SignalType> {
        vec![("input", SignalType::Consumer)].into_iter().collect()
    }

    fn get_outputs() -> HashSet<&'static str> {
        vec!["out"].into_iter().collect()
    }

    fn create(
        mut outs: HashMap<&'static str, ConsumerOutput>,
        mut input: InputLoader,
        json: Option<&str>
    ) -> Result<DummyConsumerSignal, anyhow::Error> {
        assert_eq!(json, None);
        Ok(DummyConsumerSignal {
            output: outs.remove("out").unwrap(),
            input: input.load_input("input")?,
        })
    }
}

#[test]
fn construct_graph() {
    let signals = vec![
        ("dummy_book", make_signal_for::<DummyBookSignal>()),
        ("dummy_signal", make_signal_for::<DummyConsumerSignal>()),
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
                    NamedSignalType::Consumer(("book".to_string(), "out".to_string())),
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
                    NamedSignalType::Consumer(("book".to_string(), "out".to_string())),
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
                    NamedSignalType::Consumer(("book".to_string(), "out".to_string())),
                )]
                .into_iter()
                .collect(),
            },
        ),
    ];

    let mut graph = registrar.generate_graph(&layout_vec, &sec_map, &HashMap::new()).unwrap();

    let book = graph.signal_listener("book", "out").unwrap();
    let consumer1 = graph.signal_listener("consumer1", "out").unwrap();
    let consumer2 = graph.signal_listener("consumer2", "out").unwrap();
    let consumer3 = graph.signal_listener("consumer3", "out").unwrap();

    let data = vec![].into_iter().collect();
    graph.trigger_book(sec_map.to_index(&btc).unwrap(), &data, 0, |_, _| ());
}
