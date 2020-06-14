#![allow(warnings)]
#[macro_use]
mod common;
use arby::order_book::*;
use arby::signal_graph::graph_error::*;
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
    fn get_inputs() -> HashMap<&'static str, SignalType> {
        vec![("input", SignalType::Consumer)].into_iter().collect()
    }

    fn get_outputs() -> HashSet<&'static str> {
        vec!["out"].into_iter().collect()
    }

    fn create(
        mut outs: HashMap<&'static str, ConsumerOutput>,
        mut ins: InputLoader,
        json: Option<&str>
    ) -> Result<DummyConsumerSignal, anyhow::Error> {
        assert_eq!(json, None);
        Ok(DummyConsumerSignal {
            output: outs.remove("out").unwrap(),
            input: ins.load_input("input")?,
        })
    }
}

#[test]
fn test_duplicate_registry() {
    let signals = vec![
        ("dummy", make_signal_for::<DummyBookSignal>()),
        ("dummy", make_signal_for::<DummyConsumerSignal>()),
    ];

    let registrar = GraphRegistrar::new(&signals);

    check_error!(registrar,
    GraphError::DuplicateSignalName(name) => {
        assert_eq!(name, "dummy")
    }
    );
}

fn get_sec_map() -> SecurityMap {
    let btc = Security {
        product: SmallString::from_str("BTCXBT"),
        exchange: SmallString::from_str("BITMEX"),
    };

    let sec_map = unsafe { SecurityMap::new_unchecked(&[btc.clone()]) };

    sec_map
}

#[test]
fn test_not_found() {
    let signals = vec![("dummy", make_signal_for::<DummyBookSignal>())];

    let registrar = GraphRegistrar::new(&signals).unwrap();

    let sec_map = get_sec_map();

    let layout = [(
        "wont_exist".to_string(),
        SignalCall {
            signal_name: "nonexistant".to_string(),
            inputs: HashMap::new(),
        },
    )];

    check_error!(registrar.generate_graph(&layout, &sec_map, &HashMap::new()),
    GraphError::DefinitionNotFound{definition, signal} => {
        assert_eq!(definition, "nonexistant");
        assert_eq!(signal, "wont_exist");
    }
    );
}

#[test]
fn test_input_not_exist() {
    let signals = vec![("dummy", make_signal_for::<DummyConsumerSignal>())];

    let registrar = GraphRegistrar::new(&signals).unwrap();

    let sec_map = get_sec_map();

    let layout = vec![(
        "dummy_sig".to_string(),
        SignalCall {
            signal_name: "dummy".to_string(),
            inputs: vec![
                (
                    "input".to_string(),
                    NamedSignalType::Consumer(("doesnt_exist_also".to_string(), "out".to_string())),
                ),
                (
                    "not_found".to_string(),
                    NamedSignalType::Consumer(("doesnt_exist".to_string(), "out".to_string())),
                ),
            ]
            .into_iter()
            .collect(),
        },
    )];

    check_error!(registrar.generate_graph(&layout, &sec_map, &HashMap::new()),
    GraphError::InputNotExist{signal, input} => {
        assert_eq!(signal, "dummy_sig");
        assert_eq!(input, "not_found");
    }
    );
}

#[test]
fn test_input_type_mismatch() {
    let signals = vec![("dummy", make_signal_for::<DummyBookSignal>())];

    let registrar = GraphRegistrar::new(&signals).unwrap();

    let sec_map = get_sec_map();

    let layout = vec![(
        "dummy_sig".to_string(),
        SignalCall {
            signal_name: "dummy".to_string(),
            inputs: vec![(
                "input".to_string(),
                NamedSignalType::Consumer(("wrong_type".to_string(), "out".to_string())),
            )]
            .into_iter()
            .collect(),
        },
    )];

    check_error!(registrar.generate_graph(&layout, &sec_map, &HashMap::new()),
    GraphError::InputWrongType{input, signal, given, wants} => {
        assert_eq!(signal, "dummy_sig");
        assert_eq!(input, "input");
        match given {
            NamedSignalType::Consumer((sig, out)) => {
                assert_eq!(sig, "wrong_type");
                assert_eq!(out, "out");
            },
            other => panic!("Wrong signal type {:?}", other)
        };
        match wants {
            SignalType::Book => (),
            other => panic!("Wrong signal type {:?}", other)
        };
    }
    );
}

#[test]
fn test_input_not_covered() {
    let signals = vec![("dummy", make_signal_for::<DummyBookSignal>())];

    let registrar = GraphRegistrar::new(&signals).unwrap();

    let sec_map = get_sec_map();

    let layout = vec![(
        "dummy_sig".to_string(),
        SignalCall {
            signal_name: "dummy".to_string(),
            inputs: HashMap::new(),
        },
    )];

    check_error!(registrar.generate_graph(&layout, &sec_map, &HashMap::new()),
    GraphError::InputNotGiven{input, signal} => {
        assert_eq!(signal, "dummy_sig");
        assert_eq!(input, "input");
    }
    );
}

#[test]
fn test_duplicate_instance() {
    let signals = vec![("dummy", make_signal_for::<DummyBookSignal>())];

    let registrar = GraphRegistrar::new(&signals).unwrap();

    let sec_map = get_sec_map();

    let layout = vec![
        (
            "dummy_sig".to_string(),
            SignalCall {
                signal_name: "dummy".to_string(),
                inputs: HashMap::new(),
            },
        ),
        (
            "dummy_sig".to_string(),
            SignalCall {
                signal_name: "dummy".to_string(),
                inputs: HashMap::new(),
            },
        ),
    ];

    check_error!(registrar.generate_graph(&layout, &sec_map, &HashMap::new()),
    GraphError::DuplicateSignalInstance(signal) => {
        assert_eq!(signal, "dummy_sig");
    }
    );
}

#[test]
fn test_parent_not_found() {
    let signals = vec![("dummy", make_signal_for::<DummyConsumerSignal>())];

    let registrar = GraphRegistrar::new(&signals).unwrap();

    let sec_map = get_sec_map();

    let layout = vec![(
        "parent_not_found".to_string(),
        SignalCall {
            signal_name: "dummy".to_string(),
            inputs: vec![(
                "input".to_string(),
                NamedSignalType::Consumer(("not_found".to_string(), "out".to_string())),
            )]
            .into_iter()
            .collect(),
        },
    )];

    check_error!(registrar.generate_graph(&layout, &sec_map, &HashMap::new()),
    GraphError::ParentNotFound{child, parent, input, output} => {
        assert_eq!(child, "parent_not_found");
        assert_eq!(parent, "not_found");
        assert_eq!(input, "input");
        assert_eq!(output, "out");
    }
    );
}
