use crate::exchange::normalized::MarketUpdates;
use crate::signal_graph::graph_registrar::*;
use crate::signal_graph::interface_types::*;
use serde::Deserialize;

use std::collections::{HashMap, HashSet};

pub struct Ema {
    input: ConsumerInput,
    value: ConsumerOutput,
    ratio: f64,
    cur_ratio: f64,
}

#[derive(Deserialize)]
struct EmaInit {
    ratio: f64,
}

impl CallSignal for Ema {
    fn call_signal(&mut self, _: u64, _: &MarketUpdates, graph: &GraphHandle) {
        let result_valid = self
            .input
            .get(graph)
            .map(|new_value| match self.value.get(graph) {
                Some(value) => {
                    let ratio = self.cur_ratio;
                    self.cur_ratio = 0.95 * self.cur_ratio + 0.05 * self.ratio;
                    value * (1.0 - ratio) + ratio * new_value
                }
                None => new_value,
            });
        self.value.set_from(result_valid, graph);
    }
}

impl RegisterSignal for Ema {
    type Child = Ema;
    fn get_inputs() -> HashMap<&'static str, SignalType> {
        maplit::hashmap! {
            "input" => SignalType::Consumer,
        }
    }

    fn get_outputs() -> HashSet<&'static str> {
        maplit::hashset! {
            "output"
        }
    }

    fn create(
        mut outputs: HashMap<&'static str, ConsumerOutput>,
        mut inputs: InputLoader,
        json: Option<&str>,
    ) -> Result<Self, anyhow::Error> {
        let init = serde_json::from_str::<EmaInit>(json.unwrap())?;
        Ok(Ema {
            input: inputs.load_input("input")?,
            value: outputs.remove("output").unwrap(),
            ratio: init.ratio,
            cur_ratio: 0.5,
        })
    }
}
