use crate::signal_graph::graph_registrar::*;
use crate::signal_graph::interface_types::*;

use std::collections::{HashMap, HashSet};

// What's the algorithm?
// Look at the displacement of a fast ema of fair price from a slower ema of fair
// Assume the local exchange should be following the same curve, and return
// the displacement the fast ema fair has from such a curve

pub struct Premium {
    in1: ConsumerInput,
    in2: ConsumerInput,
    diff: ConsumerOutput,
}

// TODO use case for exposing complex signals as a single reusable block

impl CallSignal for Premium {
    fn call_signal(&mut self, _: u128, graph: &GraphHandle) {
        self.diff.set_from(
            self.in1
                .and(&self.in2, graph)
                .get()
                .map(|(in1, in2)| in1 - in2),
            graph,
        );
    }
}

impl RegisterSignal for Premium {
    const PARAMS: bool = false;
    type Child = Self;
    fn get_inputs() -> HashMap<&'static str, SignalType> {
        maplit::hashmap! {
            "in1" => SignalType::Consumer,
            "in2" => SignalType::Consumer,
        }
    }

    fn get_outputs() -> HashSet<&'static str> {
        maplit::hashset! {
            "out"
        }
    }

    fn create(
        mut outputs: HashMap<&'static str, ConsumerOutput>,
        mut inputs: InputLoader,
        _: Option<&str>,
    ) -> Result<Self, anyhow::Error> {
        Ok(Premium {
            in1: inputs.load_input("in1")?,
            in2: inputs.load_input("in2")?,
            diff: outputs.remove("out").unwrap(),
        })
    }
}

/*
impl Displacement {
    pub fn handle_local(&mut self, local_fair: f64, local_size: f64) {
        let lf = self.local_fast_ema.add_value(local_fair);
        self.local_slow_ema.add_value(local_fair);
        self.local_size.add_value(local_size);
        if let Some(rf) = self.remote_fast_ema.get_value() {
            self.premium_ema.add_value(lf - rf);
        }
    }

    pub fn handle_remote(&mut self, remote_fair: f64, remote_size: f64) {
        self.remote_fast_ema.add_value(remote_fair);
        self.remote_slow_ema.add_value(remote_fair);
        self.remote_size = remote_size;
    }

    pub fn get_displacement(&self) -> Option<(f64, f64)> {
        if let (Some(lf), Some(ls), Some(rf), Some(rs), Some(lsize), Some(premium)) = (
            self.local_fast_ema.get_value(),
            self.local_slow_ema.get_value(),
            self.remote_fast_ema.get_value(),
            self.remote_slow_ema.get_value(),
            self.local_size.get_value(),
            self.premium_ema.get_value(),
        ) {
            // how far above the slower moving price is the fast fair value?
            let remote_premium = rf - rs;
            let local_premium = lf - ls;

            // discount the remoteness by the size ratio
            let total_size = lsize + self.remote_size;
            let adjust_remote = self.remote_size / total_size;
            let adjust_local = lsize / total_size;

            let remote_premium = remote_premium * adjust_remote + local_premium * adjust_local;

            // How much farther must the remote premium go (or has it gone too far?)
            Some((remote_premium - local_premium, premium))
        } else {
            None
        }
    }
}
*/
