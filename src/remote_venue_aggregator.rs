use crate::ema::Ema;
use crate::exchange::normalized::*;
use crate::fair_value::FairValue;
use crate::order_book::OrderBook;
use futures::{future::FutureExt, join, select};

use crate::signal_graph::graph_registrar::*;
use crate::signal_graph::interface_types::*;

use std::collections::{HashMap, HashSet};

// Hardcoded because futures are a bit silly for selecting variable amounts
pub struct RemoteVenueAggregator {
    fairs: Vec<(ConsumerInput, ConsumerInput)>,
    fair_mid: ConsumerOutput,
    total_size: ConsumerOutput,
}

impl RegisterSignal for RemoteVenueAggregator {
    type Child = RemoteVenueAggregator;
    const PARAMS: bool = false;

    fn get_inputs() -> HashMap<&'static str, SignalType> {
        maplit::hashmap! {
            "fair_mids" => SignalType::Aggregate,
            "fair_sizes" => SignalType::Aggregate,
        }
    }

    fn get_outputs() -> HashSet<&'static str> {
        maplit::hashset! {
            "fair",
            "size",
        }
    }

    // TODO combine inputs into one big friendly structure hiding them behind any
    fn create(
        mut outputs: HashMap<&'static str, ConsumerOutput>,
        mut inputs: InputLoader,
        json: Option<&str>,
    ) -> Result<Self, anyhow::Error> {
        let fairs: Vec<_> = inputs
            .load_input::<AggregateInputGenerator>("fair_mids")?
            .as_consumers();
        let sizes: Vec<_> = inputs
            .load_input::<AggregateInputGenerator>("fair_sizes")?
            .as_consumers();
        assert_eq!(fairs.len(), sizes.len());
        Ok(RemoteVenueAggregator {
            fairs: fairs.into_iter().zip(sizes.into_iter()).collect(),
            fair_mid: outputs.remove("fair").unwrap(),
            total_size: outputs.remove("size").unwrap(),
        })
    }
}

impl CallSignal for RemoteVenueAggregator {
    // TODO only iterate over changed, although... that requires better zipping api
    // for the aggregate updates. Must think about how to do such a thing
    fn call_signal(&mut self, _: u64, _: &MarketUpdates, graph: &GraphHandle) {
        let mut total_price = 0.0;
        let mut total_size = 0.0;
        for (fair, size) in self.fairs.iter() {
            if let Some((fair, size)) = fair.and(&size, graph).get() {
                total_price += fair * size;
                total_size += size;
            } else {
                self.fair_mid.mark_invalid(graph);
                self.total_size.mark_invalid(graph);
                return;
            }
        }
        self.fair_mid.set(total_price / total_size, graph);
        self.total_size.set(total_size, graph);
    }
}
