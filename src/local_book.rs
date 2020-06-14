use crate::signal_graph::graph_registrar::*;
use crate::signal_graph::interface_types::*;

use std::collections::{HashMap, HashSet};

pub struct BookImprovedSignal {
    book: BookViewer,
    improved_bid_to: ConsumerOutput,
    improved_ask_to: ConsumerOutput,
    tob: Option<(usize, usize)>,
}

impl CallSignal for BookImprovedSignal {
    fn call_signal(&mut self, _: u128, graph: &GraphHandle) {
        match self.book.book().bbo_price() {
            (Some(best_bid), Some(best_ask)) => {
                match self.tob {
                    Some((old_bid, old_ask)) => {
                        if best_bid > old_bid {
                            self.improved_bid_to.set(best_bid as f64, graph);
                        }
                        if best_ask < old_ask {
                            self.improved_ask_to.set(best_ask as f64, graph);
                        }
                    }
                    _ => (),
                };
                self.tob = Some((best_bid, best_ask));
            }
            _ => (),
        }
    }

    fn cleanup(&mut self, graph: &GraphHandle) {
        self.improved_bid_to.mark_invalid(graph);
        self.improved_ask_to.mark_invalid(graph);
    }
}

impl RegisterSignal for BookImprovedSignal {
    const CLEANUP: bool = true;
    type Child = Self;
    fn get_inputs() -> HashMap<&'static str, SignalType> {
        maplit::hashmap! {
            "book" => SignalType::Book,
        }
    }

    fn get_outputs() -> HashSet<&'static str> {
        maplit::hashset! {
            "improved_bid",
            "improved_ask",
        }
    }

    fn create(
        mut outputs: HashMap<&'static str, ConsumerOutput>,
        mut inputs: InputLoader,
        _: Option<&str>,
    ) -> Result<BookImprovedSignal, anyhow::Error> {
        Ok(BookImprovedSignal {
            improved_bid_to: outputs.remove("improved_bid").unwrap(),
            improved_ask_to: outputs.remove("improved_ask").unwrap(),
            book: inputs.load_input("book")?,
            tob: None,
        })
    }
}
