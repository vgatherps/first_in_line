use crate::order_book::{BuyPrice, SellPrice, SidedPrice};
use crate::signal_graph::graph_registrar::*;
use crate::signal_graph::interface_types::*;

use std::collections::{HashMap, HashSet};

fn cents_to_dollars(cents: usize) -> f64 {
    (cents as f64) * 0.01
}

pub struct FairValue {
    fair_out: ConsumerOutput,
    size_out: ConsumerOutput,
    book: BookViewer,
    score_denom: f64,
    score_offset: f64,
    dollars_out: f64,
    levels_out: usize,
}

#[derive(serde::Deserialize)]
struct FairParameters {
    score_denom: f64,
    score_offset: f64,
    dollars_out: f64,
    levels_out: usize,
}

impl RegisterSignal for FairValue {
    type Child = Self;
    fn get_inputs() -> HashMap<&'static str, SignalType> {
        maplit::hashmap! {
            "book" => SignalType::Book,
        }
    }

    fn get_outputs() -> HashSet<&'static str> {
        maplit::hashset! {
            "fair",
            "size",
        }
    }

    fn create(
        mut outputs: HashMap<&'static str, ConsumerOutput>,
        mut inputs: InputLoader,
        json: Option<&str>,
    ) -> Result<Self, anyhow::Error> {
        let FairParameters {
            score_denom,
            score_offset,
            dollars_out,
            levels_out,
        } = serde_json::from_str(json.unwrap())?;
        Ok(FairValue {
            score_denom,
            score_offset,
            dollars_out,
            levels_out,
            book: inputs.load_input("book")?,
            fair_out: outputs.remove("fair").unwrap(),
            size_out: outputs.remove("size").unwrap(),
        })
    }
}

/**
 * Fair price algorithm with depth. In short,
 * * Calculate scored price (price * shares * depth_score), as well as overall score for side
 * * use resulting values to calculate weighted midpoint
 */
impl FairValue {
    fn score(&self, distance: f64) -> f64 {
        self.score_offset + 1.0 / (1.0 + self.score_denom * distance * distance)
    }

    fn score_distanced<I>(&self, prices: I) -> (f64, f64)
    where
        I: Iterator<Item = (f64, f64, f64)>,
    {
        prices
            .take(self.levels_out)
            .take_while(|(_, distance, _)| *distance <= self.dollars_out)
            .map(|(prc, distance, sz)| (prc, self.score(distance), sz))
            .fold((0.0, 0.0), |(sum_prc, sum_shares), (prc, score, shares)| {
                let shares_score = score * shares as f64;
                (sum_prc + prc * shares_score, sum_shares + shares_score)
            })
    }
}

impl CallSignal for FairValue {
    fn call_signal(&mut self, _: u128, graph: &GraphHandle) {
        let (best_bid, best_ask) = match self.book.book().bbo_price() {
            (Some(best_bid), Some(best_ask)) => (best_bid, best_ask),
            _ => {
                self.fair_out.mark_invalid(graph);
                self.size_out.mark_invalid(graph);
                return;
            }
        };
        let best_bid = best_bid as f64;
        let best_ask = best_ask as f64;
        let book = self.book.book();
        let bids = book
            .bids()
            .map(|(prc, sz)| (cents_to_dollars(prc.unsigned()), *sz))
            .map(|(prc, sz)| (prc, best_bid - prc, sz));
        let asks = book
            .asks()
            .map(|(prc, sz)| (cents_to_dollars(prc.unsigned()), *sz))
            .map(|(prc, sz)| (prc, prc - best_ask, sz));

        let (bid_price, bid_shares) = self.score_distanced(bids);
        let (ask_price, ask_shares) = self.score_distanced(asks);

        assert!(bid_shares > 0.0);
        assert!(ask_shares > 0.0);

        let bid_price = bid_price / bid_shares;
        let ask_price = ask_price / ask_shares;

        let fair_price =
            (bid_price * ask_shares + ask_price * bid_shares) / (ask_shares + bid_shares);
        let fair_shares = ask_shares + bid_shares;
        self.fair_out.set(fair_price, graph);
        self.size_out.set(fair_shares, graph);
    }
}
