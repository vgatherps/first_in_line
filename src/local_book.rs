use crate::exchange::normalized::*;
use crate::fair_value::FairValue;
use crate::order_book::*;

use horrorshow::html;

pub struct LocalBook {
    fair: FairValue,
    tob: Option<(((usize, f64), (usize, f64)), f64)>,
    book: OrderBook,
    seen_inside: Vec<(usize, Side)>,
}

#[derive(Debug)]
pub struct InsideOrder {
    pub insert_price: usize,
    pub most_aggressive_on_side: usize,
    pub side: Side,
}

impl LocalBook {
    pub fn new(fair: FairValue) -> LocalBook {
        LocalBook {
            fair,
            tob: None,
            book: OrderBook::new(),
            seen_inside: Vec::new(),
        }
    }

    pub fn get_local_tob(&self) -> Option<(((usize, f64), (usize, f64)), f64)> {
        self.tob
    }

    pub fn handle_book_update(&mut self, events: &SmallVec<MarketEvent>) {
        for event in events {
            self.book.handle_book_event(event);
        }
        match self.book.bbo() {
            (Some((bid, bid_sz)), Some((ask, ask_sz))) => {
                let fair = self
                    .fair
                    .fair_value(self.book.bids(), self.book.asks(), (bid, ask));
                self.tob = Some((((bid, bid_sz), (ask, ask_sz)), fair.fair_price));
                self.seen_inside.retain(|(prc, _)| bid < *prc && *prc < ask);
            }
            _ => (),
        }
    }

    pub fn handle_new_order(&mut self, events: &SmallVec<MarketEvent>) -> SmallVec<InsideOrder> {
        let last_seen = self.book.last_seen();
        let mut events = if let Some((((bid, _), (ask, _)), _)) = self.tob {
            events
                .iter()
                .filter_map(|event| match event {
                    MarketEvent::OrderUpdate(order)
                        if last_seen != 0
                            && last_seen < order.exchange_time
                            && bid < order.cents
                            && order.cents < ask
                            && order.size >= 0.0001
                            && !self.seen_inside.iter().any(|(prc, _)| *prc == order.cents) =>
                    {
                        self.seen_inside.push((order.cents, order.side));
                        let inside_on_side = self
                            .seen_inside
                            .iter()
                            .filter(|(_, s)| *s == order.side)
                            .map(|(pr, _)| *pr);
                        let most_aggressive_on_side = match order.side {
                            Side::Buy => inside_on_side.max().unwrap(),
                            Side::Sell => inside_on_side.min().unwrap(),
                        };
                        Some(InsideOrder {
                            insert_price: order.cents,
                            side: order.side,
                            most_aggressive_on_side,
                        })
                    }
                    _ => None,
                })
                .collect()
        } else {
            SmallVec::new()
        };
        // Put the most aggressive (largest bids) first
        // then after the most aggressive(smallest sells) first.
        (&mut events[..]).sort_unstable_by_key(|ord| match ord.side {
            Side::Buy => (ord.insert_price as i64) * -1,
            Side::Sell => (ord.insert_price as i64),
        });
        events
    }

    pub fn get_html_info(&self) -> String {
        if let Some((((bprice, bsize), (aprice, asize)), fair)) = self.tob {
            let mut inside_bids: Vec<_> = self
                .seen_inside
                .iter()
                .filter(|(_, side)| *side == Side::Buy)
                .map(|(price, _)| *price)
                .collect();
            let mut inside_asks: Vec<_> = self
                .seen_inside
                .iter()
                .filter(|(_, side)| *side == Side::Sell)
                .map(|(price, _)| *price)
                .collect();
            inside_bids.sort();
            inside_asks.sort();
            format!(
                "{}",
                html! {
                    h3(id="remote heading", class="title") : "Local fair value summary";
                    ul(id="Local book info") {
                        li(first?=true, class="item") {
                            : format!("Local bbo: ({:.2}, {:.2})x({:.2}, {:.2}), fair {:.2}",
                                      bprice as f64 * 0.01, bsize,
                                      aprice as f64 * 0.01, asize,
                                      fair);
                        }
                        li(first?=true, class="item") {
                            : format!("Bids inside spread: {:?}",
                                      inside_bids.iter().rev().map(|prc| *prc as f64 * 0.01).collect::<Vec<_>>());
                        }
                        li(first?=true, class="item") {
                            : format!("Asks inside spread: {:?}",
                                      inside_asks.iter().map(|prc| *prc as f64 * 0.01).collect::<Vec<_>>());
                        }
                    }
                }
            )
        } else {
            String::new()
        }
    }
}
