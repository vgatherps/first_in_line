use crate::exchange::normalized::*;
use crate::fair_value::{FairValue, FairValueResult};
use crate::order_book::*;

pub struct LocalBook {
    fair: FairValue,
    tob: Option<(((usize, f64), (usize, f64)), f64)>,
    book: OrderBook,
}

impl LocalBook {
    pub fn new(fair: FairValue) -> LocalBook {
        LocalBook {
            fair,
            tob: None,
            book: OrderBook::new(),
        }
    }

    pub fn get_local_tob(&self) -> Option<(((usize, f64), (usize, f64)), f64)> {
        self.tob
    }

    pub fn handle_book_update(&mut self, events: &Vec<MarketEvent>) {
        for event in events {
            self.book.handle_book_event(event);
        }
        match self.book.bbo() {
            (Some((bid, bid_sz)), Some((ask, ask_sz))) => {
                let fair = self
                    .fair
                    .fair_value(self.book.bids(), self.book.asks(), (bid, ask));
                self.tob = Some((((bid, bid_sz), (ask, ask_sz)), fair.fair_price))
            }
            _ => (),
        }
    }
}
