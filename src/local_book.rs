use crate::exchange::normalized::*;
use crate::fair_value::FairValue;
use crate::order_book::*;

use horrorshow::html;

pub struct LocalBook {
    fair: FairValue,
    tob: Option<((usize, usize), f64)>,
    book: OrderBook,
}

#[derive(Debug)]
pub struct InsideOrder {
    pub insert_price: usize,
    pub insert_size: f64,
    pub side: Side,
}

impl LocalBook {
    pub fn new(fair: FairValue) -> LocalBook {
        LocalBook {
            fair,
            tob: None,
            book: OrderBook::new(),
        }
    }

    pub fn get_local_tob(&self) -> Option<((usize, usize), f64)> {
        self.tob
    }

    pub fn book(&self) -> &OrderBook {
        &self.book
    }

    pub fn handle_book_update(&mut self, events: &SmallVec<MarketEvent>) -> SmallVec<InsideOrder> {
        for event in events {
            self.book.handle_book_event(event);
        }
        match self.book.bbo() {
            (Some((bid, _)), Some((ask, _))) => {
                let fair = self
                    .fair
                    .fair_value(self.book.bids(), self.book.asks(), (bid, ask));
                // find all of the new bids, and all of the new asks
                let new_levels = if let Some(((old_bid, old_ask), _)) = self.tob {
                    let bids = self
                        .book
                        .bids()
                        .take_while(|(prc, _)| prc.unsigned() > old_bid)
                        .map(|(prc, size)| InsideOrder {
                            side: Side::Buy,
                            insert_price: prc.unsigned(),
                            insert_size: *size,
                        });
                    let asks = self
                        .book
                        .asks()
                        .take_while(|(prc, _)| prc.unsigned() < old_ask)
                        .map(|(prc, size)| InsideOrder {
                            side: Side::Sell,
                            insert_price: prc.unsigned(),
                            insert_size: *size,
                        });
                    bids.chain(asks).collect()
                } else {
                    SmallVec::new()
                };
                self.tob = Some(((bid, ask), fair.fair_price));
                new_levels
            }
            _ => SmallVec::new(),
        }
    }

    pub fn get_html_info(&self) -> String {
        if let Some(((bprice, aprice), fair)) = self.tob {
            format!(
                "{}",
                html! {
                    h3(id="remote heading", class="title") : "Local fair value summary";
                    ul(id="Local book info") {
                        li(first?=true, class="item") {
                            : format!("Local bbo: {:.2}x{:.2}, fair {:.2}",
                                      bprice as f64 * 0.01,
                                      aprice as f64 * 0.01,
                                      fair);
                        }
                    }
                }
            )
        } else {
            String::new()
        }
    }
}
