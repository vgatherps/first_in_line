use crate::ema::Ema;
use crate::exchange::normalized::*;
use crate::fair_value::{FairValue, FairValueResult};
use crate::order_book::OrderBook;
use futures::{future::FutureExt, select};

use horrorshow::helper::doctype;
use horrorshow::html;
use horrorshow::prelude::*;

// Hardcoded because futures are a bit silly for selecting variable amounts
// TODO make each book only return async market data when it receives a valid level
pub struct RemoteVenueAggregator {
    bitmex: MarketDataStream,
    okex_spot: MarketDataStream,
    okex_swap: MarketDataStream,
    books: [OrderBook; Exchange::COUNT as usize],
    fairs: [f64; Exchange::COUNT as usize],
    size_ema: [Ema; Exchange::COUNT as usize],
    valuer: FairValue,
}

impl RemoteVenueAggregator {
    pub fn new(
        bitmex: MarketDataStream,
        okex_spot: MarketDataStream,
        okex_swap: MarketDataStream,
        valuer: FairValue,
        size_ratio: f64,
    ) -> RemoteVenueAggregator {
        RemoteVenueAggregator {
            bitmex,
            okex_spot,
            okex_swap,
            valuer,
            fairs: Default::default(),
            size_ema: [Ema::new(size_ratio); Exchange::COUNT as usize],
            books: Default::default(),
        }
    }

    fn update_fair_for(&mut self, block: MarketEventBlock) {
        let book = &mut self.books[block.exchange as usize];
        for event in block.events {
            book.handle_book_event(&event);
        }
        match book.bbo() {
            (Some((bid, _)), Some((ask, _))) => {
                let new_fair = self.valuer.fair_value(book.bids(), book.asks(), (bid, ask));
                self.fairs[block.exchange as usize] = new_fair.fair_price;
                self.size_ema[block.exchange as usize].add_value(new_fair.fair_shares);
            }
            _ => (),
        }
    }

    pub fn calculate_fair(&self) -> Option<f64> {
        let mut total_price = 0.0;
        let mut total_size = 0.0;
        for i in 0..(Exchange::COUNT as usize) {
            let size = self.size_ema[i].get_value().unwrap_or(0.0);
            assert!(size >= 0.0);
            if size < 10.0 {
                return None;
            }
            total_price += self.fairs[i] * size;
            total_size += size;
        }
        Some(total_price / total_size)
    }

    // TODO think about fair spread
    pub async fn get_new_fair(&mut self) {
        select! {
            b = self.bitmex.next().fuse() => self.update_fair_for(b),
            b = self.okex_spot.next().fuse() => self.update_fair_for(b),
            b = self.okex_swap.next().fuse() => self.update_fair_for(b),
        }
    }

    pub fn get_exchange_description(&self, exch: Exchange) -> String {
        "".into()
    }

    pub fn get_html_info(&self) -> String {
        format!(
            "{}",
            html! {
                : doctype::HTML;
                html {
                    body {
                        // attributes
                        h2(id="heading", class="title") : "Remote fair value summary";
                        ol(id="Fair values") {
                            li(first?=true, class="item") {
                                : format!("Bitmex: {}", self.get_exchange_description(Exchange::Bitmex))
                            }
                            li(first?=false, class="item") {
                                : format!("OkexSpot: {}", self.get_exchange_description(Exchange::OkexSpot))
                            }
                            li(first?=false, class="item") {
                                : format!("OkexSwap: {}", self.get_exchange_description(Exchange::OkexSwap))
                            }
                        }
                    }
                }
            }
        )
    }
}
