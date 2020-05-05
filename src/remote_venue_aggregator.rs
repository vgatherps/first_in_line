use crate::ema::Ema;
use crate::exchange::normalized::*;
use crate::fair_value::{FairValue, FairValueResult};
use crate::order_book::OrderBook;
use futures::{future::FutureExt, select};

// Hardcoded because futures are a bit silly for selecting variable amounts
pub struct RemoteVenueAggregator {
    bitmex: MarketDataStream,
    okex_spot: MarketDataStream,
    okex_swap: MarketDataStream,
    okex_quarterly: MarketDataStream,
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
        okex_quarterly: MarketDataStream,
        valuer: FairValue,
        size_ratio: f64,
    ) -> RemoteVenueAggregator {
        RemoteVenueAggregator {
            bitmex,
            okex_spot,
            okex_swap,
            okex_quarterly,
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

    fn calculate_new_fair_price(&self) -> f64 {
        let mut total_price = 0.0;
        let mut total_size = 0.0;
        for i in 0..(Exchange::COUNT as usize) {
            let size = self.size_ema[i].get_value().unwrap_or(0.0);
            total_price += self.fairs[i] * size;
            total_size += size;
        }

        if total_size < 1000.0 {
            0.0
        } else {
            total_price / total_size
        }
    }

    // TODO think about fair spread
    pub async fn get_new_fair(&mut self) -> f64 {
        select! {
            b = self.bitmex.next().fuse() => self.update_fair_for(b),
            b = self.okex_spot.next().fuse() => self.update_fair_for(b),
            b = self.okex_swap.next().fuse() => self.update_fair_for(b),
            b = self.okex_quarterly.next().fuse() => self.update_fair_for(b),
        }

        self.calculate_new_fair_price()
    }
}
