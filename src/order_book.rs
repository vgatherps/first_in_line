use std::collections::BTreeMap;

use crate::exchange::normalized::*;

// TODO abstract out into various book events
#[derive(Debug)]
pub struct BBOClearEvent {
    pub side: Side,
    pub price: usize,
}

#[derive(Ord, PartialOrd, Clone, Copy, Debug, Eq, PartialEq)]
pub struct BuyPrice {
    value: i64,
}

#[derive(Ord, PartialOrd, Clone, Copy, Debug, Eq, PartialEq)]
pub struct SellPrice {
    value: i64,
}

pub trait SidedPrice {
    const SIDE: Side;
    fn unsigned(&self) -> usize;
    fn to_sell(&self) -> SellPrice;
    fn to_buy(&self) -> BuyPrice;
}

impl BuyPrice {
    pub fn new(price: usize) -> BuyPrice {
        BuyPrice {
            value: price as i64 * -1,
        }
    }
}

impl SidedPrice for BuyPrice {
    const SIDE: Side = Side::Buy;
    fn unsigned(&self) -> usize {
        assert!(self.value <= 0);
        (self.value * -1) as usize
    }
    fn to_buy(&self) -> BuyPrice {
        *self
    }
    fn to_sell(&self) -> SellPrice {
        SellPrice {
            value: self.value * -1,
        }
    }
}

impl SellPrice {
    pub fn new(price: usize) -> SellPrice {
        SellPrice {
            value: price as i64,
        }
    }
}

impl SidedPrice for SellPrice {
    const SIDE: Side = Side::Sell;
    fn unsigned(&self) -> usize {
        assert!(self.value >= 0);
        self.value as usize
    }

    fn to_buy(&self) -> BuyPrice {
        BuyPrice {
            value: self.value * -1,
        }
    }

    fn to_sell(&self) -> SellPrice {
        *self
    }
}

#[derive(Default)]
pub struct OrderBook {
    bids: BTreeMap<BuyPrice, f64>,
    asks: BTreeMap<SellPrice, f64>,
    last_update: usize,
}

impl OrderBook {
    pub fn new() -> OrderBook {
        OrderBook {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_update: 0,
        }
    }

    fn update_level(&mut self, price: usize, side: Side, size: f64) {
        assert!(size > 0.00001);
        match side {
            Side::Buy => {
                let price = BuyPrice::new(price);
                *self.bids.entry(price).or_insert(size) = size;
            }
            Side::Sell => {
                let price = SellPrice::new(price);
                *self.asks.entry(price).or_insert(size) = size;
            }
        }
    }

    // TODO some exchanges (okex) send removes for nonexistent levels.
    // I should understand why and where that happens
    fn delete_level(&mut self, price: usize, side: Side) -> Option<BBOClearEvent> {
        let (best_bid, best_ask) = self.bbo();
        let (test_price, test_side) = match side {
            Side::Buy => {
                let price = BuyPrice::new(price);
                self.bids.remove(&price);
                (best_bid, Side::Buy)
            }
            Side::Sell => {
                let price = SellPrice::new(price);
                self.asks.remove(&price);
                (best_ask, Side::Sell)
            }
        };
        match test_price {
            Some((test_price, _)) if test_price == price => Some(BBOClearEvent {
                side: test_side,
                price: test_price,
            }),
            _ => None,
        }
    }

    pub fn handle_book_event(&mut self, event: &MarketEvent) -> Option<BBOClearEvent> {
        match event {
            MarketEvent::Book(BookUpdate {
                cents,
                side,
                size,
                exchange_time,
            }) => {
                self.last_update = *exchange_time;
                if *size <= 0.000001 {
                    self.delete_level(*cents, *side)
                } else {
                    self.update_level(*cents, *side, *size);
                    None
                }
            }
            MarketEvent::Clear => {
                self.bids.clear();
                self.asks.clear();
                None
            }
            _ => panic!("This book is not expected to consume non book update events"),
        }
    }

    pub fn bbo(&self) -> (Option<(usize, f64)>, Option<(usize, f64)>) {
        (
            self.bids
                .iter()
                .next()
                .map(|(prc, sz)| (prc.unsigned(), *sz)),
            self.asks
                .iter()
                .next()
                .map(|(prc, sz)| (prc.unsigned(), *sz)),
        )
    }

    pub fn bids(&self) -> impl Iterator<Item = (&BuyPrice, &f64)> {
        self.bids.iter()
    }

    pub fn asks(&self) -> impl Iterator<Item = (&SellPrice, &f64)> {
        self.asks.iter()
    }
}
