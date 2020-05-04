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

impl BuyPrice {
    fn new(price: usize) -> BuyPrice {
        BuyPrice {
            value: price as i64 * -1,
        }
    }

    fn flip(&self) -> SellPrice {
        SellPrice {
            value: self.value * -1,
        }
    }

    pub fn unsigned(&self) -> usize {
        assert!(self.value <= 0);
        (self.value * -1) as usize
    }
}

impl SellPrice {
    fn new(price: usize) -> SellPrice {
        SellPrice {
            value: price as i64,
        }
    }

    fn flip(&self) -> BuyPrice {
        BuyPrice {
            value: self.value * -1,
        }
    }

    pub fn unsigned(&self) -> usize {
        assert!(self.value >= 0);
        self.value as usize
    }
}

pub struct OrderBook {
    bids: BTreeMap<BuyPrice, f64>,
    asks: BTreeMap<SellPrice, f64>,
}

impl OrderBook {
    pub fn new() -> OrderBook {
        OrderBook {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
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

    fn delete_level(&mut self, price: usize, side: Side) -> Option<BBOClearEvent> {
        let (best_bid, best_ask) = self.bbo();
        let (test_price, test_side) = match side {
            Side::Buy => {
                let price = BuyPrice::new(price);
                self.bids
                    .remove(&price)
                    .expect("Deleted a level that doesn't exist");
                (best_bid, Side::Buy)
            }
            Side::Sell => {
                let price = SellPrice::new(price);
                self.asks
                    .remove(&price)
                    .expect("Deleted a level that doesn't exist");
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
            MarketEvent::Book(BookUpdate { cents, side, size }) => {
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
