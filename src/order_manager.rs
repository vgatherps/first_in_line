use crate::exchange::normalized::*;
use crate::order_book::{BuyPrice, SellPrice, SidedPrice};

use std::collections::BTreeMap;

#[derive(Ord, PartialOrd, Eq, PartialEq)]
struct OrderKey<P: SidedPrice + PartialOrd + Ord> {
    price: P,
    id: usize
}

pub struct OrderManager {
    buys: BTreeMap<OrderKey<BuyPrice>, f64>,
    sells: BTreeMap<OrderKey<SellPrice>, f64>,
    current_id: usize,
}

impl OrderManager {
    pub fn new() -> OrderManager {
        OrderManager {
            buys: BTreeMap::new(),
            sells: BTreeMap::new(),
            current_id: 1,
        }
    }

    pub fn can_place_at<P: SidedPrice>(&self, price: &P) -> bool {
        match P::SIDE {
            Side::Buy => self
                .sells
                .keys()
                .next()
                .map(|first| first.price > price.to_sell())
                .unwrap_or(true),
            Side::Sell => self
                .buys
                .keys()
                .next()
                .map(|first| first.price > price.to_buy())
                .unwrap_or(true),
        }
    }

    pub fn add_sent_order<P: SidedPrice>(&mut self, price: &P, dollars: f64) -> bool {
        if !self.can_place_at(price) {
            return false;
        }
        let id = self.current_id;
        self.current_id += 1;
        match P::SIDE {
            Side::Buy => self.buys.insert(OrderKey {id, price: price.to_buy()}, dollars),
            Side::Sell => self.sells.insert(OrderKey {id, price: price.to_sell()}, dollars),
        };
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_can_place_empty() {
        let manager = OrderManager::new();
        assert!(manager.can_place_at(&BuyPrice::new(10)));
        assert!(manager.can_place_at(&SellPrice::new(10)));
    }

    #[test]
    fn test_can_place_close() {
        let mut manager = OrderManager::new();
        assert!(manager.add_sent_order(&BuyPrice::new(10), 10.0));
        assert!(!manager.can_place_at(&SellPrice::new(10)));
        assert!(!manager.add_sent_order(&SellPrice::new(10), 10.0));
        assert!(manager.can_place_at(&SellPrice::new(11)));
        assert!(manager.add_sent_order(&SellPrice::new(11), 10.0));
        assert!(!manager.can_place_at(&BuyPrice::new(11)));
        assert!(!manager.add_sent_order(&BuyPrice::new(11), 10.0));
    }
}
