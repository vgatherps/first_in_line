use crate::exchange::normalized::*;
use crate::order_book::{BuyPrice, SellPrice, SidedPrice};

use std::collections::{btree_map::Entry, BTreeMap};

pub struct OrderManager {
    buys: BTreeMap<BuyPrice, (usize, f64)>,
    sells: BTreeMap<SellPrice, (usize, f64)>,
}

impl OrderManager {
    pub fn new() -> OrderManager {
        OrderManager {
            buys: BTreeMap::new(),
            sells: BTreeMap::new(),
        }
    }

    pub fn can_place_at<P: SidedPrice>(&self, price: &P) -> bool {
        match P::SIDE {
            Side::Buy => self
                .sells
                .keys()
                .next()
                .map(|first| *first > price.to_sell())
                .unwrap_or(true),
            Side::Sell => self
                .buys
                .keys()
                .next()
                .map(|first| *first > price.to_buy())
                .unwrap_or(true),
        }
    }

    pub fn add_sent_order<P: SidedPrice>(&mut self, price: &P, dollars: f64) -> bool {
        if !self.can_place_at(price) {
            return false;
        }
        match P::SIDE {
            Side::Buy => self.buys.insert(price.to_buy(), (0, dollars)),
            Side::Sell => self.sells.insert(price.to_sell(), (0, dollars)),
        };
        true
    }

    pub fn give_id<P: SidedPrice>(&mut self, price: &P, id: usize) {
        assert_ne!(id, 0);
        match P::SIDE {
            Side::Buy => {
                let price = price.to_buy();
                let order = self.buys.get_mut(&price).unwrap();
                assert_eq!(order.0, 0);
                order.0 = id;
            }
            Side::Sell => {
                let price = price.to_sell();
                let order = self.sells.get_mut(&price).unwrap();
                assert_eq!(order.0, 0);
                order.0 = id;
            }
        };
    }

    pub fn remove_liquidity_from<P: SidedPrice>(
        &mut self,
        price: &P,
        dollars: f64,
        id: usize,
    ) -> bool {
        match P::SIDE {
            Side::Buy => {
                let price = price.to_buy();
                match self.buys.entry(price) {
                    Entry::Occupied(mut occ) if occ.get().0 == id => {
                        occ.get_mut().1 -= dollars;
                        if occ.get_mut().1 <= 0.0000001 {
                            occ.remove_entry();
                        }
                        true
                    },
                    _ => false,
                }
            }
            Side::Sell => {
                let price = price.to_sell();
                match self.sells.entry(price) {
                    Entry::Occupied(mut occ) if occ.get().0 == id=> {
                        occ.get_mut().1 -= dollars;
                        if occ.get_mut().1 <= 0.0000001 {
                            occ.remove_entry();
                        }
                        true
                    },
                    _ => false,
                }
            }
        }
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

    #[test]
    fn test_remove_liquidity() {
        let mut manager = OrderManager::new();
        assert!(manager.add_sent_order(&BuyPrice::new(10), 10.0));
        manager.give_id(&BuyPrice::new(10), 1);
        assert!(!manager.add_sent_order(&SellPrice::new(10), 10.0));
        assert!(manager.remove_liquidity_from(&BuyPrice::new(10), 5.0, 1));
        assert!(!manager.add_sent_order(&SellPrice::new(10), 10.0));
        assert!(!manager.remove_liquidity_from(&BuyPrice::new(10), 5.0, 2));
        assert!(!manager.add_sent_order(&SellPrice::new(10), 10.0));
        assert!(manager.remove_liquidity_from(&BuyPrice::new(10), 5.0, 1));
        assert!(manager.add_sent_order(&SellPrice::new(10), 10.0));
    }
}
