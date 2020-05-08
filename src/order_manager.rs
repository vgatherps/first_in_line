use crate::exchange::normalized::*;
use crate::order_book::{BuyPrice, SellPrice, SidedPrice};

use std::fmt::Debug;

use std::collections::{btree_map::Entry, BTreeMap};

use horrorshow::html;

#[derive(Eq, PartialEq, Copy, Clone, Debug)]
pub enum CancelStatus {
    Open,
    CancelSent,
}

#[derive(Debug)]
pub struct OrderManager {
    buys: BTreeMap<BuyPrice, (usize, f64, CancelStatus)>,
    sells: BTreeMap<SellPrice, (usize, f64, CancelStatus)>,
}

impl OrderManager {
    pub fn new() -> OrderManager {
        OrderManager {
            buys: BTreeMap::new(),
            sells: BTreeMap::new(),
        }
    }

    pub fn can_place_at<P: SidedPrice>(&self, price: &P) -> bool {
        let crosses = match P::SIDE {
            Side::Buy => self
                .sells
                .keys()
                .next()
                .map(|first| *first <= price.to_sell())
                .unwrap_or(false),
            Side::Sell => self
                .buys
                .keys()
                .next()
                .map(|first| *first <= price.to_buy())
                .unwrap_or(false),
        };
        let contains = match P::SIDE {
            Side::Buy => self.buys.contains_key(&price.to_buy()),
            Side::Sell => self.sells.contains_key(&price.to_sell()),
        };
        !contains && !crosses
    }

    pub fn add_sent_order<P: SidedPrice>(&mut self, price: &P, amount: f64) -> bool {
        if !self.can_place_at(price) {
            return false;
        }
        match P::SIDE {
            Side::Buy => self
                .buys
                .insert(price.to_buy(), (0, amount, CancelStatus::Open)),
            Side::Sell => self
                .sells
                .insert(price.to_sell(), (0, amount, CancelStatus::Open)),
        };
        true
    }

    pub fn best_buy_price_cancel(&self) -> Option<(BuyPrice, usize)> {
        self.buys
            .iter()
            .filter(|(_, (id, _, stat))| *stat != CancelStatus::CancelSent && *id != 0)
            .next()
            .map(|(k, (id, _, _))| (*k, *id))
    }

    pub fn best_sell_price_cancel(&self) -> Option<(SellPrice, usize)> {
        self.sells
            .iter()
            .filter(|(_, (id, _, stat))| *stat != CancelStatus::CancelSent && *id != 0)
            .next()
            .map(|(k, (id, _, _))| (*k, *id))
    }

    pub fn worst_buy_price_cancel(&self) -> Option<(BuyPrice, usize)> {
        self.buys
            .iter()
            .rev()
            .filter(|(_, (id, _, stat))| *stat != CancelStatus::CancelSent && *id != 0)
            .next()
            .map(|(k, (id, _, _))| (*k, *id))
    }

    pub fn worst_sell_price_cancel(&self) -> Option<(SellPrice, usize)> {
        self.sells
            .iter()
            .rev()
            .filter(|(_, (id, _, stat))| *stat != CancelStatus::CancelSent && *id != 0)
            .next()
            .map(|(k, (id, _, _))| (*k, *id))
    }

    pub fn num_uncanceled_buys(&self) -> usize {
        self.buys
            .iter()
            .filter(|(_, (id, _, stat))| *stat != CancelStatus::CancelSent && *id != 0)
            .count()
    }

    pub fn num_uncanceled_sells(&self) -> usize {
        self.sells
            .iter()
            .filter(|(_, (id, _, stat))| *stat != CancelStatus::CancelSent && *id != 0)
            .count()
    }

    pub fn cancel_buy_at(&mut self, price: BuyPrice, in_id: usize) -> bool {
        match self.buys.get_mut(&price) {
            Some((id, _, stat))
                if *id != 0 && *stat != CancelStatus::CancelSent && *id == in_id =>
            {
                *stat = CancelStatus::CancelSent;
                true
            }
            _ => false,
        }
    }

    pub fn cancel_sell_at(&mut self, price: SellPrice, in_id: usize) -> bool {
        match self.sells.get_mut(&price) {
            Some((id, _, stat))
                if *id != 0 && *stat != CancelStatus::CancelSent && *id == in_id =>
            {
                *stat = CancelStatus::CancelSent;
                true
            }
            _ => false,
        }
    }

    pub fn ack_buy_cancel(&mut self, price: BuyPrice, in_id: usize) -> f64 {
        let (id, amount, stat) = self.buys.get_mut(&price).unwrap();
        assert_eq!(*stat, CancelStatus::CancelSent);
        assert_eq!(in_id, *id);
        let amount = *amount;
        self.buys.remove(&price);
        amount
    }

    pub fn ack_sell_cancel(&mut self, price: SellPrice, in_id: usize) -> f64 {
        let (id, amount, stat) = self.sells.get_mut(&price).unwrap();
        assert_eq!(*stat, CancelStatus::CancelSent);
        assert_eq!(in_id, *id);
        let amount = *amount;
        self.sells.remove(&price);
        amount
    }

    pub fn num_buys(&self) -> usize {
        self.buys.len()
    }

    pub fn num_sells(&self) -> usize {
        self.sells.len()
    }

    pub fn give_id<P: SidedPrice + Debug>(&mut self, price: &P, id: usize, amount: f64) {
        assert_ne!(id, 0);
        match P::SIDE {
            Side::Buy => {
                let price = price.to_buy();
                let order = self.buys.get_mut(&price).unwrap();
                assert_eq!(order.0, 0);
                order.0 = id;
                order.1 = amount;
            }
            Side::Sell => {
                let price = price.to_sell();
                let order = self.sells.get_mut(&price).unwrap();
                assert_eq!(order.0, 0);
                order.0 = id;
                order.1 = amount;
            }
        };
    }

    pub fn remove_liquidity_from<P: SidedPrice>(
        &mut self,
        price: &P,
        amount: f64,
        id: usize,
    ) -> bool {
        match P::SIDE {
            Side::Buy => {
                let price = price.to_buy();
                match self.buys.entry(price) {
                    Entry::Occupied(mut occ) if occ.get().0 == id => {
                        occ.get_mut().1 -= amount;
                        if occ.get_mut().1 <= 0.0000001 {
                            occ.remove_entry();
                        }
                        true
                    }
                    _ => false,
                }
            }
            Side::Sell => {
                let price = price.to_sell();
                match self.sells.entry(price) {
                    Entry::Occupied(mut occ) if occ.get().0 == id => {
                        occ.get_mut().1 -= amount;
                        if occ.get_mut().1 <= 0.0000001 {
                            occ.remove_entry();
                        }
                        true
                    }
                    _ => false,
                }
            }
        }
    }

    pub fn get_html_info(&self) -> String {
        let buys: Vec<_> = self
            .buys
            .iter()
            .map(|(prc, (_, size, _))| format!("({:.2}x{:.4})", prc.unsigned() as f64 * 0.01, size))
            .collect();
        let sells: Vec<_> = self
            .sells
            .iter()
            .map(|(prc, (_, size, _))| format!("({:.2}x{:.4})", prc.unsigned() as f64 * 0.01, size))
            .collect();

        format!(
            "{}",
            html! {
                h3(id="open orders") : "Open Orders";
                ul(id="Tactic Summary") {
                    li(first?=true, class="item") {
                        : format!("Open buys: {:?}", buys);
                    }
                    li(first?=true, class="item") {
                        : format!("Open sells: {:?}", sells);
                    }
                }
            }
        )
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
    fn test_no_duplicate() {
        let mut manager = OrderManager::new();
        assert!(manager.add_sent_order(&BuyPrice::new(10), 10.0));
        assert!(!manager.can_place_at(&BuyPrice::new(10)));
        assert!(!manager.add_sent_order(&BuyPrice::new(10), 10.0));
        assert!(manager.add_sent_order(&SellPrice::new(11), 10.0));
        assert!(!manager.can_place_at(&SellPrice::new(11)));
        assert!(!manager.add_sent_order(&SellPrice::new(11), 10.0));
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
        manager.give_id(&BuyPrice::new(10), 1, 10.0);
        assert!(!manager.add_sent_order(&SellPrice::new(10), 10.0));
        assert!(manager.remove_liquidity_from(&BuyPrice::new(10), 5.0, 1));
        assert!(!manager.add_sent_order(&SellPrice::new(10), 10.0));
        assert!(!manager.remove_liquidity_from(&BuyPrice::new(10), 5.0, 2));
        assert!(!manager.add_sent_order(&SellPrice::new(10), 10.0));
        assert!(manager.remove_liquidity_from(&BuyPrice::new(10), 5.0, 1));
        assert!(manager.add_sent_order(&SellPrice::new(10), 10.0));
    }
}
