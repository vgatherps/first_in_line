use serde::Deserialize;
use std::collections::BTreeMap;

pub fn get_price_from_id(in_prc: usize) -> usize {
    (88 * 100000000usize).checked_sub(in_prc).unwrap()
}

#[derive(Debug)]
pub struct BBOClearEvent {
    pub side: Side,
    pub price: usize,
}

#[derive(Deserialize, Eq, PartialEq, Debug)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Deserialize, Eq, PartialEq, Debug)]
pub struct LevelDelete {
    id: usize,
    side: Side,
}

#[derive(Deserialize, Eq, PartialEq, Debug)]
pub struct LevelUpdate {
    pub id: usize,
    pub side: Side,
    pub size: usize,
}

#[derive(Deserialize, Eq, PartialEq, Debug)]
#[serde(tag = "action", content = "data")]
#[serde(rename_all = "lowercase")]
pub enum LevelData {
    Partial(Vec<LevelUpdate>),
    Insert(Vec<LevelUpdate>),
    Update(Vec<LevelUpdate>),
    Delete(Vec<LevelDelete>),
}

#[derive(Ord, PartialOrd, Clone, Copy, Debug, Eq, PartialEq)]
struct BuyPrice {
    value: i64,
}

#[derive(Ord, PartialOrd, Clone, Copy, Debug, Eq, PartialEq)]
struct SellPrice {
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

    fn unsigned(&self) -> usize {
        assert!(self.value > 0);
        self.value as usize
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

    fn unsigned(&self) -> usize {
        assert!(self.value < 0);
        (self.value * -1) as usize
    }
}

pub struct OrderBook {
    bids: BTreeMap<BuyPrice, usize>,
    asks: BTreeMap<SellPrice, usize>,
}

impl OrderBook {
    pub fn new() -> OrderBook {
        OrderBook {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        }
    }

    pub fn update_level(&mut self, update: &LevelUpdate) {
        assert_ne!(update.size, 0);
        let raw_price = get_price_from_id(update.id);
        match update.side {
            Side::Buy => {
                let price = BuyPrice::new(raw_price);
                let level = self
                    .bids
                    .get_mut(&price)
                    .expect("Got level update for nonexistent level");
                *level = update.size;
            }
            Side::Sell => {
                let price = SellPrice::new(raw_price);
                let level = self
                    .asks
                    .get_mut(&price)
                    .expect("Got level update for nonexistent level");
                *level = update.size;
            }
        }
    }

    pub fn add_level(&mut self, update: &LevelUpdate) {
        assert_ne!(update.size, 0);
        let raw_price = get_price_from_id(update.id);
        match update.side {
            Side::Buy => {
                let price = BuyPrice::new(raw_price);
                assert_eq!(self.bids.insert(price, update.size), None);
            }
            Side::Sell => {
                let price = SellPrice::new(raw_price);
                assert_eq!(self.asks.insert(price, update.size), None);
            }
        }
    }

    pub fn delete_level(&mut self, delete: &LevelDelete) -> Option<BBOClearEvent> {
        let raw_price = get_price_from_id(delete.id);
        let (best_bid, best_ask) = self.bbo();
        let (test_price, test_side) = match delete.side {
            Side::Buy => {
                let price = BuyPrice::new(raw_price);
                self.bids
                    .remove(&price)
                    .expect("Deleted a level that doesn't exist");
                (best_bid, Side::Buy)
            }
            Side::Sell => {
                let price = SellPrice::new(raw_price);
                self.asks
                    .remove(&price)
                    .expect("Deleted a level that doesn't exist");
                (best_ask, Side::Sell)
            }
        };
        match test_price {
            Some(test_price) if test_price == raw_price => Some(BBOClearEvent {
                side: test_side,
                price: test_price,
            }),
            _ => None,
        }
    }

    pub fn bbo(&self) -> (Option<usize>, Option<usize>) {
        (
            self.bids.keys().next().map(|a| (-a.value) as usize),
            self.asks.keys().next().map(|a| a.value as usize),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_deserialize() {
        let val: LevelData = serde_json::from_str("
{\"table\":\"orderBookL2\",\"action\":\"update\",\"data\":[{\"symbol\":\"XBTUSD\",\"id\":8799279500,\"side\":\"Buy\",\"size\":21944}]}
        ").unwrap();
        assert_eq!(
            val,
            LevelData::Update(vec![LevelUpdate {
                id: 8799279500,
                side: Side::Buy,
                size: 21944,
            }])
        )
    }
}
