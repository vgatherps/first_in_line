use std::collections::HashMap;

use crate::order_book::get_price_from_id;
use crate::order_book::{BBOClearEvent, LevelUpdate, Side};

#[derive(Default)]
pub struct ClearedNBBO {
    bids: HashMap<usize, u128>,
    asks: HashMap<usize, u128>,
}

impl ClearedNBBO {
    pub fn handle_cleared_bbo(&mut self, event: &BBOClearEvent, time: u128) {
        match event.side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
        .insert(event.price, time);
    }

    pub fn handle_inserted_level(&mut self, update: &LevelUpdate) -> Option<u128> {
        match update.side {
            Side::Buy => &mut self.asks,
            Side::Sell => &mut self.bids,
        }
        .remove(&get_price_from_id(update.id))
    }
}
