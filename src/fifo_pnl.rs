use std::collections::VecDeque;

use crate::order_book::{BuyPrice, SellPrice, SidedPrice};


// This could be compressed into one vector, since we never mix buys and sells
// I find it clearer not to do that
#[derive(Default)]
pub struct Fifo {
    buys: VecDeque<(BuyPrice, f64)>,
    sells: VecDeque<(SellPrice, f64)>,
    pnl: f64,
    dollars: f64,
}

impl Fifo {

    fn validate(&self) {
        assert!(self.sells.len() == 0 || self.buys.len() == 0);
    }

    pub fn pnl(&self) -> f64 {
        self.pnl
    }

    pub fn dollars(&self) -> f64 {
        self.dollars
    }
    
    pub fn add_buy(&mut self, buy_price: BuyPrice, mut size: f64) {
        self.validate();
        while let Some((sell_price, sell_size)) = self.sells.get_mut(0) {
            let sellp = sell_price.unsigned() as f64 * 0.01;
            let buyp = buy_price.unsigned() as f64 * 0.01;
            let difference = sellp - buyp;
            if *sell_size > size {
                let size_to_trade = size;
                self.pnl += difference * size_to_trade;
                self.dollars += (sellp + buyp) * size_to_trade;
                *sell_size -= size;
                return;
            } else {
                let size_to_trade = *sell_size;
                self.pnl += difference * size_to_trade;
                self.dollars += (sellp + buyp) * size_to_trade;
                size -= *sell_size;

                self.sells.pop_front();
                if size <= 0.0 {
                    return;
                }
            }
        }

        if size > 0.0 {
            self.buys.push_back((buy_price, size));
        }
    }

    pub fn add_sell(&mut self, sell_price: SellPrice, mut size: f64) {
        self.validate();
        while let Some((buy_price, buy_size)) = self.buys.get_mut(0) {
            let sellp = sell_price.unsigned() as f64 * 0.01;
            let buyp = buy_price.unsigned() as f64 * 0.01;
            let difference = sellp - buyp;
            if *buy_size > size {
                let size_to_trade = size;
                self.pnl += difference * size_to_trade;
                self.dollars += (sellp + buyp) * size_to_trade;
                *buy_size -= size;
                return;
            } else {
                let size_to_trade = *buy_size;
                self.pnl += difference * size_to_trade;
                self.dollars += (sellp + buyp) * size_to_trade;
                size -= *buy_size;

                self.buys.pop_front();
                if size <= 0.0 {
                    return;
                }
            }
        }

        if size > 0.0 {
            self.sells.push_back((sell_price, size));
        }
    }
}
