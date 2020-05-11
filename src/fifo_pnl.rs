use std::collections::VecDeque;

use crate::order_book::{BuyPrice, SellPrice, SidedPrice};

// This could be compressed into one vector, since we never mix buys and sells
// I find it clearer not to do that
#[derive(Default)]
pub struct Fifo {
    buys: VecDeque<(BuyPrice, usize)>,
    sells: VecDeque<(SellPrice, usize)>,
    pnl_btc: f64,
    xbt_traded: f64,
}

impl Fifo {
    fn validate(&self) {
        assert!(self.sells.len() == 0 || self.buys.len() == 0);
    }

    pub fn xbt(&self) -> f64 {
        self.pnl_btc
    }

    pub fn xbt_traded(&self) -> f64 {
        self.xbt_traded
    }

    pub fn add_buy(&mut self, buy_price: BuyPrice, mut size: usize) {
        self.validate();
        while let Some((sell_price, sell_size)) = self.sells.get_mut(0) {
            let sellp = sell_price.unsigned() as f64 * 0.01;
            let buyp = buy_price.unsigned() as f64 * 0.01;
            // convert to difference in btc
            let difference = 1.0 / buyp - 1.0 / sellp;
            if *sell_size > size {
                let size_to_trade = size;
                self.pnl_btc += difference * size_to_trade as f64;
                self.xbt_traded += (1.0 / sellp + 1.0 / buyp) * size_to_trade as f64;
                *sell_size -= size;
                return;
            } else {
                let size_to_trade = *sell_size;
                self.pnl_btc += difference * size_to_trade as f64;
                self.xbt_traded += (1.0 / sellp + 1.0 / buyp) * size_to_trade as f64;
                size -= *sell_size;

                self.sells.pop_front();
                if size <= 0 {
                    return;
                }
            }
        }

        if size > 0 {
            self.buys.push_back((buy_price, size));
        }
    }

    pub fn add_sell(&mut self, sell_price: SellPrice, mut size: usize) {
        self.validate();
        while let Some((buy_price, buy_size)) = self.buys.get_mut(0) {
            let sellp = sell_price.unsigned() as f64 * 0.01;
            let buyp = buy_price.unsigned() as f64 * 0.01;
            let difference = 1.0 / buyp - 1.0 / sellp;
            if *buy_size > size {
                let size_to_trade = size;
                self.pnl_btc += difference * size_to_trade as f64;
                self.xbt_traded += (sellp + buyp) * size_to_trade as f64;
                *buy_size -= size;
                return;
            } else {
                let size_to_trade = *buy_size;
                self.pnl_btc += difference * size_to_trade as f64;
                self.xbt_traded += (sellp + buyp) * size_to_trade as f64;
                size -= *buy_size;

                self.buys.pop_front();
                if size <= 0 {
                    return;
                }
            }
        }

        if size > 0 {
            self.sells.push_back((sell_price, size));
        }
    }
}
