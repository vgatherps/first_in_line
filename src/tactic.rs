use crate::exchange::normalized::Side;
use crate::local_book::InsideOrder;

pub struct Tactic {
    update_count: usize,
    required_profit: f64,
    required_fees: f64,
    imbalance_adjust: f64,
}

impl Tactic {
    pub fn new() -> Tactic {
        Tactic {
            update_count: 1,
            required_profit: 0.01 * 0.02,
            required_fees: 0.01 * 0.03,
            imbalance_adjust: 0.2,
        }
    }

    pub fn handle_book_update(
        &mut self,
        bbo: ((usize, f64), (usize, f64)),
        fair: f64,
        adjust: f64,
    ) {
    }

    fn consider_order_placement(
        &mut self,
        bbo: (usize, usize),
        around: f64,
        premium_imbalance: f64,
        prc: f64,
        side: Side,
    ) {
        let required_diff = (self.required_fees + self.required_profit) * prc;
        let dir_mult = match side {
            Side::Buy => -1.0,
            Side::Sell => 1.0,
        };
        // if we're too high, this is positive, so we subtract from the diff
        // if we're too low, this is negative, to we flip first before subtracting
        // Here, I do so before multiplying but it has the same effect
        let imbalance_adjustment = premium_imbalance * self.imbalance_adjust;
        let diff = prc - around;
        let diff = diff.min(30.0).max(-30.0);
        let diff =  (diff + imbalance_adjustment) * dir_mult;
        if diff >= required_diff {
            println!(
                "Would chase new order price {:.2}, side {:?}, {:.2}x{:.2}, around {:.2}, imbalance_adjust {:.3}, req {:.3}, diff {:.3}",
                prc, side, bbo.0 as f64 * 0.01, bbo.1 as f64 * 0.01, around, imbalance_adjustment, required_diff, diff
            );
        }
    }

    // as far as I can tell, the new order stream is almost ALWAYS newer than
    // the L2 stream. hence, I don't add logic here to arbitrate between the bbo
    // and new orders for placement
    pub fn handle_new_orders(
        &mut self,
        bbo: (usize, usize),
        fair: f64,
        adjust: f64,
        premium_imbalance: f64,
        orders: &[InsideOrder],
    ) {
        let adjusted_fair = fair + adjust;
        if let Some(first_buy) = orders.iter().filter(|o| o.side == Side::Buy).next() {
            assert!(first_buy.side == Side::Buy);
            let buy_prc = first_buy.insert_price as f64 * 0.01;
            self.consider_order_placement(bbo, adjusted_fair, premium_imbalance, buy_prc, Side::Buy);
        }
        if let Some(first_sell) = orders.iter().filter(|o| o.side == Side::Sell).next() {
            assert!(first_sell.side == Side::Sell);
            let sell_prc = first_sell.insert_price as f64 * 0.01;
            self.consider_order_placement(bbo, adjusted_fair, premium_imbalance, sell_prc, Side::Sell);
        }
    }
}
