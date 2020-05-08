use crate::bitstamp_http::{Trade, Transaction};
use crate::exchange::normalized::Side;
use crate::local_book::InsideOrder;
use crate::order_book::{BuyPrice, SellPrice, SidedPrice};
use crate::order_manager::OrderManager;
use crate::position_manager::PositionManager;

use horrorshow::html;

use std::collections::HashSet;

pub struct Tactic {
    update_count: usize,
    required_profit: f64,
    required_fees: f64,
    imbalance_adjust: f64,

    cost_of_position: f64,

    cancel_mult: f64,

    max_orders_side: usize,
    worry_orders_side: usize,

    order_manager: OrderManager,

    official_position: PositionManager,
    position: PositionManager,

    estimated_transactions: Vec<Trade>,

    last_verified_transaction_id: usize,
}

impl Tactic {
    pub fn new(
        profit_bps: f64,
        fee_bps: f64,
        cost_of_position: f64,
        position: PositionManager,
    ) -> Tactic {
        Tactic {
            update_count: 1,
            required_profit: 0.01 * profit_bps,
            required_fees: 0.01 * fee_bps,
            imbalance_adjust: 0.2,
            cancel_mult: 0.5,
            order_manager: OrderManager::new(),
            estimated_transactions: Vec::new(),
            official_position: position.clone(),
            max_orders_side: 12,
            worry_orders_side: 12,
            position,
            cost_of_position,
            last_verified_transaction_id: 0,
        }
    }

    pub fn handle_book_update(&mut self, fair: f64, adjust: f64, premium_imbalance: f64) {
        let adjusted_fair = fair + adjust;
        while let Some((price, id)) = self.order_manager.best_buy_price_cancel() {
            let buy_prc = price.unsigned() as f64 * 0.01;
            if self.consider_order_cancel(adjusted_fair, premium_imbalance, buy_prc, Side::Buy) {
                let cid = self.order_manager.cancel_buy_at(price);
                assert_eq!(cid, id);
                self.send_buy_cancel_for(id, price);
            } else {
                break;
            }
        }
        while let Some((price, id)) = self.order_manager.best_sell_price_cancel() {
            let sell_prc = price.unsigned() as f64 * 0.01;
            if self.consider_order_cancel(adjusted_fair, premium_imbalance, sell_prc, Side::Sell) {
                let cid = self.order_manager.cancel_sell_at(price);
                assert_eq!(cid, id);
                self.send_sell_cancel_for(id, price);
            } else {
                break;
            }
        }
        while self.order_manager.num_uncanceled_buys() >= self.worry_orders_side {
            if let Some((price, id)) = self.order_manager.worst_buy_price_cancel() {
                let cid = self.order_manager.cancel_buy_at(price);
                assert_eq!(cid, id);
                self.send_buy_cancel_for(id, price);
            } else {
                break;
            }
        }
        while self.order_manager.num_uncanceled_sells() >= self.worry_orders_side {
            if let Some((price, id)) = self.order_manager.worst_sell_price_cancel() {
                let cid = self.order_manager.cancel_sell_at(price);
                assert_eq!(cid, id);
                self.send_sell_cancel_for(id, price);
            } else {
                break;
            }
        }
    }

    fn send_buy_cancel_for(&mut self, id: usize, price: BuyPrice) {
        // TODO send via http client
        self.order_manager.ack_buy_cancel(price, id);
        println!(
            "Cancelled BUY order at {:2}",
            price.unsigned() as f64 * 0.01
        );
    }

    fn send_sell_cancel_for(&mut self, id: usize, price: SellPrice) {
        // TODO send via http client
        self.order_manager.ack_sell_cancel(price, id);
        println!(
            "Cancelled SELL order at {:2}",
            price.unsigned() as f64 * 0.01
        );
    }

    fn get_position_imbalance_cost(&self, fair: f64) -> f64 {
        self.position.get_position_imbalance(fair) * self.cost_of_position
    }

    fn consider_order_cancel(
        &self,
        around: f64,
        premium_imbalance: f64,
        prc: f64,
        side: Side,
    ) -> bool {
        let around = around - self.get_position_imbalance_cost(around);
        let required_diff = (self.required_fees + self.required_profit * self.cancel_mult) * prc;
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
        let diff = (diff + imbalance_adjustment) * dir_mult;

        diff < required_diff
    }

    fn consider_order_placement(
        &self,
        around: f64,
        premium_imbalance: f64,
        prc: f64,
        side: Side,
    ) -> bool {
        // if we have too many dollars, the position imbalance is positive so we downadjust the
        // fair. Too many coins, we upadjust the fair
        let around = around - self.get_position_imbalance_cost(around);
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
        let diff = (diff + imbalance_adjustment) * dir_mult;
        diff >= required_diff
    }

    // as far as I can tell, the new order stream is almost ALWAYS newer than
    // the L2 stream. hence, I don't add logic here to arbitrate between the bbo
    // and new orders for placement
    pub fn handle_new_orders(
        &mut self,
        fair: f64,
        adjust: f64,
        premium_imbalance: f64,
        orders: &[InsideOrder],
    ) {
        let adjusted_fair = fair + adjust;
        if let Some(first_buy) = orders.iter().filter(|o| o.side == Side::Buy).next() {
            if self.max_orders_side <= self.order_manager.num_buys() {
                return;
            }
            if let Some((highest_buy, _)) = self.order_manager.best_buy_price_cancel() {
                if highest_buy.unsigned() >= first_buy.insert_price {
                    return;
                }
            }
            assert!(first_buy.side == Side::Buy);
            let buy_prc = first_buy.insert_price as f64 * 0.01;
            if self.consider_order_placement(adjusted_fair, premium_imbalance, buy_prc, Side::Buy) {
                if self
                    .order_manager
                    .add_sent_order(&BuyPrice::new(first_buy.insert_price), 200.0)
                {
                    println!("Sent BUY at {:.2} size {:.4}", buy_prc, 200.0 / buy_prc);
                    self.order_manager
                        .give_id(&BuyPrice::new(first_buy.insert_price), 1);
                }
            }
        }
        if let Some(first_sell) = orders.iter().filter(|o| o.side == Side::Sell).next() {
            if self.max_orders_side <= self.order_manager.num_sells() {
                return;
            }
            if let Some((highest_sell, _)) = self.order_manager.best_sell_price_cancel() {
                if highest_sell.unsigned() <= first_sell.insert_price {
                    return;
                }
            }
            assert!(first_sell.side == Side::Sell);
            let sell_prc = first_sell.insert_price as f64 * 0.01;
            if self.consider_order_placement(adjusted_fair, premium_imbalance, sell_prc, Side::Sell)
            {
                if self
                    .order_manager
                    .add_sent_order(&SellPrice::new(first_sell.insert_price), 200.0)
                {
                    println!("Sent SELL at {:.2} size {:.4}", sell_prc, 200.0 / sell_prc);
                    self.order_manager
                        .give_id(&SellPrice::new(first_sell.insert_price), 1);
                }
            }
        }
    }

    pub fn check_seen_trade(&mut self, trade: Trade) {
        if self.last_verified_transaction_id >= trade.id {
            return;
        }

        let as_buy = BuyPrice::new((trade.price * 100.0) as usize);
        let as_sell = SellPrice::new((trade.price * 100.0) as usize);

        let fee = trade.price as f64 * trade.amount * self.position.get_fee_estimate();

        if self
            .order_manager
            .remove_liquidity_from(&as_buy, trade.amount, trade.buy_order_id)
        {
            // Our purchase succeeded, as far as we know
            self.position.buy_coins(trade.amount, trade.price, fee);
            self.estimated_transactions.push(trade);
        } else if self.order_manager.remove_liquidity_from(
            &as_sell,
            trade.amount,
            trade.sell_order_id,
        ) {
            self.position.sell_coins(trade.amount, trade.price, fee);
            self.estimated_transactions.push(trade);
        }
    }

    pub fn sync_transactions(&mut self, transactions: Vec<Transaction>) {
        if transactions.len() == 0 {
            return;
        }

        let min_id = transactions.iter().map(|t| t.id).min().unwrap();
        let max_id = transactions.iter().map(|t| t.id).max().unwrap();

        if min_id <= self.last_verified_transaction_id {
            panic!("God repeat transaction id");
        }

        let transactions_seen: HashSet<_> = transactions.iter().map(|t| t.id).collect();

        self.estimated_transactions
            .retain(|t| !transactions_seen.contains(&t.id));

        let min_remaining_trade = self
            .estimated_transactions
            .iter()
            .map(|t| t.id)
            .min()
            .unwrap_or(std::usize::MAX);

        if min_remaining_trade <= max_id {
            panic!(
                "New set of transactions missed a trade we observed: id {}",
                min_remaining_trade
            );
        }

        for transaction in transactions {
            self.official_position
                .set_fee_estimate(transaction.fee / (transaction.btc * transaction.btc_usd));
            match transaction.side {
                Side::Buy => self.official_position.buy_coins(
                    transaction.btc,
                    transaction.btc_usd,
                    transaction.fee,
                ),
                Side::Sell => self.official_position.sell_coins(
                    transaction.btc,
                    transaction.btc_usd,
                    transaction.fee,
                ),
            }
        }

        self.position = self.official_position.clone();

        // re-apply the remaining leftover trades
        for trade in self.estimated_transactions.iter() {
            let fee = trade.price as f64 * trade.amount * self.position.get_fee_estimate();
            match trade.side {
                Side::Buy => self.position.buy_coins(trade.amount, trade.price, fee),
                Side::Sell => self.position.sell_coins(trade.amount, trade.price, fee),
            }
        }
    }

    pub fn get_html_info(&self, fair: f64) -> String {
        let (desired_d, desired_c) = self.position.get_desired_position(fair);
        let imbalance = self.position.get_position_imbalance(fair);
        format!(
            "{}{}",
            html! {
                h3(id="tactic state", clas="title") : "Tactic State Summary";
                ul(id="Tactic summary") {
                    li(first?=true, class="item") {
                        : format!("Balance: {:.2} usd, {:.4} btc",
                                  self.position.dollars_balance,self.position.coins_balance);
                    }
                    li(first?=false, class="item") {
                        : format!("Available: {:.2} usd, {:.4} btc",
                                  self.position.dollars_available,self.position.coins_available);
                    }
                    li(first?=false, class="item") {
                        : format!("Desired: {:.2} usd, {:.4} btc",
                                  desired_d, desired_c);
                    }
                    li(first?=true, class="item") {
                        : format!("Last Official Balance: {:.2} usd, {:.4} btc",
                                  self.official_position.dollars_balance,self.official_position.coins_balance);
                    }
                    li(first?=false, class="item") {
                        : format!("Position imbalance: {:.2} usd, price offset {:.3}",
                                  imbalance, imbalance * self.cost_of_position);
                    }
                    li(first?=false, class="item") {
                        : format!("Estimated fee: {:.2}", self.official_position.get_fee_estimate());
                    }
                    li(first?=false, class="item") {
                        : format!("Unverified trades: {:?}", self.estimated_transactions);
                    }
                }
            },
            self.order_manager.get_html_info(),
        )
    }
}
