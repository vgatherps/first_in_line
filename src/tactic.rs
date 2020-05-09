use crate::bitstamp_http::{BitstampHttp, OrderCanceled, OrderSent};
use crate::exchange::normalized::{convert_price_cents, Side, TradeUpdate};
use crate::local_book::InsideOrder;
use crate::order_book::{BuyPrice, SellPrice, SidedPrice};
use crate::order_manager::OrderManager;
use crate::position_manager::PositionManager;

use horrorshow::html;
use xorshift::{Rng, Xorshift128};

use std::collections::VecDeque;
use std::time::SystemTime;

use std::sync::atomic::Ordering;

pub struct Tactic {
    required_profit: f64,
    required_fees: f64,
    imbalance_adjust: f64,

    cost_of_position: f64,

    cancel_mult: f64,

    max_orders_side: usize,
    worry_orders_side: usize,
    max_send: usize,
    orders_sent: usize,
    orders_canceled: usize,
    missed_cancels: usize,

    fees_paid: f64,
    initial_usd: f64,
    initial_btc: f64,

    recent_trades: VecDeque<(Side, f64, f64)>,

    order_manager: OrderManager,

    position: PositionManager,

    rng: Xorshift128,

    main_loop_not: tokio::sync::mpsc::Sender<crate::TacticInternalEvent>,
    http: std::sync::Arc<BitstampHttp>,
}

fn adjust_coins(coins: f64) -> f64 {
    (coins * 10000.0).round() / 10000.0
}

fn le_compare(a: f64, b: f64) -> bool {
    (a - 0.000001) <= b
}

// TODO should really care about exchange times

async fn order_caller(
    amount: f64,
    price: f64,
    side: Side,
    http: std::sync::Arc<BitstampHttp>,
    mut eventer: tokio::sync::mpsc::Sender<crate::TacticInternalEvent>,
) {
    let _guard = scopeguard::guard((), |_| {
        if std::thread::panicking() {
            crate::DIE.store(true, Ordering::Relaxed);
        }
    });
    let send = SystemTime::now();
    let sent = http.send_order(amount, price, side, http.clone()).await;
    if let Ok(diff) = std::time::SystemTime::now().duration_since(send) {
        println!("Send took {} ms", diff.as_millis())
    }
    assert!(eventer
        .send(crate::TacticInternalEvent::OrderSent(sent))
        .await
        .is_ok());
}

async fn cancel_caller(
    id: usize,
    http: std::sync::Arc<BitstampHttp>,
    mut eventer: tokio::sync::mpsc::Sender<crate::TacticInternalEvent>,
) {
    let _guard = scopeguard::guard((), |_| {
        if std::thread::panicking() {
            crate::DIE.store(true, Ordering::Relaxed);
        }
    });
    let send = SystemTime::now();
    if let Some(cancel) = http.send_cancel(id, http.clone()).await {
        if let Ok(diff) = std::time::SystemTime::now().duration_since(send) {
            println!("Cancel took {} ms", diff.as_millis())
        }
        assert!(eventer
            .send(crate::TacticInternalEvent::OrderCanceled(cancel))
            .await
            .is_ok());
    }
}

impl Tactic {
    pub fn new(
        profit_bps: f64,
        fee_bps: f64,
        cost_of_position: f64,
        position: PositionManager,
        http: std::sync::Arc<crate::bitstamp_http::BitstampHttp>,
        main_loop_not: tokio::sync::mpsc::Sender<crate::TacticInternalEvent>,
    ) -> Tactic {

        Tactic {
            required_profit: 0.01 * profit_bps,
            required_fees: 0.01 * fee_bps,
            imbalance_adjust: 0.2,
            cancel_mult: 0.3,
            order_manager: OrderManager::new(),
            max_orders_side: 16,
            worry_orders_side: 10,
            fees_paid: 0.0,
            initial_usd: position.dollars_balance,
            initial_btc: position.coins_balance,
            max_send: 100000,
            orders_sent: 0,
            orders_canceled: 0,
            missed_cancels: 0,
            recent_trades: VecDeque::new(),
            // reproducible seed is fine here, better for sims
            rng: xorshift::thread_rng(),
            position,
            cost_of_position,
            http,
            main_loop_not,
        }
    }

    pub fn handle_book_update(
        &mut self,
        bbo: (usize, usize),
        fair: f64,
        adjust: f64,
        premium_imbalance: f64,
    ) {
        let adjusted_fair = fair + adjust;
        while let Some((price, id)) = self.order_manager.best_buy_price_cancel() {
            let buy_prc = price.unsigned() as f64 * 0.01;
            if self.consider_order_cancel(adjusted_fair, premium_imbalance, buy_prc, Side::Buy) {
                assert!(self.order_manager.cancel_buy_at(price, id));
                self.send_buy_cancel_for(id, price);
            } else {
                break;
            }
        }
        while let Some((price, id)) = self.order_manager.best_sell_price_cancel() {
            let sell_prc = price.unsigned() as f64 * 0.01;
            if self.consider_order_cancel(adjusted_fair, premium_imbalance, sell_prc, Side::Sell) {
                assert!(self.order_manager.cancel_sell_at(price, id));
                self.send_sell_cancel_for(id, price);
            } else {
                break;
            }
        }
        let (bid, offer) = (BuyPrice::new(bbo.0), SellPrice::new(bbo.1));
        while let Some((price, id)) = self.order_manager.best_buy_price_late() {
            if price > bid && (price.unsigned() - bid.unsigned()) > 1 {
                assert!(self.order_manager.cancel_buy_at(price, id));
                self.send_buy_cancel_for(id, price);
            } else {
                break;
            }
        }
        while let Some((price, id)) = self.order_manager.best_sell_price_late() {
            if offer > price && (offer.unsigned() - price.unsigned()) > 1 {
                assert!(self.order_manager.cancel_sell_at(price, id));
                self.send_sell_cancel_for(id, price);
            } else {
                break;
            }
        }
        while self.order_manager.num_uncanceled_buys() >= self.worry_orders_side {
            if let Some((price, id)) = self.order_manager.worst_buy_price_cancel() {
                assert!(self.order_manager.cancel_buy_at(price, id));
                self.send_buy_cancel_for(id, price);
            } else {
                break;
            }
        }
        while self.order_manager.num_uncanceled_sells() >= self.worry_orders_side {
            if let Some((price, id)) = self.order_manager.worst_sell_price_cancel() {
                assert!(self.order_manager.cancel_sell_at(price, id));
                self.send_sell_cancel_for(id, price);
            } else {
                break;
            }
        }
    }

    pub fn cancel_stale_id(&mut self, id: usize, price: usize, side: Side) {
        match side {
            Side::Buy => {
                let bprice = BuyPrice::new(price);
                if  self.order_manager.cancel_buy_at(bprice, id)
                {
                    self.send_buy_cancel_for(id, bprice);
                }
            }
            Side::Sell => {
                let sprice = SellPrice::new(price);
                if self.order_manager.cancel_sell_at(sprice, id)
                {
                    self.send_sell_cancel_for(id, sprice);
                }
            }
        }
    }

    // if an order isn't gone after we canceled it, reset our state
    pub fn check_order_gone(&mut self, id: usize, price: usize, side: Side) {
        match side {
            Side::Buy => {
                let bprice = BuyPrice::new(price);
                if self.order_manager.has_buy_order(bprice, id)
                {
                    self.reset();
                }
            }
            Side::Sell => {
                let sprice = SellPrice::new(price);
                if self.order_manager.has_sell_order(sprice, id)
                {
                    self.reset();
                }
            }
        }
    }

    pub fn set_late_status(&mut self, id: usize, price: usize, side: Side) {
        match side {
            Side::Buy => {
                let price = BuyPrice::new(price);
                self.order_manager.set_late_buy_status(price, id);
            }
            Side::Sell => {
                let price = SellPrice::new(price);
                self.order_manager.set_late_sell_status(price, id);
            }
        }
    }

    fn send_buy_cancel_for(&mut self, id: usize, price: BuyPrice) {
        tokio::task::spawn(cancel_caller(
            id,
            self.http.clone(),
            self.main_loop_not.clone(),
        ));
        println!(
            "Cancelled BUY order at {:2} SENT",
            price.unsigned() as f64 * 0.01
        );
    }

    fn send_sell_cancel_for(&mut self, id: usize, price: SellPrice) {
        tokio::task::spawn(cancel_caller(
            id,
            self.http.clone(),
            self.main_loop_not.clone(),
        ));
        println!(
            "Cancelled SELL order at {:2} SENT",
            price.unsigned() as f64 * 0.01
        );
    }

    // TODO this will panic on a race? Doesn't seem to be an issue
    pub fn ack_cancel_for(&mut self, cancel: &OrderCanceled) {
        println!(
            "Acking cancel for order {} price {} amount {}",
            cancel.id, cancel.price, cancel.amount
        );
        self.orders_canceled += 1;
        let cents = convert_price_cents(cancel.price);
        match cancel.side {
            Side::Buy => {
                if let Some(known_volume) = self
                    .order_manager
                    .ack_buy_cancel(BuyPrice::new(cents), cancel.id)
                {
                    if !le_compare(cancel.amount, known_volume) {
                        println!(
                            "Got buy cancel for {}, only have {}",
                            cancel.amount, known_volume
                        );
                        self.reset();
                    }
                    self.position
                        .return_buy_balance(cancel.amount * cancel.price);
                } else {
                    self.missed_cancels += 1;
                }
            }
            Side::Sell => {
                if let Some(known_volume) = self
                    .order_manager
                    .ack_sell_cancel(SellPrice::new(cents), cancel.id)
                {
                    if !le_compare(cancel.amount, known_volume) {
                        println!(
                            "Got sell cancel for {}, only have {}",
                            cancel.amount, known_volume
                        );
                        self.reset();
                    }
                    self.position.return_sell_balance(cancel.amount);
                } else {
                    self.missed_cancels += 1;
                }
            }
        }
    }

    fn reset(&self) {
        let mut not = self.main_loop_not.clone();
        tokio::task::spawn(async move {
            assert!(not.send(crate::TacticInternalEvent::Reset(true)).await.is_ok());
        });
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
        let around = around + self.get_position_imbalance_cost(around);
        let required_diff = (self.required_fees + self.required_profit) * self.cancel_mult * prc;
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
        // if we have too many dollars, the position imbalance is positive so we adjust the fair
        // upwards, making us buy more. Selling is reversed
        let around = around + self.get_position_imbalance_cost(around);
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
        if !self.http.can_send_order() {
            return;
        }
        if self.orders_sent >= self.max_send {
            return;
        }
        // shuffle +/ 50 dollars on order size
        let base_dollars = 200.0 + (self.rng.next_f64() - 0.5) * 100.0;
        assert!(base_dollars > 100.0);
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
            let penny_prc = buy_prc + 0.01;
            let actual_buy_prc = if first_buy.insert_size > 0.5
                && self.consider_order_placement(
                    adjusted_fair,
                    premium_imbalance,
                    penny_prc,
                    Side::Buy,
                ) {
                Some(penny_prc)
            } else if self.consider_order_placement(
                adjusted_fair,
                premium_imbalance,
                buy_prc,
                Side::Buy,
            ) {
                Some(buy_prc)
            } else {
                None
            };
            if let Some(buy_prc) = actual_buy_prc {
                let buy_coins = adjust_coins(base_dollars / buy_prc);
                let buy_dollars = buy_coins * buy_prc;

                if !self
                    .order_manager
                    .can_place_at(&BuyPrice::new(convert_price_cents(buy_prc)))
                {
                    return;
                }

                if !self.position.request_buy_balance(buy_dollars) {
                    return;
                }
                if self
                    .order_manager
                    .add_sent_order(&BuyPrice::new(convert_price_cents(buy_prc)), buy_coins)
                {
                    tokio::task::spawn(order_caller(
                        buy_coins,
                        buy_prc,
                        Side::Buy,
                        self.http.clone(),
                        self.main_loop_not.clone(),
                    ));
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
            let penny_prc = sell_prc - 0.01;
            let actual_sell_prc = if first_sell.insert_size > 0.5
                && self.consider_order_placement(
                    adjusted_fair,
                    premium_imbalance,
                    penny_prc,
                    Side::Sell,
                ) {
                Some(penny_prc)
            } else if self.consider_order_placement(
                adjusted_fair,
                premium_imbalance,
                sell_prc,
                Side::Sell,
            ) {
                Some(sell_prc)
            } else {
                None
            };
            if let Some(sell_prc) = actual_sell_prc {
                let sell_dollars = base_dollars;
                let sell_coins = adjust_coins(sell_dollars / sell_prc);
                if !self
                    .order_manager
                    .can_place_at(&SellPrice::new(convert_price_cents(sell_prc)))
                {
                    return;
                }
                // TODO this only works temporarily since I don't examine trades
                if !self.position.request_sell_balance(sell_coins) {
                    return;
                }
                if self
                    .order_manager
                    .add_sent_order(&SellPrice::new(convert_price_cents(sell_prc)), sell_coins)
                {
                    tokio::task::spawn(order_caller(
                        sell_coins,
                        sell_prc,
                        Side::Sell,
                        self.http.clone(),
                        self.main_loop_not.clone(),
                    ));
                    println!("Sent SELL at {:.2} size {:.4}", sell_prc, sell_coins);
                }
            }
        }
    }

    pub fn ack_send_for(&mut self, order: &OrderSent) {
        println!("Acking send at {:.2}, {}", order.price, order.id);
        self.orders_sent += 1;
        let cents = convert_price_cents(order.price);
        match order.side {
            Side::Buy => {
                let price = BuyPrice::new(cents);
                self.order_manager.give_id(&price, order.id, order.amount);
            }
            Side::Sell => {
                let price = SellPrice::new(cents);
                self.order_manager.give_id(&price, order.id, order.amount);
            }
        }
        let mut sender = self.main_loop_not.clone();
        let side = order.side;
        let price = cents;
        let id = order.id;
        tokio::task::spawn(async move {
            // first wait 1 seconds, and set cancelable
            tokio::time::delay_for(std::time::Duration::from_millis(1000 * 1)).await;
            assert!(sender
                .send(crate::TacticInternalEvent::SetLateStatus(side, price, id))
                .await
                .is_ok());
            // wait 8ish more seconds, try and cancel order
            let random_offset = (id / 19) % 4;
            tokio::time::delay_for(std::time::Duration::from_millis(
                1000 * (5 + random_offset) as u64,
            ))
            .await;
            assert!(sender
                .send(crate::TacticInternalEvent::CancelStale(side, price, id))
                .await
                .is_ok());
            // wait 10 more seconds, try and cancel order
            // if it's still gone, we missed a trade and should reset
            tokio::time::delay_for(std::time::Duration::from_millis(
                1000 * 10 as u64,
            ))
            .await;
            assert!(sender
                .send(crate::TacticInternalEvent::CheckGone(side, price, id))
                .await
                .is_ok());
        });
    }

    pub fn check_seen_trade(&mut self, trade: &TradeUpdate) {
        let cents = trade.cents;
        let as_buy = BuyPrice::new(cents);
        let as_sell = SellPrice::new(cents);

        let dollars = cents as f64 * 0.01;

        let fee = dollars * trade.size * self.position.get_fee_estimate();

        if self
            .order_manager
            .remove_liquidity_from(&as_buy, trade.size, trade.buy_order_id)
        {
            // Our purchase succeeded, as far as we know
            self.position.buy_coins(trade.size, dollars);
            self.fees_paid += fee;
            self.recent_trades
                .push_front((Side::Buy, dollars, trade.size));

            println!(
                "Trade buy id {} price {:.2} size {:.5}",
                trade.sell_order_id, dollars, trade.size
            );
        } else if self.order_manager.remove_liquidity_from(
            &as_sell,
            trade.size,
            trade.sell_order_id,
        ) {
            self.position.sell_coins(trade.size, dollars);
            self.fees_paid += fee;
            self.recent_trades
                .push_front((Side::Sell, dollars, trade.size));
            println!(
                "Trade sell id {} price {:.2} size {:.5}",
                trade.sell_order_id, dollars, trade.size
            );
        }

        while self.recent_trades.len() > 20 {
            self.recent_trades.pop_back();
        }
    }

    /*
    pub fn sync_transactions(&mut self, transactions: &Vec<Transaction>) {
        if transactions.len() == 0 {
            return;
        }

        let min_id = transactions.iter().map(|t| t.id).min().unwrap();
        let max_id = transactions.iter().map(|t| t.id).max().unwrap();

        if min_id <= self.last_verified_transaction_id {
            panic!("Got repeat transaction id");
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
            self.fees_paid += transaction.fee;
            println!(
                "Got transaction prc {:.2} amount {:.2} side {:?}",
                transaction.btc_usd, transaction.btc, transaction.side
            );
            let cents = convert_price_cents(transaction.btc_usd);
            match transaction.side {
                Side::Buy => {
                    self.official_position.buy_coins(
                        transaction.btc,
                        transaction.btc_usd,
                    );
                    self.order_manager.remove_liquidity_from(
                        &BuyPrice::new(cents),
                        transaction.btc,
                        transaction.order_id
                    );
                }
                Side::Sell => {
                    self.official_position.sell_coins(
                        transaction.btc,
                        transaction.btc_usd,
                    );

                    self.order_manager.remove_liquidity_from(
                        &BuyPrice::new(cents),
                        transaction.btc,
                        transaction.order_id
                    );
                }
            }
        }

        self.position = self.official_position.clone();

        // re-apply the remaining leftover trades
        for trade in self.estimated_transactions.iter() {
            match trade.side {
                Side::Buy => self.position.buy_coins(trade.amount, trade.price),
                Side::Sell => self.position.sell_coins(trade.amount, trade.price),
            }
        }
    }*/

    pub fn get_html_info(&self, fair: f64) -> String {
        let (desired_d, desired_c) = self.position.get_desired_position(fair);
        let imbalance = self.position.get_position_imbalance(fair);

        let if_held_dollars = self.initial_usd + self.initial_btc * fair;
        let if_held_btc = self.initial_btc + self.initial_usd / fair;

        let up = self.position.get_total_position(fair) - if_held_dollars;

        format!(
            "{}{}",
            html! {
                h3(id="tactic state", clas="title") : "Tactic State Summary";
                ul(id="Tactic summary") {
                    li(first?=true, class="item") {
                        : format!("Balance: {:.2} usd, {:.4} btc, {:.2} total usd, {:.4} total btc",
                                  self.position.dollars_balance,self.position.coins_balance,
                                  self.position.get_total_position(fair),
                                  self.position.get_total_position(fair) / fair);
                    }
                    li(first?=false, class="item") {
                        : format!("Available: {:.2} usd, {:.4} btc",
                                  self.position.dollars_available,self.position.coins_available);
                    }
                    li(first?=true, class="item") {
                        : format!("If just held: {:.2}, total usd, {:.4} total btc. Up {:.2} usd, without fees {:.2}",
                                  if_held_dollars, if_held_btc,
                                  up, up + self.fees_paid);
                    }
                    li(first?=false, class="item") {
                        : format!("Desired: {:.2} usd, {:.4} btc",
                                  desired_d, desired_c);
                    }
                    li(first?=false, class="item") {
                        : format!("Position imbalance: {:.2} usd, price offset {:.3}",
                                  imbalance, imbalance * self.cost_of_position);
                    }
                    li(first?=false, class="item") {
                        : format!("Estimated fee bps: {:.2}, paid {:.2}", self.position.get_fee_estimate() * 100.0, self.fees_paid);
                    }
                    li(first?=false, class="item") {
                        : format!("Orders: sent {}, canceled {}, missed cancels: {}, rate {:.2}",
                                  self.orders_sent,
                                  self.orders_canceled,
                                  self.missed_cancels,
                                  if self.orders_sent == 0 {
                                      0.0
                                  } else {
                                      self.orders_canceled as f64 / self.orders_sent as f64
                                  });
                    }
                    li(first?=false, class="item") {
                        : format!("Recent trades: {:?}",
                                  self.recent_trades);
                    }
                }
            },
            self.order_manager.get_html_info(),
        )
    }
}

impl Drop for Tactic {
    fn drop(&mut self) {
        while let Some((price, id)) = self.order_manager.best_buy_price_cancel() {
            if self.order_manager.cancel_buy_at(price, id) {
                self.send_buy_cancel_for(id, price);
            }
        }
        while let Some((price, id)) = self.order_manager.best_sell_price_cancel() {
            if self.order_manager.cancel_sell_at(price, id) {
                self.send_sell_cancel_for(id, price);
            }
        }
    }
}
