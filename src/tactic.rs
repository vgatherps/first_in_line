use crate::bitmex_http::{BitmexHttp, OrderCanceled, Transaction};
use crate::exchange::normalized::{convert_price_cents, Side};
use crate::local_book::InsideOrder;
use crate::order_book::{BuyPrice, OrderBook, SellPrice, SidedPrice};
use crate::order_manager::OrderManager;
use crate::position_manager::PositionManager;

use horrorshow::html;
use xorshift::{Rng, Xorshift128};

use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};

use std::sync::atomic::Ordering;

pub struct TacticStatistics {
    orders_sent: usize,
    orders_canceled: usize,
    missed_cancels: usize,

    trades: usize,
    traded_xbt: usize,

    recent_trades: VecDeque<(Side, usize, usize)>,

    fifo: crate::fifo_pnl::Fifo,
}

impl TacticStatistics {
    pub fn new() -> TacticStatistics {
        TacticStatistics {
            orders_sent: 0,
            orders_canceled: 0,
            missed_cancels: 0,
            trades: 0,
            traded_xbt: 0,
            recent_trades: VecDeque::new(),
            fifo: Default::default(),
        }
    }
}

pub struct Tactic<'a> {
    required_profit: f64,
    required_profit_cancel: f64,
    required_fees: f64,
    imbalance_adjust: f64,

    cost_of_position: f64,
    base_trade_contracts: usize,

    max_orders_side: usize,
    worry_orders_side: usize,
    max_send: usize,

    statistics: &'a mut TacticStatistics,
    order_manager: OrderManager,

    position: PositionManager,

    rng: Xorshift128,

    main_loop_not: tokio::sync::mpsc::Sender<crate::TacticInternalEvent>,
    http: std::sync::Arc<BitmexHttp>,
}

// TODO should really care about exchange times

fn get_clid() -> usize {
    let send = SystemTime::now();
    send.duration_since(UNIX_EPOCH)
        .expect("backwards")
        .as_micros() as usize
}

async fn order_caller(
    amount: usize,
    price: f64,
    clid: usize,
    side: Side,
    http: std::sync::Arc<BitmexHttp>,
    mut eventer: tokio::sync::mpsc::Sender<crate::TacticInternalEvent>,
) {
    let _guard = scopeguard::guard((), |_| {
        if std::thread::panicking() {
            crate::DIE.store(true, Ordering::Relaxed);
        }
    });
    let send = SystemTime::now();
    if !http
        .send_order(amount, price, clid, side, http.clone())
        .await
    {
        // We got rejected for overload
        assert!(eventer
            .send(crate::TacticInternalEvent::OrderCanceled(
                crate::bitmex_http::OrderCanceled {
                    price,
                    amount,
                    side,
                    id: clid
                }
            ))
            .await
            .is_ok());
        return;
    }
    let cents = (price * 100.0).round() as usize;
    tokio::task::spawn(async move {
        // first wait 30 seconds, and set cancelable
        tokio::time::delay_for(std::time::Duration::from_millis(1000 * 30)).await;
        assert!(eventer
            .send(crate::TacticInternalEvent::SetLateStatus(side, cents, clid))
            .await
            .is_ok());

        // wait 360ish more seconds, try and cancel order
        // Now that there's better protection against a wall of stale orders at the top,
        // this is a lot less important
        let random_offset = (clid / 19) % 4;
        tokio::time::delay_for(std::time::Duration::from_millis(
            1000 * (360 + random_offset) as u64,
        ))
        .await;
        assert!(eventer
            .send(crate::TacticInternalEvent::CancelStale(side, cents, clid))
            .await
            .is_ok());

        // wait 10 more seconds
        // if it's still gone, we missed a trade and should reset
        tokio::time::delay_for(std::time::Duration::from_millis(1000 * 10 as u64)).await;
        assert!(eventer
            .send(crate::TacticInternalEvent::CheckGone(side, cents, clid))
            .await
            .is_ok());
    });

    if let Ok(diff) = std::time::SystemTime::now().duration_since(send) {
        println!("Send took {} ms", diff.as_millis())
    }
}

async fn cancel_caller(
    id: usize,
    http: std::sync::Arc<BitmexHttp>,
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

impl<'a> Tactic<'a> {
    pub fn new(
        profit_bps: f64,
        profit_bps_cancel: f64,
        fee_bps: f64,
        cost_of_position: f64,
        base_trade_contracts: usize,
        position: PositionManager,
        statistics: &'a mut TacticStatistics,
        http: std::sync::Arc<crate::bitmex_http::BitmexHttp>,
        main_loop_not: tokio::sync::mpsc::Sender<crate::TacticInternalEvent>,
    ) -> Tactic {
        assert!(profit_bps > profit_bps_cancel);
        Tactic {
            required_profit: 0.01 * 0.01 * profit_bps,
            required_profit_cancel: 0.01 * 0.01 * profit_bps_cancel,
            required_fees: 0.01 * 0.01 * fee_bps,
            imbalance_adjust: 0.2,
            order_manager: OrderManager::new(),
            max_orders_side: 16,
            worry_orders_side: 10,
            max_send: 100000,
            // reproducible seed is fine here, better for sims
            rng: xorshift::thread_rng(),
            position,
            cost_of_position,
            base_trade_contracts,
            http,
            main_loop_not,
            statistics,
        }
    }

    fn get_filtered_bbo(&self, book: &OrderBook) -> (BuyPrice, SellPrice) {
        let bids = book.bids();
        let asks = book.asks();

        // advance the book iterator until we find a level where we aren't most of it
        // Since we wait ~3 seconds to cancel orders in this fashion, it should be relatively
        // in-sync
        // Find any BBO where we aren't a tiny little participant
        let bid = bids
            .filter(|(prc, size)| {
                let our_size = self.order_manager.buy_size_at(**prc) as f64;
                our_size / **size < 0.1
            })
            .next()
            .map(|(prc, _)| *prc)
            .unwrap_or(BuyPrice::new(0));
        let ask = asks
            .filter(|(prc, size)| {
                let our_size = self.order_manager.sell_size_at(**prc) as f64;
                our_size / **size < 0.1
            })
            .next()
            .map(|(prc, _)| *prc)
            .unwrap_or(SellPrice::new(1000 * 1000 * 1000 * 1000usize));

        (bid, ask)
    }

    pub fn handle_book_update(
        &mut self,
        book: &OrderBook,
        orders: &[InsideOrder],
        fair: f64,
        adjust: f64,
        premium_imbalance: f64,
    ) {
        let adjusted_fair = fair + adjust;
        while let Some((price, id)) = self.order_manager.best_buy_price_late() {
            let buy_prc = price.unsigned() as f64 * 0.01;
            if self.consider_order_cancel(adjusted_fair, premium_imbalance, buy_prc, Side::Buy) {
                assert!(self.order_manager.cancel_buy_at(price, id));
                self.send_buy_cancel_for(id, price);
            } else {
                break;
            }
        }
        while let Some((price, id)) = self.order_manager.best_sell_price_late() {
            let sell_prc = price.unsigned() as f64 * 0.01;
            if self.consider_order_cancel(adjusted_fair, premium_imbalance, sell_prc, Side::Sell) {
                assert!(self.order_manager.cancel_sell_at(price, id));
                self.send_sell_cancel_for(id, price);
            } else {
                break;
            }
        }
        let (bid, offer) = self.get_filtered_bbo(book);
        while let Some((price, id)) = self.order_manager.best_buy_price_late() {
            let size_at = book.get_buy_size(price);
            if (price <= bid && size_at <= 50_000.0) || (price > bid && size_at <= 20_000.0) {
                assert!(self.order_manager.cancel_buy_at(price, id));
                self.send_buy_cancel_for(id, price);
            } else {
                break;
            }
        }
        while let Some((price, id)) = self.order_manager.best_sell_price_late() {
            let size_at = book.get_sell_size(price);
            if (price <= offer && size_at <= 50_000.0) || (price > offer && size_at <= 20_000.0) {
                assert!(self.order_manager.cancel_sell_at(price, id));
                self.send_sell_cancel_for(id, price);
            } else {
                break;
            }
        }

        self.handle_new_orders(fair, adjust, premium_imbalance, orders);

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
                if self.order_manager.cancel_buy_at(bprice, id) {
                    self.send_buy_cancel_for(id, bprice);
                }
            }
            Side::Sell => {
                let sprice = SellPrice::new(price);
                if self.order_manager.cancel_sell_at(sprice, id) {
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
                if self.order_manager.has_buy_order(bprice, id) {
                    self.reset();
                }
            }
            Side::Sell => {
                let sprice = SellPrice::new(price);
                if self.order_manager.has_sell_order(sprice, id) {
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
        self.statistics.orders_canceled += 1;
        let cents = convert_price_cents(cancel.price);
        match cancel.side {
            Side::Buy => {
                if let Some(known_volume) = self
                    .order_manager
                    .ack_buy_cancel(BuyPrice::new(cents), cancel.id)
                {
                    if cancel.amount > known_volume {
                        println!(
                            "Got buy cancel for {}, only have {}",
                            cancel.amount, known_volume
                        );
                        self.reset();
                    }
                    self.position.return_buy_balance(cancel.amount);
                } else {
                    self.statistics.missed_cancels += 1;
                }
            }
            Side::Sell => {
                if let Some(known_volume) = self
                    .order_manager
                    .ack_sell_cancel(SellPrice::new(cents), cancel.id)
                {
                    if cancel.amount > known_volume {
                        println!(
                            "Got sell cancel for {}, only have {}",
                            cancel.amount, known_volume
                        );
                        self.reset();
                    }
                    self.position.return_sell_balance(cancel.amount);
                } else {
                    self.statistics.missed_cancels += 1;
                }
            }
        }
    }

    fn reset(&self) {
        let mut not = self.main_loop_not.clone();
        tokio::task::spawn(async move {
            assert!(not
                .send(crate::TacticInternalEvent::Reset(true))
                .await
                .is_ok());
        });
    }

    fn get_position_imbalance_cost(&self, fair: f64) -> f64 {
        self.position.get_position_imbalance() as f64 * self.cost_of_position
    }

    fn consider_order_cancel(
        &self,
        around: f64,
        premium_imbalance: f64,
        prc: f64,
        side: Side,
    ) -> bool {
        let around = around + self.get_position_imbalance_cost(around);
        let required_diff = (self.required_fees + self.required_profit_cancel) * prc;
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
    fn handle_new_orders(
        &mut self,
        fair: f64,
        adjust: f64,
        premium_imbalance: f64,
        orders: &[InsideOrder],
    ) {
        if !self.http.can_send_order() {
            return;
        }
        if self.statistics.orders_sent >= self.max_send {
            return;
        }
        // shuffle +/ 64 dollars on order size
        let trade_xbt = self.base_trade_contracts + (self.rng.next_u64() % 128) as usize;
        assert!(trade_xbt > 100);
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
                if !self
                    .order_manager
                    .can_place_at(&BuyPrice::new(first_buy.insert_price))
                {
                    return;
                }

                if !self.position.request_buy_balance(trade_xbt) {
                    return;
                }
                let clid = get_clid();
                if self.order_manager.add_sent_order(
                    &BuyPrice::new(first_buy.insert_price),
                    trade_xbt,
                    clid,
                ) {
                    tokio::task::spawn(order_caller(
                        trade_xbt,
                        buy_prc,
                        clid,
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
            if self.consider_order_placement(adjusted_fair, premium_imbalance, sell_prc, Side::Sell)
            {
                if !self
                    .order_manager
                    .can_place_at(&SellPrice::new(first_sell.insert_price))
                {
                    return;
                }
                if !self.position.request_sell_balance(trade_xbt) {
                    return;
                }
                let clid = get_clid();
                if self.order_manager.add_sent_order(
                    &SellPrice::new(first_sell.insert_price),
                    trade_xbt,
                    clid,
                ) {
                    tokio::task::spawn(order_caller(
                        trade_xbt,
                        sell_prc,
                        clid,
                        Side::Sell,
                        self.http.clone(),
                        self.main_loop_not.clone(),
                    ));
                    println!("Sent SELL at {:.2} size {:.4}", sell_prc, trade_xbt);
                }
            }
        }
    }

    pub fn check_seen_trade(&mut self, trade: &Transaction) {
        let cents = trade.cents;
        let as_buy = BuyPrice::new(cents);
        let as_sell = SellPrice::new(cents);

        self.statistics
            .recent_trades
            .push_front((trade.side, trade.cents, trade.size));
        match trade.side {
            Side::Buy => {
                // Our purchase succeeded, as far as we know
                self.position.buy(trade.size);
                self.statistics.trades += 1;
                self.statistics.traded_xbt += trade.size;
                self.statistics
                    .fifo
                    .add_buy(BuyPrice::new(cents), trade.size);
                let removed = self.order_manager.remove_liquidity_from(
                    &BuyPrice::new(cents),
                    trade.size,
                    trade.order_id,
                );

                println!(
                    "Trade buy id {} price {:.2} size {:.5}",
                    trade.order_id, trade.cents, trade.size
                );
            }
            Side::Sell => {
                self.position.sell(trade.size);
                self.statistics.trades += 1;
                self.statistics.traded_xbt += trade.size;
                self.statistics
                    .fifo
                    .add_sell(SellPrice::new(cents), trade.size);
                self.order_manager.remove_liquidity_from(
                    &SellPrice::new(cents),
                    trade.size,
                    trade.order_id,
                );
                println!(
                    "Trade sell id {} price {:.2} size {:.5}",
                    trade.order_id, trade.cents, trade.size
                );
            }
        }

        while self.statistics.recent_trades.len() > 20 {
            self.statistics.recent_trades.pop_back();
        }
    }

    pub fn get_html_info(&self, fair: f64) -> String {
        let imbalance = self.position.get_position_imbalance();

        let trading_fees = self.statistics.fifo.xbt_traded() * self.position.get_fee_estimate();
        format!(
            "{}{}",
            html! {
                h3(id="tactic state", clas="title") : "Tactic State Summary";
                ul(id="Tactic summary") {
                    li(first?=true, class="item") {
                        : format!("Balance: {} xbt contracts",
                                  self.position.total_contracts);
                    }
                    li(first?=true, class="item") {
                        : format!("Matched trading pnl in xbt with with {:.2} and without {:.2} fees, marked to current fair",
                                  (self.statistics.fifo.xbt() - trading_fees) * fair,
                                  self.statistics.fifo.xbt() * fair);
                    }
                    li(first?=false, class="item") {
                        : format!("Position imbalance: {} contracts, price offset {:.3}",
                                  imbalance, imbalance as f64 * self.cost_of_position);
                    }
                    li(first?=false, class="item") {
                        : format!("Trades: {}, traded xbt: {:.2}",
                                  self.statistics.trades, self.statistics.traded_xbt);
                    }
                    li(first?=false, class="item") {
                        : format!("Orders: sent {}, canceled {}, missed cancels: {}, rate {:.2}",
                                  self.statistics.orders_sent,
                                  self.statistics.orders_canceled,
                                  self.statistics.missed_cancels,
                                  if self.statistics.orders_sent == 0 {
                                      0.0
                                  } else {
                                      self.statistics.orders_canceled as f64 / self.statistics.orders_sent as f64
                                  });
                    }
                    li(first?=false, class="item") {
                        : format!("Recent trades: {:?}",
                                  self.statistics.recent_trades);
                    }
                }
            },
            self.order_manager.get_html_info(),
        )
    }
}

async fn do_cancel_all(http: std::sync::Arc<BitmexHttp>) {
    http.cancel_all(http.clone()).await
}

impl<'a> Drop for Tactic<'a> {
    fn drop(&mut self) {
        tokio::task::spawn(do_cancel_all(self.http.clone()));
    }
}
