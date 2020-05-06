#![recursion_limit = "256"]

//WARNING
//WARNING
//
//I would not abide by all of the given practices in here in production quality trading code

use exchange::{
    bitmex_connection, bitstamp_connection, bitstamp_orders_connection, okex_connection, OkexType,
};

use crate::exchange::normalized::*;

use futures::{future::FutureExt, join, select};

mod displacement;
mod ema;
mod exchange;
mod fair_value;
mod local_book;
mod order_book;
mod order_manager;
mod remote_venue_aggregator;
mod tactic;

use fair_value::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut rt = tokio::runtime::Runtime::new()?;
    rt.block_on(run())
}

#[derive(Debug)]
pub enum TacticEventType {
    RemoteFair,
    LocalBook(SmallVec<MarketEvent>),
    InsideOrders(SmallVec<local_book::InsideOrder>),
}

impl TacticEventType {
    fn tactic_check_orders(&self) -> bool {
        match self {
            TacticEventType::RemoteFair | TacticEventType::LocalBook(_) => true,
            TacticEventType::InsideOrders(_) => false,
        }
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let bitstamp = bitstamp_connection();
    let bitstamp_orders = bitstamp_orders_connection();
    let bitmex = bitmex_connection();
    let okex_spot = okex_connection(OkexType::Spot);
    let okex_swap = okex_connection(OkexType::Swap);

    let (bitmex, okex_spot, okex_swap, mut bitstamp, mut bitstamp_orders) =
        join!(bitmex, okex_spot, okex_swap, bitstamp, bitstamp_orders);

    let remote_fair_value = FairValue::new(1.1, 0.0, 5.0, 10);

    let mut remote_agg = remote_venue_aggregator::RemoteVenueAggregator::new(
        bitmex,
        okex_spot,
        okex_swap,
        remote_fair_value,
        0.001,
    );

    let mut local_book = local_book::LocalBook::new(remote_fair_value);

    let mut displacement = displacement::Displacement::new();

    let mut tactic = tactic::Tactic::new();

    loop {
        let event_type = select! {
            rf = remote_agg.get_new_fair().fuse() => TacticEventType::RemoteFair,
            block = bitstamp.next().fuse() => TacticEventType::LocalBook(block.events),
            order = bitstamp_orders.next().fuse() => TacticEventType::InsideOrders(
                local_book.handle_new_order(&order.events)),
        };
        match &event_type {
            TacticEventType::RemoteFair => {
                if let Some(rf) = remote_agg.calculate_fair() {
                    displacement.handle_remote(rf);
                }
            }
            TacticEventType::LocalBook(events) => {
                local_book.handle_book_update(&events);
                if let Some((_, local_fair)) = local_book.get_local_tob() {
                    displacement.handle_local(local_fair);
                };
            }
            _ => (),
        }
        if let (
            Some((bbo, local_fair)),
            Some((displacement, expected_premium)),
            Some(remote_fair),
        ) = (
            local_book.get_local_tob(),
            displacement.get_displacement(),
            remote_agg.calculate_fair(),
        ) {
            match &event_type {
                TacticEventType::RemoteFair | TacticEventType::LocalBook(_) => {
                    tactic.handle_book_update(bbo, local_fair, displacement)
                }
                TacticEventType::InsideOrders(events) => {
                    let premium = local_fair - remote_fair;
                    tactic.handle_new_orders(
                        ((bbo.0).0, (bbo.1).0),
                        local_fair,
                        displacement,
                        premium - expected_premium,
                        &events,
                    )
                }
            };
        }
    }
}
