use exchange::{bitmex_connection, coinbase_connection, okex_connection, OkexType};
use order_book::*;

use futures::{future::FutureExt, join, select};

mod displacement;
mod ema;
mod exchange;
mod fair_value;
mod local_book;
mod order_book;
mod remote_venue_aggregator;
mod tactic;

use fair_value::*;

async fn run() {
    let bitmex = bitmex_connection();
    let coinbase = coinbase_connection();
    let okex_spot = okex_connection(OkexType::Spot);
    let okex_swap = okex_connection(OkexType::Swap);

    let (bitmex, okex_spot, okex_swap, mut coinbase) =
        join!(bitmex, okex_spot, okex_swap, coinbase);

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
        select! {
            rf = remote_agg.get_new_fair().fuse() => {}
            block = coinbase.next().fuse() => {
                local_book.handle_book_update(&block.events);
            }
        }

        match (local_book.get_local_tob(), remote_agg.calculate_fair()) {
            (Some((bbo, local_fair)), Some(remote_fair)) => {
                displacement.handle_new_fairs(remote_fair, local_fair);
                match displacement.get_displacement() {
                    (Some(prem_f), Some(prem_s)) => {
                        tactic.handle_book_update(bbo, local_fair, prem_f, prem_s)
                    }
                    _ => (),
                }
            }
            _ => {}
        }
    }
}

fn main() {
    async_std::task::block_on(run());
}
