use exchange::{
    bitmex_connection, bitstamp_connection, okex_connection, OkexType,
};

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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut rt = tokio::runtime::Runtime::new()?;
    rt.block_on(run())
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let bitmex = bitmex_connection();
    let coinbase = bitstamp_connection();
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
            rf = remote_agg.get_new_fair().fuse() => {
                if let Some(rf) = remote_agg.calculate_fair() {
                    displacement.handle_remote(rf);
                }
            }
            block = coinbase.next().fuse() => {
                local_book.handle_book_update(&block.events);
                if let Some((_, local_fair)) = local_book.get_local_tob() {
                    displacement.handle_local(local_fair);
                };
            }
        }

        if let (Some((bbo, local_fair)), Some(displacement)) =
            (local_book.get_local_tob(), displacement.get_displacement())
        {
            tactic.handle_book_update(bbo, local_fair, displacement)
        }
    }
}
