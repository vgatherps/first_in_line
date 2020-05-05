use exchange::{bitmex_connection, coinbase_connection, okex_connection, OkexType};
use order_book::*;

use futures::{future::FutureExt, join, select};

mod ema;
mod exchange;
mod fair_value;
mod maker;
mod order_book;
mod remote_venue_aggregator;

use fair_value::*;

async fn run() {
    let bitmex = bitmex_connection();
    let coinbase = coinbase_connection();
    let okex_spot = okex_connection(OkexType::Spot);
    let okex_swap = okex_connection(OkexType::Swap);

    let (bitmex, okex_spot, okex_swap, mut coinbase) =
        join!(bitmex, okex_spot, okex_swap, coinbase);

    let remote_fair_value = FairValue::new(0.9, 0.0, 10.0, 20);

    let mut remote_agg = remote_venue_aggregator::RemoteVenueAggregator::new(
        bitmex,
        okex_spot,
        okex_swap,
        remote_fair_value,
        0.001,
    );

    let mut book = OrderBook::new();
    let mut maker = maker::Maker::new(remote_fair_value);
    loop {
        select! {
            rf = remote_agg.get_new_fair().fuse() => maker.handle_remote_fair(rf),
            block = coinbase.next().fuse() => {
                for event in block.events {
                    book.handle_book_event(&event);
                }
                maker.handle_book_update(&book);
            }
        }
    }
}

fn main() {
    async_std::task::block_on(run());
}
