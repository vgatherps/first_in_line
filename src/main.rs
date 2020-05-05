use exchange::{bitmex_connection, coinbase_connection};
use order_book::*;

use futures::join;

mod ema;
mod exchange;
mod fair_value;
mod order_book;
mod remote_venue_aggregator;

use fair_value::*;

async fn run() {
    let bitmex = bitmex_connection();
    let coinbase = coinbase_connection();

    let (bitmex, mut coinbase) = join!(bitmex, coinbase);

    let remote_fair_value = FairValue::new(1.0, 0.01, 10.0, 20);

    let remote_agg =
        remote_venue_aggregator::RemoteVenueAggregator::new(bitmex, remote_fair_value, 0.001);

    let mut book = OrderBook::new();
    let fair_value = remote_fair_value;
    let mut counter: usize = 0;
    loop {
        let block = coinbase.next().await;

        for event in block.events {
            book.handle_book_event(&event);
        }

        let bbo = book.bbo();
        counter += 1;
        match bbo {
            (Some((bid, bid_sz)), Some((ask, ask_sz))) => {
                let fair = fair_value.fair_value(book.bids(), book.asks(), (bid, ask));
                if counter % 100 == 0 || fair.fair_price < 0.01 {
                    println!(
                        "({:.2}, {:.2})x({:.2}, {:.2}), fair {:.4}, counter {}",
                        bid as f64 * 0.01,
                        bid_sz,
                        ask as f64 * 0.01,
                        ask_sz,
                        fair.fair_price,
                        counter
                    );
                }
            }
            _ => (),
        }
    }
}

fn main() {
    async_std::task::block_on(run());
}
