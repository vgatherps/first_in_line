use exchange::{bitmex_connection, coinbase_connection};
use order_book::*;

mod exchange;
mod fair_value;
mod order_book;

use fair_value::*;

async fn run() {
    let mut connection = coinbase_connection().await;

    let mut book = OrderBook::new();
    let fair_value = FairValue::new(1.0, 0.01, 10.0, 20);
    let mut counter: usize = 0;
    loop {
        let events = connection.next().await;

        for event in events {
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
