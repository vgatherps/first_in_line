use exchange::bitmex_connection;
use order_book::*;

mod exchange;
mod order_book;

async fn run() {
    let mut connection = bitmex_connection().await;

    let mut book = OrderBook::new();
    let mut old_bbo = (None, None);
    loop {
        let events = connection.next().await;

        for event in events {
            book.handle_book_event(&event);
        }

        let bbo = book.bbo();
        if bbo != old_bbo {
            println!("{:?}", bbo);
        }
        old_bbo = bbo;
    }
}

fn main() {
    async_std::task::block_on(run());
}
