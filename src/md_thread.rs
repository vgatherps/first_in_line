use crossbeam_channel::Sender;
use futures::{future::FutureExt, join, select};

use crate::exchange::normalized::*;
use crate::exchange::{
    bitmex_connection, bybit_connection, coinbase_connection, huobi_connection, okex_connection,
    BybitType, HuobiType, OkexType,
};
async fn run_md_thread(queue: Sender<MarketEventBlock>) {
    let (
        mut bitmex,
        mut okex_spot,
        mut okex_swap,
        mut okex_quarterly,
        mut bybit_usdt,
        mut bybit_inverse,
        mut huobi,
        mut coinbase,
    ) = join!(
        bitmex_connection(),
        okex_connection(OkexType::Spot),
        okex_connection(OkexType::Swap),
        okex_connection(OkexType::Quarterly),
        bybit_connection(BybitType::USDT),
        bybit_connection(BybitType::Inverse),
        huobi_connection(HuobiType::Spot),
        coinbase_connection(),
    );
    let mut ping = tokio::time::interval(std::time::Duration::from_millis(1000 * 60 * 10));
    loop {
        let rval = select! {
            b = bitmex.next().fuse() => Some(b),
            b = okex_spot.next().fuse() => Some(b),
            b = okex_swap.next().fuse() => Some(b),
            b = okex_quarterly.next().fuse() => Some(b),
            b = huobi.next().fuse() => Some(b),
            b = coinbase.next().fuse() => Some(b),
            b = bybit_usdt.next().fuse() => Some(b),
            b = bybit_inverse.next().fuse() => Some(b),
            _ = ping.tick().fuse() => None,
        };

        if let Some(rval) = rval {
            match queue.send(rval) {
                Ok(_) => (),
                Err(_) => return, // the other side disconnected, we gracefully die
            }
        } else {
            join!(
                bitmex.ping(),
                okex_spot.ping(),
                okex_swap.ping(),
                okex_quarterly.ping(),
                huobi.ping(),
                coinbase.ping(),
                bybit_usdt.ping(),
                bybit_inverse.ping()
            );
        }
    }
}

pub fn start_md_thread(sender: Sender<MarketEventBlock>) {
    let mut rt = tokio::runtime::Builder::new()
        .basic_scheduler()
        .enable_all()
        .build()
        .expect("Can't build a local scheduler");
    rt.block_on(run_md_thread(sender))
}
