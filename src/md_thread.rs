use crossbeam_channel::Sender;
use futures::{
    future::{join_all, select_all, Future, FutureExt},
    join, pin_mut, select,
    task::{Context, Poll},
};

use std::sync::Arc;

use crate::exchange::normalized::*;
use crate::exchange::{
    bitmex_connection, bybit_connection, coinbase_connection, huobi_connection, okex_connection,
    BybitType, HuobiType, OkexType,
};
use crate::security_to_reader;
use crate::signal_graph::security_index::{SecurityIndex, SecurityMap};

// Futures select_all allocates EVERY SINGLE TIME IT GETS CALLED, which is absurd.
// Further, each time you call wait, you have to allocate each waited future!

// For now here's a reduced copy/past of the select_all future made to work
// well without infinite allocation

use std::mem;
use std::pin::Pin;

pub struct SelectAllMd {
    md: Vec<MarketDataStream>,
}

impl Unpin for SelectAllMd {}

impl Future for &mut SelectAllMd {
    type Output = MarketEventBlock;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let item = self
            .md
            .iter_mut()
            .find_map(|f| {
                let mut next = f.next();
                pin_mut!(next);
                match next.poll(cx) {
                    Poll::Pending => None,
                    Poll::Ready(e) => Some(e),
                }
            });
        match item {
            Some(res) => Poll::Ready(res),
            None => Poll::Pending,
        }
    }
}

async fn run_md_thread(
    queue: Sender<MarketEventBlock>,
    securities: Vec<SecurityIndex>,
    map: Arc<SecurityMap>,
) {
    let md_streams: Vec<_> = securities
        .into_iter()
        .map(|i| {
            let security = map.to_security(i);
            security_to_reader::reader_from_security(security)
        })
        .collect();
    let md_streams: Vec<_> = join_all(md_streams)
        .await
        .into_iter()
        .map(|a| a.unwrap())
        .collect();
    let mut select_md = SelectAllMd { md: md_streams };
    let mut ping = tokio::time::interval(std::time::Duration::from_millis(1000 * 60 * 10));
    loop {
        let md_fut = &mut select_md;
        let rval = select! {
            md = md_fut.fuse() => Some(md),
            ping = ping.tick().fuse() => None
        };

        if let Some(rval) = rval {
            match queue.send(rval) {
                Ok(_) => (),
                Err(_) => return, // the other side disconnected, we gracefully die
            }
        } else {
            join_all(select_md.md.iter_mut().map(|md| md.ping())).await;
        }
    }
}

pub fn start_md_thread(
    sender: Sender<MarketEventBlock>,
    securities: Vec<SecurityIndex>,
    map: Arc<SecurityMap>,
) {
    let mut rt = tokio::runtime::Builder::new()
        .basic_scheduler()
        .enable_all()
        .build()
        .expect("Can't build a local scheduler");
    rt.block_on(run_md_thread(sender, securities, map))
}
