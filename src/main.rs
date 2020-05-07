#![recursion_limit = "256"]

//WARNING
//WARNING
//
//I would not abide by all of the given practices in here in production quality trading code

use exchange::{
    bitmex_connection, bitstamp_connection, bitstamp_orders_connection, okex_connection, OkexType,
};

use crate::exchange::normalized::*;
use std::io::prelude::*;

use futures::{future::FutureExt, join, select};
use std::sync::mpsc;
use structopt::StructOpt;

use std::sync::Arc;

mod args;
mod bitstamp_http;
mod displacement;
mod ema;
mod exchange;
mod fair_value;
mod local_book;
mod order_book;
mod order_manager;
mod position_manager;
mod remote_venue_aggregator;
mod tactic;

use fair_value::*;

fn html_writer(filename: String, requests: mpsc::Receiver<String>) {
    while let Ok(request) = requests.recv() {
        let atomic = atomicwrites::AtomicFile::new(
            &filename,
            atomicwrites::OverwriteBehavior::AllowOverwrite,
        );
        atomic
            .write(|temp_file| temp_file.write_all(request.as_bytes()))
            .expect("Couldn't write html");
    }
    println!("Done writing html");
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut rt = tokio::runtime::Runtime::new()?;
    rt.block_on(run())
}

enum TacticTimerEvent {
    CheckTransactionsFrom(usize),
    CheckOpenOrders,
    OrderCanCancel,
}

enum TacticInternalEvent {
    TransactionsDataReceived,
    OpenOrdersDataReceived,
    DisplayHtml,
}

enum TacticEventType {
    RemoteFair,
    LocalBook(SmallVec<MarketEvent>),
    InsideOrders(SmallVec<local_book::InsideOrder>),
    WriteHtml,
}

async fn html_writer_loop(mut event_queue: tokio::sync::mpsc::Sender<TacticInternalEvent>) {
    loop {
        tokio::time::delay_for(std::time::Duration::from_millis(1000)).await;
        event_queue.send(TacticInternalEvent::DisplayHtml).await;
    }
}

async fn transaction_not_loop(mut event_queue: tokio::sync::mpsc::Sender<TacticInternalEvent>) {
    loop {
        tokio::time::delay_for(std::time::Duration::from_millis(1000)).await;
        event_queue.send(TacticInternalEvent::DisplayHtml).await;
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args = args::Arguments::from_args();

    let (html_queue, html_reader) = std::sync::mpsc::channel();

    let (event_queue, mut event_reader) = tokio::sync::mpsc::channel(100);

    let mut http = bitstamp_http::BitstampHttp::new(args.auth_key, args.auth_secret);
    let mut http = Arc::new(http);

    let html = args.html.clone();
    std::thread::spawn(move || html_writer(html, html_reader));

    let html_not = event_queue.clone();

    http.request_initial_transaction().await;
    let pos = position_manager::PositionManager::create(&*http).await;

    let bitstamp = bitstamp_connection();
    let bitstamp_orders = bitstamp_orders_connection();
    let bitmex = bitmex_connection();
    let okex_spot = okex_connection(OkexType::Spot);
    let okex_swap = okex_connection(OkexType::Swap);

    let (bitmex, okex_spot, okex_swap, mut bitstamp, mut bitstamp_orders) =
        join!(bitmex, okex_spot, okex_swap, bitstamp, bitstamp_orders);

    // Spawn all tasks after we've connected to everything
    tokio::task::spawn(html_writer_loop(html_not));

    let remote_fair_value = FairValue::new(1.0, 0.0, 5.0, 10);
    let local_fair_value = FairValue::new(0.7, 0.05, 20.0, 20);

    let mut remote_agg = remote_venue_aggregator::RemoteVenueAggregator::new(
        bitmex,
        okex_spot,
        okex_swap,
        remote_fair_value,
        0.001,
    );

    let mut local_book = local_book::LocalBook::new(local_fair_value);

    let mut displacement = displacement::Displacement::new();

    let mut tactic = tactic::Tactic::new(args.profit_bps, args.fee_bps);

    loop {
        let event_type = select! {
            rf = remote_agg.get_new_fair().fuse() => TacticEventType::RemoteFair,
            block = bitstamp.next().fuse() => TacticEventType::LocalBook(block.events),
            order = bitstamp_orders.next().fuse() => TacticEventType::InsideOrders(
                local_book.handle_new_order(&order.events)),
                event = event_reader.recv().fuse() => match event {
                    Some(TacticInternalEvent::DisplayHtml) => TacticEventType::WriteHtml,
                    None => panic!("event queue died"),
                    _ => panic!("Can't do work"),
                }
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
            Some((displacement_val, expected_premium)),
            Some(remote_fair),
        ) = (
            local_book.get_local_tob(),
            displacement.get_displacement(),
            remote_agg.calculate_fair(),
        ) {
            match &event_type {
                TacticEventType::RemoteFair | TacticEventType::LocalBook(_) => {
                    tactic.handle_book_update(bbo, local_fair, displacement_val)
                }
                TacticEventType::InsideOrders(events) => {
                    let premium = local_fair - remote_fair;
                    tactic.handle_new_orders(
                        ((bbo.0).0, (bbo.1).0),
                        local_fair,
                        displacement_val,
                        premium - expected_premium,
                        &events,
                    )
                }
                TacticEventType::WriteHtml => {
                    let html = format!(
                        "
                        <!DOCYPE html>
                        <html>
                        <head>
                        <meta charset=\"UTF-8\">
                        <meta name=\"description\" content=\"Bitcoin\">
                        <meta http-equiv=\"refresh\" content=\"3\" >
                        </head>
                        <body>
                        {local}
                        {remote}
                        {displacement}
                        </body>
                        </html>
                        ",
                        local = local_book.get_html_info(),
                        remote = remote_agg.get_html_info(),
                        displacement = displacement.get_html_info(),
                    );
                    html_queue.send(html).expect("Couldn't send html");
                }
            };
        }
    }
}
