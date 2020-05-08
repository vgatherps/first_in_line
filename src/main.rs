#![recursion_limit = "256"]

//WARNING
//WARNING
//
//I would not abide by all of the given practices in here in production quality trading code

use exchange::{
    bitmex_connection, bitstamp_connection, bitstamp_orders_connection, bitstamp_trades_connection,
    coinbase_connection, okex_connection, OkexType,
};

use crate::exchange::normalized::*;
use std::io::prelude::*;

use chrono::prelude::*;
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

pub enum TacticInternalEvent {
    OrderCanceled(bitstamp_http::OrderCanceled),
    OrderSent(bitstamp_http::OrderSent),
    SetLateStatus(Side, usize, usize),
    CancelStale(Side, usize, usize),
    DisplayHtml,
}

enum TacticEventType {
    RemoteFair,
    LocalBook(SmallVec<MarketEvent>),
    InsideOrders(SmallVec<local_book::InsideOrder>),
    Trades(SmallVec<TradeUpdate>),
    AckCancel(bitstamp_http::OrderCanceled),
    AckSend(bitstamp_http::OrderSent),
    CancelStale(Side, usize, usize),
    SetLateStatus(Side, usize, usize),
    WriteHtml,
}

async fn html_writer_loop(mut event_queue: tokio::sync::mpsc::Sender<TacticInternalEvent>) {
    loop {
        tokio::time::delay_for(std::time::Duration::from_millis(1000)).await;
        event_queue.send(TacticInternalEvent::DisplayHtml).await;
    }
}
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let start = Local::now();
    let args = args::Arguments::from_args();

    let (html_queue, html_reader) = std::sync::mpsc::channel();

    let (event_queue, mut event_reader) = tokio::sync::mpsc::channel(100);

    let http = bitstamp_http::BitstampHttp::new(args.auth_key, args.auth_secret);
    let http = Arc::new(http);

    let html = args.html.clone();
    std::thread::spawn(move || html_writer(html, html_reader));

    let position = position_manager::PositionManager::create(&*http).await;

    let bitstamp = bitstamp_connection();
    let bitstamp_orders = bitstamp_orders_connection();
    let bitstamp_trades = bitstamp_trades_connection();
    let bitmex = bitmex_connection();
    let okex_spot = okex_connection(OkexType::Spot);
    let okex_swap = okex_connection(OkexType::Swap);
    let okex_quarterly = okex_connection(OkexType::Quarterly);
    let coinbase = coinbase_connection();

    let (
        bitmex,
        okex_spot,
        okex_swap,
        okex_quarterly,
        coinbase,
        mut bitstamp,
        mut bitstamp_orders,
        mut bitstamp_trades,
    ) = join!(
        bitmex,
        okex_spot,
        okex_swap,
        okex_quarterly,
        coinbase,
        bitstamp,
        bitstamp_orders,
        bitstamp_trades
    );

    // Spawn all tasks after we've connected to everything
    tokio::task::spawn(html_writer_loop(event_queue.clone()));

    let remote_fair_value = FairValue::new(1.0, 0.0, 5.0, 10);
    let local_fair_value = FairValue::new(0.7, 0.05, 20.0, 20);

    let mut remote_agg = remote_venue_aggregator::RemoteVenueAggregator::new(
        bitmex,
        okex_spot,
        okex_swap,
        okex_quarterly,
        coinbase,
        remote_fair_value,
        0.001,
    );

    let mut local_book = local_book::LocalBook::new(local_fair_value);

    let mut displacement = displacement::Displacement::new();

    let mut tactic = tactic::Tactic::new(
        args.profit_bps,
        args.fee_bps,
        args.cost_of_position,
        position,
        http.clone(),
        event_queue.clone(),
    );

    loop {
        let event_type = select! {
            rf = remote_agg.get_new_fair().fuse() => TacticEventType::RemoteFair,
            block = bitstamp.next().fuse() => TacticEventType::LocalBook(block.events),
            order = bitstamp_orders.next().fuse() => TacticEventType::InsideOrders(
                local_book.handle_new_order(&order.events)),
                event = event_reader.recv().fuse() => match event {
                    Some(TacticInternalEvent::DisplayHtml) => TacticEventType::WriteHtml,
                    Some(TacticInternalEvent::OrderCanceled(cancel)) => TacticEventType::AckCancel(cancel),
                    Some(TacticInternalEvent::OrderSent(send)) => TacticEventType::AckSend(send),
                    Some(TacticInternalEvent::CancelStale(side, price, id)) => TacticEventType::CancelStale(side, price, id),
                    Some(TacticInternalEvent::SetLateStatus(side, price, id)) => TacticEventType::SetLateStatus(side, price, id),
                    None => panic!("event queue died"),
                },
            trades = bitstamp_trades.next().fuse() => TacticEventType::Trades(
                trades.events.into_iter().map(|t|
                                              match t {
                                                  MarketEvent::TradeUpdate(trade) => trade,
                                                  _ => panic!("Non-trade received"),
                                              }).collect()
                                                                           ),
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
            TacticEventType::Trades(trades) => {
                for trade in trades {
                    tactic.check_seen_trade(trade);
                }
            }
            TacticEventType::SetLateStatus(side, price, id) => {
                tactic.set_late_status(*id, *price, *side)
            }
            TacticEventType::AckCancel(cancel) => tactic.ack_cancel_for(cancel),
            TacticEventType::AckSend(sent) => tactic.ack_send_for(sent),
            TacticEventType::InsideOrders(_)
            | TacticEventType::WriteHtml
            | TacticEventType::CancelStale(_, _, _) => (),
        }
        if let (
            Some((((bid, _), (offer, _)), local_fair)),
            Some((displacement_val, expected_premium)),
            Some(remote_fair),
        ) = (
            local_book.get_local_tob(),
            displacement.get_displacement(),
            remote_agg.calculate_fair(),
        ) {
            let premium = local_fair - remote_fair;
            match &event_type {
                TacticEventType::RemoteFair | TacticEventType::LocalBook(_) => tactic
                    .handle_book_update(
                        (bid, offer),
                        local_fair,
                        displacement_val,
                        premium - expected_premium,
                    ),
                TacticEventType::InsideOrders(events) => tactic.handle_new_orders(
                    local_fair,
                    displacement_val,
                    premium - expected_premium,
                    &events,
                ),
                TacticEventType::CancelStale(side, price, id) => {
                    tactic.cancel_stale_id((bid, offer), *id, *price, *side)
                }
                TacticEventType::WriteHtml => {
                    let html = format!(
                        "
                        <!DOCYPE html>
                        <html>
                        <head>
                        <meta charset=\"UTF-8\" content-type=\"text/html\">
                        <meta name=\"description\" content=\"Bitcoin\">
                        <meta http-equiv=\"refresh\" content=\"3\" >
                        </head>
                        <body>
                        <h4>Going Since {start} </h4>
                        <h4>Last Update {now} </h4>
                        {tactic}
                        {local}
                        {remote}
                        {displacement}
                        {http}
                        </body>
                        </html>
                        ",
                        start = start,
                        now = Local::now(),
                        tactic = tactic.get_html_info(local_fair),
                        local = local_book.get_html_info(),
                        remote = remote_agg.get_html_info(),
                        displacement = displacement.get_html_info(),
                        http = http.get_html_info(),
                    );
                    html_queue.send(html).expect("Couldn't send html");
                }
                // Already handled
                TacticEventType::AckCancel(_)
                | TacticEventType::AckSend(_)
                | TacticEventType::SetLateStatus(_, _, _)
                | TacticEventType::Trades(_) => (),
            };
        }
    }
}
