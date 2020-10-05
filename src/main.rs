#![recursion_limit = "512"]
//WARNING
//WARNING
//
//I would not abide by all of the given practices in here in production quality trading code

use exchange::{
    bitmex_connection, bybit_connection, ftx_connection, huobi_connection, okex_connection,
    BybitType, HuobiType, OkexType,
};

use crate::exchange::normalized::*;
use std::io::prelude::*;

use chrono::prelude::*;
use futures::{future::FutureExt, join, select};
use std::sync::mpsc;
use structopt::StructOpt;

use std::sync::Arc;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use std::collections::{HashMap, HashSet};

mod args;
mod bybit_http;
mod displacement;
mod ema;
mod exchange;
mod fair_value;
mod fifo_pnl;
mod local_book;
mod order_book;
mod order_manager;
mod position_manager;
mod remote_venue_aggregator;
mod tactic;

use fair_value::*;

pub static DIE: AtomicBool = AtomicBool::new(false);
pub static LOOP: AtomicUsize = AtomicUsize::new(0);

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
    OrderCanceled(bybit_http::OrderCanceled),
    Trades(SmallVec<bybit_http::Transaction>),
    SetLateStatus(Side, usize, usize),
    CancelStale(Side, usize, usize),
    CheckGone(Side, usize, usize),
    DisplayHtml,
    Ping,
    Reset(bool),
}

enum TacticEventType {
    RemoteFair,
    LocalBook(SmallVec<local_book::InsideOrder>),
    Trades(SmallVec<bybit_http::Transaction>),
    AckCancel(bybit_http::OrderCanceled),
    CancelStale(Side, usize, usize),
    CheckGone(Side, usize, usize),
    SetLateStatus(Side, usize, usize),
    WriteHtml,
    Ping,
    Reset,
    None,
}

async fn reset_loop(mut event_queue: tokio::sync::mpsc::Sender<TacticInternalEvent>) {
    tokio::time::delay_for(std::time::Duration::from_millis(1000 * 60 * 10)).await;
    assert!(event_queue
        .send(TacticInternalEvent::Reset(false))
        .await
        .is_ok());
}

async fn ping_loop(mut event_queue: tokio::sync::mpsc::Sender<TacticInternalEvent>) {
    loop {
        tokio::time::delay_for(std::time::Duration::from_millis(1000 * 30)).await;
        assert!(event_queue.send(TacticInternalEvent::Ping).await.is_ok());
    }
}

async fn html_writer_loop(mut event_queue: tokio::sync::mpsc::Sender<TacticInternalEvent>) {
    loop {
        tokio::time::delay_for(std::time::Duration::from_millis(1000)).await;
        assert!(event_queue
            .send(TacticInternalEvent::DisplayHtml)
            .await
            .is_ok());
    }
}

async fn get_max_timestamp(
    last_seen: Option<usize>,
    http: Arc<bybit_http::BybitHttp>,
) -> (usize, SmallVec<bybit_http::Transaction>) {
    let transactions = http
        .request_transactions_from(last_seen.clone(), http.clone())
        .await;
    let last_seen_filter = if let Some(ls) = last_seen { ls } else { 0 };
    // We must reverse the transactions to go from oldest to newest
    let transactions: SmallVec<_> = transactions
        .into_iter()
        .rev()
        .filter(|t| t.timestamp > last_seen_filter)
        .collect();
    let last_seen = transactions
        .iter()
        .map(|t| t.timestamp.clone())
        .max()
        .unwrap_or(last_seen_filter);
    (last_seen, transactions)
}

async fn transaction_loop(
    mut last_seen: usize,
    http: Arc<bybit_http::BybitHttp>,
    mut event_queue: tokio::sync::mpsc::Sender<TacticInternalEvent>,
) {
    let myloop = LOOP.load(Ordering::Relaxed);
    let mut seen = HashSet::new();
    let mut seen_qty = HashMap::new();
    while myloop == LOOP.load(Ordering::Relaxed) {
        tokio::time::delay_for(std::time::Duration::from_millis(1000 * 5)).await;
        let (new_last_seen, transactions) =
            get_max_timestamp(Some(last_seen.clone()), http.clone()).await;
        last_seen = new_last_seen;
        let mut transactions: SmallVec<_> = transactions
            .into_iter()
            .filter(|t| !seen.contains(&t.exec_id))
            .collect();
        for transaction in &mut transactions {
            seen.insert(transaction.exec_id.clone());
            let current_cum_qty = seen_qty.entry(transaction.order_id).or_insert(0usize);
            assert!(*current_cum_qty <= transaction.cum_size);
            transaction.size = transaction.cum_size - *current_cum_qty;
            *current_cum_qty = transaction.cum_size;
        }
        if transactions.len() > 0 {
            assert!(event_queue
                .send(TacticInternalEvent::Trades(transactions))
                .await
                .is_ok());
        }
    }
    println!("Exiting transaction loop");
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let start = Local::now();
    let args = args::Arguments::from_args();
    let (html_queue, html_reader) = std::sync::mpsc::channel();

    //http state lives outside of the loop to properly rate-limit requests
    let http = bybit_http::BybitHttp::new(args.auth_key, args.auth_secret);
    let http = Arc::new(http);

    let test_position = position_manager::PositionManager::create(http.clone()).await;
    http.request_positions(http.clone()).await;

    let html = args.html.clone();
    std::thread::spawn(move || html_writer(html, html_reader));

    let mut statistics = tactic::TacticStatistics::new();

    drop(test_position);

    let mut bad_runs_count: usize = 0;
    loop {
        // This is a little weird. We need to 'kill this', but actually dropping it poisons
        // the various events pushing into it. So instead, this lives outside the data loop scope,
        // and at the end of each iteration a task is spawned (read: leaked) to consume and drop
        // all incoming messages
        let (event_queue, mut event_reader) = tokio::sync::mpsc::channel(100);
        {
            // and is part of the reset cycle
            let position = position_manager::PositionManager::create(http.clone()).await;

            let (
                mut bybit_inverse,
                okex_spot,
                okex_swap,
                okex_quarterly,
                bybit_usdt,
                ftx,
                bitmex,
                huobi,
            ) = join!(
                bybit_connection(BybitType::Inverse),
                okex_connection(OkexType::Spot),
                okex_connection(OkexType::Swap),
                okex_connection(OkexType::Quarterly),
                bybit_connection(BybitType::USDT),
                ftx_connection(),
                bitmex_connection(),
                huobi_connection(HuobiType::Spot),
            );

            // Spawn all tasks after we've connected to everything
            tokio::task::spawn(html_writer_loop(event_queue.clone()));
            tokio::task::spawn(reset_loop(event_queue.clone()));
            tokio::task::spawn(ping_loop(event_queue.clone()));
            tokio::task::spawn(transaction_loop(
                get_max_timestamp(None, http.clone()).await.0,
                http.clone(),
                event_queue.clone(),
            ));

            let remote_fair_value = FairValue::new(1.0, 0.0, 5.0, 10);
            let local_fair_value = FairValue::new(0.7, 0.05, 20.0, 20);

            let mut remote_agg = remote_venue_aggregator::RemoteVenueAggregator::new(
                okex_spot,
                okex_swap,
                okex_quarterly,
                bybit_usdt,
                bitmex,
                huobi,
                ftx,
                remote_fair_value,
                0.001,
            );

            let mut local_book = local_book::LocalBook::new(local_fair_value);

            let mut displacement = displacement::Displacement::new();

            let mut tactic = tactic::Tactic::new(
                args.profit_bps,
                args.profit_bps_cancel,
                args.fee_bps,
                args.cost_of_position,
                args.base_trade_contracts,
                position,
                &mut statistics,
                http.clone(),
                event_queue.clone(),
            );

            loop {
                if DIE.load(Ordering::Relaxed) {
                    panic!("Death variable set");
                }
                let event_type = select! {
                    block = bybit_inverse.next().fuse() => {
                        TacticEventType::LocalBook(local_book.handle_book_update(&block.events))
                    },
                    rf = remote_agg.get_new_fair().fuse() => {
                        TacticEventType::RemoteFair
                    }
                    event = event_reader.recv().fuse() => match event {
                            Some(TacticInternalEvent::DisplayHtml) => TacticEventType::WriteHtml,
                            Some(TacticInternalEvent::OrderCanceled(cancel)) => TacticEventType::AckCancel(cancel),
                            Some(TacticInternalEvent::Trades(trades)) => TacticEventType::Trades(trades),
                            Some(TacticInternalEvent::CancelStale(side, price, id)) => TacticEventType::CancelStale(side, price, id),
                            Some(TacticInternalEvent::CheckGone(side, price, id)) => TacticEventType::CheckGone(side, price, id),
                            Some(TacticInternalEvent::SetLateStatus(side, price, id)) => TacticEventType::SetLateStatus(side, price, id),
                            Some(TacticInternalEvent::Reset(is_bad)) => {
                                if is_bad {
                                    bad_runs_count += 1;
                                }
                                TacticEventType::Reset
                            },
                            Some(TacticInternalEvent::Ping) => TacticEventType::Ping,
                            None => panic!("event queue died"),
                        },
                };
                match &event_type {
                    TacticEventType::RemoteFair => {
                        if let Some((rf, rs)) = remote_agg.calculate_fair() {
                            displacement.handle_remote(rf, rs);
                        }
                    }
                    TacticEventType::LocalBook(_) => {
                        if let Some((_, (local_fair, local_size))) = local_book.get_local_tob() {
                            displacement.handle_local(local_fair, local_size);
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
                    TacticEventType::CancelStale(side, price, id) => {
                        tactic.cancel_stale_id(*id, *price, *side)
                    }
                    TacticEventType::CheckGone(side, price, id) => {
                        tactic.check_order_gone(*id, *price, *side)
                    }
                    TacticEventType::AckCancel(cancel) => tactic.ack_cancel_for(cancel),
                    TacticEventType::Reset => {
                        // let in-flight items propogate
                        // We do a cancel all before we wait, and will do another after the wait
                        // In almost all cases this should get rid of in-flight orders
                        tokio::time::delay_for(std::time::Duration::from_millis(1000 * 2)).await;

                        // Do a reset
                        break;
                    }
                    TacticEventType::Ping => {
                        let _ = join!(bybit_inverse.ping(), remote_agg.ping());
                    }
                    TacticEventType::WriteHtml | TacticEventType::None => (),
                }
                if let (
                    Some((_, (local_fair, local_size))),
                    Some((displacement_val, expected_premium)),
                    Some((remote_fair, remote_size)),
                ) = (
                    local_book.get_local_tob(),
                    displacement.get_displacement(),
                    remote_agg.calculate_fair(),
                ) {
                    let premium = local_fair - remote_fair;
                    // again, discount the displacement by size weighting.
                    // I really need to centralize these "forecasts"

                    let total_size = local_size + remote_size;
                    let remote_adjust = remote_size / total_size;
                    let local_adjust = local_size / total_size;

                    let desired_premium = premium * local_adjust + expected_premium * remote_adjust;

                    match &event_type {
                        TacticEventType::RemoteFair => tactic.handle_book_update(
                            local_book.book(),
                            &SmallVec::new(),
                            local_fair,
                            displacement_val,
                            desired_premium,
                        ),
                        TacticEventType::LocalBook(inside_orders) => tactic.handle_book_update(
                            local_book.book(),
                            &inside_orders,
                            local_fair,
                            displacement_val,
                            desired_premium,
                        ),
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
                        | TacticEventType::SetLateStatus(_, _, _)
                        | TacticEventType::CancelStale(_, _, _)
                        | TacticEventType::CheckGone(_, _, _)
                        | TacticEventType::Reset
                        | TacticEventType::Ping
                        | TacticEventType::None
                        | TacticEventType::Trades(_) => (),
                    };
                }
            }
        }

        // We keep the state in the destructor to ensure everything exits cleanly
        println!("Resetting time {}", bad_runs_count);
        assert!(bad_runs_count <= 5);
        LOOP.fetch_add(1, Ordering::SeqCst);
        tokio::spawn(async move { while let Some(_) = event_reader.recv().await {} });
        tokio::time::delay_for(std::time::Duration::from_millis(1000 * 2)).await;
    }
}
