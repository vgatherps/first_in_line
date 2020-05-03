use std::time::{SystemTime, UNIX_EPOCH};
use tungstenite::{connect, Message};

use crate::order_book::get_price_from_id;
use level_fill::*;
use order_book::*;
mod level_fill;
mod order_book;

fn run() {
    let case_url = "wss://www.bitmex.com/realtime?subscribe=orderBookL2:XBTUSD";

    let (mut ws_stream, _) = connect(case_url).unwrap();
    let mut has_started = false;
    let mut book = OrderBook::new();
    let mut level_fills = ClearedNBBO::default();
    let mut old_bbo = (None, None);
    loop {
        while let Ok(msg) = ws_stream.read_message() {
            match msg {
                Message::Text(msg) => {
                    let start = SystemTime::now();
                    let time = start
                        .duration_since(UNIX_EPOCH)
                        .expect("Time went backwards")
                        .as_micros();
                    let rval: Result<LevelData, _> = serde_json::from_str(&msg);
                    match rval {
                        Err(_) => {
                            assert!(!has_started);
                        }
                        Ok(data) => {
                            has_started = true;
                            match data {
                                LevelData::Partial(data) => {
                                    for elem in data {
                                        book.add_level(&elem);
                                    }
                                }
                                LevelData::Insert(data) => {
                                    for elem in data {
                                        book.add_level(&elem);
                                        if let Some(cleared) =
                                            level_fills.handle_inserted_level(&elem)
                                        {
                                            assert!(cleared < time);
                                            println!(
                                                "price {} new side {:?} filled in {} us",
                                                get_price_from_id(elem.id),
                                                elem.side,
                                                time - cleared
                                            );
                                        }
                                    }
                                }
                                LevelData::Update(data) => {
                                    for elem in data {
                                        book.update_level(&elem);
                                    }
                                }
                                LevelData::Delete(data) => {
                                    data.iter()
                                        .filter_map(|del| book.delete_level(&del))
                                        .for_each(|del| level_fills.handle_cleared_bbo(&del, time));
                                }
                            };

                            let bbo = book.bbo();
                            if bbo != old_bbo {
                                //println!("{:?}", bbo);
                            }
                            old_bbo = bbo;
                        }
                    }
                }
                _ => panic!("Got unexpected message type"),
            }
        }
    }
}

fn main() {
    run();
}
