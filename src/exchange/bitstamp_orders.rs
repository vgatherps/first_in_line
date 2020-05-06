// TODO TODO TODO de-duplicate from bitstamp
// TODO TODO TODO de-deplicate the buffering code found here
use crate::exchange::normalized;
use async_tungstenite::{tokio::connect_async, tungstenite::Message};
use futures::prelude::*;
use serde::Deserialize;

fn price_to_cents(price: f64) -> usize {
    (price * 100.0).round() as usize
}

#[derive(Deserialize, Debug)]
struct Order {
    microtimestamp: String,
    id: usize,
    order_type: usize,
    amount: f64,
    price: f64,
}

#[derive(Deserialize, Debug)]
struct Snapshot {
    microtimestamp: String,
    bids: Vec<[String; 3]>,
    asks: Vec<[String; 3]>,
}

#[derive(Deserialize, Debug)]
struct OrderWrapper {
    data: Order,
}

async fn get_bitstamp_stream(which: &str) -> normalized::DataStream {
    let (mut stream, _) = connect_async("wss://ws.bitstamp.net/")
        .await
        .expect("Could not connect to bitstamp api");
    println!("Connected to bitstamp");
    // What comes first
    let msg = Message::Text(format!(
        "{{
    \"event\": \"bts:subscribe\",
    \"data\": {{
        \"channel\": \"{}\"
    }}
}}",
        which
    ));
    stream
        .send(msg)
        .await
        .expect("Could not request bitstamp stream");
    // await subscription request
    stream.next().await.unwrap().unwrap();
    stream
}

// lol hardcoding
pub async fn bitstamp_orders_connection() -> normalized::MarketDataStream {
    let mut order_stream = get_bitstamp_stream("live_orders_btcusd").await;
    // wait to get a diff message before we try and request the full book
    // ensure proper ordering
    let mut first_diff_message = order_stream.next().await.unwrap().unwrap();
    let mut seen_diff_messages = vec![convert_inner(first_diff_message)];
    let mut full_response =
        reqwest::get("https://www.bitstamp.net/api/v2/order_book/btcusd&group=2")
            .await
            .unwrap()
            .text()
            .await
            .unwrap();

    let mut real_update_vec = vec![normalized::MarketEvent::Clear];

    // as far as I can tell, only the first await in the request chain is meaningfully
    // asynchronous.

    let (mut full_diff_update, timestamp) = convert_snapshot(&full_response);
    // if the most recent diff is after the book update, abort due to inconsistent
    // state. We know this will have one entry
    let first_diff = seen_diff_messages[0].1;
    if timestamp < first_diff {
        panic!(
            "Got diffs only after bookupdate: {}, {}",
            timestamp, first_diff
        );
    }
    // wait until we have some late-enough updates to ensure
    // we ditch all of the early ones
    while seen_diff_messages[seen_diff_messages.len() - 1].1 < timestamp {
        let diff_message = order_stream.next().await.unwrap().unwrap();
        seen_diff_messages.push(convert_inner(diff_message));
    }
    for (mut diff, diff_timestamp) in seen_diff_messages {
        if diff_timestamp < timestamp {
            continue;
        }
        real_update_vec.extend(diff);
    }

    // now, we first have to start requesting diff updates, then we request the full book,
    // then we ignore all diff updates that only apply to part of the book

    normalized::MarketDataStream::new_with(
        order_stream,
        normalized::Exchange::Bitstamp,
        real_update_vec,
        convert,
    )
}

fn convert(data: Message, _: &mut normalized::DataStream) -> Vec<normalized::MarketEvent> {
    convert_inner(data).0
}

fn convert_inner(data: Message) -> (Vec<normalized::MarketEvent>, usize) {
    let data = match data {
        Message::Text(data) => data,
        Message::Ping(_) => return (Vec::new(), 0),
        other => panic!("Got bogus message: {:?}", other),
    };
    let OrderWrapper { data } = serde_json::from_str(&data).expect("Couldn't parse snapshot");
    let Order {
        microtimestamp,
        id,
        order_type,
        amount,
        price,
    } = data;
    let microtimestamp = microtimestamp.parse().expect("non-integer timestamp");
    (
        vec![normalized::MarketEvent::OrderUpdate(
            normalized::OrderUpdate {
                exchange_time: microtimestamp,
                cents: price_to_cents(price),
                size: amount,
                side: if order_type == 1 {
                    normalized::Side::Sell
                } else {
                    normalized::Side::Buy
                },
                order_id: id,
            },
        )],
        microtimestamp,
    )
}

fn convert_snapshot(data: &str) -> (Vec<normalized::MarketEvent>, usize) {
    let Snapshot {
        microtimestamp,
        bids,
        asks,
    } = serde_json::from_str(data).expect("Couldn't parse snapshot");
    let mut result = vec![normalized::MarketEvent::Clear];
    let ts = microtimestamp.parse().expect("non-integer timestamp");
    bids.iter().for_each(|[price, size, id]| {
        let price: f64 = price.parse::<f64>().expect("Bad floating point");
        let size: f64 = size.parse::<f64>().expect("Bad floating point");
        result.push(normalized::MarketEvent::Book(normalized::BookUpdate {
            cents: price_to_cents(price),
            side: normalized::Side::Buy,
            size: price * size,
            exchange_time: ts,
        }))
    });
    asks.iter().for_each(|[price, size, id]| {
        let price: f64 = price.parse::<f64>().expect("Bad floating point");
        let size: f64 = size.parse::<f64>().expect("Bad floating point");
        result.push(normalized::MarketEvent::Book(normalized::BookUpdate {
            cents: price_to_cents(price),
            side: normalized::Side::Sell,
            size: price * size,
            exchange_time: ts,
        }))
    });
    (result, ts)
}
