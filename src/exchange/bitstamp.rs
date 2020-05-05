use crate::exchange::normalized;
use async_tungstenite::{tokio::connect_async, tungstenite::Message};
use futures::prelude::*;
use serde::Deserialize;

fn price_to_cents(price: f64) -> usize {
    (price * 100.0).round() as usize
}

#[derive(Deserialize, Debug)]
struct Snapshot {
    microtimestamp: String,
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

#[derive(Deserialize, Debug)]
struct SnapshotWrapper {
    data: Snapshot,
}

async fn get_bitstamp_stream(which: &str) -> normalized::DataStream {
    let (mut stream, _) = connect_async("wss://ws.bitstamp.net/")
        .await
        .expect("Could not connect to bitstamp api");
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
pub async fn bitstamp_connection() -> normalized::MarketDataStream {
    let mut l2_stream = get_bitstamp_stream("diff_order_book_btcusd").await;

    // wait to get a diff message before we try and request the full book
    // ensure proper ordering
    let mut first_diff_message = l2_stream.next().await.unwrap().unwrap();
    let mut seen_diff_messages = vec![convert_ts(first_diff_message, &mut l2_stream, false)];
    let mut full_response = reqwest::get("https://www.bitstamp.net/api/v2/order_book/btcusd")
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    let mut real_update_vec = vec![normalized::MarketEvent::Clear];

    // as far as I can tell, only the first awai in the request chain is meaningfully
    // asynchronous.

    let (mut full_diff_update, timestamp) = convert_ts(Message::Text(full_response), &mut l2_stream, true);
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
        let diff_message = l2_stream.next().await.unwrap().unwrap();
        seen_diff_messages.push(convert_ts(diff_message, &mut l2_stream, false));
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
        l2_stream,
        normalized::Exchange::Bitstamp,
        real_update_vec,
        convert,
    )
}

fn convert(data: Message, stream: &mut normalized::DataStream) -> Vec<normalized::MarketEvent> {
    convert_ts(data, stream, false).0
}

fn convert_ts(
    data: Message,
    stream: &mut normalized::DataStream,
    full: bool
) -> (Vec<normalized::MarketEvent>, usize) {
    let data = match data {
        Message::Text(data) => data,
        Message::Ping(_) => {
            return (vec![], 0);
        }
        data => panic!("Incorrect message type {:?}", data),
    };
    let data: Snapshot = if full {
        serde_json::from_str(&data).expect("Couldn't parse bitstamp message")
    } else {
        let SnapshotWrapper { data } =
            serde_json::from_str(&data).expect("Couldn't parse bitstamp message");
        data
    };
    let Snapshot {
        microtimestamp,
        bids,
        asks,
    } = data;
    let mut result = vec![normalized::MarketEvent::Clear];
    bids.iter().for_each(|[price, size]| {
        let price: f64 = price.parse::<f64>().expect("Bad floating point");
        let size: f64 = size.parse::<f64>().expect("Bad floating point");
        result.push(normalized::MarketEvent::Book(normalized::BookUpdate {
            cents: price_to_cents(price),
            side: normalized::Side::Buy,
            size: price * size,
        }))
    });
    asks.iter().for_each(|[price, size]| {
        let price: f64 = price.parse::<f64>().expect("Bad floating point");
        let size: f64 = size.parse::<f64>().expect("Bad floating point");
        result.push(normalized::MarketEvent::Book(normalized::BookUpdate {
            cents: price_to_cents(price),
            side: normalized::Side::Sell,
            size: price * size,
        }))
    });
    (result, microtimestamp.parse().expect("non-integer timestamp"))
}
