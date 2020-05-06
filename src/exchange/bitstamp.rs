use crate::exchange::{normalized, normalized::SmallVec};
use async_tungstenite::{tokio::connect_async, tungstenite::Message};
use futures::prelude::*;
use serde::Deserialize;

fn price_to_cents(price: f64) -> usize {
    (price * 100.0).round() as usize
}

#[derive(Deserialize, Debug)]
struct Snapshot {
    microtimestamp: String,
    bids: SmallVec<[String; 2]>,
    asks: SmallVec<[String; 2]>,
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
    let first_diff_message = l2_stream.next().await.unwrap().unwrap();
    let mut seen_diff_messages = vec![convert_ts(first_diff_message, &mut l2_stream, false)];
    // wait 200ms - this seems to help bitstamp
    tokio::time::delay_for(std::time::Duration::from_millis(100)).await;
    let full_response = reqwest::get("https://www.bitstamp.net/api/v2/order_book/btcusd")
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    let mut real_update_vec = vec![normalized::MarketEvent::Clear];

    // as far as I can tell, only the first awai in the request chain is meaningfully
    // asynchronous.

    let (mut full_diff_update, mut timestamp) =
        convert_ts(Message::Text(full_response), &mut l2_stream, true);
    // if the most recent diff is after the book update, abort due to inconsistent
    // state. We know this will have one entry
    let first_diff = seen_diff_messages[0].1;
    let mut tries_count: usize = 0;
    while timestamp < first_diff {
        if tries_count > 4 {
            panic!("Unable to get reasonably timestamped order book");
        }

        tries_count += 1;

        // wait 200ms and try again
        tokio::time::delay_for(std::time::Duration::from_millis(100)).await;

        let full_response =
            reqwest::get("https://www.bitstamp.net/api/v2/order_book/btcusd?group=2")
                .await
                .unwrap()
                .text()
                .await
                .unwrap();

        let (new_full_diff_update, new_timestamp) =
            convert_ts(Message::Text(full_response), &mut l2_stream, true);
        full_diff_update = new_full_diff_update;
        timestamp = new_timestamp;
    }
    real_update_vec.extend(full_diff_update);
    // wait until we have some late-enough updates to ensure
    // we ditch all of the early ones
    while seen_diff_messages[seen_diff_messages.len() - 1].1 < timestamp {
        let diff_message = l2_stream.next().await.unwrap().unwrap();
        seen_diff_messages.push(convert_ts(diff_message, &mut l2_stream, false));
    }
    for (diff, diff_timestamp) in seen_diff_messages {
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

fn convert(
    data: Message,
    stream: &mut normalized::DataStream,
) -> SmallVec<normalized::MarketEvent> {
    convert_ts(data, stream, false).0
}

fn convert_ts(
    data: Message,
    _: &mut normalized::DataStream,
    full: bool,
) -> (SmallVec<normalized::MarketEvent>, usize) {
    let data = match data {
        Message::Text(data) => data,
        Message::Ping(_) => {
            return (SmallVec::new(), 0);
        }
        data => panic!("Incorrect message type {:?}", data),
    };
    let data: Snapshot = if full {
        // TODO occasionally, this fails
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
    let mut result = SmallVec::new();
    result.push(normalized::MarketEvent::Clear);
    let ts = microtimestamp.parse().expect("non-integer timestamp");
    bids.iter().for_each(|[price, size]| {
        let price: f64 = price.parse::<f64>().expect("Bad floating point");
        let size: f64 = size.parse::<f64>().expect("Bad floating point");
        result.push(normalized::MarketEvent::Book(normalized::BookUpdate {
            cents: price_to_cents(price),
            side: normalized::Side::Buy,
            size: price * size,
            exchange_time: ts,
        }))
    });
    asks.iter().for_each(|[price, size]| {
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
