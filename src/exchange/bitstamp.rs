use crate::exchange::{normalized, normalized::{SmallVec, DataOrResponse}};
use async_tungstenite::{tokio::connect_async, tungstenite::Message};
use futures::prelude::*;
use serde::Deserialize;
type SmallString = smallstr::SmallString<[u8; 64]>;

fn price_to_cents(price: f64) -> usize {
    (price * 100.0).round() as usize
}

#[derive(Deserialize, Debug)]
struct Snapshot {
    microtimestamp: SmallString,
    bids: smallvec::SmallVec<[[SmallString; 2]; 128]>,
    asks: smallvec::SmallVec<[[SmallString; 2]; 128]>,
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
// This actually just returns the top N levels...
pub async fn bitstamp_connection() -> normalized::MarketDataStream {
    let l2_stream = get_bitstamp_stream("order_book_btcusd").await;

    normalized::MarketDataStream::new(l2_stream, normalized::Exchange::Bitstamp, convert)
}

fn convert(data: Message) -> DataOrResponse {
    let data = match data {
        Message::Text(data) => data,
        data => panic!("Incorrect message type {:?}", data),
    };
    let SnapshotWrapper { data } =
        serde_json::from_str(&data).expect("Couldn't parse bitstamp message");
    let Snapshot {
        microtimestamp,
        bids,
        asks,
    } = data;
    let mut result = SmallVec::new();
    result.reserve(42);
    result.push(normalized::MarketEvent::Clear);
    let ts = microtimestamp.parse().expect("non-integer timestamp");
    bids.iter().take(20).for_each(|[price, size]| {
        let price: f64 = price.parse::<f64>().expect("Bad floating point");
        let size: f64 = size.parse::<f64>().expect("Bad floating point");
        result.push(normalized::MarketEvent::Book(normalized::BookUpdate {
            cents: price_to_cents(price),
            side: normalized::Side::Buy,
            size: price * size,
            exchange_time: ts,
        }))
    });
    asks.iter().take(20).for_each(|[price, size]| {
        let price: f64 = price.parse::<f64>().expect("Bad floating point");
        let size: f64 = size.parse::<f64>().expect("Bad floating point");
        result.push(normalized::MarketEvent::Book(normalized::BookUpdate {
            cents: price_to_cents(price),
            side: normalized::Side::Sell,
            size: price * size,
            exchange_time: ts,
        }))
    });
    DataOrResponse::Data(result)
}
