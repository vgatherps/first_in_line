use crate::exchange::{
    normalized,
    normalized::{DataOrResponse, MarketUpdates, SmallVec},
};
use async_tungstenite::{tokio::connect_async, tungstenite::Message};
use futures::prelude::*;
use serde::Deserialize;
type SmallString = smallstr::SmallString<[u8; 64]>;

fn price_to_cents(price: f64) -> usize {
    (price * 100.0).round() as usize
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
enum Side {
    Buy,
    Sell,
}

impl Side {
    fn to_side(&self) -> normalized::Side {
        match self {
            Side::Buy => normalized::Side::Buy,
            Side::Sell => normalized::Side::Sell,
        }
    }
}

#[derive(Deserialize, Debug)]
struct Snapshot {
    bids: SmallVec<[SmallString; 2]>,
    asks: SmallVec<[SmallString; 2]>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
struct L2Update {
    changes: SmallVec<(Side, SmallString, SmallString)>,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "lowercase")]
enum BookUpdate {
    Snapshot(Snapshot),
    L2Update(L2Update),
}

// lol hardcoding
pub async fn coinbase_connection() -> normalized::MarketDataStream {
    let (mut stream, _) = connect_async("wss://ws-feed.pro.coinbase.com")
        .await
        .expect("Could not connect to coinbase api");
    // What comes first
    let msg = Message::Text(
        "{
    \"type\": \"subscribe\",
    \"product_ids\": [
        \"BTC-USD\"
    ],
    \"channels\": [
        \"level2\"
    ]
}"
        .into(),
    );
    stream.send(msg).await.expect("Could not request L2 stream");
    // await subscription request
    stream.next().await.unwrap().unwrap();
    normalized::MarketDataStream::new(stream, normalized::Exchange::Coinbase, convert)
}

fn convert(data: Message) -> DataOrResponse {
    let data = match data {
        Message::Text(data) => data,
        data => panic!("Incorrect message type {:?}", data),
    };
    let message = serde_json::from_str(&data).expect("Couldn't parse bitmex message");
    DataOrResponse::Data(match &message {
        BookUpdate::Snapshot(ups) => {
            let mut result = SmallVec::new();
            ups.bids.iter().for_each(|[price, size]| {
                let price: f64 = price.parse::<f64>().expect("Bad floating point");
                let size: f64 = size.parse::<f64>().expect("Bad floating point");
                result.push(normalized::BookUpdate {
                    cents: price_to_cents(price),
                    side: normalized::Side::Buy,
                    size: price * size,
                    exchange_time: 0,
                })
            });
            ups.asks.iter().for_each(|[price, size]| {
                let price: f64 = price.parse::<f64>().expect("Bad floating point");
                let size: f64 = size.parse::<f64>().expect("Bad floating point");
                result.push(normalized::BookUpdate {
                    cents: price_to_cents(price),
                    side: normalized::Side::Sell,
                    size: price * size,
                    exchange_time: 0,
                })
            });
            MarketUpdates::Reset(result)
        }
        BookUpdate::L2Update(L2Update { changes }) => {
            let result = changes
                .iter()
                .map(|(side, price, size)| {
                    let price: f64 = price.parse::<f64>().expect("Bad floating point");
                    let size: f64 = size.parse::<f64>().expect("Bad floating point");
                    let side = side.to_side();
                    normalized::BookUpdate {
                        cents: price_to_cents(price),
                        size: price * size,
                        side: side,
                        exchange_time: 0,
                    }
                })
                .collect();
            MarketUpdates::Book(result)
        }
    })
}
