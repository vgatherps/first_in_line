use crate::exchange::{
    normalized,
    normalized::{DataOrResponse, SmallVec},
};
use async_tungstenite::{tokio::connect_async, tungstenite::Message};
use futures::prelude::*;
use serde::Deserialize;
type SmallString = smallstr::SmallString<[u8; 64]>;

fn price_to_cents(price: f64) -> usize {
    (price * 100.0).round() as usize
}

#[derive(Copy, Clone)]
pub enum BybitType {
    USDT,
    Inverse,
}

impl BybitType {
    pub fn product(&self) -> &str {
        match self {
            BybitType::USDT => "wss://stream.bybit.com/realtime_public",
            BybitType::Inverse => "wss://stream.bybit.com/realtime",
        }
    }

    pub fn product_name(&self) -> &str {
        match self {
            BybitType::USDT => "USDT",
            BybitType::Inverse => "USD",
        }
    }

    pub fn price_size_dollars(&self, price: f64, size: f64) -> f64 {
        match self {
            BybitType::USDT => price * size,
            BybitType::Inverse => size,
        }
    }

    pub fn exchange(&self) -> normalized::Exchange {
        match self {
            BybitType::USDT => normalized::Exchange::BybitUSDT,
            BybitType::Inverse => normalized::Exchange::BybitInverse,
        }
    }

    pub fn convert(&self) -> fn(Message) -> DataOrResponse {
        match self {
            BybitType::USDT => convert_usdt,
            BybitType::Inverse => convert_inverse,
        }
    }
}

#[derive(Deserialize, Debug)]
struct Update {
    price: SmallString,
    side: normalized::Side,
    // On deletes, this will be auto-filled to zero
    #[serde(default)]
    size: f64,
}

#[derive(Deserialize, Debug)]
struct Delta {
    delete: SmallVec<Update>,
    update: SmallVec<Update>,
    insert: SmallVec<Update>,
}

#[derive(Deserialize, Debug)]
struct SnapshotInner {
    order_book: Vec<Update>
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type", content = "data")]
#[serde(rename_all = "lowercase")]
enum BookUpdate {
    Snapshot(SnapshotInner),
    Delta(Delta),
}

#[derive(Deserialize)]
struct FuturesSnapshot {
    data: Vec<Update>,
}

// lol hardcoding
pub async fn bybit_connection(which: BybitType) -> normalized::MarketDataStream {
    let (mut stream, _) = connect_async(which.product())
        .await
        .expect("Could not connect to coinbase api");
    // What comes first
    let msg = Message::Text(
        format!("{{\"op\": \"subscribe\", \"args\": [\"orderBookL2_25.BTC{}\"]}}",
                which.product_name()));
    stream.send(msg).await.expect("Could not request L2 stream");
    // We DON'T await a response since it comes out of order...
    normalized::MarketDataStream::new(stream, which.exchange(), which.convert())
}

fn convert_usdt(data: Message) -> DataOrResponse {
    convert_inner(data, BybitType::USDT)
}

fn convert_inverse(data: Message) -> DataOrResponse {
    convert_inner(data, BybitType::Inverse)
}

fn convert_inner(data: Message, which: BybitType) -> DataOrResponse {
    let data = match data {
        Message::Text(data) => data,
        data => panic!("Incorrect message type {:?}", data),
    };
    if data.contains("success") {
        return DataOrResponse::Data(SmallVec::new());
    }

    DataOrResponse::Data(if let Ok(message) = serde_json::from_str(&data) {
        match &message {
            BookUpdate::Snapshot(SnapshotInner { order_book }) => {
                let mut result = SmallVec::new();
                result.push(normalized::MarketEvent::Clear);
                order_book.iter().for_each(|Update { price, size, side }| {
                    let price: f64 = price.parse::<f64>().expect("Bad floating point");
                    let size: f64 = *size;
                    result.push(normalized::MarketEvent::Book(normalized::BookUpdate {
                        cents: price_to_cents(price),
                        size: which.price_size_dollars(price, size),
                        exchange_time: 0,
                        side: *side,
                    }))
                });
                result
            }
            BookUpdate::Delta(Delta {
                delete,
                update,
                insert,
            }) => delete
            .iter()
                .chain(update.iter())
                .chain(insert.iter())
                .map(|Update { price, size, side }| {
                    let price: f64 = price.parse::<f64>().expect("Bad floating point");
                    let size: f64 = *size;
                    normalized::MarketEvent::Book(normalized::BookUpdate {
                        cents: price_to_cents(price),
                        size: which.price_size_dollars(price, size),
                        exchange_time: 0,
                        side: *side,
                    })
                })
            .collect(),
        }
    } else {
        let futures_snapshot: FuturesSnapshot = serde_json::from_str(&data).expect("Couldn't parse json at all");
        // TODO less copy and paste
        let mut result = SmallVec::new();
        result.push(normalized::MarketEvent::Clear);
        futures_snapshot.data.iter().for_each(|Update { price, size, side }| {
            let price: f64 = price.parse::<f64>().expect("Bad floating point");
            let size: f64 = *size;
            result.push(normalized::MarketEvent::Book(normalized::BookUpdate {
                cents: price_to_cents(price),
                size: which.price_size_dollars(price, size),
                exchange_time: 0,
                side: *side,
            }))
        });
        result
    })
}
