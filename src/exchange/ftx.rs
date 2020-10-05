use crate::exchange::{
    normalized,
    normalized::{DataOrResponse, SmallVec},
};
type SmallString = smallstr::SmallString<[u8; 64]>;

use flate2::read::DeflateDecoder;
use futures::prelude::*;
use serde::Deserialize;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use std::io::prelude::Read;

fn price_to_cents(price: f64) -> usize {
    (price * 100.0).round() as usize
}

#[derive(Deserialize, Debug)]
struct Update {
    bids: SmallVec<[f64; 2]>,
    asks: SmallVec<[f64; 2]>,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type", content = "data")]
#[serde(rename_all = "lowercase")]
enum BookUpdate {
    Partial(Update),
    Update(Update),
}

// TODO verify that the connection actually works
pub async fn ftx_connection() -> normalized::MarketDataStream {
    let (mut stream, _) = connect_async("wss://ftx.com/ws/")
        .await
        .expect("Could not connect to ftx api");
    // What comes first
    let msg = Message::Text(format!(
        "{{\"op\": \"subscribe\", \"channel\": \"orderbook\", \"market\": \"BTC-PERP\"}}",
    ));
    stream.send(msg).await.expect("Could not request L2 stream");
    let ack = stream.next().await.unwrap().unwrap();
    match ack {
        Message::Text(data) => data,
        data => panic!("Incorrect ack type {:?}", data),
    };
    normalized::MarketDataStream::new(stream, normalized::Exchange::Ftx, convert_ftx)
}

pub fn convert_ftx(data: Message) -> DataOrResponse {
    let data = match data {
        Message::Text(data) => data,
        data => panic!("Incorrect message type {:?}", data),
    };
    let message = serde_json::from_str(&data).expect("Couldn't parse ftx message");
    let (mut result, ups) = match &message {
        BookUpdate::Partial(ups) => {
            let mut small = SmallVec::new();
            small.push(normalized::MarketEvent::Clear);
            (small, ups)
        }
        BookUpdate::Update(ups) => (SmallVec::new(), ups),
    };

    ups.bids.iter().for_each(|[price, size]| {
        result.push(normalized::MarketEvent::Book(normalized::BookUpdate {
            cents: price_to_cents(*price),
            size: price * size,
            side: normalized::Side::Buy,
            exchange_time: 0,
        }))
    });
    ups.asks.iter().for_each(|[price, size]| {
        result.push(normalized::MarketEvent::Book(normalized::BookUpdate {
            cents: price_to_cents(*price),
            size: price * size,
            side: normalized::Side::Sell,
            exchange_time: 0,
        }))
    });

    DataOrResponse::Data(result)
}
