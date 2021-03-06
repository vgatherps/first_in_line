use crate::exchange::{
    normalized,
    normalized::{DataOrResponse, MarketUpdates, SmallVec},
};

type SmallString = smallstr::SmallString<[u8; 64]>;

use async_tungstenite::{tokio::connect_async, tungstenite::Message};
use flate2::read::DeflateDecoder;
use futures::prelude::*;
use serde::Deserialize;

use std::io::prelude::Read;

fn price_to_cents(price: f64) -> usize {
    (price * 100.0).round() as usize
}

#[derive(Deserialize, Debug)]
struct Update {
    bids: SmallVec<[SmallString; 4]>,
    asks: SmallVec<[SmallString; 4]>,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "action", content = "data")]
#[serde(rename_all = "lowercase")]
enum BookUpdate {
    Update([Update; 1]),
    Partial([Update; 1]),
}

#[derive(Debug)]
pub enum OkexType {
    Spot,
    Swap,
    Quarterly,
}

impl OkexType {
    fn get_product(&self) -> &'static str {
        match self {
            OkexType::Spot => "spot/depth_l2_tbt:BTC-USDT",
            OkexType::Swap => "swap/depth_l2_tbt:BTC-USD-SWAP",
            OkexType::Quarterly => "futures/depth_l2_tbt:BTC-USD-200925",
        }
    }

    fn exchange(&self) -> normalized::Exchange {
        match self {
            OkexType::Spot => normalized::Exchange::OkexSpot,
            OkexType::Swap => normalized::Exchange::OkexSwap,
            OkexType::Quarterly => normalized::Exchange::OkexQuarterly,
        }
    }

    fn convert_dollars(&self, price: f64, size: f64) -> f64 {
        match self {
            OkexType::Spot => price * size,
            _ => size * 100.0,
        }
    }

    fn get_convert(&self) -> fn(Message) -> DataOrResponse {
        match self {
            OkexType::Spot => convert_spot,
            OkexType::Swap => convert_derivative,
            _ => convert_future,
        }
    }
}

// TODO verify that the connection actually works
pub async fn okex_connection(which: OkexType) -> normalized::MarketDataStream {
    let (mut stream, _) = connect_async("wss://real.OKEx.com:8443/ws/v3")
        .await
        .expect("Could not connect to okex api");
    // What comes first
    let msg = Message::Text(format!(
        "{{\"op\": \"subscribe\", \"args\": [\"{}\"]}}",
        which.get_product()
    ));
    stream.send(msg).await.expect("Could not request L2 stream");
    let ack = stream.next().await.unwrap().unwrap();
    match ack {
        Message::Binary(data) => {
            let mut deflater = DeflateDecoder::new(&data[..]);
            let mut s = String::new();
            deflater
                .read_to_string(&mut s)
                .expect("Could not unzip okex message");
            if s.contains("rror") {
                panic!("Error subscribing to api: message {}", s);
            }
        }
        data => panic!("Incorrect ack type {:?}", data),
    };
    normalized::MarketDataStream::new(stream, which.exchange(), which.get_convert())
}

fn convert_spot(data: Message) -> DataOrResponse {
    DataOrResponse::Data(convert_inner(data, OkexType::Spot))
}

fn convert_derivative(data: Message) -> DataOrResponse {
    DataOrResponse::Data(convert_inner(data, OkexType::Swap))
}

fn convert_future(data: Message) -> DataOrResponse {
    DataOrResponse::Data(convert_inner(data, OkexType::Quarterly))
}

fn convert_inner(data: Message, which: OkexType) -> MarketUpdates {
    let data = match data {
        Message::Binary(data) => {
            let mut deflater = DeflateDecoder::new(&data[..]);
            let mut s = String::new();
            deflater
                .read_to_string(&mut s)
                .expect("Could not unzip okex message");
            s
        }
        data => panic!("Incorrect message type {:?}", data),
    };
    let message = serde_json::from_str(&data).expect("Couldn't parse okex message");
    let mut result = SmallVec::new();
    let ups = match &message {
        BookUpdate::Partial([ups]) | BookUpdate::Update([ups]) => ups,
    };

    ups.bids.iter().for_each(|[price, size, _, _]| {
        let price: f64 = price.parse::<f64>().expect("Bad floating point");
        let size: f64 = size.parse::<f64>().expect("Bad floating point");
        result.push(normalized::BookUpdate {
            cents: price_to_cents(price),
            size: which.convert_dollars(price, size),
            side: normalized::Side::Buy,
            exchange_time: 0,
        })
    });
    ups.asks.iter().for_each(|[price, size, _, _]| {
        let price: f64 = price.parse::<f64>().expect("Bad floating point");
        let size: f64 = size.parse::<f64>().expect("Bad floating point");
        result.push(normalized::BookUpdate {
            cents: price_to_cents(price),
            size: which.convert_dollars(price, size),
            side: normalized::Side::Sell,
            exchange_time: 0,
        })
    });

    match &message {
        BookUpdate::Partial(_) => MarketUpdates::Reset(result),
        BookUpdate::Update(_) => MarketUpdates::Book(result),
    }
}
