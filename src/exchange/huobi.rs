use crate::exchange::{normalized, normalized::{DataOrResponse, SmallVec}};

use async_tungstenite::{tokio::connect_async, tungstenite::Message};
use flate2::read::GzDecoder;
use futures::prelude::*;
use serde::Deserialize;

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
struct UpdateWrapper {
    tick: Update,
}

#[derive(Debug)]
pub enum HuobiType {
    Spot,
    Swap,
    Quarterly,
}

impl HuobiType {
    fn get_url(&self) -> &'static str {
        match self {
            HuobiType::Spot => "wss://api-aws.huobi.pro/ws",
            HuobiType::Swap => "wss://api.btcgateway.pro/swap-ws",
            HuobiType::Quarterly => "wss://api.btcgateway.pro/ws",
        }
    }
    fn get_product(&self) -> &'static str {
        match self {
            HuobiType::Spot => "btcusdt",
            HuobiType::Swap => "BTC-USD",
            HuobiType::Quarterly => "BTC-CQ",
        }
    }

    fn exchange(&self) -> normalized::Exchange {
        match self {
            HuobiType::Spot => normalized::Exchange::HuobiSpot,
            HuobiType::Swap => normalized::Exchange::HuobiSwap,
            HuobiType::Quarterly => normalized::Exchange::HuobiQuarterly,
        }
    }

    fn convert_dollars(&self, price: f64, size: f64) -> f64 {
        match self {
            HuobiType::Spot => price * size,
            _ => size * 100.0,
        }
    }

    fn get_convert(
        &self,
    ) -> fn(Message) -> DataOrResponse {
        match self {
            HuobiType::Spot => convert_spot,
            HuobiType::Swap => convert_derivative,
            _ => convert_future,
        }
    }
}

// TODO verify that the connection actually works
pub async fn huobi_connection(which: HuobiType) -> normalized::MarketDataStream {
    let (mut stream, _) = connect_async(which.get_url())
        .await
        .expect("Could not connect to huobi api");
    // What comes first
    let msg = Message::Text(format!(
        "{{\"sub\": \"market.{}.mbp.refresh.20\", \"id\": 1}}",
        which.get_product()
    ));
    stream.send(msg).await.expect("Could not request L2 stream");
    let ack = stream.next().await.unwrap().unwrap();
    match ack {
        Message::Binary(data) => {
            let mut deflater = GzDecoder::new(&data[..]);
            let mut s = String::new();
            deflater
                .read_to_string(&mut s)
                .expect("Could not unzip huobi message");
            if s.contains("rror") {
                panic!("Error subscribing to api: message {}", s);
            }
        }
        data => panic!("Incorrect ack type {:?}", data),
    };
    normalized::MarketDataStream::new(stream, which.exchange(), which.get_convert())
}

fn convert_spot(
    data: Message,
) -> DataOrResponse {
    convert_inner(data, HuobiType::Spot)
}

fn convert_derivative(
    data: Message,
) ->DataOrResponse {
    convert_inner(data, HuobiType::Swap)
}

fn convert_future(
    data: Message,

) -> DataOrResponse {
    convert_inner(data, HuobiType::Quarterly)
}

fn convert_inner(data: Message, which: HuobiType) -> DataOrResponse {
    let data = match data {
        Message::Binary(data) => {
            let mut deflater = GzDecoder::new(&data[..]);
            let mut s = String::new();
            deflater
                .read_to_string(&mut s)
                .expect("Could not unzip huobi message");
            s
        }
        data => panic!("Incorrect message type {:?}", data),
    };

    // Let's only check on small messages
    if data.len() < 40 && data.contains("ping") {
        let pong = data.replace("ping", "pong");
        return DataOrResponse::Response(Message::Text(pong));
    }
    let message: UpdateWrapper = serde_json::from_str(&data).expect("Couldn't parse huobi message");
    let mut result = SmallVec::new(); 
    result.push(normalized::MarketEvent::Clear);

    message.tick.bids.into_iter().for_each(|[price, size]| {
        result.push(normalized::MarketEvent::Book(normalized::BookUpdate {
            cents: price_to_cents(price),
            size: which.convert_dollars(price, size),
            side: normalized::Side::Buy,
            exchange_time: 0,
        }))
    });
    message.tick.asks.into_iter().for_each(|[price, size]| {
        result.push(normalized::MarketEvent::Book(normalized::BookUpdate {
            cents: price_to_cents(price),
            size: which.convert_dollars(price, size),
            side: normalized::Side::Sell,
            exchange_time: 0,
        }))
    });

    DataOrResponse::Data(result)
}
