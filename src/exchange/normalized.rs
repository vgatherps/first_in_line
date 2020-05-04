use async_native_tls::TlsStream;
use async_std::net::TcpStream;
use async_tungstenite::stream::Stream as ATStream;
use async_tungstenite::WebSocketStream;
use futures::prelude::*;
use serde::{Deserialize, Serialize};
use tungstenite::Message;

pub type DataStream = WebSocketStream<ATStream<TcpStream, TlsStream<TcpStream>>>;

#[derive(Deserialize, Serialize, Debug, Copy, Clone)]
pub enum Exchange {
    Bitmex,
    HuobiSpot,
    HuobiPerp,
    OkExSpot,
    OkExPerp,
}

#[derive(Deserialize, Serialize, Eq, PartialEq, Debug, Copy, Clone)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct BookUpdate {
    pub cents: usize,
    pub side: Side,
    pub size: usize,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Quote {
    pub cents: usize,
    pub size: usize,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct BBOUpdate {
    bid: Option<Quote>,
    ask: Option<Quote>,
}

#[derive(Deserialize, Serialize, Debug)]
pub enum MarketEvent {
    BBO(BBOUpdate),
    Book(BookUpdate),
}

#[derive(Deserialize, Serialize, Debug)]
pub struct AllMarketDataTypes {
    exchange: Exchange,
    timestamp: u128,
    event: MarketEvent,
}

pub struct MarketDataStream {
    stream: DataStream,
    exchange: Exchange,
    operator: fn(Message, &mut DataStream) -> Vec<MarketEvent>,
}

impl MarketDataStream {
    pub fn new(
        stream: DataStream,
        exchange: Exchange,
        operator: fn(Message, &mut DataStream) -> Vec<MarketEvent>,
    ) -> MarketDataStream {
        MarketDataStream {
            stream,
            exchange,
            operator,
        }
    }

    pub fn exchange(&self) -> Exchange {
        self.exchange
    }

    // TODO handle failure gracefully, possibly try reconnecting?
    // just have a stream reconnection callback possibly.
    pub async fn next(&mut self) -> Vec<MarketEvent> {
        let op = self.operator;
        let received: Message = self
            .stream
            .next()
            .await
            .expect("Market data stream died unexpectedly")
            .expect("Couldn't get valid websockets message");
        op(received, &mut self.stream)
    }
}
