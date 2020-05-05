use async_tungstenite::{
    stream::Stream as ATStream, tokio::TokioAdapter, tungstenite::Message, WebSocketStream,
};
use futures::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio_tls::TlsStream;

pub type DataStream = async_tungstenite::tokio::TokioWebSocketStream;

#[derive(Deserialize, Serialize, Debug, Copy, Clone, Hash, Eq, PartialEq)]
#[repr(C)]
pub enum Exchange {
    Bitmex,
    OkexSpot,
    OkexSwap,
    // Non-used by remote exchanges go below here
    COUNT,
    Bitstamp,
    Coinbase,
    OkexQuarterly,
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
    pub size: f64,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Quote {
    pub cents: usize,
    pub size: f64,
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
    Clear,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct MarketEventBlock {
    pub exchange: Exchange,
    pub events: Vec<MarketEvent>,
}

pub struct MarketDataStream {
    stream: DataStream,
    exchange: Exchange,
    initial_events: Option<Vec<MarketEvent>>,
    operator: fn(Message, &mut DataStream) -> Vec<MarketEvent>,
}

impl MarketDataStream {
    pub fn new(
        stream: DataStream,
        exchange: Exchange,
        operator: fn(Message, &mut DataStream) -> Vec<MarketEvent>,
    ) -> MarketDataStream {
        Self::new_with(stream, exchange, vec![], operator)
    }

    pub fn new_with(
        stream: DataStream,
        exchange: Exchange,
        events: Vec<MarketEvent>,
        operator: fn(Message, &mut DataStream) -> Vec<MarketEvent>,
    ) -> MarketDataStream {
        MarketDataStream {
            stream,
            exchange,
            initial_events: if events.len() > 0 { Some(events) } else { None },
            operator,
        }
    }

    // TODO handle failure gracefully, possibly try reconnecting?
    // just have a stream reconnection callback possibly.
    pub async fn next(&mut self) -> MarketEventBlock {
        if let Some(events) = self.initial_events.replace(vec![]) {
            self.initial_events = None;
            return MarketEventBlock {
                events,
                exchange: self.exchange,
            };
        }
        let op = self.operator;
        loop {
            let received: Message = self
                .stream
                .next()
                .await
                .expect("Market data stream died unexpectedly")
                .expect("Couldn't get valid websockets message");
            let events = op(received, &mut self.stream);
            if events.len() > 0 {
                return MarketEventBlock {
                    events,
                    exchange: self.exchange,
                };
            }
        }
    }
}
