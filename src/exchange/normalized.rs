use async_tungstenite::tungstenite::Message;
use futures::prelude::*;

use serde::{Deserialize, Serialize};

pub type SmallVec<T> = smallvec::SmallVec<[T; 8]>;
pub type DataStream = async_tungstenite::tokio::TokioWebSocketStream;

pub fn convert_price_cents(price: f64) -> usize {
    (price * 100.0).round() as usize
}

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub enum Exchange {
    OkexSpot,
    OkexSwap,
    OkexQuarterly,
    HuobiSpot,
    BybitUSDT,
    BybitInverse,
    Coinbase,
    Bitmex,
    Ftx,
    // These don't seem to work for some reason
    HuobiSwap,
    HuobiQuarterly,
}

#[derive(Deserialize, Serialize, Eq, PartialEq, Debug, Copy, Clone)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn flip(self) -> Self {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OrderUpdate {
    pub cents: usize,
    pub size: f64,
    pub order_id: usize,
    pub side: Side,
    pub exchange_time: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BookUpdate {
    pub cents: usize,
    pub side: Side,
    pub size: f64,
    pub exchange_time: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum MarketEvent {
    Book(BookUpdate),
    OrderUpdate(OrderUpdate),
    Clear,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MarketEventBlock {
    pub exchange: Exchange,
    pub events: SmallVec<MarketEvent>,
}

pub struct MarketDataStream {
    stream: DataStream,
    exchange: Exchange,
    operator: fn(Message) -> DataOrResponse,
    num_created: usize,
}

async fn lookup_stream(exchange: Exchange) -> DataStream {
    match exchange {
        Exchange::Bitmex => crate::exchange::bitmex_connection().await,

        Exchange::HuobiSpot => {
            crate::exchange::huobi_connection(crate::exchange::huobi::HuobiType::Spot).await
        }
        Exchange::HuobiSwap => {
            crate::exchange::huobi_connection(crate::exchange::huobi::HuobiType::Swap).await
        }
        Exchange::HuobiQuarterly => {
            crate::exchange::huobi_connection(crate::exchange::huobi::HuobiType::Quarterly).await
        }
        Exchange::OkexSpot => {
            crate::exchange::okex_connection(crate::exchange::okex::OkexType::Spot).await
        }
        Exchange::OkexSwap => {
            crate::exchange::okex_connection(crate::exchange::okex::OkexType::Swap).await
        }
        Exchange::OkexQuarterly => {
            crate::exchange::okex_connection(crate::exchange::okex::OkexType::Quarterly).await
        }
        Exchange::Coinbase => crate::exchange::coinbase_connection().await,
        Exchange::BybitUSDT => {
            crate::exchange::bybit_connection(crate::exchange::bybit::BybitType::USDT).await
        }
        Exchange::BybitInverse => {
            crate::exchange::bybit_connection(crate::exchange::bybit::BybitType::Inverse).await
        }
    }
    .stream
}

pub enum DataOrResponse {
    Data(SmallVec<MarketEvent>),
    Response(Message),
}

impl MarketDataStream {
    pub fn new(
        stream: DataStream,
        exchange: Exchange,
        operator: fn(Message) -> DataOrResponse,
    ) -> MarketDataStream {
        MarketDataStream {
            stream,
            exchange,
            operator,
            num_created: 0,
        }
    }

    pub async fn ping(&mut self) {
        let _ = self.stream.send(Message::Ping(Vec::new())).await;
    }

    pub async fn next(&mut self) -> MarketEventBlock {
        let op = self.operator;
        loop {
            let received = self.stream.next().await;
            if let Some(Ok(received)) = received {
                match received {
                    Message::Ping(data) => {
                        let _ = self.stream.send(Message::Pong(data)).await;
                        continue;
                    }
                    Message::Pong(_) => continue,
                    received => {
                        let events = match op(received) {
                            DataOrResponse::Response(msg) => {
                                self.stream.send(msg).await.unwrap();
                                continue;
                            }
                            DataOrResponse::Data(events) => events,
                        };
                        if events.len() > 0 {
                            return MarketEventBlock {
                                events,
                                exchange: self.exchange,
                            };
                        }
                    }
                }
            } else {
                if self.num_created > 10 {
                    panic!("Had to recreate stream too many times");
                }
                self.num_created += 1;
                println!(
                    "Had to recreate {:?}, this is {} time. Waiting for {} ms",
                    self.exchange,
                    self.num_created,
                    self.num_created * 500
                );
                tokio::time::delay_for(std::time::Duration::from_millis(
                    500 * self.num_created as u64,
                ))
                .await;
                let _ = self.stream.close(None).await;
                self.stream = lookup_stream(self.exchange).await
            }
        }
    }
}
