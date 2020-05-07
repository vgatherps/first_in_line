use async_tungstenite::tungstenite::Message;
use futures::prelude::*;
use serde::{Deserialize, Serialize};

pub type SmallVec<T> = smallvec::SmallVec<[T; 8]>;
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
    BitstampOrders,
    Coinbase,
    OkexQuarterly,
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

#[derive(Deserialize, Serialize, Debug)]
pub struct OrderUpdate {
    pub cents: usize,
    pub size: f64,
    pub order_id: usize,
    pub side: Side,
    pub exchange_time: usize,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct BookUpdate {
    pub cents: usize,
    pub side: Side,
    pub size: f64,
    pub exchange_time: usize,
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
    OrderUpdate(OrderUpdate),
    Clear,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct MarketEventBlock {
    pub exchange: Exchange,
    pub events: SmallVec<MarketEvent>,
}

pub struct MarketDataStream {
    stream: DataStream,
    exchange: Exchange,
    operator: fn(Message, &mut DataStream) -> SmallVec<MarketEvent>,
    num_created: usize,
}

async fn lookup_stream(exchange: Exchange) -> DataStream {
    match exchange {
        Exchange::Bitmex => crate::exchange::bitmex_connection().await,
        Exchange::OkexSpot => {
            crate::exchange::okex_connection(crate::exchange::okex::OkexType::Spot).await
        }
        Exchange::OkexSwap => {
            crate::exchange::okex_connection(crate::exchange::okex::OkexType::Swap).await
        }
        Exchange::OkexQuarterly => {
            crate::exchange::okex_connection(crate::exchange::okex::OkexType::Quarterly).await
        }
        Exchange::Bitstamp => crate::exchange::bitstamp_connection().await,
        Exchange::BitstampOrders => crate::exchange::bitstamp_orders_connection().await,
        Exchange::Coinbase => crate::exchange::coinbase_connection().await,
        Exchange::COUNT => panic!("Not valid to do this lookup"),
    }
    .stream
}

impl MarketDataStream {
    pub fn new(
        stream: DataStream,
        exchange: Exchange,
        operator: fn(Message, &mut DataStream) -> SmallVec<MarketEvent>,
    ) -> MarketDataStream {
        MarketDataStream {
            stream,
            exchange,
            operator,
            num_created: 0,
        }
    }

    pub async fn next(&mut self) -> MarketEventBlock {
        let op = self.operator;
        loop {
            let received = self.stream.next().await;
            if let Some(Ok(received)) = received {
                let events = op(received, &mut self.stream);
                if events.len() > 0 {
                    return MarketEventBlock {
                        events,
                        exchange: self.exchange,
                    };
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
