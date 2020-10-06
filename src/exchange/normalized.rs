use futures::prelude::*;
use std::os::unix::io::AsRawFd;
use tokio_tungstenite::tungstenite::Message;

use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};

pub type SmallVec<T> = smallvec::SmallVec<[T; 8]>;
pub type DataStream = tokio_tungstenite::WebSocketStream<tokio_tungstenite::stream::Stream<
tokio::net::TcpStream,
tokio_native_tls::TlsStream<tokio::net::TcpStream>,
>>;

pub fn convert_price_cents(price: f64) -> usize {
    (price * 100.0).round() as usize
}

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
#[repr(C)]
pub enum Exchange {
    OkexSpot,
    OkexSwap,
    OkexQuarterly,
    HuobiSpot,
    BybitUSDT,
    Bitmex,
    Ftx,
    // Non-used by remote exchanges go below here
    COUNT,
    // This is the local exchange
    BybitInverse,
    // These ones are busted
    HuobiSwap,
    HuobiQuarterly,
}

#[derive(Deserialize, Serialize, Eq, PartialEq, Debug, Copy, Clone, Hash)]
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

impl Hash for BookUpdate {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.cents.hash(hasher);
        self.side.hash(hasher);
        self.exchange_time.hash(hasher);
        let int_size = (self.size * 100.0).round() as u64;
        int_size.hash(hasher);
    }
}

impl Hash for OrderUpdate {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.cents.hash(hasher);
        self.side.hash(hasher);
        self.exchange_time.hash(hasher);
        self.order_id.hash(hasher);
        let int_size = (self.size * 100.0).round() as u64;
        int_size.hash(hasher);
    }
}

#[derive(Serialize, Deserialize, Debug, Hash)]
pub enum MarketEvent {
    Book(BookUpdate),
    OrderUpdate(OrderUpdate),
    Clear,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MarketEventBlock {
    pub received_time: u128,
    pub exchange: Exchange,
    pub events: SmallVec<MarketEvent>,
}

pub struct MarketDataStream {
    stream: DataStream,
    exchange: Exchange,
    operator: fn(Message) -> DataOrResponse,
    num_created: usize,
    seen: bool,
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
        Exchange::Ftx => crate::exchange::ftx_connection().await,
        Exchange::OkexSwap => {
            crate::exchange::okex_connection(crate::exchange::okex::OkexType::Swap).await
        }
        Exchange::OkexQuarterly => {
            crate::exchange::okex_connection(crate::exchange::okex::OkexType::Quarterly).await
        }
        Exchange::BybitUSDT => {
            crate::exchange::bybit_connection(crate::exchange::bybit::BybitType::USDT).await
        }
        Exchange::BybitInverse => {
            crate::exchange::bybit_connection(crate::exchange::bybit::BybitType::Inverse).await
        }
        Exchange::COUNT => panic!("Not valid to do this lookup"),
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
            seen: false,
        }
    }

    pub async fn ping(&mut self) {
        let _ = self.stream.send(Message::Ping(Vec::new())).await;
    }

    pub fn quickack(&self) {
        set_sock_opt(&self.stream);
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
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_micros();
                        let events = match op(received) {
                            DataOrResponse::Response(msg) => {
                                self.stream.send(msg).await.unwrap();
                                continue;
                            }
                            DataOrResponse::Data(events) => events,
                        };
                        if events.len() > 0 {
                            if !self.seen {
                                self.seen = true;
                                println!("Saw first message from {:?}",
                                         self.exchange);
                            }
                            return MarketEventBlock {
                                events,
                                received_time: now,
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

#[cfg(target_platform = "linux")]
fn set_sock_opt(stream: &DataStream) {
    unsafe {
        let raw = stream.as_ref().as_raw_fd();
        let set_to = 1;
        libc::setsockopt(
            raw,
            libc::IPROTO_TCP,
            &libc::TCP_QUICKACK,
            &set_to as *const _ as *const libc::c_void,
            std::mem::size_of::<set_to>() as libc::sockopt_t,
        );
    }
}

#[cfg(not(target_platform = "linux"))]
fn set_sock_opt(stream: &DataStream) {}
