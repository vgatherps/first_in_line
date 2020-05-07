use crate::exchange::{normalized, normalized::SmallVec};
use async_tungstenite::{tokio::connect_async, tungstenite::Message};
use futures::prelude::*;
use serde::Deserialize;
type SmallString = smallstr::SmallString<[u8; 64]>;

fn price_to_cents(price: f64) -> usize {
    (price * 100.0).round() as usize
}

#[derive(Deserialize, Debug)]
struct Order {
    microtimestamp: SmallString,
    id: usize,
    order_type: usize,
    amount: f64,
    price: f64,
}

#[derive(Deserialize, Debug)]
struct OrderWrapper {
    data: Order,
}

async fn get_bitstamp_stream(which: &str) -> normalized::DataStream {
    let (mut stream, _) = connect_async("wss://ws.bitstamp.net/")
        .await
        .expect("Could not connect to bitstamp api");
    // What comes first
    let msg = Message::Text(format!(
        "{{
    \"event\": \"bts:subscribe\",
    \"data\": {{
        \"channel\": \"{}\"
    }}
}}",
        which
    ));
    stream
        .send(msg)
        .await
        .expect("Could not request bitstamp stream");
    // await subscription request
    stream.next().await.unwrap().unwrap();
    stream
}

// lol hardcoding
pub async fn bitstamp_orders_connection() -> normalized::MarketDataStream {
    let order_stream = get_bitstamp_stream("live_orders_btcusd").await;
    normalized::MarketDataStream::new(order_stream, normalized::Exchange::BitstampOrders, convert)
}

fn convert(data: Message, _: &mut normalized::DataStream) -> SmallVec<normalized::MarketEvent> {
    let data = match data {
        Message::Text(data) => data,
        Message::Ping(_) => return SmallVec::new(),
        other => panic!("Got bogus message: {:?}", other),
    };
    let OrderWrapper { data } = serde_json::from_str(&data).expect("Couldn't parse order");
    let Order {
        microtimestamp,
        id,
        order_type,
        amount,
        price,
    } = data;
    let microtimestamp = microtimestamp.parse().expect("non-integer timestamp");
    let rval = normalized::MarketEvent::OrderUpdate(normalized::OrderUpdate {
        exchange_time: microtimestamp,
        cents: price_to_cents(price),
        size: amount,
        side: if order_type == 1 {
            normalized::Side::Sell
        } else {
            normalized::Side::Buy
        },
        order_id: id,
    });
    let mut rvec = SmallVec::new();
    rvec.push(rval);
    rvec
}
