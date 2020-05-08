use crate::exchange::{normalized, normalized::SmallVec};
use async_tungstenite::{tokio::connect_async, tungstenite::Message};
use futures::prelude::*;
use serde::Deserialize;
type SmallString = smallstr::SmallString<[u8; 16]>;

fn price_to_cents(price: f64) -> usize {
    (price * 100.0).round() as usize
}

#[derive(Deserialize, Debug)]
struct Trade {
    sell_order_id: usize,
    buy_order_id: usize,
    #[serde(rename = "type")]
    order_type: usize,
    amount: f64,
    price: f64,
}

#[derive(Deserialize, Debug)]
struct TradeWrapper {
    data: Trade,
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
pub async fn bitstamp_trades_connection() -> normalized::MarketDataStream {
    let order_stream = get_bitstamp_stream("live_trades_btcusd").await;
    normalized::MarketDataStream::new(order_stream, normalized::Exchange::BitstampTrades, convert)
}

fn convert(data: Message, _: &mut normalized::DataStream) -> SmallVec<normalized::MarketEvent> {
    let data = match data {
        Message::Text(data) => data,
        Message::Ping(_) => return SmallVec::new(),
        other => panic!("Got bogus message: {:?}", other),
    };
    let TradeWrapper { data } = serde_json::from_str(&data).expect("Couldn't parse order");
    let Trade {
        sell_order_id,
        buy_order_id,
        order_type,
        amount,
        price,
    } = data;
    let rval = normalized::MarketEvent::TradeUpdate(normalized::TradeUpdate {
        cents: price_to_cents(price),
        size: amount,
        sell_order_id,
        buy_order_id,
        side: if order_type == 1 {
            normalized::Side::Sell
        } else {
            normalized::Side::Buy
        },
    });
    let mut rvec = SmallVec::new();
    rvec.push(rval);
    rvec
}
