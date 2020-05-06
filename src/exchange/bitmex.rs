use crate::exchange::{normalized, normalized::SmallVec};
use async_tungstenite::{tokio::connect_async, tungstenite::Message};
use futures::prelude::*;
use serde::Deserialize;

fn cents_from_id(id: usize) -> usize {
    (100000000 * 88usize).checked_sub(id).unwrap()
}

#[derive(Deserialize, Debug)]
struct Delete {
    pub id: usize,
    pub side: normalized::Side,
}

#[derive(Deserialize, Debug)]
struct Update {
    pub id: usize,
    pub side: normalized::Side,
    pub size: usize,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "action", content = "data")]
#[serde(rename_all = "lowercase")]
enum BookUpdate {
    Partial(SmallVec<Update>),
    Insert(SmallVec<Update>),
    Update(SmallVec<Update>),
    Delete(SmallVec<Delete>),
}

// lol hardcoding
pub async fn bitmex_connection() -> normalized::MarketDataStream {
    let (mut stream, _) =
        connect_async("wss://www.bitmex.com/realtime?subscribe=orderBookL2:XBTUSD")
            .await
            .expect("Could not connect to bitmex api");
    // eat the two welcome messages
    let _ = stream.next().await.unwrap();
    let _ = stream.next().await.unwrap();
    normalized::MarketDataStream::new(stream, normalized::Exchange::Bitmex, convert)
}

fn convert(data: Message, _: &mut normalized::DataStream) -> SmallVec<normalized::MarketEvent> {
    let data = match data {
        Message::Text(data) => data,
        data => panic!("Incorrect message type {:?}", data),
    };
    use BookUpdate::*;
    let data = serde_json::from_str(&data).expect("Couldn't parse bitmex message");
    let mut events: SmallVec<_> = match &data {
        Partial(ups) | Insert(ups) | Update(ups) => ups
            .iter()
            .map(|update| {
                normalized::MarketEvent::Book(normalized::BookUpdate {
                    cents: cents_from_id(update.id),
                    side: update.side,
                    size: update.size as f64,
                    exchange_time: 0,
                })
            })
            .collect(),
        Delete(ups) => ups
            .iter()
            .map(|update| {
                normalized::MarketEvent::Book(normalized::BookUpdate {
                    cents: cents_from_id(update.id),
                    side: update.side,
                    size: 0.0,
                    exchange_time: 0,
                })
            })
            .collect(),
    };
    match &data {
        Partial(_) => events.insert(0, normalized::MarketEvent::Clear),
        _ => (),
    };
    events
}
