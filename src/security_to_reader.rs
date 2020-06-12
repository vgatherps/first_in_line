use crate::exchange::normalized::*;
use crate::exchange::{
    bitmex_connection, bybit_connection, coinbase_connection, huobi_connection, okex_connection,
    BybitType, HuobiType, OkexType,
};

use crate::signal_graph::security_index::*;

pub struct MarketDataStream {
    inner: InnerMarketDataStream,
    index: SecurityIndex,
}

pub async fn reader_from_security(sec: &Security) -> Result<MarketDataStream, &Security> {
    match (sec.exchange.as_str(), sec.product.as_str()) {
        ("bitmex", "BTCMEX") => Ok(bitmex_connection().await),
        ("okex", "BTC_PERP_OK") => Ok(okex_connection(OkexType::Swap).await),
        ("okex", "BTC") => Ok(okex_connection(OkexType::Spot).await),
        ("okex", "BTC_QUARTERLY") => Ok(okex_connection(OkexType::Quarterly).await),
        ("bybit", "USDT") => Ok(bybit_connection(BybitType::USDT).await),
        ("bybit", "Inverse") => Ok(bybit_connection(BybitType::Inverse).await),
        ("huobi", "BTC_PERP_HB") => Ok(huobi_connection(HuobiType::Spot).await),
        ("gdax", "BTC") => Ok(coinbase_connection().await),
        _ => Err(sec),
    }
}
