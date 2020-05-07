use crate::bitstamp_http::BitstampHttp;

#[derive(Debug)]
pub struct PositionManager {
    coins_balance: f64,
    dollars_balance: f64,

    coins_available: f64,
    dollars_available: f64,

    fee: f64,
}

impl PositionManager {
    pub async fn create(http: &mut BitstampHttp) -> PositionManager {
        http.cancel_all().await;
        let (coins_balance, dollars_balance, fee) = http.request_positions().await;
        PositionManager {
            coins_balance,
            dollars_balance,
            coins_available: coins_balance,
            dollars_available: dollars_balance,
            fee: fee,
        }
    }
}
