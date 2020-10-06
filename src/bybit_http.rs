use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::Sha256;
type HmacSha256 = Hmac<Sha256>;

use horrorshow::html;

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::exchange::normalized::Side;

const MAX_NEW_ORDER: usize = 11;
const MAX_NEW_ORDER_DERISK: usize = 19;
const FAIL_THRESH: usize = 19;

#[derive(Deserialize)]
struct InnerOrderCanceled {
    price: f64,

    side: Side,

    #[serde(rename = "leaves_qty")]
    amount_order: usize,

    #[serde(rename = "order_link_id")]
    id: String,
}

#[derive(Debug)]
pub struct OrderCanceled {
    pub price: f64,
    pub amount: usize,
    pub side: Side,
    pub id: usize,
}

// I should parse the side, BUT my code already assumes no side is given
#[derive(Deserialize)]
struct InnerTransaction {
    #[serde(rename = "order_link_id")]
    order_link_id: String,
    #[serde(rename = "exec_type")]
    exec_type: String,

    #[serde(rename = "exec_id")]
    exec_id: String,

    leaves_qty: usize,
    order_qty: usize,

    #[serde(rename = "trade_time_ms")]
    timestamp: usize,

    #[serde(rename = "exec_price")]
    price: String,
    side: String,
}

#[derive(Deserialize)]
struct LessInnerTransaction {
    trade_list: Vec<InnerTransaction>,
}

#[derive(Debug)]
pub struct Transaction {
    pub order_id: usize,
    pub cum_size: usize,
    pub size: usize,
    pub cents: usize,
    pub timestamp: usize,
    pub exec_id: String,
    pub side: Side,
}

pub struct BybitHttp {
    http_client: reqwest::Client,
    auth_secret: String,
    auth_key: String,
    outstanding_request_counter: AtomicUsize,
}

#[derive(Deserialize)]
struct PartialBalance {
    size: isize,
}

#[derive(Deserialize)]
struct Result<T> {
    result: T,
}

async fn decrement(http: Arc<BybitHttp>) {
    tokio::time::delay_for(std::time::Duration::from_millis(1010)).await;
    http.decrement_outstanding();
}

async fn spawn_decrement_task(http: Arc<BybitHttp>, wait: bool) {
    http.increment_outstanding(wait, false).await;
    tokio::task::spawn(decrement(http));
}

async fn spawn_decrement_task_force(http: Arc<BybitHttp>) {
    http.increment_outstanding(false, true).await;
    tokio::task::spawn(decrement(http));
}

impl BybitHttp {
    pub fn new(auth_key: String, auth_secret: String) -> Self {
        BybitHttp {
            http_client: reqwest::ClientBuilder::new().build().unwrap(),
            auth_secret,
            auth_key,
            outstanding_request_counter: AtomicUsize::new(0),
        }
    }

    pub fn can_send_order(&self, derisk: bool) -> bool {
        if derisk {
            self.outstanding_request_counter.load(Ordering::Relaxed) < MAX_NEW_ORDER_DERISK
        } else {
            self.outstanding_request_counter.load(Ordering::Relaxed) < MAX_NEW_ORDER
        }
    }

    fn decrement_outstanding(&self) {
        self.outstanding_request_counter
            .fetch_sub(1, Ordering::Relaxed);
    }

    async fn increment_outstanding(&self, wait: bool, force: bool) {
        assert!(!(wait && force));
        if wait {
            while self.outstanding_request_counter.load(Ordering::Relaxed) >= FAIL_THRESH {
                tokio::time::delay_for(std::time::Duration::from_millis(50)).await;
            }
            self.outstanding_request_counter
                .fetch_add(1, Ordering::Relaxed);
        } else {
            let result = self
                .outstanding_request_counter
                .fetch_add(1, Ordering::Relaxed);
            assert!(force || result <= FAIL_THRESH);
        }
    }

    fn generate_request_params(&self, params: &mut [(&str, &str)]) -> Vec<(String, String)> {
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();
        let timestr = format!("{}", time);

        let mut params: Vec<(String, String)> = params
            .iter()
            .map(|(k, v)| (format!("{}", k), format!("{}", v)))
            .collect();
        params.push(("api_key".to_owned(), self.auth_key.clone()));
        params.push(("timestamp".to_owned(), timestr));
        params.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        let encoded_params = serde_urlencoded::to_string(&params).unwrap();

        let mut mac =
            HmacSha256::new_varkey(self.auth_secret.as_bytes()).expect("Mac works with any key");
        mac.input(encoded_params.as_bytes());
        let result = mac.result();
        let result_bytes = result.code();
        let hex_str = hex::encode(result_bytes);

        params.push(("sign".to_owned(), hex_str));
        params
    }

    // the api v1 cancel all was being weird so I do everything V2
    pub async fn cancel_all(&self, parent: Arc<Self>) {
        spawn_decrement_task_force(parent.clone()).await;

        let params = &mut [("symbol", "BTCUSD")][..];

        let params = self.generate_request_params(params);

        let result = self
            .http_client
            .post("https://api.bybit.com/v2/private/order/cancelAll")
            .form(&params)
            .send()
            .await
            .unwrap();
        let status = result.status();
        assert!(status.is_success());
    }

    pub async fn request_positions(&self, parent: Arc<Self>) -> isize {
        spawn_decrement_task(parent.clone(), true).await;

        let params = &mut [("symbol", "BTCUSD")][..];
        let params = self.generate_request_params(params);

        let result = self
            .http_client
            .get("https://api.bybit.com/v2/private/position/list")
            .query(&params)
            .send()
            .await
            .unwrap();
        let result = result.text().await.unwrap();
        let position: Result<PartialBalance> =
            serde_json::from_str(&result).expect("Could not parse position timestamp");
        position.result.size
    }

    // Now, the tracked transactions ...

    pub async fn request_transactions_from(
        &self,
        since: Option<usize>,
        parent: Arc<BybitHttp>,
    ) -> Vec<Transaction> {
        spawn_decrement_task(parent.clone(), true).await;

        let since = since.unwrap_or(0);
        let since = format!("{}", since);

        let params = &mut [
            ("start_time", since.as_str()),
            ("symbol", "BTCUSD"),
            ("order", "desc"),
        ][..];

        let params = self.generate_request_params(params);

        let result = self
            .http_client
            .get("https://api.bybit.com/v2/private/execution/list")
            .query(&params)
            .send()
            .await
            .unwrap();

        let status = result.status();
        let result = result.text().await.unwrap();

        assert!(status.is_success());

        let inner: Result<LessInnerTransaction> =
            serde_json::from_str(&result).expect("Couldn't parse transaction data");
        return inner
            .result
            .trade_list
            .into_iter()
            .filter(|t| t.exec_type == "Trade")
            .map(|t| {
                Transaction {
                    // some initial and website orders won't have this
                    order_id: t.order_link_id.parse().unwrap_or(0),
                    exec_id: t.exec_id,
                    cents: (t.price.parse::<f64>().unwrap() * 100.0).round() as usize,
                    timestamp: t.timestamp,
                    cum_size: t.order_qty.checked_sub(t.leaves_qty).unwrap(),
                    size: 0,
                    side: match t.side.as_str() {
                        "Buy" => Side::Buy,
                        "Sell" => Side::Sell,
                        _ => panic!("Got bogus side on a trade"),
                    },
                }
            })
            .collect();
    }

    pub async fn send_cancel(&self, id: usize, parent: Arc<BybitHttp>) -> Option<OrderCanceled> {
        spawn_decrement_task_force(parent.clone()).await;

        let id = format!("{}", id);

        let params = &mut [("order_link_id", id.as_str()), ("symbol", "BTCUSD")][..];

        let params = self.generate_request_params(params);

        let result = self
            .http_client
            .post("https://api.bybit.com/v2/private/order/cancel")
            .form(&params)
            .send()
            .await
            .unwrap();
        let result = result.text().await.unwrap();
        if result.contains("unknown") {
            return None;
        }
        let order = serde_json::from_str::<Result<Option<InnerOrderCanceled>>>(&result)
            .expect(&format!("Couldn't parse cancel response: {}", result))
            .result?;
        Some(OrderCanceled {
            price: order.price,
            amount: order.amount_order,
            id: order.id.parse().expect("Couldn't parse clordid"),
            side: order.side,
        })
    }

    pub async fn send_order(
        &self,
        amount: usize,
        price: f64,
        client_id: usize,
        side: Side,
        parent: Arc<BybitHttp>,
    ) -> bool {
        spawn_decrement_task(parent.clone(), false).await;
        let price = ((price * 100.0).round() + 0.01) / 100.0;
        assert_eq!(price, (price * 100.0) / 100.0);
        let side = match side {
            Side::Buy => "Buy",
            Side::Sell => "Sell",
        };

        let clord = format!("{}", client_id);
        let qty = format!("{}", amount);
        let price = format!("{:.2}", price);
        let params = &mut [
            ("order_link_id", clord.as_str()),
            ("order_type", "Limit"),
            ("time_in_force", "PostOnly"),
            ("qty", qty.as_str()),
            ("price", price.as_str()),
            ("symbol", "BTCUSD"),
            ("side", side),
        ][..];

        let params = self.generate_request_params(params);

        let result = self
            .http_client
            .post("https://api.bybit.com/v2/private/order/create")
            .form(&params)
            .send()
            .await
            .unwrap();
        let status = result.status();
        let text = result.text().await.unwrap();
        if !status.is_success() {
            panic!(
                "Failed with message {:?}, \nprice {}, size {}, side {:?}, clid {}",
                text, price, amount, side, client_id
            );
        }
        true
    }

    pub fn get_html_info(&self) -> String {
        format!(
            "{}",
            html! {
                h3(id="tactic state", class="title") : "Http Client Summary";
                ul(id="http summary") {
                    ln(first?=true, class="item") {
                        :format!("Requests in last 10 min: {}",
                                 self.outstanding_request_counter.load(Ordering::Relaxed));
                    }
                }
            }
        )
    }
}
