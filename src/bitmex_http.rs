use hmac::{Hmac, Mac};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
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

const MAX_NEW_ORDER: usize = 20;
const FAIL_THRESH: usize = 59;

#[derive(Deserialize)]
struct InnerOrderCanceled {
    price: f64,

    side: Side,

    #[serde(rename = "orderQty")]
    amount: usize,

    #[serde(rename = "clOrdID")]
    id: String,
}

pub struct OrderCanceled {
    pub price: f64,
    pub amount: usize,
    pub side: Side,
    pub id: usize,
}

// I should parse the side, BUT my code already assumes no side is given
#[derive(Deserialize)]
struct InnerTransaction {
    #[serde(rename = "clOrdID")]
    order_id: String,
    #[serde(rename = "execType")]
    exec_type: String,
    #[serde(rename = "orderQty")]
    size: usize,

    timestamp: String,
    price: f64,
    side: String,
}

#[derive(Debug)]
pub struct Transaction {
    pub order_id: usize,
    pub size: usize,
    pub cents: usize,
    pub timestamp: String,
    pub side: Side,
}

pub struct BitmexHttp {
    http_client: reqwest::Client,
    auth_secret: String,
    api_expires: HeaderName,
    api_signature: HeaderName,
    outstanding_request_counter: AtomicUsize,
}

#[derive(Deserialize)]
struct PartialBalance {
    symbol: String,
    #[serde(rename = "currentQty")]
    current_qty: isize,
}

async fn decrement(http: Arc<BitmexHttp>) {
    tokio::time::delay_for(std::time::Duration::from_millis(61 * 1000)).await;
    http.decrement_outstanding();
}

pub fn spawn_decrement_task(http: Arc<BitmexHttp>) {
    http.increment_outstanding();
    tokio::task::spawn(decrement(http));
}

impl BitmexHttp {
    pub fn new(auth_key: String, auth_secret: String) -> Self {
        let api_expires = HeaderName::from_static("api-expires");
        let api_signature = HeaderName::from_static("api-signature");
        let api_key = HeaderName::from_static("api-key");
        let api_key_val = HeaderValue::from_str(&auth_key).unwrap();
        let mut common_headers = HeaderMap::new();
        common_headers.insert(api_key, api_key_val);
        BitmexHttp {
            http_client: reqwest::ClientBuilder::new()
                .tcp_nodelay()
                .default_headers(common_headers)
                .build()
                .unwrap(),
            auth_secret,
            api_expires,
            api_signature,
            outstanding_request_counter: AtomicUsize::new(0),
        }
    }

    pub fn can_send_order(&self) -> bool {
        self.outstanding_request_counter.load(Ordering::Relaxed) < MAX_NEW_ORDER
    }

    fn decrement_outstanding(&self) {
        self.outstanding_request_counter
            .fetch_sub(1, Ordering::Relaxed);
    }

    fn increment_outstanding(&self) {
        assert!(
            self.outstanding_request_counter
                .fetch_add(1, Ordering::Relaxed)
                < FAIL_THRESH
        );
    }

    fn generate_request_headers_v2(
        &self,
        payload: &str,
        verb: &'static str,
        path: &'static str,
    ) -> HeaderMap {
        // expires 10 seconds from now
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis()
            + (10 * 1000);
        let timestr = format!("{}", time);
        let timestamp_value = HeaderValue::from_str(&timestr).unwrap();

        let mac_str = format!(
            "{verb}{path}{expires}{data}",
            verb = verb,
            path = path,
            expires = timestr,
            data = payload
        );

        let mut mac =
            HmacSha256::new_varkey(self.auth_secret.as_bytes()).expect("Mac works with any key");
        mac.input(mac_str.as_bytes());
        let result_bytes = mac.result().code();
        let hex_str = hex::encode_upper(result_bytes);

        let secret_value = HeaderValue::from_str(&hex_str).unwrap();

        let mut new_headers = HeaderMap::new();
        new_headers.reserve(3);
        new_headers.insert(self.api_expires.clone(), timestamp_value);
        new_headers.insert(self.api_signature.clone(), secret_value);

        new_headers
    }

    // the api v1 cancel all was being weird so I do everything V2
    pub async fn cancel_all(&self, parent: Arc<Self>) {
        spawn_decrement_task(parent.clone());

        let headers = self.generate_request_headers_v2("", "DELETE", "/api/v1/order/all/");

        let result = self
            .http_client
            .delete("https://www.bitmex.com/api/v1/order/all/")
            .headers(headers)
            .send()
            .await
            .unwrap();
        assert!(result.status().is_success());
    }

    pub async fn request_positions(&self, parent: Arc<Self>) -> isize {
        spawn_decrement_task(parent.clone());

        let headers = self.generate_request_headers_v2("", "GET", "/api/v1/position/");

        let result = self
            .http_client
            .get("https://www.bitmex.com/api/v1/position/")
            .headers(headers)
            .send()
            .await
            .unwrap();
        let result = result.text().await.unwrap();
        let positions: Vec<PartialBalance> =
            serde_json::from_str(&result).expect("Could not parse position timestamp");
        positions
            .iter()
            .filter(|pos| pos.symbol == "XBTUSD")
            .next()
            .map(|pos| pos.current_qty)
            .expect("Did not get a position for XBTUSD")
    }

    // Now, the tracked transactions ...

    pub async fn request_transactions_from(
        &self,
        since: String,
        parent: Arc<BitmexHttp>,
    ) -> Vec<Transaction> {
        spawn_decrement_task(parent);

        let mut result = self
            .http_client
            .get("https://www.bitmex.com/api/v1/execution/tradeHistory/")
            .form(&[("startTime", since)])
            .build()
            .unwrap();

        let body = std::str::from_utf8(result.body().unwrap().as_bytes().unwrap()).unwrap();

        let headers =
            self.generate_request_headers_v2(body, "GET", "/api/v1/execution/tradeHistory/");

        let res_headers = result.headers_mut();
        for (name, val) in headers {
            res_headers.insert(name.unwrap(), val);
        }

        let result = self.http_client.execute(result).await.unwrap();
        assert!(result.status().is_success());
        let result = result.text().await.unwrap();

        //BIG BAD: I can't be bothered to change the code to have string order ids,
        //so I hash them... and assume there won't be conflicts in 64-bit space
        let inner: Vec<InnerTransaction> =
            serde_json::from_str(&result).expect("Couldn't parse transaction data");

        inner
            .into_iter()
            .filter(|t| t.exec_type == "Trade")
            .map(|t| {
                Transaction {
                    // some initial and website orders won't have this
                    order_id: t.order_id.parse().unwrap_or(0),
                    cents: (t.price * 100.0).round() as usize,
                    size: t.size,
                    timestamp: t.timestamp,
                    side: match t.side.as_str() {
                        "Buy" => Side::Buy,
                        "Sell" => Side::Sell,
                        _ => panic!("Got bogus side on a trade"),
                    },
                }
            })
            .collect()
    }

    pub async fn send_cancel(&self, id: usize, parent: Arc<BitmexHttp>) -> Option<OrderCanceled> {
        spawn_decrement_task(parent.clone());

        let mut result = self
            .http_client
            .delete("https://www.bitmex.com/api/v1/order/")
            .form(&[("clOrdID", id)])
            .build()
            .unwrap();

        let body = std::str::from_utf8(result.body().unwrap().as_bytes().unwrap()).unwrap();

        let headers = self.generate_request_headers_v2(body, "DELETE", "/api/v1/order/");

        let res_headers = result.headers_mut();
        for (name, val) in headers {
            res_headers.insert(name.unwrap(), val);
        }

        let result = self.http_client.execute(result).await.unwrap();
        let result = result.text().await.unwrap();
        let order: [InnerOrderCanceled; 1] =
            serde_json::from_str(&result).expect("Couldn't parse cancel response");
        let order = &order[0];
        Some(OrderCanceled {
            price: order.price,
            amount: order.amount,
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
        parent: Arc<BitmexHttp>,
    ) -> bool {
        spawn_decrement_task(parent.clone());
        let price = ((price * 100.0).round() + 0.01) / 100.0;
        assert_eq!(price, (price * 100.0) / 100.0);
        // bitmex allows one to set the side by sending negative quantities
        let amount = match side {
            Side::Buy => amount as isize,
            Side::Sell => amount as isize * -1,
        };
        let mut result = self
            .http_client
            .post("https://www.bitmex.com/api/v1/order/")
            .form(&(
                ("symbol", "XBTUSD"),
                ("price", &format!("{:.2}", price)),
                ("orderQty", amount),
                ("clOrdID", client_id),
            ))
            .build()
            .unwrap();

        let body = if let Some(body) = result.body() {
            std::str::from_utf8(body.as_bytes().unwrap()).unwrap()
        } else {
            ""
        };

        let headers = self.generate_request_headers_v2(body, "POST", "/api/v1/order/");

        let res_headers = result.headers_mut();
        for (name, val) in headers {
            res_headers.insert(name.unwrap(), val);
        }

        let result = self.http_client.execute(result).await.unwrap();
        let status = result.status();
        let text = result.text().await;
        if text.contains("overloaded") {
            return false;
        }
        if !result.status().is_success() {
            panic!(
                "Failed with message {:?}, \nprice {}, size {}, side {:?}, clid {}",
                result.text().await.unwrap(),
                price,
                amount,
                side,
                client_id
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
