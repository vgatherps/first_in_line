use hmac::{Hmac, Mac};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::Deserialize;
use sha2::Sha256;
type HmacSha256 = Hmac<Sha256>;
type SmallString = smallstr::SmallString<[u8; 32]>;
use horrorshow::html;

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::exchange::normalized::Side;

const MAX_NEW_ORDER: usize = 7500;
const FAIL_THRESH: usize = 7900;

#[derive(Deserialize)]
struct Balance {
    btc_available: SmallString,
    btc_balance: SmallString,
    usd_available: SmallString,
    usd_balance: SmallString,
    fee: f64,
}

#[derive(Deserialize)]
struct InnerOrderCanceled {
    price: f64,
    amount: f64,
    #[serde(rename = "type")]
    in_type: usize,
    id: usize,
}

pub struct OrderCanceled {
    pub price: f64,
    pub amount: f64,
    pub side: Side,
    pub id: usize,
}

#[derive(Deserialize)]
struct InnerOrderSent {
    price: SmallString,
    amount: SmallString,
    #[serde(rename = "type")]
    in_type: SmallString,
    id: SmallString,
}

pub struct OrderSent {
    pub price: f64,
    pub amount: f64,
    pub side: Side,
    pub id: usize,
}

// TODO fix assuming that the last transaction was a trade
#[derive(Deserialize, Debug)]
struct InnerTransaction {
    id: usize,
    order_id: usize,
    usd: SmallString,
    btc: SmallString,
    btc_usd: f64,
    fee: SmallString,

    #[serde(rename = "type")]
    tr_type: SmallString,
}

#[derive(Debug)]
pub struct Transaction {
    pub id: usize,
    pub order_id: usize,
    pub usd: f64,
    pub btc: f64,
    pub btc_usd: f64,
    pub fee: f64,
    pub side: Side,
}

pub struct BitstampHttp {
    http_client: reqwest::Client,
    auth_key: String,
    auth_secret: String,
    x_auth_sig: HeaderName,
    x_auth_nonce: HeaderName,
    x_auth_timestamp: HeaderName,
    request_counter: AtomicUsize,
    outstanding_request_counter: AtomicUsize,
}

async fn decrement(http: Arc<BitstampHttp>) {
    tokio::time::delay_for(std::time::Duration::from_millis((10 * 60 + 64) * 1000)).await;
    http.decrement_outstanding();
}

pub fn spawn_decrement_task(http: Arc<BitstampHttp>) {
    http.increment_outstanding();
    tokio::task::spawn(decrement(http));
}

impl BitstampHttp {
    pub fn new(auth_key: String, auth_secret: String) -> Self {
        let x_auth = HeaderName::from_static("x-auth");
        let x_auth_value = HeaderValue::from_str(&("BITSTAMP ".to_string() + &auth_key)).unwrap();
        let x_auth_version = HeaderName::from_static("x-auth-version");
        let x_auth_version_value = HeaderValue::from_static("v2");
        let mut common_headers = HeaderMap::new();
        common_headers.insert(x_auth, x_auth_value);
        common_headers.insert(x_auth_version, x_auth_version_value);
        BitstampHttp {
            http_client: reqwest::ClientBuilder::new()
                .tcp_nodelay()
                .default_headers(common_headers)
                .build()
                .unwrap(),
            auth_key,
            auth_secret,
            x_auth_nonce: HeaderName::from_static("x-auth-nonce"),
            x_auth_sig: HeaderName::from_static("x-auth-signature"),
            x_auth_timestamp: HeaderName::from_static("x-auth-timestamp"),
            request_counter: AtomicUsize::new(1),
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

    // TODO TODO TODO this can all be precomputed
    fn generate_request_headers_v2(
        &self,
        payload: &str,
        time: u128,
        verb: &'static str,
        path: &'static str,
        query: &str,
    ) -> HeaderMap {
        let counter = self.request_counter.fetch_add(1, Ordering::Relaxed);
        let timestr = format!("{}", time);
        let nonce = format!("{}{}000000000000000000000000000000000000", counter, timestr);
        let nonce_36 = &nonce.as_bytes()[..36];

        let nonce_value = HeaderValue::from_bytes(nonce_36).unwrap();
        let timestamp_value = HeaderValue::from_str(&timestr).unwrap();

        let content_type = if payload.len() == 0 {
            ""
        } else {
            "application/x-www-form-urlencoded"
        };

        let mac_str = format!(
            "BITSTAMP {key}{verb}www.bitstamp.net{path}{query}{content}{nonce}{timestamp}v2{body}",
            key = self.auth_key,
            verb = verb,
            path = path,
            query = query,
            content = content_type,
            nonce = nonce_value.to_str().unwrap(),
            timestamp = timestr,
            body = payload
        );

        let mut mac =
            HmacSha256::new_varkey(self.auth_secret.as_bytes()).expect("Mac works with any key");
        mac.input(mac_str.as_bytes());
        let result_bytes = mac.result().code();
        let hex_str = hex::encode_upper(result_bytes);

        let secret_value = HeaderValue::from_str(&hex_str).unwrap();

        let mut new_headers = HeaderMap::new();
        new_headers.reserve(3);
        new_headers.insert(self.x_auth_sig.clone(), secret_value);
        new_headers.insert(self.x_auth_nonce.clone(), nonce_value);
        new_headers.insert(self.x_auth_timestamp.clone(), timestamp_value);

        new_headers
    }

    fn generate_v1_signature(&self) -> (String, String) {
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();
        let nonce_str = format!("{}", time);
        let mac_str = format!("{}jwhf0251{}", nonce_str, self.auth_key);
        let mut mac =
            HmacSha256::new_varkey(self.auth_secret.as_bytes()).expect("Mac works with any key");
        mac.input(mac_str.as_bytes());
        let result_bytes = mac.result().code();
        let hex_str = hex::encode_upper(result_bytes);
        (hex_str, nonce_str)
    }

    // These are 1-time calls, so I don't worry about balancing them

    pub async fn cancel_all(&self) {
        let (sig, nonce) = self.generate_v1_signature();
        let res = self
            .http_client
            .post("https://www.bitstamp.net/api/cancel_all_orders/")
            .form(&[
                ("key", &self.auth_key),
                ("signature", &sig),
                ("nonce", &nonce),
            ]);
        let res = res.send().await.unwrap();
        assert!(res.status().is_success());
        println!("cancel all response: {}", res.text().await.unwrap());
    }

    pub async fn request_positions(&self) -> (f64, f64, f64) {
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();

        let headers =
            self.generate_request_headers_v2("", time, "POST", "/api/v2/balance/btcusd/", "");

        let result = self
            .http_client
            .post("https://www.bitstamp.net/api/v2/balance/btcusd/")
            .headers(headers)
            .send()
            .await
            .unwrap();
        assert!(result.status().is_success());
        let result = result.text().await.unwrap();
        let Balance {
            btc_available,
            btc_balance,
            usd_available,
            usd_balance,
            fee,
        } = serde_json::from_str(&result).expect("Couldn't parse balance message");

        assert_eq!(btc_available, btc_balance);
        assert_eq!(usd_available, usd_balance);

        (
            btc_balance.parse().unwrap(),
            usd_available.parse().unwrap(),
            fee,
        )
    }

    pub async fn request_initial_transaction(&self) -> usize {
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();

        let mut result = self
            .http_client
            .post("https://www.bitstamp.net/api/v2/user_transactions/btcusd/")
            .form(&[("limit", "1")])
            .build()
            .unwrap();

        let body = std::str::from_utf8(result.body().unwrap().as_bytes().unwrap()).unwrap();

        let headers = self.generate_request_headers_v2(
            body,
            time,
            "POST",
            "/api/v2/user_transactions/btcusd/",
            "",
        );

        let res_headers = result.headers_mut();
        for (name, val) in headers {
            res_headers.insert(name.unwrap(), val);
        }

        let result = self.http_client.execute(result).await.unwrap();
        assert!(result.status().is_success());
        let result = result.text().await.unwrap();
        let initial: [InnerTransaction; 1] = serde_json::from_str(&result).unwrap();
        if initial[0].tr_type != "2" {
            panic!(
                "Got most recent transaction of non-trade type: {:?}",
                initial[0]
            );
        }
        initial[0].id
    }

    // Now, the tracked transactions ...

    pub async fn request_transactions_from(
        &self,
        from_id: usize,
        parent: Arc<BitstampHttp>,
    ) -> Vec<Transaction> {
        spawn_decrement_task(parent);
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();

        let mut result = self
            .http_client
            .post("https://www.bitstamp.net/api/v2/user_transactions/btcusd/")
            .form(&[("limit", "1")])
            .build()
            .unwrap();

        let body = std::str::from_utf8(result.body().unwrap().as_bytes().unwrap()).unwrap();

        let headers = self.generate_request_headers_v2(
            body,
            time,
            "POST",
            "/api/v2/user_transactions/btcusd/",
            "",
        );

        let res_headers = result.headers_mut();
        for (name, val) in headers {
            res_headers.insert(name.unwrap(), val);
        }

        let result = self.http_client.execute(result).await.unwrap();
        assert!(result.status().is_success());
        let result = result.text().await.unwrap();
        let transactions: Vec<InnerTransaction> = serde_json::from_str(&result).unwrap();
        transactions
            .into_iter()
            .filter(|t| t.tr_type == "2" && t.id != from_id)
            .map(|it| {
                let usd: f64 = it.usd.parse().unwrap();
                assert_ne!(usd, 0.0);
                Transaction {
                    id: it.id,
                    order_id: it.order_id,
                    usd: usd.abs(),
                    btc: it.btc.parse::<f64>().unwrap().abs(),
                    btc_usd: it.btc_usd,
                    fee: it.fee.parse().unwrap(),
                    side: if usd < 0.0 { Side::Buy } else { Side::Sell },
                }
            })
            .collect()
    }

    // TODO immediately send andasynchronously await the future
    pub async fn send_cancel(&self, id: usize, parent: Arc<BitstampHttp>) -> Option<OrderCanceled> {
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();

        let mut result = self
            .http_client
            .post("https://www.bitstamp.net/api/v2/cancel_order/")
            .form(&[("id", id)])
            .build()
            .unwrap();

        let body = std::str::from_utf8(result.body().unwrap().as_bytes().unwrap()).unwrap();

        let headers =
            self.generate_request_headers_v2(body, time, "POST", "/api/v2/cancel_order/", "");

        let res_headers = result.headers_mut();
        for (name, val) in headers {
            res_headers.insert(name.unwrap(), val);
        }

        let result = self.http_client.execute(result).await.unwrap();
        let result = result.text().await.unwrap();
        spawn_decrement_task(parent.clone());
        if result.contains("not found") {
            None
        } else {
            let order: InnerOrderCanceled =
                serde_json::from_str(&result).expect("Couldn't parse cancel response");
            Some(OrderCanceled {
                price: order.price,
                amount: order.amount,
                id: order.id,
                side: if order.in_type == 0 {
                    Side::Buy
                } else {
                    Side::Sell
                },
            })
        }
    }

    // TODO immediately send and asynchronously await the future
    pub async fn send_order(
        &self,
        amount: f64,
        price: f64,
        side: Side,
        parent: Arc<BitstampHttp>,
    ) -> OrderSent {
        let api = match side {
            Side::Buy => "/api/v2/buy/btcusd/",
            Side::Sell => "/api/v2/sell/btcusd/",
        };
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();

        let price = ((price * 100.0).round() + 0.01) / 100.0;
        assert_eq!(price, (price * 100.0) / 100.0);
        let mut result = self
            .http_client
            .post(&format!("https://www.bitstamp.net{}", api))
            .form(&[
                ("amount", format!("{:.6}", amount)),
                ("price", format!("{:.2}", price)),
            ])
            .build()
            .unwrap();

        let body = std::str::from_utf8(result.body().unwrap().as_bytes().unwrap()).unwrap();

        let headers = self.generate_request_headers_v2(body, time, "POST", api, "");

        let res_headers = result.headers_mut();
        for (name, val) in headers {
            res_headers.insert(name.unwrap(), val);
        }

        let result = self
            .http_client
            .execute(result)
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        spawn_decrement_task(parent.clone());
        let order = serde_json::from_str(&result);
        if let Ok(order) = order {
            let order: InnerOrderSent = order;
            OrderSent {
                price: order.price.parse().unwrap(),
                amount: order.amount.parse::<f64>().unwrap().abs(),
                id: order.id.parse().unwrap(),
                side: if order.in_type == "0" {
                    Side::Buy
                } else {
                    Side::Sell
                },
            }
        } else {
            panic!("Got bad order response: {}", result);
        }
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
