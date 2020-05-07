use hmac::{Hmac, Mac};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use sha2::Sha256;
use std::sync::atomic::{AtomicUsize, Ordering};
type HmacSha256 = Hmac<Sha256>;
type SmallString = smallstr::SmallString<[u8; 32]>;

use std::time::{SystemTime, UNIX_EPOCH};

use serde::Deserialize;

#[derive(Deserialize)]
struct Balance {
    btc_available: SmallString,
    btc_balance: SmallString,
    usd_available: SmallString,
    usd_balance: SmallString,
    fee: f64,
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

struct Transaction {
    id: usize,
    order_id: usize,
    usd: f64,
    btc: f64,
    btc_usd: f64,
    fee: f64,
}

pub struct BitstampHttp {
    http_client: reqwest::Client,
    auth_key: String,
    auth_secret: String,
    x_auth_sig: HeaderName,
    x_auth_nonce: HeaderName,
    x_auth_timestamp: HeaderName,
    request_counter: AtomicUsize,
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
        }
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
        let counter = self.request_counter.fetch_add(1, Ordering::Relaxed);
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();
        let nonce_str = format!("{}{}", counter, time);
        let mac_str = format!("{}jwhf0251{}", nonce_str, self.auth_key);
        let mut mac =
            HmacSha256::new_varkey(self.auth_secret.as_bytes()).expect("Mac works with any key");
        mac.input(mac_str.as_bytes());
        let result_bytes = mac.result().code();
        let hex_str = hex::encode_upper(result_bytes);
        (hex_str, nonce_str)
    }

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
        res.text().await.unwrap();
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

    pub async fn request_transactions_from(&self, from_id: usize) -> Vec<Transaction> {
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
            .map(|it| Transaction {
                id: it.id,
                order_id: it.order_id,
                usd: it.usd.parse().unwrap(),
                btc: it.btc.parse().unwrap(),
                btc_usd: it.btc_usd,
                fee: it.fee.parse().unwrap(),
            })
            .collect()
    }
}
