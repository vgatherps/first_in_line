use crate::bitmex_http::BitmexHttp;

#[derive(Debug)]
pub struct PositionManager {
    pub total_contracts: isize,
    pub buys_outstanding: isize,
    pub sells_outstanding: isize,

    fee: f64,
}

impl PositionManager {
    pub async fn create(http: std::sync::Arc<BitmexHttp>) -> PositionManager {
        http.cancel_all(http.clone()).await;
        let initial_xbt_balance = http.request_positions(http.clone()).await;
        PositionManager {
            total_contracts: initial_xbt_balance,
            buys_outstanding: 0,
            sells_outstanding: 0,
            fee: -0.025 * 0.01,
        }
    }

    pub fn buy(&mut self, xbt: usize) {
        let xbt = xbt as isize;
        self.buys_outstanding -= xbt;
        assert!(self.buys_outstanding >= 0);
        self.total_contracts += xbt;
    }

    pub fn sell(&mut self, xbt: usize) {
        let xbt = xbt as isize;
        self.sells_outstanding -= xbt;
        println!("Got sell for {} xbt", xbt);
        assert!(self.sells_outstanding >= 0);
        self.total_contracts -= xbt;
    }

    pub fn get_fee_estimate(&self) -> f64 {
        self.fee
    }

    pub fn return_buy_balance(&mut self, xbt: usize) {
        let xbt = xbt as isize;
        self.buys_outstanding -= xbt;
    }

    pub fn request_buy_balance(&mut self, xbt: usize) -> bool {
        let xbt = xbt as isize;
        if (self.buys_outstanding + xbt) < 5000 {
            self.buys_outstanding += xbt;
            true
        } else {
            false
        }
    }

    pub fn return_sell_balance(&mut self, xbt: usize) {
        let xbt = xbt as isize;
        self.sells_outstanding -= xbt;
    }

    pub fn request_sell_balance(&mut self, xbt: usize) -> bool {
        let xbt = xbt as isize;
        if (self.sells_outstanding + xbt) < 5000 {
            self.sells_outstanding += xbt;
            true
        } else {
            false
        }
    }

    // we want to be around zero, so if we're long, we push the fair down,
    // and for sell, push it up
    pub fn get_position_imbalance(&self) -> isize {
        self.total_contracts * -1
    }
}
