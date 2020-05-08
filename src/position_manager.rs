use crate::bitstamp_http::BitstampHttp;

#[derive(Clone)]
pub struct PositionManager {
    pub coins_balance: f64,
    pub dollars_balance: f64,

    pub coins_available: f64,
    pub dollars_available: f64,

    fee: f64,
}

impl PositionManager {
    pub async fn create(http: &BitstampHttp) -> PositionManager {
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

    fn validate(&self) {
        assert!(self.coins_balance >= 0.0);
        assert!(self.coins_available >= 0.0);
        assert!(self.dollars_balance >= 0.0);
        assert!(self.dollars_available >= 0.0);
        assert!(self.coins_balance >= self.coins_available);
        assert!(self.dollars_balance >= self.dollars_available);
    }

    pub fn buy_coins(&mut self, coins: f64, price: f64, fee: f64) {
        let dollars = coins * price;
        let dollars = dollars + fee; // when buying, fee is added into dollar charge
        self.coins_balance += coins;
        self.coins_available += coins;
        self.dollars_balance -= dollars;
        self.validate();
    }

    pub fn sell_coins(&mut self, coins: f64, price: f64, fee: f64) {
        let dollars = coins * price;
        let dollars = dollars - fee; // when selling, fee is removed from incoming dollars
        self.coins_balance -= coins;
        self.dollars_available += dollars;
        self.dollars_balance += dollars;
        self.validate();
    }

    pub fn set_fee_estimate(&mut self, fee: f64) {
        self.fee = fee
    }

    pub fn get_fee_estimate(&self) -> f64 {
        self.fee
    }

    pub fn return_buy_balance(&mut self, dollars: f64) {
        self.dollars_available += dollars;
    }

    pub fn request_buy_balance(&mut self, dollars: f64) -> bool {
        if dollars > self.dollars_available * (1.0 + 2.0 * self.fee) {
            self.dollars_available -= dollars;
            self.validate();
            true
        } else {
            false
        }
    }

    pub fn return_sell_balance(&mut self, coins: f64) {
        self.coins_available += coins;
        self.validate();
    }

    pub fn request_sell_balance(&mut self, coins: f64) -> bool {
        if coins > self.coins_available * (1.0 + 2.0 * self.fee) {
            self.coins_available -= coins;
            self.validate();
            true
        } else {
            false
        }
    }

    pub fn get_desired_position(&self, coins_price: f64) -> (f64, f64) {
        self.validate();
        let coins_usd = self.coins_balance * coins_price;
        let total_capital = coins_usd + self.dollars_balance;
        let desired_dollars = total_capital * 0.5;
        (desired_dollars, desired_dollars / coins_price)
    }

    // we want to be 50/50 split
    pub fn get_position_imbalance(&self, coins_price: f64) -> f64 {
        self.validate();
        let coins_usd = self.coins_balance * coins_price;
        let total_capital = coins_usd + self.dollars_balance;
        let desired_dollars = total_capital * 0.5;
        self.dollars_balance - desired_dollars
    }
}
