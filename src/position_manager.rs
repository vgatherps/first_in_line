use crate::bitstamp_http::BitstampHttp;

#[derive(Debug)]
pub struct PositionManager {
    pub coins_balance: f64,
    pub dollars_balance: f64,

    pub coins_available: f64,
    pub dollars_available: f64,

    fee: f64,
}
fn compare_ge(a: f64, b: f64) -> bool {
    (a + 0.00001) >= b
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
            fee: fee / 100.0,
        }
    }

    fn validate(&self) {
        if self.coins_balance >= 0.0
            && self.coins_available >= 0.0
            && self.dollars_balance >= 0.0
            && self.dollars_available >= 0.0
            && compare_ge(self.coins_balance, self.coins_available)
            && compare_ge(self.dollars_balance, self.dollars_available)
        {

        } else {
            panic!("Bad internal state {:?}", self);
        }
    }

    pub fn buy_coins(&mut self, coins: f64, price: f64) {
        let dollars = coins * price;
        // Here, I do my own fee management to keep things
        let dollars = dollars * (1.0 + self.fee); // when buying, fee is added into dollar charge
        println!(
            "Subtracting {} dollars to the account fee {}, db {}, da {}",
            dollars,
            dollars * self.fee,
            self.dollars_balance,
            self.dollars_available
        );
        self.coins_balance += coins;
        self.coins_available += coins;
        self.dollars_balance -= dollars;
        self.validate();
    }

    pub fn sell_coins(&mut self, coins: f64, price: f64) {
        let dollars = coins * price;
        let dollars = dollars * (1.0 - self.fee); // when selling, fee is removed from incoming dollars
        self.coins_balance -= coins;

        println!(
            "Adding {} dollars to the account fee {}, db {}, da {}",
            dollars,
            dollars * self.fee,
            self.dollars_balance,
            self.dollars_available
        );
        self.dollars_available += dollars;
        self.dollars_balance += dollars;
        self.validate();
    }

    pub fn get_fee_estimate(&self) -> f64 {
        self.fee
    }

    pub fn return_buy_balance(&mut self, dollars: f64) {
        println!(
            "Returning {} dollars to the account fee {}, db {}, da {}",
            dollars,
            self.fee * dollars,
            self.dollars_balance,
            self.dollars_available
        );
        self.dollars_available += dollars * (1.0 + self.fee);
        self.validate();
    }

    pub fn request_buy_balance(&mut self, dollars: f64) -> bool {
        if dollars * (1.0 + 2.0 * self.fee) < self.dollars_available {
            println!(
                "Requesting {} dollars to the account fee {}, db {}, da {}",
                dollars,
                self.fee * dollars,
                self.dollars_balance,
                self.dollars_available
            );
            self.dollars_available -= dollars * (1.0 + self.fee);
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
        if coins < self.coins_available {
            self.coins_available -= coins;
            self.validate();
            true
        } else {
            false
        }
    }

    pub fn get_total_position(&self, coins_price: f64) -> f64 {
        self.validate();
        let coins_usd = self.coins_balance * coins_price;
        coins_usd + self.dollars_balance
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
