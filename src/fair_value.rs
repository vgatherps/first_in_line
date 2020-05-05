use crate::order_book::{BuyPrice, SellPrice};

#[derive(Default)]
pub struct FairValueResult {
    pub fair_price: f64,
    pub fair_shares: f64,
}

fn cents_to_dollars(cents: usize) -> f64 {
    (cents as f64) * 0.01
}

#[derive(Copy, Clone)]
pub struct FairValue {
    denom: f64,
    offset: f64,
    dollars_out: f64,
    levels_out: usize,
}

/**
 * Fair price algorithm with depth. In short,
 * * Calculate scored price (price * shares * depth_score), as well as overall score for side
 * * use resulting values to calculate weighted midpoint
 */
impl FairValue {
    pub fn new(denom: f64, offset: f64, dollars_out: f64, levels_out: usize) -> FairValue {
        assert!(denom > 0.0);
        assert!(offset >= 0.0);
        assert!(dollars_out > 0.0);
        assert!(levels_out > 0);
        FairValue {
            denom,
            offset,
            dollars_out,
            levels_out,
        }
    }

    fn score(&self, distance: f64) -> f64 {
        self.offset + 1.0 / (1.0 + self.denom * distance * distance)
    }

    fn score_distanced<'a, I>(&self, prices: I) -> (f64, f64)
    where
        I: Iterator<Item = (f64, f64, f64)>,
    {
        prices
            .take(self.levels_out)
            .take_while(|(_, distance, _)| *distance <= self.dollars_out)
            .map(|(prc, distance, sz)| (prc, self.score(distance), sz))
            .fold((0.0, 0.0), |(sum_prc, sum_shares), (prc, score, shares)| {
                let shares_score = score * shares as f64;
                (sum_prc + prc * shares_score, sum_shares + shares_score)
            })
    }

    pub fn fair_value<'a, IB, IS>(&self, bids: IB, asks: IS, bbo: (usize, usize)) -> FairValueResult
    where
        IB: Iterator<Item = (&'a BuyPrice, &'a f64)>,
        IS: Iterator<Item = (&'a SellPrice, &'a f64)>,
    {
        let (best_bid, best_ask) = bbo;
        let best_bid = cents_to_dollars(best_bid);
        let best_ask = cents_to_dollars(best_ask);
        let bids = bids
            .map(|(prc, sz)| (cents_to_dollars(prc.unsigned()), *sz))
            .map(|(prc, sz)| (prc, best_bid - prc, sz));
        let asks = asks
            .map(|(prc, sz)| (cents_to_dollars(prc.unsigned()), *sz))
            .map(|(prc, sz)| (prc, prc - best_ask, sz));

        let (bid_price, bid_shares) = self.score_distanced(bids);
        let (ask_price, ask_shares) = self.score_distanced(asks);

        assert!(bid_shares > 0.0);
        assert!(ask_shares > 0.0);

        let bid_price = bid_price / bid_shares;
        let ask_price = ask_price / ask_shares;

        FairValueResult {
            fair_price: (bid_price * ask_shares + ask_price * bid_shares)
                / (ask_shares + bid_shares),
            fair_shares: ask_shares + bid_shares,
        }
    }
}
