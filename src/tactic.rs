pub struct Tactic {
    update_count: usize,
}

impl Tactic {
    pub fn new() -> Tactic {
        Tactic { update_count: 1 }
    }
    pub fn handle_book_update(
        &mut self,
        bbo: ((usize, f64), (usize, f64)),
        fair: f64,
        adjust: f64,
    ) {
        let ((bid, bid_sz), (ask, ask_sz)) = bbo;
        self.update_count += 1;
        if self.update_count > 500 && adjust.abs() > 0.5 {
            self.update_count = 0;
            println!(
                "({:.2}, {:.2})x({:.2}, {:.2}), fair {:.4}, adjust: {:.2}, adjusted_fair: {:.3}",
                bid as f64 * 0.01,
                bid_sz,
                ask as f64 * 0.01,
                ask_sz,
                fair,
                adjust,
                fair + adjust
            );
        }
    }
}
