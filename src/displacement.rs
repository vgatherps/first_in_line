use crate::ema::Ema;
use horrorshow::html;

// What's the algorithm?
// Look at the displacement of a fast ema of fair price from a slower ema of fair
// Assume the local exchange should be following the same curve, and return
// the displacement the fast ema fair has from such a curve
pub struct Displacement {
    remote_fast_ema: Ema,
    remote_slow_ema: Ema,
    local_fast_ema: Ema,
    local_slow_ema: Ema,
    local_size: Ema,
    remote_size: f64,
    premium_ema: Ema,
}

impl Displacement {
    pub fn new() -> Displacement {
        Displacement {
            remote_fast_ema: Ema::new(0.025),
            remote_slow_ema: Ema::new(0.001),
            local_fast_ema: Ema::new(0.05),
            local_slow_ema: Ema::new(0.01),
            remote_size: 0.0,
            local_size: Ema::new(0.001),
            premium_ema: Ema::new(0.001),
        }
    }

    pub fn handle_local(&mut self, local_fair: f64, local_size: f64) {
        let lf = self.local_fast_ema.add_value(local_fair);
        self.local_slow_ema.add_value(local_fair);
        self.local_size.add_value(local_size);
        if let Some(rf) = self.remote_fast_ema.get_value() {
            self.premium_ema.add_value(lf - rf);
        }
    }

    pub fn handle_remote(&mut self, remote_fair: f64, remote_size: f64) {
        self.remote_fast_ema.add_value(remote_fair);
        self.remote_slow_ema.add_value(remote_fair);
        self.remote_size = remote_size;
    }

    pub fn get_displacement(&self) -> Option<(f64, f64)> {
        if let (Some(lf), Some(ls), Some(rf), Some(rs), Some(lsize), Some(premium)) = (
            self.local_fast_ema.get_value(),
            self.local_slow_ema.get_value(),
            self.remote_fast_ema.get_value(),
            self.remote_slow_ema.get_value(),
            self.local_size.get_value(),
            self.premium_ema.get_value(),
        ) {
            // how far above the slower moving price is the fast fair value?
            let remote_premium = rf - rs;
            let local_premium = lf - ls;


            // discount the remoteness by the size ratio
            let total_size = lsize + self.remote_size;
            let adjust_remote = self.remote_size / total_size;
            let adjust_local = lsize / total_size;

            let remote_premium = remote_premium * adjust_remote + local_premium * adjust_local;

            // How much farther must the remote premium go (or has it gone too far?)
            Some((remote_premium - local_premium, premium))
        } else {
            None
        }
    }

    pub fn get_html_info(&self) -> String {
        let (momentum_disp, prem) = self.get_displacement().unwrap_or((0.0, 0.0));
        let (estimated_current, imbalance) = if let (Some(remote), Some(local)) = (
            self.remote_fast_ema.get_value(),
            self.local_fast_ema.get_value(),
        ) {
            (local - remote, prem - (local - remote))
        } else {
            (0.0, 0.0)
        };
        format!(
            "{}",
            html! {
                h3(id="displacement header", class="title") : "Price displacement summary";
                ul(id="Displacements") {
                    li(first?=true, class="item") {
                        : format!("Remote fair emas: fast {:.2}, slow {:.2}",
                                  self.remote_fast_ema.get_value_zero(),
                                  self.remote_slow_ema.get_value_zero());
                    }
                    li(first?=false, class="item") {
                        : format!("Local fair emas: fast {:.2}, slow {:.2} size {:.2}",
                                  self.local_fast_ema.get_value_zero(),
                                  self.local_slow_ema.get_value_zero(),
                                  self.local_size.get_value_zero());
                    }
                    li(first?=false, class="item") {
                        : format!("Momentum displacement: remote: {:.2}, local: {:.2}, local-to-remote: {:.3}",
                                  self.remote_fast_ema.get_value_zero() - self.remote_slow_ema.get_value_zero(),
                                  self.local_fast_ema.get_value_zero() - self.local_slow_ema.get_value_zero(),
                                  momentum_disp);
                    }
                    li(first?=false, class="item") {
                        : format!("Fair premium: {:.2}, current: {:.3}, imbalance: {:.3}",
                                  prem, estimated_current, imbalance);
                    }
                }
            }
        )
    }
}
