use crate::ema::Ema;

// What's the algorithm?
// Look at the displacement of a fast ema of fair price from a slower ema of fair
// Assume the local exchange should be following the same curve, and return
// the displacement the fast ema fair has from such a curve
pub struct Displacement {
    remote_fast_ema: Ema,
    remote_slow_ema: Ema,
    local_fast_ema: Ema,
    local_slow_ema: Ema,
    premium_ema: Ema,
}

impl Displacement {
    pub fn new() -> Displacement {
        Displacement {
            remote_fast_ema: Ema::new(0.025),
            remote_slow_ema: Ema::new(0.005),
            local_fast_ema: Ema::new(0.05),
            local_slow_ema: Ema::new(0.01),
            premium_ema: Ema::new(0.0005),
        }
    }

    pub fn handle_local(&mut self, local_fair: f64) {
        let lf = self.local_fast_ema.add_value(local_fair);
        self.local_slow_ema.add_value(local_fair);
        if let Some(rf) = self.remote_fast_ema.get_value() {
            self.premium_ema.add_value(lf - rf);
        }
    }

    pub fn handle_remote(&mut self, remote_fair: f64) {
        self.remote_fast_ema.add_value(remote_fair);
        self.remote_slow_ema.add_value(remote_fair);
    }

    pub fn get_displacement(&self) -> Option<(f64, f64)> {
        if let (Some(lf), Some(ls), Some(rf), Some(rs), Some(premium)) = (
            self.local_fast_ema.get_value(),
            self.local_slow_ema.get_value(),
            self.remote_fast_ema.get_value(),
            self.remote_slow_ema.get_value(),
            self.premium_ema.get_value(),
        ) {
            // how far above the slower moving price is the fast fair value?
            let remote_premium = rf - rs;
            let local_premium = lf - ls;

            // How much farther must the remote premium go (or has it gone too far?)
            Some((remote_premium - local_premium, premium))
        } else {
            None
        }
    }
}
