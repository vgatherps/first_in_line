use crate::ema::Ema;

pub struct Displacement {
    premium_fast_ema: Ema,
    premium_slow_ema: Ema,
}

impl Displacement {
    pub fn new() -> Displacement {
        Displacement {
            premium_fast_ema: Ema::new(0.05),
            premium_slow_ema: Ema::new(0.0001),
        }
    }

    pub fn handle_new_fairs(&mut self, remote_fair: f64, local_fair: f64) {
        if remote_fair <= 100.0 || local_fair <= 100.0 {
            return;
        }
        let diff = local_fair - remote_fair;
        if diff.abs() > 100.0 {
            return;
        }
        self.premium_fast_ema.add_value(diff);
        self.premium_slow_ema.add_value(diff);
    }

    pub fn get_displacement(&self) -> (Option<f64>, Option<f64>) {
        (
            self.premium_fast_ema.get_value(),
            self.premium_slow_ema.get_value(),
        )
    }
}
