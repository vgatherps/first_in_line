#[derive(Copy, Clone)]
pub struct Ema {
    value: Option<f64>,
    ratio: f64,
    cur_ratio: f64,
}

impl Ema {
    pub fn new(ratio: f64) -> Ema {
        assert!(ratio > 0.0);
        assert!(ratio <= 1.0);
        Ema {
            value: None,
            ratio,
            cur_ratio: 0.5,
        }
    }

    pub fn get_value(&self) -> Option<f64> {
        self.value
    }

    pub fn add_value(&mut self, new_value: f64) -> f64 {
        let new_result = match self.value {
            Some(value) => {
                let ratio = self.cur_ratio;
                self.cur_ratio = 0.95 * self.cur_ratio + 0.05 * self.ratio;
                value * (1.0 - ratio) + ratio * new_value
            }
            None => new_value,
        };

        self.value = Some(new_result);

        new_result
    }
}
