#[derive(Copy, Clone)]
pub struct Ema {
    value: Option<f64>,
    ratio: f64,
}

impl Ema {
    pub fn new(ratio: f64) -> Ema {
        assert!(ratio > 0.0);
        assert!(ratio <= 1.0);
        Ema { value: None, ratio }
    }

    pub fn get_value(&self) -> Option<f64> {
        self.value
    }

    pub fn add_value(&mut self, new_value: f64) -> f64 {
        let new_result = match self.value {
            Some(value) => value * (1.0 - self.ratio) + self.ratio * new_value,
            None => new_value,
        };

        self.value = Some(new_result);

        new_result
    }
}
