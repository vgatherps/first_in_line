#[macro_export]
macro_rules! check_error {
    ($err:expr, $pat:pat => $ensure:expr) => {{
        let result: Result<_, _> = ($err);
        match (result) {
            Ok(_) => panic!("Got ok result"),
            Err($pat) => $ensure,
            Err(val) => panic!("Got unexpected error {:?}", val),
        }
    }};
}
