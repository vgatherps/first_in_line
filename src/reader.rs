#![allow(warnings)]
use exchange::normalized;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;

mod exchange;

fn main() {
    let mut out = BufReader::new(File::open("bitmex_data.out").expect("Couldn't open file"));

    while out.fill_buf().expect("Bad file").len() > 0 {
        let data: normalized::MarketEventBlock =
            bincode::deserialize_from(&mut out).expect("Got actual market data");
        println!("{:?}", data);
    }
}
