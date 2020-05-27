use std::fs::File;
use std::io::{Read, BufRead};
use std::io::BufReader;
use std::path::Path;
use exchange::{
    bitmex_connection,
    normalized
};

mod exchange;

fn main() {
    println!("Got here0");
    let mut out = BufReader::new(File::open("bitmex_data.out").expect("Couldn't open file"));

    println!("Got here");
    while out.fill_buf().expect("Bad file").len() > 0 {
        let data: normalized::MarketEventBlock = bincode::deserialize_from(&mut out).expect("Got actual market data");
        println!("Got {:?}", data);
    }
}
