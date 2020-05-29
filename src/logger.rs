use std::fs::File;
use std::io::Write;
use std::io::BufWriter;
use exchange::{
    bitmex_connection
};

mod exchange;
mod market_logger;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut rt = tokio::runtime::Builder::new()
        .basic_scheduler()
        .enable_all()
        .build()?;
    rt.block_on(run())
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut out = BufWriter::new(File::create("bitmex_data.out").expect("Couldn't open file"));
    let mut bitmex_data = bitmex_connection().await;
    bitmex_data.next().await;

    let mut rounds = 0;
    let mut start = std::time::SystemTime::now();
    loop {
        rounds += 1;
        let msg = bitmex_data.next().await;
        let encoded: Vec<u8> = bincode::serialize(&msg).unwrap();
        if rounds >= 500 {
            let end = std::time::SystemTime::now();
            if let Ok(diff) = end.duration_since(start) {
                let rounds = rounds as f64;
                let time = diff.as_millis() as f64 / 1000.0;
                println!("Last burst got {} messages / second", rounds / time);
            }
            start = std::time::SystemTime::now();
            rounds = 0;
        }
        out.write_all(&encoded).unwrap();
    }
}
