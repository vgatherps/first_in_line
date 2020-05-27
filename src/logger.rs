use std::fs::File;
use std::io::Write;
use std::io::BufWriter;
use std::path::Path;
use exchange::{
    bitmex_connection
};

mod exchange;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut rt = tokio::runtime::Builder::new()
        .basic_scheduler()
        .enable_all()
        .build()?;
    rt.block_on(run())
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut bitmex_data = bitmex_connection().await;
    let mut out = BufWriter::new(File::create("bitmex_data.out").expect("Couldn't open file"));

    loop {
        let msg = bitmex_data.next().await;
        let encoded: Vec<u8> = bincode::serialize(&msg).unwrap();
        out.write_all(&encoded).unwrap();
    }
    
    Ok(())
}
