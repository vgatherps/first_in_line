mod bitmex;
mod bitstamp;
mod coinbase;
mod okex;

pub mod normalized;

pub use bitmex::bitmex_connection;
pub use bitstamp::bitstamp_connection;
pub use coinbase::coinbase_connection;
pub use okex::{okex_connection, OkexType};
