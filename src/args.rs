use structopt::StructOpt;

#[derive(Debug, StructOpt)]
pub struct Arguments {
    #[structopt(long, help = "Bitstamp authentication key")]
    pub auth_key: String,

    #[structopt(long, help = "Bitstamp authentication secret")]
    pub auth_secret: String,

    #[structopt(long, help = "HTML summary file output")]
    pub html: String,

    #[structopt(long, default_value = "-2.5", help = "Fee per making trades in bps")]
    pub fee_bps: f64,

    #[structopt(long, default_value = "1", help = "Profit required per trade")]
    pub profit_bps: f64,

    #[structopt(long, default_value = "1", help = "Profit required per trade when cancelling")]
    pub profit_bps_cancel: f64,

    #[structopt(long, default_value = "400", help = "Baseline trade size sans randomization")]
    pub base_trade_contracts: usize,


    #[structopt(
        long,
        default_value = "0.0015",
        help = "Pricing cost of holding a position"
    )]
    pub cost_of_position: f64,
}
