use structopt::StructOpt;

#[derive(Debug, StructOpt)]
pub struct Arguments {
    #[structopt(long, help = "Bitstamp authentication key")]
    pub auth_key: String,

    #[structopt(long, help = "Bitstamp authentication secret")]
    pub auth_secret: String,

    #[structopt(long, help = "HTML summary file output")]
    pub html: String,

    #[structopt(long, default_value = "0.03", help = "Fee per making trades in bps")]
    pub fee_bps: f64,

    #[structopt(long, default_value = "0.03", help = "Profit required per trade")]
    pub profit_bps: f64,
}
