use structopt::StructOpt;

#[derive(Debug, StructOpt)]
pub struct Arguments {
    #[structopt(long, help = "HTML summary file output")]
    pub html: String,
}
