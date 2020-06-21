use structopt::StructOpt;

#[derive(Debug, StructOpt)]
pub struct Arguments {
    #[structopt(long, help = "HTML summary file output", default_value = "index.html")]
    pub html: String,
}
