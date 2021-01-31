use clap::Clap;
use std::process::exit;

#[derive(Clap)]
#[clap(author, about, version)]
pub struct Options {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Clap)]
enum SubCommand {
    #[clap(author, about = "Set the value of a string key to a string", version)]
    Set(SetCmd),
    #[clap(author, about = "Get the string value of a given string key", version)]
    Get(GetCmd),
    #[clap(author, about = "Remove a given key", version)]
    Rm(RmCmd),
}

#[derive(Clap)]
struct SetCmd {
    key: String,
    value: String,
}

#[derive(Clap)]
struct GetCmd {
    key: String,
}

#[derive(Clap)]
struct RmCmd {
    key: String,
}

fn main() {
    let opt = Options::parse();
    match opt.subcmd {
        SubCommand::Set(cmd) => {
            eprintln!("unimplemented");
            exit(-1)
        }
        SubCommand::Get(cmd) => {
            eprintln!("unimplemented");
            exit(-1)
        }
        SubCommand::Rm(cmd) => {
            eprintln!("unimplemented");
            exit(-1)
        }
    }
}
