use clap::Clap;
use std::process::exit;

#[derive(Clap)]
#[clap(version = "0.1.0", author = "DEDZTBH <peiqial@outlook.com>")]
pub struct Opt {
    #[clap(subcommand)]
    subcmd: SubCommand
}

#[derive(Clap)]
enum SubCommand {
    #[clap(version = "0.1.0", author = "DEDZTBH <peiqial@outlook.com>")]
    Set(SetCmd),
    #[clap(version = "0.1.0", author = "DEDZTBH <peiqial@outlook.com>")]
    Get(GetCmd),
    #[clap(version = "0.1.0", author = "DEDZTBH <peiqial@outlook.com>")]
    Rm(RmCmd),
}

#[derive(Clap)]
struct SetCmd {
    key: String,
    value: String,
}

#[derive(Clap)]
struct GetCmd {
    key: String
}

#[derive(Clap)]
struct RmCmd {
    key: String
}

fn main() {
    let opt = Opt::parse();
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