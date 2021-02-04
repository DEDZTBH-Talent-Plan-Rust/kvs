use clap::Clap;
use clap::ValueHint;
use kvs::{ErrorKind, KvStore, Result};
use std::path::PathBuf;
use std::process::exit;

#[derive(Clap)]
#[clap(author, about, version)]
#[allow(dead_code)]
pub struct Options {
    #[clap(subcommand)]
    subcmd: SubCommand,
    #[clap(short, long, parse(from_os_str), value_hint = ValueHint::DirPath, default_value = "kvs_data/")]
    path: PathBuf,
}

#[derive(Clap)]
#[allow(dead_code)]
enum SubCommand {
    #[clap(author, about = "Set the value of a string key to a string", version)]
    Set(SetCmd),
    #[clap(author, about = "Get the string value of a given string key", version)]
    Get(GetCmd),
    #[clap(author, about = "Remove a given key", version)]
    Rm(RmCmd),
}

#[derive(Clap)]
#[allow(dead_code)]
struct SetCmd {
    key: String,
    value: String,
}

#[derive(Clap)]
#[allow(dead_code)]
struct GetCmd {
    key: String,
}

#[derive(Clap)]
#[allow(dead_code)]
struct RmCmd {
    key: String,
}

fn main() -> Result<()> {
    let opt = Options::parse();
    let mut store = KvStore::open(opt.path)?;
    match opt.subcmd {
        SubCommand::Set(cmd) => {
            store.set(cmd.key, cmd.value)?;
        }
        SubCommand::Get(cmd) => {
            eprintln!("unimplemented");
            exit(255)
        }
        SubCommand::Rm(cmd) => match store.remove(cmd.key) {
            Ok(_) => {}
            Err(e) => {
                if e.kind() == ErrorKind::KeyNotFound {
                    eprintln!("{}", e);
                    exit(1);
                }
                return Result::Err(e);
            }
        },
    }
    Ok(())
}
