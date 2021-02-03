use clap::Clap;
use clap::ValueHint;
use kvs::{ensure_log_file, KVLog, Result};
use std::fs::OpenOptions;
use std::io::BufWriter;
use std::path::PathBuf;
use std::process::exit;

#[derive(Clap)]
#[clap(author, about, version)]
pub struct Options {
    #[clap(subcommand)]
    subcmd: SubCommand,
    #[clap(short, long, parse(from_os_str), value_hint = ValueHint::DirPath, default_value = "kvstore_data/")]
    path: PathBuf,
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

fn main() -> Result<()> {
    let opt = Options::parse();
    match opt.subcmd {
        SubCommand::Set(cmd) => {
            // we only need to append
            let mut open_opts = OpenOptions::new();
            let log_file = ensure_log_file(&opt.path, open_opts.create(true).append(true))?;
            let writer = BufWriter::new(log_file);
            let kvlog = KVLog::new(cmd.key, cmd.value);
            kvlog.serialize_to_writer(writer)?;
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
    Ok(())
}
