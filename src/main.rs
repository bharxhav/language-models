use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "lm", version, about = "Language models from scratch")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    Read,
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Read) => {
            println!("TBA");
        }
        None => {
            println!("Run `lm --help` for usage");
        }
    }
}
