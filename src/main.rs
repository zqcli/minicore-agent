#![forbid(unsafe_code)]

use std::path::PathBuf;

use minicore_agent::{Agent, AgentConfig, AgentError, run_stdio};

#[derive(Debug)]
enum Command {
    Version,
    Stdio { config: PathBuf },
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("minicore-agent: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), AgentError> {
    match parse_command(std::env::args().skip(1))? {
        Command::Version => {
            println!("minicore-agent {}", minicore_agent::agent_version());
            Ok(())
        }
        Command::Stdio { config } => {
            let agent = Agent::open(AgentConfig::load(config).map_err(AgentError::Config)?).await?;
            run_stdio(agent).await
        }
    }
}

fn parse_command(mut args: impl Iterator<Item = String>) -> Result<Command, AgentError> {
    let Some(first) = args.next() else {
        return Err(AgentError::InvalidArguments);
    };
    if first == "--version" {
        return if args.next().is_none() {
            Ok(Command::Version)
        } else {
            Err(AgentError::InvalidArguments)
        };
    }
    if first != "--config" {
        return Err(AgentError::InvalidArguments);
    }
    let Some(config) = args.next() else {
        return Err(AgentError::InvalidArguments);
    };
    if args.next().as_deref() != Some("--stdio") || args.next().is_some() {
        return Err(AgentError::InvalidArguments);
    }
    Ok(Command::Stdio {
        config: PathBuf::from(config),
    })
}
