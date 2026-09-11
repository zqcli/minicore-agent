#![forbid(unsafe_code)]

use std::path::PathBuf;

use minicore_agent::{Agent, AgentError, run_stdio};
use tracing_subscriber::filter::{FilterExt, filter_fn};
use tracing_subscriber::layer::{Layer, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;

#[derive(Debug)]
enum Command {
    Version,
    Stdio { config: PathBuf },
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    init_tracing();
    tracing::info!("agent startup");
    if let Err(error) = run().await {
        eprintln!("minicore-agent: {error}");
        std::process::exit(1);
    }
}

fn init_tracing() {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("minicore_agent=info"));
    let target_filter = filter_fn(|metadata| {
        metadata.target() == "minicore_agent" || metadata.target().starts_with("minicore_agent::")
    });
    let filter = target_filter.and(env_filter);
    let layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_target(false)
        .with_ansi(false)
        .compact()
        .with_filter(filter);
    tracing_subscriber::registry().with(layer).init();
}

async fn run() -> Result<(), AgentError> {
    match parse_command(std::env::args().skip(1))? {
        Command::Version => {
            println!("minicore-agent {}", minicore_agent::agent_version());
            Ok(())
        }
        Command::Stdio { config } => {
            let agent = Agent::open_file(config).await?;
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
