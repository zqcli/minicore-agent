//! The `minicore-agent` command line, callable from another binary that embeds
//! the Agent.

use std::path::PathBuf;

use tracing_subscriber::filter::{FilterExt, filter_fn};
use tracing_subscriber::layer::{Layer, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;

use crate::{Agent, AgentError, run_stdio};

#[derive(Debug)]
enum Command {
    Version,
    Stdio { config: PathBuf },
}

/// Runs the `minicore-agent` command line on `args` (without the program name)
/// and returns the process exit code.
pub fn run(args: impl IntoIterator<Item = String>) -> i32 {
    init_tracing();
    tracing::info!("agent startup");
    // The Agent serves one client and waits on the network or on child tools;
    // a worker (and its allocator arena) per core only holds memory.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("Tokio runtime initialization");
    let result = runtime.block_on(serve(args));
    // run_stdio has joined owned work. A parked Tokio stdin read cannot be
    // cancelled; do not wait for that blocking thread when dropping the runtime.
    runtime.shutdown_background();
    match result {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("minicore-agent: {error}");
            1
        }
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
    // An embedding binary may have installed its own subscriber first.
    let _ = tracing_subscriber::registry().with(layer).try_init();
}

async fn serve(args: impl IntoIterator<Item = String>) -> Result<(), AgentError> {
    match parse_command(args.into_iter())? {
        Command::Version => {
            println!("minicore-agent {}", crate::agent_version());
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

#[cfg(test)]
mod tests;
