#![forbid(unsafe_code)]

use std::path::PathBuf;

use minicore_agent::{Agent, AgentError, run_stdio};
use tracing_subscriber::filter::{FilterExt, filter_fn};
use tracing_subscriber::layer::{Layer, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;

#[cfg(test)]
mod test_support;

#[cfg(all(test, unix))]
mod tests {
    use super::test_support::TempDir;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::process::{Child, Command, Stdio};
    use std::time::{Duration, Instant};

    struct Process(Child);
    impl Drop for Process {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    #[test]
    fn orderly_shutdown_exits_with_stdin_still_open() {
        let binary = std::env::current_exe()
            .unwrap()
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("minicore-agent");
        for mode in ["INT", "TERM", "rpc"] {
            let directory = TempDir::new("stdio-signals");
            let path = directory.path().join("agent.toml");
            let data_dir =
                toml::Value::String(directory.path().join("data").to_string_lossy().into_owned());
            std::fs::write(
                &path,
                format!(
                    r#"
data_dir = {data_dir}
default_profile = "test"
[profiles.test]
model = "main"
reasoning = "auto"
system_prompt = "test"
tools = []
approval = "auto"
[models.main]
provider = "open_ai_responses"
model = "test"
base_url = "https://example.invalid/v1"
api_key_env = "PATH"
physical_context_window = 32000
output_budget_tokens = 2000
safety_margin_tokens = 1000
supported_reasoning = ["auto"]
supports_tools = true
[compaction]
enabled = false
"#
                ),
            )
            .unwrap();
            let mut child = Process(
                Command::new(&binary)
                    .args(["--config", path.to_str().unwrap(), "--stdio"])
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::inherit())
                    .spawn()
                    .unwrap(),
            );
            writeln!(
                child.0.stdin.as_mut().unwrap(),
                "{{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"agent.ping\"}}"
            )
            .unwrap();
            let mut reader = BufReader::new(child.0.stdout.take().unwrap());
            let (ready, receive) = std::sync::mpsc::channel();
            let reading = std::thread::spawn(move || {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                ready.send((line, reader)).unwrap();
            });
            let (line, mut reader) = receive
                .recv_timeout(Duration::from_secs(5))
                .expect("server readiness");
            reading.join().unwrap();
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&line).unwrap()["id"],
                1
            );
            // Give stdin's next blocking read time to park before stopping.
            std::thread::sleep(Duration::from_millis(30));
            if mode == "rpc" {
                writeln!(
                    child.0.stdin.as_mut().unwrap(),
                    "{{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"agent.shutdown\"}}"
                )
                .unwrap();
            } else {
                assert!(
                    Command::new("kill")
                        .args([format!("-{mode}"), child.0.id().to_string()])
                        .status()
                        .unwrap()
                        .success()
                );
            }
            let deadline = Instant::now() + Duration::from_secs(5);
            let status = loop {
                if let Some(status) = child.0.try_wait().unwrap() {
                    break status;
                }
                assert!(Instant::now() < deadline, "{mode} waited for stdin EOF");
                std::thread::sleep(Duration::from_millis(5));
            };
            assert!(status.success(), "{mode}: {status}");
            assert!(child.0.stdin.is_some());
            let mut rest = String::new();
            reader.read_to_string(&mut rest).unwrap();
            if mode == "rpc" {
                assert_eq!(
                    serde_json::from_str::<serde_json::Value>(&rest).unwrap()["result"]["ok"],
                    true
                );
            }
        }
    }
}

#[derive(Debug)]
enum Command {
    Version,
    Stdio { config: PathBuf },
}

fn main() {
    init_tracing();
    tracing::info!("agent startup");
    let runtime = tokio::runtime::Runtime::new().expect("Tokio runtime initialization");
    let result = runtime.block_on(run());
    // run_stdio has joined owned work. A parked Tokio stdin read cannot be
    // cancelled; do not wait for that blocking thread when dropping the runtime.
    runtime.shutdown_background();
    if let Err(error) = result {
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
