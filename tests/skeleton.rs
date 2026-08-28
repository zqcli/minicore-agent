use minicore_agent::{Agent, AgentConfig, AgentError};
use minicore_runtime::SessionId;

#[test]
fn config_parses_and_rejects_out_of_bound_capacity() {
    let config = AgentConfig::from_toml(
        r#"
data_dir = "./.minicore-agent"
event_capacity = 8
"#,
    )
    .unwrap();
    assert_eq!(config.event_capacity, 8);
    assert!(matches!(
        AgentConfig::from_toml("data_dir = \".\"\nevent_capacity = 0"),
        Err(minicore_agent::ConfigError::InvalidEventCapacity)
    ));
}

#[tokio::test]
async fn agent_exposes_ping_and_a_single_event_stream() {
    let data_dir = std::env::temp_dir().join(format!(
        "minicore-agent-skeleton-{}",
        SessionId::new().unwrap()
    ));
    let config = AgentConfig::new(data_dir.clone()).unwrap();
    let mut agent = Agent::open(config).await.unwrap();
    assert_eq!(agent.ping().version, "0.1.0");

    let _events = agent.take_events().unwrap();
    assert!(matches!(
        agent.take_events(),
        Err(AgentError::EventStreamTaken)
    ));
    agent.shutdown().await.unwrap();
    tokio::fs::remove_dir_all(data_dir).await.unwrap();
}
