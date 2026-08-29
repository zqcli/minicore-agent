use minicore_agent::{AgentConfig, ConfigError};

fn complete_config(event_capacity: usize) -> String {
    format!(
        r#"
data_dir = "./.minicore-agent"
event_capacity = {event_capacity}
default_profile = "test"

[profiles.test]
model = "main"
reasoning = "auto"
system_prompt = "public config test"
tools = []
max_tool_rounds = 4
approval = "ask"

[models.main]
provider = "open_ai_responses"
model = "provider-model"
base_url = "https://example.invalid/v1"
api_key_env = "MINICORE_SKELETON_TEST_KEY"
physical_context_window = 10000
output_budget_tokens = 1000
safety_margin_tokens = 1000
supported_reasoning = ["auto"]
supports_tools = false
request_timeout_seconds = 30
"#
    )
}

#[test]
fn config_parses_and_rejects_out_of_bound_capacity() {
    let config = AgentConfig::from_toml(&complete_config(8)).unwrap();
    assert_eq!(config.event_capacity, 8);
    assert!(matches!(
        AgentConfig::from_toml(&complete_config(0)),
        Err(ConfigError::InvalidEventCapacity)
    ));
}

#[test]
fn complete_public_config_exposes_default_profile_and_model() {
    let config = AgentConfig::from_toml(&complete_config(256)).unwrap();

    assert_eq!(config.default_profile, "test");
    assert!(config.profiles.contains_key("test"));
    assert!(config.models.contains_key("main"));
}
