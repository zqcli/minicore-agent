use std::future::Future;
use std::sync::Arc;

use minicore_agent::{
    Agent, AgentConfig, AgentError, ApprovalMode, ConfigError, ModelInfo, ProfileInfo, TurnRef,
};
use minicore_runtime::model::{ModelRef, ReasoningPreference};

use minicore_agent::TurnResult;

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

#[test]
fn public_agent_query_and_wait_contract_compiles() {
    let _: fn(&Agent) -> Vec<ProfileInfo> = Agent::list_profiles;
    let _: fn(&Agent) -> Vec<ModelInfo> = Agent::list_models;
    fn wait(
        agent: &Agent,
        turn: TurnRef,
    ) -> impl Future<Output = Result<Arc<TurnResult>, AgentError>> + '_ {
        agent.wait_turn(turn)
    }
    let _ = wait;

    let profile = ProfileInfo {
        id: "coding".to_owned(),
        model: "deep".to_owned(),
        reasoning: ReasoningPreference::High,
        tools: vec!["read".to_owned()],
        approval: ApprovalMode::Auto,
    };
    assert_eq!(profile.id, "coding");
    assert_eq!(profile.model, "deep");
    assert_eq!(profile.reasoning, ReasoningPreference::High);
    assert_eq!(profile.tools, ["read"]);
    assert_eq!(profile.approval, ApprovalMode::Auto);

    let model = ModelInfo {
        id: "deep".to_owned(),
        model_ref: "deep".parse::<ModelRef>().unwrap(),
        context_window: 128_000,
        supports_tools: true,
        supported_reasoning: vec![ReasoningPreference::High],
    };
    assert_eq!(model.id, "deep");
    assert_eq!(model.model_ref.to_string(), "deep");
    assert_eq!(model.context_window, 128_000);
    assert!(model.supports_tools);
    assert_eq!(model.supported_reasoning, [ReasoningPreference::High]);
}
