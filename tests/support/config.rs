use std::path::{Path, PathBuf};

use serde_json::{Value, json};

pub fn provider_config(base_url: &str, key_env: &str, tools: &[&str]) -> Value {
    json!({
        "event_capacity": 256,
        "default_profile": "test",
        "profiles": {"test": {
            "model": "main",
            "reasoning": "auto",
            "system_prompt": "Use configured tools and finish the task.",
            "tools": tools,
            "max_tool_rounds": 4,
            "approval": "auto"
        }},
        "models": {"main": {
            "provider": "open_ai_responses",
            "model": "provider-model",
            "base_url": base_url,
            "api_key_env": key_env,
            "physical_context_window": 16000,
            "output_budget_tokens": 1024,
            "safety_margin_tokens": 1000,
            "supported_reasoning": ["auto", "disabled", "low", "medium", "high"],
            "supports_tools": true,
            "request_timeout_seconds": 5
        }}
    })
}

pub fn data_config(base_url: &str, key_env: &str, prompt: &str, tools: &[&str]) -> Value {
    let mut config = provider_config(base_url, key_env, tools);
    config["profiles"]["test"]["model"] = json!("test");
    config["profiles"]["test"]["system_prompt"] = json!(prompt);
    let mut model = config["models"]["main"].take();
    model["model"] = json!("fixture");
    model["physical_context_window"] = json!(100000);
    model["output_budget_tokens"] = json!(1000);
    model["supported_reasoning"] = json!(["auto"]);
    config["models"] = json!({"test": model});
    config
}

pub fn write_config(base: &Path, mut config: Value) -> PathBuf {
    let path = base.join("agent.toml");
    config["data_dir"] = json!(base.join("data"));
    write_config_at(&path, config);
    path
}

pub fn write_config_at(path: &Path, config: Value) {
    let config = toml::Value::try_from(config).unwrap();
    std::fs::write(path, toml::to_string(&config).unwrap()).unwrap();
}
