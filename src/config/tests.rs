use minicore_runtime::model::ReasoningPreference; // used by ModelConfig below

use super::*;

use crate::models::ModelConfig;
use crate::profiles::ApprovalMode;

fn disabled_compaction() -> CompactionConfig {
    CompactionConfig {
        enabled: false,
        ..CompactionConfig::default()
    }
}

fn model_config(
    supported_reasoning: BTreeSet<ReasoningPreference>,
    supports_tools: bool,
) -> ModelConfig {
    ModelConfig::OpenAiResponses {
        model: "provider-model".to_owned(),
        base_url: "https://example.invalid/v1".to_owned(),
        api_key_env: "MINICORE_CONFIG_TEST_KEY".to_owned(),
        physical_context_window: 10_000,
        output_budget_tokens: 1_000,
        safety_margin_tokens: 1_000,
        supported_reasoning,
        supports_tools,
        request_timeout_seconds: Some(30),
    }
}

fn profile() -> Profile {
    Profile {
        model: "main".to_owned(),
        reasoning: ReasoningPreference::Auto,
        system_prompt: "test system prompt".to_owned(),
        tools: Vec::new(),
        max_tool_rounds: 4,
        approval: ApprovalMode::Ask,
    }
}

fn valid_config() -> AgentConfig {
    AgentConfig {
        data_dir: PathBuf::from("test-data"),
        event_capacity: 128,
        default_profile: "test".to_owned(),
        profiles: BTreeMap::from([("test".to_owned(), profile())]),
        models: BTreeMap::from([(
            "main".to_owned(),
            model_config(BTreeSet::from([ReasoningPreference::Auto]), true),
        )]),
        loop_options: LoopOverrides::default(),
        compaction: disabled_compaction(),
    }
}

fn config_with_prompt_spec(spec: &str) -> String {
    format!(
        r#"
data_dir = "/tmp/minicore-agent-config-test-data"
event_capacity = 128
default_profile = "test"

[profiles.test]
model = "main"
reasoning = "auto"
system_prompt = {spec}
tools = []
max_tool_rounds = 4
approval = "ask"

[models.main]
provider = "open_ai_responses"
model = "provider-model"
base_url = "https://example.invalid/v1"
api_key_env = "MINICORE_CONFIG_TEST_KEY"
physical_context_window = 10000
output_budget_tokens = 1000
safety_margin_tokens = 1000
supported_reasoning = ["auto"]
supports_tools = true
request_timeout_seconds = 30
"#,
        spec = spec
    )
}

fn toml_path(path: &Path) -> String {
    toml::Value::String(path.to_string_lossy().into_owned()).to_string()
}

#[test]
fn empty_default_profile_is_rejected() {
    let mut config = valid_config();
    config.default_profile.clear();

    assert!(matches!(
        config.validate(),
        Err(ConfigError::MissingDefaultProfile)
    ));
}

#[test]
fn empty_profiles_are_rejected() {
    let mut config = valid_config();
    config.profiles.clear();

    assert!(matches!(
        config.validate(),
        Err(ConfigError::MissingProfiles)
    ));
}

#[test]
fn empty_models_are_rejected() {
    let mut config = valid_config();
    config.models.clear();

    assert!(matches!(config.validate(), Err(ConfigError::MissingModels)));
}

#[test]
fn default_profile_id_must_exist() {
    let mut config = valid_config();
    config.default_profile = "missing".to_owned();

    assert!(matches!(
        config.validate(),
        Err(ConfigError::InvalidDefaultProfile)
    ));
}

#[test]
fn profile_model_must_exist() {
    let mut config = valid_config();
    config.profiles.get_mut("test").unwrap().model = "missing".to_owned();

    assert!(matches!(
        config.validate(),
        Err(ConfigError::ProfileModelNotFound)
    ));
}

#[test]
fn unsupported_reasoning_is_rejected() {
    let mut config = valid_config();
    config.profiles.get_mut("test").unwrap().reasoning = ReasoningPreference::High;

    assert!(matches!(
        config.validate(),
        Err(ConfigError::UnsupportedReasoning)
    ));
}

#[test]
fn tools_require_model_tool_support() {
    let mut config = valid_config();
    config.profiles.get_mut("test").unwrap().tools = vec!["read".to_owned()];
    config.models.insert(
        "main".to_owned(),
        model_config(BTreeSet::from([ReasoningPreference::Auto]), false),
    );

    assert!(matches!(
        config.validate(),
        Err(ConfigError::ToolsNotSupported)
    ));
}

#[test]
fn valid_default_profile_is_accepted() {
    assert!(valid_config().validate().is_ok());
}

#[test]
fn removed_subagent_tool_is_rejected_from_profile_configuration() {
    let mut config = valid_config();
    config.profiles.get_mut("test").unwrap().tools = vec!["subagent".to_owned()];
    assert_eq!(config.validate(), Err(ConfigError::InvalidProfile));

    let text =
        config_with_prompt_spec(r#""system""#).replace("tools = []", "tools = [\"subagent\"]");
    assert!(matches!(
        AgentConfig::from_toml(&text),
        Err(ConfigError::InvalidProfile)
    ));
}

#[test]
fn compaction_policy_bounds_are_enforced() {
    for (target, trigger, valid) in [
        (50u8, 80u8, true),
        (0, 80, false),
        (80, 80, false),
        (81, 80, false),
        (50, 0, false),
        (50, 101, false),
    ] {
        let mut config = valid_config();
        config.compaction = CompactionConfig {
            enabled: true,
            trigger_percent: trigger,
            target_percent: target,
        };
        if valid {
            assert!(config.validate().is_ok());
        } else {
            assert_eq!(config.validate(), Err(ConfigError::InvalidCompaction));
        }
    }
}

#[test]
fn compaction_defaults_are_enabled_at_ninety_five_fifty() {
    let config = AgentConfig::from_toml(&config_with_prompt_spec(r#""system""#)).unwrap();
    assert!(config.compaction.enabled);
    assert_eq!(config.compaction.trigger_percent, 95);
    assert_eq!(config.compaction.target_percent, 50);
}

#[test]
fn explicit_compaction_table_overrides_defaults() {
    let text = format!(
        "{}\n[compaction]\nenabled = false\ntrigger_percent = 90\ntarget_percent = 60\n",
        config_with_prompt_spec(r#""system""#)
    );
    let config = AgentConfig::from_toml(&text).unwrap();
    assert!(!config.compaction.enabled);
    assert_eq!(config.compaction.trigger_percent, 90);
    assert_eq!(config.compaction.target_percent, 60);
}

#[test]
fn loop_overrides_are_validated() {
    let mut config = valid_config();
    config.loop_options.event_capacity = Some(0);
    assert_eq!(config.validate(), Err(ConfigError::InvalidLoopOptions));

    let mut config = valid_config();
    config.loop_options.max_pending_steers = Some(0);
    assert_eq!(config.validate(), Err(ConfigError::InvalidLoopOptions));

    let mut config = valid_config();
    config.loop_options.model_timeout_seconds = Some(0);
    assert_eq!(config.validate(), Err(ConfigError::InvalidLoopOptions));

    let mut config = valid_config();
    config.loop_options.model_retry_attempts = Some(5);
    assert_eq!(config.validate(), Err(ConfigError::InvalidLoopOptions));

    let mut config = valid_config();
    config.loop_options.model_retry_base_delay_millis = Some(0);
    assert_eq!(config.validate(), Err(ConfigError::InvalidLoopOptions));
}

#[test]
fn loop_options_applies_overrides_to_runtime_defaults() {
    let mut config = valid_config();
    config.loop_options.event_capacity = Some(16);
    config.loop_options.max_pending_steers = Some(8);
    config.loop_options.model_retry_attempts = Some(3);
    config.loop_options.model_retry_base_delay_millis = Some(250);
    let options = config.loop_options().unwrap();
    assert_eq!(options.max_tool_rounds, 0);
    assert_eq!(options.event_capacity, 16);
    assert_eq!(options.max_pending_steers, 8);
    assert_eq!(options.model_retry_attempts, 3);
    assert_eq!(options.model_retry_base_delay, Duration::from_millis(250));
}

#[test]
fn history_bytes_follow_the_model_input_budget_up_to_the_runtime_ceiling() {
    let mut config = valid_config();
    let default = config.loop_options().unwrap().limits.max_history_bytes;
    for (window, expected) in [
        (10_000, default),
        (400_000, 0),
        (10_000_000, 32 * 1024 * 1024),
    ] {
        let crate::models::ModelConfig::OpenAiResponses {
            physical_context_window,
            ..
        } = config.models.get_mut("main").unwrap();
        *physical_context_window = window;
        let budget = config.models["main"].input_budget_tokens() as usize;
        let options = config.loop_options_for_model("main").unwrap();
        let expected = if expected == 0 { budget * 8 } else { expected };
        assert_eq!(options.limits.max_history_bytes, expected);
    }
}

#[test]
fn automatic_options_give_prompt_preparation_a_model_timeout_floor() {
    let mut config = valid_config();
    config.compaction.enabled = true;
    config.loop_options.prompt_timeout_seconds = Some(5);
    config.loop_options.model_timeout_seconds = Some(17);
    let options = config.loop_options_for_model("main").unwrap();
    assert_eq!(options.prompt_timeout, Duration::from_secs(30));
    assert_eq!(options.model_timeout, Duration::from_secs(17));
    config.loop_options.model_timeout_seconds = Some(45);
    assert_eq!(
        config
            .loop_options_for_model("main")
            .unwrap()
            .prompt_timeout,
        Duration::from_secs(45)
    );
    config.loop_options.model_timeout_seconds = Some(17);

    config.loop_options.prompt_timeout_seconds = Some(30);
    let options = config.loop_options_for_model("main").unwrap();
    assert_eq!(options.prompt_timeout, Duration::from_secs(30));

    config.compaction.enabled = false;
    config.loop_options.prompt_timeout_seconds = Some(5);
    let options = config.loop_options_for_model("main").unwrap();
    assert_eq!(options.prompt_timeout, Duration::from_secs(5));
}

#[test]
fn extended_reasoning_values_parse_from_toml() {
    for (wire, expected) in [
        ("xhigh", ReasoningPreference::XHigh),
        ("max", ReasoningPreference::Max),
        ("ultra", ReasoningPreference::Ultra),
    ] {
        let config = AgentConfig::from_toml(&format!(
            r#"
data_dir = "./test-data"
event_capacity = 128
default_profile = "test"

[profiles.test]
model = "main"
reasoning = "{wire}"
system_prompt = "test system prompt"
tools = []
max_tool_rounds = 4
approval = "ask"

[models.main]
provider = "open_ai_responses"
model = "provider-model"
base_url = "https://example.invalid/v1"
api_key_env = "MINICORE_CONFIG_TEST_KEY"
physical_context_window = 10000
output_budget_tokens = 1000
safety_margin_tokens = 1000
supported_reasoning = ["{wire}"]
supports_tools = true
request_timeout_seconds = 30
"#
        ))
        .unwrap();
        assert_eq!(config.profiles["test"].reasoning, expected);
        assert_eq!(
            config.models["main"].supported_reasoning(),
            &BTreeSet::from([expected])
        );
    }
}

#[test]
fn valid_complete_toml_passes_validation() {
    let config = AgentConfig::from_toml(
        r#"
data_dir = "./test-data"
event_capacity = 128
default_profile = "test"

[profiles.test]
model = "main"
reasoning = "auto"
system_prompt = "test system prompt"
tools = []
max_tool_rounds = 4
approval = "ask"

[models.main]
provider = "open_ai_responses"
model = "provider-model"
base_url = "https://example.invalid/v1"
api_key_env = "MINICORE_CONFIG_TEST_KEY"
physical_context_window = 10000
output_budget_tokens = 1000
safety_margin_tokens = 1000
supported_reasoning = ["auto"]
supports_tools = true
request_timeout_seconds = 30

[loop]
event_capacity = 128
"#,
    );

    assert!(config.is_ok(), "complete valid TOML was rejected");
}

#[test]
fn load_resolves_a_system_prompt_file_relative_to_the_config_parent() {
    let root = std::env::temp_dir().join(format!(
        "minicore-agent-config-file-red-{}",
        std::process::id()
    ));
    let prompts = root.join("prompts");
    std::fs::create_dir_all(&prompts).unwrap();
    std::fs::write(prompts.join("coding.md"), "line one\r\nline two").unwrap();
    let config_path = root.join("agent.toml");
    let data_dir = root.join("data");
    let text = format!(
        r#"
data_dir = {data_dir}
event_capacity = 128
default_profile = "test"

[profiles.test]
model = "main"
reasoning = "auto"
system_prompt = {{ file = "prompts/coding.md" }}
tools = []
max_tool_rounds = 4
approval = "ask"

[models.main]
provider = "open_ai_responses"
model = "provider-model"
base_url = "https://example.invalid/v1"
api_key_env = "MINICORE_CONFIG_FILE_TEST_KEY"
physical_context_window = 10000
output_budget_tokens = 1000
safety_margin_tokens = 1000
supported_reasoning = ["auto"]
supports_tools = true
request_timeout_seconds = 30
"#,
        data_dir = toml_path(&data_dir)
    );
    std::fs::write(&config_path, text).unwrap();

    let config = AgentConfig::load(&config_path).unwrap();
    assert_eq!(config.profiles["test"].system_prompt, "line one\nline two");
    std::fs::write(prompts.join("coding.md"), "changed after load").unwrap();
    assert_eq!(config.profiles["test"].system_prompt, "line one\nline two");

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn relative_system_prompt_file_requires_a_config_path() {
    let result = AgentConfig::from_toml(&config_with_prompt_spec(
        r#"{ file = "prompts/missing.md" }"#,
    ));
    assert!(matches!(
        result,
        Err(ConfigError::SystemPromptFileNeedsConfigPath)
    ));
}

#[test]
fn legacy_inline_deserialization_stays_pure_and_file_requires_an_explicit_entrypoint() {
    let inline: AgentConfig =
        toml::from_str(&config_with_prompt_spec(r#""legacy inline prompt""#)).unwrap();
    assert_eq!(
        inline.profiles["test"].system_prompt,
        "legacy inline prompt"
    );

    let root = std::env::temp_dir().join(format!(
        "minicore-agent-config-deserialize-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let prompt = root.join("prompt.txt");
    std::fs::write(&prompt, "absolute prompt").unwrap();
    let spec = format!(r#"{{ file = {path} }}"#, path = toml_path(&prompt));
    assert!(toml::from_str::<AgentConfig>(&config_with_prompt_spec(&spec)).is_err());
    let config = AgentConfig::from_toml(&config_with_prompt_spec(&spec)).unwrap();
    assert_eq!(config.profiles["test"].system_prompt, "absolute prompt");

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn prompt_file_object_is_strictly_typed() {
    for spec in [
        "7".to_owned(),
        r#"{ file = 7 }"#.to_owned(),
        r#"{ file = "prompt", extra = true }"#.to_owned(),
    ] {
        assert!(matches!(
            AgentConfig::from_toml(&config_with_prompt_spec(&spec)),
            Err(ConfigError::InvalidSystemPromptFile)
        ));
    }
}

#[test]
fn windows_style_prompt_path_is_toml_roundtrip_safe() {
    let path = Path::new(r"C:\Users\name\prompt.md");
    let spec = format!(r#"{{ file = {path} }}"#, path = toml_path(path));
    let parsed = toml::from_str::<toml::Value>(&format!("system_prompt = {spec}"));
    assert_eq!(
        parsed.unwrap()["system_prompt"]["file"].as_str(),
        Some(r"C:\Users\name\prompt.md")
    );
}

#[test]
fn prompt_file_content_has_explicit_size_encoding_and_content_errors() {
    let root = std::env::temp_dir().join(format!(
        "minicore-agent-config-content-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let prompt = root.join("prompt");
    let spec = format!(r#"{{ file = {path} }}"#, path = toml_path(&prompt));

    for (bytes, expected) in [
        (b"".as_slice(), ConfigError::SystemPromptFileInvalidContent),
        (
            b"bad\0content".as_slice(),
            ConfigError::SystemPromptFileInvalidContent,
        ),
        (&[0xff, 0xfe][..], ConfigError::SystemPromptFileUtf8),
    ] {
        std::fs::write(&prompt, bytes).unwrap();
        assert!(matches!(
            AgentConfig::from_toml(&config_with_prompt_spec(&spec)),
            Err(error) if error == expected
        ));
    }

    std::fs::write(&prompt, vec![b'a'; 128 * 1024]).unwrap();
    assert!(AgentConfig::from_toml(&config_with_prompt_spec(&spec)).is_ok());
    std::fs::write(&prompt, vec![b'a'; 128 * 1024 + 1]).unwrap();
    assert!(matches!(
        AgentConfig::from_toml(&config_with_prompt_spec(&spec)),
        Err(ConfigError::SystemPromptFileTooLarge)
    ));

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn prompt_file_errors_do_not_include_path_or_file_contents() {
    let root = std::env::temp_dir().join(format!(
        "minicore-agent-config-safe-error-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let prompt = root.join("private-prompt-path");
    let secret = "private-prompt-content";
    std::fs::write(&prompt, format!("{secret}\0")).unwrap();
    let spec = format!(r#"{{ file = {path} }}"#, path = toml_path(&prompt));
    let error = AgentConfig::from_toml(&config_with_prompt_spec(&spec)).unwrap_err();
    let diagnostic = format!("{error} {error:?}");
    let path_text = prompt.to_string_lossy().into_owned();
    assert!(!diagnostic.contains(&path_text));
    assert!(!diagnostic.contains(secret));
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn prompt_file_must_be_a_regular_file() {
    let root = std::env::temp_dir().join(format!(
        "minicore-agent-config-regular-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let spec = format!(r#"{{ file = {path} }}"#, path = toml_path(&root));
    assert!(matches!(
        AgentConfig::from_toml(&config_with_prompt_spec(&spec)),
        Err(ConfigError::SystemPromptFileNotRegular)
    ));
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(unix)]
#[test]
fn prompt_file_symlink_to_a_regular_file_is_allowed() {
    use std::os::unix::fs::symlink;

    let root = std::env::temp_dir().join(format!(
        "minicore-agent-config-symlink-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let target = root.join("target");
    let link = root.join("link");
    std::fs::write(&target, "prompt").unwrap();
    symlink(&target, &link).unwrap();
    let spec = format!(r#"{{ file = {path} }}"#, path = toml_path(&link));
    let config = AgentConfig::from_toml(&config_with_prompt_spec(&spec)).unwrap();
    assert_eq!(config.profiles["test"].system_prompt, "prompt");
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(unix)]
#[test]
fn load_uses_the_supplied_parent_for_a_symlinked_config_file() {
    use std::os::unix::fs::symlink;

    let root = std::env::temp_dir().join(format!(
        "minicore-agent-config-alias-parent-{}",
        std::process::id()
    ));
    let alias_dir = root.join("alias");
    let real_dir = root.join("real");
    std::fs::create_dir_all(alias_dir.join("prompts")).unwrap();
    std::fs::create_dir_all(real_dir.join("prompts")).unwrap();
    std::fs::write(alias_dir.join("prompts/prompt"), "alias prompt").unwrap();
    std::fs::write(real_dir.join("prompts/prompt"), "real prompt").unwrap();
    let config_text = format!(
        r#"
data_dir = {data_dir}
event_capacity = 128
default_profile = "test"

[profiles.test]
model = "main"
reasoning = "auto"
system_prompt = {{ file = "prompts/prompt" }}
tools = []
max_tool_rounds = 4
approval = "ask"

[models.main]
provider = "open_ai_responses"
model = "provider-model"
base_url = "https://example.invalid/v1"
api_key_env = "MINICORE_CONFIG_ALIAS_TEST_KEY"
physical_context_window = 10000
output_budget_tokens = 1000
safety_margin_tokens = 1000
supported_reasoning = ["auto"]
supports_tools = true
request_timeout_seconds = 30
"#,
        data_dir = toml_path(&root.join("data"))
    );
    let real_config = real_dir.join("agent.toml");
    std::fs::write(&real_config, config_text).unwrap();
    let alias_config = alias_dir.join("agent.toml");
    symlink(&real_config, &alias_config).unwrap();

    let config = AgentConfig::load(&alias_config).unwrap();
    assert_eq!(config.profiles["test"].system_prompt, "alias prompt");

    std::fs::remove_dir_all(root).unwrap();
}
