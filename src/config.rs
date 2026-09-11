use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use thiserror::Error;

use minicore_runtime::{LoopOptions, LoopStartError};

use crate::models::{ModelConfig, Models};
use crate::profiles::Profiles;
pub use crate::profiles::{ApprovalMode, Profile};
use crate::tools::KNOWN_TOOL_NAMES;

const MAX_EVENT_CAPACITY: usize = 4_096;
const DEFAULT_EVENT_CAPACITY: usize = 256;
const MAX_TOOL_ROUNDS: u16 = 1_024;
/// System prompt is merged with AGENTS.md into one model system message, so
/// the profile half is capped well under the absolute `ModelMessage` ceiling.
pub(crate) const MAX_PROFILE_SYSTEM_PROMPT_BYTES: usize = 128 * 1024;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    pub data_dir: PathBuf,
    #[serde(default = "default_event_capacity")]
    pub event_capacity: usize,
    #[serde(default)]
    pub default_profile: String,
    #[serde(default)]
    pub profiles: BTreeMap<String, Profile>,
    #[serde(default)]
    pub models: BTreeMap<String, ModelConfig>,
    #[serde(default, rename = "loop")]
    pub loop_options: LoopOverrides,
}

impl AgentConfig {
    pub fn from_toml(text: &str) -> Result<Self, ConfigError> {
        Self::from_toml_with_base(text, None)
    }

    fn from_toml_with_base(text: &str, config_dir: Option<&Path>) -> Result<Self, ConfigError> {
        if text.trim().is_empty() {
            return Err(ConfigError::Empty);
        }
        let mut value: toml::Value = toml::from_str(text).map_err(|_| ConfigError::Parse)?;
        resolve_system_prompt_files(&mut value, config_dir)?;
        let config: Self = value.try_into().map_err(|_| ConfigError::Parse)?;
        config.validate()?;
        Ok(config)
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()
                .map_err(|_| ConfigError::Read)?
                .join(path)
        };
        let text = std::fs::read_to_string(&path).map_err(|_| ConfigError::Read)?;
        Self::from_toml_with_base(&text, path.parent())
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.data_dir.as_os_str().is_empty() {
            return Err(ConfigError::EmptyDataDir);
        }
        if !(1..=MAX_EVENT_CAPACITY).contains(&self.event_capacity) {
            return Err(ConfigError::InvalidEventCapacity);
        }
        if self.default_profile.is_empty() {
            return Err(ConfigError::MissingDefaultProfile);
        }
        if self.profiles.is_empty() {
            return Err(ConfigError::MissingProfiles);
        }
        if self.models.is_empty() {
            return Err(ConfigError::MissingModels);
        }
        if !self.profiles.contains_key(&self.default_profile) {
            return Err(ConfigError::InvalidDefaultProfile);
        }
        for (id, profile) in &self.profiles {
            let mut tools = BTreeSet::new();
            if id.is_empty()
                || profile.model.is_empty()
                || profile.system_prompt.is_empty()
                || profile.system_prompt.len() > MAX_PROFILE_SYSTEM_PROMPT_BYTES
                || profile
                    .system_prompt
                    .chars()
                    .any(|character| character.is_control() && !matches!(character, '\n' | '\t'))
                || !(1..=MAX_TOOL_ROUNDS).contains(&profile.max_tool_rounds)
                || profile.tools.iter().any(|name| {
                    !KNOWN_TOOL_NAMES.contains(&name.as_str()) || !tools.insert(name.as_str())
                })
            {
                return Err(ConfigError::InvalidProfile);
            }
        }
        for (id, model) in &self.models {
            if Models::model_ref(id).is_err() || model.validate().is_err() {
                return Err(ConfigError::InvalidModel);
            }
        }
        for profile in self.profiles.values() {
            let model = self
                .models
                .get(&profile.model)
                .ok_or(ConfigError::ProfileModelNotFound)?;
            if !model.supported_reasoning().contains(&profile.reasoning) {
                return Err(ConfigError::UnsupportedReasoning);
            }
            if !profile.tools.is_empty() && !model.supports_tools() {
                return Err(ConfigError::ToolsNotSupported);
            }
        }
        self.loop_options(32).map(|_| ())
    }

    pub(crate) fn profiles(&self) -> Profiles {
        Profiles::from_values(self.profiles.clone())
    }

    /// Builds the per-loop `LoopOptions` for one session turn: runtime safe
    /// defaults, the session's tool-round budget, and this config's `[loop]`
    /// overrides. Runtime `LoopLimits` are not configurable in this phase.
    pub(crate) fn loop_options(&self, max_tool_rounds: u16) -> Result<LoopOptions, ConfigError> {
        let mut options =
            LoopOptions::default_checked().map_err(|_| ConfigError::InvalidLoopOptions)?;
        options.max_tool_rounds = max_tool_rounds;
        let overrides = &self.loop_options;
        if let Some(value) = overrides.event_capacity {
            options.event_capacity = value;
        }
        if let Some(value) = overrides.max_pending_steers {
            options.max_pending_steers = value;
        }
        if let Some(value) = overrides.prompt_timeout_seconds {
            options.prompt_timeout = Duration::from_secs(value);
        }
        if let Some(value) = overrides.model_timeout_seconds {
            options.model_timeout = Duration::from_secs(value);
        }
        if let Some(value) = overrides.policy_timeout_seconds {
            options.policy_timeout = Duration::from_secs(value);
        }
        if let Some(value) = overrides.tool_timeout_seconds {
            options.tool_timeout = Duration::from_secs(value);
        }
        if let Some(value) = overrides.model_retry_attempts {
            options.model_retry_attempts = value;
        }
        if let Some(value) = overrides.model_retry_base_delay_millis {
            options.model_retry_base_delay = Duration::from_millis(value);
        }
        options
            .validate()
            .map_err(|_| ConfigError::InvalidLoopOptions)?;
        Ok(options)
    }
}

fn resolve_system_prompt_files(
    value: &mut toml::Value,
    config_dir: Option<&Path>,
) -> Result<(), ConfigError> {
    let Some(profiles) = value
        .get_mut("profiles")
        .and_then(toml::Value::as_table_mut)
    else {
        return Ok(());
    };
    for (_, profile) in profiles.iter_mut() {
        let Some(profile) = profile.as_table_mut() else {
            continue;
        };
        let Some(prompt) = profile.get("system_prompt") else {
            continue;
        };
        let file = match prompt {
            toml::Value::String(_) => continue,
            toml::Value::Table(table) => {
                if table.len() != 1 {
                    return Err(ConfigError::InvalidSystemPromptFile);
                }
                let Some(toml::Value::String(file)) = table.get("file") else {
                    return Err(ConfigError::InvalidSystemPromptFile);
                };
                file.clone()
            }
            _ => return Err(ConfigError::InvalidSystemPromptFile),
        };
        if file.is_empty() || file.chars().any(char::is_control) {
            return Err(ConfigError::InvalidSystemPromptFile);
        }
        let path = Path::new(&file);
        let path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            config_dir
                .ok_or(ConfigError::SystemPromptFileNeedsConfigPath)?
                .join(path)
        };
        profile.insert(
            "system_prompt".to_owned(),
            toml::Value::String(read_system_prompt_file(&path)?),
        );
    }
    Ok(())
}

fn read_system_prompt_file(path: &Path) -> Result<String, ConfigError> {
    let metadata = std::fs::metadata(path).map_err(|_| ConfigError::SystemPromptFileRead)?;
    if !metadata.file_type().is_file() {
        return Err(ConfigError::SystemPromptFileNotRegular);
    }
    let file = std::fs::File::open(path).map_err(|_| ConfigError::SystemPromptFileRead)?;
    if !file
        .metadata()
        .map_err(|_| ConfigError::SystemPromptFileRead)?
        .file_type()
        .is_file()
    {
        return Err(ConfigError::SystemPromptFileNotRegular);
    }
    let mut bytes = Vec::new();
    file.take((MAX_PROFILE_SYSTEM_PROMPT_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| ConfigError::SystemPromptFileRead)?;
    if bytes.len() > MAX_PROFILE_SYSTEM_PROMPT_BYTES {
        return Err(ConfigError::SystemPromptFileTooLarge);
    }
    let text = String::from_utf8(bytes).map_err(|_| ConfigError::SystemPromptFileUtf8)?;
    let text = text.replace("\r\n", "\n");
    if text.is_empty()
        || text
            .chars()
            .any(|character| character.is_control() && !matches!(character, '\n' | '\t'))
    {
        return Err(ConfigError::SystemPromptFileInvalidContent);
    }
    Ok(text)
}

fn default_event_capacity() -> usize {
    DEFAULT_EVENT_CAPACITY
}

/// Per-loop overrides applied on top of the runtime `LoopOptions` safe
/// defaults. Each field is bounded by the same checks `AgentLoop::start` runs.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoopOverrides {
    pub event_capacity: Option<usize>,
    pub max_pending_steers: Option<usize>,

    pub prompt_timeout_seconds: Option<u64>,
    pub model_timeout_seconds: Option<u64>,
    pub policy_timeout_seconds: Option<u64>,
    pub tool_timeout_seconds: Option<u64>,

    pub model_retry_attempts: Option<u8>,
    pub model_retry_base_delay_millis: Option<u64>,
}

pub(crate) const fn map_loop_start_error(error: LoopStartError) -> crate::AgentError {
    match error {
        LoopStartError::HistoryTooLarge => crate::AgentError::HistoryTooLarge,
        LoopStartError::InvalidInput => crate::AgentError::InvalidInput,
        LoopStartError::InvalidConfig => crate::AgentError::InvalidSessionSettings,
        LoopStartError::NoTokioRuntime
        | LoopStartError::InvalidOptions
        | LoopStartError::IdGeneration => crate::AgentError::Internal,
        _ => crate::AgentError::Internal,
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ConfigError {
    #[error("configuration text is empty")]
    Empty,
    #[error("configuration could not be parsed (invalid syntax or unsupported fields)")]
    Parse,
    #[error("system prompt file configuration is invalid")]
    InvalidSystemPromptFile,
    #[error("system prompt file requires a configuration file path")]
    SystemPromptFileNeedsConfigPath,
    #[error("system prompt file could not be read")]
    SystemPromptFileRead,
    #[error("system prompt file is not a regular file")]
    SystemPromptFileNotRegular,
    #[error("system prompt file is too large")]
    SystemPromptFileTooLarge,
    #[error("system prompt file is not valid UTF-8")]
    SystemPromptFileUtf8,
    #[error("system prompt file content is invalid")]
    SystemPromptFileInvalidContent,
    #[error("configuration file could not be read")]
    Read,
    #[error("configuration data_dir must not be empty")]
    EmptyDataDir,
    #[error("configuration event_capacity is outside its bound")]
    InvalidEventCapacity,
    #[error("configuration profiles are missing")]
    MissingProfiles,
    #[error("configuration models are missing")]
    MissingModels,
    #[error("configuration default_profile is missing")]
    MissingDefaultProfile,
    #[error("configuration default_profile is not defined")]
    InvalidDefaultProfile,
    #[error("configuration profile is invalid")]
    InvalidProfile,
    #[error("configuration model is invalid")]
    InvalidModel,
    #[error("Model API key environment variable is missing or empty")]
    MissingModelApiKey,
    #[error("configuration profile model was not found")]
    ProfileModelNotFound,
    #[error("configuration reasoning is unsupported")]
    UnsupportedReasoning,
    #[error("configuration tools are unsupported")]
    ToolsNotSupported,
    #[error("configuration loop overrides are invalid")]
    InvalidLoopOptions,
}

#[cfg(test)]
mod tests {
    use minicore_runtime::model::ReasoningPreference; // used by ModelConfig below

    use super::*;

    use crate::models::ModelConfig;
    use crate::profiles::ApprovalMode;

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
        let options = config.loop_options(7).unwrap();
        assert_eq!(options.max_tool_rounds, 7);
        assert_eq!(options.event_capacity, 16);
        assert_eq!(options.max_pending_steers, 8);
        assert_eq!(options.model_retry_attempts, 3);
        assert_eq!(options.model_retry_base_delay, Duration::from_millis(250));
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
}
