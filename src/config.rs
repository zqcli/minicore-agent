use std::collections::{BTreeMap, BTreeSet};
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
        if text.trim().is_empty() {
            return Err(ConfigError::Empty);
        }
        let config: Self = toml::from_str(text).map_err(|_| ConfigError::Parse)?;
        config.validate()?;
        Ok(config)
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|_| ConfigError::Read)?;
        Self::from_toml(&text)
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
}
