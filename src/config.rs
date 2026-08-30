use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use thiserror::Error;

use minicore_runtime::config::KernelConfig;
use minicore_runtime::value::BoundedText;

use crate::models::{ModelConfig, Models};
use crate::profiles::Profiles;
pub use crate::profiles::{ApprovalMode, Profile, ProfileCompaction};
use crate::tools::KNOWN_TOOL_NAMES;

const MAX_EVENT_CAPACITY: usize = 4_096;
const DEFAULT_EVENT_CAPACITY: usize = 256;
const MAX_TOOL_ROUNDS: u16 = 1_024;

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
    #[serde(default)]
    pub kernel: KernelOverrides,
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
                || BoundedText::new(&profile.system_prompt).is_err()
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
            match &profile.compaction {
                ProfileCompaction::Disabled => {}
                ProfileCompaction::Model { .. } => {
                    return Err(ConfigError::UnsupportedCompaction);
                }
            }
        }
        self.kernel_config().map(|_| ())
    }

    pub(crate) fn profiles(&self) -> Profiles {
        Profiles::from_values(self.profiles.clone())
    }

    pub(crate) fn kernel_config(&self) -> Result<KernelConfig, ConfigError> {
        let mut kernel = KernelConfig::default_checked().map_err(|_| ConfigError::InvalidKernel)?;
        kernel.event_capacity = self.event_capacity;
        if let Some(value) = self.kernel.command_capacity {
            kernel.command_capacity = value;
        }
        if let Some(value) = self.kernel.runner_capacity {
            kernel.runner_capacity = value;
        }
        if let Some(value) = self.kernel.event_capacity {
            kernel.event_capacity = value;
        }
        if let Some(value) = self.kernel.model_call_timeout_seconds {
            kernel.model_call_timeout = Duration::from_secs(value);
        }
        if let Some(value) = self.kernel.tool_call_timeout_seconds {
            kernel.tool_call_timeout = Duration::from_secs(value);
        }
        if let Some(value) = self.kernel.context_timeout_seconds {
            kernel.context_timeout = Duration::from_secs(value);
        }
        kernel.validate().map_err(|_| ConfigError::InvalidKernel)?;
        Ok(kernel)
    }
}

fn default_event_capacity() -> usize {
    DEFAULT_EVENT_CAPACITY
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KernelOverrides {
    pub command_capacity: Option<usize>,
    pub runner_capacity: Option<usize>,
    pub event_capacity: Option<usize>,
    pub model_call_timeout_seconds: Option<u64>,
    pub tool_call_timeout_seconds: Option<u64>,
    pub context_timeout_seconds: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ConfigError {
    #[error("configuration text is empty")]
    Empty,
    #[error("configuration could not be parsed")]
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
    #[error("configuration profile model was not found")]
    ProfileModelNotFound,
    #[error("configuration reasoning is unsupported")]
    UnsupportedReasoning,
    #[error("configuration tools are unsupported")]
    ToolsNotSupported,
    #[error("configuration compaction is unsupported")]
    UnsupportedCompaction,
    #[error("configuration kernel overrides are invalid")]
    InvalidKernel,
}

#[cfg(test)]
mod tests {
    use minicore_runtime::model::ReasoningPreference;

    use super::*;

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
            compaction: ProfileCompaction::Disabled,
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
            kernel: KernelOverrides::default(),
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
    fn model_compaction_is_rejected_when_unsupported() {
        let mut config = valid_config();
        config.profiles.get_mut("test").unwrap().compaction = ProfileCompaction::Model {
            trigger_tokens: 1_000,
            target_tokens: 500,
        };

        assert!(matches!(
            config.validate(),
            Err(ConfigError::UnsupportedCompaction)
        ));
    }

    #[test]
    fn valid_default_profile_is_accepted() {
        assert!(valid_config().validate().is_ok());
    }

    #[test]
    fn second_profile_is_validated_for_missing_model() {
        let mut config = valid_config();
        let mut second = profile();
        second.model = "missing".to_owned();
        config.profiles.insert("second".to_owned(), second);

        assert!(matches!(
            config.validate(),
            Err(ConfigError::ProfileModelNotFound)
        ));
    }

    #[test]
    fn second_profile_is_validated_for_unsupported_reasoning() {
        let mut config = valid_config();
        let mut second = profile();
        second.reasoning = ReasoningPreference::High;
        config.profiles.insert("second".to_owned(), second);

        assert!(matches!(
            config.validate(),
            Err(ConfigError::UnsupportedReasoning)
        ));
    }

    #[test]
    fn second_profile_is_validated_for_unsupported_tools() {
        let mut config = valid_config();
        config.models.insert(
            "main".to_owned(),
            model_config(BTreeSet::from([ReasoningPreference::Auto]), false),
        );
        let mut second = profile();
        second.tools = vec!["read".to_owned()];
        config.profiles.insert("second".to_owned(), second);

        assert!(matches!(
            config.validate(),
            Err(ConfigError::ToolsNotSupported)
        ));
    }

    #[test]
    fn second_profile_is_validated_for_unsupported_compaction() {
        let mut config = valid_config();
        let mut second = profile();
        second.compaction = ProfileCompaction::Model {
            trigger_tokens: 1_000,
            target_tokens: 500,
        };
        config.profiles.insert("second".to_owned(), second);

        assert!(matches!(
            config.validate(),
            Err(ConfigError::UnsupportedCompaction)
        ));
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
"#,
        );

        assert!(config.is_ok(), "complete valid TOML was rejected");
    }
}
