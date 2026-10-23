use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use thiserror::Error;

use minicore_runtime::{LoopOptions, LoopStartError};

use crate::compaction::CompactionPolicy;
use crate::models::{ModelConfig, Models};
use crate::profiles::Profiles;
pub use crate::profiles::{ApprovalMode, Profile};
use crate::tools::KNOWN_TOOL_NAMES;

const MAX_EVENT_CAPACITY: usize = 4_096;
const DEFAULT_EVENT_CAPACITY: usize = 256;
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
    #[serde(default)]
    pub compaction: CompactionConfig,
}

/// Agent-global automatic compaction policy. This is runtime policy, not
/// persisted Session state; a reload replaces the future-turn policy.
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactionConfig {
    #[serde(default = "default_compaction_enabled")]
    pub enabled: bool,
    #[serde(default = "default_compaction_trigger_percent")]
    pub trigger_percent: u8,
    #[serde(default = "default_compaction_target_percent")]
    pub target_percent: u8,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            enabled: default_compaction_enabled(),
            trigger_percent: default_compaction_trigger_percent(),
            target_percent: default_compaction_target_percent(),
        }
    }
}

impl CompactionConfig {
    pub(crate) fn validate(&self) -> Result<(), ConfigError> {
        if self.target_percent == 0
            || self.trigger_percent == 0
            || self.trigger_percent > 100
            || self.target_percent >= self.trigger_percent
        {
            return Err(ConfigError::InvalidCompaction);
        }
        Ok(())
    }

    pub(crate) fn policy(&self) -> CompactionPolicy {
        CompactionPolicy {
            enabled: self.enabled,
            trigger_percent: self.trigger_percent,
            target_percent: self.target_percent,
        }
    }
}

fn default_compaction_enabled() -> bool {
    true
}

fn default_compaction_trigger_percent() -> u8 {
    95
}

fn default_compaction_target_percent() -> u8 {
    50
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
        let path = Self::absolute_lexical_path(path.as_ref())?;
        let text = std::fs::read_to_string(&path).map_err(|_| ConfigError::Read)?;
        Self::from_toml_with_base(&text, path.parent())
    }

    /// Makes the path absolute without canonicalizing it. In particular,
    /// symlinked parents remain part of the lexical path used for relative
    /// prompt files and later reloads.
    pub(crate) fn absolute_lexical_path(path: &Path) -> Result<PathBuf, ConfigError> {
        if path.is_absolute() {
            Ok(path.to_path_buf())
        } else {
            std::env::current_dir()
                .map(|current| current.join(path))
                .map_err(|_| ConfigError::Read)
        }
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
        self.compaction.validate()?;
        self.loop_options().map(|_| ())
    }

    pub(crate) fn profiles(&self) -> Profiles {
        Profiles::from_values(self.profiles.clone())
    }

    /// Builds the per-loop `LoopOptions` for one session turn: runtime safe
    /// defaults and this config's `[loop]` overrides. Legacy stored/profile
    /// round budgets remain readable but no longer stop normal agent turns.
    /// Runtime `LoopLimits` are not configurable in this phase.
    pub(crate) fn loop_options(&self) -> Result<LoopOptions, ConfigError> {
        let mut options =
            LoopOptions::default_checked().map_err(|_| ConfigError::InvalidLoopOptions)?;
        options.max_tool_rounds = 0;
        let overrides = &self.loop_options;
        macro_rules! apply_overrides {
            ($($field:ident = $value:expr),+ $(,)?) => {
                $(if let Some(value) = $value { options.$field = value; })+
            };
        }
        apply_overrides! {
            event_capacity = overrides.event_capacity,
            max_pending_steers = overrides.max_pending_steers,
            prompt_timeout = overrides.prompt_timeout_seconds.map(Duration::from_secs),
            model_timeout = overrides.model_timeout_seconds.map(Duration::from_secs),
            policy_timeout = overrides.policy_timeout_seconds.map(Duration::from_secs),
            tool_timeout = overrides.tool_timeout_seconds.map(Duration::from_secs),
            model_retry_attempts = overrides.model_retry_attempts,
            model_retry_base_delay = overrides.model_retry_base_delay_millis.map(Duration::from_millis),
        }
        options
            .validate()
            .map_err(|_| ConfigError::InvalidLoopOptions)?;
        Ok(options)
    }

    pub(crate) fn loop_options_for_model(
        &self,
        model_id: &str,
    ) -> Result<LoopOptions, ConfigError> {
        let mut options = self.loop_options()?;
        if self.compaction.enabled {
            options.prompt_timeout = options.prompt_timeout.max(options.model_timeout);
            if let Some(model) = self.models.get(model_id)
                && let Some(timeout) = model.request_timeout()
            {
                options.prompt_timeout = options.prompt_timeout.max(timeout);
            }
            options
                .validate()
                .map_err(|_| ConfigError::InvalidLoopOptions)?;
        }
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
    #[error("Model TLS trust store is invalid")]
    InvalidTlsTrustStore,
    #[error("configuration profile model was not found")]
    ProfileModelNotFound,
    #[error("configuration reasoning is unsupported")]
    UnsupportedReasoning,
    #[error("configuration tools are unsupported")]
    ToolsNotSupported,
    #[error("configuration loop overrides are invalid")]
    InvalidLoopOptions,
    #[error("configuration compaction policy is invalid")]
    InvalidCompaction,
}

#[cfg(test)]
mod tests;
