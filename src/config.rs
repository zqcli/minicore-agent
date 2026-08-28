use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use thiserror::Error;

use minicore_runtime::config::KernelConfig;
use minicore_runtime::tools::ToolName;
use minicore_runtime::value::BoundedText;

use crate::models::{ModelConfig, Models};
use crate::profiles::Profiles;
pub use crate::profiles::{ApprovalMode, Profile, ProfileCompaction};

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
    pub fn new(data_dir: impl Into<PathBuf>) -> Result<Self, ConfigError> {
        let config = Self {
            data_dir: data_dir.into(),
            event_capacity: DEFAULT_EVENT_CAPACITY,
            default_profile: String::new(),
            profiles: BTreeMap::new(),
            models: BTreeMap::new(),
            kernel: KernelOverrides::default(),
        };
        config.validate()?;
        Ok(config)
    }

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
        if !self.default_profile.is_empty() && !self.profiles.contains_key(&self.default_profile) {
            return Err(ConfigError::InvalidDefaultProfile);
        }
        for (id, profile) in &self.profiles {
            if id.is_empty()
                || profile.model.is_empty()
                || profile.system_prompt.is_empty()
                || BoundedText::new(&profile.system_prompt).is_err()
                || !(1..=MAX_TOOL_ROUNDS).contains(&profile.max_tool_rounds)
                || profile
                    .tools
                    .iter()
                    .any(|name| name.parse::<ToolName>().is_err())
                || !valid_compaction(&profile.compaction)
            {
                return Err(ConfigError::InvalidProfile);
            }
        }
        for (id, model) in &self.models {
            if Models::model_ref(id).is_err() || model.validate().is_err() {
                return Err(ConfigError::InvalidModel);
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

fn valid_compaction(compaction: &ProfileCompaction) -> bool {
    match compaction {
        ProfileCompaction::Disabled => true,
        ProfileCompaction::Model {
            trigger_tokens,
            target_tokens,
        } => *trigger_tokens > 0 && *target_tokens > 0 && target_tokens < trigger_tokens,
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
    #[error("configuration default_profile is not defined")]
    InvalidDefaultProfile,
    #[error("configuration profile is invalid")]
    InvalidProfile,
    #[error("configuration model is invalid")]
    InvalidModel,
    #[error("configuration kernel overrides are invalid")]
    InvalidKernel,
}
