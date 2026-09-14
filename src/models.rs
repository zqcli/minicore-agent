use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use minicore_runtime::LoopId;
use minicore_runtime::model::{Model, ModelError, ModelRef, ModelRequest, ReasoningPreference};

mod openai;

#[derive(Clone, Deserialize)]
#[serde(tag = "provider", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModelConfig {
    OpenAiResponses {
        model: String,
        base_url: String,
        api_key_env: String,
        physical_context_window: u32,
        output_budget_tokens: u32,
        safety_margin_tokens: u32,
        supported_reasoning: BTreeSet<ReasoningPreference>,
        supports_tools: bool,
        #[serde(default)]
        request_timeout_seconds: Option<u64>,
    },
}

impl fmt::Debug for ModelConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OpenAiResponses {
                model,
                physical_context_window,
                output_budget_tokens,
                safety_margin_tokens,
                supported_reasoning,
                supports_tools,
                request_timeout_seconds,
                ..
            } => formatter
                .debug_struct("OpenAiResponses")
                .field("model", model)
                .field("base_url", &"<redacted>")
                .field("api_key_env", &"<redacted>")
                .field("physical_context_window", physical_context_window)
                .field("output_budget_tokens", output_budget_tokens)
                .field("safety_margin_tokens", safety_margin_tokens)
                .field("supported_reasoning", supported_reasoning)
                .field("supports_tools", supports_tools)
                .field("request_timeout_seconds", request_timeout_seconds)
                .finish(),
        }
    }
}

impl ModelConfig {
    pub(crate) fn validate(&self) -> Result<(), ModelConfigError> {
        match self {
            Self::OpenAiResponses {
                model,
                base_url,
                api_key_env,
                physical_context_window,
                output_budget_tokens,
                safety_margin_tokens,
                supported_reasoning,
                request_timeout_seconds,
                ..
            } => {
                if model.trim().is_empty()
                    || base_url.trim().is_empty()
                    || api_key_env.trim().is_empty()
                    || *output_budget_tokens == 0
                    || supported_reasoning.is_empty()
                    || output_budget_tokens
                        .checked_add(*safety_margin_tokens)
                        .is_none_or(|reserved| *physical_context_window <= reserved)
                    || request_timeout_seconds.is_some_and(|seconds| seconds == 0)
                    || openai::endpoint(base_url).is_err()
                {
                    return Err(ModelConfigError::InvalidConfiguration);
                }
                Ok(())
            }
        }
    }

    pub(crate) fn credential_env_name(&self) -> &str {
        match self {
            Self::OpenAiResponses { api_key_env, .. } => api_key_env,
        }
    }

    pub(crate) fn supported_reasoning(&self) -> &BTreeSet<ReasoningPreference> {
        match self {
            Self::OpenAiResponses {
                supported_reasoning,
                ..
            } => supported_reasoning,
        }
    }

    pub(crate) fn supports_tools(&self) -> bool {
        match self {
            Self::OpenAiResponses { supports_tools, .. } => *supports_tools,
        }
    }

    pub(crate) fn request_timeout(&self) -> Option<Duration> {
        match self {
            Self::OpenAiResponses {
                request_timeout_seconds,
                ..
            } => request_timeout_seconds.map(Duration::from_secs),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum ModelConfigError {
    #[error("model configuration is invalid")]
    InvalidConfiguration,
    #[error("model is not configured")]
    NotFound,
    #[error("model API key environment variable is missing or empty")]
    MissingApiKey,
    #[error("model HTTP client could not be constructed")]
    ClientBuild,
    #[error("model reference is invalid")]
    InvalidReference,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ModelInfo {
    pub id: String,
    pub model_ref: ModelRef,
    pub context_window: u64,
    pub supports_tools: bool,
    pub supported_reasoning: Vec<ReasoningPreference>,
}

pub(crate) trait ProviderBudget: Send + Sync {
    fn estimate_request_tokens(
        &self,
        request: &ModelRequest,
        loop_id: Option<LoopId>,
        request_index: Option<u32>,
    ) -> Result<u64, ModelError>;

    fn estimate_request_bytes(
        &self,
        request: &ModelRequest,
        loop_id: Option<LoopId>,
        request_index: Option<u32>,
    ) -> Result<usize, ModelError>;
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct DefaultProviderBudget;

#[cfg(test)]
impl ProviderBudget for DefaultProviderBudget {
    fn estimate_request_tokens(
        &self,
        request: &ModelRequest,
        loop_id: Option<LoopId>,
        request_index: Option<u32>,
    ) -> Result<u64, ModelError> {
        let bytes = self.estimate_request_bytes(request, loop_id, request_index)?;
        Ok(bytes.div_ceil(4) as u64)
    }

    fn estimate_request_bytes(
        &self,
        request: &ModelRequest,
        _loop_id: Option<LoopId>,
        _request_index: Option<u32>,
    ) -> Result<usize, ModelError> {
        let mut bytes = 0usize;
        let mut writer = BudgetCountingWriter(&mut bytes);
        openai::serialize_request_for_budget(request, &mut writer)?;
        Ok(bytes)
    }
}

#[cfg(test)]
struct BudgetCountingWriter<'a>(&'a mut usize);

#[cfg(test)]
impl std::io::Write for BudgetCountingWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        *self.0 = self.0.saturating_add(buf.len());
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct ModelEntry {
    pub(crate) model: Arc<dyn Model>,
    pub(crate) budget: Arc<dyn ProviderBudget>,
}

pub(crate) struct Models {
    values: BTreeMap<String, ModelEntry>,
}

impl Clone for Models {
    fn clone(&self) -> Self {
        Self {
            values: self
                .values
                .iter()
                .map(|(id, entry)| (id.clone(), entry.clone()))
                .collect(),
        }
    }
}

impl Models {
    pub(crate) async fn from_config(
        config: &BTreeMap<String, ModelConfig>,
    ) -> Result<Self, ModelConfigError> {
        Self::from_config_with_env(config, |name| std::env::var(name).ok())
    }

    fn from_config_with_env(
        config: &BTreeMap<String, ModelConfig>,
        mut read_env: impl FnMut(&str) -> Option<String>,
    ) -> Result<Self, ModelConfigError> {
        let mut values = BTreeMap::new();
        for (id, value) in config {
            value.validate()?;
            let entry = match value {
                ModelConfig::OpenAiResponses {
                    model,
                    base_url,
                    api_key_env,
                    physical_context_window,
                    output_budget_tokens,
                    safety_margin_tokens,
                    supported_reasoning,
                    supports_tools,
                    request_timeout_seconds,
                } => {
                    let api_key = read_env(api_key_env)
                        .filter(|value| !value.trim().is_empty())
                        .ok_or(ModelConfigError::MissingApiKey)?;
                    let reserved = output_budget_tokens
                        .checked_add(*safety_margin_tokens)
                        .ok_or(ModelConfigError::InvalidConfiguration)?;
                    let settings = openai::OpenAiResponsesSettings {
                        model_ref: Self::model_ref(id)?,
                        provider_model: model.clone(),
                        endpoint: openai::endpoint(base_url)?,
                        api_key,
                        effective_context_window: u64::from(
                            physical_context_window
                                .checked_sub(reserved)
                                .ok_or(ModelConfigError::InvalidConfiguration)?,
                        ),
                        output_budget_tokens: *output_budget_tokens,
                        supported_reasoning: supported_reasoning.clone(),
                        supports_tools: *supports_tools,
                        request_timeout: request_timeout_seconds.map(Duration::from_secs),
                    };
                    let openai_model = Arc::new(openai::OpenAiResponsesModel::new(settings)?);
                    ModelEntry {
                        model: Arc::clone(&openai_model) as Arc<dyn Model>,
                        budget: Arc::clone(&openai_model) as Arc<dyn ProviderBudget>,
                    }
                }
            };
            values.insert(id.clone(), entry);
        }
        Ok(Self { values })
    }

    #[cfg(test)]
    pub(crate) fn from_values(values: BTreeMap<String, Arc<dyn Model>>) -> Self {
        let entries = values
            .into_iter()
            .map(|(id, model)| {
                (
                    id,
                    ModelEntry {
                        model,
                        budget: Arc::new(DefaultProviderBudget),
                    },
                )
            })
            .collect();
        Self { values: entries }
    }

    pub(crate) fn get(&self, id: &str) -> Result<Arc<dyn Model>, ModelConfigError> {
        self.values
            .get(id)
            .map(|entry| Arc::clone(&entry.model))
            .ok_or(ModelConfigError::NotFound)
    }

    pub(crate) fn get_budget(&self, id: &str) -> Result<Arc<dyn ProviderBudget>, ModelConfigError> {
        self.values
            .get(id)
            .map(|entry| Arc::clone(&entry.budget))
            .ok_or(ModelConfigError::NotFound)
    }

    pub(crate) fn list(&self) -> Vec<ModelInfo> {
        self.values
            .iter()
            .map(|(id, entry)| {
                let descriptor = entry.model.descriptor();
                ModelInfo {
                    id: id.clone(),
                    model_ref: descriptor.model_ref.clone(),
                    context_window: descriptor.context_window,
                    supports_tools: descriptor.supports_tools,
                    supported_reasoning: descriptor.supported_reasoning.iter().copied().collect(),
                }
            })
            .collect()
    }

    pub(crate) fn model_ref(id: &str) -> Result<ModelRef, ModelConfigError> {
        id.parse().map_err(|_| ModelConfigError::InvalidReference)
    }
}

pub(crate) fn serialize_openai_request_for_budget(
    request: &ModelRequest,
    writer: &mut dyn std::io::Write,
) -> Result<(), ()> {
    openai::serialize_request_for_budget(request, writer).map_err(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(base_url: &str, output_budget_tokens: u32) -> ModelConfig {
        config_with_credential_env(base_url, output_budget_tokens, "TEST_OPENAI_API_KEY")
    }

    fn config_with_credential_env(
        base_url: &str,
        output_budget_tokens: u32,
        api_key_env: &str,
    ) -> ModelConfig {
        ModelConfig::OpenAiResponses {
            model: "provider-model".to_owned(),
            base_url: base_url.to_owned(),
            api_key_env: api_key_env.to_owned(),
            physical_context_window: 10_000,
            output_budget_tokens,
            safety_margin_tokens: 1_000,
            supported_reasoning: BTreeSet::from([ReasoningPreference::Auto]),
            supports_tools: true,
            request_timeout_seconds: Some(30),
        }
    }

    #[test]
    fn model_configs_expose_credential_environment_names_without_debug_leaks() {
        const FIRST_ENV: &str = "MINICORE_FIRST_MODEL_CREDENTIAL";
        const SECOND_ENV: &str = "MINICORE_SECOND_MODEL_CREDENTIAL";

        let first = config_with_credential_env("https://example.invalid/v1", 1_000, FIRST_ENV);
        let second = config_with_credential_env("https://example.invalid/v1", 1_000, SECOND_ENV);

        assert_eq!(first.credential_env_name(), FIRST_ENV);
        assert_eq!(second.credential_env_name(), SECOND_ENV);
        let debug = format!("{first:?} {second:?}");
        assert!(!debug.contains(FIRST_ENV));
        assert!(!debug.contains(SECOND_ENV));
    }

    #[test]
    fn config_rejects_unsafe_base_urls_and_zero_output_budget() {
        for base_url in [
            "ftp://example.invalid/v1",
            "https://user:pass@example.invalid/v1",
            "https://example.invalid/v1?secret=value",
            "https://example.invalid/v1#fragment",
        ] {
            assert_eq!(
                config(base_url, 1_000).validate(),
                Err(ModelConfigError::InvalidConfiguration)
            );
        }
        assert_eq!(
            config("https://example.invalid/v1", 0).validate(),
            Err(ModelConfigError::InvalidConfiguration)
        );
        assert_eq!(
            openai::endpoint("https://example.invalid/v1/")
                .unwrap()
                .as_str(),
            "https://example.invalid/v1/responses"
        );

        let mut reserved_equals_physical = config("https://example.invalid/v1", 9_000);
        assert_eq!(
            reserved_equals_physical.validate(),
            Err(ModelConfigError::InvalidConfiguration)
        );
        let ModelConfig::OpenAiResponses {
            output_budget_tokens,
            request_timeout_seconds,
            ..
        } = &mut reserved_equals_physical;
        *output_budget_tokens = 1_000;
        *request_timeout_seconds = Some(0);
        assert_eq!(
            reserved_equals_physical.validate(),
            Err(ModelConfigError::InvalidConfiguration)
        );
    }

    #[test]
    fn models_require_a_nonempty_key_and_build_exact_descriptors() {
        let values = BTreeMap::from([(
            "profile-model".to_owned(),
            ModelConfig::OpenAiResponses {
                model: "provider-model".to_owned(),
                base_url: "https://example.invalid/v1/".to_owned(),
                api_key_env: "TEST_OPENAI_API_KEY".to_owned(),
                physical_context_window: 20_000,
                output_budget_tokens: 3_000,
                safety_margin_tokens: 2_000,
                supported_reasoning: BTreeSet::from([
                    ReasoningPreference::Auto,
                    ReasoningPreference::High,
                ]),
                supports_tools: false,
                request_timeout_seconds: Some(30),
            },
        )]);
        assert!(matches!(
            Models::from_config_with_env(&values, |_| None),
            Err(ModelConfigError::MissingApiKey)
        ));
        assert!(matches!(
            Models::from_config_with_env(&values, |_| Some("  ".to_owned())),
            Err(ModelConfigError::MissingApiKey)
        ));
        assert!(matches!(
            Models::from_config_with_env(&values, |_| Some("bad\nkey".to_owned())),
            Err(ModelConfigError::InvalidConfiguration)
        ));

        let debug = format!("{:?}", values["profile-model"]);
        assert!(debug.contains("provider-model"));
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("example.invalid"));
        assert!(!debug.contains("TEST_OPENAI_API_KEY"));
        assert!(!debug.contains("bad\nkey"));

        let models =
            Models::from_config_with_env(&values, |_| Some("test-key".to_owned())).unwrap();
        let descriptor = models.get("profile-model").unwrap().descriptor().clone();
        assert_eq!(descriptor.model_ref.as_str(), "profile-model");
        assert_eq!(descriptor.context_window, 15_000);
        assert!(!descriptor.supports_tools);
        assert_eq!(
            descriptor.supported_reasoning,
            BTreeSet::from([ReasoningPreference::Auto, ReasoningPreference::High])
        );
    }
}
