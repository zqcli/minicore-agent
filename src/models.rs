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
    #[error("model TLS trust store is invalid")]
    InvalidTlsTrustStore,
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

fn utility_budget_error(kind: minicore_runtime::model::ModelErrorKind) -> ModelError {
    use minicore_runtime::error::{DiagnosticCategory, DiagnosticCode, DiagnosticSummary};
    ModelError::permanent(
        kind,
        minicore_runtime::model::DeliveryState::NotStarted,
        DiagnosticSummary::new(
            DiagnosticCode::InvalidConfiguration,
            DiagnosticCategory::Model,
            minicore_runtime::value::BoundedText::new(
                "utility provider input budget or lifecycle check failed",
            )
            .expect("static utility diagnostic"),
            false,
        ),
    )
}

struct BudgetCheckedModel {
    inner: Arc<dyn Model>,
    budget: Arc<dyn ProviderBudget>,
}

impl Model for BudgetCheckedModel {
    fn descriptor(&self) -> &minicore_runtime::model::ModelDescriptor {
        self.inner.descriptor()
    }

    fn start(
        &self,
        request: ModelRequest,
        context: minicore_runtime::model::ModelCallContext,
    ) -> minicore_runtime::model::ModelStartFuture<'_> {
        Box::pin(async move {
            use minicore_runtime::model::ModelErrorKind;
            if context.cancellation.is_cancelled() {
                return Err(utility_budget_error(ModelErrorKind::Cancelled));
            }
            if std::time::Instant::now() >= context.deadline {
                return Err(utility_budget_error(ModelErrorKind::Timeout));
            }
            let tokens = self.budget.estimate_request_tokens(
                &request,
                Some(context.loop_id),
                Some(context.request_index),
            )?;
            if tokens > self.descriptor().context_window {
                return Err(utility_budget_error(ModelErrorKind::InvalidRequest));
            }
            self.inner.start(request, context).await
        })
    }
}

#[derive(Clone)]
pub(crate) struct ModelEntry {
    pub(crate) physical_context_window: u64,
    pub(crate) model: Arc<dyn Model>,
    pub(crate) budget: Arc<dyn ProviderBudget>,
}

#[derive(Clone)]
pub(crate) struct Models {
    values: BTreeMap<String, ModelEntry>,
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
                    let reserved = output_budget_tokens + safety_margin_tokens;
                    let settings = openai::OpenAiResponsesSettings {
                        model_ref: Self::model_ref(id)?,
                        provider_model: model.clone(),
                        endpoint: openai::endpoint(base_url)?,
                        api_key,
                        effective_context_window: u64::from(physical_context_window - reserved),
                        output_budget_tokens: *output_budget_tokens,
                        supported_reasoning: supported_reasoning.clone(),
                        supports_tools: *supports_tools,
                        request_timeout: request_timeout_seconds.map(Duration::from_secs),
                    };
                    let openai_model = Arc::new(openai::OpenAiResponsesModel::new(settings)?);
                    ModelEntry {
                        physical_context_window: u64::from(*physical_context_window),
                        model: Arc::clone(&openai_model) as Arc<dyn Model>,
                        budget: Arc::clone(&openai_model) as Arc<dyn ProviderBudget>,
                    }
                }
            };
            values.insert(id.clone(), entry);
        }
        Ok(Self { values })
    }

    fn entry(&self, id: &str) -> Result<&ModelEntry, ModelConfigError> {
        self.values.get(id).ok_or(ModelConfigError::NotFound)
    }

    pub(crate) fn get(&self, id: &str) -> Result<Arc<dyn Model>, ModelConfigError> {
        Ok(Arc::clone(&self.entry(id)?.model))
    }

    /// The same transport and serializer, with an explicit utility-only budget.
    pub(crate) fn get_utility(&self, id: &str) -> Result<Arc<dyn Model>, ModelConfigError> {
        let entry = self.entry(id)?;
        Ok(Arc::new(BudgetCheckedModel {
            inner: Arc::clone(&entry.model),
            budget: Arc::clone(&entry.budget),
        }))
    }

    pub(crate) fn physical_context_window(&self, id: &str) -> Option<u64> {
        self.entry(id)
            .ok()
            .map(|entry| entry.physical_context_window)
    }

    pub(crate) fn get_budget(&self, id: &str) -> Result<Arc<dyn ProviderBudget>, ModelConfigError> {
        Ok(Arc::clone(&self.entry(id)?.budget))
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

/// Validate provider-owned metadata at the durable history admission boundary.
pub(crate) fn validate_history_replay(
    replay: &minicore_runtime::model::ProviderReplay,
    parts: &[minicore_runtime::model::AssistantPart],
) -> Result<(), ()> {
    openai::validate_history_replay(replay, parts)
}

#[cfg(test)]
pub(crate) mod tests;
