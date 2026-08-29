use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use minicore_runtime::model::{Model, ModelRef, ReasoningPreference};

#[derive(Clone, Debug, Deserialize)]
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
                if model.is_empty()
                    || base_url.is_empty()
                    || api_key_env.is_empty()
                    || supported_reasoning.is_empty()
                    || *physical_context_window
                        <= output_budget_tokens.saturating_add(*safety_margin_tokens)
                    || request_timeout_seconds.is_some_and(|seconds| seconds == 0)
                {
                    return Err(ModelConfigError::InvalidConfiguration);
                }
                Ok(())
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum ModelConfigError {
    #[error("model configuration is invalid")]
    InvalidConfiguration,
    #[error("model is not configured")]
    NotFound,
    #[error("OpenAI Responses models are not implemented in this phase")]
    NotImplemented,
    #[error("model reference is invalid")]
    InvalidReference,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct ModelInfo {
    pub(crate) id: String,
    pub(crate) model_ref: ModelRef,
    pub(crate) context_window: u64,
    pub(crate) supports_tools: bool,
    pub(crate) supported_reasoning: Vec<ReasoningPreference>,
}

pub(crate) struct Models {
    values: BTreeMap<String, Arc<dyn Model>>,
}

impl Clone for Models {
    fn clone(&self) -> Self {
        Self {
            values: self
                .values
                .iter()
                .map(|(id, model)| (id.clone(), Arc::clone(model)))
                .collect(),
        }
    }
}

impl Models {
    pub(crate) async fn from_config(
        config: &BTreeMap<String, ModelConfig>,
    ) -> Result<Self, ModelConfigError> {
        for value in config.values() {
            value.validate()?;
        }
        if !config.is_empty() {
            return Err(ModelConfigError::NotImplemented);
        }
        Ok(Self {
            values: BTreeMap::new(),
        })
    }

    #[cfg(test)]
    pub(crate) fn from_values(values: BTreeMap<String, Arc<dyn Model>>) -> Self {
        Self { values }
    }

    pub(crate) fn get(&self, id: &str) -> Result<Arc<dyn Model>, ModelConfigError> {
        self.values
            .get(id)
            .map(Arc::clone)
            .ok_or(ModelConfigError::NotFound)
    }

    pub(crate) fn contains(&self, id: &str) -> bool {
        self.values.contains_key(id)
    }

    pub(crate) fn list(&self) -> Vec<ModelInfo> {
        self.values
            .iter()
            .map(|(id, model)| {
                let descriptor = model.descriptor();
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
