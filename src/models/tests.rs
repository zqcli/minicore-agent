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

    let models = Models::from_config_with_env(&values, |_| Some("test-key".to_owned())).unwrap();
    let descriptor = models.get("profile-model").unwrap().descriptor().clone();
    assert_eq!(descriptor.model_ref.as_str(), "profile-model");
    assert_eq!(descriptor.context_window, 15_000);
    assert_eq!(
        models.physical_context_window("profile-model"),
        Some(20_000)
    );
    assert!(!descriptor.supports_tools);
    assert_eq!(
        descriptor.supported_reasoning,
        BTreeSet::from([ReasoningPreference::Auto, ReasoningPreference::High])
    );
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct DefaultProviderBudget;

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

struct BudgetCountingWriter<'a>(&'a mut usize);

impl std::io::Write for BudgetCountingWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        *self.0 = self.0.saturating_add(buf.len());
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Models {
    pub(crate) fn from_values(values: BTreeMap<String, Arc<dyn Model>>) -> Self {
        let entries = values
            .into_iter()
            .map(|(id, model)| {
                (
                    id,
                    ModelEntry {
                        physical_context_window: model.descriptor().context_window,
                        model,
                        budget: Arc::new(DefaultProviderBudget),
                    },
                )
            })
            .collect();
        Self { values: entries }
    }
}
