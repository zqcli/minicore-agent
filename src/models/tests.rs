use super::*;

fn catalog_config(
    models: BTreeMap<String, ModelConfig>,
    required: &[&str],
) -> crate::config::AgentConfig {
    use crate::config::{AgentConfig, CompactionConfig, LoopOverrides, Profile};
    AgentConfig {
        data_dir: std::path::PathBuf::from("unused"),
        event_capacity: 256,
        default_profile: "profile-0".into(),
        models,
        profiles: required
            .iter()
            .enumerate()
            .map(|(index, model)| {
                (
                    format!("profile-{index}"),
                    Profile {
                        model: (*model).into(),
                        system_prompt: "Answer briefly.".into(),
                        tools: vec![],
                        reasoning: ReasoningPreference::Auto,
                        max_tool_rounds: 0,
                        approval: crate::profiles::ApprovalMode::Auto,
                    },
                )
            })
            .collect(),
        loop_options: LoopOverrides::default(),
        compaction: CompactionConfig::default(),
    }
}

#[test]
fn unreferenced_credentials_are_deferred_but_catalog_metadata_remains_available() {
    let config = catalog_config(
        BTreeMap::from([
            (
                "active".into(),
                config_with_credential_env("https://example.invalid", 1000, "ACTIVE_KEY"),
            ),
            (
                "spare".into(),
                config_with_credential_env("https://example.invalid", 1000, "SPARE_KEY"),
            ),
        ]),
        &["active"],
    );
    for spare in [None, Some(" "), Some("bad\nkey")] {
        let mut reads = Vec::new();
        let models = Models::from_config_with_env(&config, |name| {
            reads.push(name.to_owned());
            if name == "ACTIVE_KEY" {
                Some("active-key".into())
            } else {
                spare.map(str::to_owned)
            }
        })
        .unwrap();
        assert_eq!(reads, ["ACTIVE_KEY", "SPARE_KEY"]);
        let expected = if spare == Some("bad\nkey") {
            ModelConfigError::InvalidConfiguration
        } else {
            ModelConfigError::MissingApiKey
        };
        assert!(matches!(models.get("spare"), Err(error) if error == expected));
        assert!(matches!(models.get_utility("spare"), Err(error) if error == expected));
        assert!(matches!(models.get_budget("spare"), Err(error) if error == expected));
        assert!(matches!(
            models.get("unknown"),
            Err(ModelConfigError::NotFound)
        ));
        assert!(models.clone().get("active").is_ok());
        assert_eq!(
            models
                .list()
                .iter()
                .map(|info| info.id.as_str())
                .collect::<Vec<_>>(),
            ["active", "spare"]
        );
        assert_eq!(models.physical_context_window("spare"), Some(10_000));
        let mut required = config.clone();
        required.profiles.get_mut("profile-0").unwrap().model = "spare".into();
        assert!(
            matches!(Models::from_config_with_env(&required, |_| spare.map(str::to_owned)), Err(error) if error == expected)
        );
    }
}

#[tokio::test]
async fn sessions_validate_unreferenced_credentials_and_reload_stays_atomic() {
    use crate::agent::{Agent, CreateSession};
    let temp = crate::test_support::TempDir::new("model-credentials");
    let mut config = catalog_config(
        BTreeMap::from([
            (
                "active".into(),
                config_with_credential_env("https://example.invalid", 1000, "ACTIVE_KEY"),
            ),
            (
                "spare".into(),
                config_with_credential_env("https://example.invalid", 1000, "SPARE_KEY"),
            ),
        ]),
        &["active"],
    );
    config.data_dir = temp.path().join("data");
    let without_spare = || {
        Models::from_config_with_env(&config, |name| (name == "ACTIVE_KEY").then(|| "key".into()))
            .unwrap()
    };
    let mut agent = Agent::open_with_models(config.clone(), without_spare())
        .await
        .unwrap();
    let session = || CreateSession {
        workspace: temp.path().to_owned(),
        profile: String::new(),
        model: Some("spare".into()),
        reasoning: None,
        title: None,
    };
    assert!(agent.create_session(session()).await.is_err());
    let ready = Models::from_config_with_env(&config, |_| Some("key".into())).unwrap();
    agent.apply_reload_settings(config.clone(), ready).unwrap();
    agent.create_session(session()).await.unwrap();
    assert!(
        agent
            .apply_reload_settings(config.clone(), without_spare())
            .is_err()
    );
    // Failed reload must retain the ready catalog as well as existing sessions.
    agent.create_session(session()).await.unwrap();
    agent.shutdown().await.unwrap();
}

#[test]
fn token_estimate_handles_ascii_cjk_and_mixed_utf8_without_zeroing_small_inputs() {
    for (text, expected) in [
        ("".into(), 0),
        ("a".into(), 1),
        ("a".repeat(401), 101),
        ("界".repeat(4096), 4096),
        ("é".repeat(1000), 1000),
        ("😀".repeat(1000), 1000),
        (format!("{}{}", "a".repeat(1000), "界".repeat(1000)), 1000),
    ] {
        assert_eq!(estimate_tokens(text.as_bytes()), expected);
    }
}

#[tokio::test]
async fn in_stream_context_rejection_recovers_through_real_compaction() {
    use crate::agent::{Agent, CreateSession};
    use crate::openai_mock::{MockResponse, MockServer};
    use serde_json::json;
    let reply = |text| {
        MockResponse::sse(&[
            json!({"type":"response.output_text.delta","delta":text}),
            json!({"type":"response.completed","response":{"status":"completed"}}),
        ])
    };
    let server = MockServer::spawn([
        MockResponse::sse(&[
            json!({"type":"response.output_item.done","output_index":0,"item":{"type":"function_call","call_id":"read_1","name":"read","arguments":"{\"path\":\"note.txt\"}"}}),
            json!({"type":"response.completed","response":{"status":"completed"}}),
        ]),
        MockResponse::sse(&[
            json!({"type":"response.created"}),
            json!({"type":"response.failed","response":{"error":{"code":"context_length_exceeded","message":"PRIVATE"}}}),
        ]),
        reply("The earlier conversation covered a long historical topic."),
        reply("recovered answer"),
    ]).await;
    let temp = crate::test_support::TempDir::new("sse-recovery");
    std::fs::write(
        temp.path().join("note.txt"),
        "historical data\n".repeat(400),
    )
    .unwrap();
    let mut config = catalog_config(
        BTreeMap::from([("main".into(), config(server.base_url(), 1000))]),
        &["main"],
    );
    config.data_dir = temp.path().join("data");
    config.profiles.get_mut("profile-0").unwrap().tools = vec!["read".into()];
    let models = Models::from_config_with_env(&config, |_| Some("key".into())).unwrap();
    let mut agent = Agent::open_with_models(config, models).await.unwrap();
    let session = agent
        .create_session(CreateSession {
            workspace: temp.path().to_owned(),
            profile: String::new(),
            model: None,
            reasoning: None,
            title: None,
        })
        .await
        .unwrap();
    let turn = agent
        .send_for_test(session.session_id, "read note.txt".into())
        .await
        .unwrap();
    let result = agent.wait_turn(turn).await.unwrap();
    agent.join_turn_task_for_test(session.session_id).await;
    assert_eq!(
        result.report.outcome,
        minicore_runtime::LoopOutcome::Completed
    );
    assert_eq!(
        agent
            .session_context(session.session_id)
            .unwrap()
            .recovery
            .unwrap()
            .outcome,
        "recovered"
    );
    agent.shutdown().await.unwrap();
    let requests = server.finish().await;
    assert_eq!(requests.len(), 4);
    assert!(requests[3].body().len() < requests[1].body().len());
    assert!(String::from_utf8_lossy(requests[3].body()).contains("earlier conversation"));
}

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
    let catalog = catalog_config(values.clone(), &["profile-model"]);
    assert!(matches!(
        Models::from_config_with_env(&catalog, |_| None),
        Err(ModelConfigError::MissingApiKey)
    ));
    assert!(matches!(
        Models::from_config_with_env(&catalog, |_| Some("  ".to_owned())),
        Err(ModelConfigError::MissingApiKey)
    ));
    assert!(matches!(
        Models::from_config_with_env(&catalog, |_| Some("bad\nkey".to_owned())),
        Err(ModelConfigError::InvalidConfiguration)
    ));

    let debug = format!("{:?}", values["profile-model"]);
    assert!(debug.contains("provider-model"));
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("example.invalid"));
    assert!(!debug.contains("TEST_OPENAI_API_KEY"));
    assert!(!debug.contains("bad\nkey"));

    let models = Models::from_config_with_env(&catalog, |_| Some("test-key".to_owned())).unwrap();
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
        _loop_id: Option<LoopId>,
        _request_index: Option<u32>,
    ) -> Result<u64, ModelError> {
        let mut body = Vec::new();
        openai::serialize_request_for_budget(request, &mut body)?;
        Ok(estimate_tokens(&body))
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
                        descriptor: model.descriptor().clone(),
                        provider: Ok(ModelProvider {
                            model,
                            budget: Arc::new(DefaultProviderBudget),
                        }),
                    },
                )
            })
            .collect();
        Self { values: entries }
    }
}
