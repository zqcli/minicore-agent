use super::*;

pub(crate) struct PreparedSession {
    pub(super) stored: crate::store::StoredSession,
    pub(super) workspace: Arc<Workspace>,
    pub(super) compaction: Arc<CompactionState>,
}

pub(super) struct ResolvedSessionSettings {
    pub(super) profile: String,
    pub(super) model: String,
    pub(super) reasoning: ReasoningPreference,
}

pub(super) struct ExecutionConfigFactory<'a> {
    pub(super) models: &'a Models,
    pub(super) command_environment: &'a CommandEnvironment,
    pub(super) policy: crate::compaction::CompactionPolicy,
}

impl ExecutionConfigFactory<'_> {
    pub(super) fn setup(
        &self,
        record: &SessionRecord,
        workspace: Arc<Workspace>,
        options: LoopOptions,
        compaction: Arc<CompactionState>,
        events: &AgentEventSink,
    ) -> Result<SessionSetup, AgentError> {
        let presentation = crate::presentation::Presentation::new(
            record.session_id,
            events.clone(),
            Arc::clone(&compaction),
        );
        presentation.set_model_label(record.model.clone());
        let tool_data = Arc::new(ToolData::new());
        let tool_owners = ToolOwners::new();
        let observer = ToolObserver::new(record.session_id, Arc::clone(&tool_data), events.clone());
        let parts = SessionParts {
            presentation,
            observer,
            tool_owners,
            compaction,
        };
        let (config, auto) = self.build(record, workspace, &parts)?;
        Ok(SessionSetup {
            parts,
            tool_data,
            config,
            auto,
            options,
        })
    }

    pub(super) fn build(
        &self,
        record: &SessionRecord,
        workspace: Arc<Workspace>,
        parts: &SessionParts,
    ) -> Result<(ExecutionConfig, Option<AutoContext>), AgentError> {
        let model = self
            .models
            .get(&record.model)
            .map_err(map_model_config_error)?;
        let budget = self
            .models
            .get_budget(&record.model)
            .map_err(map_model_config_error)?;
        let utility_model = self
            .models
            .get_utility(&record.model)
            .map_err(map_model_config_error)?;
        let auto = self.policy.enabled.then(|| AutoContext {
            binding: crate::compaction::auto::AutoContextBinding {
                model: utility_model,
                budget: Arc::clone(&budget),
                policy: self.policy,
            },
            state: Arc::clone(&parts.compaction),
        });
        // The binding is published only when the Session actually installs
        // this config (`Session::new`/`update`/`replace_future_config`).
        // Publishing here would bind the shared state to a config that may
        // still fail later in this build or be rejected before commit.
        //
        // With automatic compaction disabled no recovery ticket can ever
        // exist, so keep the raw model (and its established cancellation
        // behavior) instead of paying the wrapper's per-request content and
        // ticket hashing for nothing.
        let model: Arc<dyn minicore_runtime::model::Model> = if auto.is_some() {
            crate::compaction::CompactingModel::new(model, budget, Arc::clone(&parts.compaction))
        } else {
            model
        };
        let model = crate::presentation::PresentationModel::new_with_observer(
            model,
            self.models
                .physical_context_window(&record.model)
                .expect("validated model"),
            Arc::clone(&parts.presentation),
            Arc::clone(&parts.observer),
        );
        let tools = crate::tools::build_tools_with_presentation(
            &record.tools,
            Arc::clone(&workspace),
            self.command_environment.clone(),
            &parts.presentation,
            &parts.observer,
            &parts.tool_owners,
        )
        .map_err(map_build_tools_error)?;
        let policy: Option<Arc<dyn ToolPolicy>> = if record.tools.is_empty() {
            None
        } else {
            Some(crate::presentation::PresentationPolicy::new_with_observer(
                Arc::new(Policy::new(record.approval)),
                Arc::clone(&parts.observer),
            ))
        };
        let prompt: Arc<dyn PromptProvider> = Arc::new(
            ProjectPromptProvider::with_auto(
                workspace,
                record.system_prompt.clone(),
                Arc::clone(&parts.compaction),
                auto.clone(),
            )
            .map_err(|_| AgentError::InvalidSessionSettings)?,
        );
        let config = ExecutionConfig::new(model, record.reasoning, tools, policy, prompt)
            .map_err(|_| AgentError::InvalidSessionSettings)?;
        Ok((config, auto))
    }
}

pub(crate) struct SessionParts {
    pub(crate) presentation: Arc<crate::presentation::Presentation>,
    pub(crate) tool_owners: Arc<ToolOwners>,
    pub(crate) observer: Arc<ToolObserver>,
    pub(crate) compaction: Arc<CompactionState>,
}

pub(crate) struct SessionSetup {
    pub(crate) parts: SessionParts,
    pub(crate) tool_data: Arc<ToolData>,
    pub(crate) config: ExecutionConfig,
    pub(crate) auto: Option<AutoContext>,
    pub(crate) options: LoopOptions,
}

pub(super) fn build_record(
    session_id: crate::ids::SessionId,
    settings: &ResolvedSessionSettings,
    profile: &Profile,
    title: Option<String>,
    workspace: PathBuf,
) -> Result<SessionRecord, AgentError> {
    let now = crate::store::utc_timestamp().map_err(|_| AgentError::Store)?;
    Ok(SessionRecord {
        format_version: SESSION_FORMAT_VERSION,
        session_id,
        title,
        profile: settings.profile.clone(),
        workspace,
        model: settings.model.clone(),
        reasoning: settings.reasoning,
        system_prompt: profile.system_prompt.clone(),
        tools: profile.tools.clone(),
        max_tool_rounds: profile.max_tool_rounds,
        approval: profile.approval,
        created_at: now.clone(),
        updated_at: now,
    })
}

const REMOVED_TOOL_NAME: &str = "subagent";

pub(super) fn contains_removed_tool(tools: &[String]) -> bool {
    tools.iter().any(|name| name == REMOVED_TOOL_NAME)
}

fn map_build_tools_error(error: BuildToolsError) -> AgentError {
    match error {
        BuildToolsError::InvalidConfiguration => AgentError::InvalidSessionSettings,
        BuildToolsError::Internal => AgentError::Internal,
    }
}

pub(super) fn map_model_config_error(error: ModelConfigError) -> AgentError {
    match error {
        ModelConfigError::NotFound => AgentError::ModelNotFound,
        ModelConfigError::MissingApiKey => {
            AgentError::Config(crate::config::ConfigError::MissingModelApiKey)
        }
        ModelConfigError::InvalidTlsTrustStore => {
            AgentError::Config(crate::config::ConfigError::InvalidTlsTrustStore)
        }
        ModelConfigError::InvalidConfiguration
        | ModelConfigError::ClientBuild
        | ModelConfigError::InvalidReference => {
            AgentError::Config(crate::config::ConfigError::InvalidModel)
        }
    }
}

pub(super) fn command_environment(config: &AgentConfig) -> CommandEnvironment {
    CommandEnvironment::new(
        config
            .models
            .values()
            .map(ModelConfig::credential_env_name)
            .map(OsString::from),
    )
}

// Keep prior credential names scrubbed from future Bash children too. A
// removed model's key may still be present in the Agent process environment;
// dropping that name during reload would make it visible to a later command.
pub(super) fn reload_command_environment(
    current: &CommandEnvironment,
    candidate: &AgentConfig,
) -> CommandEnvironment {
    current.extended(
        candidate
            .models
            .values()
            .map(ModelConfig::credential_env_name)
            .map(OsString::from),
    )
}

impl Agent {
    pub(super) fn execution_factory(&self) -> ExecutionConfigFactory<'_> {
        ExecutionConfigFactory {
            models: &self.models,
            command_environment: &self.command_environment,
            policy: self.config.compaction.policy(),
        }
    }

    pub(super) fn resolve_session_settings(
        &self,
        request: &CreateSession,
    ) -> Result<ResolvedSessionSettings, AgentError> {
        let profile = if request.profile.is_empty() {
            self.config.default_profile.clone()
        } else {
            request.profile.clone()
        };
        let template = self
            .profiles
            .get(&profile)
            .ok_or(AgentError::ProfileNotFound)?;
        let model = request
            .model
            .clone()
            .unwrap_or_else(|| template.model.clone());
        let reasoning = request.reasoning.unwrap_or(template.reasoning);
        let configured = self.models.get(&model).map_err(map_model_config_error)?;
        let descriptor = configured.descriptor();
        if !descriptor.supports_reasoning(reasoning)
            || (!template.tools.is_empty() && !descriptor.supports_tools)
        {
            return Err(AgentError::InvalidSessionSettings);
        }
        Ok(ResolvedSessionSettings {
            profile,
            model,
            reasoning,
        })
    }
}
