use super::*;

/// Owns temporary paths when created with `new`; `at` reopens a caller-owned store.
pub(super) struct AgentFixture {
    _directory: Option<TempDir>,
    pub(super) data_dir: PathBuf,
    pub(super) workspace: PathBuf,
    config: AgentConfig,
    models: BTreeMap<String, Arc<ScriptedModel>>,
}

impl AgentFixture {
    pub(super) fn new(label: &str) -> Self {
        let directory = TempDir::new(label);
        let mut fixture = Self::at(&directory.path().join("data"));
        fixture.workspace = directory.path().join("workspace");
        std::fs::create_dir_all(&fixture.workspace).unwrap();
        fixture._directory = Some(directory);
        fixture
    }

    pub(super) fn at(data_dir: &Path) -> Self {
        Self {
            _directory: None,
            data_dir: data_dir.to_path_buf(),
            workspace: data_dir.join("workspace"),
            config: config(data_dir.to_path_buf(), BTreeMap::new(), read_profile()),
            models: BTreeMap::new(),
        }
    }

    pub(super) fn file(self, path: &str, content: &[u8]) -> Self {
        let path = self.workspace.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
        self
    }

    pub(super) fn script(self, steps: impl IntoIterator<Item = Step>) -> Self {
        self.models(BTreeMap::from([(
            "main".to_owned(),
            ScriptedModel::new("main", steps).with_pacing(std::time::Duration::from_millis(2)),
        )]))
    }

    pub(super) fn models(mut self, models: BTreeMap<String, Arc<ScriptedModel>>) -> Self {
        self.config.models = models
            .keys()
            .map(|id| (id.clone(), model_config("MINICORE_AGENT_TEST_KEY")))
            .collect();
        self.models = models;
        self
    }

    pub(super) fn model(&self, id: &str) -> Arc<ScriptedModel> {
        Arc::clone(&self.models[id])
    }

    pub(super) fn profile(mut self, profile: Profile) -> Self {
        self.config.profiles.insert("test".to_owned(), profile);
        self
    }

    pub(super) fn model_configs(mut self, configs: BTreeMap<String, ModelConfig>) -> Self {
        self.config.models = configs;
        self
    }

    pub(super) fn auto(mut self) -> Self {
        self.config.compaction = CompactionConfig {
            enabled: true,
            trigger_percent: 80,
            target_percent: 50,
        };
        // Keep candidate provider timeouts observable in derived prompt options.
        if self
            .config
            .models
            .values()
            .any(|model| model.request_timeout().is_some())
        {
            self.config.loop_options.prompt_timeout_seconds = Some(1);
            self.config.loop_options.model_timeout_seconds = Some(5);
        }
        self
    }

    pub(super) fn options(mut self, options: AgentOptions) -> Self {
        self.config.event_capacity = options.agent_event_capacity;
        self.config.loop_options.event_capacity = options.loop_event_capacity;
        self
    }

    pub(super) async fn open(&self) -> Agent {
        let models = self
            .models
            .iter()
            .map(|(id, model)| (id.clone(), Arc::clone(model) as Arc<dyn Model>))
            .collect();
        Agent::open_with_models(self.config.clone(), Models::from_values(models))
            .await
            .unwrap()
    }

    pub(super) async fn start(&self) -> (Agent, crate::agent::SessionInfo) {
        let mut agent = self.open().await;
        let info = create_session(&mut agent, &self.workspace).await;
        (agent, info)
    }
}
